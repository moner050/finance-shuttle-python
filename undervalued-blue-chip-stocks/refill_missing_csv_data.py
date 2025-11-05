# -*- coding: utf-8 -*-
"""
refill_missing_data.py

기존 details_cache CSV 파일에서 비어있는 재무 데이터를 다시 채워주는 스크립트
"""

import os
import time
import math
import random
import warnings
import logging
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from curl_cffi import requests

warnings.filterwarnings("ignore")
logging.getLogger("yfinance").setLevel(logging.CRITICAL)

session = requests.Session(impersonate="chrome")

# ===================== CONFIG =====================
CONFIG = {
    "INPUT_FILE": "details_cache_us_all_20251105_131751.csv",  # 입력 파일명
    "OUTPUT_SUFFIX": "_refilled",  # 출력 파일 접미사

    # 재시도 설정
    "MAX_RETRIES": 3,
    "RETRY_DELAY": 1.0,  # 초

    # 병렬 처리
    "WORKERS": 1,  # 동시 처리 스레드 수

    # 진행상황 출력
    "PROGRESS_INTERVAL": 50,  # N개마다 진행상황 출력

    # 어떤 필드가 비어있을 때 다시 수집할지 (우선순위가 높은 필드들)
    "CRITICAL_FIELDS": [
        "Sector", "Industry", "MktCap($B)",
        "PE", "RevYoY", "OpMarginTTM"
    ],

    # 디버깅
    "VERBOSE": True,
    "SAVE_BACKUP": True,  # 원본 파일 백업 여부
}


# ============== 데이터 검증 함수들 (원본과 동일) ==============

def validate_numeric(value, min_val=None, max_val=None, allow_negative=False):
    """숫자 값 검증"""
    if value is None or (isinstance(value, float) and (math.isnan(value) or math.isinf(value))):
        return None
    try:
        val = float(value)
        if math.isnan(val) or math.isinf(val):
            return None
        if not allow_negative and val < 0:
            return None
        if min_val is not None and val < min_val:
            return None
        if max_val is not None and val > max_val:
            return None
        return val
    except (TypeError, ValueError):
        return None


def validate_percentage(value, min_pct=-100, max_pct=1000):
    return validate_numeric(value, min_val=min_pct, max_val=max_pct, allow_negative=True)


def validate_ratio(value, min_ratio=0, max_ratio=1000):
    return validate_numeric(value, min_val=min_ratio, max_val=max_ratio, allow_negative=False)


def validate_market_cap(value):
    return validate_numeric(value, min_val=1_000_000, max_val=20_000_000_000_000, allow_negative=False)


def validate_price(value):
    return validate_numeric(value, min_val=0.01, max_val=100_000, allow_negative=False)


# ============== 재무제표 유틸 함수들 ==============

REV_ALIASES = ["total revenue", "revenues", "revenue", "net sales", "sales", "total net sales"]
OP_ALIASES = ["operating income", "operating income (loss)", "income from operations", "operating profit", "ebit"]
FCF_ALIASES = ["free cash flow", "free cashflow", "freecashflow"]
EPS_ALIASES = ["diluted eps", "basic eps", "eps (diluted)", "eps (basic)", "earnings per share", "eps"]
NET_INCOME_ALIASES = ["net income", "net income common stockholders"]
DIL_SHARES_ALIASES = ["diluted average shares", "weighted average shares diluted"]


def _find_row(index_like, aliases, exclude=None):
    if index_like is None:
        return None
    exclude = [w.lower() for w in (exclude or [])]
    idx = [str(x).lower() for x in index_like]
    for key in aliases:
        k = key.lower()
        for i, s in enumerate(idx):
            if k in s and not any(x in s for x in exclude):
                return index_like[i]
    return None


def ttm_sum(df, row, n=4):
    if df is None or df.empty or row not in df.index or df.shape[1] < n:
        return None
    cols = sorted(df.columns, reverse=True)[:n]
    try:
        vals = pd.to_numeric(df.loc[row, cols], errors="coerce").fillna(0)
        result = float(vals.sum())
        return result if not math.isnan(result) else None
    except:
        return None


def ttm_yoy_growth(df_q, row):
    if df_q is None or df_q.empty or row not in df_q.index or df_q.shape[1] < 8:
        return None
    cols = sorted(df_q.columns, reverse=True)
    try:
        curr = float(pd.to_numeric(df_q.loc[row, cols[:4]], errors="coerce").fillna(0).sum())
        prev = float(pd.to_numeric(df_q.loc[row, cols[4:8]], errors="coerce").fillna(0).sum())
    except:
        return None
    if prev <= 0:
        return None
    growth = (curr / prev) - 1.0
    return validate_percentage(growth, min_pct=-0.99, max_pct=9.99)


def _safe_df(getter, max_retries=3):
    """DataFrame 안전하게 가져오기"""
    for attempt in range(max_retries):
        try:
            df = getter()
            if df is not None and hasattr(df, 'empty') and not df.empty:
                return df
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(0.5 + random.uniform(0, 0.5))
    return None


# ============== PER 계산 함수 ==============

def calculate_pe_ratio(ticker, price, info, df_q, df_a):
    """PER 계산"""
    pe_values = []

    # 방법 1: info에서 직접
    try:
        trailing_pe = info.get("trailingPE")
        forward_pe = info.get("forwardPE")
        if trailing_pe and trailing_pe > 0:
            validated = validate_ratio(trailing_pe, min_ratio=0.1, max_ratio=500)
            if validated:
                pe_values.append(validated)
        if forward_pe and forward_pe > 0:
            validated = validate_ratio(forward_pe, min_ratio=0.1, max_ratio=500)
            if validated:
                pe_values.append(validated)
    except:
        pass

    # 방법 2: trailing EPS
    try:
        trailing_eps = info.get("trailingEps")
        if trailing_eps and trailing_eps > 0 and price and price > 0:
            pe_calculated = price / trailing_eps
            if 0 < pe_calculated < 1000:
                pe_values.append(pe_calculated)
    except:
        pass

    # 유효한 PER 중 중간값 반환
    valid_pes = [pe for pe in pe_values if pe is not None and 0 < pe < 500]
    if valid_pes:
        return validate_ratio(np.median(valid_pes), min_ratio=0.1, max_ratio=500)

    return None


# ============== 데이터 수집 함수 ==============

def fetch_missing_data(ticker, price):
    """티커에 대한 누락된 데이터 수집"""
    result = {}

    for attempt in range(CONFIG["MAX_RETRIES"]):
        try:
            t = yf.Ticker(ticker, session=session)
            info = t.get_info() or {}

            if info:
                # 기본 정보
                result["Sector"] = info.get("sector")
                result["Industry"] = info.get("industry")
                result["MktCap($B)"] = round(validate_market_cap(info.get("marketCap")) / 1e9, 2) if info.get(
                    "marketCap") else None

                # 재무제표 가져오기
                q_is = _safe_df(lambda: t.quarterly_income_stmt)
                if q_is is None:
                    q_is = _safe_df(lambda: t.quarterly_financials)

                a_is = _safe_df(lambda: t.income_stmt)
                if a_is is None:
                    a_is = _safe_df(lambda: t.financials)

                cf_q = _safe_df(lambda: t.quarterly_cashflow)
                balance_a = _safe_df(lambda: t.balance_sheet)

                # RevYoY 계산
                if q_is is not None:
                    rev_row = _find_row(q_is.index, REV_ALIASES, exclude=["per share"])
                    if rev_row:
                        result["RevYoY"] = ttm_yoy_growth(q_is, rev_row)

                # OpMarginTTM 계산
                if q_is is not None:
                    rev_row = _find_row(q_is.index, REV_ALIASES, exclude=["per share"])
                    op_row = _find_row(q_is.index, OP_ALIASES)
                    if rev_row and op_row:
                        rev_ttm = ttm_sum(q_is, rev_row, 4)
                        op_ttm = ttm_sum(q_is, op_row, 4)
                        if rev_ttm and op_ttm and rev_ttm > 0:
                            margin = op_ttm / rev_ttm
                            result["OpMarginTTM"] = validate_percentage(margin, min_pct=-1.0, max_pct=1.0)

                # info 기반 지표들
                result["OperatingMargins(info)"] = validate_percentage(info.get("operatingMargins"), min_pct=-1.0,
                                                                       max_pct=1.0)
                result["ROE(info)"] = validate_percentage(info.get("returnOnEquity"), min_pct=-5.0, max_pct=5.0)
                result["ROA(info)"] = validate_percentage(info.get("returnOnAssets"), min_pct=-5.0, max_pct=5.0)

                # EV/EBITDA
                ev = info.get("enterpriseValue")
                ebitda = info.get("ebitda")
                if ev and ebitda and float(ebitda) > 0:
                    result["EV_EBITDA"] = validate_ratio(float(ev) / float(ebitda), min_ratio=-100, max_ratio=500)

                # PE 계산
                result["PE"] = calculate_pe_ratio(ticker, price, info, q_is, a_is)

                # PEG
                if result.get("PE") and result["PE"] > 0:
                    earnings_growth = info.get("earningsGrowth") or info.get("earningsQuarterlyGrowth")
                    if earnings_growth and earnings_growth > 0:
                        peg = result["PE"] / (earnings_growth * 100)
                        result["PEG"] = validate_ratio(peg, min_ratio=0, max_ratio=100)

                # FCF Yield
                if cf_q is not None:
                    fcf_row = _find_row(cf_q.index, FCF_ALIASES)
                    if fcf_row:
                        fcf_ttm = ttm_sum(cf_q, fcf_row, 4)
                        mktcap = info.get("marketCap")
                        if fcf_ttm and mktcap and float(mktcap) > 0:
                            result["FCF_Yield"] = validate_percentage(fcf_ttm / mktcap, min_pct=-1.0, max_pct=1.0)

                # 기타 비율들
                result["PB"] = validate_ratio(info.get("priceToBook"), min_ratio=0, max_ratio=100)
                result["PS"] = validate_ratio(info.get("priceToSalesTrailing12Months"), min_ratio=0, max_ratio=100)
                result["DivYield"] = validate_percentage(info.get("dividendYield"), min_pct=0, max_pct=0.5)
                result["PayoutRatio"] = validate_percentage(info.get("payoutRatio"), min_pct=0, max_pct=2.0)

                # 베타 및 소유 구조
                result["Beta"] = validate_numeric(info.get("beta"), min_val=-5, max_val=5, allow_negative=True)
                result["ShortPercent"] = validate_percentage(info.get("shortPercentOfFloat"), min_pct=0, max_pct=1.0)
                result["InsiderOwnership"] = validate_percentage(info.get("heldPercentInsiders"), min_pct=0,
                                                                 max_pct=1.0)
                result["InstitutionOwnership"] = validate_percentage(info.get("heldPercentInstitutions"), min_pct=0,
                                                                     max_pct=1.0)

                # 성공하면 반환
                return result

        except Exception as e:
            if CONFIG["VERBOSE"]:
                print(f"  ⚠️  {ticker} 시도 {attempt + 1} 실패: {str(e)}")

            if attempt < CONFIG["MAX_RETRIES"] - 1:
                time.sleep(CONFIG["RETRY_DELAY"] * (2 ** attempt))

    # 모든 시도 실패
    return result


def refill_row(args):
    """단일 행의 누락 데이터 채우기 (병렬 처리용)"""
    idx, row = args
    ticker = row["Ticker"]
    price = row["Price"]

    try:
        # 누락된 필드 확인
        missing_fields = []
        for field in CONFIG["CRITICAL_FIELDS"]:
            if pd.isna(row.get(field)) or row.get(field) == "":
                missing_fields.append(field)

        if not missing_fields:
            return idx, None, "No missing critical fields"

        # 데이터 수집
        new_data = fetch_missing_data(ticker, price)

        if new_data:
            return idx, new_data, f"Updated {len([k for k, v in new_data.items() if v is not None])} fields"
        else:
            return idx, None, "No data collected"

    except Exception as e:
        return idx, None, f"Error: {str(e)}"


# ============== 메인 처리 함수 ==============

def refill_missing_data_main():
    """메인 처리 함수"""
    print("\n" + "=" * 60)
    print("🔄 누락 데이터 재수집 시작")
    print("=" * 60)

    # 1. CSV 파일 읽기
    input_file = CONFIG["INPUT_FILE"]
    if not os.path.exists(input_file):
        print(f"❌ 파일을 찾을 수 없습니다: {input_file}")
        return

    print(f"📂 파일 로드 중: {input_file}")
    df = pd.read_csv(input_file)
    print(f"✅ 총 {len(df)}개 행 로드됨")

    # 2. 백업 생성
    if CONFIG["SAVE_BACKUP"]:
        backup_file = input_file.replace(".csv", "_backup.csv")
        df.to_csv(backup_file, index=False)
        print(f"💾 백업 저장: {backup_file}")

    # 3. 누락 데이터 통계
    print("\n📊 누락 데이터 통계:")
    critical_missing = {}
    for field in CONFIG["CRITICAL_FIELDS"]:
        if field in df.columns:
            missing_count = df[field].isna().sum()
            critical_missing[field] = missing_count
            print(f"  - {field}: {missing_count}개 ({missing_count / len(df) * 100:.1f}%)")

    # 4. 재수집 대상 선정
    needs_refill = df[df[CONFIG["CRITICAL_FIELDS"]].isna().any(axis=1)]
    print(f"\n🎯 재수집 대상: {len(needs_refill)}개 종목")

    if len(needs_refill) == 0:
        print("✅ 모든 데이터가 이미 채워져 있습니다!")
        return

    # 5. 병렬 처리로 데이터 수집
    print(f"\n⚙️  {CONFIG['WORKERS']}개 스레드로 병렬 처리 시작...")

    tasks = [(idx, row) for idx, row in needs_refill.iterrows()]
    updated_count = 0
    failed_count = 0

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=CONFIG["WORKERS"]) as executor:
        futures = {executor.submit(refill_row, task): task for task in tasks}

        for i, future in enumerate(as_completed(futures), 1):
            idx, new_data, message = future.result()

            if new_data:
                # DataFrame 업데이트
                for field, value in new_data.items():
                    if value is not None and field in df.columns:
                        df.at[idx, field] = value
                updated_count += 1
            else:
                failed_count += 1

            # 진행상황 출력
            if i % CONFIG["PROGRESS_INTERVAL"] == 0 or i == len(tasks):
                elapsed = time.time() - start_time
                rate = i / elapsed if elapsed > 0 else 0
                eta = (len(tasks) - i) / rate if rate > 0 else 0
                print(f"  📈 진행: {i}/{len(tasks)} ({i / len(tasks) * 100:.1f}%) | "
                      f"성공: {updated_count} | 실패: {failed_count} | "
                      f"속도: {rate:.1f}개/초 | 예상 남은 시간: {eta / 60:.1f}분")

    # 6. 결과 저장
    output_file = input_file.replace(".csv", f"{CONFIG['OUTPUT_SUFFIX']}.csv")
    df.to_csv(output_file, index=False)

    elapsed_total = time.time() - start_time

    print("\n" + "=" * 60)
    print("✅ 완료!")
    print("=" * 60)
    print(f"📁 출력 파일: {output_file}")
    print(f"⏱️  총 소요 시간: {elapsed_total / 60:.1f}분")
    print(f"✔️  업데이트 성공: {updated_count}개")
    print(f"❌ 실패: {failed_count}개")

    # 7. 개선 통계
    print("\n📊 개선 통계:")
    for field in CONFIG["CRITICAL_FIELDS"]:
        if field in df.columns:
            before = critical_missing[field]
            after = df[field].isna().sum()
            improved = before - after
            print(f"  - {field}: {before} → {after} (개선: {improved}개, {improved / before * 100:.1f}%)")

    print("=" * 60 + "\n")


if __name__ == "__main__":
    refill_missing_data_main()