# -*- coding: utf-8 -*-
"""
screener_from_details_cache.py

(인터넷 연결 불필요) build_details_cache.py가 만든 details_cache_{source}.csv/.xlsx
하나만으로 4개 프로파일 결과를 Excel로 출력.

권장 설치:
  pip install -U pandas numpy XlsxWriter openpyxl stats

개선사항:
1. 버핏 스타일에 더 적합한 점수 체계
2. 섹터별 차별화된 평가
3. 현실적인 필터링 조건
4. 더 다양한 재무 지표 반영
"""

import os, math, time, random
import pandas as pd, numpy as np
from datetime import datetime
from scipy import stats

class ValuationModels:
    """기관 스타일 적정가 계산 클래스"""

    @staticmethod
    def dcf_valuation(row, growth_rate=0.08, discount_rate=0.10, terminal_rate=0.02):
        """
        단순화된 DCF 모델
        """
        try:
            # 현재 EPS 계산
            current_eps = row['Price'] / row['PE'] if row['PE'] and row['PE'] > 0 else 0

            if current_eps <= 0:
                return None

            # 10년간 예측
            years = 10
            future_eps = [current_eps * ((1 + growth_rate) ** i) for i in range(1, years + 1)]

            # 현금흐름 할인
            discounted_eps = [eps / ((1 + discount_rate) ** i) for i, eps in enumerate(future_eps, 1)]

            # 터미널 가치
            terminal_eps = future_eps[-1] * (1 + terminal_rate)
            terminal_value = terminal_eps / (discount_rate - terminal_rate)
            discounted_terminal = terminal_value / ((1 + discount_rate) ** years)

            return sum(discounted_eps) + discounted_terminal

        except Exception:
            return None

    @staticmethod
    def relative_valuation(df, target_row):
        """
        동종업체 비교를 통한 적정가
        """
        try:
            sector = target_row['Sector']
            price = target_row['Price']

            # 동일 섹터 필터링
            sector_peers = df[df['Sector'] == sector]

            if len(sector_peers) < 5:
                return None

            valuations = []

            # PER 비교
            if pd.notna(target_row['PE']) and target_row['PE'] > 0:
                sector_median_pe = sector_peers['PE'].median()
                eps = price / target_row['PE']
                pe_fair_value = sector_median_pe * eps
                valuations.append(pe_fair_value)

            # PBR 비교
            if pd.notna(target_row['PB']) and target_row['PB'] > 0:
                sector_median_pb = sector_peers['PB'].median()
                bps = price / target_row['PB']
                pb_fair_value = sector_median_pb * bps
                valuations.append(pb_fair_value)

            # EV/EBITDA 비교
            if pd.notna(target_row['EV_EBITDA']) and target_row['EV_EBITDA'] > 0:
                sector_median_ev_ebitda = sector_peers['EV_EBITDA'].median()
                ev_fair_value = price * (sector_median_ev_ebitda / target_row['EV_EBITDA'])
                valuations.append(ev_fair_value)

            # P/FFO 비교 (리츠)
            if pd.notna(target_row.get('P_FFO')) and target_row.get('P_FFO', 0) > 0:
                sector_median_p_ffo = sector_peers['P_FFO'].median()
                ffo_fair_value = price * (sector_median_p_ffo / target_row['P_FFO'])
                valuations.append(ffo_fair_value)

            return sum(valuations) / len(valuations) if valuations else None

        except Exception:
            return None

    @staticmethod
    def dividend_discount_model(row, required_return=0.08):
        """
        배당할인모델
        """
        try:
            div_yield = row.get('DivYield', 0)
            if not div_yield or div_yield <= 0:
                return None

            current_dividend = row['Price'] * div_yield
            growth_rate = min(0.05, row.get('RevYoY', 0.03) * 0.5)  # 보수적 성장률

            # 고든 성장모델
            if growth_rate >= required_return:
                growth_rate = required_return - 0.01

            fair_value = current_dividend * (1 + growth_rate) / (required_return - growth_rate)
            return fair_value

        except Exception:
            return None

    @staticmethod
    def graham_number(row):
        """
        벤저민 그레이엄의 가치공식
        """
        try:
            eps = row['Price'] / row['PE'] if row['PE'] and row['PE'] > 0 else 0
            bps = row['Price'] / row['PB'] if row['PB'] and row['PB'] > 0 else 0

            if eps <= 0 or bps <= 0:
                return None

            graham_val = math.sqrt(22.5 * eps * bps)
            return graham_val

        except Exception:
            return None


def calculate_comprehensive_fair_value(df):
    """
    종합 적정가 계산
    """
    fair_value_data = []

    for idx, row in df.iterrows():
        valuations = []

        # 다양한 모델로 적정가 계산
        dcf_val = ValuationModels.dcf_valuation(row)
        if dcf_val: valuations.append(dcf_val)

        rel_val = ValuationModels.relative_valuation(df, row)
        if rel_val: valuations.append(rel_val)

        ddm_val = ValuationModels.dividend_discount_model(row)
        if ddm_val: valuations.append(ddm_val)

        graham_val = ValuationModels.graham_number(row)
        if graham_val: valuations.append(graham_val)

        # 적정가 평균 (이상치 제거)
        if valuations:
            # IQR 방식으로 이상치 제거
            q1 = np.percentile(valuations, 25)
            q3 = np.percentile(valuations, 75)
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr

            filtered_vals = [v for v in valuations if lower_bound <= v <= upper_bound]
            fair_value = np.mean(filtered_vals) if filtered_vals else np.mean(valuations)
        else:
            fair_value = None

        # 현재가 대비 할인/프리미엄률
        current_price = row['Price']
        if fair_value and current_price > 0:
            discount_pct = (fair_value - current_price) / current_price * 100
        else:
            discount_pct = None

        fair_value_data.append({
            'FairValue_DCF': dcf_val,
            'FairValue_Relative': rel_val,
            'FairValue_DDM': ddm_val,
            'FairValue_Graham': graham_val,
            'FairValue_Composite': fair_value,
            'Discount_Pct': discount_pct
        })

    return pd.DataFrame(fair_value_data, index=df.index)

FIN_SECTORS = {"banks", "financial", "insurance", "capital markets"}
REIT_SECTORS = {"reit", "real estate", "property"}
CYCLICAL_SECTORS = {"energy", "materials", "industrials", "consumer cyclical"}
DEFENSIVE_SECTORS = {"utilities", "consumer defensive", "healthcare"}

# 통합 CONFIG 설정
CONFIG = {
    "DETAILS_CACHE_FILE": "details_cache_us_all_refiltered.csv",
    "RUN_PROFILES": ["buffett_lite", "buffett_strict", "swing", "daytrade"],

    # 버핏 공통 필터
    "MIN_MKTCAP": 50_000_000,  # 5천만 달러
    "MIN_PRICE": 1.0,
    "MIN_DOLLAR_VOLUME": 1_000_000,
    "MAX_EV_EBITDA_HARD": 25.0,
    "HARD_PEG_MAX": 2.0,
    "HARD_PE_MAX": 30.0,
    "MIN_REV_TTM_YOY_HF": -0.15,
    "MIN_OP_MARGIN_HF": 0.05,
    "MAX_DEBT_EQUITY": 2.0,
    "MIN_ROE_HF": 0.08,
    "MIN_FCFY_HF": 0.02,

    # 추가된 설정들
    "OP_MARGIN_EXEMPT_SECTORS": FIN_SECTORS,  # 영업이익률 필터에서 제외할 섹터
    "MIN_DISCOUNT_PCT": 0.0,  # 최소 할인율
    "MAX_DISCOUNT_PCT": 50.0,  # 최대 할인율

    # 트레이딩 필터
    "SWING_FILTERS": {
        "MIN_PRICE": 5.0,
        "MIN_DOLLAR_VOLUME": 5_000_000,
        "MIN_RVOL": 1.2,
        "ATR_PCT_RANGE": [0.02, 0.12],
        "TREND_RULE": "close>sma20>sma50",
        "MIN_RET20": 0.00
    },
    "DAY_FILTERS": {
        "MIN_PRICE": 5.0,
        "MIN_DOLLAR_VOLUME": 20_000_000,
        "MIN_RVOL": 2.0,
        "ATR_PCT_RANGE": [0.03, 0.20],
        "TREND_RULE": "any",
        "MIN_RET5": 0.03
    },

    # 점수 가중치
    "W_GROWTH": 0.15,
    "W_QUALITY": 0.35,
    "W_VALUE": 0.40,
    "W_CATALYST": 0.10,

    "OUT_PREFIX": "IMPROVED_SCREENER",
}

def enhanced_valuation_screener():
    """
    적정가 계산이 통합된 개선된 스크리너
    """
    # 데이터 로드
    df = load_cache(CONFIG["DETAILS_CACHE_FILE"])

    # 적정가 계산
    print("Calculating fair values...")
    fair_values_df = calculate_comprehensive_fair_value(df)
    df = pd.concat([df, fair_values_df], axis=1)

    # 버핏형 필터링 + 적정가 필터
    def enhanced_buffett_filter(row, cfg):
        # 기존 버핏 필터
        if not enhanced_pass_buffett_base(row, cfg):
            return False

        # 적정가 필터 추가
        discount_pct = row.get('Discount_Pct')
        if discount_pct is None:
            return False

        # 적정가 대비 적절한 할인율인지 확인
        if not (cfg["MIN_DISCOUNT_PCT"] <= discount_pct <= cfg["MAX_DISCOUNT_PCT"]):
            return False

        return True

    results = {}

    # 버핏-Lite (적정가 필터 포함)
    mask_lite = df.apply(lambda r: enhanced_buffett_filter(r, CONFIG), axis=1)
    raw_lite = df[mask_lite].copy()

    if not raw_lite.empty:
        scored_lite = build_scores_buffett(raw_lite, CONFIG)
        # 적정가 정보를 점수에 반영
        scored_lite['ValuationAdjustedScore'] = scored_lite['TotalScore'] * (
                1 + scored_lite['Discount_Pct'].fillna(0) / 100
        )
        results["buffett_lite"] = scored_lite.sort_values("ValuationAdjustedScore", ascending=False)

    # 버핏-Strict (더 엄격한 적정가 필터)
    strict_cfg = CONFIG.copy()
    strict_cfg.update({
        "MIN_DISCOUNT_PCT": 15.0,
        "MAX_DISCOUNT_PCT": 40.0,
        "MIN_MKTCAP": 2_000_000_000,
        "MIN_OP_MARGIN_HF": 0.12,
    })

    mask_strict = df.apply(lambda r: enhanced_buffett_filter(r, strict_cfg), axis=1)
    raw_strict = df[mask_strict].copy()

    if not raw_strict.empty:
        scored_strict = build_scores_buffett(raw_strict, strict_cfg)
        scored_strict['ValuationAdjustedScore'] = scored_strict['TotalScore'] * (
                1 + scored_strict['Discount_Pct'].fillna(0) / 100
        )
        results["buffett_strict"] = scored_strict.sort_values("ValuationAdjustedScore", ascending=False)

    # 트레이딩 프로파일 추가 (swing, daytrade)
    for prof in ("swing", "daytrade"):
        mask_tr = df.apply(lambda r: pass_trading(r, prof, CONFIG), axis=1)
        base = df[mask_tr].copy()
        if not base.empty:
            scored = build_scores_trading(base, profile=prof, cfg=CONFIG)
            # 트레이딩 결과에 필요한 컬럼 선택
            trading_cols = [
                "Ticker", "Name", "Sector", "Price", "DollarVol($M)", "RVOL",
                "ATR_PCT", "SMA20", "SMA50", "RET5", "RET20",
                "MomentumScore", "TrendScore", "LiquidityScore", "VolatilityScore", "TotalScore"
            ]
            trading_cols = [c for c in trading_cols if c in scored.columns]
            results[prof] = scored[trading_cols].sort_values("TotalScore", ascending=False)

    # 결과 저장
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_name = f"COMPREHENSIVE_SCREENER_{ts}.xlsx"

    with pd.ExcelWriter(out_name, engine='openpyxl') as writer:
        # 버핏 프로파일 시트
        for profile in ["buffett_lite", "buffett_strict"]:
            if profile in results and not results[profile].empty:
                # 주요 컬럼 선택
                cols = [
                    'Ticker', 'Name', 'Sector', 'Price', 'FairValue_Composite', 'Discount_Pct',
                    'MktCap($B)', 'PE', 'PEG', 'EV_EBITDA', 'FCF_Yield', 'ROE(info)',
                    'GrowthScore', 'QualityScore', 'ValueScore', 'TotalScore', 'ValuationAdjustedScore'
                ]
                cols = [c for c in cols if c in results[profile].columns]
                results[profile][cols].to_excel(writer, sheet_name=profile[:31], index=False)

        # 트레이딩 프로파일 시트
        for profile in ["swing", "daytrade"]:
            if profile in results and not results[profile].empty:
                results[profile].to_excel(writer, sheet_name=profile[:31], index=False)

        # 통합 요약 시트
        summary_data = []
        for profile in ["buffett_lite", "buffett_strict", "swing", "daytrade"]:
            if profile in results and not results[profile].empty:
                if profile.startswith('buffett'):
                    avg_discount = results[profile]['Discount_Pct'].mean()
                    median_pe = results[profile]['PE'].median()
                    summary_data.append({
                        'Profile': profile,
                        'Stocks_Count': len(results[profile]),
                        'Avg_Discount_Pct': avg_discount,
                        'Median_PE': median_pe,
                        'Top_Tickers': ', '.join(results[profile].head(5)['Ticker'].tolist())
                    })
                else:
                    # 트레이딩 프로파일 요약
                    avg_rvol = results[profile]['RVOL'].mean()
                    avg_atr = results[profile]['ATR_PCT'].mean()
                    summary_data.append({
                        'Profile': profile,
                        'Stocks_Count': len(results[profile]),
                        'Avg_RVOL': avg_rvol,
                        'Avg_ATR_PCT': avg_atr,
                        'Top_Tickers': ', '.join(results[profile].head(5)['Ticker'].tolist())
                    })

        pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)

        # 상위 종목 통합 시트
        top_stocks = []
        for profile in ["buffett_strict", "buffett_lite", "swing", "daytrade"]:
            if profile in results and not results[profile].empty:
                top_df = results[profile].head(10).copy()
                top_df['Profile'] = profile
                top_stocks.append(top_df)

        if top_stocks:
            pd.concat(top_stocks, ignore_index=True).to_excel(writer, sheet_name='Top_Stocks_All', index=False)

    print(f"[COMPREHENSIVE SCREENER] Results saved to: {out_name}")
    print(f" - 포함된 프로파일: {list(results.keys())}")
    print(f" - 총 종목 수:")
    for profile, result_df in results.items():
        print(f"   * {profile}: {len(result_df)}개")

    return results

def _winsor_series(s: pd.Series, p=0.02):
    s = s.astype(float)
    lo, hi = s.quantile(p), s.quantile(1-p)
    return s.clip(lower=lo, upper=hi)

def _percentile_rank(s: pd.Series, higher=True):
    s = s.astype(float)
    if not higher:
        s = -s
    return s.rank(pct=True, method="average")

def _clip01(x):
    try:
        return max(0.0, min(1.0, float(x)))
    except Exception:
        return np.nan


def check_data_quality_before_screening(df):
    """스크리너 실행 전 데이터 품질 확인"""
    print("=== 데이터 품질 확인 ===")

    essential_columns = {
        '버핏 분석': ['Price', 'MktCap($B)', 'RevYoY', 'OpMarginTTM', 'ROE(info)', 'PE', 'EV_EBITDA'],
        '트레이딩 분석': ['SMA20', 'SMA50', 'ATR_PCT', 'RVOL', 'RET5', 'RET20']
    }

    for category, columns in essential_columns.items():
        print(f"\n{category}:")
        for col in columns:
            if col in df.columns:
                non_null = df[col].notna().sum()
                pct = (non_null / len(df)) * 100
                print(f"  {col}: {non_null}/{len(df)} ({pct:.1f}%)")
            else:
                print(f"  {col}: ❌ 컬럼 없음")

    # NULL 비율이 높은 컬럼 식별
    low_quality_cols = []
    for col in df.columns:
        if df[col].notna().sum() / len(df) < 0.3:  # 30% 미만 데이터
            low_quality_cols.append(col)

    if low_quality_cols:
        print(f"\n⚠️ 주의: 데이터가 부족한 컬럼들: {low_quality_cols}")

def build_scores_buffett(df: pd.DataFrame, cfg=CONFIG):
    """개선된 버핏 스타일 점수 계산"""
    temp = df.copy()

    # 누락될 수 있는 컬럼들에 대한 안전장치
    if "ROE_5Y_Avg" not in temp.columns:
        temp["ROE_5Y_Avg"] = temp["ROE(info)"]  # 기본값으로 ROE(info) 사용

    if "Debt_to_Equity" not in temp.columns:
        temp["Debt_to_Equity"] = None  # 또는 적절한 기본값

    if "BuybackYield" not in temp.columns:
        temp["BuybackYield"] = None

    if "P_FFO" not in temp.columns:
        temp["P_FFO"] = None

    # 데이터 전처리
    temp["_OpMarginUse"] = temp[["OpMarginTTM", "OperatingMargins(info)"]].max(axis=1, numeric_only=True)

    # Winsorize로 이상치 처리
    for col in ["RevYoY", "_OpMarginUse", "ROE(info)", "ROE_5Y_Avg", "FCF_Yield",
                "EV_EBITDA", "PE", "PEG", "PB", "DivYield", "Debt_to_Equity"]:
        if col in temp.columns:
            temp[col] = _winsor_series(temp[col].astype(float), p=0.02)

    # 섹터 정보 준비
    sectors = temp["Sector"].fillna("").str.lower()

    growth_s = [];
    qual_s = [];
    val_s = [];
    cat_s = []

    for i, row in temp.iterrows():
        sec = str(row.get("Sector") or "").lower()

        # 성장 점수: 매출성장 + EPS 성장 기대
        rev_growth = row.get("RevYoY") or 0
        # PEG가 낮을수록 성장성 좋음 (역수 사용)
        peg_score = 1.0 / (row.get("PEG") or 2.0) if row.get("PEG") and row.get("PEG") > 0 else 0
        growth_score = np.nanmean([rev_growth, peg_score])
        growth_s.append(growth_score)

        # 질 점수: 수익성 + 재무건전성
        quality_components = []

        # 수익성 지표
        op_margin = row.get("_OpMarginUse")
        roe = row.get("ROE(info)") or row.get("ROE_5Y_Avg")
        if op_margin: quality_components.append(op_margin)
        if roe: quality_components.append(roe)

        # 재무건전성 지표
        debt_equity = row.get("Debt_to_Equity")
        if debt_equity is not None:
            # 부채비율이 낮을수록 점수 높음
            debt_score = max(0, 1.0 - (debt_equity / cfg["MAX_DEBT_EQUITY"]))
            quality_components.append(debt_score)

        # FCF Yield (현금창출능력)
        fcf_yield = row.get("FCF_Yield")
        if fcf_yield and fcf_yield > 0:
            quality_components.append(fcf_yield)

        qual_s.append(np.nanmean(quality_components) if quality_components else 0.5)

        # 가치 점수: 섹터별 차별화
        if any(x in sec for x in FIN_SECTORS):
            # 금융주: P/B, ROE, Div Yield
            val_components = [
                _percentile_rank(temp["PB"], False)[i],
                _percentile_rank(temp["ROE(info)"], True)[i] if "ROE(info)" in temp else 0.5,
                _percentile_rank(temp["DivYield"], True)[i] if "DivYield" in temp else 0.5
            ]
        elif any(x in sec for x in REIT_SECTORS):
            # 리츠: P/FFO, Div Yield
            val_components = [
                _percentile_rank(temp["P_FFO"], False)[i] if "P_FFO" in temp else 0.5,
                _percentile_rank(temp["DivYield"], True)[i] if "DivYield" in temp else 0.5
            ]
        else:
            # 일반 주식: 다양한 가치 지표
            val_components = []
            for col, higher in [("FCF_Yield", True), ("EV_EBITDA", False),
                                ("PE", False), ("PEG", False), ("PB", False)]:
                if col in temp.columns:
                    val_components.append(_percentile_rank(temp[col], higher)[i])

        val_s.append(np.nanmean(val_components) if val_components else 0.5)

        # 촉매 점수: 배당, 자사주 매입
        catalyst_components = []
        if "DivYield" in temp and pd.notna(row.get("DivYield")):
            catalyst_components.append(_percentile_rank(temp["DivYield"], True)[i])

        if "BuybackYield" in temp and pd.notna(row.get("BuybackYield")):
            catalyst_components.append(_percentile_rank(temp["BuybackYield"], True)[i])

        # 거래량/변동성 (주목도 지표)
        if "RVOL" in temp and pd.notna(row.get("RVOL")):
            rvol_score = min(1.0, (row.get("RVOL") or 1) / 3.0)  # RVOL 3이면 만점
            catalyst_components.append(rvol_score)

        cat_s.append(np.nanmean(catalyst_components) if catalyst_components else 0.5)

    # 점수 정규화
    temp["GrowthScore"] = pd.Series(growth_s, index=temp.index).rank(pct=True).fillna(0.5)
    temp["QualityScore"] = pd.Series(qual_s, index=temp.index).rank(pct=True).fillna(0.5)
    temp["ValueScore"] = pd.Series(val_s, index=temp.index).fillna(0.5)
    temp["CatalystScore"] = pd.Series(cat_s, index=temp.index).fillna(0.5)

    # 총점 계산
    temp["TotalScore"] = 100 * (
            cfg["W_GROWTH"] * temp["GrowthScore"] +
            cfg["W_QUALITY"] * temp["QualityScore"] +
            cfg["W_VALUE"] * temp["ValueScore"] +
            cfg["W_CATALYST"] * temp["CatalystScore"]
    )

    return temp


def enhanced_pass_buffett_base(row, cfg=CONFIG):
    """개선된 버핏 스타일 필터링"""
    # 기본 유동성 필터
    price = row.get("Price")
    dv = (row.get("DollarVol($M)") or 0) * 1_000_000
    if pd.isna(price) or pd.isna(dv):
        return False
    if price < cfg["MIN_PRICE"] or dv < cfg["MIN_DOLLAR_VOLUME"]:
        return False

    # 시가총액 필터
    mktcap = (row.get("MktCap($B)") or 0) * 1_000_000_000
    if mktcap and mktcap < cfg["MIN_MKTCAP"]:
        return False

    # 성장성 필터
    rev_yoy = row.get("RevYoY")
    if (rev_yoy is None) or (rev_yoy < cfg["MIN_REV_TTM_YOY_HF"]):
        return False

    # 수익성 필터 (섹터별 면제)
    sec = str(row.get("Sector") or "").lower()
    op_margin = row.get("OpMarginTTM") or row.get("OperatingMargins(info)")
    if sec not in cfg["OP_MARGIN_EXEMPT_SECTORS"]:
        if (op_margin is None) or (op_margin < cfg["MIN_OP_MARGIN_HF"]):
            return False

    # 재무건전성 필터
    debt_equity = row.get("Debt_to_Equity")
    if debt_equity and debt_equity > cfg["MAX_DEBT_EQUITY"]:
        return False

    # 수익성 필터 (ROE)
    if cfg["MIN_ROE_HF"] is not None:
        roe = row.get("ROE(info)") or row.get("ROE_5Y_Avg")
        if roe is None or roe < cfg["MIN_ROE_HF"]:
            return False

    # 가치 필터
    if cfg["HARD_PEG_MAX"] is not None:
        peg = row.get("PEG")
        if peg is not None and peg > cfg["HARD_PEG_MAX"]:
            return False

    if cfg["HARD_PE_MAX"] is not None:
        pe = row.get("PE")
        if pe is not None and pe > cfg["HARD_PE_MAX"]:
            return False

    if cfg["MAX_EV_EBITDA_HARD"] is not None:
        ev_eb = row.get("EV_EBITDA")
        if ev_eb is not None and ev_eb > cfg["MAX_EV_EBITDA_HARD"]:
            return False

    # 현금창출능력 필터
    if cfg["MIN_FCFY_HF"] is not None:
        fcfy = row.get("FCF_Yield")
        if fcfy is None or fcfy < cfg["MIN_FCFY_HF"]:
            return False

    return True

def run_enhanced_screener():
    """개선된 스크리너 실행"""
    df = load_cache(CONFIG["DETAILS_CACHE_FILE"])
    results = {}

    # 버핏-Lite (관대한 조건)
    mask_lite = df.apply(lambda r: enhanced_pass_buffett_base(r, CONFIG), axis=1)
    raw_lite = df[mask_lite].copy()

    if not raw_lite.empty:
        scored_lite = build_scores_buffett(raw_lite, CONFIG)
        cols = ["Ticker", "Name", "Sector", "Industry", "MktCap($B)", "Price", "DollarVol($M)",
                "RevYoY", "OpMarginTTM", "ROE(info)", "ROE_5Y_Avg", "Debt_to_Equity",
                "EV_EBITDA", "PE", "PEG", "FCF_Yield", "PB", "DivYield", "P_FFO",
                "GrowthScore", "QualityScore", "ValueScore", "CatalystScore", "TotalScore"]
        cols = [c for c in cols if c in scored_lite.columns]
        results["buffett_lite"] = scored_lite[cols].sort_values("TotalScore", ascending=False).reset_index(drop=True)

    # 버핏-Strict (엄격한 조건)
    strict_cfg = CONFIG.copy()
    strict_cfg.update({
        "MIN_MKTCAP": 2_000_000_000,
        "MIN_OP_MARGIN_HF": 0.10,
        "MIN_REV_TTM_YOY_HF": 0.00,
        "MAX_EV_EBITDA_HARD": 15.0,
        "HARD_PEG_MAX": 1.5,
        "HARD_PE_MAX": 20.0,
        "MIN_ROE_HF": 0.12,
        "MIN_FCFY_HF": 0.03,
        "MAX_DEBT_EQUITY": 1.5,
    })

    mask_strict = df.apply(lambda r: enhanced_pass_buffett_base(r, strict_cfg), axis=1)
    raw_strict = df[mask_strict].copy()

    if not raw_strict.empty:
        scored_strict = build_scores_buffett(raw_strict, strict_cfg)
        results["buffett_strict"] = scored_strict[cols].sort_values("TotalScore", ascending=False).reset_index(
            drop=True)

    # 트레이딩 프로파일 (기존 유지)
    for prof in ("swing", "daytrade"):
        mask_tr = df.apply(lambda r: pass_trading(r, prof, CONFIG), axis=1)
        base = df[mask_tr].copy()
        if not base.empty:
            scored = build_scores_trading(base, profile=prof, cfg=CONFIG)
            trading_cols = ["Ticker", "Price", "DollarVol($M)", "RVOL", "ATR_PCT", "SMA20", "SMA50", "RET5", "RET20",
                            "MomentumScore", "TrendScore", "LiquidityScore", "VolatilityScore", "TotalScore"]
            trading_cols = [c for c in trading_cols if c in scored.columns]
            results[prof] = scored[trading_cols].sort_values("TotalScore", ascending=False).reset_index(drop=True)

    # 결과 저장
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_name = f"{CONFIG['OUT_PREFIX']}_{ts}.xlsx"

    with pd.ExcelWriter(out_name, engine="openpyxl") as writer:
        for prof, df_out in results.items():
            sheet_name = prof[:31]
            if df_out is None or df_out.empty:
                pd.DataFrame({"Status": [f"No results for {prof}"]}).to_excel(writer, sheet_name=sheet_name,
                                                                              index=False)
            else:
                df_out.to_excel(writer, sheet_name=sheet_name, index=False)

        # 요약 시트
        summary_data = []
        for prof in CONFIG["RUN_PROFILES"]:
            count = len(results.get(prof, pd.DataFrame()))
            summary_data.append({"Profile": prof, "Stocks Found": count})

        pd.DataFrame(summary_data).to_excel(writer, sheet_name="Summary", index=False)

        # 상위 종목 통합 시트
        top_stocks = []
        for prof in ["buffett_strict", "buffett_lite","swing", "daytrade"]:
            if prof in results and not results[prof].empty:
                top_df = results[prof].head(20).copy()
                top_df["Profile"] = prof
                top_stocks.append(top_df)

        if top_stocks:
            pd.concat(top_stocks, ignore_index=True).to_excel(writer, sheet_name="Top Stocks", index=False)

    print(f"[ENHANCED SCREENER] Results saved to: {out_name}")
    return results

def build_scores_trading(df: pd.DataFrame, profile, cfg=CONFIG):
    temp=df.copy()
    for col in ["RET5","RET20"]:
        if col in temp.columns: temp[col]=_winsor_series(temp[col].astype(float).fillna(0), p=0.02)
        else: temp[col]=0.0
    mom=np.nanmean([_percentile_rank(temp["RET5"], True),
                    _percentile_rank(temp["RET20"], True)], axis=0)
    temp["MomentumScore"]=pd.Series(mom, index=temp.index).fillna(0.5)

    dl=_percentile_rank(temp["DollarVol($M)"], True) if "DollarVol($M)" in temp.columns else pd.Series(0.5, index=temp.index)
    rv=_percentile_rank(temp["RVOL"].fillna(1.0), True) if "RVOL" in temp.columns else pd.Series(0.5, index=temp.index)
    temp["LiquidityScore"]=np.nanmean([dl,rv], axis=0)

    close=temp["Price"]; s20=temp["SMA20"]; s50=temp["SMA50"]
    trend=[]
    for i in temp.index:
        c,sma20,sma50=close[i], s20[i], s50[i]
        score=0.5
        try:
            if (c is not None) and (sma20 is not None) and (sma50 is not None):
                if c>sma20>sma50: score=1.0
                elif c>sma20: score=0.75
                elif sma20 and sma50 and sma20>sma50: score=0.65
                else: score=0.25
        except Exception: score=0.5
        trend.append(score)
    temp["TrendScore"]=pd.Series([_clip01(x) for x in trend], index=temp.index)

    flt = cfg["SWING_FILTERS"] if profile=="swing" else cfg["DAY_FILTERS"]
    lo,hi = flt["ATR_PCT_RANGE"]; target=(lo+hi)/2.0; sigma=(hi-lo)/2.0
    vols=[]
    for v in temp["ATR_PCT"].fillna(target):
        try: s=math.exp(-((float(v)-target)**2)/(2*(sigma**2)))
        except Exception: s=0.5
        vols.append(s)
    temp["VolatilityScore"]=pd.Series([_clip01(x) for x in vols], index=temp.index)

    weights = {"swing":{"momentum":0.45,"trend":0.25,"liquidity":0.20,"volatility":0.10},
               "daytrade":{"momentum":0.30,"trend":0.10,"liquidity":0.40,"volatility":0.20}}[profile]
    temp["TotalScore"]=100*(weights["momentum"]*temp["MomentumScore"]
                           +weights["trend"]*temp["TrendScore"]
                           +weights["liquidity"]*temp["LiquidityScore"]
                           +weights["volatility"]*temp["VolatilityScore"])
    return temp

def load_cache(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Details cache not found: {path}")
    df=pd.read_csv(path)
    # 타입 보정
    num_cols=["Price","DollarVol($M)","SMA20","SMA50","ATR_PCT","RVOL","RET5","RET20",
              "MktCap($B)","RevYoY","OpMarginTTM","OperatingMargins(info)","ROE(info)","EV_EBITDA",
              "PE","PEG","FCF_Yield","PB","DivYield","P_FFO","BuybackYield"]
    for c in num_cols:
        if c in df.columns: df[c]=pd.to_numeric(df[c], errors="coerce")
    return df

def pass_buffett_base(row, cfg=CONFIG):
    # 기본 컷
    price=row.get("Price"); dv=(row.get("DollarVol($M)") or 0)*1_000_000
    if pd.isna(price) or pd.isna(dv): return False
    if price < cfg["MIN_PRICE"] or dv < cfg["MIN_DOLLAR_VOLUME"]: return False
    mktcap=(row.get("MktCap($B)") or 0)*1_000_000_000
    if mktcap and mktcap < cfg["MIN_MKTCAP"]: return False
    rev_yoy=row.get("RevYoY"); opm=row.get("OpMarginTTM")
    if (rev_yoy is None) or (rev_yoy < cfg["MIN_REV_TTM_YOY_HF"]): return False
    sec=str(row.get("Sector") or "").lower()
    if sec not in cfg["OP_MARGIN_EXEMPT_SECTORS"]:
        if (opm is None) or (opm < cfg["MIN_OP_MARGIN_HF"]): return False
    # 추가 하드컷(있으면 적용)
    if cfg["HARD_PEG_MAX"] is not None:
        peg=row.get("PEG")
        if peg is not None and peg > cfg["HARD_PEG_MAX"]: return False
    if cfg["HARD_PE_MAX"] is not None:
        pe=row.get("PE")
        if pe is not None and pe > cfg["HARD_PE_MAX"]: return False
    if cfg["MAX_EV_EBITDA_HARD"] is not None:
        ev_eb=row.get("EV_EBITDA")
        if ev_eb is not None and ev_eb > cfg["MAX_EV_EBITDA_HARD"]: return False
    if cfg["MIN_ROE_HF"] is not None:
        roe=row.get("ROE(info)")
        if roe is None or roe < cfg["MIN_ROE_HF"]: return False
    if cfg["MIN_FCFY_HF"] is not None:
        fcfy=row.get("FCF_Yield")
        if fcfy is None or fcfy < cfg["MIN_FCFY_HF"]: return False
    return True

def pass_trading(row, profile, cfg=CONFIG):
    f = cfg["SWING_FILTERS"] if profile=="swing" else cfg["DAY_FILTERS"]
    price=row.get("Price"); dv=(row.get("DollarVol($M)") or 0)*1_000_000
    rvol=row.get("RVOL"); atr=row.get("ATR_PCT")
    if price is None or dv is None: return False
    if price < f["MIN_PRICE"] or dv < f["MIN_DOLLAR_VOLUME"]: return False
    if (rvol is None) or (rvol < f["MIN_RVOL"]): return False
    lo,hi = f["ATR_PCT_RANGE"]
    if (atr is None) or (atr < lo) or (atr > hi): return False
    rule=f.get("TREND_RULE","any").lower()
    sma20=row.get("SMA20"); sma50=row.get("SMA50")
    if rule=="close>sma20>sma50":
        if not (price and sma20 and sma50 and (price>sma20>sma50)): return False
    elif rule=="sma20>50":
        if not (sma20 and sma50 and sma20>sma50): return False
    if profile=="swing":
        ret20=row.get("RET20")
        if ret20 is not None and ret20 < f["MIN_RET20"]: return False
    if profile=="daytrade":
        ret5=row.get("RET5")
        if ret5 is not None and ret5 < f["MIN_RET5"]: return False
    return True

def run_from_cache():
    df=load_cache(CONFIG["DETAILS_CACHE_FILE"])
    results={}
    rejects_count={}

    # 버핏-Lite
    mask = df.apply(lambda r: pass_buffett_base(r, CONFIG), axis=1)
    raw = df[mask].copy()
    if not raw.empty:
        scored=build_scores_buffett(raw, CONFIG)
        cols=["Ticker","Name","Sector","MktCap($B)","Price","DollarVol($M)","RevYoY","OpMarginTTM","OperatingMargins(info)","ROE(info)",
              "EV_EBITDA","PE","PEG","FCF_Yield","BuybackYield","PB","DivYield","P_FFO",
              "GrowthScore","QualityScore","ValueScore","CatalystScore","TotalScore"]
        cols=[c for c in cols if c in scored.columns]
        results["buffett_lite"]=scored[cols].sort_values("TotalScore", ascending=False).reset_index(drop=True)
    else:
        results["buffett_lite"]=pd.DataFrame()

    # 버핏-Strict (하드컷 강화)
    strict_cfg=CONFIG.copy()
    strict_cfg.update({
        "MIN_MKTCAP": max(CONFIG["MIN_MKTCAP"], 2_000_000_000),
        "MIN_OP_MARGIN_HF": max(CONFIG["MIN_OP_MARGIN_HF"], 0.10),
        "MIN_REV_TTM_YOY_HF": max(CONFIG["MIN_REV_TTM_YOY_HF"], 0.00),
        "MAX_EV_EBITDA_HARD": 18.0,
        "HARD_PEG_MAX": 1.8,
        "HARD_PE_MAX": 25.0,
        "MIN_ROE_HF": 0.10,
        "MIN_FCFY_HF": 0.02,
    })
    mask2 = df.apply(lambda r: pass_buffett_base(r, strict_cfg), axis=1)
    raw2 = df[mask2].copy()
    if not raw2.empty:
        scored2=build_scores_buffett(raw2, strict_cfg)
        cols2=["Ticker","Name","Sector","MktCap($B)","Price","DollarVol($M)","RevYoY","OpMarginTTM","OperatingMargins(info)","ROE(info)",
               "EV_EBITDA","PE","PEG","FCF_Yield","BuybackYield","PB","DivYield","P_FFO",
               "GrowthScore","QualityScore","ValueScore","CatalystScore","TotalScore"]
        cols2=[c for c in cols2 if c in scored2.columns]
        results["buffett_strict"]=scored2[cols2].sort_values("TotalScore", ascending=False).reset_index(drop=True)
    else:
        results["buffett_strict"]=pd.DataFrame()

    # Trading 프로파일들
    for prof in ("swing","daytrade"):
        mask_tr = df.apply(lambda r: pass_trading(r, prof, CONFIG), axis=1)
        base = df[mask_tr].copy()
        if not base.empty:
            scored=build_scores_trading(base, profile=prof, cfg=CONFIG)
            cols=["Ticker","Price","DollarVol($M)","RVOL","ATR_PCT","SMA20","SMA50","RET5","RET20",
                  "MomentumScore","TrendScore","LiquidityScore","VolatilityScore","TotalScore"]
            cols=[c for c in cols if c in scored.columns]
            results[prof]=scored[cols].sort_values("TotalScore", ascending=False).reset_index(drop=True)
        else:
            results[prof]=pd.DataFrame()

    # 엑셀 저장
    ts=datetime.now().strftime("%Y%m%d_%H%M%S")
    out_name=f"{CONFIG['OUT_PREFIX']}_{ts}.xlsx"
    engine=None
    try:
        import xlsxwriter  # noqa
        engine="xlsxwriter"
    except Exception:
        try:
            import openpyxl  # noqa
            engine="openpyxl"
        except Exception:
            engine=None
    if engine is None:
        raise RuntimeError("엑셀 저장을 위해 XlsxWriter 또는 openpyxl이 필요합니다.")

    with pd.ExcelWriter(out_name, engine=engine) as w:
        for prof,dfout in results.items():
            if dfout is None or dfout.empty:
                pd.DataFrame(columns=["No results"]).to_excel(w, sheet_name=prof[:31], index=False)
            else:
                dfout.to_excel(w, sheet_name=prof[:31], index=False)
        # summary
        summary=[]
        for prof in CONFIG["RUN_PROFILES"]:
            summary.append({"profile":prof, "num_pass": len(results.get(prof, pd.DataFrame()))})
        pd.DataFrame(summary).to_excel(w, sheet_name="summary", index=False)

    print(f"[DONE] saved {out_name}")

if __name__ == "__main__":
    df = load_cache(CONFIG["DETAILS_CACHE_FILE"])
    check_data_quality_before_screening(df)
    enhanced_valuation_screener()
    # run_enhanced_screener()
