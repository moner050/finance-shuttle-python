# -*- coding: utf-8 -*-
"""
screener_from_details_cache.py

(인터넷 연결 불필요) build_details_cache.py가 만든 details_cache_{source}.csv/.xlsx
하나만으로 4개 프로파일 결과를 Excel로 출력.

권장 설치:
  pip install -U pandas numpy XlsxWriter openpyxl scipy

개선사항:
1. 버핏 스타일에 더 적합한 점수 체계
2. 섹터별 차별화된 평가
3. 현실적인 필터링 조건
4. 더 다양한 재무 지표 반영
"""

import os, math, time, random, warnings
import pandas as pd, numpy as np
from datetime import datetime

warnings.filterwarnings("ignore", category=RuntimeWarning)

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

# 파일 상단에 섹터 상수 정의 (CONFIG보다 먼저 위치해야 함)
FIN_SECTORS = {"banks", "financial", "insurance", "capital markets"}
REIT_SECTORS = {"reit", "real estate", "property"}
CYCLICAL_SECTORS = {"energy", "materials", "industrials", "consumer cyclical"}
DEFENSIVE_SECTORS = {"utilities", "consumer defensive", "healthcare"}

# 통합 CONFIG 설정 (현대적 버핏 철학 반영 + 엄격한 기준)
CONFIG = {
    "DETAILS_CACHE_FILE": "details_cache_us_all_20251025_194009.csv",
    "RUN_PROFILES": ["buffett_lite", "buffett_strict", "modern_buffett", "swing", "daytrade"],

    # 데이터가 있는 지표들만 필수 조건으로 사용
    "MIN_MKTCAP": 1_000_000_000,
    "MIN_PRICE": 10.0,
    "MIN_DOLLAR_VOLUME": 10_000_000,
    "HARD_PE_MAX": 20.0,
    "MIN_REV_TTM_YOY_HF": 0.03,
    "MIN_OP_MARGIN_HF": 0.10,
    "MAX_DEBT_EQUITY": 1.0,
    "MIN_ROE_HF": 0.12,

    # 데이터 부족으로 제외된 지표들
    "HARD_PEG_MAX": None,
    "MAX_EV_EBITDA_HARD": None,
    "MIN_FCFY_HF": None,
    "MIN_DIV_YIELD": None,

    # 추가 필터 설정
    "OP_MARGIN_EXEMPT_SECTORS": FIN_SECTORS,
    "MIN_DISCOUNT_PCT": 10.0,
    "MAX_DISCOUNT_PCT": 40.0,

    # 현대적 버핏 필터
    "MODERN_BUFFETT": {
        "MIN_MKTCAP": 10_000_000_000,  # 100억 달러로 완화 (500억은 너무 엄격)
        "MIN_PRICE": 15.0,
        "MIN_DOLLAR_VOLUME": 20_000_000,
        "MIN_OP_MARGIN_HF": 0.15,
        "MIN_REV_TTM_YOY_HF": 0.08,
        "MAX_DEBT_EQUITY": 0.8,
        "MIN_ROE_HF": 0.18,
        "HARD_PE_MAX": 18.0,
        "MIN_DISCOUNT_PCT": 15.0,
        "MAX_DISCOUNT_PCT": 35.0,
        "MIN_MOAT_SCORE": 0.7,
        "OP_MARGIN_EXEMPT_SECTORS": FIN_SECTORS,
        "PREFERRED_SECTORS": {
            "technology", "consumer defensive", "financial services",
            "energy", "healthcare", "utilities"
        },
        "W_GROWTH": 0.20,
        "W_QUALITY": 0.45,
        "W_VALUE": 0.30,
        "W_CATALYST": 0.05
    },

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

    # 기본 점수 가중치
    "W_GROWTH": 0.15,
    "W_QUALITY": 0.35,
    "W_VALUE": 0.40,
    "W_CATALYST": 0.10,

    "OUT_PREFIX": "ELITE_SCREENER",
}

# 현대적 버핏 필터링 함수들
def enhanced_buffett_modern_filter(row, cfg):
    """워렌 버핏 최근 철학 반영 필터"""
    modern_cfg = cfg["MODERN_BUFFETT"]
    combined_cfg = {**cfg, **modern_cfg}

    if not enhanced_pass_buffett_base(row, combined_cfg):
        return False

    # 현대적 버핏 추가 기준
    if not has_economic_moat(row, modern_cfg):
        return False

    if not has_stable_cashflow(row, modern_cfg):
        return False

    if not passes_modern_financial_health(row, modern_cfg):
        return False

    return True


def has_economic_moat(row, cfg):
    """경제적 해자(competitive advantage) 확인"""
    moat_score = 0
    components = []

    # 고수익성 (지속적 높은 ROE)
    roe = row.get("ROE(info)")
    if roe and roe > cfg.get("MIN_ROE_HF", 0.15):
        components.append(1.0)
    elif roe and roe > 0.12:
        components.append(0.7)
    else:
        components.append(0.3)

    # 높은 영업이익률 (가격결정력)
    op_margin = row.get("OpMarginTTM") or row.get("OperatingMargins(info)")
    if op_margin and op_margin > 0.20:
        components.append(1.0)
    elif op_margin and op_margin > 0.15:
        components.append(0.8)
    else:
        components.append(0.4)

    # 브랜드 가치 (배당 지속성으로 간접 측정)
    div_yield = row.get("DivYield")
    if div_yield and div_yield > 0.02:
        components.append(0.9)
    elif div_yield and div_yield > 0:
        components.append(0.6)
    else:
        components.append(0.3)

    moat_score = sum(components) / len(components) if components else 0
    return moat_score >= cfg.get("MIN_MOAT_SCORE", 0.7)


def has_stable_cashflow(row, cfg):
    """안정적인 현금흐름 확인"""
    # FCF Yield 기준 충족
    fcf_yield = row.get("FCF_Yield")
    if not fcf_yield or fcf_yield < cfg.get("MIN_FCFY_HF", 0.04):
        return False

    # 부채 대비 FCF 생성능력
    debt_equity = row.get("Debt_to_Equity")
    if debt_equity and debt_equity > 0:
        fcf_to_debt = fcf_yield / debt_equity
        if fcf_to_debt < 0.05:  # 부채 대비 FCF 생성능력 부족
            return False

    return True


def passes_modern_financial_health(row, cfg):
    """현대적 재무건전성 검증"""
    # 부채비율 검증
    debt_equity = row.get("Debt_to_Equity")
    if debt_equity and debt_equity > cfg.get("MAX_DEBT_EQUITY", 0.8):
        return False

    # 유동성 비율 (간접 측정 - 현재자산/현재부채 데이터가 없을 경우 기본 통과)
    current_assets = row.get("CurrentAssets")
    current_liabilities = row.get("CurrentLiabilities")
    if current_assets and current_liabilities:
        current_ratio = current_assets / current_liabilities
        if current_ratio < cfg.get("MIN_CURRENT_RATIO", 1.5):
            return False

    return True


def build_modern_buffett_scores(df: pd.DataFrame, cfg=CONFIG):
    """현대적 버핏 철학 반영 점수 계산"""
    temp = df.copy()
    modern_cfg = cfg["MODERN_BUFFETT"]

    # 기본 점수 계산 (현대적 가중치 적용)
    temp = build_scores_buffett(temp, modern_cfg)

    # 현대적 버핏 점수 요소 추가
    modern_scores = []

    for idx, row in temp.iterrows():
        modern_score_components = []

        # 1. 경제적 해자 점수
        moat_score = 0
        if has_economic_moat(row, modern_cfg):
            moat_score = 0.9
        else:
            # 해자 요소별 점수 계산
            roe_score = min(1.0, (row.get("ROE(info)") or 0) / 0.20)
            margin_score = min(1.0, (row.get("OpMarginTTM") or 0) / 0.25)
            brand_score = 1.0 if row.get("DivYield", 0) > 0.02 else 0.5
            moat_score = (roe_score + margin_score + brand_score) / 3

        modern_score_components.append(moat_score)

        # 2. 현금흐름 안정성 점수
        fcf_stability = 1.0 if has_stable_cashflow(row, modern_cfg) else 0.3
        modern_score_components.append(fcf_stability)

        # 3. 재무건전성 점수
        health_score = 1.0 if passes_modern_financial_health(row, modern_cfg) else 0.4
        modern_score_components.append(health_score)

        # 4. 경기방어성 점수 (섹터 기반)
        sector = str(row.get("Sector") or "").lower()
        defensive_score = 0.7  # 기본값
        if any(x in sector for x in ["consumer defensive", "utilities", "healthcare"]):
            defensive_score = 0.9
        elif any(x in sector for x in ["technology", "financial"]):
            defensive_score = 0.8
        elif any(x in sector for x in ["energy", "cyclical"]):
            defensive_score = 0.5

        modern_score_components.append(defensive_score)

        modern_score = sum(modern_score_components) / len(modern_score_components)
        modern_scores.append(modern_score)

    temp["ModernBuffettScore"] = pd.Series(modern_scores, index=temp.index)

    # 종합 점수에 현대적 요소 반영
    temp["TotalScore_Modern"] = (
            temp["TotalScore"] * 0.7 +
            temp["ModernBuffettScore"] * 100 * 0.3
    )

    return temp


def enhanced_valuation_screener():
    """
    강화된 버핏 기준을 적용한 통합 스크리너
    """
    # 데이터 로드
    df = load_cache(CONFIG["DETAILS_CACHE_FILE"])

    # 적정가 계산
    print("Calculating fair values...")
    fair_values_df = calculate_comprehensive_fair_value(df)
    df = pd.concat([df, fair_values_df], axis=1)

    results = {}

    # 1. 버핏-Lite (강화된 기본 조건)
    mask_lite = df.apply(lambda r: enhanced_pass_buffett_base(r, CONFIG), axis=1)
    raw_lite = df[mask_lite].copy()

    if not raw_lite.empty:
        scored_lite = build_scores_buffett(raw_lite, CONFIG)
        scored_lite['ValuationAdjustedScore'] = scored_lite['TotalScore'] * (
                1 + scored_lite['Discount_Pct'].fillna(0) / 100
        )
        scored_lite = scored_lite[scored_lite['TotalScore'] >= 60]
        results["buffett_lite"] = scored_lite.sort_values("ValuationAdjustedScore", ascending=False)

    # 2. 버핏-Strict (극히 엄격한 조건)
    strict_cfg = CONFIG.copy()
    strict_cfg.update({
        "MIN_MKTCAP": 2_000_000_000,    # 20억 달러
        "MIN_PRICE": 10.0,              # 15달러 이상
        "MIN_DOLLAR_VOLUME": 10_000_000,# 1000만 달러
        "MIN_DISCOUNT_PCT": 12.0,       # 최소 152% 할인
        "MIN_OP_MARGIN_HF": 0.12,       # 영업이익률 15% 이상
        "MIN_REV_TTM_YOY_HF": 0.05,     # 매출성장률 8% 이상
        "HARD_PE_MAX": 20.0,            # PER 18배 이하
        "MIN_ROE_HF": 0.15,             # ROE 18% 이상
        "MAX_DEBT_EQUITY": 1.0,         # 부채비율 0.8 이하
    })

    # 디버깅: 상위 10개 종목만 테스트
    print("\n🔍 Buffett-Strict 필터링 디버깅 (상위 10개 종목):")
    test_df = df.head(10).copy()
    for idx, row in test_df.iterrows():
        ticker = row.get('Ticker', 'Unknown')
        print(f"\n📊 {ticker} 필터링 결과:")
        enhanced_pass_buffett_base(row, strict_cfg, debug=True)

    mask_strict = df.apply(lambda r: enhanced_pass_buffett_base(r, strict_cfg), axis=1)
    raw_strict = df[mask_strict].copy()

    print(f"\n📈 Buffett-Strict 결과: {len(raw_strict)}개 종목 통과")

    if not raw_strict.empty:
        scored_strict = build_scores_buffett(raw_strict, strict_cfg)
        scored_strict['ValuationAdjustedScore'] = scored_strict['TotalScore'] * (
                1 + scored_strict['Discount_Pct'].fillna(0) / 100
        )
        scored_strict = scored_strict[scored_strict['TotalScore'] >= 70]
        results["buffett_strict"] = scored_strict.sort_values("ValuationAdjustedScore", ascending=False)
    else:
        # 조건을 완화한 대체 strict 설정
        print("⚠️ Buffett-Strict 조건이 너무 엄격합니다. 조건을 완화합니다...")
        alternative_strict_cfg = CONFIG.copy()
        alternative_strict_cfg.update({
            "MIN_MKTCAP": 2_000_000_000,  # 20억 달러로 완화
            "MIN_PRICE": 10.0,  # 10달러로 완화
            "MIN_DISCOUNT_PCT": 12.0,  # 12% 할인으로 완화
            "MIN_OP_MARGIN_HF": 0.12,  # 12%로 완화
            "MIN_ROE_HF": 0.15,  # 15%로 완화
        })

        mask_alt_strict = df.apply(lambda r: enhanced_pass_buffett_base(r, alternative_strict_cfg), axis=1)
        raw_alt_strict = df[mask_alt_strict].copy()

        if not raw_alt_strict.empty:
            scored_alt_strict = build_scores_buffett(raw_alt_strict, alternative_strict_cfg)
            scored_alt_strict['ValuationAdjustedScore'] = scored_alt_strict['TotalScore'] * (
                    1 + scored_alt_strict['Discount_Pct'].fillna(0) / 100
            )
            scored_alt_strict = scored_alt_strict[scored_alt_strict['TotalScore'] >= 65]
            results["buffett_strict"] = scored_alt_strict.sort_values("ValuationAdjustedScore", ascending=False)
            print(f"✅ 대체 Buffett-Strict: {len(results['buffett_strict'])}개 종목 발견")

    # 3. 현대적 버핏 (Modern Buffett)
    mask_modern = df.apply(lambda r: enhanced_buffett_modern_filter(r, CONFIG), axis=1)
    raw_modern = df[mask_modern].copy()

    if not raw_modern.empty:
        scored_modern = build_modern_buffett_scores(raw_modern, CONFIG)
        scored_modern = scored_modern[scored_modern['TotalScore_Modern'] >= 75]
        results["modern_buffett"] = scored_modern.sort_values("TotalScore_Modern", ascending=False)

    # 4. 트레이딩 프로파일 (swing, daytrade)
    for prof in ("swing", "daytrade"):
        mask_tr = df.apply(lambda r: pass_trading(r, prof, CONFIG), axis=1)
        base = df[mask_tr].copy()
        if not base.empty:
            scored = build_scores_trading(base, profile=prof, cfg=CONFIG)
            trading_cols = [
                "Ticker", "Name", "Sector", "Price", "DollarVol($M)", "RVOL",
                "ATR_PCT", "SMA20", "SMA50", "RET5", "RET20",
                "MomentumScore", "TrendScore", "LiquidityScore", "VolatilityScore", "TotalScore"
            ]
            trading_cols = [c for c in trading_cols if c in scored.columns]
            results[prof] = scored[trading_cols].sort_values("TotalScore", ascending=False)

    # 결과 저장
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_name = f"ELITE_SCREENER_{ts}.xlsx"

    with pd.ExcelWriter(out_name, engine='openpyxl') as writer:
        # 버핏 프로파일 시트
        for profile in ["buffett_lite", "buffett_strict", "modern_buffett"]:
            if profile in results and not results[profile].empty:
                if profile == "modern_buffett":
                    cols = [
                        'Ticker', 'Name', 'Sector', 'Price', 'FairValue_Composite', 'Discount_Pct',
                        'MktCap($B)', 'PE', 'ROE(info)', 'Debt_to_Equity', 'DivYield',
                        'GrowthScore', 'QualityScore', 'ValueScore', 'ModernBuffettScore', 'TotalScore_Modern'
                    ]
                else:
                    cols = [
                        'Ticker', 'Name', 'Sector', 'Price', 'FairValue_Composite', 'Discount_Pct',
                        'MktCap($B)', 'PE', 'ROE(info)', 'Debt_to_Equity', 'DivYield',
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
        for profile in ["buffett_lite", "buffett_strict", "modern_buffett", "swing", "daytrade"]:
            if profile in results and not results[profile].empty:
                if profile.startswith('buffett'):
                    avg_discount = results[profile]['Discount_Pct'].mean()
                    median_pe = results[profile]['PE'].median()
                    avg_roe = results[profile]['ROE(info)'].mean()
                    summary_data.append({
                        'Profile': profile,
                        'Stocks_Count': len(results[profile]),
                        'Avg_Discount_Pct': f"{avg_discount:.1f}%",
                        'Median_PE': f"{median_pe:.1f}",
                        'Avg_ROE': f"{avg_roe:.1f}%",
                        'Top_Tickers': ', '.join(results[profile].head(3)['Ticker'].tolist())
                    })
                else:
                    avg_rvol = results[profile]['RVOL'].mean()
                    avg_atr = results[profile]['ATR_PCT'].mean()
                    summary_data.append({
                        'Profile': profile,
                        'Stocks_Count': len(results[profile]),
                        'Avg_RVOL': f"{avg_rvol:.1f}",
                        'Avg_ATR_PCT': f"{avg_atr:.2f}%",
                        'Top_Tickers': ', '.join(results[profile].head(3)['Ticker'].tolist())
                    })

        pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)

        # 📋 열 설명 시트 추가
        # 📋 열 설명 시트 추가 (개선된 버전)
        column_explanations = [
            {
                '열 이름': 'Ticker',
                '의미': '종목코드',
                '설명': '주식 시장에서 사용하는 고유 기호',
                '적정 범위/기준': '-'
            },
            {
                '열 이름': 'Name',
                '의미': '회사명',
                '설명': '상장회사 공식 명칭',
                '적정 범위/기준': '-'
            },
            {
                '열 이름': 'Sector',
                '의미': '업종/섹터',
                '설명': '기술, 헬스케어, 금융 등 산업 분류',
                '적정 범위/기준': '경기방어성 섹터(소비재, 헬스케어) 선호'
            },
            {
                '열 이름': 'Price',
                '의미': '현재 주가',
                '설명': '현재 시장에서 거래되는 주식 가격',
                '적정 범위/기준': '10달러 이상 (저가주 리스크 회피)'
            },
            {
                '열 이름': 'FairValue_Composite',
                '의미': '종합 적정가',
                '설명': '여러 가치 평가 모델을 종합하여 계산한 공정 가치',
                '적정 범위/기준': '현재가보다 높을수록 좋음'
            },
            {
                '열 이름': 'Discount_Pct',
                '의미': '할인율 (%)',
                '설명': '적정가 대비 현재 주가가 낮은 정도, 양수면 저평가',
                '적정 범위/기준': '✅ 10-40%: 좋음\n⚠️ 0-10%: 보통\n❌ 0% 이하: 고평가'
            },
            {
                '열 이름': 'MktCap($B)',
                '의미': '시가총액 (10억 달러)',
                '설명': '회사의 전체 시장 가치',
                '적정 범위/기준': '✅ 10억$ 이상: 대형주\n⚠️ 1-10억$: 중형주\n❌ 1억$ 미만: 소형주(리스크)'
            },
            {
                '열 이름': 'PE',
                '의미': '주가수익비율',
                '설명': '주가를 주당순이익으로 나눈 값, 낮을수록 저평가',
                '적정 범위/기준': '✅ 8-20배: 저PER\n⚠️ 20-30배: 보통\n❌ 30배 이상: 고PER'
            },
            {
                '열 이름': 'PEG',
                '의미': '주가수익비율 성장률 배수',
                '설명': 'PER을 성장률로 나눈 값, 1 이하가 이상적',
                '적정 범위/기준': '✅ 0.5-1.0: 매우 좋음\n⚠️ 1.0-1.5: 보통\n❌ 1.5 이상: 고평가'
            },
            {
                '열 이름': 'EV_EBITDA',
                '의미': '기업가치 대비 EBITDA 비율',
                '설명': '기업 인수 비용 대비 영업이익',
                '적정 범위/기준': '✅ 5-12배: 좋음\n⚠️ 12-18배: 보통\n❌ 18배 이상: 고평가'
            },
            {
                '열 이름': 'FCF_Yield',
                '의미': '자유현금흐름 수익률',
                '설명': '주가 대비 자유현금흐름 비율, 높을수록 좋음',
                '적정 범위/기준': '✅ 5% 이상: 우량\n⚠️ 2-5%: 보통\n❌ 2% 미만: 약함'
            },
            {
                '열 이름': 'ROE(info)',
                '의미': '자기자본이익률',
                '설명': '자본 대비 순이익률, 수익성 지표',
                '적정 범위/기준': '✅ 15% 이상: 우량\n⚠️ 8-15%: 보통\n❌ 8% 미만: 약함'
            },
            {
                '열 이름': 'Debt_to_Equity',
                '의미': '부채비율',
                '설명': '자본 대비 부채 비율, 낮을수록 재무건전성 좋음',
                '적정 범위/기준': '✅ 0.5 이하: 매우 건전\n⚠️ 0.5-1.0: 보통\n❌ 1.0 이상: 위험'
            },
            {
                '열 이름': 'DivYield',
                '의미': '배당수익률',
                '설명': '주가 대비 배당금 비율',
                '적정 범위/기준': '✅ 2-6%: 적정\n⚠️ 6% 이상: 주의필요\n❌ 0%: 배당없음'
            },
            {
                '열 이름': 'GrowthScore',
                '의미': '성장성 점수',
                '설명': '매출 성장, 수익 성장 등 성장성 종합 점수',
                '적정 범위/기준': '✅ 70점 이상: 강한성장\n⚠️ 50-70점: 보통성장\n❌ 50점 미만: 낮은성장'
            },
            {
                '열 이름': 'QualityScore',
                '의미': '질 점수',
                '설명': '수익성, 재무건전성, 경영 효율성 종합 점수',
                '적정 범위/기준': '✅ 70점 이상: 우량기업\n⚠️ 50-70점: 보통기업\n❌ 50점 미만: 취약기업'
            },
            {
                '열 이름': 'ValueScore',
                '의미': '가치 점수',
                '설명': '저평가 정도, 다양한 가치 지표 종합 점수',
                '적정 범위/기준': '✅ 70점 이상: 저평가\n⚠️ 50-70점: 공정가치\n❌ 50점 미만: 고평가'
            },
            {
                '열 이름': 'TotalScore',
                '의미': '종합 총점',
                '설명': '성장성 + 질 + 가치 점수의 가중합',
                '적정 범위/기준': '✅ 70점 이상: 최우량\n⚠️ 60-70점: 우량\n❌ 60점 미만: 일반'
            },
            {
                '열 이름': 'ValuationAdjustedScore',
                '의미': '가치 조정 종합점수',
                '설명': '종합 총점에 할인율을 추가 반영한 최종 점수',
                '적정 범위/기준': '✅ 80점 이상: 매우매력적\n⚠️ 70-80점: 매력적\n❌ 70점 미만: 보통'
            },
            {
                '열 이름': 'ModernBuffettScore',
                '의미': '현대적 버핏 점수',
                '설명': '경제적 해자, 현금흐름 안정성 등 현대적 버핏 요소 점수',
                '적정 범위/기준': '✅ 0.8 이상: 강한해자\n⚠️ 0.6-0.8: 보통해자\n❌ 0.6 미만: 약한해자'
            },
            {
                '열 이름': 'TotalScore_Modern',
                '의미': '현대적 버핏 종합점수',
                '설명': '현대적 버핏 철학을 반영한 최종 점수',
                '적정 범위/기준': '✅ 75점 이상: 현대적우량\n⚠️ 65-75점: 현대적보통\n❌ 65점 미만: 일반'
            },
            {
                '열 이름': 'DollarVol($M)',
                '의미': '달러 거래량 (백만 달러)',
                '설명': '하루 거래 대금, 클수록 유동성 좋음',
                '적정 범위/기준': '✅ 10M$ 이상: 높은유동성\n⚠️ 1-10M$: 보통유동성\n❌ 1M$ 미만: 낮은유동성'
            },
            {
                '열 이름': 'RVOL',
                '의미': '상대 거래량',
                '설명': '평균 대비 거래량 비율 (1.0 = 평균)',
                '적정 범위/기준': '✅ 1.2-3.0: 적정관심\n⚠️ 0.8-1.2: 평균\n❌ 0.8 미만: 관심낮음'
            },
            {
                '열 이름': 'ATR_PCT',
                '의미': '평균 실제 범위 (%)',
                '설명': '주가 변동성 크기, 높을수록 등락 심함',
                '적정 범위/기준': '✅ 2-8%: 적정변동성\n⚠️ 8-15%: 고변동성\n❌ 15% 이상: 매우높은변동성'
            },
            {
                '열 이름': 'SMA20',
                '의미': '20일 이동평균',
                '설명': '단기 추세선',
                '적정 범위/기준': '✅ 주가 > SMA20: 상승추세\n⚠️ 주가 ≈ SMA20: 횡보\n❌ 주가 < SMA20: 하락추세'
            },
            {
                '열 이름': 'SMA50',
                '의미': '50일 이동평균',
                '설명': '중기 추세선',
                '적정 범위/기준': '✅ SMA20 > SMA50: 강한상승\n⚠️ SMA20 ≈ SMA50: 중립\n❌ SMA20 < SMA50: 약세'
            },
            {
                '열 이름': 'RET5',
                '의미': '5일 수익률',
                '설명': '최근 5일간 주가 등락율',
                '적정 범위/기준': '✅ 3-10%: 강한모멘텀\n⚠️ 0-3%: 약한모멘텀\n❌ 0% 미만: 하락모멘텀'
            },
            {
                '열 이름': 'RET20',
                '의미': '20일 수익률',
                '설명': '최근 20일간 주가 등락율',
                '적정 범위/기준': '✅ 5-20%: 강한상승\n⚠️ 0-5%: 약한상승\n❌ 0% 미만: 하락추세'
            },
            {
                '열 이름': 'MomentumScore',
                '의미': '모멘텀 점수',
                '설명': '단기 주가 추세 강도 (최근 상승력)',
                '적정 범위/기준': '✅ 70점 이상: 강한모멘텀\n⚠️ 50-70점: 보통모멘텀\n❌ 50점 미만: 약한모멘텀'
            },
            {
                '열 이름': 'TrendScore',
                '의미': '트렌드 점수',
                '설명': '장기 추세 방향성 (상승/하락/횡보)',
                '적정 범위/기준': '✅ 70점 이상: 강한상승추세\n⚠️ 50-70점: 약한상승/횡보\n❌ 50점 미만: 하락추세'
            },
            {
                '열 이름': 'LiquidityScore',
                '의미': '유동성 점수',
                '설명': '매매 용이성 (거래량, 거래대금 종합)',
                '적정 범위/기준': '✅ 70점 이상: 높은유동성\n⚠️ 50-70점: 보통유동성\n❌ 50점 미만: 낮은유동성'
            },
            {
                '열 이름': 'VolatilityScore',
                '의미': '변동성 점수',
                '설명': '적정 변동성 (너무 낮거나 높지 않은 적정 수준)',
                '적정 범위/기준': '✅ 60-80점: 이상적변동성\n⚠️ 40-60점: 높은변동성\n❌ 40점 미만: 매우높은변동성'
            },
        ]

        # DataFrame 생성 및 엑셀 저장
        explanation_df = pd.DataFrame(column_explanations)
        explanation_df.to_excel(writer, sheet_name='열_설명', index=False)

        # 시트 서식 조정 (컬럼 너비 자동 조정)
        worksheet = writer.sheets['열_설명']
        worksheet.column_dimensions['A'].width = 15  # 열 이름
        worksheet.column_dimensions['B'].width = 12  # 의미
        worksheet.column_dimensions['C'].width = 25  # 설명
        worksheet.column_dimensions['D'].width = 35  # 적정 범위/기준

        # 🎯 필터 기준 설명 시트 추가
        filter_criteria = [
            {
                '프로파일': 'buffett_lite',
                '선정 기준': '기본 버핏 조건 - 안정적인 우량주',
                '주요 필터': [
                    '시가총액 ≥ 10억 달러 (대형주 안정성)',
                    '주가 ≥ 10달러 (저가주 리스크 회피)',
                    '거래대금 ≥ 1000만 달러 (유동성 보장)',
                    '매출성장률 ≥ 3% (성장성 확인)',
                    '영업이익률 ≥ 10% (수익성 기준)',
                    'ROE ≥ 12% (자본효율성)',
                    '부채비율 ≤ 1.0 (재무건전성)',
                    'PER ≤ 20배 (가치 평가)',
                    '적정가 대비 할인율 ≥ 10% (안전마진)',
                    '종합점수 ≥ 60점 (종합 평가)'
                ],
                '적합 투자자': '장기 가치투자 입문자, 안정성 중시 투자자'
            },
            {
                '프로파일': 'buffett_strict',
                '선정 기준': '엄격한 버핏 조건 - 고품질 우량주',
                '주요 필터': [
                    '시가총액 ≥ 50억 달러',
                    '주가 ≥ 15달러',
                    '거래대금 ≥ 2000만 달러',
                    '매출성장률 ≥ 8%',
                    '영업이익률 ≥ 15%',
                    'ROE ≥ 18%',
                    '부채비율 ≤ 0.8',
                    'PER ≤ 18배',
                    '적정가 대비 할인율 ≥ 15%',
                    '종합점수 ≥ 70점'
                ],
                '적합 투자자': '경험丰富的 가치투자자, 고품질 주식 선호'
            },
            {
                '프로파일': 'modern_buffett',
                '선정 기준': '현대적 버핏 조건 - 대형 우량주 + 경제적 해자',
                '주요 필터': [
                    '시가총액 ≥ 100억 달러',
                    '주가 ≥ 15달러',
                    '거래대금 ≥ 2000만 달러',
                    '매출성장률 ≥ 8%',
                    '영업이익률 ≥ 15%',
                    'ROE ≥ 18%',
                    '부채비율 ≤ 0.8',
                    '경제적 해자 점수 ≥ 0.7',
                    '현금흐름 안정성 통과',
                    '현대적 버핏 점수 ≥ 75점'
                ],
                '적합 투자자': '워렌 버핏 현대적 철학 따르는 투자자, 초대형주 선호'
            },
            {
                '프로파일': 'swing',
                '선정 기준': '스윙트레이딩 - 중기 모멘텀 + 추세',
                '주요 필터': [
                    '주가 ≥ 5달러',
                    '거래대금 ≥ 500만 달러',
                    '상대거래량 ≥ 1.2',
                    '변동성 (ATR) 2~12%',
                    '주가 > 20일이평 > 50일이평',
                    '20일 수익률 ≥ 0%',
                    '모멘텀 점수 중시 (45%)'
                ],
                '적합 투자자': '중기 트레이더, 추세 모멘텀 전략가'
            },
            {
                '프로파일': 'daytrade',
                '선정 기준': '데이트레이딩 - 단기 모멘텀 + 유동성',
                '주요 필터': [
                    '주가 ≥ 5달러',
                    '거래대금 ≥ 2000만 달러',
                    '상대거래량 ≥ 2.0',
                    '변동성 (ATR) 3~20%',
                    '5일 수익률 ≥ 3%',
                    '유동성 점수 중시 (40%)',
                    '모멘텀 점수 중시 (30%)'
                ],
                '적합 투자자': '단기 스캘퍼, 고변동성 주식 선호 트레이더'
            }
        ]

        # 필터 기준을 DataFrame으로 변환
        filter_data = []
        for criteria in filter_criteria:
            filter_str = '\n'.join([f"• {item}" for item in criteria['주요 필터']])
            filter_data.append({
                '프로파일': criteria['프로파일'],
                '선정 기준': criteria['선정 기준'],
                '주요 필터 조건': filter_str,
                '적합 투자자': criteria['적합 투자자']
            })

        pd.DataFrame(filter_data).to_excel(writer, sheet_name='필터_기준', index=False)

        # 💡 투자 가이드 시트 추가
        investment_guide = [
            {
                '구분': '버핏 스타일',
                '투자 철학': '가치투자 - 내재가치보다 저렴한 우량주 매수',
                '보유 기간': '장기 (1년 이상)',
                '매수 타이밍': ['할인율 10% 이상', '시장 과열기 피하기', '종합점수 60점 이상'],
                '매도 타이밍': ['할인율 0% 이하 (고평가)', '기본적 악화', '대체 투자처 발견'],
                '리스크 관리': ['분산투자', '재무제표 정기 점검', '장기 보유 인내']
            },
            {
                '구분': '트레이딩 스타일',
                '투자 철학': '기술적 분석 - 추세와 모멘텀 활용',
                '보유 기간': '스윙: 수일~수주, 데이: 당일',
                '매수 타이밍': ['추세 상승 확인', '모멘텀 가속', '지지선 돌파'],
                '매도 타이밍': ['저항선 도달', '모멘텀 약화', '손절라인 도달'],
                '리스크 관리': ['고정 손절라인 설정', '포지션 사이즈 관리', '감정적 거래 금지']
            },
            {
                '구분': '공통 원칙',
                '투자 철학': '계획된 투자, 감정적 결정 금지',
                '보유 기간': '전략에 따른 일관된 실행',
                '매수 타이밍': ['확률 유리할 때', '리스크-보상비 좋을 때'],
                '매도 타이밍': ['전략적 목표 도달', '가정 변경 시'],
                '리스크 관리': ['자본의 1-2% 이상 단일종목 투자 금지', '정기적 포트폴리오 리밸런싱']
            }
        ]

        pd.DataFrame(investment_guide).to_excel(writer, sheet_name='투자_가이드', index=False)

    print(f"[ELITE SCREENER] Results saved to: {out_name}")
    print(f"🎯 고품질 저평가 우량주 필터링 결과:")
    for profile, result_df in results.items():
        if profile.startswith('buffett') and not result_df.empty:
            discount_avg = result_df['Discount_Pct'].mean()
            roe_avg = result_df['ROE(info)'].mean()
            print(f"   📊 {profile}: {len(result_df)}개 (할인율: {discount_avg:.1f}%, ROE: {roe_avg:.1f}%)")

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
    """개선된 버핏 스타일 점수 계산 (데이터 누락 대응)"""
    temp = df.copy()

    # 누락될 수 있는 컬럼들에 대한 안전장치
    if "ROE_5Y_Avg" not in temp.columns:
        temp["ROE_5Y_Avg"] = temp["ROE(info)"]  # 기본값으로 ROE(info) 사용

    if "Debt_to_Equity" not in temp.columns:
        temp["Debt_to_Equity"] = np.nan

    if "BuybackYield" not in temp.columns:
        temp["BuybackYield"] = np.nan

    if "P_FFO" not in temp.columns:
        temp["P_FFO"] = np.nan

    if "FCF_Yield" not in temp.columns:
        temp["FCF_Yield"] = np.nan

    # 데이터 전처리
    temp["_OpMarginUse"] = temp[["OpMarginTTM", "OperatingMargins(info)"]].max(axis=1, numeric_only=True)

    # Winsorize로 이상치 처리 (데이터 있는 경우만)
    for col in ["RevYoY", "_OpMarginUse", "ROE(info)", "ROE_5Y_Avg", "FCF_Yield",
                "EV_EBITDA", "PE", "PEG", "PB", "DivYield", "Debt_to_Equity"]:
        if col in temp.columns and temp[col].notna().sum() > 0:
            temp[col] = _winsor_series(temp[col].astype(float), p=0.02)
        elif col in temp.columns:
            temp[col] = np.nan

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
        # PEG가 낮을수록 성장성 좋음 (역수 사용, 데이터 있을 때만)
        peg = row.get("PEG")
        if peg and not pd.isna(peg) and peg > 0:
            peg_score = 1.0 / peg
        else:
            peg_score = 0  # 데이터 없으면 0
        growth_components = [rev_growth, peg_score]
        growth_components = [x for x in growth_components if not pd.isna(x)]
        growth_score = np.nanmean(growth_components) if growth_components else 0
        growth_s.append(growth_score)

        # 질 점수: 수익성 + 재무건전성
        quality_components = []

        # 수익성 지표
        op_margin = row.get("_OpMarginUse")
        roe = row.get("ROE(info)") or row.get("ROE_5Y_Avg")
        if op_margin and not pd.isna(op_margin):
            quality_components.append(op_margin)
        if roe and not pd.isna(roe):
            quality_components.append(roe)

        # 재무건전성 지표
        debt_equity = row.get("Debt_to_Equity")
        if debt_equity is not None and not pd.isna(debt_equity):
            # 부채비율이 낮을수록 점수 높음
            debt_score = max(0, 1.0 - (debt_equity / cfg.get("MAX_DEBT_EQUITY", 2.0)))
            quality_components.append(debt_score)

        # FCF Yield (현금창출능력)
        fcf_yield = row.get("FCF_Yield")
        if fcf_yield and not pd.isna(fcf_yield) and fcf_yield > 0:
            quality_components.append(fcf_yield)

        qual_s.append(np.nanmean(quality_components) if quality_components else 0.5)

        # 가치 점수: 섹터별 차별화
        val_components = []

        if any(x in sec for x in FIN_SECTORS):
            # 금융주: P/B, ROE, Div Yield
            if "PB" in temp.columns and not pd.isna(row.get("PB")):
                val_components.append(_percentile_rank(temp["PB"], False)[i])
            if "ROE(info)" in temp.columns and not pd.isna(row.get("ROE(info)")):
                val_components.append(_percentile_rank(temp["ROE(info)"], True)[i])
            if "DivYield" in temp.columns and not pd.isna(row.get("DivYield")):
                val_components.append(_percentile_rank(temp["DivYield"], True)[i])

        elif any(x in sec for x in REIT_SECTORS):
            # 리츠: P/FFO, Div Yield
            if "P_FFO" in temp.columns and not pd.isna(row.get("P_FFO")):
                val_components.append(_percentile_rank(temp["P_FFO"], False)[i])
            if "DivYield" in temp.columns and not pd.isna(row.get("DivYield")):
                val_components.append(_percentile_rank(temp["DivYield"], True)[i])
        else:
            # 일반 주식: 다양한 가치 지표 (데이터 있는 것만 사용)
            for col, higher in [("FCF_Yield", True), ("EV_EBITDA", False),
                                ("PE", False), ("PEG", False), ("PB", False)]:
                if col in temp.columns and not pd.isna(row.get(col)):
                    val_components.append(_percentile_rank(temp[col], higher)[i])

        # val_components가 비어있지 않을 때만 계산
        if val_components:
            val_score = np.nanmean(val_components)
        else:
            val_score = 0.5  # 기본값

        val_s.append(val_score)

        # 촉매 점수: 배당, 자사주 매입
        catalyst_components = []
        if "DivYield" in temp and not pd.isna(row.get("DivYield")):
            catalyst_components.append(_percentile_rank(temp["DivYield"], True)[i])

        if "BuybackYield" in temp and not pd.isna(row.get("BuybackYield")):
            catalyst_components.append(_percentile_rank(temp["BuybackYield"], True)[i])

        # 거래량/변동성 (주목도 지표)
        if "RVOL" in temp and not pd.isna(row.get("RVOL")):
            rvol_score = min(1.0, (row.get("RVOL") or 1) / 3.0)
            catalyst_components.append(rvol_score)

        cat_s.append(np.nanmean(catalyst_components) if catalyst_components else 0.5)

    # 점수 정규화
    temp["GrowthScore"] = pd.Series(growth_s, index=temp.index).rank(pct=True).fillna(0.5)
    temp["QualityScore"] = pd.Series(qual_s, index=temp.index).rank(pct=True).fillna(0.5)
    temp["ValueScore"] = pd.Series(val_s, index=temp.index).fillna(0.5)
    temp["CatalystScore"] = pd.Series(cat_s, index=temp.index).fillna(0.5)

    # 총점 계산
    temp["TotalScore"] = 100 * (
            cfg.get("W_GROWTH", 0.15) * temp["GrowthScore"] +
            cfg.get("W_QUALITY", 0.35) * temp["QualityScore"] +
            cfg.get("W_VALUE", 0.40) * temp["ValueScore"] +
            cfg.get("W_CATALYST", 0.10) * temp["CatalystScore"]
    )

    return temp

def enhanced_pass_buffett_base(row, cfg=CONFIG, debug=False):
    """강화된 버핏 스타일 필터링 (디버깅 모드 추가)"""
    # 기본 유동성 필터
    price = row.get("Price")
    dv = (row.get("DollarVol($M)") or 0) * 1_000_000
    if pd.isna(price) or pd.isna(dv):
        if debug: print(f"  ❌ 유동성 필터 실패: price={price}, dv={dv}")
        return False

    if price < cfg.get("MIN_PRICE", 10.0) or dv < cfg.get("MIN_DOLLAR_VOLUME", 10_000_000):
        if debug: print(f"  ❌ 최소가격/거래량 필터: price={price}, dv={dv}")
        return False

    # 시가총액 필터
    mktcap = (row.get("MktCap($B)") or 0) * 1_000_000_000
    min_mktcap = cfg.get("MIN_MKTCAP", 1_000_000_000)
    if mktcap and mktcap < min_mktcap:
        if debug: print(f"  ❌ 시가총액 필터: mktcap={mktcap}, min={min_mktcap}")
        return False

    # 성장성 필터
    rev_yoy = row.get("RevYoY")
    min_rev_yoy = cfg.get("MIN_REV_TTM_YOY_HF", 0.05)
    if (rev_yoy is None) or (rev_yoy < min_rev_yoy):
        if debug: print(f"  ❌ 성장성 필터: rev_yoy={rev_yoy}, min={min_rev_yoy}")
        return False

    # 수익성 필터 (섹터별 면제)
    sec = str(row.get("Sector") or "").lower()
    op_margin = row.get("OpMarginTTM") or row.get("OperatingMargins(info)")
    min_op_margin = cfg.get("MIN_OP_MARGIN_HF", 0.12)
    if sec not in cfg.get("OP_MARGIN_EXEMPT_SECTORS", FIN_SECTORS):
        if (op_margin is None) or (op_margin < min_op_margin):
            if debug: print(f"  ❌ 수익성 필터: op_margin={op_margin}, min={min_op_margin}, sector={sec}")
            return False

    # 재무건전성 필터 (데이터 있을 때만 적용)
    debt_equity = row.get("Debt_to_Equity")
    max_debt_equity = cfg.get("MAX_DEBT_EQUITY", 1.0)
    if debt_equity and not pd.isna(debt_equity) and debt_equity > max_debt_equity:
        if debug: print(f"  ❌ 재무건전성 필터: debt_equity={debt_equity}, max={max_debt_equity}")
        return False

    # 수익성 필터 (ROE)
    roe = row.get("ROE(info)") or row.get("ROE_5Y_Avg")
    min_roe = cfg.get("MIN_ROE_HF", 0.15)
    if roe is None or pd.isna(roe) or roe < min_roe:
        if debug: print(f"  ❌ ROE 필터: roe={roe}, min={min_roe}")
        return False

    # 가치 필터 (데이터 있을 때만 적용, 조건이 None이면 체크하지 않음)
    peg = row.get("PEG")
    max_peg = cfg.get("HARD_PEG_MAX")
    if (peg is not None and not pd.isna(peg) and
        max_peg is not None and
        peg > max_peg):
        if debug: print(f"  ❌ PEG 필터: peg={peg}, max={max_peg}")
        return False

    pe = row.get("PE")
    max_pe = cfg.get("HARD_PE_MAX")
    if (pe is not None and not pd.isna(pe) and
        max_pe is not None and
        pe > max_pe):
        if debug: print(f"  ❌ PE 필터: pe={pe}, max={max_pe}")
        return False

    ev_eb = row.get("EV_EBITDA")
    max_ev_eb = cfg.get("MAX_EV_EBITDA_HARD")
    if (ev_eb is not None and not pd.isna(ev_eb) and
        max_ev_eb is not None and
        ev_eb > max_ev_eb):
        if debug: print(f"  ❌ EV/EBITDA 필터: ev_eb={ev_eb}, max={max_ev_eb}")
        return False

    # 현금창출능력 필터 (데이터 있을 때만 적용, 조건이 None이면 체크하지 않음)
    fcfy = row.get("FCF_Yield")
    min_fcfy = cfg.get("MIN_FCFY_HF")
    if (fcfy is not None and not pd.isna(fcfy) and
        min_fcfy is not None and
        fcfy < min_fcfy):
        if debug: print(f"  ❌ FCF Yield 필터: fcfy={fcfy}, min={min_fcfy}")
        return False

    # 배당 수익률 필터 (데이터 있을 때만 적용, 조건이 None이면 체크하지 않음)
    div_yield = row.get("DivYield")
    min_div_yield = cfg.get("MIN_DIV_YIELD")
    if (min_div_yield is not None and
        div_yield is not None and not pd.isna(div_yield) and
        div_yield < min_div_yield):
        if debug: print(f"  ❌ 배당수익률 필터: div_yield={div_yield}, min={min_div_yield}")
        return False

    # 적정가 할인율 필터
    discount_pct = row.get('Discount_Pct')
    min_discount = cfg.get("MIN_DISCOUNT_PCT", 10.0)
    if discount_pct is None or pd.isna(discount_pct) or discount_pct < min_discount:
        if debug: print(f"  ❌ 할인율 필터: discount_pct={discount_pct}, min={min_discount}")
        return False

    if debug: print(f"  ✅ 모든 필터 통과!")
    return True

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

if __name__ == "__main__":
    # 데이터 로드 및 품질 확인
    df = load_cache(CONFIG["DETAILS_CACHE_FILE"])
    check_data_quality_before_screening(df)

    # 통합 스크리너 실행
    comprehensive_results = enhanced_valuation_screener()

    print("\n✅ 모든 스크리닝 완료!")
    print("📋 생성된 엑셀 파일에는 다음 프로파일이 포함됩니다:")
    print("   - buffett_lite, buffett_strict, modern_buffett (버핏 스타일)")
    print("   - swing, daytrade (트레이딩 스타일)")
    print("   - Summary (종합 요약)")