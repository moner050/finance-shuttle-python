# -*- coding: utf-8 -*-
"""
screener_from_details_cache.py

(인터넷 연결 불필요) build_details_cache.py가 만든 details_cache_{source}.csv/.xlsx
하나만으로 4개 프로파일 결과를 Excel로 출력.

권장 설치:
  pip install -U pandas numpy XlsxWriter openpyxl

개선사항:
1. 버핏 스타일에 더 적합한 점수 체계
2. 섹터별 차별화된 평가
3. 현실적인 필터링 조건
4. 더 다양한 재무 지표 반영
"""

import os, math, time, random
import pandas as pd, numpy as np
from datetime import datetime

CONFIG = {
    "DETAILS_CACHE_FILE": "details_cache_us_all.csv",
    "RUN_PROFILES": ["buffett_lite", "buffett_strict", "swing", "daytrade"],

    # 버핏 공통 컷
    "MIN_MKTCAP": 800_000_000,
    "MIN_PRICE": 1.0,
    "MIN_DOLLAR_VOLUME": 900_000,
    "MAX_EV_EBITDA_HARD": 25.0,
    "HARD_PEG_MAX": 2.0,
    "HARD_PE_MAX": 30.0,
    "MIN_REV_TTM_YOY_HF": -0.15,
    "MIN_OP_MARGIN_HF": 0.05,
    "MAX_DEBT_EQUITY": 2.0,
    "MIN_ROE_HF": 0.08,
    "MIN_FCFY_HF": 0.02,

    # 섹터별 OP Margin 면제
    "OP_MARGIN_EXEMPT_SECTORS": {
        "financial services", "banks", "insurance", "capital markets",
        "mortgage finance", "utilities", "real estate", "reit"
    },

    # 개선된 점수 가중치 (버핏 스타일)
    "W_GROWTH": 0.15,      # 증가: 성장성 더 강조
    "W_QUALITY": 0.35,     # 증가: 수익성과 재무건전성 강조
    "W_VALUE": 0.40,       # 감소: 가치보다 질 더 강조
    "W_CATALYST": 0.10,    # 유지

    # 트레이딩 컷
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

    # 출력
    "OUT_PREFIX": "IMPROVED_SCREENER",
}

FIN_SECTORS = {"banks", "financial", "insurance", "capital markets"}
REIT_SECTORS = {"reit", "real estate", "property"}
CYCLICAL_SECTORS = {"energy", "materials", "industrials", "consumer cyclical"}
DEFENSIVE_SECTORS = {"utilities", "consumer defensive", "healthcare"}

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

def build_scores_buffett(df: pd.DataFrame, cfg=CONFIG):
    """개선된 버핏 스타일 점수 계산"""
    temp = df.copy()

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
        for prof in ["buffett_strict", "buffett_lite"]:
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
    run_enhanced_screener()
