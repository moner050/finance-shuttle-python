# -*- coding: utf-8 -*-
"""
build_details_cache.py

유니버스(US_ALL/SP500/커스텀)를 불러와 OHLCV(기본 120d)에서 라이트 지표(Price, DollarVol, RVOL, ATR_PCT 등)를 전종목 산출
→ 라이트 컷 통과 종목(및 상위 DETAILED_TOP_K)에 한해 재무 지표(RevYoY, OpMarginTTM, EV/EBITDA, FCFY 등)까지 수집
→ 단일 캐시 파일(details_cache_{source}.csv / .xlsx)에 저장

권장 설치:
  pip install -U yfinance==0.2.43 pandas numpy XlsxWriter openpyxl requests matplotlib

유니버스(US_ALL/SP500/커스텀)를 불러와 OHLCV(기본 120d)에서 라이트 지표(Price, DollarVol, RVOL, ATR_PCT 등)를 전종목 산출
→ 라이트 컷 통과 종목(및 상위 DETAILED_TOP_K)에 한해 재무 지표(RevYoY, OpMarginTTM, EV/EBITDA, FCFY 등)까지 수집
→ 단일 캐시 파일(details_cache_{source}.csv / .xlsx)에 저장

개선사항:
1. EV/EBITDA 계산 로직 강화
2. FCF Yield 계산 방식 개선
3. 재무 데이터 품질 향상
4. 에러 처리 보완
"""

import os, io, time, math, random, warnings, logging, requests
import pandas as pd, numpy as np, yfinance as yf
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

warnings.filterwarnings("ignore", category=FutureWarning)
logging.getLogger("yfinance").setLevel(logging.CRITICAL)

# ===================== CONFIG =====================
CONFIG = {
    "UNIVERSE_SOURCE": "us_all",  # "us_all" | "sp500" | "custom"
    "CUSTOM_TICKERS": [],  # UNIVERSE_SOURCE="custom"일 때 사용

    # 캐시 출력
    "OUT_BASENAME": "",  # 비우면 자동: details_cache_{source}.csv
    "INCLUDE_EXCEL": True,

    # OHLCV 프리로드(라이트 지표)
    "PRELOAD_PERIOD": "120d",
    "PRELOAD_CHUNK": 60,
    "BATCH_RETRIES": 3,
    "SINGLE_RETRIES": 2,
    "FALLBACK_MAX_WORKERS": 12,
    "YF_THREADS": True,
    "SLEEP_SEC": 0.15,

    # 라이트 컷(라이트 통과 종목만 상세 재무 호출)
    "MIN_PRICE": 1.0,
    "MIN_DOLLAR_VOLUME": 900_000,  # Price * avg20Vol
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

    # 상세 재무 호출 대상 범위
    "DETAILED_TOP_K": 1000,  # 라이트 통과 중 DollarVol 상위 K까지 상세 재무 호출
    "MAX_TICKERS": 5000,
    "UNIVERSE_OFFSET": 0,
    "SHUFFLE_UNIVERSE": True,

    # 버핏형 하드컷 기본선
    "MIN_MKTCAP": 800_000_000,

    # 요청 헤더
    "USER_AGENT": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",

    # 개선된 설정값 추가
    "MIN_REV_YOY": -0.20,  # 최소 매출 성장률
    "MIN_OP_MARGIN": 0.05,  # 최소 영업이익률
    "MAX_DEBT_EQUITY": 2.0,  # 최대 부채비율
}
# ==================================================

HEADERS = {"User-Agent": CONFIG["USER_AGENT"]}
HTTP_SESSION = requests.Session(); HTTP_SESSION.headers.update(HEADERS)

def _normalize_ticker(t): return str(t).strip().upper().replace(".", "-")

def _read_html(url: str):
    r = HTTP_SESSION.get(url, timeout=30); r.raise_for_status()
    return pd.read_html(io.StringIO(r.text))

def get_sp500_symbols():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    df = _read_html(url)[0]
    col = next((c for c in df.columns if str(c).lower().startswith("symbol")), "Symbol")
    syms = df[col].dropna().astype(str).tolist()
    print(f"[S&P500] from Wikipedia: {len(syms)}")
    return [_normalize_ticker(s) for s in syms]

def _fetch_text(url):
    r = HTTP_SESSION.get(url, timeout=30, allow_redirects=True)
    r.raise_for_status()
    return r.text

def _read_pipe_text_to_df(text: str) -> pd.DataFrame:
    return pd.read_csv(io.StringIO(text), sep="|")

def _normalize_symbol_df(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower(): c for c in df.columns}
    sym  = cols.get("symbol") or cols.get("act symbol") or cols.get("nasdaq symbol") or list(df.columns)[0]
    name = cols.get("security name") or cols.get("securityname") or cols.get("security")
    etf  = cols.get("etf")
    test = cols.get("test issue") or cols.get("testissue")
    fin  = cols.get("financial status") or cols.get("financialstatus")
    ex   = cols.get("exchange")
    out = df.copy()
    out.rename(columns={sym:"Symbol"}, inplace=True)
    if name: out.rename(columns={name:"SecurityName"}, inplace=True)
    if etf:  out.rename(columns={etf:"ETF"}, inplace=True)
    if test: out.rename(columns={test:"TestIssue"}, inplace=True)
    if fin:  out.rename(columns={fin:"FinancialStatus"}, inplace=True)
    if ex:   out.rename(columns={ex:"Exchange"}, inplace=True)
    for c in ["Symbol","SecurityName","ETF","TestIssue","FinancialStatus","Exchange"]:
        if c not in out.columns: out[c] = None
    out["Symbol"] = out["Symbol"].astype(str).str.upper().str.replace(".", "-", regex=False)
    # 기본 클린업
    mask_test = out["TestIssue"].astype(str).str.upper().ne("Y")
    fin_s = out["FinancialStatus"].astype(str).str.upper()
    mask_fin = (~fin_s.isin(["D","E","H","S","C","T"]))
    return out[mask_test & mask_fin]

def _filter_common_stock(df: pd.DataFrame) -> pd.DataFrame:
    name_str = df["SecurityName"].astype(str).str.lower()
    is_common_kw = name_str.str.contains(r"common stock|ordinary shares|class [ab]\s+common|shs", regex=True)
    is_deriv_kw = name_str.str.contains(r"warrant|right|unit|preferred|preference|pref|etf|fund|trust|note|debenture|bond|adr|adr\.", regex=True)
    base = df[is_common_kw & ~is_deriv_kw]
    return base if not base.empty else df[~is_deriv_kw]

def get_all_us_listed_common():
    urls = [
        "https://ftp.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt",
        "https://ftp.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt",
        "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt",
        "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt",
        "https://ftp.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt",
        "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt",
    ]
    dfs=[]
    for u in urls:
        try:
            txt = _fetch_text(u)
            df = _normalize_symbol_df(_read_pipe_text_to_df(txt))
            dfs.append(df)
            print(f"[US_ALL] fetched: {u}")
        except Exception as e:
            print(f"[US_ALL] skip {u} -> {e}")
    if not dfs: raise RuntimeError("Failed to fetch symbol lists")
    df = pd.concat(dfs, ignore_index=True)
    df = _filter_common_stock(df)
    syms = df["Symbol"].dropna().unique().tolist()
    print(f"[US_ALL] total symbols after filter: {len(syms)}")
    return sorted(syms)

def load_universe():
    src = CONFIG["UNIVERSE_SOURCE"]
    if src == "sp500": u = get_sp500_symbols()
    elif src == "us_all": u = get_all_us_listed_common()
    elif src == "custom": u = [_normalize_ticker(x) for x in CONFIG["CUSTOM_TICKERS"]]
    else: raise ValueError("UNIVERSE_SOURCE must be one of: us_all, sp500, custom")
    if CONFIG["SHUFFLE_UNIVERSE"]: random.shuffle(u)
    if CONFIG["MAX_TICKERS"]: u = u[CONFIG["UNIVERSE_OFFSET"]: CONFIG["UNIVERSE_OFFSET"] + CONFIG["MAX_TICKERS"]]
    elif CONFIG["UNIVERSE_OFFSET"]: u = u[CONFIG["UNIVERSE_OFFSET"]:]
    print(f"[Universe] {src} total={len(u)} sample={u[:12]}")
    return u

# ============== OHLCV → 라이트 지표 ==============
def _tz_naive(obj):
    if hasattr(obj, "index"):
        try:
            if getattr(obj.index, "tz", None) is not None:
                obj = obj.copy()
                obj.index = obj.index.tz_convert("UTC").tz_localize(None)
        except Exception: pass
    return obj

def _compute_ta_single(c, h, l, v):
    if c is None or c.dropna().shape[0] < 5: return None
    c=c.dropna()
    s20 = c.rolling(20).mean()
    s50 = c.rolling(50).mean() if c.shape[0] >= 50 else pd.Series(index=c.index, dtype=float)
    last_close=float(c.iloc[-1])
    sma20=float(s20.iloc[-1]) if s20.dropna().size else None
    sma50=float(s50.iloc[-1]) if s50.dropna().size else None
    ret5 = float(c.pct_change(5).iloc[-1]) if c.shape[0]>=6 else None
    ret20= float(c.pct_change(20).iloc[-1]) if c.shape[0]>=21 else None
    avg20=today_vol=rvol=None
    if v is not None and v.dropna().shape[0] >= 2:
        v=v.dropna()
        avg20=float(v.rolling(20).mean().iloc[-1]) if v.shape[0]>=20 else float(v.mean())
        today_vol=float(v.iloc[-1]); rvol = today_vol/avg20 if avg20>0 else None
    atr=atr_pct=None
    if h is not None and l is not None and c.shape[0]>=15 and h.dropna().shape[0] and l.dropna().shape[0]:
        prev_close=c.shift(1)
        tr=pd.concat([h-l,(h-prev_close).abs(),(l-prev_close).abs()],axis=1).max(axis=1)
        atr=float(tr.rolling(14).mean().iloc[-1]) if tr.dropna().shape[0]>=14 else None
        atr_pct=(atr/last_close) if (atr and last_close>0) else None
    return {"last_price":last_close,"sma20":sma20,"sma50":sma50,"ret5":ret5,"ret20":ret20,
            "avg20_vol":avg20,"today_vol":today_vol,"rvol":rvol,"atr":atr,"atr_pct":atr_pct}

def _compute_ta_metrics(df):
    out={}
    if isinstance(df.columns, pd.MultiIndex):
        fields=set(df.columns.get_level_values(0))
        tks=sorted(set(df.columns.get_level_values(1)))
        ck="Adj Close" if "Adj Close" in fields else "Close"
        for t in tks:
            try:
                c=df[(ck,t)].dropna()
                h=df[("High",t)].dropna() if ("High",t) in df.columns else None
                l=df[("Low",t)].dropna()  if ("Low",t)  in df.columns else None
                v=df[("Volume",t)].dropna() if ("Volume",t) in df.columns else None
                m=_compute_ta_single(c,h,l,v)
                if m: out[t]=m
            except Exception: continue
    else:
        ck="Adj Close" if "Adj Close" in df.columns else "Close"
        c=df.get(ck); h=df.get("High"); l=df.get("Low"); v=df.get("Volume")
        m=_compute_ta_single(c,h,l,v)
        if m: out["__SINGLE__"]=m
    return out

def preload_ohlcv_light(tickers, period="120d", chunk=60, batch_retries=3, single_retries=2, workers=12, threads=True, sleep=0.15):
    TA, PX, VOL = {}, {}, {}
    ok=set(); last_px={}; avg_vol={}
    def consume(df, batch):
        if not isinstance(df, pd.DataFrame) or df.empty: return
        df=_tz_naive(df); m=_compute_ta_metrics(df)
        if isinstance(df.columns, pd.MultiIndex):
            ck = "Adj Close" if "Adj Close" in set(df.columns.get_level_values(0)) else "Close"
            for t in batch:
                try:
                    if (ck,t) not in df.columns: continue
                    c=df[(ck,t)].dropna()
                    if len(c)>=2:
                        ok.add(t); last_px[t]=float(c.iloc[-1])
                        v=df[("Volume",t)].dropna() if ("Volume",t) in df.columns else pd.Series(dtype=float)
                        avg_vol[t]=float(v.mean()) if len(v)>0 else 0.0
                    if t in m:
                        TA[t]=m[t]; PX[t]=m[t]["last_price"]
                        VOL[t]=m[t]["avg20_vol"] if m[t]["avg20_vol"] is not None else avg_vol.get(t,0.0)
                except Exception: pass
        else:
            t=batch[0]; ms=m.get("__SINGLE__")
            if ms is None: return
            try:
                ck="Adj Close" if "Adj Close" in df.columns else "Close"
                c=df[ck].dropna()
                if len(c)>=2:
                    ok.add(t); last_px[t]=float(c.iloc[-1])
                    v=df["Volume"].dropna() if "Volume" in df.columns else pd.Series(dtype=float)
                    avg_vol[t]=float(v.mean()) if len(v)>0 else 0.0
                TA[t]=ms; PX[t]=ms["last_price"]; VOL[t]=ms["avg20_vol"] if ms["avg20_vol"] is not None else avg_vol.get(t,0.0)
            except Exception: pass

    for i in range(0,len(tickers),chunk):
        batch=tickers[i:i+chunk]; df=None
        for att in range(batch_retries):
            try:
                df=yf.download(batch, period=period, interval="1d", auto_adjust=False, progress=False, threads=threads)
            except Exception: df=None
            if isinstance(df,pd.DataFrame) and not df.empty: break
            time.sleep((1.5**att)+random.random()*0.3)
        if not isinstance(df,pd.DataFrame) or df.empty:
            # fallback singles with thread pool
            def fetch_one(t):
                sdf=None
                for a in range(single_retries+1):
                    try:
                        sdf=yf.download(t, period=period, interval="1d", auto_adjust=False, progress=False, threads=False)
                    except Exception: sdf=None
                    if isinstance(sdf,pd.DataFrame) and not sdf.empty: break
                    time.sleep((1.5**a)+random.random()*0.2)
                return t,sdf
            with ThreadPoolExecutor(max_workers=workers) as ex:
                futs=[ex.submit(fetch_one,t) for t in batch]
                for fu in as_completed(futs):
                    t,sdf=fu.result(); consume(sdf,[t])
        else:
            consume(df,batch)
        time.sleep(sleep)
    return TA,PX,VOL,ok

# ============== 상세 재무 유틸 ==============
REV_ALIASES = ["total revenue","revenues","revenue","net sales","sales","total net sales"]
OP_ALIASES  = ["operating income","operating income (loss)","income from operations","operating profit","operating profit (loss)","ebit"]
FCF_ALIASES = ["free cash flow","free cashflow","freecashflow"]
DA_ALIASES  = ["depreciation","depreciation & amortization","depreciation and amortization"]
EPS_ALIASES = ["diluted eps","basic eps","eps (diluted)","eps (basic)","earnings per share","eps"]
NET_INCOME_ALIASES = ["net income","net income common stockholders","net income applicable to common shares"]
DIL_SHARES_ALIASES = ["diluted average shares","weighted average shares diluted","weighted average diluted shares outstanding","weighted average diluted shares","weighted average shares - diluted","weighted average number of shares diluted"]
FIN_SECTORS  = {"banks","financial","insurance","capital markets"}
REIT_SECTORS = {"reit","real estate","property"}

def _find_row(index_like, aliases, exclude=None):
    if index_like is None: return None
    exclude=[w.lower() for w in (exclude or [])]
    idx=[str(x).lower() for x in index_like]
    for key in aliases:
        k=key.lower()
        for i,s in enumerate(idx):
            if k in s and not any(x in s for x in exclude):
                return index_like[i]
    return None

def coalesce(*vals):
    for v in vals:
        try:
            if v is None: continue
            if isinstance(v,float) and math.isnan(v): continue
            return v
        except Exception: continue
    return None

def ttm_sum(df: pd.DataFrame, row, n=4, absolute=False):
    if df is None or df.empty or row not in df.index or df.shape[1]<n: return None
    cols=sorted(df.columns, reverse=True)[:n]
    try:
        vals=pd.to_numeric(df.loc[row, cols], errors="coerce").fillna(0)
        return float(vals.abs().sum()) if absolute else float(vals.sum())
    except Exception: return None

def ttm_yoy_growth(df_q: pd.DataFrame, row):
    if df_q is None or df_q.empty or row not in df_q.index or df_q.shape[1]<8: return None
    cols=sorted(df_q.columns, reverse=True)
    try:
        curr=float(pd.to_numeric(df_q.loc[row, cols[:4]], errors="coerce").fillna(0).sum())
        prev=float(pd.to_numeric(df_q.loc[row, cols[4:8]], errors="coerce").fillna(0).sum())
    except Exception: return None
    if prev<=0: return None
    return (curr/prev)-1.0

def annual_yoy_growth(df_a: pd.DataFrame, row):
    if df_a is None or df_a.empty or row not in df_a.index or df_a.shape[1]<2: return None
    cols=sorted(df_a.columns, reverse=True)[:2]
    try:
        curr=float(pd.to_numeric(df_a.loc[row, cols[0]], errors="coerce"))
        prev=float(pd.to_numeric(df_a.loc[row, cols[1]], errors="coerce"))
    except Exception: return None
    if prev<=0: return None
    return (curr/prev)-1.0

def _last4_sum_row(df, aliases):
    if df is None or df.empty: return None
    row=_find_row(df.index, aliases)
    if not row or df.shape[1]<4: return None
    cols=sorted(df.columns, reverse=True)[:4]
    return float(pd.to_numeric(df.loc[row, cols], errors="coerce").fillna(0).sum())

def _last_col(df, aliases):
    if df is None or df.empty: return None
    row=_find_row(df.index, aliases)
    if not row: return None
    col=sorted(df.columns, reverse=True)[0]
    return float(pd.to_numeric(df.loc[row, col], errors="coerce"))

def _eps_ttm_from_statements(df_q, df_a):
    ni=_last4_sum_row(df_q, NET_INCOME_ALIASES)
    sh=_last_col(df_a, DIL_SHARES_ALIASES)
    if ni and sh and sh>0: return ni/sh
    return None

def safe_pe(price, info_dict, df_q, df_a):
    pe=coalesce(info_dict.get("trailingPE"), info_dict.get("forwardPE"))
    if pe is not None and isinstance(pe,(int,float)) and pe>0: return float(pe)
    teps=info_dict.get("trailingEps")
    if teps and isinstance(teps,(int,float)) and teps>0 and price: return float(price)/float(teps)
    eps_ttm=_eps_ttm_from_statements(df_q, df_a)
    if eps_ttm and eps_ttm>0 and price: return float(price)/float(eps_ttm)
    return None

def _parse_growth_to_pct(val):
    if val is None: return None
    try:
        if isinstance(val,str):
            s=val.strip().replace('%','').replace('+','')
            if s.lower() in {'n/a','na','nan','none','-',''}: return None
            return float(s)
        x=float(val); return x*100.0 if abs(x)<=1.0 else x
    except Exception: return None

def estimate_peg_from_earnings_trend(tic: yf.Ticker, pe_value):
    if pe_value is None or pe_value <= 0: return None
    et=None
    for attr in ("earnings_trend","get_earnings_trend"):
        try:
            et=getattr(tic,attr); et=et() if callable(et) else et; break
        except Exception: continue
    growth_pct=None
    if isinstance(et,pd.DataFrame) and ("period" in et.columns) and ("growth" in et.columns):
        for key in ["+5y","5y","next 5y","+1y","1y"]:
            row=et.loc[et["period"].astype(str).str.lower().str.contains(key,na=False)]
            if not row.empty:
                growth_pct=_parse_growth_to_pct(row["growth"].iloc[0]); break
    if growth_pct is None:
        try:
            info=tic.get_info() or {}
            g=info.get("earningsGrowth") or info.get("earningsQuarterlyGrowth")
            growth_pct=_parse_growth_to_pct(g)
        except Exception: pass
    if growth_pct and growth_pct>0:
        return float(pe_value)/float(growth_pct)
    return None

def get_eps_annual_series(tic: yf.Ticker):
    eps_vals=[]; df_a=None
    try:
        df_a=tic.income_stmt
        if df_a is None or df_a.empty: df_a=tic.financials
    except Exception: pass
    if df_a is not None and not df_a.empty:
        row_eps=_find_row(df_a.index, EPS_ALIASES)
        if row_eps:
            try:
                vals=pd.to_numeric(df_a.loc[row_eps], errors="coerce").dropna()
                return list(vals.sort_index().values)
            except Exception: pass
        else:
            ni_row=_find_row(df_a.index, NET_INCOME_ALIASES)
            sh_row=_find_row(df_a.index, DIL_SHARES_ALIASES)
            if ni_row and sh_row:
                try:
                    ni=pd.to_numeric(df_a.loc[ni_row], errors="coerce")
                    sh=pd.to_numeric(df_a.loc[sh_row], errors="coerce").replace(0,np.nan)
                    vals=(ni/sh).dropna()
                    return list(vals.sort_index().values)
                except Exception: pass
    try:
        earn=tic.earnings
        if earn is not None and not earn.empty:
            info={}
            try: info=tic.get_info() or {}
            except Exception: pass
            so=info.get("sharesOutstanding")
            if so and so>0:
                ser=pd.to_numeric(earn["Earnings"], errors="coerce")/float(so)
                return list(ser.sort_index().dropna().values)
    except Exception: pass
    return []

def eps_cagr_from_series(vals, min_years=3, max_years=5):
    v=[float(x) for x in vals if x is not None and not np.isnan(x)]
    if len(v)<min_years: return None
    use=v[-max_years:];
    if len(use)<min_years: return None
    first,last=use[0],use[-1]
    if first<=0 or last<=0: return None
    years=len(use)-1
    if years<=0: return None
    return (last/first)**(1.0/years)-1.0

def estimate_peg_from_eps_cagr(tic: yf.Ticker, pe_value, min_years=3, max_years=5):
    if pe_value is None or pe_value<=0: return None
    cagr=eps_cagr_from_series(get_eps_annual_series(tic), min_years, max_years)
    if cagr is None or cagr<=0: return None
    return float(pe_value)/(float(cagr)*100.0)

def fetch_details_for_ticker(tkr, price, avg_vol):
    """라이트 통과 종목에 대해 상세 재무 지표 수집"""
    try:
        t=yf.Ticker(tkr)
        info=t.get_info() or {}
    except Exception:
        info={}
    mktcap=info.get("marketCap")
    dollar_vol=(float(price)*float(avg_vol)) if (price is not None and avg_vol is not None) else None

    # 분기 손익/현금흐름/재무상태표
    def _safe_df(getter):
        try:
            df = getter()
            if df is not None and hasattr(df, "empty") and not df.empty:
                return df
        except Exception:
            pass
        return None

    q_is = _safe_df(lambda: t.quarterly_income_stmt) or _safe_df(lambda: t.quarterly_financials)
    a_is = _safe_df(lambda: t.income_stmt) or _safe_df(lambda: t.financials)
    cf_q = _safe_df(lambda: t.quarterly_cashflow)
    balance_q = _safe_df(lambda: t.quarterly_balance_sheet)
    balance_a = _safe_df(lambda: t.balance_sheet)

    # Rev/OpMargin/RevYoY
    rev_row = _find_row(q_is.index, REV_ALIASES,
                        exclude=["per share", "operating revenue", "royalty"]) if q_is is not None else None
    op_row = _find_row(q_is.index, OP_ALIASES) if q_is is not None else None

    rev_ttm = ttm_sum(q_is, rev_row, 4) if rev_row else None
    op_ttm = ttm_sum(q_is, op_row, 4) if op_row else None
    op_margin = (op_ttm / rev_ttm) if (op_ttm is not None and rev_ttm not in (None, 0)) else None

    # 개선된 YoY 계산
    rev_yoy = ttm_yoy_growth(q_is, rev_row) if rev_row else None
    if (rev_yoy is None) and (a_is is not None) and (rev_row in getattr(a_is, 'index', [])):
        rev_yoy = annual_yoy_growth(a_is, rev_row)

    # 개선된 EV/EBITDA 계산
    ev = info.get("enterpriseValue")
    ebitda = info.get("ebitda")

    # EBITDA 계산 개선: 여러 방법 시도
    if ebitda is None or (isinstance(ebitda, (int, float)) and ebitda <= 0):
        # 방법 1: 분기 데이터에서 계산
        if q_is is not None:
            op_row_q = _find_row(q_is.index, OP_ALIASES)
            da_row_q = _find_row(cf_q.index, DA_ALIASES) if cf_q is not None else None

            if op_row_q and da_row_q:
                op_ttm_q = ttm_sum(q_is, op_row_q, 4)
                da_ttm_q = ttm_sum(cf_q, da_row_q, 4) if da_row_q else None
                if op_ttm_q is not None and da_ttm_q is not None:
                    ebitda = op_ttm_q + da_ttm_q

        # 방법 2: 연간 데이터에서 계산
        if (ebitda is None or ebitda <= 0) and a_is is not None:
            op_row_a = _find_row(a_is.index, OP_ALIASES)
            da_row_a = _find_row(cf_q.index, DA_ALIASES) if cf_q is not None else None

            if op_row_a:
                try:
                    col = sorted(a_is.columns, reverse=True)[0]
                    op_ann = float(pd.to_numeric(a_is.loc[op_row_a, col], errors="coerce"))
                    da_ann = float(pd.to_numeric(cf_q.loc[da_row_a, col],
                                                 errors="coerce")) if da_row_a and col in cf_q.columns else 0
                    ebitda = op_ann + da_ann if op_ann else None
                except Exception:
                    pass

    ev_ebitda = float(ev) / float(ebitda) if (ev and ebitda and float(ebitda) > 0) else None

    # PER/PEG 계산
    pe = safe_pe(price, info, q_is, a_is)
    peg = info.get("pegRatio")
    if peg in (None, float("nan")) and pe is not None:
        peg = estimate_peg_from_earnings_trend(t, pe) or estimate_peg_from_eps_cagr(t, pe, 3, 5)

    # 개선된 FCF Yield 계산
    fcf_yield = None
    if isinstance(cf_q, pd.DataFrame):
        fcf_row = _find_row(cf_q.index, FCF_ALIASES)
        if not fcf_row:
            # FCF = Operating Cash Flow - Capital Expenditure
            op_cf_row = _find_row(cf_q.index, ["operating cash flow", "cash from operations"])
            capex_row = _find_row(cf_q.index, ["capital expenditure", "purchase of property", "capital expenditures"])
            if op_cf_row and capex_row:
                op_cf_ttm = ttm_sum(cf_q, op_cf_row, 4)
                capex_ttm = ttm_sum(cf_q, capex_row, 4, absolute=True)
                if op_cf_ttm is not None and capex_ttm is not None:
                    fcf_ttm = op_cf_ttm - capex_ttm
                    fcf_yield = (float(fcf_ttm) / float(ev)) if (fcf_ttm is not None and ev and float(ev) > 0) else None
        else:
            fcf_ttm = ttm_sum(cf_q, fcf_row, 4)
            fcf_yield = (float(fcf_ttm) / float(ev)) if (fcf_ttm is not None and ev and float(ev) > 0) else None

    # 부채비율 계산 추가
    debt_equity = None
    if balance_a is not None:
        debt_row = _find_row(balance_a.index, ["total debt", "total liabilities", "long term debt"])
        equity_row = _find_row(balance_a.index, ["total equity", "shareholders equity"])

        if debt_row and equity_row:
            try:
                col = sorted(balance_a.columns, reverse=True)[0]
                total_debt = float(pd.to_numeric(balance_a.loc[debt_row, col], errors="coerce"))
                total_equity = float(pd.to_numeric(balance_a.loc[equity_row, col], errors="coerce"))
                if total_equity and total_equity > 0:
                    debt_equity = total_debt / total_equity
            except Exception:
                pass

    # 배당 관련 지표 강화
    div_yield = info.get("dividendYield") or info.get("trailingAnnualDividendYield")
    if div_yield and isinstance(div_yield, float):
        div_yield = div_yield  # 이미 %로 되어 있음
    elif div_yield and isinstance(div_yield, str):
        try:
            div_yield = float(div_yield.strip('%')) / 100
        except:
            div_yield = None

    # 5년 평균 ROE 계산 시도
    roe_5y = None
    if a_is is not None:
        net_income_row = _find_row(a_is.index, NET_INCOME_ALIASES)
        equity_row = _find_row(a_is.index, ["total equity", "shareholders equity"])

        if net_income_row and equity_row and a_is.shape[1] >= 5:
            try:
                roe_values = []
                for col in sorted(a_is.columns, reverse=True)[:5]:
                    ni = float(pd.to_numeric(a_is.loc[net_income_row, col], errors="coerce"))
                    eq = float(pd.to_numeric(a_is.loc[equity_row, col], errors="coerce"))
                    if ni and eq and eq > 0:
                        roe_values.append(ni / eq)

                if len(roe_values) >= 3:  # 최소 3년치 데이터
                    roe_5y = sum(roe_values) / len(roe_values)
            except Exception:
                pass

    # Buyback Yield(절대값)
    buyback_yield=None
    if isinstance(cf_q,pd.DataFrame):
        try:
            buy_rows=["repurchase of capital stock","purchase of treasury stock","common stock repurchased","repurchases of common stock","stock repurchased","share repurchases","stock buyback"]
            row=_find_row(cf_q.index, buy_rows)
            buy_ttm=ttm_sum(cf_q, row,4, absolute=True) if row else None
            buyback_yield=(float(buy_ttm)/float(mktcap)) if (buy_ttm is not None and mktcap) else None
        except Exception: pass

    rec = {
        "Ticker": tkr,
        "Name": info.get("longName") or info.get("shortName") or tkr,
        "Sector": info.get("sector"),
        "Industry": info.get("industry"),
        "MktCap($B)": round((mktcap or 0) / 1_000_000_000, 2) if mktcap else None,
        "Price": round(price, 2) if price is not None else None,
        "DollarVol($M)": round((dollar_vol or 0) / 1_000_000, 2) if dollar_vol is not None else None,

        # 재무 건강성 지표 추가
        "Debt_to_Equity": debt_equity,
        "ROE_5Y_Avg": roe_5y,

        # 라이트(TA) 필드
        "SMA20": None,  # 외부에서 채워짐
        "SMA50": None,
        "ATR_PCT": None,
        "RVOL": None,
        "RET5": None,
        "RET20": None,

        # 상세 재무
        "RevYoY": rev_yoy,
        "OpMarginTTM": op_margin,
        "OperatingMargins(info)": info.get("operatingMargins"),
        "ROE(info)": info.get("returnOnEquity"),
        "EV_EBITDA": ev_ebitda,
        "PE": pe,
        "PEG": peg,
        "FCF_Yield": fcf_yield,
        "PB": info.get("priceToBook") or info.get("priceToTangBook") or info.get("priceToBookRatio"),
        "DivYield": div_yield,
        "PayoutRatio": info.get("payoutRatio"),
    }
    return rec

# ============== 라이트 컷 함수(캐시 단계에서 후보 축소용) ==============
def pass_light_generic(price, dollar_vol):
    if price is None or dollar_vol is None: return False
    return (price >= CONFIG["MIN_PRICE"]) and (dollar_vol >= CONFIG["MIN_DOLLAR_VOLUME"])

def build_details_cache():
    source = CONFIG["UNIVERSE_SOURCE"]
    tickers = load_universe()

    # OHLCV 라이트 지표
    TA,PX,VOL,ok = preload_ohlcv_light(
        tickers,
        period=CONFIG["PRELOAD_PERIOD"],
        chunk=CONFIG["PRELOAD_CHUNK"],
        batch_retries=CONFIG["BATCH_RETRIES"],
        single_retries=CONFIG["SINGLE_RETRIES"],
        workers=CONFIG["FALLBACK_MAX_WORKERS"],
        threads=CONFIG["YF_THREADS"],
        sleep=CONFIG["SLEEP_SEC"]
    )
    if not ok:
        raise RuntimeError("OHLCV 라이트 프리로드 실패(빈 결과)")

    # 라이트 표 생성(전 종목)
    lite_rows=[]
    for t in tickers:
        tta=TA.get(t,{})
        price=PX.get(t); avg20=VOL.get(t)
        if price is None or avg20 is None: continue
        dollar_vol=price*avg20
        row={
            "Ticker": t,
            "Price": round(price,2),
            "DollarVol($M)": round(dollar_vol/1_000_000,2),
            "SMA20": tta.get("sma20"),
            "SMA50": tta.get("sma50"),
            "ATR_PCT": tta.get("atr_pct"),
            "RVOL": tta.get("rvol"),
            "RET5": tta.get("ret5"),
            "RET20": tta.get("ret20"),
        }
        lite_rows.append(row)
    lite_df=pd.DataFrame(lite_rows)
    if lite_df.empty: raise RuntimeError("라이트 지표 표가 비어 있음")

    # 상세 호출 대상(라이트 컷 통과 + DollarVol 상위 K)
    lite_df["_pass_light_generic"]=lite_df.apply(lambda r: pass_light_generic(r["Price"], r["DollarVol($M)"]*1_000_000), axis=1)
    cand = lite_df[lite_df["_pass_light_generic"]].sort_values("DollarVol($M)", ascending=False).head(CONFIG["DETAILED_TOP_K"])

    print(f"[Details] candidates: {len(cand)} / light total: {len(lite_df)}")

    # 상세 재무 수집(순차+가벼운 sleep)
    detail_rows=[]
    for i,(t,row) in enumerate(cand.set_index("Ticker").iterrows(), start=1):
        try:
            rec=fetch_details_for_ticker(t, price=row["Price"], avg_vol=(row["DollarVol($M)"]*1_000_000)/max(1e-9,row["Price"]))
            # 라이트 필드 병합
            rec.update({
                "SMA20": row.get("SMA20"),
                "SMA50": row.get("SMA50"),
                "ATR_PCT": row.get("ATR_PCT"),
                "RVOL": row.get("RVOL"),
                "RET5": row.get("RET5"),
                "RET20": row.get("RET20"),
            })
            detail_rows.append(rec)
            if (i%50)==0: print(f"  - collected {i} / {len(cand)}")
        except Exception:
            continue
        time.sleep(0.05 + random.random()*0.05)

    details_df=pd.DataFrame(detail_rows)

    # 라이트만 있는 종목도 함께 저장(상세 없음) → 나중에 캐시 기반 트레이딩 프로파일 가능
    out = pd.merge(
        lite_df.drop(columns=["_pass_light_generic"]),
        details_df,
        on="Ticker", how="left",
        suffixes=("","")
    )

    # 포맷 정리
    for c in ["RevYoY","OpMarginTTM","OperatingMargins(info)","ROE(info)","FCF_Yield","DivYield"]:
        if c in out.columns:
            out[c] = out[c].astype(float)

    out["CreatedAtUTC"]=datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    out["Source"]=source

    base = CONFIG["OUT_BASENAME"].strip() or f"details_cache_{source}"
    csv_path = f"{base}.csv"
    out.to_csv(csv_path, index=False)
    print(f"[Cache] saved: {csv_path} (rows={len(out)})")

    if CONFIG["INCLUDE_EXCEL"]:
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
        if engine:
            xlsx_path=f"{base}.xlsx"
            with pd.ExcelWriter(xlsx_path, engine=engine) as w:
                out.to_excel(w, index=False, sheet_name="details_cache")
            print(f"[Cache] saved: {xlsx_path}")
        else:
            print("[Cache] Excel 저장 생략(XlsxWriter/openpyxl 미설치).")

if __name__ == "__main__":
    build_details_cache()
