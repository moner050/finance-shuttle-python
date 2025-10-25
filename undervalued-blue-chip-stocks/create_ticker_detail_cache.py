# -*- coding: utf-8 -*-
"""
build_details_cache.py

유니버스(US_ALL/SP500/커스텀)를 불러와 OHLCV(기본 120d)에서 라이트 지표(Price, DollarVol, RVOL, ATR_PCT 등)를 전종목 산출
→ 라이트 컷 통과 종목(및 상위 DETAILED_TOP_K)에 한해 재무 지표(RevYoY, OpMarginTTM, EV/EBITDA, FCFY 등)까지 수집
→ 단일 캐시 파일(details_cache_{source}.csv / .xlsx)에 저장

권장 설치:
  pip install -U yfinance==0.2.43 pandas numpy XlsxWriter openpyxl requests matplotlib

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

warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=UserWarning)
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
    "PRELOAD_CHUNK": 50,  # 청크 크기 줄임
    "BATCH_RETRIES": 5,  # 재시도 횟수 증가
    "SINGLE_RETRIES": 3,
    "FALLBACK_MAX_WORKERS": 8,  # 워커 수 줄임
    "YF_THREADS": False,  # 스레드 비활성화 (안정성)
    "SLEEP_SEC": 0.25,  # 대기 시간 증가

    # 네트워크 설정
    "REQUEST_TIMEOUT": 60,
    "PROXY_SETTINGS": {},  # {'http': 'http://proxy:port', 'https': 'https://proxy:port'}

    # 라이트 컷(라이트 통과 종목만 상세 재무 호출)
    "MIN_PRICE": 1.0,
    "MIN_DOLLAR_VOLUME": 900_000,

    # 상세 재무 호출 대상 범위
    "DETAILED_TOP_K": 12000,  # 상위 K 줄임
    "MAX_TICKERS": 12000,  # 최대 티커 수 줄임 (처리 속도 향상)
    "UNIVERSE_OFFSET": 0,
    "SHUFFLE_UNIVERSE": True,

    # 버핏형 하드컷 기본선
    "MIN_MKTCAP": 800_000_000,

    # 요청 헤더
    "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}
# ==================================================

HEADERS = {"User-Agent": CONFIG["USER_AGENT"]}
HTTP_SESSION = requests.Session()
HTTP_SESSION.headers.update(HEADERS)

# 세션 설정
session = requests.Session()
session.headers.update({"User-Agent": CONFIG["USER_AGENT"]})
if CONFIG["PROXY_SETTINGS"]:
    session.proxies.update(CONFIG["PROXY_SETTINGS"])

def _normalize_ticker(t):
    return str(t).strip().upper().replace(".", "-")

def _read_html(url: str):
    try:
        r = session.get(url, timeout=CONFIG["REQUEST_TIMEOUT"])
        r.raise_for_status()
        return pd.read_html(io.StringIO(r.text))
    except Exception as e:
        print(f"HTML 읽기 실패 {url}: {e}")
        return []


def get_sp500_symbols():
    """S&P 500 종목 리스트 가져오기"""
    urls = [
        "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
        "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"
    ]

    for url in urls:
        try:
            if "wikipedia" in url:
                tables = _read_html(url)
                if tables:
                    df = tables[0]
                    col = next((c for c in df.columns if str(c).lower().startswith("symbol")), "Symbol")
                    syms = df[col].dropna().astype(str).tolist()
                    print(f"[S&P500] Wikipedia에서 {len(syms)}개 종목 로드")
                    return [_normalize_ticker(s) for s in syms]
            else:
                r = session.get(url, timeout=CONFIG["REQUEST_TIMEOUT"])
                r.raise_for_status()
                df = pd.read_csv(io.StringIO(r.text))
                if 'Symbol' in df.columns:
                    syms = df['Symbol'].dropna().astype(str).tolist()
                    print(f"[S&P500] GitHub에서 {len(syms)}개 종목 로드")
                    return [_normalize_ticker(s) for s in syms]
        except Exception as e:
            print(f"[S&P500] {url} 실패: {e}")
            continue

    # 폴백: 하드코딩된 주요 S&P 500 종목
    fallback_sp500 = ['AAPL', 'MSFT', 'AMZN', 'GOOGL', 'GOOG', 'TSLA', 'BRK-B', 'UNH', 'JNJ', 'XOM',
                      'JPM', 'V', 'NVDA', 'PG', 'MA', 'HD', 'CVX', 'LLY', 'ABBV', 'PFE']
    print(f"[S&P500] 폴백: {len(fallback_sp500)}개 주요 종목 사용")
    return fallback_sp500

def _fetch_text(url):
    try:
        r = session.get(url, timeout=CONFIG["REQUEST_TIMEOUT"], allow_redirects=True)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"텍스트 가져오기 실패 {url}: {e}")
        return ""

def _read_pipe_text_to_df(text: str) -> pd.DataFrame:
    try:
        return pd.read_csv(io.StringIO(text), sep="|")
    except Exception as e:
        print(f"파이프 텍스트 읽기 실패: {e}")
        return pd.DataFrame()


def _normalize_symbol_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    cols = {c.lower(): c for c in df.columns}
    sym = cols.get("symbol") or cols.get("act symbol") or cols.get("nasdaq symbol") or list(df.columns)[0]

    out = df.copy()
    out.rename(columns={sym: "Symbol"}, inplace=True)
    out["Symbol"] = out["Symbol"].astype(str).str.upper().str.replace(".", "-", regex=False)

    # 기본 필터링
    if "TestIssue" in out.columns:
        mask_test = out["TestIssue"].astype(str).str.upper().ne("Y")
        out = out[mask_test]

    if "FinancialStatus" in out.columns:
        fin_s = out["FinancialStatus"].astype(str).str.upper()
        mask_fin = (~fin_s.isin(["D", "E", "H", "S", "C", "T"]))
        out = out[mask_fin]

    return out


def _filter_common_stock(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    name_str = df.get("SecurityName", pd.Series([""] * len(df))).astype(str).str.lower()

    is_common_kw = name_str.str.contains(
        r"common stock|ordinary shares|class [ab]\s+common|shs",
        regex=True, na=False
    )
    is_deriv_kw = name_str.str.contains(
        r"warrant|right|unit|preferred|preference|pref|etf|fund|trust|note|debenture|bond|adr|adr\.",
        regex=True, na=False
    )

    base = df[is_common_kw & ~is_deriv_kw]
    return base if not base.empty else df[~is_deriv_kw]


def get_all_us_listed_common():
    """모든 미국 상장 주식 종목 가져오기 - 개선된 버전"""
    urls = [
        "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt",
        "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt",
        "https://old.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download",
        "https://old.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nyse&render=download",
    ]

    dfs = []
    fetched_count = 0

    for u in urls:
        try:
            txt = _fetch_text(u)
            if not txt:
                continue

            df = _normalize_symbol_df(_read_pipe_text_to_df(txt))
            if not df.empty:
                dfs.append(df)
                fetched_count += len(df)
                print(f"[US_ALL] {u}에서 {len(df)}개 종목 로드")

        except Exception as e:
            print(f"[US_ALL] {u} 건너뜀: {e}")
            continue

    if not dfs:
        print("[US_ALL] 모든 소스 실패, 폴백 종목 사용")
        # 기본 폴백 종목
        fallback_tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'BRK-B', 'V', 'JNJ', 'WMT',
                            'PG', 'JPM', 'UNH', 'HD', 'DIS', 'PYPL', 'NFLX', 'ADBE', 'CRM', 'INTC']
        return fallback_tickers

    df_combined = pd.concat(dfs, ignore_index=True)
    df_combined = _filter_common_stock(df_combined)

    syms = df_combined["Symbol"].dropna().unique().tolist()
    print(f"[US_ALL] 필터 후 총 {len(syms)}개 종목")
    return sorted(syms)


def load_universe():
    """유니버스 로드 - 안정성 개선"""
    src = CONFIG["UNIVERSE_SOURCE"]

    try:
        if src == "sp500":
            u = get_sp500_symbols()
        elif src == "us_all":
            u = get_all_us_listed_common()
        elif src == "custom":
            u = [_normalize_ticker(x) for x in CONFIG["CUSTOM_TICKERS"]]
        else:
            raise ValueError("UNIVERSE_SOURCE는 us_all, sp500, custom 중 하나여야 합니다")

        if CONFIG["SHUFFLE_UNIVERSE"]:
            random.shuffle(u)

        if CONFIG["MAX_TICKERS"]:
            u = u[CONFIG["UNIVERSE_OFFSET"]:CONFIG["UNIVERSE_OFFSET"] + CONFIG["MAX_TICKERS"]]
        elif CONFIG["UNIVERSE_OFFSET"]:
            u = u[CONFIG["UNIVERSE_OFFSET"]:]

        print(f"[유니버스] {src} 총={len(u)}개 샘플={u[:8]}")
        return u

    except Exception as e:
        print(f"[유니버스] 로드 실패: {e}")
        # 기본 종목으로 폴백
        return ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'BRK-B', 'JNJ', 'JPM', 'V']

# ============== OHLCV → 라이트 지표 ==============

def _compute_ta_single(c, h, l, v):
    """단일 종목 기술적 지표 계산"""
    try:
        if c is None or len(c.dropna()) < 5:
            return None

        c = c.dropna()
        if len(c) == 0:
            return None

        # 기본 지표 계산
        last_close = float(c.iloc[-1])

        # 이동평균
        s20 = c.rolling(20).mean().iloc[-1] if len(c) >= 20 else None
        s50 = c.rolling(50).mean().iloc[-1] if len(c) >= 50 else None

        # 수익률
        ret5 = c.pct_change(5).iloc[-1] if len(c) >= 6 else None
        ret20 = c.pct_change(20).iloc[-1] if len(c) >= 21 else None

        # 거래량 지표
        avg20_vol = today_vol = rvol = None
        if v is not None and len(v.dropna()) > 0:
            v_clean = v.dropna()
            avg20_vol = float(v_clean.rolling(20).mean().iloc[-1]) if len(v_clean) >= 20 else float(v_clean.mean())
            today_vol = float(v_clean.iloc[-1]) if len(v_clean) > 0 else None
            rvol = today_vol / avg20_vol if avg20_vol and avg20_vol > 0 else 1.0

        # ATR
        atr = atr_pct = None
        if h is not None and l is not None and len(h.dropna()) > 0 and len(l.dropna()) > 0:
            h_clean, l_clean = h.dropna(), l.dropna()
            if len(h_clean) >= 2 and len(l_clean) >= 2:
                prev_close = c.shift(1)
                tr = pd.concat([
                    h_clean - l_clean,
                    (h_clean - prev_close).abs(),
                    (l_clean - prev_close).abs()
                ], axis=1).max(axis=1)
                atr = float(tr.rolling(14).mean().iloc[-1]) if len(tr) >= 14 else None
                atr_pct = (atr / last_close) if atr and last_close > 0 else None

        return {
            "last_price": last_close,
            "sma20": float(s20) if s20 else None,
            "sma50": float(s50) if s50 else None,
            "ret5": float(ret5) if ret5 else None,
            "ret20": float(ret20) if ret20 else None,
            "avg20_vol": avg20_vol,
            "today_vol": today_vol,
            "rvol": rvol,
            "atr": atr,
            "atr_pct": atr_pct
        }
    except Exception as e:
        print(f"TA 계산 실패: {e}")
        return None

def _compute_ta_metrics(df):
    """DataFrame에서 기술적 지표 계산"""
    out = {}

    try:
        if isinstance(df.columns, pd.MultiIndex):
            # 멀티인덱스 (배치 다운로드)
            fields = set(df.columns.get_level_values(0))
            tickers = sorted(set(df.columns.get_level_values(1)))

            close_col = "Adj Close" if "Adj Close" in fields else "Close"

            for t in tickers:
                try:
                    if (close_col, t) not in df.columns:
                        continue

                    c = df[(close_col, t)].dropna()
                    h = df[("High", t)].dropna() if ("High", t) in df.columns else None
                    l = df[("Low", t)].dropna() if ("Low", t) in df.columns else None
                    v = df[("Volume", t)].dropna() if ("Volume", t) in df.columns else None

                    metrics = _compute_ta_single(c, h, l, v)
                    if metrics:
                        out[t] = metrics
                except Exception:
                    continue
        else:
            # 단일 인덱스
            close_col = "Adj Close" if "Adj Close" in df.columns else "Close"
            c = df[close_col] if close_col in df.columns else None
            h = df["High"] if "High" in df.columns else None
            l = df["Low"] if "Low" in df.columns else None
            v = df["Volume"] if "Volume" in df.columns else None

            metrics = _compute_ta_single(c, h, l, v)
            if metrics:
                out["__SINGLE__"] = metrics

    except Exception as e:
        print(f"TA 메트릭스 계산 실패: {e}")

    return out

def safe_yf_download(tickers, **kwargs):
    """안전한 yfinance 다운로드"""
    max_retries = kwargs.pop('max_retries', 3)
    timeout = kwargs.pop('timeout', 30)

    for attempt in range(max_retries):
        try:
            data = yf.download(tickers, **kwargs)
            if data is not None and not data.empty:
                return data
        except Exception as e:
            print(f"yfinance 다운로드 시도 {attempt + 1}/{max_retries} 실패: {e}")
            if attempt < max_retries - 1:
                sleep_time = (2 ** attempt) + random.random()
                print(f"{sleep_time:.1f}초 후 재시도...")
                time.sleep(sleep_time)

    return None


def preload_ohlcv_light(tickers, period="120d", chunk=50, **kwargs):
    """OHLCV 데이터 프리로드 - 개선된 버전"""
    TA, PX, VOL = {}, {}, {}
    ok_tickers = set()

    print(f"[OHLCV] {len(tickers)}개 종목 로드 시작...")

    for i in range(0, len(tickers), chunk):
        batch = tickers[i:i + chunk]
        batch_name = f"{i + 1}-{min(i + chunk, len(tickers))}"

        print(f"[OHLCV] 배치 {batch_name} 처리 중...")

        # 배치 다운로드 시도
        batch_data = None
        for attempt in range(CONFIG["BATCH_RETRIES"]):
            try:
                batch_data = safe_yf_download(
                    batch,
                    period=period,
                    interval="1d",
                    auto_adjust=False,
                    progress=False,
                    threads=CONFIG["YF_THREADS"],
                    timeout=30
                )
                if batch_data is not None and not batch_data.empty:
                    break
            except Exception as e:
                print(f"배치 {batch_name} 시도 {attempt + 1} 실패: {e}")
                time.sleep((1.5 ** attempt) + random.random())

        processed_in_batch = 0

        if batch_data is not None and not batch_data.empty:
            # 배치 데이터 처리
            metrics = _compute_ta_metrics(batch_data)

            if isinstance(batch_data.columns, pd.MultiIndex):
                # 멀티인덱스 처리
                close_col = "Adj Close" if "Adj Close" in set(batch_data.columns.get_level_values(0)) else "Close"

                for t in batch:
                    try:
                        if (close_col, t) not in batch_data.columns:
                            continue

                        prices = batch_data[(close_col, t)].dropna()
                        if len(prices) < 5:
                            continue

                        last_price = float(prices.iloc[-1])

                        # 거래량 계산
                        avg_vol = 0
                        if ("Volume", t) in batch_data.columns:
                            vols = batch_data[("Volume", t)].dropna()
                            avg_vol = float(vols.rolling(20).mean().iloc[-1]) if len(vols) >= 20 else float(vols.mean())

                        ok_tickers.add(t)
                        PX[t] = last_price
                        VOL[t] = avg_vol

                        if t in metrics:
                            TA[t] = metrics[t]
                        else:
                            # 메트릭스가 없으면 기본값 생성
                            TA[t] = {
                                "last_price": last_price,
                                "sma20": last_price,
                                "sma50": last_price,
                                "ret5": 0.0,
                                "ret20": 0.0,
                                "avg20_vol": avg_vol,
                                "rvol": 1.0,
                                "atr_pct": 0.02
                            }

                        processed_in_batch += 1

                    except Exception as e:
                        print(f"종목 {t} 처리 실패: {e}")
                        continue
            else:
                # 단일 종목 처리
                if batch and len(batch) == 1:
                    t = batch[0]
                    try:
                        prices = batch_data[close_col].dropna() if close_col in batch_data.columns else None
                        if prices is None or len(prices) < 5:
                            continue

                        last_price = float(prices.iloc[-1])

                        # 거래량
                        avg_vol = 0
                        if "Volume" in batch_data.columns:
                            vols = batch_data["Volume"].dropna()
                            avg_vol = float(vols.rolling(20).mean().iloc[-1]) if len(vols) >= 20 else float(vols.mean())

                        ok_tickers.add(t)
                        PX[t] = last_price
                        VOL[t] = avg_vol

                        if "__SINGLE__" in metrics:
                            TA[t] = metrics["__SINGLE__"]
                        else:
                            TA[t] = {
                                "last_price": last_price,
                                "sma20": last_price,
                                "sma50": last_price,
                                "ret5": 0.0,
                                "ret20": 0.0,
                                "avg20_vol": avg_vol,
                                "rvol": 1.0,
                                "atr_pct": 0.02
                            }

                        processed_in_batch += 1

                    except Exception as e:
                        print(f"단일 종목 {t} 처리 실패: {e}")

        # 배치 실패 시 개별 다운로드
        if processed_in_batch == 0:
            print(f"배치 {batch_name} 실패, 개별 다운로드 시도...")

            def download_single(t):
                for attempt in range(CONFIG["SINGLE_RETRIES"]):
                    try:
                        data = safe_yf_download(
                            t,
                            period=period,
                            interval="1d",
                            auto_adjust=False,
                            progress=False,
                            threads=False,
                            timeout=30
                        )
                        if data is not None and not data.empty:
                            return t, data
                    except Exception:
                        time.sleep((1.5 ** attempt) + random.random() * 0.3)
                return t, None

            # 스레드 풀 사용
            with ThreadPoolExecutor(max_workers=CONFIG["FALLBACK_MAX_WORKERS"]) as executor:
                futures = [executor.submit(download_single, t) for t in batch]

                for future in as_completed(futures):
                    t, data = future.result()
                    if data is not None:
                        try:
                            metrics = _compute_ta_metrics(data)
                            if "__SINGLE__" in metrics:
                                close_col = "Adj Close" if "Adj Close" in data.columns else "Close"
                                prices = data[close_col].dropna()

                                if len(prices) >= 5:
                                    last_price = float(prices.iloc[-1])

                                    # 거래량
                                    avg_vol = 0
                                    if "Volume" in data.columns:
                                        vols = data["Volume"].dropna()
                                        avg_vol = float(vols.rolling(20).mean().iloc[-1]) if len(vols) >= 20 else float(
                                            vols.mean())

                                    ok_tickers.add(t)
                                    PX[t] = last_price
                                    VOL[t] = avg_vol
                                    TA[t] = metrics["__SINGLE__"]
                                    processed_in_batch += 1
                        except Exception as e:
                            print(f"개별 종목 {t} 처리 실패: {e}")

        print(f"[OHLCV] 배치 {batch_name} 완료: {processed_in_batch}/{len(batch)}개 성공")
        time.sleep(CONFIG["SLEEP_SEC"])

    print(f"[OHLCV] 전체 완료: {len(ok_tickers)}/{len(tickers)}개 종목 성공")
    return TA, PX, VOL, ok_tickers

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

def _safe_df(getter):
    """
    수정된 버전: DataFrame boolean 평가 문제 해결
    """
    try:
        df = getter()
        # 명시적으로 DataFrame 존재 여부 확인
        if df is not None and hasattr(df, 'empty') and not df.empty:
            return df
    except Exception as e:
        # 에러 로깅 (선택사항)
        pass
    return None

def safe_pe(price, info_dict, df_q, df_a):
    """
    PER 계산 함수 보완
    """
    try:
        pe = coalesce(info_dict.get("trailingPE"), info_dict.get("forwardPE"))
        if pe is not None and isinstance(pe, (int, float)) and pe > 0:
            return float(pe)

        teps = info_dict.get("trailingEps")
        if teps and isinstance(teps, (int, float)) and teps > 0 and price:
            return float(price) / float(teps)

        # 수정: DataFrame 안전성 확인
        if df_q is not None and df_a is not None:
            eps_ttm = _eps_ttm_from_statements(df_q, df_a)
            if eps_ttm and eps_ttm > 0 and price:
                return float(price) / float(eps_ttm)

        return None
    except Exception:
        return None


def fetch_details_for_ticker(tkr, price, avg_vol):
    """
    수정된 상세 데이터 수집 함수 - 에러 처리 강화
    """
    try:
        t = yf.Ticker(tkr)
        info = t.get_info() or {}
    except Exception as e:
        # 초기 정보 수집 실패 시 기본 정보 반환
        return {
            "Ticker": tkr,
            "Name": tkr,
            "Sector": None,
            "Industry": None,
            "MktCap($B)": None,
            "Price": round(price, 2) if price is not None else None,
            "DollarVol($M)": None,
            "RevYoY": None,
            "OpMarginTTM": None,
            "OperatingMargins(info)": None,
            "ROE(info)": None,
            "EV_EBITDA": None,
            "PE": None,
            "PEG": None,
            "FCF_Yield": None,
            "PB": None,
            "DivYield": None,
            "PayoutRatio": None,
        }

    try:
        mktcap = info.get("marketCap")
        dollar_vol = (float(price) * float(avg_vol)) if (price is not None and avg_vol is not None) else None

        # 재무제표 데이터 수집 (에러 처리 강화)
        q_is = _safe_df(lambda: t.quarterly_income_stmt)
        if q_is is None:
            q_is = _safe_df(lambda: t.quarterly_financials)

        a_is = _safe_df(lambda: t.income_stmt)
        if a_is is None:
            a_is = _safe_df(lambda: t.financials)

        cf_q = _safe_df(lambda: t.quarterly_cashflow)
        balance_a = _safe_df(lambda: t.balance_sheet)

        # 매출/영업이익 데이터 (안전한 처리)
        rev_yoy = None
        op_margin = None

        # 수정: DataFrame 존재 여부 명시적 확인
        if q_is is not None and not q_is.empty:
            rev_row = _find_row(q_is.index, REV_ALIASES, exclude=["per share", "operating revenue", "royalty"])
            op_row = _find_row(q_is.index, OP_ALIASES)

            if rev_row:
                rev_ttm = ttm_sum(q_is, rev_row, 4)
                rev_yoy = ttm_yoy_growth(q_is, rev_row)

                # 연간 데이터로 폴백
                if rev_yoy is None and a_is is not None and not a_is.empty and rev_row in a_is.index:
                    rev_yoy = annual_yoy_growth(a_is, rev_row)

            if op_row and rev_ttm and rev_ttm > 0:
                op_ttm = ttm_sum(q_is, op_row, 4)
                if op_ttm:
                    op_margin = op_ttm / rev_ttm

        # EV/EBITDA 계산 (에러 처리 강화)
        ev = info.get("enterpriseValue")
        ebitda = info.get("ebitda")
        ev_ebitda = None

        try:
            if ev and ebitda and float(ebitda) > 0:
                ev_ebitda = float(ev) / float(ebitda)
        except (TypeError, ValueError):
            pass

        # PER/PEG 계산
        pe = safe_pe(price, info, q_is, a_is)
        peg = info.get("pegRatio")

        if (peg is None or math.isnan(peg)) and pe is not None:
            try:
                peg = estimate_peg_from_earnings_trend(t, pe) or estimate_peg_from_eps_cagr(t, pe, 3, 5)
            except Exception:
                peg = None

        # FCF Yield 계산
        fcf_yield = None
        if cf_q is not None and not cf_q.empty:
            fcf_row = _find_row(cf_q.index, FCF_ALIASES)
            if fcf_row:
                fcf_ttm = ttm_sum(cf_q, fcf_row, 4)
                if fcf_ttm and ev and float(ev) > 0:
                    fcf_yield = float(fcf_ttm) / float(ev)

        # 배당수익률 (안전한 처리)
        div_yield = None
        try:
            div_yield = info.get("dividendYield") or info.get("trailingAnnualDividendYield")
            if div_yield and isinstance(div_yield, str):
                div_yield = float(div_yield.strip('%')) / 100
        except (TypeError, ValueError, AttributeError):
            div_yield = None

        rec = {
            "Ticker": tkr,
            "Name": info.get("longName") or info.get("shortName") or tkr,
            "Sector": info.get("sector"),
            "Industry": info.get("industry"),
            "MktCap($B)": round((mktcap or 0) / 1_000_000_000, 2) if mktcap else None,
            "Price": round(price, 2) if price is not None else None,
            "DollarVol($M)": round((dollar_vol or 0) / 1_000_000, 2) if dollar_vol is not None else None,
            "RevYoY": rev_yoy,
            "OpMarginTTM": op_margin,
            "OperatingMargins(info)": info.get("operatingMargins"),
            "ROE(info)": info.get("returnOnEquity"),
            "EV_EBITDA": ev_ebitda,
            "PE": pe,
            "PEG": peg,
            "FCF_Yield": fcf_yield,
            "PB": info.get("priceToBook") or info.get("priceToBookRatio"),
            "DivYield": div_yield,
            "PayoutRatio": info.get("payoutRatio"),
        }
        return rec

    except Exception as e:
        # 상세 데이터 수집 중 에러 발생 시 기본 정보 반환
        print(f"종목 {tkr} 상세 데이터 수집 중 에러: {str(e)}")
        return {
            "Ticker": tkr,
            "Name": info.get("longName") or info.get("shortName") or tkr,
            "Sector": info.get("sector"),
            "Industry": info.get("industry"),
            "MktCap($B)": round((mktcap or 0) / 1_000_000_000, 2) if mktcap else None,
            "Price": round(price, 2) if price is not None else None,
            "DollarVol($M)": round((dollar_vol or 0) / 1_000_000, 2) if dollar_vol is not None else None,
            "RevYoY": None,
            "OpMarginTTM": None,
            "OperatingMargins(info)": None,
            "ROE(info)": None,
            "EV_EBITDA": None,
            "PE": None,
            "PEG": None,
            "FCF_Yield": None,
            "PB": None,
            "DivYield": None,
            "PayoutRatio": None,
        }


def build_details_cache():
    """
    수정된 캐시 빌드 함수 - 컬럼 중복 문제 해결
    """
    source = CONFIG["UNIVERSE_SOURCE"]
    tickers = load_universe()

    # OHLCV 라이트 지표 수집 (기존 코드 유지)
    TA, PX, VOL, ok = preload_ohlcv_light(
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

    # 라이트 표 생성
    lite_rows = []
    for t in tickers:
        tta = TA.get(t, {})
        price = PX.get(t)
        avg20 = VOL.get(t)
        if price is None or avg20 is None:
            continue
        dollar_vol = price * avg20
        row = {
            "Ticker": t,
            "Price": round(price, 2),
            "DollarVol($M)": round(dollar_vol / 1_000_000, 2),
            "SMA20": tta.get("sma20"),
            "SMA50": tta.get("sma50"),
            "ATR_PCT": tta.get("atr_pct"),
            "RVOL": tta.get("rvol"),
            "RET5": tta.get("ret5"),
            "RET20": tta.get("ret20"),
        }
        lite_rows.append(row)

    lite_df = pd.DataFrame(lite_rows)
    if lite_df.empty:
        raise RuntimeError("라이트 지표 표가 비어 있음")

    # 상세 호출 대상 선정 - 수정된 부분
    lite_df["_pass_light_generic"] = lite_df.apply(
        lambda r: pass_light_generic(r["Price"], r["DollarVol($M)"] * 1_000_000), axis=1
    )

    # 라이트 필터 통과한 종목 중에서만 상위 K개 선정
    passed_tickers = lite_df[lite_df["_pass_light_generic"]]
    print(f"라이트 필터 통과: {len(passed_tickers)}개")

    cand = passed_tickers.sort_values("DollarVol($M)", ascending=False).head(CONFIG["DETAILED_TOP_K"])
    print(f"상세 데이터 수집 대상: {len(cand)}개")
    print(f"[상세데이터] 후보: {len(cand)} / 라이트 총계: {len(lite_df)}")

    # 상세 재무 수집
    detail_rows = []
    success_count = 0

    for i, (t, row) in enumerate(cand.set_index("Ticker").iterrows(), start=1):
        try:
            rec = fetch_details_for_ticker(
                t,
                price=row["Price"],
                avg_vol=(row["DollarVol($M)"] * 1_000_000) / max(1e-9, row["Price"])
            )

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
            success_count += 1

        except Exception as e:
            print(f"종목 {t} 상세 데이터 수집 실패: {str(e)}")
            continue

        if (i % 50) == 0:
            print(f"  - {i}/{len(cand)} 완료 (성공: {success_count})")

        time.sleep(0.05 + random.random() * 0.05)

    print(f"[상세데이터] 최종 수집: {success_count}/{len(cand)} 종목")

    # 결과 병합 - 라이트 필터 통과한 전체 종목과 상세 데이터 병합
    details_df = pd.DataFrame(detail_rows)

    # 🔥 중요: 라이트 필터 통과한 전체 종목과 상세 데이터 LEFT JOIN
    out = pd.merge(
        passed_tickers.drop(columns=["_pass_light_generic"]),
        details_df,
        on="Ticker",
        how="left"  # 라이트 필터 통과한 모든 종목은 유지
    )

    print(f"최종 CSV 행 수: {len(out)} (라이트 필터 통과: {len(passed_tickers)})")

    # 방법 2: 또는 lite_df의 모든 데이터를 유지하면서 details_df의 데이터로 업데이트
    # out = lite_df.drop(columns=["_pass_light_generic"]).copy()
    # for col in details_df.columns:
    #     if col != "Ticker":
    #         out[col] = out["Ticker"].map(details_df.set_index("Ticker")[col])

    # 데이터 타입 정리
    for c in ["RevYoY", "OpMarginTTM", "OperatingMargins(info)", "ROE(info)", "FCF_Yield", "DivYield"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors='coerce')

    out["CreatedAtUTC"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    out["Source"] = source

    # 저장
    base = CONFIG["OUT_BASENAME"].strip() or f"details_cache_{source}"
    csv_path = f"{base}.csv"
    out.to_csv(csv_path, index=False)
    print(f"[캐시] 저장 완료: {csv_path} (행: {len(out)})")

    if CONFIG["INCLUDE_EXCEL"]:
        try:
            xlsx_path = f"{base}.xlsx"
            out.to_excel(xlsx_path, index=False)
            print(f"[캐시] 엑셀 저장: {xlsx_path}")
        except Exception as e:
            print(f"[캐시] 엑셀 저장 실패: {e}")

    return out

# ============== 라이트 컷 함수(캐시 단계에서 후보 축소용) ==============
def pass_light_generic(price, dollar_vol):
    if price is None or dollar_vol is None: return False
    return (price >= CONFIG["MIN_PRICE"]) and (dollar_vol >= CONFIG["MIN_DOLLAR_VOLUME"])

if __name__ == "__main__":
    build_details_cache()
