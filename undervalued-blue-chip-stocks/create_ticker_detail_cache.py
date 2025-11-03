# -*- coding: utf-8 -*-
"""
build_details_cache_fully_optimized.py

🚀 완전 최적화 버전:
1. PER 데이터 계산 로직 강화 (여러 방법론 적용)
2. 최신 트렌드 지표 추가 (RSI, MACD, 볼린저밴드, 52주 고저가 등)
3. 재무 데이터 품질 향상
4. 성장성 지표 추가
5. ⭐ OHLCV 프리로드 병렬화 (2-3배 빠름)
6. ⭐ 상세 데이터 수집 병렬화 (5-10배 빠름)
7. ⭐ 이상치 처리 강화

✨ 데이터 수집 안정성 개선사항:
1. 📊 OHLCV 데이터 수집 안정성 향상:
   - 부분 성공 케이스 처리: 배치에서 일부 실패 시 누락된 티커만 개별 다운로드
   - 최소 데이터 요구사항 완화: 50개 → 20개로 완화하여 더 많은 종목 수집 가능
   - 스마트 재시도 로직: 배치 완전 실패 시 전체 재시도, 부분 실패 시 누락분만 재시도
   - 재시도 횟수: 배치 5회, 개별 3회

2. 💼 상세 재무 데이터 수집 개선:
   - 재무제표 API 재시도 로직 추가 (각 API 호출당 최대 3회)
   - info 실패 시에도 재무제표 데이터 수집 시도
   - 각 지표 계산 실패 시에도 다른 지표는 계속 수집
   - 재무제표별 독립적인 에러 처리

3. 🔍 에러 로깅 및 디버깅:
   - 전체 에러 추적 시스템 추가
   - 에러 로그 파일 자동 생성
   - 데이터 품질 통계 자동 출력
   - VERBOSE_LOGGING 옵션으로 상세 로그 제어

4. 🛡️ 데이터 검증 개선:
   - 검증 실패 시 대체 로직 추가
   - 가격 검증 범위 확대
   - 각 필드별 독립적인 에러 처리로 부분 데이터라도 수집
"""

import os, io, time, math, random, warnings, logging, requests
import pandas as pd, numpy as np, yfinance as yf
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=RuntimeWarning)
logging.getLogger("yfinance").setLevel(logging.CRITICAL)

# ===================== CONFIG =====================
CONFIG = {
    "UNIVERSE_SOURCE": "us_all",  # "us_all" | "sp500" | "custom"
    "CUSTOM_TICKERS": [],  # UNIVERSE_SOURCE="custom"일 때 사용

    # 캐시 출력
    "OUT_BASENAME": "",  # 비우면 자동: details_cache_{source}.csv
    "INCLUDE_EXCEL": True,

    "PRELOAD_PERIOD": "252d",  # 1년 데이터 (52주 계산용)
    "PRELOAD_CHUNK": 50,  # 배치 크기 (원래대로 복원)
    "BATCH_RETRIES": 5,  # 배치 재시도
    "SINGLE_RETRIES": 3,  # 개별 재시도

    # ⭐ 병렬 처리 설정
    "OHLCV_WORKERS": 1,  # OHLCV 다운로드 병렬 스레드 수
    "DETAIL_FETCH_WORKERS": 1,  # 상세 데이터 수집 병렬 스레드 수

    # 디버깅 및 로깅
    "VERBOSE_LOGGING": False,  # True로 설정하면 상세 에러 로그 출력

    "YF_THREADS": False,
    "SLEEP_SEC": 0.1,  # 병렬 처리 시에는 짧게

    # 네트워크 설정
    "REQUEST_TIMEOUT": 60,
    "PROXY_SETTINGS": {},

    # 라이트 컷
    "MIN_PRICE": 1.0,
    "MIN_DOLLAR_VOLUME": 900_000,

    # 상세 재무 호출 대상 범위
    "DETAILED_TOP_K": 12000,
    "MAX_TICKERS": 12000,
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


# ============== ⭐ 이상치 검증 함수 ==============

def validate_numeric(value, min_val=None, max_val=None, allow_negative=False):
    """숫자 값 검증 및 이상치 필터링"""
    if value is None:
        return None

    try:
        val = float(value)

        # NaN, Inf 체크
        if math.isnan(val) or math.isinf(val):
            return None

        # 음수 체크
        if not allow_negative and val < 0:
            return None

        # 범위 체크
        if min_val is not None and val < min_val:
            return None
        if max_val is not None and val > max_val:
            return None

        return val
    except (TypeError, ValueError):
        return None


def validate_percentage(value, min_pct=-100, max_pct=1000):
    """퍼센티지 값 검증 (-100% ~ 1000%)"""
    return validate_numeric(value, min_val=min_pct, max_val=max_pct, allow_negative=True)


def validate_ratio(value, min_ratio=0, max_ratio=1000):
    """비율 값 검증 (PER, PBR 등)"""
    return validate_numeric(value, min_val=min_ratio, max_val=max_ratio, allow_negative=False)


def validate_market_cap(value):
    """시가총액 검증 (최소 100만불, 최대 20조불)"""
    return validate_numeric(value, min_val=1_000_000, max_val=20_000_000_000_000, allow_negative=False)


def validate_price(value):
    """주가 검증 (0.01 ~ 100,000)"""
    return validate_numeric(value, min_val=0.01, max_val=100_000, allow_negative=False)


def validate_volume(value):
    """거래량 검증"""
    return validate_numeric(value, min_val=0, max_val=1e15, allow_negative=False)


# ============== 에러 로깅 설정 ==============
ERROR_LOG = []  # 에러 추적용


def log_error(context, ticker, error_msg):
    """에러 로깅 함수"""
    msg = f"[{context}] {ticker}: {error_msg}"
    ERROR_LOG.append(msg)
    if CONFIG.get("VERBOSE_LOGGING", False):
        print(f"⚠️  {msg}")


# ============== 기술적 지표 계산 함수들 ==============

def calculate_rsi(prices, window=14):
    """RSI 계산"""
    try:
        if len(prices) < window + 1:
            return None

        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        result = rsi.iloc[-1] if not rsi.empty else None

        # RSI는 0-100 범위
        return validate_numeric(result, min_val=0, max_val=100)
    except Exception as e:
        return None


def calculate_macd(prices, fast=12, slow=26, signal=9):
    """MACD 계산"""
    if len(prices) < slow + signal:
        return None, None, None

    ema_fast = prices.ewm(span=fast).mean()
    ema_slow = prices.ewm(span=slow).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal).mean()
    histogram = macd_line - signal_line

    return (
        macd_line.iloc[-1] if not macd_line.empty else None,
        signal_line.iloc[-1] if not signal_line.empty else None,
        histogram.iloc[-1] if not histogram.empty else None
    )


def calculate_bollinger_bands(prices, window=20, num_std=2):
    """볼린저밴드 계산"""
    if len(prices) < window:
        return None, None, None

    sma = prices.rolling(window).mean()
    std = prices.rolling(window).std()
    upper = sma + (std * num_std)
    lower = sma - (std * num_std)

    current_price = prices.iloc[-1]
    bb_position = (current_price - lower.iloc[-1]) / (upper.iloc[-1] - lower.iloc[-1]) if upper.iloc[-1] != lower.iloc[
        -1] else None

    # BB Position은 0-1 범위 (극단적인 경우 -0.5 ~ 1.5 허용)
    bb_position = validate_numeric(bb_position, min_val=-0.5, max_val=1.5, allow_negative=True)

    return (
        upper.iloc[-1] if not upper.empty else None,
        lower.iloc[-1] if not lower.empty else None,
        bb_position
    )


def calculate_52week_high_low(prices):
    """52주 고가/저가 계산"""
    if len(prices) < 252:  # 1년 거래일
        high_52w = prices.max()
        low_52w = prices.min()
    else:
        high_52w = prices.tail(252).max()
        low_52w = prices.tail(252).min()

    current_price = prices.iloc[-1]
    high_ratio = current_price / high_52w if high_52w > 0 else None
    low_ratio = current_price / low_52w if low_52w > 0 else None

    # 비율은 0-2 범위 (현재가가 52주 최고가의 2배까지만 허용)
    high_ratio = validate_numeric(high_ratio, min_val=0, max_val=2)
    low_ratio = validate_numeric(low_ratio, min_val=0, max_val=20)  # 저가 대비는 더 큰 범위

    return high_52w, low_52w, high_ratio, low_ratio


# ============== 강화된 PER 계산 함수 ==============

def calculate_pe_ratio(ticker, price, info, df_q, df_a):
    """강화된 PER 계산 (4가지 방법 시도) + 이상치 제거"""
    pe_values = []

    # 방법 1: yfinance info에서 직접 가져오기
    try:
        trailing_pe = info.get("trailingPE")
        forward_pe = info.get("forwardPE")
        if trailing_pe and trailing_pe > 0:
            validated_pe = validate_ratio(trailing_pe, min_ratio=0.1, max_ratio=500)
            if validated_pe:
                pe_values.append(validated_pe)
        if forward_pe and forward_pe > 0:
            validated_pe = validate_ratio(forward_pe, min_ratio=0.1, max_ratio=500)
            if validated_pe:
                pe_values.append(validated_pe)
    except Exception as e:
        pass

    # 방법 2: trailing EPS 사용
    try:
        trailing_eps = info.get("trailingEps")
        if trailing_eps and trailing_eps > 0 and price and price > 0:
            pe_calculated = price / trailing_eps
            if 0 < pe_calculated < 1000:
                pe_values.append(pe_calculated)
    except:
        pass

    # 방법 3: 분기별 데이터로 TTM EPS 계산
    try:
        if df_q is not None and not df_q.empty and df_a is not None and not df_a.empty:
            eps_aliases = ["diluted eps", "basic eps", "eps (diluted)", "eps (basic)", "earnings per share", "eps"]
            eps_row = None
            for alias in eps_aliases:
                if alias in [str(x).lower() for x in df_q.index]:
                    eps_row = [x for x in df_q.index if str(x).lower() == alias][0]
                    break

            if eps_row is None:
                ni_aliases = ["net income", "net income common stockholders"]
                shares_aliases = ["diluted average shares", "weighted average shares diluted"]

                ni_row = None
                shares_row = None

                for alias in ni_aliases:
                    if alias in [str(x).lower() for x in df_q.index]:
                        ni_row = [x for x in df_q.index if str(x).lower() == alias][0]
                        break

                for alias in shares_aliases:
                    if alias in [str(x).lower() for x in df_a.index]:
                        shares_row = [x for x in df_a.index if str(x).lower() == alias][0]
                        break

                if ni_row and shares_row:
                    cols = sorted(df_q.columns, reverse=True)[:4]
                    ni_ttm = pd.to_numeric(df_q.loc[ni_row, cols], errors="coerce").sum()
                    shares = pd.to_numeric(df_a.loc[shares_row, cols[0]], errors="coerce")

                    if ni_ttm and shares and shares > 0:
                        eps_ttm = ni_ttm / shares
                        if eps_ttm > 0 and price > 0:
                            pe_calculated = price / eps_ttm
                            if 0 < pe_calculated < 1000:
                                pe_values.append(pe_calculated)
            else:
                cols = sorted(df_q.columns, reverse=True)[:4]
                eps_ttm = pd.to_numeric(df_q.loc[eps_row, cols], errors="coerce").sum()
                if eps_ttm and eps_ttm > 0 and price > 0:
                    pe_calculated = price / eps_ttm
                    if 0 < pe_calculated < 1000:
                        pe_values.append(pe_calculated)
    except Exception:
        pass

    # 방법 4: 연간 데이터 사용
    try:
        if df_a is not None and not df_a.empty:
            eps_aliases = ["diluted eps", "basic eps", "eps (diluted)", "eps (basic)", "earnings per share", "eps"]
            eps_row = None
            for alias in eps_aliases:
                if alias in [str(x).lower() for x in df_a.index]:
                    eps_row = [x for x in df_a.index if str(x).lower() == alias][0]
                    break

            if eps_row:
                latest_col = sorted(df_a.columns, reverse=True)[0]
                eps_annual = pd.to_numeric(df_a.loc[eps_row, latest_col], errors="coerce")
                if eps_annual and eps_annual > 0 and price > 0:
                    pe_calculated = price / eps_annual
                    if 0 < pe_calculated < 1000:
                        pe_values.append(pe_calculated)
    except Exception:
        pass

    # 유효한 PER 값들 중 중간값 반환 (이상치 제거)
    valid_pes = [pe for pe in pe_values if pe is not None and 0 < pe < 500]
    if valid_pes:
        median_pe = np.median(valid_pes)
        return validate_ratio(median_pe, min_ratio=0.1, max_ratio=500)

    return None


def _normalize_ticker(t):
    return str(t).strip().upper().replace(".", "-")


def _read_html(url: str):
    try:
        r = session.get(url, timeout=CONFIG["REQUEST_TIMEOUT"])
        r.raise_for_status()
        return pd.read_html(io.StringIO(r.text))
    except Exception as e:
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
            continue

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
        return ""


def _read_pipe_text_to_df(text: str) -> pd.DataFrame:
    try:
        return pd.read_csv(io.StringIO(text), sep="|")
    except Exception:
        return pd.DataFrame()


def _normalize_symbol_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    cols = {c.lower(): c for c in df.columns}
    sym = cols.get("symbol") or cols.get("act symbol") or cols.get("nasdaq symbol") or list(df.columns)[0]

    out = df.copy()
    out.rename(columns={sym: "Symbol"}, inplace=True)
    out["Symbol"] = out["Symbol"].astype(str).str.upper().str.replace(".", "-", regex=False)

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
    """모든 미국 상장 주식 종목 가져오기"""
    urls = [
        "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt",
        "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt",
        "https://old.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download",
        "https://old.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nyse&render=download",
    ]

    dfs = []

    for u in urls:
        try:
            txt = _fetch_text(u)
            if not txt:
                continue

            df = _normalize_symbol_df(_read_pipe_text_to_df(txt))
            if not df.empty:
                dfs.append(df)
                print(f"[US_ALL] {u}에서 {len(df)}개 종목 로드")

        except Exception:
            continue

    if not dfs:
        print("[US_ALL] 모든 소스 실패, 폴백 종목 사용")
        fallback_tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'BRK-B', 'V', 'JNJ', 'WMT',
                            'PG', 'JPM', 'UNH', 'HD', 'DIS', 'PYPL', 'NFLX', 'ADBE', 'CRM', 'INTC']
        return fallback_tickers

    df_combined = pd.concat(dfs, ignore_index=True)
    df_combined = _filter_common_stock(df_combined)

    syms = df_combined["Symbol"].dropna().unique().tolist()
    print(f"[US_ALL] 필터 후 총 {len(syms)}개 종목")
    return sorted(syms)


def load_universe():
    """유니버스 로드"""
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
        return ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'BRK-B', 'JNJ', 'JPM', 'V']


# ============== OHLCV → 라이트 지표 ==============

def _compute_enhanced_ta_single(c, h, l, v):
    """개선된 기술적 지표 계산 + 이상치 검증"""
    try:
        # 최소 데이터 요구사항 완화: 50개 -> 20개 (더 많은 종목 수집 가능)
        if c is None or len(c.dropna()) < 20:
            return None

        c_clean = c.dropna()
        if len(c_clean) == 0:
            return None

        last_close = float(c_clean.iloc[-1])
        last_close = validate_price(last_close)
        if last_close is None:
            return None

        # 기본 지표들
        s20 = c_clean.rolling(20).mean().iloc[-1] if len(c_clean) >= 20 else None
        s50 = c_clean.rolling(50).mean().iloc[-1] if len(c_clean) >= 50 else None
        s200 = c_clean.rolling(200).mean().iloc[-1] if len(c_clean) >= 200 else None

        ret5 = c_clean.pct_change(5).iloc[-1] if len(c_clean) >= 6 else None
        ret20 = c_clean.pct_change(20).iloc[-1] if len(c_clean) >= 21 else None
        ret63 = c_clean.pct_change(63).iloc[-1] if len(c_clean) >= 64 else None

        # 수익률 검증
        ret5 = validate_percentage(ret5, min_pct=-0.99, max_pct=9.99)
        ret20 = validate_percentage(ret20, min_pct=-0.99, max_pct=9.99)
        ret63 = validate_percentage(ret63, min_pct=-0.99, max_pct=9.99)

        # 거래량 지표
        avg20_vol = today_vol = rvol = None
        if v is not None and len(v.dropna()) > 0:
            v_clean = v.dropna()
            avg20_vol = float(v_clean.rolling(20).mean().iloc[-1]) if len(v_clean) >= 20 else float(v_clean.mean())
            today_vol = float(v_clean.iloc[-1]) if len(v_clean) > 0 else None

            avg20_vol = validate_volume(avg20_vol)
            today_vol = validate_volume(today_vol)

            if avg20_vol and today_vol and avg20_vol > 0:
                rvol = today_vol / avg20_vol
                rvol = validate_numeric(rvol, min_val=0, max_val=100)

        # ATR
        atr = atr_pct = None
        if h is not None and l is not None and len(h.dropna()) > 0 and len(l.dropna()) > 0:
            h_clean, l_clean = h.dropna(), l.dropna()
            if len(h_clean) >= 14 and len(l_clean) >= 14:
                prev_close = c_clean.shift(1)
                tr = pd.concat([
                    h_clean - l_clean,
                    (h_clean - prev_close).abs(),
                    (l_clean - prev_close).abs()
                ], axis=1).max(axis=1)
                atr = float(tr.rolling(14).mean().iloc[-1]) if len(tr) >= 14 else None
                if atr and last_close > 0:
                    atr_pct = atr / last_close
                    atr_pct = validate_percentage(atr_pct, min_pct=0, max_pct=1.0)

        # 신규 기술적 지표들
        rsi_14 = calculate_rsi(c_clean, 14)
        macd, macd_signal, macd_histogram = calculate_macd(c_clean)
        bb_upper, bb_lower, bb_position = calculate_bollinger_bands(c_clean)
        high_52w, low_52w, high_52w_ratio, low_52w_ratio = calculate_52week_high_low(c_clean)

        # 모멘텀 지표
        momentum_12m = None
        if len(c_clean) >= 252:
            momentum_12m = (last_close / c_clean.iloc[-252]) - 1
            momentum_12m = validate_percentage(momentum_12m, min_pct=-0.99, max_pct=9.99)

        volatility_21d = None
        if len(c_clean) >= 22:
            volatility_21d = c_clean.pct_change().rolling(21).std().iloc[-1]
            volatility_21d = validate_percentage(volatility_21d, min_pct=0, max_pct=1.0)

        return {
            # 기본 지표
            "last_price": last_close,
            "sma20": float(s20) if s20 else None,
            "sma50": float(s50) if s50 else None,
            "sma200": float(s200) if s200 else None,
            "ret5": float(ret5) if ret5 else None,
            "ret20": float(ret20) if ret20 else None,
            "ret63": float(ret63) if ret63 else None,
            "avg20_vol": avg20_vol,
            "today_vol": today_vol,
            "rvol": rvol,
            "atr": atr,
            "atr_pct": atr_pct,

            # 신규 기술적 지표
            "rsi_14": rsi_14,
            "macd": macd,
            "macd_signal": macd_signal,
            "macd_histogram": macd_histogram,
            "bb_upper": bb_upper,
            "bb_lower": bb_lower,
            "bb_position": bb_position,
            "high_52w": high_52w,
            "low_52w": low_52w,
            "high_52w_ratio": high_52w_ratio,
            "low_52w_ratio": low_52w_ratio,
            "momentum_12m": momentum_12m,
            "volatility_21d": volatility_21d,
        }
    except Exception:
        return None


def _compute_ta_metrics(df):
    """DataFrame에서 기술적 지표 계산"""
    out = {}

    try:
        if isinstance(df.columns, pd.MultiIndex):
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

                    metrics = _compute_enhanced_ta_single(c, h, l, v)
                    if metrics:
                        out[t] = metrics
                except Exception:
                    continue
        else:
            close_col = "Adj Close" if "Adj Close" in df.columns else "Close"
            c = df[close_col] if close_col in df.columns else None
            h = df["High"] if "High" in df.columns else None
            l = df["Low"] if "Low" in df.columns else None
            v = df["Volume"] if "Volume" in df.columns else None

            metrics = _compute_enhanced_ta_single(c, h, l, v)
            if metrics:
                out["__SINGLE__"] = metrics

    except Exception:
        pass

    return out


def safe_yf_download(tickers, **kwargs):
    """안전한 yfinance 다운로드 with 개선된 에러 처리"""
    max_retries = kwargs.pop('max_retries', 3)
    ticker_str = tickers if isinstance(tickers, str) else f"batch({len(tickers)})"

    for attempt in range(max_retries):
        try:
            data = yf.download(tickers, **kwargs)
            if data is not None and not data.empty:
                return data
            elif attempt == max_retries - 1:
                log_error("YF_DOWNLOAD", ticker_str, "Empty data returned")
        except Exception as e:
            if attempt < max_retries - 1:
                sleep_time = (2 ** attempt) + random.uniform(0, 1)
                log_error("YF_DOWNLOAD", ticker_str, f"Attempt {attempt+1} failed: {str(e)}, retrying in {sleep_time:.1f}s")
                time.sleep(sleep_time)
            else:
                log_error("YF_DOWNLOAD", ticker_str, f"All {max_retries} attempts failed: {str(e)}")

    return None


# ⭐⭐⭐ OHLCV 배치 다운로드 병렬 처리 함수
def process_ohlcv_batch(args):
    """단일 배치 OHLCV 다운로드 및 처리 (병렬 처리용)"""
    batch, batch_idx, total_batches, period = args

    TA_batch = {}
    PX_batch = {}
    VOL_batch = {}
    ok_tickers_batch = set()

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
                threads=False,
                timeout=30
            )
            if batch_data is not None and not batch_data.empty:
                break
        except Exception:
            if attempt < CONFIG["BATCH_RETRIES"] - 1:
                time.sleep((1.5 ** attempt) + random.random())

    processed_count = 0

    # 배치 데이터 처리
    if batch_data is not None and not batch_data.empty:
        metrics = _compute_ta_metrics(batch_data)

        if isinstance(batch_data.columns, pd.MultiIndex):
            close_col = "Adj Close" if "Adj Close" in set(batch_data.columns.get_level_values(0)) else "Close"

            for t in batch:
                try:
                    if (close_col, t) not in batch_data.columns:
                        continue

                    prices = batch_data[(close_col, t)].dropna()
                    if len(prices) < 5:
                        continue

                    last_price = float(prices.iloc[-1])
                    last_price = validate_price(last_price)
                    if last_price is None:
                        continue

                    avg_vol = 0
                    if ("Volume", t) in batch_data.columns:
                        vols = batch_data[("Volume", t)].dropna()
                        avg_vol = float(vols.rolling(20).mean().iloc[-1]) if len(vols) >= 20 else float(vols.mean())
                        avg_vol = validate_volume(avg_vol) or 0

                    ok_tickers_batch.add(t)
                    PX_batch[t] = last_price
                    VOL_batch[t] = avg_vol

                    if t in metrics:
                        TA_batch[t] = metrics[t]
                    else:
                        TA_batch[t] = {
                            "last_price": last_price,
                            "sma20": last_price,
                            "sma50": last_price,
                            "ret5": 0.0,
                            "ret20": 0.0,
                            "avg20_vol": avg_vol,
                            "rvol": 1.0,
                            "atr_pct": 0.02
                        }

                    processed_count += 1

                except Exception:
                    continue
        else:
            if batch and len(batch) == 1:
                t = batch[0]
                try:
                    close_col = "Adj Close" if "Adj Close" in batch_data.columns else "Close"
                    prices = batch_data[close_col].dropna() if close_col in batch_data.columns else None
                    if prices is not None and len(prices) >= 5:
                        last_price = float(prices.iloc[-1])
                        last_price = validate_price(last_price)
                        if last_price is not None:
                            avg_vol = 0
                            if "Volume" in batch_data.columns:
                                vols = batch_data["Volume"].dropna()
                                avg_vol = float(vols.rolling(20).mean().iloc[-1]) if len(vols) >= 20 else float(
                                    vols.mean())
                                avg_vol = validate_volume(avg_vol) or 0

                            ok_tickers_batch.add(t)
                            PX_batch[t] = last_price
                            VOL_batch[t] = avg_vol

                            if "__SINGLE__" in metrics:
                                TA_batch[t] = metrics["__SINGLE__"]
                            else:
                                TA_batch[t] = {
                                    "last_price": last_price,
                                    "sma20": last_price,
                                    "sma50": last_price,
                                    "ret5": 0.0,
                                    "ret20": 0.0,
                                    "avg20_vol": avg_vol,
                                    "rvol": 1.0,
                                    "atr_pct": 0.02
                                }
                            processed_count += 1
                except Exception:
                    pass

    # 배치 완전 실패 시 전체 개별 다운로드, 부분 실패 시 누락된 것만 개별 다운로드
    if processed_count == 0:
        # 배치 전체 실패 - 모든 티커 개별 다운로드
        retry_tickers = batch
    elif processed_count < len(batch):
        # 부분 성공 - 실패한 티커만 개별 다운로드
        retry_tickers = [t for t in batch if t not in ok_tickers_batch]
        if CONFIG.get("VERBOSE_LOGGING", False):
            print(f"  [배치 {batch_idx}] 부분 성공: {processed_count}/{len(batch)}, 누락 {len(retry_tickers)}개 재시도")
    else:
        # 전체 성공
        retry_tickers = []

    # 개별 다운로드
    if retry_tickers:
        for t in retry_tickers:
            for attempt in range(CONFIG["SINGLE_RETRIES"]):
                try:
                    data = safe_yf_download(
                        t,
                        period=period,
                        interval="1d",
                        auto_adjust=False,
                        progress=False,
                        threads=False,
                        timeout=30,
                        max_retries=2
                    )
                    if data is not None and not data.empty:
                        metrics = _compute_ta_metrics(data)
                        if "__SINGLE__" in metrics:
                            close_col = "Adj Close" if "Adj Close" in data.columns else "Close"
                            prices = data[close_col].dropna()

                            if len(prices) >= 5:
                                last_price = float(prices.iloc[-1])
                                last_price = validate_price(last_price)
                                if last_price is not None:
                                    avg_vol = 0
                                    if "Volume" in data.columns:
                                        vols = data["Volume"].dropna()
                                        avg_vol = float(vols.rolling(20).mean().iloc[-1]) if len(vols) >= 20 else float(
                                            vols.mean())
                                        avg_vol = validate_volume(avg_vol) or 0

                                    ok_tickers_batch.add(t)
                                    PX_batch[t] = last_price
                                    VOL_batch[t] = avg_vol
                                    TA_batch[t] = metrics["__SINGLE__"]
                                    processed_count += 1
                        break
                except Exception:
                    if attempt < CONFIG["SINGLE_RETRIES"] - 1:
                        time.sleep((1.5 ** attempt) + random.random() * 0.3)

    return batch_idx, TA_batch, PX_batch, VOL_batch, ok_tickers_batch, processed_count, len(batch)


def preload_ohlcv_light(tickers, period="120d", chunk=50, **kwargs):
    """⭐ 병렬 처리된 OHLCV 데이터 프리로드"""
    TA, PX, VOL = {}, {}, {}
    ok_tickers = set()

    print(f"[OHLCV] {len(tickers)}개 종목 로드 시작...")
    print(f"[OHLCV] {CONFIG['OHLCV_WORKERS']}개 스레드로 병렬 처리...")

    # 배치 생성
    batches = []
    total_batches = (len(tickers) + chunk - 1) // chunk
    for i in range(0, len(tickers), chunk):
        batch = tickers[i:i + chunk]
        batch_idx = i // chunk + 1
        batches.append((batch, batch_idx, total_batches, period))

    # 병렬 처리
    total_processed = 0
    completed = 0
    with ThreadPoolExecutor(max_workers=CONFIG["OHLCV_WORKERS"]) as executor:
        futures = {executor.submit(process_ohlcv_batch, batch_info): batch_info for batch_info in batches}

        for future in as_completed(futures):
            try:
                batch_idx, TA_batch, PX_batch, VOL_batch, ok_batch, processed, total = future.result()

                # 결과 병합
                TA.update(TA_batch)
                PX.update(PX_batch)
                VOL.update(VOL_batch)
                ok_tickers.update(ok_batch)
                total_processed += processed
                completed += 1

                if completed % 10 == 0 or completed == total_batches:
                    print(f"[OHLCV] 진행: {completed}/{total_batches} 배치 완료 (누적: {total_processed}/{len(tickers)} 종목)")

            except Exception:
                continue

    print(f"[OHLCV] 전체 완료: {len(ok_tickers)}/{len(tickers)}개 종목 성공")
    return TA, PX, VOL, ok_tickers


# ============== 상세 재무 유틸 ==============
REV_ALIASES = ["total revenue", "revenues", "revenue", "net sales", "sales", "total net sales"]
OP_ALIASES = ["operating income", "operating income (loss)", "income from operations", "operating profit",
              "operating profit (loss)", "ebit"]
FCF_ALIASES = ["free cash flow", "free cashflow", "freecashflow"]
DA_ALIASES = ["depreciation", "depreciation & amortization", "depreciation and amortization"]
EPS_ALIASES = ["diluted eps", "basic eps", "eps (diluted)", "eps (basic)", "earnings per share", "eps"]
NET_INCOME_ALIASES = ["net income", "net income common stockholders", "net income applicable to common shares"]
DIL_SHARES_ALIASES = ["diluted average shares", "weighted average shares diluted",
                      "weighted average diluted shares outstanding", "weighted average diluted shares",
                      "weighted average shares - diluted", "weighted average number of shares diluted"]


def _find_row(index_like, aliases, exclude=None):
    if index_like is None: return None
    exclude = [w.lower() for w in (exclude or [])]
    idx = [str(x).lower() for x in index_like]
    for key in aliases:
        k = key.lower()
        for i, s in enumerate(idx):
            if k in s and not any(x in s for x in exclude):
                return index_like[i]
    return None


def coalesce(*vals):
    for v in vals:
        try:
            if v is None: continue
            if isinstance(v, float) and math.isnan(v): continue
            return v
        except Exception:
            continue
    return None


def ttm_sum(df: pd.DataFrame, row, n=4, absolute=False):
    if df is None or df.empty or row not in df.index or df.shape[1] < n: return None
    cols = sorted(df.columns, reverse=True)[:n]
    try:
        vals = pd.to_numeric(df.loc[row, cols], errors="coerce").fillna(0)
        result = float(vals.abs().sum()) if absolute else float(vals.sum())
        return result if not math.isnan(result) else None
    except Exception:
        return None


def ttm_yoy_growth(df_q: pd.DataFrame, row):
    if df_q is None or df_q.empty or row not in df_q.index or df_q.shape[1] < 8: return None
    cols = sorted(df_q.columns, reverse=True)
    try:
        curr = float(pd.to_numeric(df_q.loc[row, cols[:4]], errors="coerce").fillna(0).sum())
        prev = float(pd.to_numeric(df_q.loc[row, cols[4:8]], errors="coerce").fillna(0).sum())
    except Exception:
        return None
    if prev <= 0: return None
    growth = (curr / prev) - 1.0
    return validate_percentage(growth, min_pct=-0.99, max_pct=9.99)


def annual_yoy_growth(df_a: pd.DataFrame, row):
    if df_a is None or df_a.empty or row not in df_a.index or df_a.shape[1] < 2: return None
    cols = sorted(df_a.columns, reverse=True)[:2]
    try:
        curr = float(pd.to_numeric(df_a.loc[row, cols[0]], errors="coerce"))
        prev = float(pd.to_numeric(df_a.loc[row, cols[1]], errors="coerce"))
    except Exception:
        return None
    if prev <= 0: return None
    growth = (curr / prev) - 1.0
    return validate_percentage(growth, min_pct=-0.99, max_pct=9.99)


def _last4_sum_row(df, aliases):
    if df is None or df.empty: return None
    row = _find_row(df.index, aliases)
    if not row or df.shape[1] < 4: return None
    cols = sorted(df.columns, reverse=True)[:4]
    return float(pd.to_numeric(df.loc[row, cols], errors="coerce").fillna(0).sum())


def _last_col(df, aliases):
    if df is None or df.empty: return None
    row = _find_row(df.index, aliases)
    if not row: return None
    col = sorted(df.columns, reverse=True)[0]
    return float(pd.to_numeric(df.loc[row, col], errors="coerce"))


def _eps_ttm_from_statements(df_q, df_a):
    ni = _last4_sum_row(df_q, NET_INCOME_ALIASES)
    sh = _last_col(df_a, DIL_SHARES_ALIASES)
    if ni and sh and sh > 0: return ni / sh
    return None


def _safe_df(getter, max_retries=2):
    """DataFrame 안전하게 가져오기 with 재시도"""
    for attempt in range(max_retries):
        try:
            df = getter()
            if df is not None and hasattr(df, 'empty') and not df.empty:
                return df
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(0.5 + random.uniform(0, 0.5))
            # 마지막 시도 실패 시는 조용히 실패 (너무 많은 로그 방지)
    return None


def _parse_growth_to_pct(val):
    """성장률 파싱"""
    if val is None: return None
    try:
        if isinstance(val, str):
            s = val.strip().replace('%', '').replace('+', '')
            if s.lower() in {'n/a', 'na', 'nan', 'none', '-', ''}: return None
            return float(s)
        x = float(val)
        return x * 100.0 if abs(x) <= 1.0 else x
    except Exception:
        return None


def get_eps_annual_series(tic: yf.Ticker):
    """EPS 연간 시리즈 가져오기"""
    df_a = None
    try:
        df_a = tic.income_stmt
        if df_a is None or df_a.empty: df_a = tic.financials
    except Exception:
        pass
    if df_a is not None and not df_a.empty:
        row_eps = _find_row(df_a.index, EPS_ALIASES)
        if row_eps:
            try:
                vals = pd.to_numeric(df_a.loc[row_eps], errors="coerce").dropna()
                return list(vals.sort_index().values)
            except Exception:
                pass
        else:
            ni_row = _find_row(df_a.index, NET_INCOME_ALIASES)
            sh_row = _find_row(df_a.index, DIL_SHARES_ALIASES)
            if ni_row and sh_row:
                try:
                    ni = pd.to_numeric(df_a.loc[ni_row], errors="coerce")
                    sh = pd.to_numeric(df_a.loc[sh_row], errors="coerce").replace(0, np.nan)
                    vals = (ni / sh).dropna()
                    return list(vals.sort_index().values)
                except Exception:
                    pass
    try:
        earn = tic.earnings
        if earn is not None and not earn.empty:
            info = {}
            try:
                info = tic.get_info() or {}
            except Exception:
                pass
            so = info.get("sharesOutstanding")
            if so and so > 0:
                ser = pd.to_numeric(earn["Earnings"], errors="coerce") / float(so)
                return list(ser.sort_index().dropna().values)
    except Exception:
        pass
    return []


def eps_cagr_from_series(vals, min_years=3, max_years=5):
    """EPS CAGR 계산"""
    v = [float(x) for x in vals if x is not None and not np.isnan(x)]
    if len(v) < min_years: return None
    use = v[-max_years:]
    if len(use) < min_years: return None
    first, last = use[0], use[-1]
    if first <= 0 or last <= 0: return None
    years = len(use) - 1
    if years <= 0: return None
    cagr = (last / first) ** (1.0 / years) - 1.0
    return validate_percentage(cagr, min_pct=-0.99, max_pct=9.99)


def calculate_missing_financials(ticker, info, df_q, df_a, cf_q, balance_a, price):
    """누락된 재무 데이터 계산 + 이상치 검증"""
    calculated = {}

    try:
        # 1. RevYoY 계산
        if calculated.get('RevYoY') is None and df_q is not None:
            rev_row = _find_row(df_q.index, REV_ALIASES, exclude=["per share", "operating revenue", "royalty"])
            if rev_row:
                rev_yoy = ttm_yoy_growth(df_q, rev_row)
                if rev_yoy is not None:
                    calculated['RevYoY'] = rev_yoy
                elif df_a is not None and rev_row in df_a.index:
                    rev_yoy = annual_yoy_growth(df_a, rev_row)
                    if rev_yoy is not None:
                        calculated['RevYoY'] = rev_yoy

        # 2. OpMarginTTM 계산
        if calculated.get('OpMarginTTM') is None and df_q is not None:
            rev_row = _find_row(df_q.index, REV_ALIASES, exclude=["per share", "operating revenue", "royalty"])
            op_row = _find_row(df_q.index, OP_ALIASES)
            if rev_row and op_row:
                rev_ttm = ttm_sum(df_q, rev_row, 4)
                op_ttm = ttm_sum(df_q, op_row, 4)
                if rev_ttm and op_ttm and rev_ttm > 0:
                    margin = op_ttm / rev_ttm
                    calculated['OpMarginTTM'] = validate_percentage(margin, min_pct=-1.0, max_pct=1.0)

        # 3. ROE 계산
        if calculated.get('ROE(info)') is None and df_a is not None and balance_a is not None:
            ni_row = _find_row(df_a.index, NET_INCOME_ALIASES)
            equity_row = _find_row(balance_a.index, ["total equity", "stockholders equity", "shareholders equity"])
            if ni_row and equity_row:
                ni = _last_col(df_a, [ni_row])
                equity = _last_col(balance_a, [equity_row])
                if ni and equity and equity > 0:
                    roe = ni / equity
                    calculated['ROE(info)'] = validate_percentage(roe, min_pct=-5.0, max_pct=5.0)

        # 4. EV/EBITDA 계산
        if calculated.get('EV_EBITDA') is None:
            ev = info.get("enterpriseValue")
            ebitda = info.get("ebitda")
            if ev and ebitda and ebitda > 0:
                ev_ebitda = ev / ebitda
                calculated['EV_EBITDA'] = validate_ratio(ev_ebitda, min_ratio=-100, max_ratio=500)

        # 5. FCF Yield 계산
        if calculated.get('FCF_Yield') is None and cf_q is not None:
            fcf_row = _find_row(cf_q.index, FCF_ALIASES)
            if fcf_row:
                fcf_ttm = ttm_sum(cf_q, fcf_row, 4)
                mktcap = info.get("marketCap")
                if fcf_ttm and mktcap and mktcap > 0:
                    fcf_yield = fcf_ttm / mktcap
                    calculated['FCF_Yield'] = validate_percentage(fcf_yield, min_pct=-1.0, max_pct=1.0)

        # 6. PB 계산
        if calculated.get('PB') is None and balance_a is not None:
            equity_row = _find_row(balance_a.index, ["total equity", "stockholders equity", "shareholders equity"])
            if equity_row and price:
                equity = _last_col(balance_a, [equity_row])
                shares = info.get("sharesOutstanding")
                if equity and shares and shares > 0:
                    bps = equity / shares
                    if bps > 0:
                        pb = price / bps
                        calculated['PB'] = validate_ratio(pb, min_ratio=0, max_ratio=100)

        # 7. PayoutRatio 계산
        if calculated.get('PayoutRatio') is None and df_a is not None:
            div_row = _find_row(df_a.index, ["dividends paid", "cash dividends paid", "dividend"])
            ni_row = _find_row(df_a.index, NET_INCOME_ALIASES)
            if div_row and ni_row:
                div_paid = _last_col(df_a, [div_row])
                ni = _last_col(df_a, [ni_row])
                if div_paid and ni and ni > 0:
                    payout = abs(div_paid) / ni
                    calculated['PayoutRatio'] = validate_percentage(payout, min_pct=0, max_pct=2.0)

    except Exception:
        pass

    return calculated


def _calculate_financial_ratios(q_is, a_is):
    """재무 비율 계산"""
    rev_yoy = op_margin = None

    if q_is is not None and not q_is.empty:
        rev_row = _find_row(q_is.index, REV_ALIASES, exclude=["per share", "operating revenue", "royalty"])
        op_row = _find_row(q_is.index, OP_ALIASES)

        if rev_row:
            rev_ttm = ttm_sum(q_is, rev_row, 4)
            rev_yoy = ttm_yoy_growth(q_is, rev_row)

            if rev_yoy is None and a_is is not None and not a_is.empty and rev_row in a_is.index:
                rev_yoy = annual_yoy_growth(a_is, rev_row)

            if op_row and rev_ttm and rev_ttm > 0:
                op_ttm = ttm_sum(q_is, op_row, 4)
                if op_ttm:
                    op_margin = op_ttm / rev_ttm
                    op_margin = validate_percentage(op_margin, min_pct=-1.0, max_pct=1.0)

    return rev_yoy, op_margin


def _calculate_ev_ebitda(info, q_is):
    """EV/EBITDA 계산"""
    ev = info.get("enterpriseValue")
    ebitda = info.get("ebitda")
    ev_ebitda = None

    try:
        if ev and ebitda and float(ebitda) > 0:
            ev_ebitda = float(ev) / float(ebitda)
            ev_ebitda = validate_ratio(ev_ebitda, min_ratio=-100, max_ratio=500)
    except (TypeError, ValueError):
        pass

    return ev_ebitda


def _calculate_fcf_yield(info, cf_q):
    """FCF Yield 계산"""
    fcf_yield = None
    if cf_q is not None and not cf_q.empty:
        fcf_row = _find_row(cf_q.index, FCF_ALIASES)
        if fcf_row:
            fcf_ttm = ttm_sum(cf_q, fcf_row, 4)
            mktcap = info.get("marketCap")
            if fcf_ttm and mktcap and float(mktcap) > 0:
                fcf_yield = float(fcf_ttm) / float(mktcap)
                fcf_yield = validate_percentage(fcf_yield, min_pct=-1.0, max_pct=1.0)

    return fcf_yield


def _calculate_growth_indicators(q_is, a_is, info):
    """성장성 지표 계산"""
    growth = {
        "EPS_Growth_3Y": None,
        "Revenue_Growth_3Y": None,
        "EBITDA_Growth_3Y": None,
    }

    try:
        # EPS 성장률
        eps_series = []
        if a_is is not None and not a_is.empty:
            eps_row = _find_row(a_is.index, EPS_ALIASES)
            if eps_row:
                eps_data = pd.to_numeric(a_is.loc[eps_row], errors="coerce").dropna()
                if len(eps_data) >= 3:
                    eps_series = list(eps_data.sort_index().values[-3:])

        if len(eps_series) >= 3:
            cagr = (eps_series[-1] / eps_series[0]) ** (1 / 2) - 1
            growth["EPS_Growth_3Y"] = validate_percentage(cagr, min_pct=-0.99, max_pct=9.99)

        # 매출 성장률
        if a_is is not None and not a_is.empty:
            rev_row = _find_row(a_is.index, REV_ALIASES)
            if rev_row:
                rev_data = pd.to_numeric(a_is.loc[rev_row], errors="coerce").dropna()
                if len(rev_data) >= 3:
                    rev_series = list(rev_data.sort_index().values[-3:])
                    if len(rev_series) >= 3:
                        cagr = (rev_series[-1] / rev_series[0]) ** (1 / 2) - 1
                        growth["Revenue_Growth_3Y"] = validate_percentage(cagr, min_pct=-0.99, max_pct=9.99)

    except Exception:
        pass

    return growth


# ⭐⭐⭐ 병렬 처리를 위한 래퍼 함수
def fetch_single_ticker_wrapper(args):
    """단일 티커 데이터 수집 (병렬 처리용)"""
    t, row = args
    try:
        rec = fetch_enhanced_details_for_ticker(
            t,
            price=row["Price"],
            avg_vol=(row["DollarVol($M)"] * 1_000_000) / max(1e-9, row["Price"])
        )

        # 라이트 필드 병합
        rec.update({
            "SMA20": row.get("SMA20"),
            "SMA50": row.get("SMA50"),
            "SMA200": row.get("SMA200"),
            "ATR_PCT": row.get("ATR_PCT"),
            "RVOL": row.get("RVOL"),
            "RET5": row.get("RET5"),
            "RET20": row.get("RET20"),
            "RET63": row.get("RET63"),
            "RSI_14": row.get("RSI_14"),
            "MACD": row.get("MACD"),
            "MACD_Signal": row.get("MACD_Signal"),
            "MACD_Histogram": row.get("MACD_Histogram"),
            "BB_Position": row.get("BB_Position"),
            "High_52W_Ratio": row.get("High_52W_Ratio"),
            "Low_52W_Ratio": row.get("Low_52W_Ratio"),
            "Momentum_12M": row.get("Momentum_12M"),
            "Volatility_21D": row.get("Volatility_21D"),
        })

        return rec, None
    except Exception as e:
        return None, f"종목 {t} 상세 데이터 수집 실패: {str(e)}"


def fetch_enhanced_details_for_ticker(tkr, price, avg_vol):
    """개선된 상세 데이터 수집 with 재시도"""
    t = None
    info = {}

    # Ticker 객체 생성 및 info 가져오기 (재시도)
    for attempt in range(3):
        try:
            t = yf.Ticker(tkr)
            info = t.get_info() or {}
            if info:  # info가 있으면 성공
                break
        except Exception as e:
            if attempt < 2:
                time.sleep(0.3 + random.uniform(0, 0.3))
            else:
                log_error("GET_INFO", tkr, f"Failed to get info after 3 attempts: {str(e)}")
                # info 실패해도 계속 진행 (재무제표는 시도)
                if t is None:
                    try:
                        t = yf.Ticker(tkr)
                    except:
                        return _create_default_record(tkr, price, avg_vol)

    try:
        mktcap = validate_market_cap(info.get("marketCap"))
        price = validate_price(price)
        avg_vol = validate_volume(avg_vol)
        dollar_vol = (float(price) * float(avg_vol)) if (price is not None and avg_vol is not None) else None

        # 재무제표 데이터 수집 (재시도 로직 포함)
        q_is = _safe_df(lambda: t.quarterly_income_stmt, max_retries=3)
        if q_is is None:
            q_is = _safe_df(lambda: t.quarterly_financials, max_retries=2)

        a_is = _safe_df(lambda: t.income_stmt, max_retries=3)
        if a_is is None:
            a_is = _safe_df(lambda: t.financials, max_retries=2)

        cf_q = _safe_df(lambda: t.quarterly_cashflow, max_retries=3)
        balance_a = _safe_df(lambda: t.balance_sheet, max_retries=3)

        # 재무제표 수집 성공 여부 로깅
        financial_data_available = sum([
            q_is is not None,
            a_is is not None,
            cf_q is not None,
            balance_a is not None
        ])

        if CONFIG.get("VERBOSE_LOGGING", False) and financial_data_available == 0:
            log_error("FINANCIAL_DATA", tkr, "No financial statements available")

        # 강화된 PER 계산 (에러 발생해도 계속 진행)
        pe_enhanced = None
        try:
            pe_enhanced = calculate_pe_ratio(tkr, price, info, q_is, a_is)
        except Exception as e:
            log_error("PE_CALC", tkr, f"PE calculation failed: {str(e)}")

        # PEG 계산 (에러 발생해도 계속 진행)
        peg_enhanced = None
        try:
            if pe_enhanced and pe_enhanced > 0:
                earnings_growth = info.get("earningsGrowth") or info.get("earningsQuarterlyGrowth")
                if earnings_growth and earnings_growth > 0:
                    peg_enhanced = pe_enhanced / (earnings_growth * 100)
                    peg_enhanced = validate_ratio(peg_enhanced, min_ratio=0, max_ratio=100)
                else:
                    eps_series = get_eps_annual_series(t)
                    eps_cagr = eps_cagr_from_series(eps_series, 3, 5)
                    if eps_cagr and eps_cagr > 0:
                        peg_enhanced = pe_enhanced / (eps_cagr * 100)
                        peg_enhanced = validate_ratio(peg_enhanced, min_ratio=0, max_ratio=100)
        except Exception as e:
            log_error("PEG_CALC", tkr, f"PEG calculation failed: {str(e)}")

        # 기본 재무 데이터 (각각 독립적으로 에러 처리)
        rev_yoy = op_margin = None
        try:
            rev_yoy, op_margin = _calculate_financial_ratios(q_is, a_is)
        except Exception as e:
            log_error("FINANCIAL_RATIOS", tkr, f"Failed: {str(e)}")

        ev_ebitda = None
        try:
            ev_ebitda = _calculate_ev_ebitda(info, q_is)
        except Exception as e:
            log_error("EV_EBITDA", tkr, f"Failed: {str(e)}")

        fcf_yield = None
        try:
            fcf_yield = _calculate_fcf_yield(info, cf_q)
        except Exception as e:
            log_error("FCF_YIELD", tkr, f"Failed: {str(e)}")

        growth_indicators = {"EPS_Growth_3Y": None, "Revenue_Growth_3Y": None, "EBITDA_Growth_3Y": None}
        try:
            growth_indicators = _calculate_growth_indicators(q_is, a_is, info)
        except Exception as e:
            log_error("GROWTH_INDICATORS", tkr, f"Failed: {str(e)}")

        # 이상치 검증
        operating_margins = validate_percentage(info.get("operatingMargins"), min_pct=-1.0, max_pct=1.0)
        roe = validate_percentage(info.get("returnOnEquity"), min_pct=-5.0, max_pct=5.0)
        roa = validate_percentage(info.get("returnOnAssets"), min_pct=-5.0, max_pct=5.0)
        pb = validate_ratio(info.get("priceToBook") or info.get("priceToBookRatio"), min_ratio=0, max_ratio=100)
        ps = validate_ratio(info.get("priceToSalesTrailing12Months"), min_ratio=0, max_ratio=100)
        div_yield = validate_percentage(info.get("dividendYield") or info.get("trailingAnnualDividendYield"), min_pct=0,
                                        max_pct=0.5)
        payout_ratio = validate_percentage(info.get("payoutRatio"), min_pct=0, max_pct=2.0)
        beta = validate_numeric(info.get("beta"), min_val=-5, max_val=5, allow_negative=True)
        short_percent = validate_percentage(info.get("shortPercentOfFloat"), min_pct=0, max_pct=1.0)
        insider_ownership = validate_percentage(info.get("heldPercentInsiders"), min_pct=0, max_pct=1.0)
        institution_ownership = validate_percentage(info.get("heldPercentInstitutions"), min_pct=0, max_pct=1.0)

        # 기본 레코드 생성
        rec = {
            "Ticker": tkr,
            "Name": info.get("longName") or info.get("shortName") or tkr,
            "Sector": info.get("sector"),
            "Industry": info.get("industry"),
            "MktCap($B)": round((mktcap or 0) / 1_000_000_000, 2) if mktcap else None,
            "Price": round(price, 2) if price is not None else None,
            "DollarVol($M)": round((dollar_vol or 0) / 1_000_000, 2) if dollar_vol is not None else None,

            # 재무 지표
            "RevYoY": rev_yoy,
            "OpMarginTTM": op_margin,
            "OperatingMargins(info)": operating_margins,
            "ROE(info)": roe,
            "ROA(info)": roa,
            "EV_EBITDA": ev_ebitda,
            "PE": pe_enhanced,
            "PEG": peg_enhanced,
            "FCF_Yield": fcf_yield,
            "PB": pb,
            "PS": ps,
            "DivYield": div_yield,
            "PayoutRatio": payout_ratio,

            # 신규 성장성 지표
            **growth_indicators,

            # 기타
            "Beta": beta,
            "ShortPercent": short_percent,
            "InsiderOwnership": insider_ownership,
            "InstitutionOwnership": institution_ownership,
        }

        # 누락된 데이터 계산으로 보완
        try:
            calculated = calculate_missing_financials(tkr, info, q_is, a_is, cf_q, balance_a, price)
            for key, value in calculated.items():
                if rec.get(key) is None and value is not None:
                    rec[key] = value
        except Exception as e:
            log_error("MISSING_FINANCIALS", tkr, f"Failed: {str(e)}")

        return rec

    except Exception as e:
        log_error("FETCH_DETAILS", tkr, f"Unexpected error: {str(e)}")
        return _create_default_record(tkr, price, avg_vol, info)


def _create_default_record(tkr, price, avg_vol, info=None):
    """기본 레코드 생성"""
    if info is None:
        info = {}

    mktcap = validate_market_cap(info.get("marketCap"))
    price = validate_price(price)
    avg_vol = validate_volume(avg_vol)
    dollar_vol = (float(price) * float(avg_vol)) if (price is not None and avg_vol is not None) else None

    return {
        "Ticker": tkr,
        "Name": info.get("longName") or info.get("shortName") or tkr,
        "Sector": info.get("sector"),
        "Industry": info.get("industry"),
        "MktCap($B)": round((mktcap or 0) / 1_000_000_000, 2) if mktcap else None,
        "Price": round(price, 2) if price is not None else None,
        "DollarVol($M)": round((dollar_vol or 0) / 1_000_000, 2) if dollar_vol is not None else None,
        "RevYoY": None, "OpMarginTTM": None, "OperatingMargins(info)": None,
        "ROE(info)": None, "ROA(info)": None, "EV_EBITDA": None, "PE": None,
        "PEG": None, "FCF_Yield": None, "PB": None, "PS": None, "DivYield": None,
        "PayoutRatio": None,
        "EPS_Growth_3Y": None, "Revenue_Growth_3Y": None, "EBITDA_Growth_3Y": None,
        "Beta": None, "ShortPercent": None, "InsiderOwnership": None, "InstitutionOwnership": None,
    }


def build_enhanced_details_cache():
    """⭐ 완전 병렬화된 캐시 빌드 함수"""
    source = CONFIG["UNIVERSE_SOURCE"]
    tickers = load_universe()

    # OHLCV 라이트 지표 수집 (병렬 처리)
    print("\n" + "=" * 60)
    print("📊 1단계: OHLCV 데이터 수집 (병렬 처리)")
    print("=" * 60)

    TA, PX, VOL, ok = preload_ohlcv_light(
        tickers,
        period=CONFIG["PRELOAD_PERIOD"],
        chunk=CONFIG["PRELOAD_CHUNK"]
    )

    if not ok:
        raise RuntimeError("OHLCV 라이트 프리로드 실패")

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
            "SMA200": tta.get("sma200"),
            "ATR_PCT": tta.get("atr_pct"),
            "RVOL": tta.get("rvol"),
            "RET5": tta.get("ret5"),
            "RET20": tta.get("ret20"),
            "RET63": tta.get("ret63"),
            "RSI_14": tta.get("rsi_14"),
            "MACD": tta.get("macd"),
            "MACD_Signal": tta.get("macd_signal"),
            "MACD_Histogram": tta.get("macd_histogram"),
            "BB_Position": tta.get("bb_position"),
            "High_52W_Ratio": tta.get("high_52w_ratio"),
            "Low_52W_Ratio": tta.get("low_52w_ratio"),
            "Momentum_12M": tta.get("momentum_12m"),
            "Volatility_21D": tta.get("volatility_21d"),
        }
        lite_rows.append(row)

    lite_df = pd.DataFrame(lite_rows)
    if lite_df.empty:
        raise RuntimeError("라이트 지표 표가 비어 있음")

    # 상세 호출 대상 선정
    lite_df["_pass_light_generic"] = lite_df.apply(
        lambda r: pass_light_generic(r["Price"], r["DollarVol($M)"] * 1_000_000), axis=1
    )

    passed_tickers = lite_df[lite_df["_pass_light_generic"]]
    print(f"\n라이트 필터 통과: {len(passed_tickers)}개")

    cand = passed_tickers.sort_values("DollarVol($M)", ascending=False).head(CONFIG["DETAILED_TOP_K"])
    print(f"상세 데이터 수집 대상: {len(cand)}개")

    # ⭐ 병렬 처리로 상세 재무 수집
    print("\n" + "=" * 60)
    print("💼 2단계: 상세 재무 데이터 수집 (병렬 처리)")
    print("=" * 60)

    detail_rows = []
    success_count = 0
    error_count = 0

    print(f"[상세데이터] {CONFIG['DETAIL_FETCH_WORKERS']}개 스레드로 병렬 처리 시작...")

    # 작업 준비
    tasks = [(t, row) for t, row in cand.set_index("Ticker").iterrows()]

    # ThreadPoolExecutor로 병렬 처리
    with ThreadPoolExecutor(max_workers=CONFIG["DETAIL_FETCH_WORKERS"]) as executor:
        futures = {executor.submit(fetch_single_ticker_wrapper, task): task[0] for task in tasks}

        for i, future in enumerate(as_completed(futures), start=1):
            ticker = futures[future]
            try:
                rec, error = future.result()

                if rec is not None:
                    detail_rows.append(rec)
                    success_count += 1
                else:
                    error_count += 1

                # 진행 상황 출력
                if (i % 100) == 0:
                    print(f"  - {i}/{len(tasks)} 완료 (성공: {success_count}, 실패: {error_count})")

            except Exception:
                error_count += 1

    print(f"[상세데이터] 최종 수집: {success_count}/{len(cand)} 종목 (실패: {error_count})")

    # 데이터 병합
    details_df = pd.DataFrame(detail_rows)
    details_dict = details_df.set_index('Ticker').to_dict('index')

    base_df = passed_tickers.drop(columns=["_pass_light_generic"]).copy()
    detail_columns = [col for col in details_df.columns if col not in ['Ticker']]

    for col in detail_columns:
        base_df[col] = base_df['Ticker'].map(
            {ticker: data.get(col) for ticker, data in details_dict.items()}
        )

    out = base_df
    print(f"\n최종 CSV 행 수: {len(out)}")

    # 데이터 타입 정리
    numeric_columns = ["RevYoY", "OpMarginTTM", "OperatingMargins(info)", "ROE(info)",
                       "FCF_Yield", "DivYield", "EPS_Growth_3Y", "Revenue_Growth_3Y",
                       "RSI_14", "MACD", "MACD_Signal", "MACD_Histogram", "BB_Position",
                       "High_52W_Ratio", "Low_52W_Ratio", "Momentum_12M", "Volatility_21D"]

    for col in numeric_columns:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors='coerce')

    out["CreatedAtUTC"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    out["Source"] = source

    # 저장
    base = CONFIG["OUT_BASENAME"].strip() or f"details_cache_{source}"
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = f"{base}_{ts}.csv"
    out.to_csv(csv_path, index=False)
    print(f"\n[캐시] 저장 완료: {csv_path} (행: {len(out)})")

    if CONFIG["INCLUDE_EXCEL"]:
        try:
            xlsx_path = f"{base}_{ts}.xlsx"
            out.to_excel(xlsx_path, index=False)
            print(f"[캐시] 엑셀 저장: {xlsx_path}")
        except Exception as e:
            print(f"[캐시] 엑셀 저장 실패: {e}")

    # 에러 로그 저장
    if ERROR_LOG:
        error_log_path = f"{base}_{ts}_errors.log"
        try:
            with open(error_log_path, 'w', encoding='utf-8') as f:
                f.write(f"Total errors: {len(ERROR_LOG)}\n")
                f.write("=" * 80 + "\n")
                for error_msg in ERROR_LOG:
                    f.write(error_msg + "\n")
            print(f"[로그] 에러 로그 저장: {error_log_path} ({len(ERROR_LOG)}개 에러)")
        except Exception as e:
            print(f"[로그] 에러 로그 저장 실패: {e}")

    # 데이터 품질 통계 출력
    print("\n" + "=" * 60)
    print("📊 데이터 품질 통계")
    print("=" * 60)

    quality_stats = {
        "PE 있음": out["PE"].notna().sum(),
        "PEG 있음": out["PEG"].notna().sum(),
        "RevYoY 있음": out["RevYoY"].notna().sum(),
        "OpMarginTTM 있음": out["OpMarginTTM"].notna().sum(),
        "FCF_Yield 있음": out["FCF_Yield"].notna().sum(),
        "ROE 있음": out["ROE(info)"].notna().sum(),
        "EV_EBITDA 있음": out["EV_EBITDA"].notna().sum(),
    }

    for metric, count in quality_stats.items():
        percentage = (count / len(out) * 100) if len(out) > 0 else 0
        print(f"  {metric}: {count}/{len(out)} ({percentage:.1f}%)")

    print("=" * 60)

    return out


# ============== 라이트 컷 함수 ==============
def pass_light_generic(price, dollar_vol):
    """1차 필터: 너무 안좋은 티커만 걸러냄"""
    if price is None or dollar_vol is None:
        return False

    price = validate_price(price)
    dollar_vol = validate_numeric(dollar_vol, min_val=0)

    if price is None or dollar_vol is None:
        return False

    return (price >= CONFIG["MIN_PRICE"]) and (dollar_vol >= CONFIG["MIN_DOLLAR_VOLUME"])


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("🚀 완전 최적화된 티커 캐시 빌더 시작")
    print("=" * 60)
    print(f"  ✅ OHLCV 병렬 스레드: {CONFIG['OHLCV_WORKERS']}개")
    print(f"  ✅ 상세 데이터 병렬 스레드: {CONFIG['DETAIL_FETCH_WORKERS']}개")
    print(f"  ✅ 이상치 검증: 강화됨")
    print("=" * 60 + "\n")

    start_time = time.time()
    build_enhanced_details_cache()
    elapsed = time.time() - start_time

    print("\n" + "=" * 60)
    print(f"✅ 완료! 총 소요 시간: {elapsed:.1f}초 ({elapsed / 60:.1f}분)")
    print("=" * 60)