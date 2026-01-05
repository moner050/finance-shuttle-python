# -*- coding: utf-8 -*-

"""collect_derived_fundamentals_daily.py

목표
- yfinance 가격(일봉) + 현재 시점 재무 스냅샷(주식수/TTM EPS/Book Value per Share)을 결합해서
  '일별로 변하는 파생 펀더멘털 지표' 시계열을 생성합니다.

생성 지표
- MarketCap(USD): 일별 시총 (가격 * 주식수)
- PER: 일별 PER (가격 / trailingEps)
- PBR: 일별 PBR (가격 / bookValue)

중요한 제약
- trailingEps / bookValue / sharesOutstanding 는 yfinance get_info() 기준 "현재 시점" 값이며,
  과거 발표 시점으로 롤백된(point-in-time) 재무값이 아닙니다.
  따라서 결과는 "현재 기준 값으로 과거 가격에 재적용"한 파생 시계열입니다.

가격 처리
- auto_adjust=False 로 Close(원시 가격)를 받습니다.
- 액면분할(Split) 이벤트는 Ticker.splits 로 가져와서, 분할 이전 구간에만 분할 계수를 적용해
  '분할만 반영된(price_split_adj)' 가격을 생성합니다.
  (배당에 따른 조정은 반영하지 않습니다.)

사용 예시
  python undervalued-blue-chip-stocks/collect_derived_fundamentals_daily.py --out-dir derived-fundamentals-output --lookback-days 365
  python undervalued-blue-chip-stocks/collect_derived_fundamentals_daily.py --tickers AAPL,MSFT,NVDA --out-dir derived-fundamentals-output
"""

import argparse
import os
import random
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import yfinance as yf


DEFAULT_TICKERS = [
    "AAPL", "MSFT", "AMZN", "GOOGL", "META",
    "NVDA", "TSLA", "BRK-B", "JPM", "V",
    "MA", "UNH", "JNJ", "XOM", "PG",
    "HD", "CVX", "LLY", "ABBV", "KO",
    "PEP", "COST", "MCD", "NKE", "DIS",
    "CRM", "ADBE", "WMT", "INTC", "CSCO",
]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_tickers_arg(tickers_arg: str) -> list[str]:
    if not tickers_arg:
        return []
    return [x.strip().upper() for x in tickers_arg.split(",") if x.strip()]


def safe_get_info(t: yf.Ticker, max_retries: int = 3) -> dict:
    last_err = None
    for attempt in range(max_retries):
        try:
            info = t.get_info() or {}
            if info:
                return info
            last_err = RuntimeError("Empty info")
        except Exception as e:
            last_err = e

        backoff = (2 ** attempt) * 0.6 + random.uniform(0, 0.4)
        time.sleep(backoff)

    return {"_error": str(last_err)}


def safe_yf_download(
    tickers: list[str],
    start: str,
    end: str,
    interval: str,
    timeout: int,
    threads: bool,
    max_retries: int = 3,
    sleep_base_sec: float = 0.8,
) -> pd.DataFrame:
    last_err = None

    for attempt in range(max_retries):
        try:
            df = yf.download(
                tickers=tickers,
                start=start,
                end=end,
                interval=interval,
                auto_adjust=False,
                actions=False,
                progress=False,
                threads=threads,
                group_by="column",
                timeout=timeout,
            )
            if df is not None and not df.empty:
                return df
            last_err = RuntimeError("Empty DataFrame")
        except Exception as e:
            last_err = e

        backoff = (2 ** attempt) * sleep_base_sec + random.uniform(0, 0.5)
        time.sleep(backoff)

    raise RuntimeError(f"yfinance download failed after {max_retries} retries: {last_err}")


def split_multiindex_ohlcv(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        if ticker not in set(df.columns.get_level_values(1)):
            return pd.DataFrame()
        out = df.xs(ticker, axis=1, level=1, drop_level=True).copy()
    else:
        out = df.copy()

    wanted = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]
    cols = [c for c in wanted if c in out.columns]
    out = out[cols].copy()

    return out


def _build_split_cum_factor(index: pd.DatetimeIndex, splits: pd.Series) -> pd.Series:
    """분할 이벤트를 기반으로, 날짜별 '미래 분할 누적 계수'를 생성합니다.

    - 목적: 분할일 당일 이후는 계수 적용 X, 분할일 이전 구간에만 적용
    - 구현: 분할일을 index에서 찾아 "분할일 직전 거래일"에 ratio를 곱한 뒤, 뒤에서부터 누적곱
    """

    if index is None or len(index) == 0:
        return pd.Series([], dtype=float)

    factor_daily = pd.Series(1.0, index=index)

    if splits is None or splits.empty:
        return factor_daily

    try:
        splits2 = splits.dropna().copy()
        splits2.index = pd.to_datetime(splits2.index, errors="coerce")
        splits2 = splits2[splits2.index.notna()]
    except Exception:
        return factor_daily

    for split_dt, ratio in splits2.items():
        try:
            ratio_f = float(ratio)
            if ratio_f <= 0:
                continue

            # split_dt가 index에 없을 수 있으므로 삽입 위치 기반으로 처리
            pos = int(index.searchsorted(split_dt))

            # split_dt 당일은 보통 분할 이후 가격이므로, '직전 거래일'에만 적용되게 pos-1 사용
            apply_pos = pos - 1
            if apply_pos >= 0 and apply_pos < len(index):
                factor_daily.iloc[apply_pos] = float(factor_daily.iloc[apply_pos]) * ratio_f
        except Exception:
            continue

    cum_factor = factor_daily.iloc[::-1].cumprod().iloc[::-1]
    return cum_factor


def build_derived_daily_df(
    ticker: str,
    ohlcv_df: pd.DataFrame,
    shares_outstanding: float | None,
    trailing_eps: float | None,
    book_value: float | None,
    splits: pd.Series,
    fetched_at_utc: str,
) -> pd.DataFrame:
    if ohlcv_df is None or ohlcv_df.empty:
        return pd.DataFrame()

    df = ohlcv_df.copy()
    df.index = pd.to_datetime(df.index, errors="coerce")
    df = df[df.index.notna()].sort_index()

    if "Close" not in df.columns:
        return pd.DataFrame()

    cum_factor = _build_split_cum_factor(df.index, splits)

    df["split_cum_factor"] = pd.to_numeric(cum_factor, errors="coerce").fillna(1.0)
    df["price_split_adj"] = pd.to_numeric(df["Close"], errors="coerce") / df["split_cum_factor"].replace(0, pd.NA)

    so = None
    try:
        so = float(shares_outstanding) if shares_outstanding is not None else None
        if so is not None and so <= 0:
            so = None
    except Exception:
        so = None

    eps = None
    try:
        eps = float(trailing_eps) if trailing_eps is not None else None
        if eps is not None and eps <= 0:
            eps = None
    except Exception:
        eps = None

    bv = None
    try:
        bv = float(book_value) if book_value is not None else None
        if bv is not None and bv <= 0:
            bv = None
    except Exception:
        bv = None

    if so is not None:
        df["market_cap"] = df["price_split_adj"] * so
    else:
        df["market_cap"] = pd.NA

    if eps is not None:
        df["pe"] = df["price_split_adj"] / eps
    else:
        df["pe"] = pd.NA

    if bv is not None:
        df["pb"] = df["price_split_adj"] / bv
    else:
        df["pb"] = pd.NA

    out = df.reset_index().rename(columns={"index": "Date"})
    out["Date"] = pd.to_datetime(out["Date"], errors="coerce").dt.date.astype(str)

    out.insert(0, "ticker", ticker)
    out["fetched_at_utc"] = fetched_at_utc
    out["shares_outstanding_used"] = so
    out["trailing_eps_used"] = eps
    out["book_value_used"] = bv

    wanted_cols = [
        "ticker",
        "Date",
        "Open",
        "High",
        "Low",
        "Close",
        "Adj Close",
        "Volume",
        "split_cum_factor",
        "price_split_adj",
        "shares_outstanding_used",
        "trailing_eps_used",
        "book_value_used",
        "market_cap",
        "pe",
        "pb",
        "fetched_at_utc",
    ]

    final_cols = [c for c in wanted_cols if c in out.columns]
    out = out[final_cols].copy()

    numeric_cols = [c for c in out.columns if c not in {"ticker", "Date", "fetched_at_utc"}]
    for c in numeric_cols:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Build derived daily fundamentals time-series (MarketCap/PE/PB) using yfinance.")
    parser.add_argument("--out-dir", default="derived-fundamentals-output", help="Output directory for per-ticker CSV files.")
    parser.add_argument("--lookback-days", type=int, default=365, help="Lookback window in days (default: 365).")
    parser.add_argument("--tickers", default="", help="Comma-separated tickers. If omitted, uses built-in ~30 tickers.")
    parser.add_argument("--interval", default="1d", help="yfinance interval (default: 1d).")
    parser.add_argument("--batch-size", type=int, default=20, help="Batch size for yfinance.download (default: 20).")
    parser.add_argument("--timeout", type=int, default=30, help="Request timeout seconds (default: 30).")
    parser.add_argument("--threads", action="store_true", help="Enable yfinance internal threading (default: off).")
    parser.add_argument("--sleep-sec", type=float, default=0.2, help="Sleep between batches (default: 0.2).")
    args = parser.parse_args()

    tickers = parse_tickers_arg(args.tickers) or DEFAULT_TICKERS
    out_dir = args.out_dir
    lookback_days = args.lookback_days
    interval = args.interval
    batch_size = args.batch_size
    timeout = args.timeout
    threads = args.threads
    sleep_sec = args.sleep_sec

    os.makedirs(out_dir, exist_ok=True)

    now = utc_now()
    fetched_at_utc = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    start_dt = (now - timedelta(days=lookback_days)).date()
    end_dt = (now + timedelta(days=1)).date()

    start = start_dt.strftime("%Y-%m-%d")
    end = end_dt.strftime("%Y-%m-%d")

    ts = now.strftime("%Y%m%d_%H%M%S")

    total = len(tickers)
    saved = 0

    # 티커별 info/splits는 개별 호출이 필요하므로 미리 수집
    meta = {}
    for tkr in tickers:
        try:
            t = yf.Ticker(tkr)
            info = safe_get_info(t)
            splits = pd.Series(dtype=float)
            try:
                splits = t.splits
            except Exception:
                splits = pd.Series(dtype=float)

            meta[tkr] = {
                "sharesOutstanding": info.get("sharesOutstanding"),
                "trailingEps": info.get("trailingEps"),
                "bookValue": info.get("bookValue"),
                "splits": splits,
            }
        except Exception:
            meta[tkr] = {
                "sharesOutstanding": None,
                "trailingEps": None,
                "bookValue": None,
                "splits": pd.Series(dtype=float),
            }

    for i in range(0, total, batch_size):
        batch = tickers[i:i + batch_size]
        print(f"[DOWNLOAD] batch {i // batch_size + 1} / {(total + batch_size - 1) // batch_size} : {batch}")

        df = safe_yf_download(
            tickers=batch,
            start=start,
            end=end,
            interval=interval,
            timeout=timeout,
            threads=threads,
        )

        for tkr in batch:
            ohlcv = split_multiindex_ohlcv(df, tkr)
            if ohlcv.empty:
                print(f"  - [SKIP] {tkr}: OHLCV empty")
                continue

            m = meta.get(tkr, {})
            derived = build_derived_daily_df(
                ticker=tkr,
                ohlcv_df=ohlcv,
                shares_outstanding=m.get("sharesOutstanding"),
                trailing_eps=m.get("trailingEps"),
                book_value=m.get("bookValue"),
                splits=m.get("splits", pd.Series(dtype=float)),
                fetched_at_utc=fetched_at_utc,
            )

            if derived.empty:
                print(f"  - [SKIP] {tkr}: derived empty")
                continue

            out_path = os.path.join(out_dir, f"{tkr}_derived_fundamentals_{start_dt}_{end_dt}_{interval}_{ts}.csv")
            derived.to_csv(out_path, index=False, encoding="utf-8-sig")
            saved += 1
            print(f"  - [SAVE] {tkr}: {out_path} (rows={len(derived)})")

        if sleep_sec > 0 and (i + batch_size) < total:
            time.sleep(sleep_sec)

    print(f"[OK] total_tickers={total} saved={saved} start={start} end={end} interval={interval}")


if __name__ == "__main__":
    main()
