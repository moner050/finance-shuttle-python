# -*- coding: utf-8 -*-

"""collect_ohlcv_last_year.py

- yfinance 를 이용해 약 30개 티커의 '최근 1년(기본 365일) OHLCV(일봉)'를 수집합니다.
- 결과는 out-dir 아래에 '티커별 CSV'로 저장합니다.

사용 예시:
  python undervalued-blue-chip-stocks/collect_ohlcv_last_year.py --out-dir ohlcv-output --lookback-days 365
  python undervalued-blue-chip-stocks/collect_ohlcv_last_year.py --tickers AAPL,MSFT,NVDA --out-dir ohlcv-output

주의:
- auto_adjust=False 로 저장하면 Yahoo의 Adj Close 컬럼을 함께 받을 수 있습니다.
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

    out = out.reset_index()
    if "Date" in out.columns:
        out["Date"] = pd.to_datetime(out["Date"], errors="coerce").dt.date.astype(str)

    for c in cols:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    out = out.dropna(subset=["Date"])
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect last 1y OHLCV per ticker via yfinance.")
    parser.add_argument("--out-dir", default="ohlcv-output", help="Output directory for per-ticker CSV files.")
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
    start_dt = (now - timedelta(days=lookback_days)).date()
    end_dt = (now + timedelta(days=1)).date()

    start = start_dt.strftime("%Y-%m-%d")
    end = end_dt.strftime("%Y-%m-%d")

    ts = now.strftime("%Y%m%d_%H%M%S")

    total = len(tickers)
    saved = 0

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

        for t in batch:
            tdf = split_multiindex_ohlcv(df, t)
            if tdf.empty:
                print(f"  - [SKIP] {t}: empty")
                continue

            out_path = os.path.join(out_dir, f"{t}_ohlcv_{start_dt}_{end_dt}_{interval}_{ts}.csv")
            tdf.to_csv(out_path, index=False, encoding="utf-8-sig")
            saved += 1
            print(f"  - [SAVE] {t}: {out_path} (rows={len(tdf)})")

        if sleep_sec > 0 and (i + batch_size) < total:
            time.sleep(sleep_sec)

    print(f"[OK] total_tickers={total} saved={saved} start={start} end={end} interval={interval}")


if __name__ == "__main__":
    main()
