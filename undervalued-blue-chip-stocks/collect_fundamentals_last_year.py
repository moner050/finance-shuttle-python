# -*- coding: utf-8 -*-

"""collect_fundamentals_last_year.py

- yfinance 를 이용해 약 30개 티커의 '현재 시점 스냅샷(info)'과
  '최근 1년(기본 365일) 분기 재무제표(손익/재무상태/현금흐름)'를 수집합니다.
- 결과는 out-dir 아래 CSV 2개로 저장합니다.

사용 예시:
  python undervalued-blue-chip-stocks/collect_fundamentals_last_year.py --out-dir fundamentals-output --lookback-days 365
  python undervalued-blue-chip-stocks/collect_fundamentals_last_year.py --tickers AAPL,MSFT,NVDA --out-dir fundamentals-output
"""

import argparse
import os
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

# info()는 필드가 계속 변동될 수 있으니, “주요 지표만 안전하게” 뽑아 저장
DEFAULT_INFO_FIELDS = [
    "symbol",
    "shortName",
    "longName",
    "exchange",
    "currency",
    "quoteType",
    "sector",
    "industry",

    "marketCap",
    "enterpriseValue",

    "currentPrice",
    "previousClose",
    "open",
    "dayLow",
    "dayHigh",
    "fiftyTwoWeekLow",
    "fiftyTwoWeekHigh",

    "trailingPE",
    "forwardPE",
    "priceToBook",
    "pegRatio",
    "beta",

    "profitMargins",
    "grossMargins",
    "operatingMargins",
    "ebitdaMargins",

    "returnOnAssets",
    "returnOnEquity",

    "revenueGrowth",
    "earningsGrowth",

    "totalRevenue",
    "ebitda",
    "netIncomeToCommon",

    "totalCash",
    "totalDebt",
    "debtToEquity",
    "currentRatio",
    "quickRatio",

    "dividendRate",
    "dividendYield",
    "payoutRatio",
]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def safe_get_info(t: yf.Ticker) -> dict:
    try:
        return t.get_info() or {}
    except Exception:
        return {}


def _coerce_statement_columns_to_datetime(df: pd.DataFrame) -> pd.DataFrame:
    # yfinance 재무제표 DF의 columns(기간 끝 날짜)이 Timestamp/str 섞여 나올 수 있어 통일
    out = df.copy()
    out.columns = pd.to_datetime(out.columns, errors="coerce")
    out = out.loc[:, out.columns.notna()]
    return out


def statement_to_long(
    df: pd.DataFrame,
    ticker: str,
    statement_type: str,
    cutoff_dt: datetime,
    fetched_at_utc: str,
) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["ticker", "statement", "period_end", "item", "value", "fetched_at_utc"])

    df2 = _coerce_statement_columns_to_datetime(df)
    if df2.empty:
        return pd.DataFrame(columns=["ticker", "statement", "period_end", "item", "value", "fetched_at_utc"])

    cutoff_date = pd.Timestamp(cutoff_dt.date())
    kept_cols = [c for c in df2.columns if pd.Timestamp(c.date()) >= cutoff_date]

    # 최근 1년치 컬럼이 하나도 안 잡히면(데이터 부족 등) 가장 최신 컬럼 1개라도 저장
    if not kept_cols:
        latest_col = sorted(df2.columns)[-1]
        kept_cols = [latest_col]

    df2 = df2[kept_cols].copy()
    df2.index = df2.index.astype(str)

    long_df = (
        df2.reset_index(names="item")
        .melt(id_vars=["item"], var_name="period_end", value_name="value")
    )

    long_df["value"] = pd.to_numeric(long_df["value"], errors="coerce")
    long_df = long_df.dropna(subset=["value"])

    long_df.insert(0, "ticker", ticker)
    long_df.insert(1, "statement", statement_type)
    long_df["period_end"] = pd.to_datetime(long_df["period_end"], errors="coerce").dt.date.astype(str)
    long_df["fetched_at_utc"] = fetched_at_utc

    return long_df[["ticker", "statement", "period_end", "item", "value", "fetched_at_utc"]]


def parse_tickers_arg(tickers_arg: str) -> list[str]:
    if not tickers_arg:
        return []
    return [x.strip().upper() for x in tickers_arg.split(",") if x.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Collect last 1y fundamental data (info snapshot + quarterly statements) via yfinance."
    )
    parser.add_argument("--out-dir", default="fundamentals-output", help="Output directory for CSV files.")
    parser.add_argument("--lookback-days", type=int, default=365, help="Lookback window in days (default: 365).")
    parser.add_argument("--tickers", default="", help="Comma-separated tickers. If omitted, uses built-in ~30 tickers.")
    parser.add_argument("--sleep-sec", type=float, default=0.4, help="Sleep between tickers to reduce rate limiting.")
    args = parser.parse_args()

    tickers = parse_tickers_arg(args.tickers) or DEFAULT_TICKERS
    out_dir = args.out_dir
    lookback_days = args.lookback_days
    sleep_sec = args.sleep_sec

    os.makedirs(out_dir, exist_ok=True)

    fetched_at = utc_now()
    fetched_at_utc = fetched_at.strftime("%Y-%m-%dT%H:%M:%SZ")
    cutoff_dt = fetched_at - timedelta(days=lookback_days)

    info_rows = []
    stmt_rows = []

    for i, symbol in enumerate(tickers, start=1):
        t = yf.Ticker(symbol)

        # 1) info snapshot
        info = safe_get_info(t)
        row = {"ticker": symbol, "fetched_at_utc": fetched_at_utc}
        for k in DEFAULT_INFO_FIELDS:
            row[k] = info.get(k)
        info_rows.append(row)

        # 2) quarterly statements (최근 1년치)
        try:
            income_q = getattr(t, "quarterly_income_stmt", None)
        except Exception:
            income_q = None

        try:
            balance_q = getattr(t, "quarterly_balance_sheet", None)
        except Exception:
            balance_q = None

        try:
            cashflow_q = getattr(t, "quarterly_cashflow", None)
        except Exception:
            cashflow_q = None

        stmt_rows.append(statement_to_long(income_q, symbol, "income_quarterly", cutoff_dt, fetched_at_utc))
        stmt_rows.append(statement_to_long(balance_q, symbol, "balance_quarterly", cutoff_dt, fetched_at_utc))
        stmt_rows.append(statement_to_long(cashflow_q, symbol, "cashflow_quarterly", cutoff_dt, fetched_at_utc))

        if sleep_sec > 0 and i < len(tickers):
            time.sleep(sleep_sec)

    info_df = pd.DataFrame(info_rows)
    stmt_df = (
        pd.concat([x for x in stmt_rows if x is not None and not x.empty], ignore_index=True)
        if stmt_rows else pd.DataFrame()
    )

    ts = fetched_at.strftime("%Y%m%d_%H%M%S")
    info_path = os.path.join(out_dir, f"fundamentals_info_snapshot_{ts}.csv")
    stmt_path = os.path.join(out_dir, f"fundamentals_statements_last{lookback_days}d_{ts}.csv")

    info_df.to_csv(info_path, index=False, encoding="utf-8-sig")
    stmt_df.to_csv(stmt_path, index=False, encoding="utf-8-sig")

    print(f"[OK] tickers={len(tickers)} lookback_days={lookback_days} fetched_at_utc={fetched_at_utc}")
    print(f"[SAVE] info: {info_path} (rows={len(info_df)})")
    print(f"[SAVE] statements: {stmt_path} (rows={len(stmt_df)})")


if __name__ == "__main__":
    main()
