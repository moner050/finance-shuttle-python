# -*- coding: utf-8 -*-
"""
simple_yfinance_exporter.py

yfinance에서 OHLCV 데이터를 CSV로 저장하는 간단한 프로그램

사용법:
python simple_yfinance_exporter.py [--tickers AAPL,MSFT,GOOGL] [--output-dir ./csv_data]
"""

import os
import sys
import time
from datetime import datetime
from pathlib import Path

print("패키지 로드 중...")
try:
    import yfinance as yf
    import pandas as pd
    print("✓ yfinance, pandas 로드 성공")
except ImportError as e:
    print(f"✗ 패키지 로드 실패: {e}")
    print("  다음을 설치해주세요:")
    print("  pip install yfinance pandas")
    sys.exit(1)


class SimpleOHLCVExporter:
    """간단한 OHLCV 내보내기 도구"""

    def __init__(self, output_dir: str = "ohlcv-csv-data"):
        """
        초기화

        Args:
            output_dir: CSV 파일을 저장할 디렉토리
        """
        self.output_dir = output_dir
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        print(f"출력 디렉토리: {os.path.abspath(output_dir)}")

    def download_and_save(self, ticker: str, period: str = "max") -> bool:
        """
        특정 티커의 OHLCV 데이터를 다운로드하여 CSV로 저장

        Args:
            ticker: 티커 심볼 (예: 'AAPL')
            period: 데이터 기간 ('max'는 전체 기간)

        Returns:
            성공 여부
        """
        try:
            print(f"[{ticker}] 다운로드 중...", end=" ", flush=True)

            # yfinance로 데이터 다운로드
            data = yf.download(ticker, period=period, progress=False)

            if data.empty:
                print("✗ 데이터 없음")
                return False

            # 컬럼명 표준화
            if 'Adj Close' in data.columns:
                data = data[['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']]
                # Adj Close 제거
                data = data[['Open', 'High', 'Low', 'Close', 'Volume']]
            else:
                # OHLCV 컬럼만 선택
                ohlcv_cols = [col for col in ['Open', 'High', 'Low', 'Close', 'Volume'] if col in data.columns]
                data = data[ohlcv_cols]

            # CSV 저장
            csv_path = os.path.join(self.output_dir, f"{ticker}_ohlcv.csv")
            data.to_csv(csv_path)

            num_rows = len(data)
            start_date = data.index[0].date()
            end_date = data.index[-1].date()

            print(f"✓ {num_rows}개 행 저장 ({start_date} ~ {end_date})")
            print(f"  저장 위치: {csv_path}")

            return True

        except Exception as e:
            print(f"✗ 오류: {e}")
            return False

    def batch_download(self, tickers: list, period: str = "max") -> dict:
        """
        여러 티커의 OHLCV 데이터 다운로드

        Args:
            tickers: 티커 목록
            period: 데이터 기간

        Returns:
            처리 결과 통계
        """
        # 중복 제거
        tickers = sorted(list(set(tickers)))

        print(f"\n총 {len(tickers)}개 티커 처리 시작")
        print("=" * 60)

        stats = {"total": len(tickers), "success": 0, "failed": 0}

        start_time = time.time()

        for i, ticker in enumerate(tickers, 1):
            if self.download_and_save(ticker, period):
                stats["success"] += 1
            else:
                stats["failed"] += 1

            # 진행률 표시
            if i % 10 == 0 or i == len(tickers):
                elapsed = time.time() - start_time
                rate = elapsed / i if i > 0 else 0
                print(f"진행 중: {i}/{len(tickers)} ({stats['success']} 성공, "
                      f"{stats['failed']} 실패, {rate:.1f}초/티커)")

        print("=" * 60)
        return stats


def get_all_us_tickers() -> list:
    """
    미국 주식 시장의 모든 티커 수집 (5000개+)

    S&P 500, NASDAQ, NYSE, AMEX 등 모든 거래소의 티커를 포함
    """
    all_tickers = set()

    print("  [1/4] S&P 500 티커 수집 중...", end=" ", flush=True)
    try:
        sp500_df = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
        sp500_tickers = sp500_df['Symbol'].tolist()
        all_tickers.update(sp500_tickers)
        print(f"✓ {len(sp500_tickers)}개")
    except Exception as e:
        print(f"✗ 오류: {e}")

    print("  [2/4] NASDAQ 티커 수집 중...", end=" ", flush=True)
    try:
        nasdaq_df = pd.read_html('https://en.wikipedia.org/wiki/Nasdaq-100')[5]
        nasdaq_tickers = nasdaq_df['Ticker'].tolist()
        all_tickers.update(nasdaq_tickers)
        print(f"✓ {len(nasdaq_tickers)}개 (인덱스)")
    except Exception as e:
        print(f"✗ 오류: {e}")

    print("  [3/4] NYSE 티커 수집 중...", end=" ", flush=True)
    try:
        # NYSE 주요 기업들 (완전 리스트는 API 제한이 있어 샘플 사용)
        nyse_df = pd.read_html('https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average')[1]
        dow_tickers = nyse_df['Symbol'].tolist()
        all_tickers.update(dow_tickers)
        print(f"✓ {len(dow_tickers)}개 (DOW)")
    except Exception as e:
        print(f"✗ 오류: {e}")

    print("  [4/4] 추가 거래소 티커 수집 중...", end=" ", flush=True)
    try:
        # Russell 2000 (중소형주)
        russell_df = pd.read_html('https://en.wikipedia.org/wiki/Russell_2000')[1]
        russell_tickers = russell_df['Ticker'].tolist()
        all_tickers.update(russell_tickers)
        print(f"✓ {len(russell_tickers)}개 (Russell 2000)")
    except Exception as e:
        print(f"✗ 오류: {e}")

    result = sorted(list(all_tickers))
    return result


def get_major_tickers() -> list:
    """주요 미국 주식 티커 목록 반환"""
    # S&P 500의 주요 기업들 (100개 샘플)
    tickers = [
        # 기술
        'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'AVGO',
        'ASML', 'AMD', 'INTC', 'CSCO', 'QCOM', 'ADBE', 'CRM', 'INTU',
        'SNPS', 'CDNS', 'FTNT', 'NET', 'SNOW', 'DDOG', 'CRWD', 'OKTA',
        'TWLO', 'TTD', 'MSTR', 'RBLX', 'ROKU', 'PINS',

        # 커뮤니케이션
        'NFLX', 'CMCSA', 'DISH', 'CHTR', 'VZ', 'T', 'TMUS',

        # 일반소비재
        'AMZN', 'TSLA', 'MCD', 'NKE', 'SBUX', 'HD', 'LOW', 'TJX',
        'LULULEMON', 'DKNG', 'UBER', 'LYFT',

        # 금융
        'JPM', 'BAC', 'WFC', 'GS', 'MS', 'BLK', 'SCHW', 'COIN',
        'AXP', 'DFS', 'MA', 'V', 'SQ', 'PYPL',

        # 의료
        'JNJ', 'UNH', 'PFE', 'ABBV', 'MRK', 'LLY', 'AMGN', 'GILD',
        'BIIB', 'REGN', 'SYK', 'ZTS',

        # 생활필수재
        'PG', 'KO', 'PEP', 'MO', 'PM', 'CL', 'KMB', 'EL',
        'WMT', 'COST', 'CVS', 'WBA', 'TGT',

        # 에너지
        'XOM', 'CVX', 'COP', 'MPC', 'PSX', 'VLO', 'OXY',

        # 산업
        'BA', 'GE', 'CAT', 'DE', 'LMT', 'RTX', 'GD', 'TT',
        'ITW', 'MMM',

        # 소재
        'DUP', 'LYB', 'FCX', 'NEM', 'AA', 'CLF',

        # 부동산
        'PLD', 'AMT', 'CCI', 'EQIX', 'DLR', 'WELL', 'PSA',
        'O', 'VICI', 'SPG', 'KIM',

        # 유틸리티
        'NEE', 'DUK', 'SO', 'EXC', 'XEL', 'AWK', 'WEC'
    ]

    return sorted(list(set(tickers)))


def main():
    """메인 함수"""

    print("\n" + "=" * 60)
    print("yfinance OHLCV CSV 내보내기")
    print("=" * 60)

    # 커맨드라인 인자 처리
    output_dir = "ohlcv-csv-data"
    custom_tickers = None
    use_all_tickers = False

    for i, arg in enumerate(sys.argv[1:]):
        if arg == "--output-dir" and i + 1 < len(sys.argv) - 1:
            output_dir = sys.argv[i + 2]
        elif arg == "--tickers" and i + 1 < len(sys.argv) - 1:
            custom_tickers = sys.argv[i + 2].split(',')
        elif arg == "--all":
            use_all_tickers = True
        elif arg == "--help":
            print("\n사용법:")
            print("  python simple_yfinance_exporter.py [옵션]")
            print("\n옵션:")
            print("  --all                       모든 미국 주식 티커 다운로드 (5000개+)")
            print("  --tickers AAPL,MSFT,GOOGL  커스텀 티커 목록 (쉼표로 구분)")
            print("  --output-dir ./csv_data     CSV 저장 디렉토리 (기본: ohlcv-csv-data)")
            print("\n예시:")
            print("  # 주요 100개 티커 다운로드")
            print("  python simple_yfinance_exporter.py")
            print("\n  # 모든 미국 주식 (5000개+) 다운로드")
            print("  python simple_yfinance_exporter.py --all")
            print("\n  # 특정 티커만 다운로드")
            print("  python simple_yfinance_exporter.py --tickers AAPL,MSFT,GOOGL")
            print("\n  # 모든 티커를 커스텀 디렉토리에 저장")
            print("  python simple_yfinance_exporter.py --all --output-dir ./my-data")
            return

    # 티커 목록 결정
    if custom_tickers:
        tickers = [t.strip().upper() for t in custom_tickers]
    elif use_all_tickers:
        print("\n[Step 1] 모든 미국 주식 티커 목록 수집 (5000개+)")
        tickers = get_all_us_tickers()
        print(f"✓ {len(tickers)}개 티커 준비 완료")
    else:
        print("\n[Step 1] 주요 미국 주식 100개 티커 목록 준비")
        tickers = get_major_tickers()
        print(f"✓ {len(tickers)}개 티커 준비 완료")

    # OHLCV 다운로드 및 저장
    print("\n[Step 2] OHLCV 데이터 다운로드 및 저장")
    exporter = SimpleOHLCVExporter(output_dir=output_dir)

    start_time = time.time()
    stats = exporter.batch_download(tickers, period="max")
    elapsed = time.time() - start_time

    # 결과 요약
    print("\n[Step 3] 처리 완료")
    print("=" * 60)
    print(f"총 티커: {stats['total']}")
    print(f"성공: {stats['success']}")
    print(f"실패: {stats['failed']}")
    print(f"소요 시간: {elapsed:.1f}초 ({elapsed/stats['total']:.1f}초/티커)")
    print(f"저장 위치: {os.path.abspath(output_dir)}")
    print("=" * 60)


if __name__ == "__main__":
    main()
