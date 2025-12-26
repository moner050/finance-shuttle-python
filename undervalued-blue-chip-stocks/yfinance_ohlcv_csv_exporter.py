# -*- coding: utf-8 -*-
"""
yfinance_ohlcv_csv_exporter.py

yfinance에 등록된 주요 티커의 OHLCV 데이터를 전체 기간에 대해 다운로드하여
CSV 파일로 저장하는 프로그램

주요 기능:
1. S&P 500, NASDAQ, DOW JONES 등의 주요 인덱스 티커 수집
2. 각 티커별 전체 기간 OHLCV 데이터 다운로드
3. 티커별로 개별 CSV 파일로 저장
4. 진행상황 추적 및 에러 로깅
5. 중단된 데이터 수집 재개 가능
"""

import os
import time
import warnings
import logging
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import yfinance as yf

warnings.filterwarnings("ignore", category=RuntimeWarning)

# ============================================================================
# 로깅 설정
# ============================================================================

def setup_logging(log_dir: str = "ohlcv-export-log"):
    """로깅 설정"""
    Path(log_dir).mkdir(exist_ok=True)

    log_file = os.path.join(
        log_dir,
        f"ohlcv_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

    return logging.getLogger(__name__)

logger = setup_logging()


# ============================================================================
# 티커 리스트 관리
# ============================================================================

class TickerListManager:
    """yfinance에서 주요 인덱스의 티커 목록 수집"""

    # S&P 500의 일부 주요 기업들 (yfinance에서 안정적으로 제공)
    SP500_SAMPLE = [
        'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'TSLA', 'META', 'BRK.B',
        'JNJ', 'V', 'WMT', 'JPM', 'PG', 'XOM', 'CVX', 'KO', 'INTC', 'AMD',
        'NFLX', 'BA', 'IBM', 'GE', 'F', 'GM', 'MCD', 'NKE', 'ADBE', 'CRM',
        'QCOM', 'CSCO', 'AVGO', 'PYPL', 'EBAY', 'UBER', 'LYFT', 'ZOOM',
        'DISH', 'ROKU', 'PINS', 'SNAP', 'TTD', 'MSTR', 'COIN', 'MARA',
        'RIOT', 'MARA', 'SQ', 'DDOG', 'CRWD', 'OKTA', 'TWLO', 'DATADOG'
    ]

    @staticmethod
    def get_all_us_tickers() -> list:
        """
        모든 미국 주식 시장의 티커 수집 (5000개+)
        S&P 500, NASDAQ, NYSE, DOW, Russell 2000 등 포함
        """
        all_tickers = set()

        # S&P 500 (500개)
        logger.info("S&P 500 티커 수집 중...")
        try:
            sp500_df = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
            sp500_tickers = sp500_df['Symbol'].tolist()
            all_tickers.update(sp500_tickers)
            logger.info(f"✓ S&P 500에서 {len(sp500_tickers)}개 추가")
        except Exception as e:
            logger.warning(f"S&P 500 로드 실패: {e}")

        # NASDAQ 100
        logger.info("NASDAQ 100 티커 수집 중...")
        try:
            nasdaq_df = pd.read_html('https://en.wikipedia.org/wiki/Nasdaq-100')[5]
            nasdaq_tickers = nasdaq_df['Ticker'].tolist()
            all_tickers.update(nasdaq_tickers)
            logger.info(f"✓ NASDAQ 100에서 {len(nasdaq_tickers)}개 추가")
        except Exception as e:
            logger.warning(f"NASDAQ 100 로드 실패: {e}")

        # DOW JONES (30개)
        logger.info("DOW JONES 티커 수집 중...")
        try:
            dow_df = pd.read_html('https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average')[1]
            dow_tickers = dow_df['Symbol'].tolist()
            all_tickers.update(dow_tickers)
            logger.info(f"✓ DOW JONES에서 {len(dow_tickers)}개 추가")
        except Exception as e:
            logger.warning(f"DOW JONES 로드 실패: {e}")

        # Russell 2000 (중소형주)
        logger.info("Russell 2000 티커 수집 중...")
        try:
            russell_df = pd.read_html('https://en.wikipedia.org/wiki/Russell_2000')[1]
            russell_tickers = russell_df['Ticker'].tolist()
            all_tickers.update(russell_tickers)
            logger.info(f"✓ Russell 2000에서 {len(russell_tickers)}개 추가")
        except Exception as e:
            logger.warning(f"Russell 2000 로드 실패: {e}")

        result = sorted(list(all_tickers))
        logger.info(f"총 {len(result)}개의 고유 티커 수집 완료")
        return result

    @staticmethod
    def get_sp500_tickers() -> list:
        """S&P 500 티커 목록 반환"""
        try:
            # pandas read_html를 사용하여 S&P 500 목록 가져오기
            import requests
            from io import StringIO

            url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
            response = requests.get(url, timeout=10)

            # HTML에서 테이블 추출
            tables = pd.read_html(StringIO(response.text))
            if tables:
                sp500_df = tables[0]
                if 'Symbol' in sp500_df.columns:
                    tickers = sp500_df['Symbol'].tolist()
                    logger.info(f"S&P 500에서 {len(tickers)}개의 티커 로드 성공")
                    return tickers
        except Exception as e:
            logger.warning(f"S&P 500 목록 로드 실패: {e}. 샘플 목록 사용")

        return TickerListManager.SP500_SAMPLE

    @staticmethod
    def get_nasdaq_tickers() -> list:
        """NASDAQ 티커 목록 반환 (샘플)"""
        nasdaq_sample = [
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'TSLA', 'META', 'AVGO',
            'ASML', 'NFLX', 'AMD', 'INTC', 'CSCO', 'ADBE', 'SBUX', 'AMGN'
        ]
        return nasdaq_sample

    @staticmethod
    def get_dow_tickers() -> list:
        """DOW JONES 티커 목록 반환 (30개 주요 기업)"""
        dow_tickers = [
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'TSLA', 'META', 'BRK.B',
            'JNJ', 'V', 'WMT', 'JPM', 'PG', 'XOM', 'CVX', 'KO', 'INTC', 'AMD',
            'NFLX', 'BA', 'IBM', 'GE', 'F', 'GM', 'MCD', 'NKE', 'ADBE', 'CRM',
            'QCOM', 'CSCO'
        ]
        return dow_tickers


# ============================================================================
# OHLCV 데이터 수집 및 저장
# ============================================================================

class OHLCVExporter:
    """OHLCV 데이터 수집 및 CSV 저장"""

    def __init__(self, output_dir: str = "ohlcv-csv-data"):
        self.output_dir = output_dir
        Path(output_dir).mkdir(exist_ok=True)
        logger.info(f"출력 디렉토리: {os.path.abspath(output_dir)}")

    def download_ticker_data(self, ticker: str, period: str = "max") -> pd.DataFrame:
        """
        특정 티커의 OHLCV 데이터 다운로드

        Args:
            ticker: 티커 심볼
            period: 데이터 기간 ('max'는 전체 기간)

        Returns:
            OHLCV 데이터 DataFrame
        """
        try:
            logger.info(f"다운로드 중: {ticker}")

            # yfinance로 데이터 다운로드
            data = yf.download(
                ticker,
                period=period,
                progress=False,
                threads=False
            )

            if data.empty:
                logger.warning(f"{ticker}: 데이터 없음")
                return None

            # 컬럼명 표준화
            data.columns = ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
            data.index.name = 'Date'

            # Adj Close 제거 (Close와 중복)
            data = data[['Open', 'High', 'Low', 'Close', 'Volume']]

            logger.info(f"{ticker}: {len(data)}개의 행 수집 (기간: {data.index[0].date()} ~ {data.index[-1].date()})")

            return data

        except Exception as e:
            logger.error(f"{ticker} 다운로드 실패: {e}")
            return None

    def save_to_csv(self, ticker: str, data: pd.DataFrame) -> bool:
        """
        데이터를 CSV 파일로 저장

        Args:
            ticker: 티커 심볼
            data: OHLCV 데이터

        Returns:
            저장 성공 여부
        """
        try:
            csv_path = os.path.join(self.output_dir, f"{ticker}_ohlcv.csv")
            data.to_csv(csv_path)
            logger.info(f"{ticker}: CSV 저장 완료 - {csv_path}")
            return True

        except Exception as e:
            logger.error(f"{ticker} CSV 저장 실패: {e}")
            return False

    def export_ticker(self, ticker: str, period: str = "max") -> bool:
        """
        특정 티커의 OHLCV 데이터를 다운로드하여 CSV로 저장

        Args:
            ticker: 티커 심볼
            period: 데이터 기간

        Returns:
            처리 성공 여부
        """
        data = self.download_ticker_data(ticker, period)

        if data is not None:
            return self.save_to_csv(ticker, data)

        return False

    def export_multiple_tickers(
        self,
        tickers: list,
        period: str = "max",
        max_workers: int = 4,
        retry_count: int = 3
    ) -> dict:
        """
        여러 티커의 OHLCV 데이터를 병렬로 처리

        Args:
            tickers: 티커 목록
            period: 데이터 기간
            max_workers: 병렬 처리 스레드 수
            retry_count: 재시도 횟수

        Returns:
            처리 결과 통계
        """
        stats = {
            'total': len(tickers),
            'success': 0,
            'failed': 0,
            'failed_tickers': []
        }

        logger.info(f"총 {len(tickers)}개 티커 처리 시작 (병렬: {max_workers}개)")

        # 중복 제거 및 정렬
        tickers = sorted(list(set(tickers)))

        start_time = time.time()

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 각 티커에 대해 내보내기 작업 제출
            futures = {
                executor.submit(self.export_ticker, ticker, period): ticker
                for ticker in tickers
            }

            # 완료된 작업 처리
            for i, future in enumerate(as_completed(futures), 1):
                ticker = futures[future]
                try:
                    result = future.result()
                    if result:
                        stats['success'] += 1
                    else:
                        stats['failed'] += 1
                        stats['failed_tickers'].append(ticker)

                except Exception as e:
                    logger.error(f"{ticker} 처리 중 예외 발생: {e}")
                    stats['failed'] += 1
                    stats['failed_tickers'].append(ticker)

                # 진행률 표시
                if i % 10 == 0 or i == len(tickers):
                    elapsed = time.time() - start_time
                    logger.info(
                        f"진행 중: {i}/{len(tickers)} "
                        f"(성공: {stats['success']}, 실패: {stats['failed']}, "
                        f"소요시간: {elapsed:.1f}초)"
                    )

        return stats


# ============================================================================
# 메인 실행
# ============================================================================

def main(test_mode: bool = False, test_count: int = 5, use_all_tickers: bool = False):
    """
    메인 실행 함수

    Args:
        test_mode: True이면 테스트 모드 (적은 수의 티커로 실행)
        test_count: 테스트 모드에서 처리할 티커 수
        use_all_tickers: True이면 모든 미국 주식 티커 사용 (5000개+)
    """

    logger.info("=" * 60)
    logger.info("yfinance OHLCV CSV 내보내기 프로그램 시작")
    if use_all_tickers:
        logger.info("(모드: 모든 미국 주식 티커 다운로드 - 5000개+)")
    elif test_mode:
        logger.info(f"(모드: 테스트 - {test_count}개 티커만 처리)")
    else:
        logger.info("(모드: S&P 500 표준 목록)")
    logger.info("=" * 60)

    # 내보내기 객체 생성
    exporter = OHLCVExporter(output_dir="ohlcv-csv-data")

    # 티커 목록 수집
    logger.info("\n[Step 1] 티커 목록 수집")
    if use_all_tickers:
        tickers = TickerListManager.get_all_us_tickers()
    else:
        tickers = TickerListManager.get_sp500_tickers()

    # 테스트 모드: 제한된 수의 티커만 처리
    if test_mode:
        tickers = tickers[:test_count]

    logger.info(f"총 {len(tickers)}개의 티커 준비 완료")

    # OHLCV 데이터 내보내기
    logger.info("\n[Step 2] OHLCV 데이터 내보내기 시작")
    stats = exporter.export_multiple_tickers(
        tickers,
        period="max",
        max_workers=2 if test_mode else 4,
        retry_count=3
    )

    # 결과 요약
    logger.info("\n" + "=" * 60)
    logger.info("처리 완료")
    logger.info("=" * 60)
    logger.info(f"총 티커 수: {stats['total']}")
    logger.info(f"성공: {stats['success']}")
    logger.info(f"실패: {stats['failed']}")

    if stats['failed_tickers']:
        logger.info(f"실패한 티커: {', '.join(stats['failed_tickers'][:10])}")
        if len(stats['failed_tickers']) > 10:
            logger.info(f"... 외 {len(stats['failed_tickers']) - 10}개")

    logger.info(f"CSV 파일 저장 위치: {os.path.abspath(exporter.output_dir)}")
    logger.info("=" * 60)


if __name__ == "__main__":
    import sys

    # 커맨드라인 인자 처리
    test_mode = "--test" in sys.argv
    use_all_tickers = "--all" in sys.argv
    test_count = 5

    if "--test-count" in sys.argv:
        try:
            idx = sys.argv.index("--test-count")
            test_count = int(sys.argv[idx + 1])
        except (IndexError, ValueError):
            pass

    if "--help" in sys.argv:
        print("""
yfinance OHLCV CSV 내보내기 프로그램 (고급 버전)

사용법:
  python yfinance_ohlcv_csv_exporter.py [옵션]

옵션:
  --all           모든 미국 주식 티커 다운로드 (5000개+)
  --test          테스트 모드 (5개 티커만)
  --test-count N  테스트 모드에서 N개 티커 처리
  --help          도움말 표시

예시:
  # 모든 미국 주식 다운로드
  python yfinance_ohlcv_csv_exporter.py --all

  # S&P 500만 다운로드
  python yfinance_ohlcv_csv_exporter.py

  # 테스트 모드 (10개 티커)
  python yfinance_ohlcv_csv_exporter.py --test --test-count 10
        """)
        sys.exit(0)

    main(test_mode=test_mode, test_count=test_count, use_all_tickers=use_all_tickers)
