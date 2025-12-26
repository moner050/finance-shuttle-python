# yfinance OHLCV CSV 내보내기 프로그램

yfinance에서 주식 티커의 OHLCV(Open, High, Low, Close, Volume) 데이터를 CSV 파일로 저장하는 프로그램입니다.

## 특징

- **주요 미국 주식 데이터**: S&P 500의 주요 기업 100개 기본 제공
- **커스텀 티커 지원**: 원하는 티커 목록으로 다운로드 가능
- **전체 기간 데이터**: 각 티커의 모든 사용 가능한 거래 데이터 다운로드
- **개별 CSV 파일**: 각 티커별로 별도의 CSV 파일로 저장
- **진행상황 추적**: 실시간 진행률 및 처리 통계 표시
- **오류 처리**: 다운로드 실패 시 해당 티커만 재시도 가능

## 설치

### 1. 필수 패키지 설치

```bash
pip install yfinance==0.2.43 pandas multitasking
```

또는 README.md의 설치 명령어 사용:

```bash
pip install -U yfinance==0.2.43 pandas numpy matplotlib XlsxWriter openpyxl
```

**참고**: 더 최신 버전의 yfinance를 사용하려면 아래 추가 패키지 설치:

```bash
pip install beautifulsoup4 lxml requests
```

### 2. 패키지 확인

설치 후 다음 명령어로 확인:

```bash
python3 -c "import yfinance, pandas; print('✓ 설치 완료')"
```

## 사용법

### 기본 사용 (주요 100개 티커 다운로드)

```bash
python3 simple_yfinance_exporter.py
```

이 명령어는 S&P 500의 주요 기업 100개 티커의 OHLCV 데이터를 다운로드하여 `ohlcv-csv-data/` 디렉토리에 저장합니다.

### 모든 미국 주식 다운로드 (5000개+)

```bash
# 모든 미국 주식 (S&P 500, NASDAQ, DOW, Russell 2000 등)
python3 simple_yfinance_exporter.py --all
```

이 명령어는 다음 인덱스의 모든 티커를 수집하여 다운로드합니다:
- **S&P 500**: 약 500개 기업
- **NASDAQ 100**: 약 100개 기업
- **DOW JONES**: 30개 우량주
- **Russell 2000**: 약 2,000개 중소형주
- **총**: 약 5,000개 이상의 고유 티커

**예상 시간**: 200-500개 티커는 약 1-2시간, 5000개+ 모든 티커는 12-24시간 (인터넷 속도에 따라 다름)

### 특정 티커만 다운로드

```bash
# 콤마로 구분된 티커 목록
python3 simple_yfinance_exporter.py --tickers AAPL,MSFT,GOOGL,AMZN

# 한 두 개의 티커만 다운로드
python3 simple_yfinance_exporter.py --tickers AAPL,MSFT
```

### 출력 디렉토리 변경

```bash
python3 simple_yfinance_exporter.py --output-dir ./my-csv-data

# 또는 두 옵션 함께 사용
python3 simple_yfinance_exporter.py --all --output-dir ./my-csv-data
```

### 도움말 표시

```bash
python3 simple_yfinance_exporter.py --help
```

## 출력

### 디렉토리 구조

```
ohlcv-csv-data/
├── AAPL_ohlcv.csv
├── MSFT_ohlcv.csv
├── GOOGL_ohlcv.csv
└── ... (다른 티커들)
```

### CSV 파일 구조

각 CSV 파일의 구조:

```
Date,Open,High,Low,Close,Volume
2015-01-02,111.39,111.44,107.35,109.33,53204600
2015-01-05,106.37,110.00,106.25,106.63,64285200
2015-01-06,107.84,108.00,106.27,106.33,65797100
...
```

## 제공되는 티커

프로그램은 기본적으로 다음 범주의 주요 기업 100개 티커를 포함합니다:

- **기술 (TECH)**: AAPL, MSFT, GOOGL, AMZN, NVDA, META, TSLA, 등
- **통신 (COMMUNICATION)**: NFLX, CMCSA, DISH, VZ, T, 등
- **금융 (FINANCE)**: JPM, BAC, WFC, GS, MS, BLK, 등
- **의료 (HEALTHCARE)**: JNJ, UNH, PFE, ABBV, MRK, LLY, 등
- **소비재 (CONSUMER)**: MCD, NKE, SBUX, HD, LOW, 등
- **에너지 (ENERGY)**: XOM, CVX, COP, MPC, 등
- **산업 (INDUSTRIAL)**: BA, GE, CAT, DE, LMT, 등
- **부동산 (REAL ESTATE)**: PLD, AMT, CCI, EQIX, DLR, 등
- **유틸리티 (UTILITIES)**: NEE, DUK, SO, EXC, 등

더 많은 티커를 추가하려면 `get_major_tickers()` 함수를 수정하세요.

## 주의사항

### 다운로드 시간

- 100개 티커: 약 2-5분 (인터넷 속도에 따라 다름)
- 10개 티커: 약 15-30초

### API 제한

- yfinance는 공개 API를 사용하므로 과도한 요청 시 일시적으로 차단될 수 있습니다
- 대량 다운로드 시에는 프로그램이 자동으로 속도를 조절합니다

### 데이터 정확성

- 가격 데이터는 Yahoo Finance에서 제공합니다
- 일부 구형 또는 비유동성 종목은 데이터가 불완전할 수 있습니다

## 고급 사용법

### 프로그램 수정 (티커 목록 추가)

`simple_yfinance_exporter.py` 파일의 `get_major_tickers()` 함수를 수정하여 티커를 추가할 수 있습니다:

```python
def get_major_tickers() -> list:
    """주요 미국 주식 티커 목록 반환"""
    tickers = [
        # 기존 티커들...
        'YOUR_TICKER',  # 새로운 티커 추가
        'ANOTHER_TICKER',  # 또 다른 티커
    ]
    return sorted(list(set(tickers)))
```

### 기간 제한 (최근 데이터만 다운로드)

프로그램을 수정하여 다운로드 기간을 제한할 수 있습니다. `batch_download` 메서드의 `period` 매개변수를 변경하세요:

```python
# '1y' = 1년, '6mo' = 6개월, '3mo' = 3개월, '1mo' = 1개월
exporter.batch_download(tickers, period="1y")  # 최근 1년만 다운로드
```

## 문제 해결

### "No module named 'yfinance'" 에러

yfinance가 설치되지 않았습니다. 설치 섹션을 참고하여 설치하세요:

```bash
pip install yfinance==0.2.43
```

### "No module named 'pandas'" 에러

pandas가 설치되지 않았습니다:

```bash
pip install pandas
```

### 다운로드 속도가 느림

- 인터넷 연결 확인
- 한 번에 적은 수의 티커로 시도 (예: 5-10개)
- 다시 시도하기 전에 몇 분 대기

### 특정 티커 다운로드 실패

일부 티커는 yfinance에서 지원하지 않거나 현재 데이터를 사용할 수 없을 수 있습니다. 다른 티커를 시도해보세요.

## 파일 설명

### simple_yfinance_exporter.py (권장)

간단하고 사용하기 쉬운 메인 프로그램입니다. 다음 기능을 지원합니다:

```bash
# 주요 100개 티커 (기본)
python3 simple_yfinance_exporter.py

# 모든 미국 주식 (5000개+)
python3 simple_yfinance_exporter.py --all

# 특정 티커만
python3 simple_yfinance_exporter.py --tickers AAPL,MSFT,GOOGL

# 커스텀 출력 디렉토리
python3 simple_yfinance_exporter.py --all --output-dir ./data
```

### yfinance_ohlcv_csv_exporter.py (고급)

더 고급 기능을 가진 버전입니다. 다음 특징을 포함합니다:
- 병렬 처리로 더 빠른 다운로드
- 상세 로깅 및 재시도 로직
- 다운로드 중 발생한 오류 추적
- 테스트 모드 지원

사용법:

```bash
# 테스트 모드 (5개 티커)
python3 yfinance_ohlcv_csv_exporter.py --test

# 테스트 모드 (10개 티커)
python3 yfinance_ohlcv_csv_exporter.py --test --test-count 10

# S&P 500 다운로드
python3 yfinance_ohlcv_csv_exporter.py

# 모든 미국 주식 다운로드 (5000개+)
python3 yfinance_ohlcv_csv_exporter.py --all

# 도움말
python3 yfinance_ohlcv_csv_exporter.py --help
```

## 라이센스

이 프로그램은 Python Finance Shuttle 프로젝트의 일부입니다.

## 참고

- [yfinance 공식 문서](https://github.com/ranaroussi/yfinance)
- [pandas 공식 문서](https://pandas.pydata.org/)
- [Yahoo Finance](https://finance.yahoo.com/)
