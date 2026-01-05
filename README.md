# Python 금융 관련 도우미 (셔틀)

## 버전
- Python 3.10.11
  - Pandas 2.2.3
  - numpy 2.2.6
  - yfinance 0.2.43
  - openpyxl 3.1.5
- Pycharm 2025.1.1.1

- Pycharm IDE 및 Python 다운로드 링크
  - Python  
    https://www.python.org/downloads/release/python-31011/
  - Pycharm  
    https://www.jetbrains.com/ko-kr/pycharm/download/?section=windows

## 프로젝트 사용 전 명령어
```shell
pip install -U yfinance==0.2.43 pandas numpy matplotlib XlsxWriter openpyxl
```

## 프로젝트 목표
- 저평가 우량주 발굴
  - 미국 및 한국 저평가 우량주 찾기
  - 버핏과 같은 가치투자, 그리고 스켈핑 및 단타 종목에 유리한 종목들을 발굴 및 정리

- 주식 및 코인 자동매매 툴 개발
  - 사용자별 매매 전략 선택 및 해당 종목에 대한 전략 선택시 예상 수익률 계산
  - 일별 거래 수익률 데이터 볼 수 있는 백오피스 화면 개발
  - Slack 을 이용한 매수 및 매도 알림 기능 설정
  - 한국, 미국, 코인 자동매매 가능하도록 설정

## 1년 펀더멘털 수집 (yfinance)
- 약 30개 티커(기본값)를 고정해두고, 각 티커별로
  1) 현재 시점 fundamental 스냅샷(info)
  2) 현재 시점 기준 최근 1년 분기 재무제표(손익/재무상태/현금흐름)
  를 CSV로 저장합니다.

```shell
python undervalued-blue-chip-stocks/collect_fundamentals_last_year.py --out-dir fundamentals-output --lookback-days 365
```

- 티커를 직접 지정하고 싶으면:
```shell
python undervalued-blue-chip-stocks/collect_fundamentals_last_year.py --tickers AAPL,MSFT,NVDA,TSLA --out-dir fundamentals-output
```

## 1년 OHLCV 수집 (yfinance)
- 약 30개 티커(기본값)를 고정해두고, 각 티커별로 최근 1년 OHLCV(일봉)를 CSV로 저장합니다.
- 결과는 out-dir 아래에 "티커별 CSV"로 생성됩니다.

```shell
python undervalued-blue-chip-stocks/collect_ohlcv_last_year.py --out-dir ohlcv-output --lookback-days 365
```

- 티커를 직접 지정하고 싶으면:
```shell
python undervalued-blue-chip-stocks/collect_ohlcv_last_year.py --tickers AAPL,MSFT,NVDA,TSLA --out-dir ohlcv-output
```

## 일별 파생 펀더멘털(PER/PBR/시총)
- 목적: "일별 가격 변화"에 의해 변하는 파생 지표(시총/PER/PBR)를 1년치 시계열로 생성합니다.
- 구성:
  - 일봉 OHLCV: `yfinance.download()`로 수집
  - 주식수/TTM EPS/Book Value per Share: `Ticker.get_info()` 기준 "현재 시점" 값을 사용
  - 액면분할(Split) 이벤트: `Ticker.splits`를 이용해 분할만 반영(배당 조정은 반영하지 않음)

```shell
python undervalued-blue-chip-stocks/collect_derived_fundamentals_daily.py --out-dir derived-fundamentals-output --lookback-days 365
```

- 티커를 직접 지정하고 싶으면:
```shell
python undervalued-blue-chip-stocks/collect_derived_fundamentals_daily.py --tickers AAPL,MSFT,NVDA,TSLA --out-dir derived-fundamentals-output
```
