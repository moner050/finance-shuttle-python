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
