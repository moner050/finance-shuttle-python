import yfinance as yf
from datetime import datetime, timedelta

# 티커 심볼 설정
ticker ="NVDA"

# 날짜 설정 (오늘로부터 3년 전)
end_date = datetime.now().strftime('%Y-%m-%d')
start_date = (datetime.now() - timedelta(days=3*365)).strftime('%Y-%m-%d')

# yfinance를 사용하여 데이터 다운로드
data = yf.download(ticker, start=start_date, end=end_date)

# CSV 파일로 저장
csv_file_name = f"{ticker}_{start_date}~{end_date}.csv"
data.to_csv("./yfinance-data/" + csv_file_name)

print(f"{ticker}의 최근 3년간 일봉 데이터가 '{csv_file_name}' 파일로 저장되었습니다.")