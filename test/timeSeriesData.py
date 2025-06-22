import pandas as pd

# 문자열 리스트를 DateTimeIndex로 변환하기
dates = ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"]
dateTimeIndex = pd.to_datetime(dates)
print(dateTimeIndex[0])

# 인덱스를 사용하여 DateFrame 생성
data = {"Value":[100, 200, 300, 400, 500]}
df = pd.DataFrame(data, index=dateTimeIndex, columns=["Value"])
print(df)

# DatetimeIndex 를 이용해서 특정 데이터에 접근
print(df.loc["2024-01-01"])

# 날짜 범위로 데이터 슬라이싱
print(df.loc["2024-01-01":"2024-01-02"])

# 일정한 빈도의 날짜 범위생성
date_range = pd.date_range(start="2024-01-01", end="2024-01-10", freq="D")
print(date_range)
print(date_range[0], type(date_range[0]))

# 2시간 간격의 날짜 범위 생성
date_range = pd.date_range(start="2024-01-01", end="2024-01-10", freq="2h")
print(date_range)

# 애플의 일봉 데이터 불러오기
df = pd.read_csv("../yfinance-data/AAPL_2022-06-16~2025-06-15.csv", sep=r'\s*,\s*', header=0, engine='python')
print(df.head(3))

print(df["Date"])
print(type(df["Date"].values[0]))

# 새로 생성한 timestamp 를 인덱스로 사용하기
df["timestamp"] = pd.to_datetime(df["Date"])
df = df.set_index("timestamp")
print(df.head(3))

# 2024-03-11 ~ 2024-03-15 데이터만 슬라이싱
print(df.loc["2024-03-11":"2024-03-15"])

# shift 메소드 : 시계열 데이터를 시간적으로 앞이나 뒤로 이동시키는데 사용
df = pd.read_csv("../yfinance-data/AAPL_2022-06-16~2025-06-15.csv", sep=r'\s*,\s*', header=0, engine='python')
df["timestamp"] = pd.to_datetime(df["Date"])
df = df.set_index("timestamp")
print(df.head(5))

# Close 이전 시간 대 값 shift
df["Previous_Close"] = df["Close"].shift(1)
print(df.head(5))

# Clsoe 기준으로 전달 종가 대비 수익률을 계산
df["Daily_Return"] = (df["Close"] - df["Previous_Close"]) / df["Previous_Close"]
print(df.head(5))

# Rolling 메소드: 금융 데이터 분석에서 자주 사용되며, 시계열 데이터의 이동 평균 계산시 사용.
df = pd.read_csv("../yfinance-data/AAPL_2022-06-16~2025-06-15.csv", sep=r'\s*,\s*', header=0, engine='python')
df["timestamp"] = pd.to_datetime(df["Date"])
df = df.set_index("timestamp")
print(df.head(5))

# 5일 이동평균 구하기
print(df["Close"].rolling(window=5).mean().head(10))

# Close 기준으로 3일 이평선 계산하기
df["3_day_MA"] = df["Close"].rolling(window=3).mean()
print(df.head(5))

# Close 기준 5일 이동표준편차 계산
df["5_day_std"] = df["Close"].rolling(window=5).std()
print(df.head(10))

# 7일을 window로 최대값, 최소값 계산
df["7_day_max_hight"] = df["High"].rolling(window=7).max()
df["7_day_min_low"] = df["Low"].rolling(window=7).min()
print(df.head(10))

# 10일 볼린저 밴드 구하기
df = pd.read_csv("../yfinance-data/AAPL_2022-06-16~2025-06-15.csv", sep=r'\s*,\s*', header=0, engine='python')
df["timestamp"] = pd.to_datetime(df["Date"])
df = df.set_index("timestamp")
window_length = 10

df["Moving_Average"] = df["Close"].rolling(window=window_length).mean()
df["Moving_STD"] = df["Close"].rolling(window=window_length).std()
df["Upper_Bollinger"] = df["Moving_Average"] + (df["Moving_STD"] * 2)
df["Lower_Bollinger"] = df["Moving_Average"] - (df["Moving_STD"] * 2)

print(df.head(20))

# Resample : 시계열 데이터의 빈도를 변환할 때 사용됨.
# 예를 들어, 일별 데이터를 주별이나 월별 데이터로 집계할 수 있다.
# df = pd.read_csv("../yfinance-data/005930.KS_2025-06-19~2025-06-16.csv", sep=r'\s*,\s*', header=0, engine='python')
# CSV 파일 읽기
df = pd.read_csv("../yfinance-data/005930.KS_2025-06-19~2025-06-16.csv")

# Datetime 컬럼을 datetime 타입으로 변환
df["Datetime"] = pd.to_datetime(df["Datetime"])

# 숫자 컬럼들을 적절한 타입으로 변환
df[['Open', 'High', 'Low', 'Close', 'Volume']] = df[['Open', 'High', 'Low', 'Close', 'Volume']].astype(int)

# Datetime을 인덱스로 설정
df = df.set_index("Datetime")

# 1시간 단위로 Volume 합계 계산
hourly_volume = df["Volume"].resample("1h").sum().reset_index()

print(hourly_volume.head())

# 1분봉 데이터의 volume 에 대해 30분을 기준으로 평균, 합산, 최대, 최소 계산하기
mean_sum_max_min_volume = df["Volume"].resample("30min").agg(["mean", "sum", "max", "min"])

print(mean_sum_max_min_volume.head())

# 1분봉 데이터를 5분봉 데이터로 변환
resample_1min_to_5min = df.resample("5min").agg(
    {
        "Open":"first",
        "High":"max",
        "Low":"min",
        "Close":"last",
        "Volume":"sum"
    }
).reset_index()

print(resample_1min_to_5min.head())

# 1분봉 데이터를 1시간 봉 데이터로 변환
resample_1min_to_1h = df.resample("1h").agg(
    {
        "Open":"first",
        "High":"max",
        "Low":"min",
        "Close":"last",
        "Volume":"sum"
    }
).reset_index()

print(resample_1min_to_1h.head())