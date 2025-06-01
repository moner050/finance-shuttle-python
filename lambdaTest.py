import pandas as pd

# 더하기 함수
add = lambda x, y: x + y
print(add(1, 2))

# 정렬 함수
points = [(1,2), (3,1), (5,4)]
points.sort(key = lambda point: point[1])
print(points)

# map 함수와 함께 사용해 각 요소에 연산 적용
nums = [1,2,3,4,5]
squared = map(lambda x: x ** 2, nums)
print(list(squared))

# filter 사용해 각 요소에 연산 적용
nums = [1,2,3,4,5,6,7,8,9]
even = filter(lambda x: x % 2 == 0, nums)
print(list(even))

# pandas 에 lambda 사용해보기
df = pd.DataFrame({
    'A': [1,2,3],
    'B': [4,5,6]
})
print(df)

# 'A' 열의 각 값에 10 더하기
df['A'] = df['A'].apply(lambda x : x + 10)
print(df)
# 'C' 라는 열 추가해 10보다 크면 large, 작으면 small 단어 넣어주기
df['C'] = df['A'].apply(lambda x : "large" if x > 11 else "small")
print(df)
# "SUM" 이라는 열에
df["sum"] = df.apply(
    lambda row : row["A"] + row["B"], axis=1
)
print(df)

# Series 에서 특정 조건을 만족하는 요소 필터링
s = pd.Series([1,2,3,4,5])
filtered_s = s[s.apply(lambda x : x > 2)]
print(filtered_s)

# lambda 또는 Python 함수 사용해 가격 변동률 계산
data = pd.read_csv("./test-file/test_appl.csv", index_col = 0)
print(data.head(3))
data["PriceChangePersent"] = data.apply(
    lambda row : (row['Close'] - row['Open']) / row['Open'] * 100, axis=1
)
print(data.head())
def calculate_price_change_persent(row):
    return (row['Close'] - row['Open']) / row['Open'] * 100
data["PriceChangePersent"] = data.apply(calculate_price_change_persent, axis=1)
print(data.head())

# pandas 의 DataFrame 의 index 를 기본정수 index 로 재설정 하기
df = pd.read_csv("./test-file/test_appl.csv", index_col = 0)
df = df.reset_index()
print(df.head())
df = df.set_index("Date")
print(df.head())
df = df.reset_index()
print(df.head())
# index 기준으로 특정 행 지우기'
print(df.drop(index=[1]))
# index 로 행 지운 다음 index 재조정
print(df.drop(index=[1]).reset_index())
# index 가 새로운 열로 추가되지 않게 하기
print(df.drop(index=[1]).reset_index(drop=True))

# 하나 이상의 행과 열을 합치기
df_appl = pd.read_csv("./yfinance-data/AAPL.csv", index_col = 0).head(5)
df_nvda = pd.read_csv("./yfinance-data/NVDA.csv", index_col = 0).head(5)

print(df_appl)
print(df_nvda)

# DataFrame 을 가로로 연결하기
pd.concat([df_appl, df_nvda], axis = 1)
# DataFrame 을 세로로 연결하기
pd.concat([df_appl, df_nvda], axis = 0)
# DataFrame 을 세로로 연결하기 (원래 인덱스는 무시)
pd.concat([df_appl, df_nvda], axis = 0, ignore_index = True)