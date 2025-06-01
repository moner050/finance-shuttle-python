import pandas as pd
import numpy as np

data = {
    "Name": ["Jacky", "Steven", "Gregory"],
    "Age": [38, 28, 58],
    "Driver": [True, False, False]
}

# 데이터 프레임 정의
df = pd.DataFrame(data)
print(df)

# 데이터 프레임 및 해당 데이터 요소의 타입 출력
print( type(df) )           # <class 'pandas.core.frame.DataFrame'>
print( type(df["Age"]))     # <class 'pandas.core.series.Series'>
print( df.index)            # RangeIndex(start=0, stop=3, step=1)
print(df)
df.index.name = "index"
print(df)
df.reset_index()
print(df)

# 인덱스 명칭 추가
df.index = ["a","b","c"]
df.index.name = "index"
print(df)

# 새로운 컬럼 데이터 추가
df["Location"] = ["Area 1","Area 2","Area 3"]
print(df)

# 컬럼 이름 변경
df = df.rename(columns={"Name":"Person"})
print(df)

# 컬럼 삭제하기
df = df.drop(columns="Location")
print(df)

# 새로운 데이터 프레임 생성
data = {
    "Name": ["Harry"],
    "Age": [10],
    "Driver": [True]
}

new_df = pd.DataFrame(data)
print(new_df)

# 새로운 데이터 행 추가
df = pd.concat([df, new_df])
print(df)

data = {
    "Name": ["Jacky", "Steven", "Gregory", "Harry"],
    "Age": [38, 28, 10, 23],
    "Driver": [True, False, False, True]
}

df = pd.DataFrame(data)
df.index = ["a","b","c","d"]
df.index.name = "index"

print(df)

# 특정 열 선택
print(df["Name"])
# 여러 열 선택
print(df[["Name", "Age"]])
# 여러 행 선택
print(df[0:2])

# index 가 a 인 행 선택하기
print(df.loc["a"])
# index 가 a 인 행에서 Name 데이터 선택하기
print(df.loc["a","Name"])
print(df.loc["a"]["Name"])

# 2번째 행 데이터 접근하기
print(df.iloc[1])
# 2번째 행의 Age 데이터 접근하기
print(df.iloc[1]["Age"])
print(df.iloc[1, 1])

# Age 값이 25 보다 크거나 같은 값 출력
print(df["Age"] >= 25)
print(df[df["Age"] >= 25])

# Index 레이블 및 컬럼 레이블 값으로 접근하기
print(df.at["a", "Driver"])
print(df.iat[0, 2])

# .at .iat 으로 특정 셀의 값 변경하기
print(df)
df.at["a", "Driver"] = False
print(df)

# csv 데이터 불러오기
csv_data = pd.read_csv("./test-file/test_appl.csv", index_col=0)
csv_data.head(3)
print(csv_data)
# 데이터 csv 로 저장
csv_data.to_csv("./test-file/test-save.csv")
print(csv_data)

# Excel 데이터 읽기
excel_data = pd.read_excel("./test-file/test_appl.xlsx", sheet_name="appl", engine="openpyxl", index_col=0)
excel_data.head(3)
# data를 Excel 로 내보내기
excel_data.to_excel("./test-file/test-save.xlsx", sheet_name="appl", engine="openpyxl")

# 데이터 정보 요약하기
data = pd.read_csv("./test-file/test_appl.csv", index_col=0)
print(data.describe())

# 데이터 산술연산 하기
print(data["Open"] / data["Close"])
print(data["Open"] + data["Close"])

# 데이터 통계연산 하기
print(data["Open"].min())   # 최솟값
print(data["Open"].sum())   # 합계
print(data["Open"].max())   # 최대값

# 매일 시초에 사서 고가에 팔았을때를 가정했을 때 수익률 계산
profit = data["High"] / data["Open"]
print(profit.head(3))
# 누적 수익률 계산하기
print(profit.cumprod().head())

# 전날과 시초값 차이 계산하기
print(data["Open"].diff().head())
print(data["Open"].diff(2).head())

# dataFrame 을 이용해 전체를 거래량 열에 대해 정렬
print(data.sort_values(by="Volume").head()) # 오름차순
print(data.sort_values(by="Volume", ascending=False).head())    # 내림차순
# index 기준으로 같이 정렬
print(data.sort_values(by="Volume", ascending=False).sort_index())    # 내림차순
# 컬럼 하나만 정렬하기
print(data["Open"].sort_values().head())

# 결측치(Nan) 데이터 처리하기
df = pd.DataFrame({
    "A": [1, 2, np.nan, 4],
    "B": [5, np.nan, np.nan, 8],
    "C": [10, 20, 30, 40]
})
print(df)
# 결측치 데이터 확인하기
print(df.isnull())
print(df.isnull().values.any()) # 하나라도 Nan 이면 True
print(df.isnull().sum())    # 각 열별 결측치 데이터 개수 확인하기
# 모든 결측치 데이터 0으로 채우기
data_fill_zero = df.fillna(0)
print(data_fill_zero)
# 결측치 데이터를 특정 값으로 변경하기
df_replaced = df.replace(np.nan, -1)
print(df_replaced)
# 결측치 데이터를 앞방향 데이터로 채우기
df_fill_forward = df.fillna(method="ffill")
print(df_fill_forward)
# 결측치 데이터를 뒷방향 데이터로 채우기
df_fill_backward = df.fillna(method="bfill")
print(df_fill_backward)
# 결측지 데이터를 해당 열의 평균치 값으로 채우기
df_filled_mean = df.copy()
df_filled_mean["B"] = df_filled_mean["B"].fillna(df_filled_mean["B"].mean())
print(df_filled_mean)
# 결측치를 보유한 행 제거
print(df.dropna())
# 결측치를 보유한 열 제거
print(df.dropna(axis=1))

# index 를 기반으로 데이터 제거하기
print(df.drop(index=[1,2]))

# List 의 각 숫자 제곱하기
numbers = [1,2,3,4,5]
print([num ** 2 for num in numbers])
# Map 을 사용해서 각 숬자 제곱하기
def square(number):
    return number ** 2
square_numbers = map(square, numbers)
print(list(square_numbers))

# pandas 에서 map 함수 사용하기
s = pd.Series([1,2,3])
# 각 원소에 10 곱하기
def multiply_by_ten(x):
    return x * 10
result_series = s.map(multiply_by_ten)
print(result_series)

# 사전을 사용한 매핑
s = pd.Series(["Apple", "Banana", "Carrot"])
fruit_colors = {
    "Apple": "red",
    "Banana": "yellow",
    "Carrot": "orange"
}
result_fruit_colors = s.map(fruit_colors)
print(result_fruit_colors)

# apply 함수 사용하기
df = pd.DataFrame({
    "A": [1,2,3],
    "B": [10,13,15],
})
# 최대값과 최솟값의 차 구하는 함수
def diff_max_min(x):
    return x.max() - x.min()
# axis = 0 이면 함수가 각 열에 독립적으로 적용됨
print(df.apply(diff_max_min, axis=0))
# axis = 1 이면 함수가 각 행에) 독립적으로 적용됨
print(df.apply(diff_max_min, axis=1))

# apply 를 이용해 새로운 컬럼 생성하기
df = pd.DataFrame({
    "name": ["John", "Lucy", "Mark", "Jane"],
    "age": [20, 22, 35, 15]
})
def age_category(age):
    if age < 18:
        return "Underage"
    elif age < 30:
        return "Young"
    else:
        return "Adult"
df["age_category"] = df["age"].apply(age_category)
print(df)