import pandas as pd

# Pandas 의 groupby 는 데이터를 특정 카테고리로 분류 후 그룹에 대해 집계, 변환, 필터링 등의 연산을 제공함
df = pd.read_csv("./test-file/titanic.csv", index_col=0)
print(df.head())

# 좌석등급을 기준으로 살아남은 사람 카운트
print(df.groupby("Pclass")["Survived"].sum().reset_index())
# 생존 여부에 따른 티켓 가격 평균 구하기
print(df.groupby("Survived")["Fare"].mean().reset_index())
# 좌석등급, 생존 여부에 따른 티켓 가격 평균 구하기
print(df.groupby(["Pclass" ,"Survived"])["Fare"].mean().reset_index())

# 좌석등급의 값 종류별(1,2,3)로 dataframe split
grouped = df.groupby("Pclass")

for k, item in grouped:
    print(k, item.shape)
    print(item.head(4))
    print("=" * 50)

print(grouped["Survived"].sum())
grouped = df.groupby("Pclass")

# Pclass 별로 나이의 평균 계산
print(grouped["Age"].mean().reset_index())

# Agg 함수 사용해서 Pclass 별 Age 의 min, max 구하기
print(grouped["Age"].agg(["max", "min"]))

# agg 함수 사용해서 원하는 열 마다 다른 집계함수 사용
# Pclass 별로 Fare 열의 평균 및 표준편차, Age 열의 min, max 구하기
print(grouped.agg({
    "Fare":["mean", "std"],
    "Age":["min", "max"]}))

df = pd.read_csv("./test-file/titanic.csv", index_col=0)
print(df.shape)
print(df.head(3))
grouped = df.groupby("Pclass")

print(grouped["Fare"].mean())
print(grouped["Fare"].transform("mean"))

df = pd.read_csv("./test-file/titanic.csv", index_col=0)
print(df.head(3))

grouped = df.groupby("Pclass")

# Pclass 별 승객수가 80명 이상인 클래스만 필터링
filtered = grouped.filter(lambda x: len(x) >= 80)
print(filtered)

# Pclass 별 승객 수 확인하기
for k, item in grouped:
    print(k, item.shape)