# refilter_existing_cache.py
import pandas as pd


def refilter_existing_cache(input_file, output_file=None):
    """기존 캐시 파일에 라이트 필터 재적용"""
    df = pd.read_csv(input_file)
    print(f"원본 파일: {len(df)} 행")

    # 라이트 필터 재적용 (CONFIG 값 사용)
    def pass_light_generic(row):
        price = row.get('Price')
        dv = (row.get('DollarVol($M)') or 0) * 1_000_000
        if pd.isna(price) or pd.isna(dv):
            return False
        return (price >= 1.0) and (dv >= 900_000)

    # 라이트 필터 적용
    light_filter_mask = df.apply(pass_light_generic, axis=1)
    filtered_df = df[light_filter_mask].copy()

    print(f"라이트 필터 통과: {len(filtered_df)} 행")
    print(f"제거된 행: {len(df) - len(filtered_df)}")

    # 추가: 유효한 상세 데이터가 있는지 확인
    valid_detail_mask = (
            filtered_df['Name'].notna() &
            filtered_df['Sector'].notna() &
            filtered_df['MktCap($B)'].notna()
    )

    print(f"상세 데이터 있는 종목: {valid_detail_mask.sum()}")

    if output_file is None:
        output_file = input_file.replace('.csv', '_refiltered.csv')

    filtered_df.to_csv(output_file, index=False)
    print(f"재필터링된 파일 저장: {output_file}")

    return filtered_df


if __name__ == "__main__":
    refiltered_df = refilter_existing_cache("details_cache_us_all.csv")

    # 통계 출력
    print("\n=== 최종 데이터 품질 ===")
    total = len(refiltered_df)
    for col in ['Name', 'Sector', 'MktCap($B)', 'PE', 'RevYoY']:
        if col in refiltered_df.columns:
            non_null = refiltered_df[col].notna().sum()
            pct = (non_null / total) * 100 if total > 0 else 0
            print(f"{col}: {non_null}/{total} ({pct:.1f}%)")