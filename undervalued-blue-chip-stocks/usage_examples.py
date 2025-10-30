# -*- coding: utf-8 -*-
"""
usage_examples.py
개선된 스크리너 사용 예제 모음
"""

from improved_stock_screener import (
    StockScreener, 
    FilterCriteria, 
    ScreenerConfig
)
import pandas as pd

def example_basic_usage():
    """기본 사용 예제"""
    print("=" * 60)
    print("예제 1: 기본 사용법")
    print("=" * 60)
    
    # 스크리너 생성
    screener = StockScreener()
    
    # 스크리닝 실행 (최소 점수 60점)
    results = screener.screen_stocks('details_cache_us_all.csv', min_score=60)
    
    # 결과 출력
    for profile, df in results.items():
        if not df.empty:
            print(f"\n[{profile}]")
            print(f"종목 수: {len(df)}")
            print(f"Top 3: {df.head(3)['Ticker'].tolist()}")

def example_custom_filter():
    """커스텀 필터 예제"""
    print("\n" + "=" * 60)
    print("예제 2: 커스텀 필터 생성")
    print("=" * 60)
    
    # 나만의 필터 기준 생성
    my_criteria = FilterCriteria(
        min_mktcap=10_000_000_000,  # 100억 달러 이상 대형주만
        min_roe=0.20,                # ROE 20% 이상
        max_pe=15,                    # PE 15 이하
        min_op_margin=0.20,           # 영업이익률 20% 이상
        max_debt_equity=0.5           # 부채비율 50% 이하
    )
    
    # 스크리너에 커스텀 프로파일 추가
    screener = StockScreener()
    screener.config.PROFILES['my_super_value'] = my_criteria
    
    # 필터 적용
    df = pd.read_csv('details_cache_us_all.csv')
    filtered = screener.apply_filters(df, 'my_super_value')
    
    print(f"필터 통과 종목: {len(filtered)}개")
    if not filtered.empty:
        print(f"종목 예시: {filtered.head(5)['Ticker'].tolist()}")

def example_sector_analysis():
    """섹터별 분석 예제"""
    print("\n" + "=" * 60)
    print("예제 3: 섹터별 분석")
    print("=" * 60)
    
    screener = StockScreener()
    df = screener.load_data('details_cache_us_all.csv')
    
    # 섹터별 평균 지표
    sector_stats = df.groupby('Sector').agg({
        'PE': 'median',
        'ROE(info)': 'mean',
        'OpMarginTTM': 'mean',
        'Debt_to_Equity': 'median'
    }).round(2)
    
    print("\n섹터별 주요 지표:")
    print(sector_stats.head(10))

def example_value_focus():
    """가치주 중심 스크리닝"""
    print("\n" + "=" * 60)
    print("예제 4: 가치주 중심 스크리닝")
    print("=" * 60)
    
    screener = StockScreener()
    
    # 가치 점수 가중치 높이기
    screener.config.SCORE_WEIGHTS['super_value'] = {
        'growth': 0.10,
        'quality': 0.30,
        'value': 0.50,  # 가치 비중 50%
        'momentum': 0.10
    }
    
    # 데이터 로드 및 필터
    df = screener.load_data('details_cache_us_all.csv')
    filtered = screener.apply_filters(df, 'value_basic')
    
    # 가치 중심 점수 계산
    scored = screener.calculate_scores(filtered, 'super_value')
    
    # 상위 10개 종목
    top_value = scored.nlargest(10, 'TotalScore')[['Ticker', 'Name', 'PE', 'PB', 'TotalScore']]
    print("\nTop 10 가치주:")
    print(top_value)

def example_growth_quality():
    """성장+품질 종목 찾기"""
    print("\n" + "=" * 60)
    print("예제 5: 성장+품질 종목")
    print("=" * 60)
    
    screener = StockScreener()
    df = screener.load_data('details_cache_us_all.csv')
    
    # 성장+품질 필터
    growth_quality = df[
        (df['RevYoY'] > 0.15) &         # 매출성장 15% 이상
        (df['ROE(info)'] > 0.15) &      # ROE 15% 이상
        (df['OpMarginTTM'] > 0.15) &    # 영업이익률 15% 이상
        (df['Debt_to_Equity'] < 1.0)    # 부채비율 100% 이하
    ]
    
    if not growth_quality.empty:
        print(f"성장+품질 종목: {len(growth_quality)}개")
        print(f"종목 리스트: {growth_quality.head(10)['Ticker'].tolist()}")

def example_trading_setup():
    """트레이딩 셋업 찾기"""
    print("\n" + "=" * 60)
    print("예제 6: 트레이딩 셋업")
    print("=" * 60)
    
    screener = StockScreener()
    df = screener.load_data('details_cache_us_all.csv')
    
    # 돌파 직전 종목 찾기
    breakout_setup = df[
        (df['Price'] > df['SMA20']) &           # 20일선 위
        (df['SMA20'] > df['SMA50']) &           # 20일선 > 50일선
        (df['RVOL'] > 1.5) &                    # 거래량 증가
        (df['RSI_14'].between(50, 65)) &        # RSI 중립~상승
        (df['ATR_PCT'].between(0.02, 0.05))     # 적정 변동성
    ]
    
    if not breakout_setup.empty:
        print(f"돌파 셋업 종목: {len(breakout_setup)}개")
        top_setup = breakout_setup.nlargest(5, 'RVOL')[['Ticker', 'Price', 'RVOL', 'RSI_14']]
        print("\nTop 5 셋업:")
        print(top_setup)

def example_dividend_stocks():
    """배당주 스크리닝"""
    print("\n" + "=" * 60)
    print("예제 7: 안정적 배당주")
    print("=" * 60)
    
    screener = StockScreener()
    df = screener.load_data('details_cache_us_all.csv')
    
    # 배당주 필터
    dividend_stocks = df[
        (df['DivYield'] > 0.02) &               # 배당수익률 2% 이상
        (df['ROE(info)'] > 0.10) &              # ROE 10% 이상
        (df['Debt_to_Equity'] < 1.5) &          # 부채비율 150% 이하
        (df['OpMarginTTM'] > 0.10) &            # 영업이익률 10% 이상
        (df['MktCap($B)'] > 10)                 # 시총 100억 달러 이상
    ]
    
    if not dividend_stocks.empty:
        dividend_stocks = dividend_stocks.sort_values('DivYield', ascending=False)
        print(f"우량 배당주: {len(dividend_stocks)}개")
        top_div = dividend_stocks.head(10)[['Ticker', 'Name', 'DivYield', 'ROE(info)', 'PE']]
        print("\nTop 10 배당주:")
        print(top_div)

def example_undervalued():
    """저평가 종목 찾기"""
    print("\n" + "=" * 60)
    print("예제 8: 저평가 종목")
    print("=" * 60)
    
    screener = StockScreener()
    df = screener.load_data('details_cache_us_all.csv')
    
    # 적정가치 계산
    fair_values = screener.valuation.calculate_fair_value(df)
    df = pd.concat([df, fair_values], axis=1)
    
    # 저평가 종목 (20% 이상 할인)
    undervalued = df[
        (df['Discount'] > 0.20) &               # 20% 이상 저평가
        (df['PE'] > 0) & (df['PE'] < 20) &      # 합리적 PE
        (df['ROE(info)'] > 0.10) &              # ROE 10% 이상
        (df['MktCap($B)'] > 1)                  # 시총 10억 달러 이상
    ]
    
    if not undervalued.empty:
        undervalued = undervalued.sort_values('Discount', ascending=False)
        print(f"저평가 종목: {len(undervalued)}개")
        top_under = undervalued.head(10)[['Ticker', 'Name', 'Price', 'FairValue', 'Discount']]
        print("\nTop 10 저평가주:")
        print(top_under)

def run_all_examples():
    """모든 예제 실행"""
    examples = [
        example_basic_usage,
        example_custom_filter,
        example_sector_analysis,
        example_value_focus,
        example_growth_quality,
        example_trading_setup,
        example_dividend_stocks,
        example_undervalued
    ]
    
    for example in examples:
        try:
            example()
        except FileNotFoundError:
            print(f"\n⚠️ CSV 파일이 없습니다. download_and_test.py를 먼저 실행하세요.")
            break
        except Exception as e:
            print(f"\n❌ 오류 발생: {e}")
            continue

if __name__ == "__main__":
    print("🚀 스크리너 사용 예제 시작\n")
    run_all_examples()
    print("\n✅ 모든 예제 완료!")
