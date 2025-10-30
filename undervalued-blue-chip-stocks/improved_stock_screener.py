# -*- coding: utf-8 -*-
"""
improved_stock_screener.py
개선된 미국 주식 스크리너 - 중복 제거, 구조 개선, 현실적 필터링
"""

import os
import math
import warnings
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from openpyxl.formatting.rule import CellIsRule

warnings.filterwarnings("ignore", category=RuntimeWarning)


# ============================================================================
# 설정 클래스 (데이터클래스로 관리)
# ============================================================================

@dataclass
class FilterCriteria:
    """필터 기준 데이터클래스"""
    min_mktcap: float = 500_000_000  # 5억 달러 (중소형주 포함)
    min_price: float = 5.0  # 5달러 (페니스톡 제외)
    min_dollar_volume: float = 1_000_000  # 100만 달러 (유동성 기준 완화)
    max_pe: float = 35.0  # S&P500 평균 고려
    min_rev_growth: float = -0.05  # -5% (턴어라운드 기회 포함)
    min_op_margin: float = 0.05  # 5% (섹터별 차이 고려)
    max_debt_equity: float = 2.5  # 2.5배 (산업별 차이 고려)
    min_roe: float = 0.08  # 8% (현실적 기준)


class ScreenerConfig:
    """스크리너 설정 관리"""

    # 프로파일별 필터 기준
    PROFILES = {
        'value_basic': FilterCriteria(),  # 기본 가치투자
        'value_strict': FilterCriteria(
            min_mktcap=2_000_000_000,  # 20억 달러
            min_dollar_volume=5_000_000,  # 500만 달러
            max_pe=25.0,
            min_rev_growth=0.02,
            min_op_margin=0.10,
            max_debt_equity=1.5,
            min_roe=0.12
        ),
        'growth_quality': FilterCriteria(  # 성장+품질
            min_mktcap=1_000_000_000,
            min_rev_growth=0.10,
            min_op_margin=0.15,
            min_roe=0.15,
            max_debt_equity=1.0
        ),
        'momentum': {  # 모멘텀 트레이딩
            'min_price': 10.0,
            'min_volume': 3_000_000,
            'min_rvol': 1.2,
            'rsi_range': (30, 70),
            'ret20_min': 0.02
        },
        'swing': {  # 스윙 트레이딩
            'min_price': 5.0,
            'min_volume': 1_000_000,
            'atr_range': (0.02, 0.10),
            'rsi_range': (25, 75)
        }
    }

    # 점수 가중치 (프로파일별로 다르게 적용)
    SCORE_WEIGHTS = {
        'value': {'growth': 0.15, 'quality': 0.35, 'value': 0.40, 'momentum': 0.10},
        'growth': {'growth': 0.40, 'quality': 0.30, 'value': 0.20, 'momentum': 0.10},
        'balanced': {'growth': 0.25, 'quality': 0.30, 'value': 0.30, 'momentum': 0.15},
        'trading': {'growth': 0.10, 'quality': 0.20, 'value': 0.20, 'momentum': 0.50}
    }

    # 섹터별 조정 파라미터
    SECTOR_ADJUSTMENTS = {
        'technology': {'pe_multiplier': 1.3, 'margin_discount': 0.0},
        'financial': {'pe_multiplier': 0.8, 'margin_discount': 0.5, 'use_pb': True},
        'utilities': {'pe_multiplier': 0.9, 'margin_discount': 0.3},
        'healthcare': {'pe_multiplier': 1.2, 'margin_discount': 0.1},
        'real estate': {'pe_multiplier': 1.0, 'margin_discount': 0.4, 'use_pb': True}
    }


# ============================================================================
# 유틸리티 클래스
# ============================================================================

class DataProcessor:
    """데이터 처리 유틸리티"""

    @staticmethod
    def winsorize(series: pd.Series, limits: Tuple[float, float] = (0.01, 0.99)) -> pd.Series:
        """이상치 제거 (Winsorization)"""
        return series.clip(
            lower=series.quantile(limits[0]),
            upper=series.quantile(limits[1])
        )

    @staticmethod
    def normalize_score(series: pd.Series, ascending: bool = True) -> pd.Series:
        """점수 정규화 (0-1 범위)"""
        if not ascending:
            series = -series
        return series.rank(pct=True, method='average')

    @staticmethod
    def safe_divide(numerator: float, denominator: float, default: float = 0) -> float:
        """안전한 나눗셈"""
        try:
            if denominator and denominator != 0:
                return numerator / denominator
            return default
        except:
            return default


# ============================================================================
# 가치평가 모델 (단순화)
# ============================================================================

class ValuationModel:
    """통합 가치평가 모델"""

    @staticmethod
    def calculate_fair_value(df: pd.DataFrame) -> pd.DataFrame:
        """적정가치 계산 (단순화된 버전)"""
        fair_values = []

        for idx, row in df.iterrows():
            price = row.get('Price', 0)
            pe = row.get('PE', 0)
            pb = row.get('PB', 0)
            sector = str(row.get('Sector', '')).lower()

            # 섹터 평균 대비 상대가치
            sector_data = df[df['Sector'] == row['Sector']]

            valuations = []

            # 1. PE 기반 가치
            if pe > 0 and len(sector_data) > 3:
                sector_pe_median = sector_data['PE'][sector_data['PE'] > 0].median()
                if sector_pe_median and not pd.isna(sector_pe_median):
                    eps = price / pe
                    pe_value = sector_pe_median * eps
                    valuations.append(pe_value)

            # 2. PB 기반 가치 (금융, 부동산)
            if pb > 0 and any(x in sector for x in ['financ', 'real', 'bank']):
                sector_pb_median = sector_data['PB'][sector_data['PB'] > 0].median()
                if sector_pb_median and not pd.isna(sector_pb_median):
                    bps = price / pb
                    pb_value = sector_pb_median * bps
                    valuations.append(pb_value)

            # 3. 단순 DCF (연 8% 성장, 10% 할인율 가정)
            if pe > 0 and pe < 50:
                eps = price / pe
                dcf_value = eps * 15  # 단순화된 DCF 배수
                valuations.append(dcf_value)

            # 평균 적정가치
            if valuations:
                fair_value = np.mean(valuations)
                discount = (fair_value - price) / price if price > 0 else 0
            else:
                fair_value = price
                discount = 0

            fair_values.append({
                'FairValue': fair_value,
                'Discount': discount
            })

        return pd.DataFrame(fair_values, index=df.index)


# ============================================================================
# 메인 스크리너 클래스
# ============================================================================

class StockScreener:
    """통합 주식 스크리너"""

    def __init__(self, config: ScreenerConfig = None):
        self.config = config or ScreenerConfig()
        self.processor = DataProcessor()
        self.valuation = ValuationModel()

    def load_data(self, filepath: str) -> pd.DataFrame:
        """데이터 로드 및 전처리"""
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"파일을 찾을 수 없습니다: {filepath}")

        df = pd.read_csv(filepath)

        # 숫자형 변환
        numeric_cols = [
            'Price', 'DollarVol($M)', 'MktCap($B)', 'PE', 'PB', 'ROE(info)',
            'OpMarginTTM', 'RevYoY', 'Debt_to_Equity', 'RVOL', 'RSI_14',
            'RET5', 'RET20', 'ATR_PCT', 'SMA20', 'SMA50'
        ]

        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # 이상치 제거
        winsor_cols = ['PE', 'PB', 'Debt_to_Equity', 'RevYoY']
        for col in winsor_cols:
            if col in df.columns:
                df[col] = self.processor.winsorize(df[col].fillna(df[col].median()))

        return df

    def apply_filters(self, df: pd.DataFrame, profile: str) -> pd.DataFrame:
        """프로파일별 필터 적용"""
        if profile not in self.config.PROFILES:
            raise ValueError(f"알 수 없는 프로파일: {profile}")

        if profile in ['value_basic', 'value_strict', 'growth_quality']:
            return self._apply_fundamental_filter(df, profile)
        else:
            return self._apply_trading_filter(df, profile)

    def _apply_fundamental_filter(self, df: pd.DataFrame, profile: str) -> pd.DataFrame:
        """펀더멘털 필터"""
        criteria = self.config.PROFILES[profile]

        # 섹터별 조정 적용
        mask = pd.Series([True] * len(df), index=df.index)

        for idx, row in df.iterrows():
            sector = str(row.get('Sector', '')).lower()
            sector_adj = self.config.SECTOR_ADJUSTMENTS.get(
                next((k for k in self.config.SECTOR_ADJUSTMENTS if k in sector), 'default'),
                {'pe_multiplier': 1.0, 'margin_discount': 0}
            )

            # 기본 필터
            if row.get('MktCap($B)', 0) * 1e9 < criteria.min_mktcap:
                mask[idx] = False
                continue

            if row.get('Price', 0) < criteria.min_price:
                mask[idx] = False
                continue

            if row.get('DollarVol($M)', 0) * 1e6 < criteria.min_dollar_volume:
                mask[idx] = False
                continue

            # PE 필터 (섹터 조정)
            pe_limit = criteria.max_pe * sector_adj.get('pe_multiplier', 1.0)
            if row.get('PE', 0) > pe_limit:
                mask[idx] = False
                continue

            # 마진 필터 (섹터 조정)
            margin_req = criteria.min_op_margin * (1 - sector_adj.get('margin_discount', 0))
            op_margin = row.get('OpMarginTTM') or row.get('OperatingMargins(info)', 0)
            if op_margin < margin_req:
                mask[idx] = False
                continue

            # ROE 필터
            roe = row.get('ROE(info)', 0)
            if roe < criteria.min_roe:
                mask[idx] = False
                continue

            # 부채비율 필터
            if row.get('Debt_to_Equity', 0) > criteria.max_debt_equity:
                mask[idx] = False
                continue

        return df[mask]

    def _apply_trading_filter(self, df: pd.DataFrame, profile: str) -> pd.DataFrame:
        """트레이딩 필터"""
        criteria = self.config.PROFILES[profile]

        mask = (
                (df['Price'] >= criteria['min_price']) &
                (df['DollarVol($M)'] * 1e6 >= criteria['min_volume'])
        )

        if 'min_rvol' in criteria:
            mask &= (df['RVOL'] >= criteria['min_rvol'])

        if 'rsi_range' in criteria:
            rsi_min, rsi_max = criteria['rsi_range']
            mask &= (df['RSI_14'] >= rsi_min) & (df['RSI_14'] <= rsi_max)

        if 'atr_range' in criteria:
            atr_min, atr_max = criteria['atr_range']
            mask &= (df['ATR_PCT'] >= atr_min) & (df['ATR_PCT'] <= atr_max)

        if 'ret20_min' in criteria:
            mask &= (df['RET20'] >= criteria['ret20_min'])

        return df[mask]

    def calculate_scores(self, df: pd.DataFrame, score_type: str = 'balanced') -> pd.DataFrame:
        """종합 점수 계산"""
        weights = self.config.SCORE_WEIGHTS[score_type]

        # 성장 점수
        growth_components = []
        if 'RevYoY' in df.columns:
            growth_components.append(self.processor.normalize_score(df['RevYoY']))
        if 'RET20' in df.columns:
            growth_components.append(self.processor.normalize_score(df['RET20']))

        growth_score = np.mean(growth_components, axis=0) if growth_components else 0.5

        # 품질 점수
        quality_components = []
        if 'ROE(info)' in df.columns:
            quality_components.append(self.processor.normalize_score(df['ROE(info)']))
        if 'OpMarginTTM' in df.columns:
            quality_components.append(self.processor.normalize_score(df['OpMarginTTM']))

        quality_score = np.mean(quality_components, axis=0) if quality_components else 0.5

        # 가치 점수
        value_components = []
        if 'PE' in df.columns:
            value_components.append(self.processor.normalize_score(df['PE'], ascending=False))
        if 'PB' in df.columns:
            value_components.append(self.processor.normalize_score(df['PB'], ascending=False))
        if 'Discount' in df.columns:
            value_components.append(self.processor.normalize_score(df['Discount']))

        value_score = np.mean(value_components, axis=0) if value_components else 0.5

        # 모멘텀 점수
        momentum_components = []
        if 'RVOL' in df.columns:
            momentum_components.append(self.processor.normalize_score(df['RVOL']))
        if 'RSI_14' in df.columns:
            rsi_norm = (df['RSI_14'] - 30) / 40  # 30-70 범위 정규화
            momentum_components.append(rsi_norm.clip(0, 1))

        momentum_score = np.mean(momentum_components, axis=0) if momentum_components else 0.5

        # 종합 점수
        df['GrowthScore'] = growth_score
        df['QualityScore'] = quality_score
        df['ValueScore'] = value_score
        df['MomentumScore'] = momentum_score

        df['TotalScore'] = (
                                   weights['growth'] * df['GrowthScore'] +
                                   weights['quality'] * df['QualityScore'] +
                                   weights['value'] * df['ValueScore'] +
                                   weights['momentum'] * df['MomentumScore']
                           ) * 100

        return df

    def screen_stocks(self, filepath: str, min_score: float = 60) -> Dict[str, pd.DataFrame]:
        """전체 스크리닝 실행"""
        print("📊 데이터 로딩...")
        df = self.load_data(filepath)
        print(f"✅ {len(df)}개 종목 로드 완료")

        # 적정가치 계산
        print("💰 적정가치 계산 중...")
        fair_values = self.valuation.calculate_fair_value(df)
        df = pd.concat([df, fair_values], axis=1)

        results = {}

        # 프로파일별 스크리닝
        profiles = [
            ('value_basic', 'value', 55),  # (프로파일, 점수타입, 최소점수)
            ('value_strict', 'value', 65),
            ('growth_quality', 'growth', 60),
            ('momentum', 'trading', 60),
            ('swing', 'trading', 55)
        ]

        for profile_name, score_type, min_threshold in profiles:
            print(f"\n🔍 {profile_name} 스크리닝...")

            # 필터 적용
            filtered = self.apply_filters(df, profile_name)

            if filtered.empty:
                print(f"   ⚠️ 조건 충족 종목 없음")
                continue

            # 점수 계산
            scored = self.calculate_scores(filtered.copy(), score_type)

            # 최소 점수 필터
            final = scored[scored['TotalScore'] >= min_threshold]

            # 결과 정리
            if not final.empty:
                # 핵심 컬럼만 선택
                cols = self._select_columns(profile_name, final.columns)
                results[profile_name] = final[cols].sort_values('TotalScore', ascending=False)
                print(f"   ✅ {len(results[profile_name])}개 종목 발굴")
            else:
                print(f"   ⚠️ 최소 점수 충족 종목 없음")

        return results

    def _select_columns(self, profile: str, available_cols: List[str]) -> List[str]:
        """프로파일별 출력 컬럼 선택"""
        base_cols = ['Ticker', 'Name', 'Sector', 'Price']

        if profile in ['value_basic', 'value_strict', 'growth_quality']:
            specific_cols = [
                'FairValue', 'Discount', 'PE', 'PB', 'ROE(info)',
                'OpMarginTTM', 'RevYoY', 'Debt_to_Equity',
                'GrowthScore', 'QualityScore', 'ValueScore', 'TotalScore'
            ]
        else:
            specific_cols = [
                'DollarVol($M)', 'RVOL', 'ATR_PCT', 'RSI_14',
                'RET5', 'RET20', 'SMA20', 'SMA50',
                'MomentumScore', 'TotalScore'
            ]

        return base_cols + [col for col in specific_cols if col in available_cols]


# ============================================================================
# 엑셀 출력 클래스 (단순화)
# ============================================================================

class ExcelExporter:
    """엑셀 출력 관리"""

    @staticmethod
    def export(results: Dict[str, pd.DataFrame], filename: str = None):
        """결과를 엑셀로 출력"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M")
            filename = f"stock_screener_{timestamp}.xlsx"

        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # 요약 시트
            summary_data = []
            for profile, df in results.items():
                if not df.empty:
                    top_tickers = ', '.join(df.head(5)['Ticker'].tolist())
                    summary_data.append({
                        'Profile': profile,
                        'Count': len(df),
                        'Avg Score': f"{df['TotalScore'].mean():.1f}",
                        'Top 5 Tickers': top_tickers
                    })

            if summary_data:
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
                ExcelExporter._format_sheet(writer, 'Summary', summary_df)

            # 각 프로파일별 시트
            for profile, df in results.items():
                if not df.empty:
                    # 숫자 포맷 조정 (소수인 스코어 점수에 100을 곱하고 소수점 두번째 자리만 표기)
                    df_copy = df.copy()
                    for col in ['Discount', 'ROE(info)', 'OpMarginTTM', 'RevYoY', 'ATR_PCT', 'RET5', 'RET20', 'GrowthScore', 'QualityScore', 'ValueScore', 'MomentumScore']:
                        if col in df_copy.columns:
                            df_copy[col] = df_copy[col].apply(
                                lambda x: f"{x * 100:.3f}" if pd.notna(x) else ""
                            )

                    # 숫자 포맷 조정 (소수점을 두번째 자리까지 표기)
                    for col in ['FairValue', 'TotalScore', 'RVOL', 'SMA20', 'SMA50', 'PE', 'PB', 'RSI_14']:
                        if col in df_copy.columns:
                            df_copy[col] = df_copy[col].apply(
                                lambda x: f"{x:.3f}" if pd.notna(x) else ""
                            )

                    # 숫자 포맷 조정 (소수점을 퍼센트로)
                    for col in ['Discount', 'ROE(info)', 'OpMarginTTM', 'RevYoY', 'PE', 'PB', 'ATR_PCT', 'RET5', 'RET20']:
                        if col in df_copy.columns:
                            df_copy[col] = df_copy[col].apply(
                                lambda x: f"{x}%" if pd.notna(x) else ""
                            )

                    df_copy.to_excel(writer, sheet_name=profile[:30], index=False)
                    ExcelExporter._format_sheet(writer, profile[:30], df_copy)

        print(f"\n📁 결과 저장 완료: {filename}")
        return filename

    @staticmethod
    def _format_sheet(writer, sheet_name: str, df: pd.DataFrame):
        """시트 포맷 적용"""
        worksheet = writer.sheets[sheet_name]

        # 헤더 스타일
        header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
        header_font = Font(color="FFFFFF", bold=True, size=11)

        for col in range(1, len(df.columns) + 1):
            cell = worksheet.cell(row=1, column=col)
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal='center')

        # 컬럼 너비 자동 조정
        for idx, col in enumerate(df.columns, 1):
            max_length = len(str(col))
            for row in range(2, min(len(df) + 2, 100)):
                try:
                    cell_value = worksheet.cell(row=row, column=idx).value
                    if cell_value:
                        max_length = max(max_length, len(str(cell_value)))
                except:
                    pass

            adjusted_width = min(max_length + 2, 30)
            worksheet.column_dimensions[get_column_letter(idx)].width = adjusted_width

        # 틀 고정
        worksheet.freeze_panes = 'B2'


# ============================================================================
# 메인 실행 함수
# ============================================================================

def main(csv_file: str = "details_cache_us_all.csv"):
    """메인 실행 함수"""
    try:
        # 스크리너 인스턴스 생성
        screener = StockScreener()

        # 스크리닝 실행
        results = screener.screen_stocks(csv_file)

        if not results:
            print("\n❌ 조건을 충족하는 종목이 없습니다.")
            return None

        # 엑셀 출력
        output_file = ExcelExporter.export(results)

        # 결과 요약 출력
        print("\n" + "=" * 60)
        print("📊 스크리닝 결과 요약")
        print("=" * 60)

        for profile, df in results.items():
            if not df.empty:
                print(f"\n[{profile}]")
                print(f"  • 종목 수: {len(df)}개")
                print(f"  • 평균 점수: {df['TotalScore'].mean():.1f}")
                print(f"  • Top 3: {', '.join(df.head(3)['Ticker'].tolist())}")

        print("\n✅ 스크리닝 완료!")
        return results

    except FileNotFoundError as e:
        print(f"\n❌ 오류: {e}")
        print("CSV 파일을 다운로드하고 경로를 확인해주세요.")
        return None
    except Exception as e:
        print(f"\n❌ 예상치 못한 오류: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    # CSV 파일 경로를 인자로 전달 가능
    import sys

    csv_path = sys.argv[1] if len(sys.argv) > 1 else "details_cache_us_all.csv"
    main(csv_path)