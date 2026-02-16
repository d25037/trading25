"""
財務指標シグナル実装

VectorBTベースの財務指標関連シグナル関数を提供

データソース特性（load_statements_data period_type推奨）:
    - "FY" (デフォルト・推奨): 本決算のみ
        対象シグナル: PER, PBR, ROE, EPS成長率, 配当利回り, PEG, Forward EPS
    - "2Q": 中間決算のみ
        対象シグナル: 営業キャッシュフロー（中間決算でも発表）
    - "all": 全四半期（最新情報優先時）
        対象シグナル: 売上高, 営業利益率（四半期ごとの最新情報を反映）

FYのみで利用可能なデータ:
    - BPS (1株当たり純資産)
    - DividendFY (通期配当)
    - NextYearForecastEPS (来期予想EPS)

このモジュールは後方互換性のため、各サブモジュールから全関数を再エクスポートする。
"""

# ヘルパー関数（内部利用）
from .fundamental_helpers import (
    _calc_consecutive_threshold_signal,
    _calc_growth_signal,
    _calc_ratio_signal,
    _calc_threshold_signal,
)

# バリュエーション系シグナル
from .fundamental_valuation import (
    is_high_book_to_market,
    is_undervalued_by_pbr,
    is_undervalued_by_per,
    is_undervalued_growth_by_peg,
)

# 成長率系シグナル
from .fundamental_growth import (
    is_expected_growth_eps,
    is_growing_dividend_per_share,
    is_growing_eps,
    is_growing_profit,
    is_growing_sales,
)

# 収益性・品質系シグナル
from .fundamental_quality import (
    is_high_dividend_yield,
    is_high_operating_margin,
    is_high_roa,
    is_high_roe,
)

# キャッシュフロー系・時価総額系シグナル
from .fundamental_cashflow import (
    cfo_yield_threshold,
    is_growing_cfo_yield,
    is_growing_simple_fcf_yield,
    market_cap_threshold,
    operating_cash_flow_threshold,
    simple_fcf_threshold,
    simple_fcf_yield_threshold,
)

__all__ = [
    # ヘルパー関数（内部利用）
    "_calc_growth_signal",
    "_calc_threshold_signal",
    "_calc_ratio_signal",
    "_calc_consecutive_threshold_signal",
    # バリュエーション系
    "is_undervalued_by_per",
    "is_undervalued_by_pbr",
    "is_high_book_to_market",
    "is_undervalued_growth_by_peg",
    # 成長率系
    "is_growing_eps",
    "is_expected_growth_eps",
    "is_growing_profit",
    "is_growing_sales",
    "is_growing_dividend_per_share",
    # 収益性・品質系
    "is_high_roe",
    "is_high_roa",
    "is_high_operating_margin",
    "is_high_dividend_yield",
    # キャッシュフロー系・時価総額系
    "operating_cash_flow_threshold",
    "simple_fcf_threshold",
    "cfo_yield_threshold",
    "simple_fcf_yield_threshold",
    "is_growing_cfo_yield",
    "is_growing_simple_fcf_yield",
    "market_cap_threshold",
]
