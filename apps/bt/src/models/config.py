"""
Config Models

システム実行環境設定に関するPydanticモデル群
"""

from typing import Dict, List, Literal, Optional

from pydantic import BaseModel, Field, ValidationInfo, field_validator, model_validator


class ParameterOptimizationConfig(BaseModel):
    """パラメータ最適化設定"""

    enabled: bool = Field(default=False, description="パラメータ最適化有効化")
    method: str = Field(
        default="grid_search",
        description="最適化手法 ('grid_search' or 'random_search')",
    )
    n_trials: int = Field(default=100, description="ランダムサーチ用試行回数")
    n_jobs: int = Field(
        default=-1, description="並列処理数（1=シングルプロセス、-1=全CPUコア）"
    )
    scoring_weights: Dict[str, float] = Field(
        default_factory=lambda: {
            "sharpe_ratio": 0.5,
            "calmar_ratio": 0.3,
            "total_return": 0.2,
        },
        description="複合スコアリング用の重み付け（正規化後の重み付け合計）",
    )

    @field_validator("method")
    @classmethod
    def validate_method(cls, v):
        valid_methods = ["grid_search", "random_search"]
        if v not in valid_methods:
            raise ValueError(f"methodは{valid_methods}のいずれかである必要があります")
        return v


class WalkForwardConfig(BaseModel):
    """ウォークフォワード分析設定"""

    enabled: bool = Field(default=False, description="ウォークフォワード有効化")
    train_window: int = Field(default=252, description="学習期間（営業日数）")
    test_window: int = Field(default=63, description="検証期間（営業日数）")
    step: int | None = Field(default=None, description="ステップ幅（Noneでtest_window）")
    max_splits: int | None = Field(default=None, description="最大分割数（Noneで制限なし）")


class SharedConfig(BaseModel):
    """全戦略共通の実行環境設定パラメータ"""

    initial_cash: float = Field(default=10000000, description="初期資金")
    fees: float = Field(default=0.001, description="取引手数料")
    slippage: float = Field(default=0.0, description="スリッページ（比例コスト）")
    spread: float = Field(default=0.0, description="スプレッド（比例コスト）")
    borrow_fee: float = Field(default=0.0, description="借株費用（比例コスト）")
    max_concurrent_positions: int | None = Field(
        default=None, description="日次新規エントリー数の上限（既存保有ポジションは含まない）"
    )
    max_exposure: float | None = Field(
        default=None, description="1ポジションあたりの最大エクスポージャ（0-1）"
    )
    start_date: str | None = Field(default="", description="開始日")
    end_date: str | None = Field(default="", description="終了日")
    dataset: str = Field(default="primeExTopix500", description="データセット名")
    include_margin_data: bool = Field(
        default=True, description="信用残高データを含めるか"
    )
    include_statements_data: bool = Field(
        default=True, description="財務諸表データを含めるか（PERフィルター等で使用）"
    )
    relative_mode: bool = Field(
        default=False, description="相対価格モード（個別銘柄OHLC ÷ ベンチマークOHLC）"
    )
    benchmark_table: str = Field(default="topix", description="ベンチマークテーブル名")
    group_by: bool = Field(
        default=True,
        description="VectorBTポートフォリオ統合設定（複数銘柄を一つのポートフォリオとして扱う）",
    )
    cash_sharing: bool = Field(
        default=True, description="VectorBT資金共有設定（複数銘柄間で資金を共有する）"
    )
    printlog: bool = Field(default=False, description="ログ出力設定")
    stock_codes: List[str] = Field(
        default=["all"], description="実行対象銘柄リスト (['all']で全銘柄)"
    )
    direction: str = Field(
        default="longonly",
        description="VectorBT取引方向設定 ('longonly', 'shortonly', 'both')",
    )

    # Portfolio Optimization Settings (Kelly Criterion Only)
    kelly_fraction: float = Field(
        default=1.0, description="ケリー係数（0.5=Half Kelly推奨, 1.0=Full Kelly）"
    )
    min_allocation: float = Field(default=0.01, description="最小配分率（1%）")
    max_allocation: float = Field(default=0.5, description="最大配分率（50%）")

    # Parameter Optimization Settings
    parameter_optimization: Optional[ParameterOptimizationConfig] = Field(
        default_factory=ParameterOptimizationConfig,
        description="パラメータ最適化設定",
    )

    # Walk-forward Settings
    walk_forward: WalkForwardConfig = Field(
        default_factory=WalkForwardConfig, description="ウォークフォワード設定"
    )

    # Timeframe Settings
    timeframe: Literal["daily", "weekly"] = Field(
        default="daily",
        description="データの時間軸 ('daily'=日足, 'weekly'=週足)",
    )

    @model_validator(mode="after")
    def resolve_stock_codes(self, info: ValidationInfo):
        """stock_codes の "all" を動的に解決"""
        if info.context and info.context.get("resolve_stock_codes") is False:
            return self
        if self.stock_codes == ["all"]:
            # データベースから銘柄一覧を取得
            from src.data import get_stock_list

            try:
                self.stock_codes = get_stock_list(self.dataset)
            except Exception as e:
                # APIエラー時は明示的なエラーメッセージを出力
                from loguru import logger

                logger.error(f"銘柄リスト取得エラー: {e}")
                logger.error(f"データセット: {self.dataset}")
                raise ValueError(
                    f"銘柄リストの取得に失敗しました。"
                    f"APIサーバーが起動しているか、データセット '{self.dataset}' が存在するか確認してください。"
                    f"エラー詳細: {e}"
                ) from e

            # 銘柄が0件の場合もエラー
            if not self.stock_codes:
                raise ValueError(
                    f"データセット '{self.dataset}' に銘柄が見つかりませんでした。"
                )
        return self

    @field_validator("initial_cash")
    @classmethod
    def validate_initial_cash(cls, v):
        if v <= 0:
            raise ValueError("初期資金は正の値である必要があります")
        return v

    @field_validator("fees")
    @classmethod
    def validate_fees(cls, v):
        if v < 0 or v >= 1:
            raise ValueError("手数料は0以上1未満である必要があります")
        return v

    @field_validator("slippage", "spread", "borrow_fee")
    @classmethod
    def validate_costs(cls, v):
        if v < 0 or v >= 1:
            raise ValueError("コストは0以上1未満である必要があります")
        return v

    @field_validator("max_concurrent_positions")
    @classmethod
    def validate_max_concurrent_positions(cls, v):
        if v is not None and v <= 0:
            raise ValueError("max_concurrent_positionsは1以上である必要があります")
        return v

    @field_validator("max_exposure")
    @classmethod
    def validate_max_exposure(cls, v):
        if v is not None and (v <= 0 or v > 1):
            raise ValueError("max_exposureは0より大きく1以下である必要があります")
        return v

    @field_validator("direction")
    @classmethod
    def validate_direction(cls, v):
        valid_directions = ["longonly", "shortonly", "both"]
        if v not in valid_directions:
            raise ValueError(
                f"directionは{valid_directions}のいずれかである必要があります"
            )
        return v
