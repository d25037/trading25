"""Application-owned backtest result contracts."""

from pydantic import BaseModel, Field


class BacktestResultSummary(BaseModel):
    """バックテスト結果サマリー"""

    total_return: float = Field(description="トータルリターン (%)")
    sharpe_ratio: float = Field(description="シャープレシオ")
    sortino_ratio: float | None = Field(default=None, description="ソルティノレシオ")
    calmar_ratio: float = Field(description="カルマーレシオ")
    max_drawdown: float = Field(description="最大ドローダウン (%)")
    win_rate: float = Field(description="勝率 (%)")
    trade_count: int = Field(description="取引回数")
    html_path: str | None = Field(default=None, description="結果HTMLファイルのパス")
