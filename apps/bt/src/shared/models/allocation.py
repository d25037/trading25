"""
Kelly基準資金配分統計情報モデル

Kelly基準による資金配分計算結果を管理するPydanticモデル
Notebook/ターミナル両対応の表示機能を提供
"""

from pydantic import BaseModel, Field


class AllocationInfo(BaseModel):
    """
    Kelly基準資金配分統計情報

    2段階最適化バックテストで計算されたKelly基準による資金配分情報を管理。
    Jupyter NotebookとターミナルCLI両方で適切に表示される。

    Attributes:
        method: 配分計算手法（"kelly"固定）
        allocation: 最適配分率（0.0-1.0）
        win_rate: 戦略全体勝率（0.0-1.0）
        avg_win: 平均勝ちトレード（リターン）
        avg_loss: 平均負けトレード（絶対値）
        total_trades: 全トレード数
        full_kelly: Full Kelly係数（0.0-1.0）
        kelly_fraction: Kelly係数適用率（0.5=Half Kelly, 1.0=Full Kelly等）
    """

    method: str = Field(default="kelly", description="配分計算手法")
    allocation: float = Field(..., ge=0.0, le=1.0, description="最適配分率")
    win_rate: float = Field(..., ge=0.0, le=1.0, description="戦略全体勝率")
    avg_win: float = Field(..., ge=0.0, description="平均勝ちトレード")
    avg_loss: float = Field(..., ge=0.0, description="平均負けトレード")
    total_trades: int = Field(..., ge=0, description="全トレード数")
    full_kelly: float = Field(..., description="Full Kelly係数")
    kelly_fraction: float = Field(..., gt=0.0, description="Kelly係数適用率")

    def get_kelly_label(self) -> str:
        """
        Kelly係数のラベルを取得

        Returns:
            str: Kelly係数ラベル（Half Kelly/Full Kelly/2x Kelly/Custom）
        """
        if self.kelly_fraction == 0.5:
            return "Half Kelly"
        elif self.kelly_fraction == 1.0:
            return "Full Kelly"
        elif self.kelly_fraction == 2.0:
            return "2x Kelly"
        else:
            return "Custom"

    def __str__(self) -> str:
        """
        ターミナル表示用文字列表現

        Returns:
            str: フォーマット済み統計情報
        """
        kelly_label = self.get_kelly_label()
        lines = [
            "=" * 60,
            "🎯 Kelly基準資金配分最適化",
            "=" * 60,
            f"Kelly係数: {self.kelly_fraction} ({kelly_label})",
            f"戦略全体勝率: {self.win_rate:.1%}",
            f"平均勝ちトレード: {self.avg_win:.4f}",
            f"平均負けトレード: {self.avg_loss:.4f}",
            f"全トレード数: {self.total_trades:,}件",
            f"Full Kelly: {self.full_kelly:.1%}",
            f"最適配分率: {self.allocation:.1%}",
            "=" * 60,
            "💡 実運用: シグナルが出た銘柄にこの配分率で投資",
            "=" * 60,
        ]
        return "\n".join(lines)

    def _repr_html_(self) -> str:
        """
        Jupyter Notebook表示用HTML表現

        Returns:
            str: HTML形式の統計情報テーブル
        """
        kelly_label = self.get_kelly_label()
        html = f"""
        <div style="font-family: monospace; margin: 20px 0;">
            <h3 style="color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px;">
                🎯 Kelly基準資金配分最適化
            </h3>
            <table style="border-collapse: collapse; width: 100%; margin-top: 15px;">
                <tr style="background-color: #ecf0f1;">
                    <td style="padding: 10px; border: 1px solid #bdc3c7; font-weight: bold;">Kelly係数</td>
                    <td style="padding: 10px; border: 1px solid #bdc3c7;">{self.kelly_fraction} ({kelly_label})</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #bdc3c7; font-weight: bold;">戦略全体勝率</td>
                    <td style="padding: 10px; border: 1px solid #bdc3c7;">{self.win_rate:.1%}</td>
                </tr>
                <tr style="background-color: #ecf0f1;">
                    <td style="padding: 10px; border: 1px solid #bdc3c7; font-weight: bold;">平均勝ちトレード</td>
                    <td style="padding: 10px; border: 1px solid #bdc3c7;">{self.avg_win:.4f}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #bdc3c7; font-weight: bold;">平均負けトレード</td>
                    <td style="padding: 10px; border: 1px solid #bdc3c7;">{self.avg_loss:.4f}</td>
                </tr>
                <tr style="background-color: #ecf0f1;">
                    <td style="padding: 10px; border: 1px solid #bdc3c7; font-weight: bold;">全トレード数</td>
                    <td style="padding: 10px; border: 1px solid #bdc3c7;">{self.total_trades:,}件</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #bdc3c7; font-weight: bold;">Full Kelly</td>
                    <td style="padding: 10px; border: 1px solid #bdc3c7;">{self.full_kelly:.1%}</td>
                </tr>
                <tr style="background-color: #e8f5e9;">
                    <td style="padding: 10px; border: 1px solid #bdc3c7; font-weight: bold;">最適配分率</td>
                    <td style="padding: 10px; border: 1px solid #bdc3c7; font-weight: bold; color: #27ae60;">{self.allocation:.1%}</td>
                </tr>
            </table>
            <p style="margin-top: 15px; padding: 10px; background-color: #fff3cd; border-left: 4px solid #ffc107;">
                💡 <strong>実運用:</strong> シグナルが出た銘柄にこの配分率で投資
            </p>
        </div>
        """
        return html
