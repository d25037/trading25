"""
Dataset Presets

Hono の 9 プリセットを Python 辞書で定義。
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class PresetConfig:
    markets: list[str] = field(default_factory=lambda: ["prime"])
    include_margin: bool = True
    include_statements: bool = True
    include_topix: bool = True
    include_sector_indices: bool = True
    market_cap_filter: int | None = None
    scale_categories: list[str] | None = None
    exclude_scale_categories: list[str] | None = None
    max_stocks: int | None = None


PRESETS: dict[str, PresetConfig] = {
    "fullMarket": PresetConfig(markets=["prime", "standard", "growth"]),
    "primeMarket": PresetConfig(markets=["prime"]),
    "standardMarket": PresetConfig(markets=["standard"]),
    "growthMarket": PresetConfig(markets=["growth"]),
    "topix100": PresetConfig(
        markets=["prime"],
        scale_categories=["TOPIX Core30", "TOPIX Large70"],
    ),
    "topix500": PresetConfig(
        markets=["prime"],
        scale_categories=["TOPIX Core30", "TOPIX Large70", "TOPIX Mid400"],
    ),
    "mid400": PresetConfig(
        markets=["prime"],
        scale_categories=["TOPIX Mid400"],
    ),
    "primeExTopix500": PresetConfig(
        markets=["prime"],
        exclude_scale_categories=["TOPIX Core30", "TOPIX Large70", "TOPIX Mid400"],
    ),
    "quickTesting": PresetConfig(
        markets=["prime"],
        max_stocks=3,
    ),
}


def get_preset(name: str) -> PresetConfig | None:
    """プリセット名から設定を取得"""
    return PRESETS.get(name)


def list_presets() -> list[str]:
    """利用可能なプリセット名一覧"""
    return list(PRESETS.keys())
