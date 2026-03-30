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
    label: str | None = None


PRESETS: dict[str, PresetConfig] = {
    "fullMarket": PresetConfig(markets=["prime", "standard", "growth"], label="Full Market"),
    "primeMarket": PresetConfig(markets=["prime"], label="Prime Market"),
    "standardMarket": PresetConfig(markets=["standard"], label="Standard Market"),
    "growthMarket": PresetConfig(markets=["growth"], label="Growth Market"),
    "topix100": PresetConfig(
        markets=["prime"],
        scale_categories=["TOPIX Core30", "TOPIX Large70"],
        label="TOPIX 100",
    ),
    "topix500": PresetConfig(
        markets=["prime", "standard", "growth"],
        scale_categories=["TOPIX Core30", "TOPIX Large70", "TOPIX Mid400"],
        label="TOPIX 500",
    ),
    "mid400": PresetConfig(
        markets=["prime"],
        scale_categories=["TOPIX Mid400"],
        label="Mid400",
    ),
    "primeExTopix500": PresetConfig(
        markets=["prime"],
        exclude_scale_categories=["TOPIX Core30", "TOPIX Large70", "TOPIX Mid400"],
        label="Prime ex TOPIX500",
    ),
    "quickTesting": PresetConfig(
        markets=["prime"],
        max_stocks=3,
        label="Quick Testing",
    ),
}


def get_preset(name: str) -> PresetConfig | None:
    """プリセット名から設定を取得"""
    return PRESETS.get(name)


def get_preset_label(name: str) -> str | None:
    """プリセット名から表示ラベルを取得"""
    preset = get_preset(name)
    return preset.label if preset is not None else None


def list_presets() -> list[str]:
    """利用可能なプリセット名一覧"""
    return list(PRESETS.keys())
