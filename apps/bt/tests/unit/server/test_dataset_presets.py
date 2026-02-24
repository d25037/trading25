"""Tests for dataset_presets module."""

from src.application.services.dataset_presets import PRESETS, PresetConfig, get_preset, list_presets


def test_presets_count() -> None:
    assert len(PRESETS) == 9


def test_get_preset_exists() -> None:
    preset = get_preset("quickTesting")
    assert preset is not None
    assert isinstance(preset, PresetConfig)
    assert preset.max_stocks == 3


def test_get_preset_unknown() -> None:
    assert get_preset("nonExistent") is None


def test_list_presets() -> None:
    names = list_presets()
    assert len(names) == 9
    assert "fullMarket" in names
    assert "quickTesting" in names


def test_full_market_preset() -> None:
    p = get_preset("fullMarket")
    assert p is not None
    assert p.markets == ["prime", "standard", "growth"]
    assert p.include_margin is True
    assert p.include_statements is True
    assert p.include_topix is True


def test_topix100_preset() -> None:
    p = get_preset("topix100")
    assert p is not None
    assert p.scale_categories == ["TOPIX Core30", "TOPIX Large70"]


def test_prime_ex_topix500_preset() -> None:
    p = get_preset("primeExTopix500")
    assert p is not None
    assert p.exclude_scale_categories == ["TOPIX Core30", "TOPIX Large70", "TOPIX Mid400"]
