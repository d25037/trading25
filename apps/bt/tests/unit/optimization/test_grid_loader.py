from pathlib import Path

import pytest

from src.domains.optimization.grid_loader import (
    find_grid_config_path,
    flatten_params,
    generate_combinations,
    load_default_config,
    load_grid_config,
)


class TestFindGridConfigPath:
    def test_returns_explicit_path_when_exists(self, tmp_path: Path) -> None:
        config_path = tmp_path / "custom_grid.yaml"
        config_path.write_text("parameter_ranges: {}\n", encoding="utf-8")

        result = find_grid_config_path("demo", str(config_path))

        assert result == str(config_path)

    def test_resolves_from_search_dirs(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        search_dir = tmp_path / "optimization"
        search_dir.mkdir(parents=True)
        config_path = search_dir / "demo_grid.yaml"
        config_path.write_text("parameter_ranges: {}\n", encoding="utf-8")
        monkeypatch.setattr(
            "src.shared.paths.get_all_optimization_grid_dirs",
            lambda: [search_dir],
        )

        result = find_grid_config_path("demo")

        assert result == str(config_path)

    def test_raises_when_fallback_path_is_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "src.shared.paths.get_all_optimization_grid_dirs",
            lambda: [],
        )

        with pytest.raises(FileNotFoundError, match="config/optimization/demo_grid.yaml"):
            find_grid_config_path("demo")


class TestLoadGridConfig:
    def test_loads_yaml_document(self, tmp_path: Path) -> None:
        config_path = tmp_path / "demo_grid.yaml"
        config_path.write_text(
            "parameter_ranges:\n  entry_filter_params:\n    breakout:\n      period: [10, 20]\n",
            encoding="utf-8",
        )

        result = load_grid_config(str(config_path))

        assert result["parameter_ranges"]["entry_filter_params"]["breakout"]["period"] == [10, 20]


class TestLoadDefaultConfig:
    def test_loads_default_config_via_shared_resolver(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        default_path = tmp_path / "default.yaml"
        default_path.write_text(
            (
                "default:\n"
                "  parameters:\n"
                "    shared_config:\n"
                "      dataset: custom\n"
                "      parameter_optimization:\n"
                "        enabled: true\n"
            ),
            encoding="utf-8",
        )
        monkeypatch.setattr(
            "src.domains.optimization.grid_loader.get_default_config_path",
            lambda: default_path,
        )

        result = load_default_config()

        assert result["shared_config"]["dataset"] == "custom"
        assert result["parameter_optimization"]["enabled"] is True


class TestFlattenParams:
    def test_flattens_nested_lists(self) -> None:
        result = flatten_params(
            {"per": {"threshold": [10, 15]}, "ignored": "value"},
            "entry_filter_params.fundamental",
        )

        assert result == [("entry_filter_params.fundamental.per.threshold", [10, 15])]


class TestGenerateCombinations:
    def test_generates_cartesian_product_and_skips_none(self) -> None:
        combinations = generate_combinations(
            {
                "entry_filter_params": {
                    "breakout": {
                        "period": [10, 20],
                        "enabled": [True, False],
                    },
                    "ignored": None,
                },
                "exit_trigger_params": None,
            }
        )

        assert len(combinations) == 4
        assert {
            "entry_filter_params.breakout.period": 10,
            "entry_filter_params.breakout.enabled": True,
        } in combinations

    def test_returns_single_empty_combination_for_empty_ranges(self) -> None:
        assert generate_combinations({}) == [{}]
