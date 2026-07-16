from __future__ import annotations

import ast
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
ENGINE_PATH = PROJECT_ROOT / "src/domains/optimization/engine.py"
CLI_PATH = PROJECT_ROOT / "src/entrypoints/cli/optimize.py"
SOURCE_ROOT = PROJECT_ROOT / "src"
CURRENT_DOCS = (
    PROJECT_ROOT / "docs/parameter-optimization.md",
    PROJECT_ROOT / "docs/parameter-optimization-system.md",
)


def test_runtime_optimization_has_no_legacy_grid_path_surface() -> None:
    engine_source = ENGINE_PATH.read_text(encoding="utf-8")
    cli_source = CLI_PATH.read_text(encoding="utf-8")
    engine_tree = ast.parse(engine_source, filename=str(ENGINE_PATH))

    constructor = next(
        node
        for node in ast.walk(engine_tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == "__init__"
    )
    argument_names = {
        argument.arg
        for argument in (
            *constructor.args.posonlyargs,
            *constructor.args.args,
            *constructor.args.kwonlyargs,
        )
    }

    assert "grid_config_path" not in argument_names
    assert "grid_config_path" not in engine_source
    assert "grid_config_path" not in cli_source
    assert "optimization_spec_source" in engine_source
    for path in SOURCE_ROOT.rglob("*.py"):
        assert "grid_config_path" not in path.read_text(encoding="utf-8"), str(path)


def test_current_optimization_docs_only_describe_strategy_linked_specs() -> None:
    forbidden = ("config/optimization/", "_grid.yaml", "grid_config_path")
    for path in CURRENT_DOCS:
        source = path.read_text(encoding="utf-8")
        for token in forbidden:
            assert token not in source, f"{path.relative_to(PROJECT_ROOT)} contains {token}"
        assert "optimization" in source
        assert "strategy YAML" in source


def test_signal_overlay_has_no_current_sector_mapping_fallback() -> None:
    loader_path = PROJECT_ROOT / "src/application/services/screening_market_loader.py"
    service_path = PROJECT_ROOT / "src/application/services/signal_service.py"
    provider_path = PROJECT_ROOT / "src/application/services/analytics_data_provider.py"
    combined = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (loader_path, service_path, provider_path)
    )

    assert "load_market_stock_sector_mapping" not in combined
    assert "stocks_latest" not in combined
    assert "load_market_stock_sector_history" in combined
    assert "stock_master_daily" in combined
