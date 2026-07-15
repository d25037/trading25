from __future__ import annotations

import ast
from pathlib import Path


SRC_ROOT = Path(__file__).parents[3] / "src"
MARKET_ROOT = SRC_ROOT / "infrastructure" / "db" / "market"


def _function_names(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    return {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
    }


def test_market_db_exposes_no_legacy_time_series_or_standalone_adjusted_upserts() -> (
    None
):
    names = _function_names(MARKET_ROOT / "market_db.py")

    assert names.isdisjoint(
        {
            "upsert_stock_data",
            "upsert_stock_minute_data",
            "upsert_topix_data",
            "upsert_indices_data",
            "upsert_options_225_data",
            "upsert_margin_data",
            "upsert_statements",
            "upsert_statement_metrics_adjusted",
            "upsert_daily_valuation",
            "upsert_daily_valuation_from_adjusted_metrics",
        }
    )
    assert not (MARKET_ROOT / "time_series_writers.py").exists()


def test_production_has_no_legacy_market_db_writer_callers() -> None:
    forbidden_attributes = {
        "upsert_stock_data",
        "upsert_stock_minute_data",
        "upsert_topix_data",
        "upsert_indices_data",
        "upsert_options_225_data",
        "upsert_margin_data",
        "upsert_statements",
        "upsert_statement_metrics_adjusted",
        "upsert_daily_valuation",
        "upsert_daily_valuation_from_adjusted_metrics",
    }
    callers: list[str] = []
    for path in SRC_ROOT.rglob("*.py"):
        if "infrastructure/db/dataset_io" in path.as_posix():
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                if node.func.attr in forbidden_attributes:
                    callers.append(f"{path.relative_to(SRC_ROOT)}:{node.lineno}")

    assert callers == []


def test_lineage_validation_is_not_imported_through_writer_private_names() -> None:
    valuation_source = (MARKET_ROOT / "valuation_writers.py").read_text(
        encoding="utf-8"
    )
    basis_source = (MARKET_ROOT / "adjustment_basis_writers.py").read_text(
        encoding="utf-8"
    )

    assert "adjustment_basis_writers._validate" not in valuation_source
    assert "def _validate_lineages" not in basis_source
    assert "def _validate_final_catalog" not in basis_source
