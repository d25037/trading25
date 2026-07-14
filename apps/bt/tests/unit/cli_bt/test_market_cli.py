from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from typer.testing import CliRunner

from src.entrypoints.cli import app
from src.entrypoints.cli import market as market_module

runner = CliRunner()


def test_build_market_compact_paths_defaults_to_settings(tmp_path: Path) -> None:
    source_path = tmp_path / "market-timeseries" / "market.duckdb"
    with patch(
        "src.entrypoints.cli.market.get_settings",
        return_value=SimpleNamespace(market_timeseries_dir=str(source_path.parent)),
    ):
        resolved_source, resolved_output = market_module._build_market_compact_paths(
            db_path=None,
            output_path=None,
        )

    assert resolved_source == source_path
    assert resolved_output == source_path.with_name("market.compact.duckdb")


def test_bt_market_compact_command_reports_output(tmp_path: Path) -> None:
    source_path = tmp_path / "market.duckdb"
    output_path = tmp_path / "market.compact.duckdb"
    result_payload = SimpleNamespace(
        source_path=source_path,
        output_path=output_path,
        source_bytes=1024,
        output_bytes=512,
        table_count=3,
        elapsed_ms=12.5,
    )

    with patch(
        "src.entrypoints.cli.market.compact_market_duckdb",
        return_value=result_payload,
    ) as mock_compact:
        result = runner.invoke(
            app,
            [
                "market-compact",
                "--db-path",
                str(source_path),
                "--output-path",
                str(output_path),
            ],
        )

    assert result.exit_code == 0
    mock_compact.assert_called_once_with(source_path, output_path, overwrite=False)
    assert f"output: {output_path}" in result.stdout
