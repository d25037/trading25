"""Real Nautilus runtime smoke test for daily verification backtests."""

from __future__ import annotations

import importlib
import json
import os
from pathlib import Path

import pandas as pd
import pytest

from src.domains.backtest import nautilus_adapter as adapter
from src.domains.backtest.contracts import EngineFamily, RunType, RunSpec
from src.domains.backtest.nautilus_adapter import (
    NautilusVerificationRunner,
    _PreparedPortfolioInputs,
)
from src.shared.models.config import SharedConfig


def _require_nautilus_runtime() -> None:
    if os.environ.get("NAUTILUS_SMOKE_REQUIRE_RUNTIME") == "1":
        importlib.import_module("nautilus_trader")
        return
    pytest.importorskip("nautilus_trader")


pytestmark = pytest.mark.nautilus_smoke


def _build_prepared_inputs() -> _PreparedPortfolioInputs:
    return _PreparedPortfolioInputs(
        strategy_name="demo",
        dataset_name="sample",
        shared_config=SharedConfig(
            universe_preset="sample",
            stock_codes=["1301"],
            initial_cash=1_000_000,
            timeframe="daily",
            direction="longonly",
            group_by=True,
            cash_sharing=True,
        ),
        compiled_strategy=None,
        open_data=pd.DataFrame(
            {"1301": [100.0, 102.0]},
            index=pd.to_datetime(["2024-01-04", "2024-01-05"]),
        ),
        close_data=pd.DataFrame(
            {"1301": [104.0, 103.0]},
            index=pd.to_datetime(["2024-01-04", "2024-01-05"]),
        ),
        entries_data=pd.DataFrame(
            {"1301": [True, False]},
            index=pd.to_datetime(["2024-01-04", "2024-01-05"]),
        ),
        execution_mode="next_session_round_trip",
        effective_fees=0.0,
        effective_slippage=0.0,
        allocation_per_asset=1.0,
    )


def test_nautilus_real_runtime_smoke(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _require_nautilus_runtime()
    runner = NautilusVerificationRunner()
    prepared = _build_prepared_inputs()
    output_dir = Path.cwd() / ".tmp" / "nautilus-runtime-smoke" / tmp_path.name
    output_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        runner._vectorbt_runner,
        "build_parameters_for_strategy",
        lambda strategy, config_override=None: {
            "shared_config": {"universe_preset": "sample", "timeframe": "daily"},
            "entry_filter_params": {},
        },
    )
    monkeypatch.setattr(
        runner._vectorbt_runner.config_loader,
        "load_strategy_config",
        lambda strategy_name: {"strategy_params": {"name": strategy_name}},
    )
    monkeypatch.setattr(
        runner._vectorbt_runner.config_loader,
        "get_output_directory",
        lambda strategy_config: output_dir,
    )
    monkeypatch.setattr(
        adapter,
        "_prepare_portfolio_inputs",
        lambda strategy_name, parameters: prepared,
    )

    result = runner.execute(
        "demo-strategy",
        run_spec=RunSpec(
            run_type=RunType.BACKTEST,
            strategy_name="demo-strategy",
            dataset_snapshot_id="sample",
            market_snapshot_id="market:latest",
            engine_family=EngineFamily.NAUTILUS,
            execution_policy_version="nautilus-daily-verification-v1",
        ),
        run_id="nautilus-smoke-run",
    )

    assert result.html_path is None
    assert result.metrics_path is not None and result.metrics_path.exists()

    manifest_path = Path(str(result.summary["_manifest_path"]))
    engine_path = Path(str(result.summary["_engine_path"]))
    diagnostics_path = Path(str(result.summary["_diagnostics_path"]))

    assert manifest_path.exists()
    assert engine_path.exists()
    assert diagnostics_path.exists()

    metrics_payload = json.loads(result.metrics_path.read_text(encoding="utf-8"))
    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    engine_payload = json.loads(engine_path.read_text(encoding="utf-8"))
    diagnostics_payload = json.loads(diagnostics_path.read_text(encoding="utf-8"))

    assert metrics_payload["trade_count"] == 1
    assert manifest_payload["html_path"] is None
    assert manifest_payload["run_spec"]["engine_family"] == "nautilus"
    assert (
        manifest_payload["run_spec"]["execution_policy_version"]
        == "nautilus-daily-verification-v1"
    )
    assert engine_payload["engine"] == "nautilus"
    assert isinstance(engine_payload.get("engineVersion"), str)
    assert engine_payload["strategyCount"] == 1
    assert diagnostics_payload["engine"] == "nautilus"
    assert diagnostics_payload["tradePlanCount"] == 1
