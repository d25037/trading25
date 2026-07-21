# pyright: reportMissingImports=false, reportArgumentType=false
"""Child harness that executes an extracted, provenance-checked Market v4 tree.

This file only supplies deterministic fixture rows and process measurements.
All Market writes and adjusted-metrics orchestration are imported from the
merge-base source tree selected by ``benchmark_market_v5_sync.py``.
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import date, timedelta
import hashlib
import json
import os
from pathlib import Path
import resource
import sys
import time
from typing import Any

from src.application.services.adjusted_metrics_materialization_run import (
    run_shielded_materialization,
)
from src.application.services.adjusted_metrics_materializer import (
    AdjustedMetricsMaterializer,
)
from src.application.services.sync_stock_data_fetch import (
    execute_stock_data_rest_date,
)
from src.application.services.sync_strategies import SyncContext
from src.infrastructure.db.market.market_writer_resources import (
    MarketWriterResourceFactory,
    MarketWriterSession,
)


def _codes(count: int) -> list[str]:
    return [f"{1000 + index:04d}" for index in range(count)]


def _raw_stock_rows(
    row_count: int,
    codes: list[str],
    *,
    start: date,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for index in range(row_count):
        code = codes[index % len(codes)]
        session = (start + timedelta(days=index // len(codes))).isoformat()
        price = float(100 + index % 17)
        rows.append(
            {
                "Code": f"{code}0",
                "Date": session,
                "O": price,
                "H": price + 2,
                "L": price - 1,
                "C": price + 1,
                "Vo": 1_000 + index,
                "Va": (price + 1) * (1_000 + index),
                "AdjFactor": 1.0,
                "AdjO": price,
                "AdjH": price + 2,
                "AdjL": price - 1,
                "AdjC": price + 1,
                "AdjVo": 1_000 + index,
            }
        )
    return rows


def _normalized_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    return [
        {
            "code": str(row["Code"])[:-1],
            "date": row["Date"],
            "open": row["O"],
            "high": row["H"],
            "low": row["L"],
            "close": row["C"],
            "volume": row["Vo"],
            "turnover_value": row["Va"],
            "adjustment_factor": row["AdjFactor"],
            "adjusted_open": row["AdjO"],
            "adjusted_high": row["AdjH"],
            "adjusted_low": row["AdjL"],
            "adjusted_close": row["AdjC"],
            "adjusted_volume": row["AdjVo"],
            "created_at": f"{row['Date']}T00:00:00+00:00",
        }
        for row in rows
    ]


def _fingerprint(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _peak_rss_bytes() -> int:
    peak = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return peak if sys.platform == "darwin" else peak * 1024


def _tree_size(root: Path) -> int:
    return sum(path.stat().st_size for path in root.rglob("*") if path.is_file())


class _FixtureClient:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = rows

    async def get_paginated_with_meta(
        self,
        path: str,
        params: dict[str, object] | None = None,
    ) -> tuple[list[dict[str, Any]], int]:
        if path != "/equities/bars/daily" or "date" not in (params or {}):
            raise AssertionError(f"unexpected actual-v4 fixture request: {path}")
        return [dict(row) for row in self._rows], 1


def _factory(workspace: Path) -> MarketWriterResourceFactory:
    data_root = workspace / "data"
    return MarketWriterResourceFactory(
        data_root=data_root,
        market_root=data_root / "market-timeseries",
    )


def _close(session: MarketWriterSession) -> None:
    token = session.close_writable_handles()
    resources = session.reopen_read_only(token)
    resources.close()
    session.release_after_read_only_reopen(token)


def _fixture_shape(fixture: dict[str, object]) -> tuple[list[str], int, int, int]:
    scenarios = fixture["scenarios"]
    assert isinstance(scenarios, dict)
    legacy = scenarios["legacy_all_code_local_projection"]
    one_day = scenarios["provider_one_day"]
    assert isinstance(legacy, dict) and isinstance(one_day, dict)
    all_codes = int(legacy["allCodes"])
    rows_per_code = int(legacy["rowsPerCode"])
    incoming_rows = int(one_day["newRows"])
    affected_codes = int(one_day["affectedCodes"])
    return _codes(all_codes), rows_per_code, incoming_rows, affected_codes


def _seed(fixture: dict[str, object], workspace: Path) -> dict[str, object]:
    codes, rows_per_code, _incoming_rows, _affected_codes = _fixture_shape(fixture)
    history = _raw_stock_rows(
        len(codes) * rows_per_code,
        codes,
        start=date(2026, 1, 1),
    )
    session = _factory(workspace).reset_and_open()
    store = session.handles.time_series_store
    try:
        store.publish_topix_data(
            [
                {"date": row["Date"], "open": 1, "high": 1, "low": 1, "close": 1}
                for row in history[:: len(codes)]
            ]
        )
        store.publish_stock_data(_normalized_rows(history))
        store.index_topix_data()
        store.index_stock_data()
        result = asyncio.run(
            run_shielded_materialization(
                AdjustedMetricsMaterializer(session.handles.market_db),
                timeout_seconds=3_600,
                on_progress=lambda _progress: None,
            )
        )
        if result.completed_codes != len(codes):
            raise RuntimeError("actual v4 seed materialization did not complete")
        _close(session)
    except BaseException:
        if not session._handles_closed:
            _close(session)
        raise
    return {"seedProcessId": os.getpid(), "seedCompletedCodes": result.completed_codes}


def _measure(
    fixture: dict[str, object],
    workspace: Path,
    *,
    implementation_commit: str,
    source_blob_hashes: dict[str, str],
) -> dict[str, object]:
    codes, rows_per_code, incoming_rows, affected_codes = _fixture_shape(fixture)
    incoming = _raw_stock_rows(
        incoming_rows,
        codes[:affected_codes],
        start=date(2026, 1, 1) + timedelta(days=rows_per_code),
    )
    session = _factory(workspace).open_existing()
    store = session.handles.time_series_store
    before_storage = _tree_size(workspace)
    started_wall = time.perf_counter()
    started_cpu = time.process_time()
    try:
        async def run_production_paths():
            ctx = SyncContext(
                client=_FixtureClient(incoming),
                market_db=session.handles.market_db,
                time_series_store=store,
                cancelled=asyncio.Event(),
                on_progress=lambda *_args: None,
            )
            ingestion = await execute_stock_data_rest_date(
                ctx,
                date=str(incoming[0]["Date"]),
            )
            store.index_stock_data()
            materialization = await run_shielded_materialization(
                AdjustedMetricsMaterializer(session.handles.market_db),
                timeout_seconds=3_600,
                on_progress=lambda _progress: None,
            )
            return ingestion, materialization

        ingestion, result = asyncio.run(run_production_paths())
        _close(session)
    except BaseException:
        if not session._handles_closed:
            _close(session)
        raise
    return {
        "schemaVersion": 4,
        "stockPriceAdjustmentMode": "local_projection_v2_event_time",
        "implementationCommit": implementation_commit,
        "implementationPath": (
            "apps/bt/src/application/services/sync_stock_data_fetch.py:"
            "execute_stock_data_rest_date -> "
            "adjusted_metrics_materialization_run.py:run_shielded_materialization -> "
            "adjusted_metrics_materializer.py:AdjustedMetricsMaterializer.reconcile"
        ),
        "productionEntryReference": (
            "apps/bt/src/application/services/sync_service.py:500-505;"
            "apps/bt/src/application/services/sync_strategies.py:1307-1314"
        ),
        "implementationBlobHashes": source_blob_hashes,
        "measurementPath": "isolated_merge_base_v4_production_adjusted_metrics_pit",
        "processId": os.getpid(),
        "wallSeconds": time.perf_counter() - started_wall,
        "cpuSeconds": time.process_time() - started_cpu,
        "peakRssBytes": _peak_rss_bytes(),
        "newRows": ingestion.stocks_updated,
        "allCodeMaterializerInvocations": 1,
        "materializedCodes": result.completed_codes,
        "storageGrowthBytes": max(0, _tree_size(workspace) - before_storage),
        "inputFingerprint": _fingerprint({"universe": codes, "incoming": incoming}),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--implementation-commit", required=True)
    parser.add_argument("--source-blob-hashes", type=Path, required=True)
    operation = parser.add_mutually_exclusive_group(required=True)
    operation.add_argument("--seed", action="store_true")
    operation.add_argument("--measure", action="store_true")
    args = parser.parse_args()
    fixture = json.loads(args.fixture.read_text(encoding="utf-8"))
    hashes = json.loads(args.source_blob_hashes.read_text(encoding="utf-8"))
    if not isinstance(fixture, dict) or not isinstance(hashes, dict):
        raise ValueError("fixture and source blob hashes must be objects")
    result = (
        _seed(fixture, args.workspace)
        if args.seed
        else _measure(
            fixture,
            args.workspace,
            implementation_commit=args.implementation_commit,
            source_blob_hashes={str(key): str(value) for key, value in hashes.items()},
        )
    )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
