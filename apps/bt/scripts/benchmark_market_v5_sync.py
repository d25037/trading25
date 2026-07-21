"""Fixture-backed benchmark of the production Market v5 sync coordinator.

Each scenario is seeded and materialized to a healthy baseline in one child
process, then measured in a fresh child process. Provider requests/pages,
store mutations, affected codes, published rows, and adjusted materializer
calls are recorded at the production boundaries that actually performed them.
The fixture supplies rows and universe shape only; it never supplies counters.
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass, field
from datetime import date, timedelta
import hashlib
import io
import json
import math
import os
from pathlib import Path
import resource
import re
import shutil
import subprocess
import sys
import tarfile
import time
from typing import Any, TypedDict

from src.application.services.adjusted_metrics_materializer import (
    AdjustedMetricsMaterializer,
)
from src.application.services.sync_stock_data_fetch import (
    StockDataIngestionSession,
    execute_stock_data_rest_date,
)
from src.application.services.sync_strategies import SyncContext
from src.infrastructure.db.market.market_writer_resources import (
    MarketWriterResourceFactory,
    MarketWriterSession,
)
from src.infrastructure.db.market.market_operation_lease import MarketOperationLease
from src.infrastructure.db.market.query_helpers import normalize_stock_code
from src.shared.provider_stock_window import ProviderStockStage


_SCENARIO_NAMES = (
    "provider_noop",
    "provider_one_day",
    "provider_fundamentals_only",
    "provider_split_drift",
    "legacy_all_code_local_projection",
)
_PAGE_SIZE = 250
_ACTUAL_V4_COMMIT = "1f0929f041b66b497df42ec6dd709563892c77ed"
_ACTUAL_V4_BLOB_HASHES = {
    "apps/bt/src/application/services/adjusted_metrics_materialization_run.py": (
        "a2daca9153624859c914113b4335f6cf26889d2b"
    ),
    "apps/bt/src/application/services/adjusted_metrics_materializer.py": (
        "6f0c9730732725e7cbaa064916e4820cf19a3cb8"
    ),
    "apps/bt/src/application/services/sync_service.py": (
        "eb971f64891a8d05f842677d88cc38382eda6a53"
    ),
    "apps/bt/src/application/services/sync_stock_data_fetch.py": (
        "bada33df26cf408599a84f38710693b6da625550"
    ),
    "apps/bt/src/application/services/sync_strategies.py": (
        "e2445e07a45ca8ebf5c06bf743ca9750ce534375"
    ),
    "apps/bt/src/infrastructure/db/market/market_writer_resources.py": (
        "bca8cb746ae2ca86342bb3c5a317a9a6791daddc"
    ),
    "apps/bt/src/infrastructure/db/market/time_series_store.py": (
        "7588fb35af530835d9f17e2890f23c4bda391db0"
    ),
}


def _required_non_negative_int(payload: dict[str, object], field: str) -> int:
    value = payload.get(field)
    if type(value) is not int or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return value


def _peak_rss_bytes() -> int:
    peak = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return peak if sys.platform == "darwin" else peak * 1024


def _tree_size(root: Path) -> int:
    return sum(path.stat().st_size for path in root.rglob("*") if path.is_file())


def _tree_checksum(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(path for path in root.rglob("*") if path.is_file()):
        digest.update(path.relative_to(root).as_posix().encode())
        with path.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                digest.update(chunk)
    return digest.hexdigest()


def _canonical_fingerprint(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _validated_representative_metrics(
    payload: object,
    *,
    schema_version: int,
    adjustment_mode: str,
) -> dict[str, object]:
    if not isinstance(payload, dict):
        raise ValueError("representative benchmark evidence must be an object")
    if payload.get("schemaVersion") != schema_version:
        raise ValueError(
            f"representative benchmark evidence must be schema v{schema_version}"
        )
    if payload.get("stockPriceAdjustmentMode") != adjustment_mode:
        raise ValueError(
            "representative benchmark evidence has an incompatible adjustment mode"
        )
    validated = dict(payload)
    for metric_field in ("wallSeconds", "cpuSeconds", "peakRssBytes"):
        value = payload.get(metric_field)
        if not isinstance(value, (int, float)) or isinstance(value, bool) or value <= 0:
            raise ValueError(
                f"representative benchmark evidence {metric_field} must be positive"
            )
    commit = payload.get("implementationCommit")
    if not isinstance(commit, str) or re.fullmatch(r"[0-9a-f]{40}", commit) is None:
        raise ValueError(
            "representative benchmark evidence requires an exact implementationCommit"
        )
    implementation_path = payload.get("implementationPath")
    if not isinstance(implementation_path, str) or not implementation_path.strip():
        raise ValueError(
            "representative benchmark evidence requires an exact implementationPath"
        )
    return validated


def _implementation_commit() -> str:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=Path(__file__).resolve().parents[3],
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _positive_metric(payload: dict[str, object], field: str) -> float:
    value = payload[field]
    assert isinstance(value, (int, float)) and not isinstance(value, bool)
    return float(value)


def _representative_comparison(
    v4_payload: object,
    v5_payload: object,
) -> dict[str, object]:
    v4 = _validated_representative_metrics(
        v4_payload,
        schema_version=4,
        adjustment_mode="local_projection_v2_event_time",
    )
    v5 = _validated_representative_metrics(
        v5_payload,
        schema_version=5,
        adjustment_mode="provider_adjusted_v1",
    )
    v4_fingerprint = v4.get("inputFingerprint")
    v5_fingerprint = v5.get("inputFingerprint")
    if (
        not isinstance(v4_fingerprint, str)
        or len(v4_fingerprint) != 64
        or v4_fingerprint != v5_fingerprint
    ):
        raise ValueError(
            "representative v4/v5 evidence must use the same inputFingerprint"
        )
    return {
        "v4": v4,
        "v5": v5,
        "v5ToV4WallRatio": _positive_metric(v5, "wallSeconds")
        / _positive_metric(v4, "wallSeconds"),
        "v5ToV4CpuRatio": _positive_metric(v5, "cpuSeconds")
        / _positive_metric(v4, "cpuSeconds"),
        "v5ToV4PeakRssRatio": _positive_metric(v5, "peakRssBytes")
        / _positive_metric(v4, "peakRssBytes"),
    }


def _codes(count: int) -> list[str]:
    return [f"{1000 + index:04d}" for index in range(count)]


def _raw_stock_rows(
    row_count: int,
    codes: list[str],
    *,
    adjustment_factor: float = 1.0,
    start: date = date(2026, 1, 1),
) -> list[dict[str, object]]:
    if row_count and not codes:
        raise ValueError("stock rows require at least one code")
    rows: list[dict[str, object]] = []
    for index in range(row_count):
        code = codes[index % len(codes)]
        trading_date = (start + timedelta(days=index // len(codes))).isoformat()
        price = float(100 + index % 17)
        rows.append(
            {
                "Code": f"{code}0",
                "Date": trading_date,
                "O": price,
                "H": price + 2,
                "L": price - 1,
                "C": price + 1,
                "Vo": 1_000 + index,
                "Va": (price + 1) * (1_000 + index),
                "AdjFactor": adjustment_factor,
                "AdjO": price,
                "AdjH": price + 2,
                "AdjL": price - 1,
                "AdjC": price + 1,
                "AdjVo": 1_000 + index,
            }
        )
    return rows


def _normalized_rows(raw_rows: list[dict[str, object]]) -> list[dict[str, object]]:
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
        for row in raw_rows
    ]


def _statement_rows(codes: list[str]) -> list[dict[str, object]]:
    return [
        {
            "code": code,
            "statement_id": f"{code}:2026-01-01",
            "disclosed_date": "2026-01-01",
            "disclosed_at": "2026-01-01T15:30:00+09:00",
            "period_start": "2025-01-01",
            "period_end": "2025-12-31",
            "type_of_current_period": "FY",
            "type_of_document": "FYFinancialStatements",
            "earnings_per_share": 10.0,
            "bps": 100.0,
        }
        for code in codes
    ]


@dataclass
class _ObservedFixtureClient:
    date_rows: list[dict[str, object]]
    provider_windows: dict[str, list[dict[str, object]]]
    plan: str = "standard"
    calls: list[dict[str, object]] = field(default_factory=list)

    async def get(
        self, path: str, params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        raise AssertionError(f"unexpected non-paginated request: {path} {params}")

    async def get_paginated(
        self, path: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        rows, _pages = await self.get_paginated_with_meta(path, params=params)
        return rows

    async def get_paginated_with_meta(
        self,
        path: str,
        params: dict[str, str] | None = None,
        max_pages: int = 10_000,
    ) -> tuple[list[dict[str, Any]], int]:
        normalized_params = dict(params or {})
        if path == "/equities/bars/daily" and "code" in normalized_params:
            code = normalized_params["code"]
            rows = self.provider_windows.get(code, [])
        elif path == "/equities/bars/daily" and "date" in normalized_params:
            rows = self.date_rows
        elif path == "/fins/summary":
            rows = []
        else:
            raise AssertionError(
                f"unexpected fixture request: {path} {normalized_params}"
            )
        pages = max(1, math.ceil(len(rows) / _PAGE_SIZE))
        if pages > max_pages:
            raise AssertionError("fixture response exceeded coordinator max_pages")
        for page in range(pages):
            self.calls.append(
                {"path": path, "params": normalized_params, "page": page + 1}
            )
        return [dict(row) for row in rows], pages


class _MaterializerResultObservation(TypedDict):
    totalCodes: int
    completedCodes: int
    failedCodes: int


class _MaterializerCallObservation(TypedDict):
    requestedCodes: list[str]
    processedCodes: list[str]
    result: _MaterializerResultObservation


@dataclass
class _MaterializerSpy:
    materializer: AdjustedMetricsMaterializer
    universe: frozenset[str]
    calls: list[_MaterializerCallObservation] = field(default_factory=list)

    async def rebuild(self, codes: frozenset[str]) -> None:
        requested = tuple(
            sorted(
                normalized
                for code in codes
                if (normalized := normalize_stock_code(code))
            )
        )
        processed: list[str] = []
        previous_completed = 0

        def observe_progress(
            completed: int, _total: int, code: str | None, _rows: int
        ) -> None:
            nonlocal previous_completed
            if code is not None and completed > previous_completed:
                processed.append(normalize_stock_code(code))
            previous_completed = completed

        result = await asyncio.to_thread(
            self.materializer.rebuild_current_basis,
            codes,
            on_progress=observe_progress,
        )
        self.calls.append(
            {
                "requestedCodes": list(requested),
                "processedCodes": processed,
                "result": {
                    "totalCodes": result.total_codes,
                    "completedCodes": result.completed_codes,
                    "failedCodes": result.total_codes - result.completed_codes,
                },
            }
        )

    @property
    def all_code_invocations(self) -> int:
        return sum(
            frozenset(call["processedCodes"]) == self.universe
            and call["result"]["completedCodes"] == len(self.universe)
            and call["result"]["failedCodes"] == 0
            for call in self.calls
        )

    @property
    def completed_codes(self) -> int:
        return sum(int(call["result"]["completedCodes"]) for call in self.calls)

    @property
    def processed_codes(self) -> int:
        return sum(len(call["processedCodes"]) for call in self.calls)


def _open_scenario(root: Path) -> MarketWriterSession:
    data_root = root / "data"
    return MarketWriterResourceFactory(
        data_root=data_root,
        market_root=data_root / "market-timeseries",
        # Historical factory method name is retained, but HEAD initializes the
        # current Market schema (v5/provider_adjusted_v1).
    ).reset_and_open_v4()


def _open_seeded_scenario(root: Path) -> MarketWriterSession:
    data_root = root / "data"
    return MarketWriterResourceFactory(
        data_root=data_root,
        market_root=data_root / "market-timeseries",
    ).open_existing()


def _close_scenario(session: MarketWriterSession) -> None:
    token = session.close_writable_handles()
    resources = session.reopen_read_only(token)
    resources.close()
    session.release_after_read_only_reopen(token)


def _shared_input(
    fixture: dict[str, object], name: str
) -> tuple[list[str], list[dict[str, object]], int]:
    scenarios = fixture["scenarios"]
    assert isinstance(scenarios, dict)
    legacy = scenarios["legacy_all_code_local_projection"]
    assert isinstance(legacy, dict)
    universe = _codes(_required_non_negative_int(legacy, "allCodes"))
    if name == "legacy_all_code_local_projection":
        source = scenarios["provider_one_day"]
    else:
        source = scenarios[name]
    assert isinstance(source, dict)
    row_count = _required_non_negative_int(source, "newRows")
    affected_count = _required_non_negative_int(source, "affectedCodes")
    rows_per_code = _required_non_negative_int(legacy, "rowsPerCode")
    target_codes = universe[:affected_count]
    factor = 0.5 if name == "provider_split_drift" else 1.0
    incoming_start = (
        date(2026, 1, 1)
        if name == "provider_split_drift"
        else date(2026, 1, 1) + timedelta(days=rows_per_code)
    )
    incoming = _raw_stock_rows(
        row_count,
        target_codes,
        adjustment_factor=factor,
        start=incoming_start,
    )
    return universe, incoming, rows_per_code


async def _run_price_coordinator(
    *,
    ctx: SyncContext,
    incoming: list[dict[str, object]],
    target_date: str,
    stage: ProviderStockStage,
) -> tuple[int, int, int, frozenset[str]]:
    session = StockDataIngestionSession()
    fetched = await execute_stock_data_rest_date(
        ctx,
        session=session,
        date=target_date,
    )
    outcome = await session.commit(ctx, stage=stage)
    return (
        fetched.api_calls + outcome.api_calls,
        outcome.appended_rows,
        outcome.replaced_rows,
        outcome.affected_codes,
    )


def _seed_scenario_child(
    name: str,
    fixture: dict[str, object],
    workspace: Path,
) -> dict[str, Any]:
    universe, _incoming, rows_per_code = _shared_input(fixture, name)
    scenario_root = workspace / name
    scenario_root.mkdir(parents=True, exist_ok=False)
    session = _open_scenario(scenario_root)
    store = session.handles.time_series_store
    market_db = session.handles.market_db
    history_raw = _raw_stock_rows(len(universe) * rows_per_code, universe)
    history = _normalized_rows(history_raw)
    provider_as_of = (
        date(2026, 1, 1) + timedelta(days=rows_per_code - 1)
    ).isoformat()
    provider_plan = fixture.get("providerPlan")
    if not isinstance(provider_plan, str):
        raise ValueError("benchmark seed requires providerPlan")
    try:
        if history:
            store.publish_topix_data(
                [
                    {
                        "date": row["date"],
                        "open": 1,
                        "high": 1,
                        "low": 1,
                        "close": 1,
                    }
                    for row in history[:: max(1, len(universe))]
                ]
            )
            store.publish_stock_data(
                history,
                stage=ProviderStockStage(
                    provider_plan=provider_plan,
                    provider_as_of=provider_as_of,
                    provider_codes=frozenset(universe),
                ),
            )
        store.index_stock_data()
        seed_result = AdjustedMetricsMaterializer(market_db).rebuild_current_basis(
            frozenset(universe)
        )
        snapshot = market_db.get_adjusted_metrics_snapshot()
        pending_codes = market_db.list_current_basis_recompute_pending_codes()
        expected_codes = len(universe)
        baseline = {
            "excludedFromMeasurements": True,
            "pendingCurrentBasisCodeCount": int(
                snapshot["pendingCurrentBasisCodeCount"]
            ),
            "readyProviderWindowCount": int(snapshot["readyProviderWindowCount"]),
            "currentBasisStateCount": int(snapshot["currentBasisStateCount"]),
        }
        expected_baseline = {
            "excludedFromMeasurements": True,
            "pendingCurrentBasisCodeCount": 0,
            "readyProviderWindowCount": expected_codes,
            "currentBasisStateCount": expected_codes,
        }
        if (
            seed_result.total_codes != expected_codes
            or seed_result.completed_codes != expected_codes
            or pending_codes
            or baseline != expected_baseline
            or int(snapshot["invalidCurrentBasisStateCount"]) != 0
        ):
            raise RuntimeError(
                "benchmark seed did not reach a healthy current-basis baseline: "
                f"result={seed_result!r}, pending={pending_codes!r}, "
                f"snapshot={snapshot!r}"
            )
        _close_scenario(session)
    except BaseException:
        if not session._handles_closed:
            _close_scenario(session)
        raise
    return {"seedProcessId": os.getpid(), "seedBaseline": baseline}


def _run_scenario_child(
    name: str,
    fixture: dict[str, object],
    workspace: Path,
) -> dict[str, Any]:
    scenarios = fixture["scenarios"]
    assert isinstance(scenarios, dict)
    payload = scenarios[name]
    assert isinstance(payload, dict)
    provider_plan = fixture.get("providerPlan")
    if not isinstance(provider_plan, str):
        raise ValueError("benchmark scenario requires providerPlan")
    universe, incoming, rows_per_code = _shared_input(fixture, name)
    target_date = (
        date(2026, 1, 1)
        if name == "provider_split_drift"
        else date(2026, 1, 1) + timedelta(days=rows_per_code)
    ).isoformat()
    scenario_root = workspace / name
    session = _open_seeded_scenario(scenario_root)
    store = session.handles.time_series_store
    market_db = session.handles.market_db
    history_raw = _raw_stock_rows(len(universe) * rows_per_code, universe)
    if name == "provider_noop":
        incoming = history_raw[:1]
    provider_windows = {
        f"{code}0": [row for row in history_raw if row["Code"] == f"{code}0"]
        for code in universe
    }
    if name == "provider_split_drift" and incoming:
        code = str(incoming[0]["Code"])
        replacement = [
            dict(row)
            for row in provider_windows[code]
            if str(row["Date"]) <= target_date
        ]
        replacement[0] = dict(incoming[0])
        provider_windows[code] = replacement
    client = _ObservedFixtureClient(incoming, provider_windows)
    spy = _MaterializerSpy(
        AdjustedMetricsMaterializer(market_db),
        frozenset(universe),
    )
    published_rows = 0
    replaced_rows = 0
    affected: frozenset[str] = frozenset()
    before_storage = _tree_size(scenario_root)
    started_wall = time.perf_counter()
    started_cpu = time.process_time()
    try:
        if name == "provider_fundamentals_only":
            target_codes = universe[
                : _required_non_negative_int(payload, "affectedCodes")
            ]
            asyncio.run(
                client.get_paginated_with_meta(
                    "/fins/summary", params={"code": f"{target_codes[0]}0"}
                )
            )
            result = store.publish_statements(_statement_rows(target_codes))
            affected = result.affected_codes
            published_rows = result.stats.inserted
            asyncio.run(spy.rebuild(affected))
            coordinator = "FundamentalsPublish+AdjustedMetricsMaterializer"
        else:
            ctx = SyncContext(
                client=client,
                market_db=market_db,
                time_series_store=store,
                cancelled=asyncio.Event(),
                on_progress=lambda *_args: None,
                provider_plan=provider_plan,
                recompute_affected_stock_codes=spy.rebuild,
            )
            _, published_rows, replaced_rows, affected = asyncio.run(
                _run_price_coordinator(
                    ctx=ctx,
                    incoming=incoming,
                    target_date=target_date,
                    stage=ProviderStockStage(
                        provider_plan=provider_plan,
                        provider_as_of=target_date,
                        provider_codes=frozenset(universe),
                    ),
                )
            )
            coordinator = "StockDataIngestionSession"
            if name == "legacy_all_code_local_projection":
                asyncio.run(spy.rebuild(frozenset(universe)))
                coordinator += "+LegacyAllCodeAdapter"
            else:
                pending_codes = frozenset(
                    market_db.list_current_basis_recompute_pending_codes()
                )
                if pending_codes:
                    asyncio.run(spy.rebuild(pending_codes))
        store.index_stock_data()
        store.index_statements()
        snapshot_after = market_db.get_adjusted_metrics_snapshot()
        pending_after = int(snapshot_after["pendingCurrentBasisCodeCount"])
        if pending_after != 0:
            raise RuntimeError(
                "benchmark measurement left current-basis recompute pending: "
                f"{pending_after}"
            )
        _close_scenario(session)
    except BaseException:
        if not session._handles_closed:
            _close_scenario(session)
        raise
    wall_seconds = time.perf_counter() - started_wall
    cpu_seconds = time.process_time() - started_cpu
    input_fingerprint = _canonical_fingerprint(
        {"universe": universe, "incoming": incoming}
    )
    row_mutations = published_rows + replaced_rows
    work_units = len(client.calls) + row_mutations + spy.completed_codes
    return {
        "engine": payload.get("engine"),
        "coordinator": coordinator,
        "measurementPath": "production_sync_coordinator_duckdb_parquet",
        "processId": os.getpid(),
        "wallSeconds": round(wall_seconds, 9),
        "cpuSeconds": round(cpu_seconds, 9),
        "peakRssBytes": _peak_rss_bytes(),
        "requests": len(
            {
                (call["path"], json.dumps(call["params"], sort_keys=True))
                for call in client.calls
            }
        ),
        "pages": len(client.calls),
        "affectedCodes": len(affected),
        "publishedCodes": len({str(row["Code"]) for row in incoming}),
        "newRows": published_rows,
        "rowMutations": row_mutations,
        "providerWindowRowsReplaced": replaced_rows,
        "currentBasisRecomputedCodes": spy.processed_codes,
        "currentBasisCompletedCodes": spy.completed_codes,
        "currentBasisProcessedCodes": spy.processed_codes,
        "pendingCurrentBasisCodeCountAfterMeasurement": pending_after,
        "workUnits": work_units,
        "storageGrowthBytes": max(0, _tree_size(scenario_root) - before_storage),
        "allCodeMaterializerInvocations": spy.all_code_invocations,
        "materializerSpy": {
            "implementation": "AdjustedMetricsMaterializer.rebuild_current_basis",
            "calls": spy.calls,
        },
        "observations": {
            "fixtureDeclaredCounters": False,
            "clientCalls": client.calls,
            "storePublishedRows": published_rows,
            "storeReplacedRows": replaced_rows,
        },
        "inputFingerprint": input_fingerprint,
        "checksumSha256": _tree_checksum(scenario_root),
    }


def _run_representative_v5_child(
    fixture: dict[str, object],
    workspace: Path,
) -> dict[str, object]:
    scenarios = fixture["scenarios"]
    assert isinstance(scenarios, dict)
    payload = scenarios["provider_one_day"]
    assert isinstance(payload, dict)
    row_count = _required_non_negative_int(payload, "newRows")
    affected_count = _required_non_negative_int(payload, "affectedCodes")
    codes = _codes(affected_count)
    session = _open_seeded_scenario(workspace)
    store = session.handles.time_series_store
    market_db = session.handles.market_db
    latest = market_db.get_latest_stock_data_date()
    start = (
        date.fromisoformat(latest) + timedelta(days=1)
        if latest is not None
        else date(2026, 1, 1)
    )
    incoming = _raw_stock_rows(row_count, codes, start=start)
    client = _ObservedFixtureClient(incoming, {})
    provider_plan = fixture.get("providerPlan")
    if not isinstance(provider_plan, str):
        raise ValueError("representative benchmark requires providerPlan")
    ctx = SyncContext(
        client=client,
        market_db=market_db,
        time_series_store=store,
        cancelled=asyncio.Event(),
        on_progress=lambda *_args: None,
        provider_plan=provider_plan,
        recompute_affected_stock_codes=lambda _codes: asyncio.sleep(0),
    )
    before_storage = _tree_size(workspace)
    started_wall = time.perf_counter()
    started_cpu = time.process_time()
    try:
        provider_as_of = start.isoformat()
        api_calls, appended_rows, replaced_rows, affected = asyncio.run(
            _run_price_coordinator(
                ctx=ctx,
                incoming=incoming,
                target_date=provider_as_of,
                stage=ProviderStockStage(
                    provider_plan=provider_plan,
                    provider_as_of=provider_as_of,
                    provider_codes=frozenset(codes),
                ),
            )
        )
        store.index_stock_data()
        _close_scenario(session)
    except BaseException:
        if not session._handles_closed:
            _close_scenario(session)
        raise
    return {
        "schemaVersion": 5,
        "stockPriceAdjustmentMode": "provider_adjusted_v1",
        "implementationCommit": _implementation_commit(),
        "implementationPath": (
            "apps/bt/src/application/services/sync_stock_data_fetch.py:"
            "execute_stock_data_rest_date+StockDataIngestionSession.commit"
        ),
        "measurementPath": (
            "representative_copy_production_sync_coordinator_duckdb_parquet"
        ),
        "processId": os.getpid(),
        "wallSeconds": time.perf_counter() - started_wall,
        "cpuSeconds": time.process_time() - started_cpu,
        "peakRssBytes": _peak_rss_bytes(),
        "requests": api_calls,
        "pages": len(client.calls),
        "newRows": appended_rows,
        "providerWindowRowsReplaced": replaced_rows,
        "affectedCodes": len(affected),
        "storageGrowthBytes": max(0, _tree_size(workspace) - before_storage),
        "inputFingerprint": _canonical_fingerprint(incoming),
    }


def _run_representative_v5_benchmark(
    fixture: dict[str, object],
    *,
    market_root: Path,
    workspace: Path,
) -> dict[str, object]:
    source_database = market_root / "market.duckdb"
    if not source_database.is_file():
        raise FileNotFoundError(f"representative market.duckdb not found: {market_root}")
    source_before = _tree_checksum(market_root)
    scenario_root = workspace / "scenario"
    destination = scenario_root / "data" / "market-timeseries"
    destination.parent.mkdir(parents=True, exist_ok=False)
    with MarketOperationLease.acquire(
        market_root.parent,
        exclusive=False,
        blocking=False,
    ):
        shutil.copytree(market_root, destination)
    if _tree_checksum(market_root) != source_before:
        raise RuntimeError("representative Market changed while its copy was prepared")
    fixture_path = workspace / "representative-fixture.json"
    fixture_path.write_text(json.dumps(fixture, sort_keys=True), encoding="utf-8")
    completed = subprocess.run(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            "--fixture",
            str(fixture_path),
            "--workspace",
            str(scenario_root),
            "--representative-v5-child",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            "representative v5 benchmark child failed "
            f"({completed.returncode}): {completed.stderr.strip()}"
        )
    result = json.loads(completed.stdout)
    if not isinstance(result, dict):
        raise RuntimeError("representative v5 benchmark child returned a non-object")
    if _tree_checksum(market_root) != source_before:
        raise RuntimeError("representative Market source was mutated by benchmark")
    result["sourceMarketTreeSha256"] = source_before
    return result


def _run_isolated_child(
    name: str,
    fixture_path: Path,
    workspace: Path,
    *,
    seed: bool,
) -> dict[str, Any]:
    mode = "seed" if seed else "measurement"
    completed = subprocess.run(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            "--fixture",
            str(fixture_path),
            "--workspace",
            str(workspace),
            "--seed-scenario" if seed else "--child-scenario",
            name,
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"benchmark {mode} child {name} failed ({completed.returncode}): "
            f"{completed.stderr.strip()}"
        )
    result = json.loads(completed.stdout)
    if not isinstance(result, dict):
        raise RuntimeError("benchmark child returned a non-object")
    return result


def _run_actual_v4_benchmark(
    fixture: dict[str, object],
    *,
    workspace: Path,
) -> dict[str, object]:
    """Measure the exact merge-base v4 adjusted_metrics_pit implementation."""

    workspace = workspace.resolve()
    repository_root = Path(__file__).resolve().parents[3]
    for path, expected_hash in _ACTUAL_V4_BLOB_HASHES.items():
        completed = subprocess.run(
            ["git", "rev-parse", f"{_ACTUAL_V4_COMMIT}:{path}"],
            cwd=repository_root,
            check=True,
            capture_output=True,
            text=True,
        )
        if completed.stdout.strip() != expected_hash:
            raise RuntimeError(f"actual v4 source blob mismatch: {path}")
    archive = subprocess.run(
        ["git", "archive", _ACTUAL_V4_COMMIT, "apps/bt/src"],
        cwd=repository_root,
        check=True,
        capture_output=True,
    )
    source_root = workspace / "source"
    source_root.mkdir(parents=True, exist_ok=False)
    with tarfile.open(fileobj=io.BytesIO(archive.stdout), mode="r:") as bundle:
        bundle.extractall(source_root, filter="data")
    fixture_path = workspace / "fixture.json"
    fixture_path.write_text(json.dumps(fixture, sort_keys=True), encoding="utf-8")
    hashes_path = workspace / "source-blob-hashes.json"
    hashes_path.write_text(
        json.dumps(_ACTUAL_V4_BLOB_HASHES, sort_keys=True), encoding="utf-8"
    )
    scenario_root = workspace / "scenario"
    helper = Path(__file__).with_name("benchmark_market_v4_actual.py")
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(source_root / "apps/bt")
    common = [
        sys.executable,
        str(helper),
        "--fixture",
        str(fixture_path),
        "--workspace",
        str(scenario_root),
        "--implementation-commit",
        _ACTUAL_V4_COMMIT,
        "--source-blob-hashes",
        str(hashes_path),
    ]
    for operation in ("--seed", "--measure"):
        completed = subprocess.run(
            [*common, operation],
            cwd=workspace,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            raise RuntimeError(
                f"actual v4 benchmark {operation} failed ({completed.returncode}): "
                f"{completed.stderr.strip()}"
            )
        payload = json.loads(completed.stdout)
        if not isinstance(payload, dict):
            raise RuntimeError("actual v4 benchmark child returned a non-object")
        if operation == "--measure":
            return payload
    raise AssertionError("actual v4 measurement child did not run")


def run_benchmark_fixture(
    fixture: dict[str, object],
    *,
    evidence_source: str = "production_fixture",
    representative_evidence_reason: str | None = None,
    representative_inspection: dict[str, object] | None = None,
    representative_comparison: dict[str, object] | None = None,
    representative_fixture_comparison: bool = False,
    actual_v4_metrics: dict[str, object] | None = None,
    workspace: Path,
) -> dict[str, Any]:
    if representative_fixture_comparison:
        raise ValueError(
            "legacy all-code fixture adapter is not actual Market v4 evidence"
        )
    if fixture.get("fixtureVersion") != 1:
        raise ValueError("fixtureVersion must be exactly 1")
    scenarios = fixture.get("scenarios")
    if not isinstance(scenarios, dict) or set(scenarios) != set(_SCENARIO_NAMES):
        raise ValueError("fixture must define the exact benchmark scenario set")
    workspace.mkdir(parents=True, exist_ok=True)
    fixture_path = workspace / "child-fixture.json"
    fixture_path.write_text(json.dumps(fixture, sort_keys=True), encoding="utf-8")
    measured: dict[str, dict[str, Any]] = {}
    for name in _SCENARIO_NAMES:
        seed = _run_isolated_child(name, fixture_path, workspace, seed=True)
        metrics = _run_isolated_child(name, fixture_path, workspace, seed=False)
        metrics.update(seed)
        measured[name] = metrics
    normal = measured["provider_one_day"]
    fundamentals = measured["provider_fundamentals_only"]
    drift = measured["provider_split_drift"]
    legacy = measured["legacy_all_code_local_projection"]
    fundamentals_payload = scenarios["provider_fundamentals_only"]
    drift_payload = scenarios["provider_split_drift"]
    assert isinstance(fundamentals_payload, dict)
    assert isinstance(drift_payload, dict)
    expected_fundamentals = _required_non_negative_int(
        fundamentals_payload, "affectedCodes"
    )
    expected_drift = _required_non_negative_int(drift_payload, "affectedCodes")
    assertions = {
        "legacyBaselineInvokesAllCodeMaterializer": legacy[
            "allCodeMaterializerInvocations"
        ]
        == 1,
        "normalIncrementalUsesNoAllCodeMaterializer": normal[
            "allCodeMaterializerInvocations"
        ]
        == 0,
        "normalIncrementalWorkBelowLegacyBaseline": normal["workUnits"]
        < legacy["workUnits"],
        "splitDriftRefreshLimitedToAffectedCodes": drift["affectedCodes"]
        < len(
            _codes(
                _required_non_negative_int(
                    scenarios["legacy_all_code_local_projection"], "allCodes"
                )
            )
        )
        and drift["allCodeMaterializerInvocations"] == 0,
        "currentAndLegacyUseIdenticalInput": normal["inputFingerprint"]
        == legacy["inputFingerprint"],
        "scenariosUseIsolatedProcesses": len(
            {metrics["processId"] for metrics in measured.values()}
        )
        == len(measured),
        "seedDrainExcludedInSeparateProcesses": all(
            metrics["seedProcessId"] != metrics["processId"]
            and metrics["seedBaseline"]["pendingCurrentBasisCodeCount"] == 0
            for metrics in measured.values()
        ),
        "boundedMaterializerUsesActualProcessedCodes": (
            fundamentals["currentBasisCompletedCodes"] == expected_fundamentals
            and fundamentals["currentBasisProcessedCodes"] == expected_fundamentals
            and drift["currentBasisCompletedCodes"] == expected_drift
            and drift["currentBasisProcessedCodes"] == expected_drift
            and all(
                metrics["pendingCurrentBasisCodeCountAfterMeasurement"] == 0
                for metrics in measured.values()
            )
        ),
    }
    if actual_v4_metrics is not None:
        if representative_comparison is not None:
            raise ValueError("actual v4 metrics conflict with explicit comparison")
        v5_path = "apps/bt/src/application/services/sync_stock_data_fetch.py"
        repository_root = Path(__file__).resolve().parents[3]
        v5_blob = subprocess.run(
            ["git", "rev-parse", f"HEAD:{v5_path}"],
            cwd=repository_root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        representative_comparison = _representative_comparison(
            actual_v4_metrics,
            {
                "schemaVersion": 5,
                "stockPriceAdjustmentMode": "provider_adjusted_v1",
                "implementationCommit": _implementation_commit(),
                "implementationPath": (
                    f"{v5_path}:execute_stock_data_rest_date+"
                    "StockDataIngestionSession.commit"
                ),
                "implementationBlobHashes": {v5_path: v5_blob},
                "measurementPath": normal["measurementPath"],
                "wallSeconds": normal["wallSeconds"],
                "cpuSeconds": normal["cpuSeconds"],
                "peakRssBytes": normal["peakRssBytes"],
                "inputFingerprint": normal["inputFingerprint"],
            },
        )
    return {
        "schemaVersion": 2,
        "benchmark": "market_v5_incremental_sync",
        "evidenceSource": evidence_source,
        "representativeEvidence": (
            "measured" if representative_comparison is not None else "unavailable"
        ),
        "representativeEvidenceReason": (
            None
            if representative_comparison is not None
            else representative_evidence_reason
        ),
        "representativeInspection": representative_inspection,
        "representativeComparison": representative_comparison,
        "providerPlan": fixture.get("providerPlan"),
        "measurementNotes": {
            "resources": "per-scenario child-process wall/cpu/peak RSS",
            "seed": "production materializer drain in a separate process before resource measurement",
            "requests": "observed fixture-backed J-Quants client calls/pages",
            "storage": "isolated Market v5 DuckDB + Parquet byte growth",
            "scaling": "observed production coordinator/store/materializer calls",
            "materializerResults": "actual total/completed/failed result and successful normalized processed-code callbacks",
            "legacyBaseline": "explicit all-code legacy adapter over identical input",
        },
        "scenarios": measured,
        "comparison": {
            "normalToLegacyWorkRatio": normal["workUnits"] / legacy["workUnits"],
            "normalToLegacyRowMutationRatio": (
                normal["rowMutations"] / legacy["rowMutations"]
                if legacy["rowMutations"]
                else 0.0
            ),
            "normalAffectedCodeDelta": legacy["affectedCodes"]
            - normal["affectedCodes"],
        },
        "assertions": assertions,
        "allAssertionsPassed": all(assertions.values()),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--representative-evidence-reason")
    parser.add_argument("--representative-market-root", type=Path)
    parser.add_argument("--representative-v4-evidence", type=Path)
    parser.add_argument("--actual-v4-merge-base", action="store_true")
    parser.add_argument("--child-scenario", choices=_SCENARIO_NAMES)
    parser.add_argument("--seed-scenario", choices=_SCENARIO_NAMES)
    parser.add_argument("--representative-v5-child", action="store_true")
    args = parser.parse_args()
    fixture = json.loads(args.fixture.read_text(encoding="utf-8"))
    if not isinstance(fixture, dict):
        raise ValueError("benchmark fixture must be an object")
    if args.child_scenario:
        result = _run_scenario_child(args.child_scenario, fixture, args.workspace)
        print(json.dumps(result, sort_keys=True))
        return 0
    if args.seed_scenario:
        result = _seed_scenario_child(args.seed_scenario, fixture, args.workspace)
        print(json.dumps(result, sort_keys=True))
        return 0
    if args.representative_v5_child:
        result = _run_representative_v5_child(fixture, args.workspace)
        print(json.dumps(result, sort_keys=True))
        return 0
    representative_inspection: dict[str, object] | None = None
    representative_comparison: dict[str, object] | None = None
    actual_v4_metrics: dict[str, object] | None = None
    representative_reason = args.representative_evidence_reason
    if args.actual_v4_merge_base:
        actual_v4_metrics = _run_actual_v4_benchmark(
            fixture,
            workspace=args.workspace / "actual-v4",
        )
        representative_reason = None
    if args.representative_market_root is not None:
        import duckdb

        database = args.representative_market_root / "market.duckdb"
        representative_inspection = {
            "marketRoot": str(args.representative_market_root),
            "databasePresent": database.is_file(),
        }
        if database.is_file():
            connection = duckdb.connect(str(database), read_only=True)
            try:
                schema_row = connection.execute(
                    "SELECT MAX(version) FROM market_schema_version"
                ).fetchone()
                mode_row = connection.execute(
                    "SELECT value FROM sync_metadata "
                    "WHERE key = 'stock_price_adjustment_mode'"
                ).fetchone()
            finally:
                connection.close()
            schema_version = schema_row[0] if schema_row else None
            adjustment_mode = mode_row[0] if mode_row else None
            representative_inspection.update(
                {
                    "schemaVersion": schema_version,
                    "stockPriceAdjustmentMode": adjustment_mode,
                    "eligible": schema_version == 5
                    and adjustment_mode == "provider_adjusted_v1",
                }
            )
            if representative_inspection["eligible"]:
                if args.representative_v4_evidence is None:
                    raise ValueError(
                        "eligible representative Market requires "
                        "--representative-v4-evidence"
                    )
                v4_metrics = json.loads(
                    args.representative_v4_evidence.read_text(encoding="utf-8")
                )
                v5_metrics = _run_representative_v5_benchmark(
                    fixture,
                    market_root=args.representative_market_root,
                    workspace=args.workspace / "representative-v5",
                )
                representative_comparison = _representative_comparison(
                    v4_metrics,
                    v5_metrics,
                )
                representative_reason = None
            else:
                representative_reason = (
                    "read-only inspection found local Market schema "
                    f"{schema_version!r} / mode {adjustment_mode!r}, not eligible v5"
                )
        else:
            representative_inspection["eligible"] = False
            representative_reason = "read-only inspection found no local market.duckdb"
    report = run_benchmark_fixture(
        fixture,
        evidence_source=(
            "matched_actual_v4_production_fixture"
            if actual_v4_metrics is not None
            else "production_fixture"
        ),
        representative_evidence_reason=representative_reason,
        representative_inspection=representative_inspection,
        representative_comparison=representative_comparison,
        actual_v4_metrics=actual_v4_metrics,
        workspace=args.workspace,
    )
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output is None:
        print(payload, end="")
    else:
        args.output.write_text(payload, encoding="utf-8")
    return 0 if report["allAssertionsPassed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
