"""Fixture-backed benchmark of the production Market v5 publish path.

Every measured scenario opens an isolated Market writer and invokes the same
DuckDB/Parquet store methods used by incremental sync. The fixture controls
only input cardinality and page batches; mutation and affected-code counters
come from production ``SemanticDeltaResult`` values. No active Market or
J-Quants endpoint is opened.
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta
import hashlib
import json
from pathlib import Path
import resource
import sys
import time
from typing import Any

from src.application.services.adjusted_metrics_materializer import (
    AdjustedMetricsMaterializer,
)
from src.infrastructure.db.market.market_mutations import SemanticDeltaResult
from src.infrastructure.db.market.market_writer_resources import (
    MarketWriterResourceFactory,
    MarketWriterSession,
)
from src.shared.provider_stock_window import provider_stock_source_fingerprint


_SCENARIO_NAMES = (
    "provider_noop",
    "provider_one_day",
    "provider_fundamentals_only",
    "provider_split_drift",
    "legacy_all_code_local_projection",
)


class _FixturePageClient:
    def __init__(self, scenario: str) -> None:
        self._scenario = scenario
        self.requests = 0
        self.digest = hashlib.sha256()

    def fetch_pages(self, pages: int) -> None:
        for page in range(pages):
            self.requests += 1
            self.digest.update(f"{self._scenario}:page:{page}".encode())


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


def _codes(count: int) -> list[str]:
    return [f"{1000 + index:04d}" for index in range(count)]


def _stock_rows(row_count: int, codes: list[str]) -> list[dict[str, object]]:
    if row_count and not codes:
        raise ValueError("stock rows require at least one affected code")
    start = date(2026, 1, 1)
    rows: list[dict[str, object]] = []
    for index in range(row_count):
        code = codes[index % len(codes)]
        trading_date = (start + timedelta(days=index // len(codes))).isoformat()
        price = float(100 + index % 17)
        rows.append(
            {
                "code": code,
                "date": trading_date,
                "open": price,
                "high": price + 2,
                "low": price - 1,
                "close": price + 1,
                "volume": 1_000 + index,
                "turnover_value": (price + 1) * (1_000 + index),
                "adjustment_factor": 1.0,
                "adjusted_open": price,
                "adjusted_high": price + 2,
                "adjusted_low": price - 1,
                "adjusted_close": price + 1,
                "adjusted_volume": 1_000 + index,
                "created_at": f"{trading_date}T00:00:00+00:00",
            }
        )
    return rows


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


def _open_scenario(root: Path) -> MarketWriterSession:
    data_root = root / "data"
    return MarketWriterResourceFactory(
        data_root=data_root,
        market_root=data_root / "market-timeseries",
    ).reset_and_open_v4()


def _close_scenario(session: MarketWriterSession) -> None:
    token = session.close_writable_handles()
    resources = session.reopen_read_only(token)
    resources.close()
    session.release_after_read_only_reopen(token)


def _mutation_metrics(result: SemanticDeltaResult) -> tuple[int, int, set[str]]:
    return result.stats.inserted, result.mutated_rows, set(result.affected_codes)


def _run_scenario(
    name: str,
    payload: dict[str, object],
    workspace: Path,
    *,
    universe_size: int,
) -> dict[str, Any]:
    engine = payload.get("engine")
    if engine not in {"provider_v5_incremental", "local_projection_all_code"}:
        raise ValueError(f"{name}.engine is unsupported")
    pages = _required_non_negative_int(payload, "pages")
    scenario_root = workspace / name
    scenario_root.mkdir(parents=True, exist_ok=False)
    session = _open_scenario(scenario_root)
    store = session.handles.time_series_store
    market_db = session.handles.market_db
    client = _FixturePageClient(name)
    inserted = 0
    row_mutations = 0
    affected: set[str] = set()
    recomputed_codes = 0
    all_code_invocations = 0
    replaced_rows = 0
    seed: list[dict[str, object]] = []
    target_codes: list[str] = []
    rows_per_code = 0
    try:
        if name == "provider_noop":
            seed = _stock_rows(1, _codes(1))
            store.publish_stock_data(seed, provider_plan="standard")
            store.index_stock_data()
        elif name == "provider_fundamentals_only":
            target_codes = _codes(_required_non_negative_int(payload, "affectedCodes"))
            seed = _stock_rows(len(target_codes), target_codes)
            seed_result = store.publish_stock_data(seed, provider_plan="standard")
            AdjustedMetricsMaterializer(market_db).rebuild_current_basis(
                seed_result.affected_codes
            )
            store.index_stock_data()
        elif name == "provider_split_drift":
            target_codes = _codes(_required_non_negative_int(payload, "affectedCodes"))
            rows_per_code = _required_non_negative_int(payload, "rowsPerAffectedCode")
            for code in target_codes:
                window = _stock_rows(rows_per_code, [code])
                fingerprint = provider_stock_source_fingerprint(window)
                store.replace_stock_provider_window(
                    code,
                    window,
                    {"start": window[0]["date"], "end": window[-1]["date"]},
                    {
                        "provider_plan": "standard",
                        "provider_as_of": window[-1]["date"],
                        "provider_source_fingerprint": fingerprint,
                    },
                )
            store.index_stock_data()

        before_storage = _tree_size(scenario_root)
        started_wall = time.perf_counter()
        started_cpu = time.process_time()
        client.fetch_pages(pages)

        if name == "provider_noop":
            result = store.publish_stock_data(seed, provider_plan="standard")
            inserted, row_mutations, affected = _mutation_metrics(result)
        elif name == "provider_one_day":
            target_codes = _codes(_required_non_negative_int(payload, "affectedCodes"))
            rows = _stock_rows(_required_non_negative_int(payload, "newRows"), target_codes)
            result = store.publish_stock_data(rows, provider_plan="standard")
            inserted, row_mutations, affected = _mutation_metrics(result)
            rebuild = AdjustedMetricsMaterializer(market_db).rebuild_current_basis(
                result.affected_codes
            )
            recomputed_codes = rebuild.completed_codes
        elif name == "provider_fundamentals_only":
            result = store.publish_statements(_statement_rows(target_codes))
            inserted, row_mutations, affected = _mutation_metrics(result)
            rebuild = AdjustedMetricsMaterializer(market_db).rebuild_current_basis(
                result.affected_codes
            )
            recomputed_codes = rebuild.completed_codes
            row_mutations += rebuild.mutation_stats["statements"].mutated_rows
        elif name == "provider_split_drift":
            incoming = []
            for code in target_codes:
                drift = _stock_rows(1, [code])[0]
                drift["adjustment_factor"] = 0.5
                incoming.append(drift)
            drift_codes = set(store.detect_stock_provider_drift(incoming))
            for code, drift in zip(target_codes, incoming, strict=True):
                window = _stock_rows(rows_per_code, [code])
                window[0] = drift
                fingerprint = provider_stock_source_fingerprint(window)
                result = store.replace_stock_provider_window(
                    code,
                    window,
                    {"start": window[0]["date"], "end": window[-1]["date"]},
                    {
                        "provider_plan": "standard",
                        "provider_as_of": window[-1]["date"],
                        "provider_source_fingerprint": fingerprint,
                    },
                )
                inserted += result.stats.inserted
                row_mutations += result.mutated_rows
                affected.update(result.affected_codes)
                replaced_rows += len(window)
            if affected != drift_codes:
                raise RuntimeError("drift detection and provider-window replacement disagree")
            rebuild = AdjustedMetricsMaterializer(market_db).rebuild_current_basis(affected)
            recomputed_codes = rebuild.completed_codes
        else:
            all_codes = _codes(_required_non_negative_int(payload, "allCodes"))
            rows = _stock_rows(
                len(all_codes) * _required_non_negative_int(payload, "rowsPerCode"),
                all_codes,
            )
            result = store.publish_stock_data(rows, provider_plan="standard")
            inserted, row_mutations, affected = _mutation_metrics(result)
            all_code_invocations = int(len(affected) == universe_size)

        store.index_stock_data()
        store.index_statements()
        _close_scenario(session)
        wall_seconds = time.perf_counter() - started_wall
        cpu_seconds = time.process_time() - started_cpu
    except BaseException:
        if not session._handles_closed:
            _close_scenario(session)
        raise

    storage_growth = max(0, _tree_size(scenario_root) - before_storage)
    work_units = client.requests + row_mutations + replaced_rows + recomputed_codes
    return {
        "engine": engine,
        "measurementPath": "production_duckdb_parquet_store",
        "wallSeconds": round(wall_seconds, 9),
        "cpuSeconds": round(cpu_seconds, 9),
        "peakRssBytes": _peak_rss_bytes(),
        "requests": client.requests,
        "pages": pages,
        "affectedCodes": len(affected),
        "newRows": inserted,
        "rowMutations": row_mutations,
        "providerWindowRowsReplaced": replaced_rows,
        "currentBasisRecomputedCodes": recomputed_codes,
        "workUnits": work_units,
        "storageGrowthBytes": storage_growth,
        "allCodeMaterializerInvocations": all_code_invocations,
        "checksumSha256": _tree_checksum(scenario_root),
    }


def run_benchmark_fixture(
    fixture: dict[str, object],
    *,
    evidence_source: str = "production_fixture",
    representative_evidence_reason: str | None = None,
    workspace: Path,
) -> dict[str, Any]:
    if fixture.get("fixtureVersion") != 1:
        raise ValueError("fixtureVersion must be exactly 1")
    scenarios = fixture.get("scenarios")
    if not isinstance(scenarios, dict) or set(scenarios) != set(_SCENARIO_NAMES):
        raise ValueError("fixture must define the exact benchmark scenario set")
    legacy_payload = scenarios["legacy_all_code_local_projection"]
    if not isinstance(legacy_payload, dict):
        raise ValueError("legacy baseline must be an object")
    universe_size = _required_non_negative_int(legacy_payload, "allCodes")
    workspace.mkdir(parents=True, exist_ok=True)
    measured: dict[str, dict[str, Any]] = {}
    for name in _SCENARIO_NAMES:
        payload = scenarios[name]
        if not isinstance(payload, dict):
            raise ValueError(f"{name} must be an object")
        measured[name] = _run_scenario(
            name, payload, workspace, universe_size=universe_size
        )

    normal = measured["provider_one_day"]
    drift = measured["provider_split_drift"]
    legacy = measured["legacy_all_code_local_projection"]
    assertions = {
        "legacyBaselineInvokesAllCodeMaterializer": (
            legacy["allCodeMaterializerInvocations"] == 1
        ),
        "normalIncrementalUsesNoAllCodeMaterializer": (
            normal["allCodeMaterializerInvocations"] == 0
        ),
        "normalIncrementalWorkBelowLegacyBaseline": (
            normal["workUnits"] < legacy["workUnits"]
        ),
        "splitDriftRefreshLimitedToAffectedCodes": (
            drift["affectedCodes"] < legacy["affectedCodes"]
            and drift["allCodeMaterializerInvocations"] == 0
        ),
    }
    return {
        "schemaVersion": 1,
        "benchmark": "market_v5_incremental_sync",
        "evidenceSource": evidence_source,
        "representativeEvidence": "unavailable",
        "representativeEvidenceReason": representative_evidence_reason,
        "providerPlan": fixture.get("providerPlan"),
        "measurementNotes": {
            "resources": "wall/cpu/peak RSS observed around production store operations",
            "requests": "calls made through the fixture page client; no network access",
            "storage": "isolated Market DuckDB + Parquet byte growth",
            "scaling": "production mutation results, affected codes, and bounded recompute calls",
            "legacyBaseline": "production publish path invoked with the full modeled universe",
        },
        "scenarios": measured,
        "comparison": {
            "normalToLegacyWorkRatio": normal["workUnits"] / legacy["workUnits"],
            "normalToLegacyRowMutationRatio": (
                normal["rowMutations"] / legacy["rowMutations"]
            ),
            "normalAffectedCodeDelta": (
                legacy["affectedCodes"] - normal["affectedCodes"]
            ),
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
    args = parser.parse_args()
    fixture = json.loads(args.fixture.read_text(encoding="utf-8"))
    if not isinstance(fixture, dict):
        raise ValueError("benchmark fixture must be an object")
    report = run_benchmark_fixture(
        fixture,
        representative_evidence_reason=args.representative_evidence_reason,
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
