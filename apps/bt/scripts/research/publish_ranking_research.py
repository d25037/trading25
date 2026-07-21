#!/usr/bin/env python3
"""Create and verify immutable Daily Ranking research publications."""

from __future__ import annotations

import argparse
from datetime import date, datetime
from decimal import Decimal
import hashlib
import json
import math
from pathlib import Path
import re
import tomllib
from typing import TypedDict, cast

import duckdb


SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
REPO_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_REGISTRY = (
    REPO_ROOT / "apps/bt/tests/fixtures/research/ranking_publication_registry.json"
)
DEFAULT_CATALOG = REPO_ROOT / "apps/bt/docs/experiments/research-catalog-metadata.toml"


class PublicationPolicy(TypedDict):
    decision_sql: str
    metric_sql: dict[str, str]
    selection_policy: str
    selection_keys: tuple[str, ...]
    pit_kind: str


POLICIES: dict[str, PublicationPolicy] = {
    "market-behavior/ranking-trend-acceleration-conditional-lift": {
        "decision_sql": (
            "SELECT recommendation FROM decision_gate_df "
            "WHERE gate = 'final_decision' AND passed"
        ),
        "metric_sql": {
            "binary_gate_pass_count": (
                "SELECT count(*) FROM decision_gate_df "
                "WHERE recommendation = 'add_binary_badge_only' AND passed"
            ),
            "binary_gate_total": (
                "SELECT count(*) FROM decision_gate_df "
                "WHERE recommendation = 'add_binary_badge_only'"
            ),
            "continuous_gate_pass_count": (
                "SELECT count(*) FROM decision_gate_df "
                "WHERE recommendation = 'add_continuous_columns' AND passed"
            ),
            "continuous_gate_total": (
                "SELECT count(*) FROM decision_gate_df "
                "WHERE recommendation = 'add_continuous_columns'"
            ),
            "observation_count": (
                "SELECT sum(observation_count) FROM coverage_diagnostics_df"
            ),
            "topk_complete_row_count": (
                "SELECT count(*) FROM topk_priority_lift_df "
                "WHERE outcome_status = 'complete'"
            ),
            "topk_incomplete_row_count": (
                "SELECT count(*) FROM topk_priority_lift_df "
                "WHERE outcome_status <> 'complete'"
            ),
        },
        "selection_policy": "trend_candidate_group_membership_before_outcomes_v1",
        "selection_keys": ("date", "code", "candidate_group"),
        "pit_kind": "price_projection",
    },
    "market-behavior/ranking-fixed-return-priority-evidence": {
        "decision_sql": (
            "SELECT reason FROM decision_gate "
            "WHERE decision_key = 'final_recommendation'"
        ),
        "metric_sql": {
            "observation_count": "SELECT sum(observation_count) FROM coverage_attrition",
            "strict_value_observation_count": (
                "SELECT observation_count FROM coverage_attrition "
                "WHERE scaffold_family = 'strict_value_long_only'"
            ),
            "topk_complete_row_count": (
                "SELECT count(*) FROM topk_priority_lift "
                "WHERE outcome_status = 'complete'"
            ),
            "topk_incomplete_row_count": (
                "SELECT count(*) FROM topk_priority_lift "
                "WHERE outcome_status <> 'complete'"
            ),
            "value_extension_observation_count": (
                "SELECT observation_count FROM coverage_attrition "
                "WHERE scaffold_family = 'value_extension_long_only'"
            ),
        },
        "selection_policy": (
            "fixed_return_free_scaffold_membership_before_outcomes_v1"
        ),
        "selection_keys": ("date", "code", "scaffold_family"),
        "pit_kind": "price_projection",
    },
    "market-behavior/ranking-technical-fit-score-shape-evidence": {
        "decision_sql": (
            "SELECT decision FROM decision_gate "
            "WHERE decision_key = 'fixed_vs_ols'"
        ),
        "metric_sql": {
            "fixed_core_oos_mean_lift_pct": (
                "SELECT avg(mean_lift_pct) FROM oos_fit_score_lift "
                "WHERE is_primary AND horizon = 20 AND family = 'fixed' "
                "AND ring = 'core_high_high'"
            ),
            "fixed_top5_mean_lift_pct": (
                "SELECT avg(topk_lift_pct) FROM topk_operational_lift "
                "WHERE horizon = 20 AND family = 'fixed' AND k = 5"
            ),
            "near1_fixed_minus_ols_mean_lift_pct": (
                "SELECT avg(fixed_minus_ols_lift_pct) "
                "FROM fixed_vs_ols_paired WHERE horizon = 20 "
                "AND ring = 'near_high_high_1'"
            ),
            "observation_count": "SELECT sum(observation_count) FROM coverage_attrition",
            "ols_core_oos_mean_lift_pct": (
                "SELECT avg(mean_lift_pct) FROM oos_fit_score_lift "
                "WHERE is_primary AND horizon = 20 AND family = 'ols' "
                "AND ring = 'core_high_high'"
            ),
            "ols_top5_mean_lift_pct": (
                "SELECT avg(topk_lift_pct) FROM topk_operational_lift "
                "WHERE horizon = 20 AND family = 'ols' AND k = 5"
            ),
            "topk_complete_row_count": (
                "SELECT count(*) FROM topk_operational_lift "
                "WHERE outcome_status = 'complete'"
            ),
            "topk_incomplete_row_count": (
                "SELECT count(*) FROM topk_operational_lift "
                "WHERE outcome_status <> 'complete'"
            ),
        },
        "selection_policy": "technical_fit_ring_membership_before_outcomes_v1",
        "selection_keys": ("date", "code", "ring"),
        "pit_kind": "pit_lineage",
    },
}


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sha256_file(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


def _json_bytes(value: object) -> bytes:
    return (
        json.dumps(value, allow_nan=False, ensure_ascii=False, indent=2, sort_keys=True)
        + "\n"
    ).encode()


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _load_json_object(path: Path, *, label: str) -> dict[str, object]:
    try:
        value = json.loads(
            path.read_text(encoding="utf-8"), object_pairs_hook=_reject_duplicate_keys
        )
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"malformed or missing {label}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be an object")
    return value


def _canonical_value(value: object) -> object:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("table values must be finite")
        return {"float64": value.hex()}
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("table values must be finite")
        return {"decimal": str(value)}
    if isinstance(value, (date, datetime)):
        return {"temporal": value.isoformat()}
    if isinstance(value, bytes):
        return {"bytes": value.hex()}
    if isinstance(value, (list, tuple)):
        return [_canonical_value(item) for item in value]
    if isinstance(value, dict):
        return {
            str(key): _canonical_value(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    raise ValueError(f"unsupported result value: {type(value).__name__}")


def _validate_hash_fields(value: object, path: str = "manifest") -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            child = f"{path}.{key}"
            if (key.endswith("_sha256") or key.endswith("Sha256")) and (
                not isinstance(item, str) or SHA256_RE.fullmatch(item) is None
            ):
                raise ValueError(f"malformed SHA-256 at {child}")
            _validate_hash_fields(item, child)
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _validate_hash_fields(item, f"{path}[{index}]")


def _collect_manifest_audit_fields(value: object) -> dict[str, object]:
    """Retain every manifest hash/count/audit/PIT field in the digest."""

    if not isinstance(value, dict):
        return {}
    result: dict[str, object] = {}
    for key, item in sorted(value.items()):
        if isinstance(item, dict):
            nested = _collect_manifest_audit_fields(item)
            if nested:
                result[key] = nested
            continue
        selected = any(
            token in key.lower()
            for token in ("sha256", "hash", "count", "audit", "projection", "pit")
        )
        if selected:
            result[key] = _canonical_value(item)
    return result


_REQUIRED_PROJECTION_AUDIT_HASHES = (
    "price_projection_sha256",
    "signal_basis_sha256",
    "signal_segment_sha256",
    "completion_basis_sha256",
    "completion_segment_sha256",
    "forward_outcome_sha256",
    "next_open_outcome_sha256",
)
_REQUIRED_PROJECTION_AUDIT_COUNTS = (
    "canonical_raw_row_count",
    "signal_feature_row_count",
    "outcome_request_row_count",
    "completed_outcome_row_count",
    "signal_basis_row_count",
    "signal_segment_row_count",
    "completion_basis_row_count",
    "completion_segment_row_count",
)
_REQUIRED_TECHNICAL_AUDIT_COUNTS = (
    "basis_id_count",
    "consumed_daily_valuation_row_count",
    "verified_basis_row_count",
    "verified_segment_row_count",
)


def _manifest_audit(
    metadata: object, policy: PublicationPolicy
) -> dict[str, object]:
    """Validate and collect the detailed runner-produced audit evidence."""

    if not isinstance(metadata, dict):
        raise ValueError("manifest audit metadata missing")
    lineage: dict[str, object] | None = None
    if policy["pit_kind"] == "pit_lineage":
        raw_lineage = metadata.get("pit_lineage")
        if not isinstance(raw_lineage, dict):
            raise ValueError("manifest audit technical lineage missing")
        lineage = raw_lineage
        raw_projection = lineage.get("price_projection")
    else:
        raw_projection = metadata.get("price_projection")
    if not isinstance(raw_projection, dict):
        raise ValueError("manifest audit price projection missing")
    for field in _REQUIRED_PROJECTION_AUDIT_HASHES:
        value = raw_projection.get(field)
        if not isinstance(value, str) or SHA256_RE.fullmatch(value) is None:
            raise ValueError(f"manifest audit hash missing or malformed: {field}")
    for field in _REQUIRED_PROJECTION_AUDIT_COUNTS:
        value = raw_projection.get(field)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise ValueError(f"manifest audit count missing or malformed: {field}")
    if lineage is not None:
        basis_hash = lineage.get("basis_id_sha256")
        if not isinstance(basis_hash, str) or SHA256_RE.fullmatch(basis_hash) is None:
            raise ValueError("manifest audit hash missing or malformed: basis_id_sha256")
        for field in _REQUIRED_TECHNICAL_AUDIT_COUNTS:
            value = lineage.get(field)
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise ValueError(f"manifest audit count missing or malformed: {field}")
    audit = _collect_manifest_audit_fields(metadata)
    if not audit:
        raise ValueError("manifest audit is empty")
    return audit


def _table_digest(conn: duckdb.DuckDBPyConnection, table: str) -> dict[str, object]:
    escaped = table.replace('"', '""')
    description = conn.execute(f'DESCRIBE SELECT * FROM "{escaped}"').fetchall()
    schema = [{"name": str(row[0]), "type": str(row[1])} for row in description]
    encoded = [
        json.dumps(
            [_canonical_value(value) for value in row],
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        for row in conn.execute(f'SELECT * FROM "{escaped}"').fetchall()
    ]
    encoded.sort()
    return {
        "name": table,
        "schema": schema,
        "row_count": len(encoded),
        "content_sha256": _sha256_bytes("\n".join(encoded).encode()),
    }


def _single_value(conn: duckdb.DuckDBPyConnection, sql: str, *, label: str) -> object:
    rows = conn.execute(sql).fetchall()
    if len(rows) != 1 or len(rows[0]) != 1 or rows[0][0] is None:
        raise ValueError(f"missing or ambiguous {label} row")
    _canonical_value(rows[0][0])
    return rows[0][0]


def _required_price_projection(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError("missing price projection PIT contract")
    expected = {
        "physical_price_source": "stock_data_raw",
        "verification_status": "verified",
        "no_stock_data_fallback": True,
        "signal_basis_policy": "exact_signal_date_basis_across_full_lookback",
        "completion_basis_policy": (
            "exact_completion_date_basis_applied_to_signal_and_completion_endpoints"
        ),
        "next_open_integrity_policy": (
            "exact_stock_entry_session_and_topix_entry_endpoint_no_backfill"
        ),
    }
    for key, expected_value in expected.items():
        if value.get(key) != expected_value:
            raise ValueError(f"price projection PIT contract mismatch: {key}")
    return {key: value[key] for key in expected}


def _pit_contract(metadata: object, policy: PublicationPolicy) -> dict[str, object]:
    if not isinstance(metadata, dict):
        raise ValueError("missing result metadata")
    if policy["pit_kind"] == "price_projection":
        return {"price_projection": _required_price_projection(metadata.get("price_projection"))}
    lineage = metadata.get("pit_lineage")
    if not isinstance(lineage, dict):
        raise ValueError("missing technical PIT lineage")
    expected = {
        "data_plane": "physical_market.duckdb_schema_v4",
        "stock_price_adjustment_mode": "local_projection_v2_event_time",
        "universe_source": "stock_master_daily",
        "as_of_policy": "exact_signal_date_no_latest_fallback",
        "verification_status": "verified",
        "no_service_local_recomputation": True,
        "no_basis_fallback": True,
    }
    for key, expected_value in expected.items():
        if lineage.get(key) != expected_value:
            raise ValueError(f"technical PIT contract mismatch: {key}")
    sources = lineage.get("basis_dependent_sources")
    if not isinstance(sources, list) or set(sources) != {"daily_valuation", "stock_data_raw"}:
        raise ValueError("technical PIT basis sources mismatch")
    return {
        **expected,
        "basis_dependent_sources": sorted(cast(list[str], sources)),
        "price_projection": _required_price_projection(lineage.get("price_projection")),
    }


def _selection_audit(metadata: object, policy: PublicationPolicy) -> dict[str, object]:
    if not isinstance(metadata, dict) or not isinstance(metadata.get("selection_audit"), dict):
        raise ValueError("missing full selection audit")
    audit = cast(dict[str, object], metadata["selection_audit"])
    if audit.get("policy") != policy["selection_policy"]:
        raise ValueError("selection audit policy mismatch")
    if audit.get("key_columns") != list(policy["selection_keys"]):
        raise ValueError("selection audit key columns mismatch")
    row_count = audit.get("row_count")
    if not isinstance(row_count, int) or isinstance(row_count, bool) or row_count <= 0:
        raise ValueError("selection audit row count must be positive")
    if metadata.get("observation_count") != row_count:
        raise ValueError("selection audit does not cover every observation")
    sha256 = audit.get("sha256")
    if not isinstance(sha256, str) or SHA256_RE.fullmatch(sha256) is None:
        raise ValueError("selection audit SHA-256 malformed")
    return {
        "policy": audit["policy"],
        "key_columns": audit["key_columns"],
        "row_count": row_count,
        "sha256": sha256,
    }


def build_publication_digest(
    bundle_dir: str | Path, source_commit: str, dirty: bool
) -> dict[str, object]:
    """Build a deterministic semantic digest without writing files."""

    bundle = Path(bundle_dir)
    if dirty:
        raise ValueError("dirty source cannot be published")
    if COMMIT_RE.fullmatch(source_commit) is None:
        raise ValueError("source commit must be a full lowercase git SHA")
    paths = {name: bundle / name for name in ("manifest.json", "results.duckdb", "summary.md")}
    missing = [name for name, path in paths.items() if not path.is_file()]
    if missing:
        raise ValueError(f"missing bundle artifacts: {', '.join(missing)}")
    manifest = _load_json_object(paths["manifest.json"], label="manifest")
    _validate_hash_fields(manifest)
    if manifest.get("git_dirty") is not False:
        raise ValueError("dirty bundle cannot be published")
    if manifest.get("git_commit") != source_commit:
        raise ValueError("source commit mismatch")
    experiment_id = manifest.get("experiment_id")
    run_id = manifest.get("run_id")
    if not isinstance(experiment_id, str) or experiment_id not in POLICIES:
        raise ValueError("unsupported experiment policy")
    if not isinstance(run_id, str) or not run_id:
        raise ValueError("missing run ID")
    expected_tables = manifest.get("output_tables")
    if (
        not isinstance(expected_tables, list)
        or not expected_tables
        or not all(isinstance(item, str) and item for item in expected_tables)
        or len(expected_tables) != len(set(expected_tables))
    ):
        raise ValueError("invalid output tables in manifest")
    policy = POLICIES[experiment_id]
    metadata = manifest.get("result_metadata")
    selection_audit = _selection_audit(metadata, policy)
    pit_contract = _pit_contract(metadata, policy)
    manifest_audit = _manifest_audit(metadata, policy)

    conn = duckdb.connect(str(paths["results.duckdb"]), read_only=True)
    try:
        actual_tables = sorted(
            row[0]
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main' AND table_type = 'BASE TABLE'"
            ).fetchall()
        )
        if actual_tables != sorted(expected_tables):
            raise ValueError("output tables mismatch")
        tables = [_table_digest(conn, table) for table in actual_tables]
        decision = _single_value(conn, policy["decision_sql"], label="decision")
        if not isinstance(decision, str) or not decision:
            raise ValueError("invalid decision")
        metrics = {
            name: _single_value(conn, sql, label=f"metric {name}")
            for name, sql in sorted(policy["metric_sql"].items())
        }
        if any(cast(int, table["row_count"]) == 0 for table in tables):
            raise ValueError("output tables must be non-empty")
    finally:
        conn.close()

    return {
        "schema_version": 3,
        "experiment_id": experiment_id,
        "published_run_id": run_id,
        "decision": decision,
        "source": {"git_commit": source_commit, "git_dirty": False},
        "database_fingerprint": _canonical_value(manifest.get("db_fingerprint")),
        "artifacts": {
            "manifest_sha256": _sha256_file(paths["manifest.json"]),
            "results_file_sha256": _sha256_file(paths["results.duckdb"]),
            "results_semantic_sha256": _sha256_bytes(_json_bytes(tables)),
            "summary_sha256": _sha256_file(paths["summary.md"]),
        },
        "tables": tables,
        "pit_contract": pit_contract,
        "manifest_audit": manifest_audit,
        "selection_audit": selection_audit,
        "decision_metrics": metrics,
    }


def _display_value(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, (float, Decimal)):
        numeric = float(value)
        if not math.isfinite(numeric):
            raise ValueError("README metric must be finite")
        return f"{numeric:.10f}"
    return str(value)


def _markdown_table(text: str, heading: str) -> dict[str, str]:
    marker = f"## {heading}"
    if text.count(marker) != 1:
        raise ValueError(f"README requires exactly one {heading} section")
    section = text.split(marker, 1)[1].split("\n## ", 1)[0]
    rows: dict[str, str] = {}
    for line in section.splitlines():
        if not line.startswith("|"):
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) != 2 or cells[0] in {"Field", "Metric"} or set(cells[0]) == {"-"}:
            continue
        key, value = cells
        if key in rows:
            raise ValueError(f"duplicate README table key: {key}")
        if len(value) < 2 or not value.startswith("`") or not value.endswith("`"):
            raise ValueError(f"README table value must be code formatted: {key}")
        rows[key] = value[1:-1]
    return rows


def _readme_identity(readme_path: Path, digest: dict[str, object]) -> dict[str, object]:
    try:
        text = readme_path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise ValueError("malformed or missing README") from exc
    source = cast(dict[str, object], digest["source"])
    expected_identity = {
        "experiment_id": str(digest["experiment_id"]),
        "run_id": str(digest["published_run_id"]),
        "decision": str(digest["decision"]),
        "source_commit": str(source["git_commit"]),
        "git_dirty": "false",
    }
    identity = _markdown_table(text, "Publication Identity")
    if identity != expected_identity:
        raise ValueError("README publication identity mismatch")
    expected_metrics = {
        key: _display_value(value)
        for key, value in cast(dict[str, object], digest["decision_metrics"]).items()
    }
    if _markdown_table(text, "Published Metrics") != expected_metrics:
        raise ValueError("README published metrics mismatch")
    return {
        "path": f"apps/bt/docs/experiments/{digest['experiment_id']}/README.md",
        "sha256": _sha256_file(readme_path),
        **expected_identity,
    }


def _load_registry(path: Path) -> dict[str, object]:
    return _load_json_object(path, label="publication registry")


def _load_catalog(path: Path) -> dict[str, object]:
    try:
        value = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise ValueError("malformed or missing publication catalog") from exc
    experiments = value.get("experiments")
    if not isinstance(experiments, dict):
        raise ValueError("publication catalog lacks experiments")
    return cast(dict[str, object], experiments)


def _resolve(path: object, *, repo_root: Path) -> Path:
    if not isinstance(path, str) or not path:
        raise ValueError("publication path missing")
    candidate = Path(path).expanduser()
    return (candidate if candidate.is_absolute() else repo_root / candidate).resolve()


def _validate_publication_identity(
    *,
    digest: dict[str, object],
    bundle_dir: Path,
    digest_path: Path,
    readme_path: Path,
    registry_path: Path,
    catalog_path: Path,
    repo_root: Path,
) -> dict[str, object]:
    experiment_id = str(digest["experiment_id"])
    run_id = str(digest["published_run_id"])
    registry = _load_registry(registry_path)
    catalog = _load_catalog(catalog_path)
    entry = registry.get(experiment_id)
    mirror = catalog.get(experiment_id)
    if not isinstance(entry, dict) or not isinstance(mirror, dict):
        raise ValueError("registry/catalog identity missing")
    required = {
        "canonicalRunId",
        "canonicalDecision",
        "sourceCommit",
        "bundlePath",
        "digestPath",
        "readmePath",
        "artifactHashes",
        "supersededRunIds",
    }
    if not required.issubset(entry) or not required.issubset(mirror):
        raise ValueError("registry/catalog publication fields incomplete")
    for field in required:
        if entry[field] != mirror[field]:
            raise ValueError(f"registry/catalog mismatch: {field}")
    source = cast(dict[str, object], digest["source"])
    expected_scalars = {
        "canonicalRunId": run_id,
        "canonicalDecision": digest["decision"],
        "sourceCommit": source["git_commit"],
    }
    for field, value in expected_scalars.items():
        if entry.get(field) != value:
            raise ValueError(f"registry {field} mismatch")
    if _resolve(entry["bundlePath"], repo_root=repo_root) != bundle_dir.resolve():
        raise ValueError("registry bundle path mismatch")
    if _resolve(entry["digestPath"], repo_root=repo_root) != digest_path.resolve():
        raise ValueError("registry digest path mismatch")
    if _resolve(entry["readmePath"], repo_root=repo_root) != readme_path.resolve():
        raise ValueError("registry README path mismatch")
    artifacts = cast(dict[str, object], digest["artifacts"])
    expected_hashes = {
        "manifest": artifacts["manifest_sha256"],
        "results": artifacts["results_file_sha256"],
        "summary": artifacts["summary_sha256"],
    }
    if entry.get("artifactHashes") != expected_hashes:
        raise ValueError("registry artifact hashes mismatch")
    superseded = entry.get("supersededRunIds")
    if (
        not isinstance(superseded, list)
        or not all(isinstance(item, str) and item for item in superseded)
        or len(superseded) != len(set(superseded))
        or run_id in superseded
    ):
        raise ValueError("invalid superseded publication identity")
    seen_identities: dict[str, str] = {}
    for other_id, raw in registry.items():
        if not isinstance(raw, dict):
            raise ValueError(f"malformed registry entry: {other_id}")
        raw_superseded = raw.get("supersededRunIds")
        if not isinstance(raw_superseded, list):
            raise ValueError(f"malformed superseded identities: {other_id}")
        identities = [raw.get("canonicalRunId"), *raw_superseded]
        for identity in identities:
            if not isinstance(identity, str) or not identity:
                raise ValueError(f"malformed publication identity: {other_id}")
            owner = seen_identities.get(identity)
            if owner is not None:
                raise ValueError(
                    "publication identity is not globally unique: "
                    f"{identity} ({owner}, {other_id})"
                )
            seen_identities[identity] = str(other_id)
    return entry


def publish(
    bundle_dir: str | Path,
    digest_path: str | Path,
    readme_path: str | Path,
    registry_path: str | Path,
    source_commit: str,
    catalog_path: str | Path = DEFAULT_CATALOG,
    repo_root: str | Path = REPO_ROOT,
) -> dict[str, object]:
    """Create one canonical digest with exclusive-create semantics."""

    output = Path(digest_path)
    if output.exists():
        raise FileExistsError(f"publication digest already exists: {output}")
    digest = build_publication_digest(bundle_dir, source_commit, False)
    digest["readme"] = _readme_identity(Path(readme_path), digest)
    _validate_publication_identity(
        digest=digest,
        bundle_dir=Path(bundle_dir),
        digest_path=output,
        readme_path=Path(readme_path),
        registry_path=Path(registry_path),
        catalog_path=Path(catalog_path),
        repo_root=Path(repo_root),
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("xb") as handle:
        handle.write(_json_bytes(digest))
    return digest


def verify_publication(
    bundle_dir: str | Path,
    digest_path: str | Path,
    readme_path: str | Path,
    registry_path: str | Path,
    catalog_path: str | Path = DEFAULT_CATALOG,
    repo_root: str | Path = REPO_ROOT,
) -> dict[str, object]:
    """Read-only byte-exact verification of one registered publication."""

    path = Path(digest_path)
    raw = path.read_bytes()
    committed = _load_json_object(path, label="digest")
    if raw != _json_bytes(committed):
        raise ValueError("publication digest is not canonical JSON bytes")
    if committed.get("schema_version") != 3:
        raise ValueError("unsupported digest schema")
    source = committed.get("source")
    if not isinstance(source, dict) or source.get("git_dirty") is not False:
        raise ValueError("invalid digest source")
    source_commit = source.get("git_commit")
    if not isinstance(source_commit, str):
        raise ValueError("invalid digest source commit")
    rebuilt = build_publication_digest(bundle_dir, source_commit, False)
    rebuilt["readme"] = _readme_identity(Path(readme_path), rebuilt)
    _validate_publication_identity(
        digest=rebuilt,
        bundle_dir=Path(bundle_dir),
        digest_path=path,
        readme_path=Path(readme_path),
        registry_path=Path(registry_path),
        catalog_path=Path(catalog_path),
        repo_root=Path(repo_root),
    )
    if raw != _json_bytes(rebuilt):
        raise ValueError("publication digest verification failed")
    return rebuilt


def verify_registered_publications(
    research_root: str | Path,
    registry_path: str | Path = DEFAULT_REGISTRY,
    catalog_path: str | Path = DEFAULT_CATALOG,
    repo_root: str | Path = REPO_ROOT,
) -> list[str]:
    """Verify every registered publication without mutating any artifact."""

    registry_file = Path(registry_path)
    registry = _load_registry(registry_file)
    repository = Path(repo_root)
    verified: list[str] = []
    for experiment_id, raw_entry in sorted(registry.items()):
        if not isinstance(raw_entry, dict):
            raise ValueError(f"malformed registry entry: {experiment_id}")
        bundle_path = _resolve(raw_entry.get("bundlePath"), repo_root=repository)
        expected_bundle = (
            Path(research_root).expanduser()
            / experiment_id
            / str(raw_entry.get("canonicalRunId"))
        ).resolve()
        if bundle_path != expected_bundle:
            raise ValueError(f"registered bundle root mismatch: {experiment_id}")
        verify_publication(
            bundle_path,
            _resolve(raw_entry.get("digestPath"), repo_root=repository),
            _resolve(raw_entry.get("readmePath"), repo_root=repository),
            registry_file,
            catalog_path,
            repository,
        )
        verified.append(experiment_id)
    return verified


def _cli() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="mode", required=True)
    publish_parser = subparsers.add_parser("publish")
    publish_parser.add_argument("--research-root", type=Path, required=True)
    publish_parser.add_argument("--experiment-id", required=True)
    publish_parser.add_argument("--run-id", required=True)
    publish_parser.add_argument("--digest-path", type=Path, required=True)
    publish_parser.add_argument("--readme-path", type=Path, required=True)
    publish_parser.add_argument("--registry-path", type=Path, default=DEFAULT_REGISTRY)
    publish_parser.add_argument("--catalog-path", type=Path, default=DEFAULT_CATALOG)
    publish_parser.add_argument("--source-commit", required=True)
    verify_parser = subparsers.add_parser("verify")
    verify_parser.add_argument("--research-root", type=Path, required=True)
    verify_parser.add_argument("--registry-path", type=Path, default=DEFAULT_REGISTRY)
    verify_parser.add_argument("--catalog-path", type=Path, default=DEFAULT_CATALOG)
    verify_parser.add_argument("--experiment-id")
    verify_parser.add_argument("--run-id")
    verify_parser.add_argument("--digest-path", type=Path)
    verify_parser.add_argument("--readme-path", type=Path)
    args = parser.parse_args()
    if args.mode == "publish":
        publish(
            args.research_root / args.experiment_id / args.run_id,
            args.digest_path,
            args.readme_path,
            args.registry_path,
            args.source_commit,
            args.catalog_path,
        )
    elif all(
        value is not None
        for value in (args.experiment_id, args.run_id, args.digest_path, args.readme_path)
    ):
        verify_publication(
            args.research_root / args.experiment_id / args.run_id,
            args.digest_path,
            args.readme_path,
            args.registry_path,
            args.catalog_path,
        )
    elif any(
        value is not None
        for value in (args.experiment_id, args.run_id, args.digest_path, args.readme_path)
    ):
        parser.error("explicit verify requires experiment, run, digest, and README")
    else:
        verify_registered_publications(
            args.research_root, args.registry_path, args.catalog_path
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli())
