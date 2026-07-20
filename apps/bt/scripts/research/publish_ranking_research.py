#!/usr/bin/env python3
"""Create and verify immutable Daily Ranking research publication digests."""

from __future__ import annotations

import argparse
from datetime import date, datetime
from decimal import Decimal
import hashlib
import json
import math
from pathlib import Path
import re
from typing import TypedDict, cast
import duckdb


SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")

class PublicationPolicy(TypedDict):
    decision_sql: str
    metric_sql: dict[str, str]


POLICIES: dict[str, PublicationPolicy] = {
    "market-behavior/ranking-trend-acceleration-conditional-lift": {
        "decision_sql": (
            "SELECT recommendation FROM decision_gate_df "
            "WHERE gate = 'final_decision' AND passed"
        ),
        "metric_sql": {
            "decision_gate_row_count": "SELECT count(*) FROM decision_gate_df",
            "passed_gate_count": (
                "SELECT count(*) FROM decision_gate_df WHERE passed"
            ),
        },
    },
    "market-behavior/ranking-fixed-return-priority-evidence": {
        "decision_sql": (
            "SELECT reason FROM decision_gate "
            "WHERE decision_key = 'final_recommendation'"
        ),
        "metric_sql": {
            "decision_gate_row_count": "SELECT count(*) FROM decision_gate",
            "passed_gate_count": "SELECT count(*) FROM decision_gate WHERE passed",
        },
    },
    "market-behavior/ranking-technical-fit-score-shape-evidence": {
        "decision_sql": (
            "SELECT decision FROM decision_gate "
            "WHERE decision_key = 'fixed_vs_ols'"
        ),
        "metric_sql": {
            "decision_gate_row_count": "SELECT count(*) FROM decision_gate",
            "passed_gate_count": "SELECT count(*) FROM decision_gate WHERE passed",
        },
    },
}


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sha256_file(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


def _json_bytes(value: object) -> bytes:
    return (
        json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode()


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


def _collect_audit_fields(value: object) -> object:
    if not isinstance(value, dict):
        return {}
    result: dict[str, object] = {}
    for key, item in sorted(value.items()):
        selected = any(
            token in key.lower()
            for token in ("sha256", "hash", "count", "audit", "projection", "pit")
        )
        if isinstance(item, dict):
            nested = _collect_audit_fields(item)
            if nested:
                result[key] = nested
        elif selected:
            result[key] = _canonical_value(item)
    return result


def _table_digest(conn: duckdb.DuckDBPyConnection, table: str) -> dict[str, object]:
    escaped = table.replace('"', '""')
    description = conn.execute(f'DESCRIBE SELECT * FROM "{escaped}"').fetchall()
    schema = [{"name": str(row[0]), "type": str(row[1])} for row in description]
    rows = conn.execute(f'SELECT * FROM "{escaped}"').fetchall()
    encoded = [
        json.dumps(
            [_canonical_value(value) for value in row],
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        for row in rows
    ]
    encoded.sort()
    content = "\n".join(encoded).encode()
    return {
        "name": table,
        "schema": schema,
        "row_count": len(rows),
        "content_sha256": _sha256_bytes(content),
    }


def _single_value(
    conn: duckdb.DuckDBPyConnection, sql: str, *, label: str
) -> object:
    rows = conn.execute(sql).fetchall()
    if len(rows) != 1 or len(rows[0]) != 1 or rows[0][0] is None:
        raise ValueError(f"missing or ambiguous {label} row")
    return _canonical_value(rows[0][0])


def build_publication_digest(
    bundle_dir: str | Path,
    source_commit: str,
    dirty: bool,
) -> dict[str, object]:
    """Build a deterministic semantic digest without writing any files."""
    bundle = Path(bundle_dir)
    if dirty:
        raise ValueError("dirty source cannot be published")
    if COMMIT_RE.fullmatch(source_commit) is None:
        raise ValueError("source commit must be a full lowercase git SHA")
    paths = {
        name: bundle / name
        for name in ("manifest.json", "results.duckdb", "summary.md")
    }
    missing = [name for name, path in paths.items() if not path.is_file()]
    if missing:
        raise ValueError(f"missing bundle artifacts: {', '.join(missing)}")
    try:
        manifest = json.loads(paths["manifest.json"].read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError("malformed manifest") from exc
    if not isinstance(manifest, dict):
        raise ValueError("manifest must be an object")
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
            raise ValueError(
                "output tables mismatch: "
                f"manifest={sorted(expected_tables)!r} actual={actual_tables!r}"
            )
        tables = [_table_digest(conn, table) for table in actual_tables]
        policy = POLICIES[experiment_id]
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

    results_sha256 = _sha256_bytes(_json_bytes(tables))
    selection_cohort = {
        str(table["name"]): {
            "row_count": table["row_count"],
            "schema_sha256": _sha256_bytes(_json_bytes(table["schema"])),
            "content_sha256": table["content_sha256"],
        }
        for table in tables
        if any(
            token in str(table["name"]).lower()
            for token in ("selection", "cohort", "observation", "ring", "scaffold", "candidate")
        )
    }
    metadata = manifest.get("result_metadata")
    projection_audit = _collect_audit_fields(metadata)
    if isinstance(metadata, dict) and isinstance(metadata.get("price_projection"), dict):
        projection_audit = {
            **cast(dict[str, object], projection_audit),
            **cast(
                dict[str, object],
                _collect_audit_fields(metadata["price_projection"]),
            ),
        }
    return {
        "schema_version": 2,
        "experiment_id": experiment_id,
        "published_run_id": run_id,
        "decision": decision,
        "source": {"git_commit": source_commit, "git_dirty": False},
        "database_fingerprint": _canonical_value(manifest.get("db_fingerprint")),
        "artifacts": {
            "manifest_sha256": _sha256_file(paths["manifest.json"]),
            "results_sha256": results_sha256,
            "summary_sha256": _sha256_file(paths["summary.md"]),
        },
        "tables": tables,
        "projection_audit": projection_audit,
        "selection_cohort": selection_cohort,
        "decision_metrics": metrics,
    }


def _load_registry(path: Path) -> dict[str, object]:
    try:
        registry = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("malformed or missing publication registry") from exc
    if not isinstance(registry, dict):
        raise ValueError("publication registry must be an object")
    return registry


def _registry_entry(
    registry_path: Path, experiment_id: str, run_id: str, decision: str
) -> dict[str, object]:
    registry = _load_registry(registry_path)
    entry = registry.get(experiment_id)
    if not isinstance(entry, dict):
        raise ValueError("registry identity missing")
    if entry.get("canonicalRunId") != run_id:
        raise ValueError("registry run identity mismatch")
    if entry.get("canonicalDecision") != decision:
        raise ValueError("registry decision identity mismatch")
    source = entry.get("sourceCommit")
    if not isinstance(source, str) or COMMIT_RE.fullmatch(source) is None:
        raise ValueError("registry source commit missing or malformed")
    return entry


def _readme_identity(readme_path: Path, digest: dict[str, object]) -> dict[str, object]:
    try:
        text = readme_path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise ValueError("malformed or missing README") from exc
    if "## Published Readout" not in text:
        raise ValueError("README lacks Published Readout")
    for field in ("published_run_id", "decision"):
        if str(digest[field]) not in text:
            raise ValueError(f"README {field} identity mismatch")
    source = cast(dict[str, object], digest["source"])
    commit = str(source["git_commit"])
    if commit not in text:
        raise ValueError("README source identity mismatch")
    return {
        "path": str(readme_path),
        "sha256": _sha256_file(readme_path),
        "experiment_id": digest["experiment_id"],
        "run_id": digest["published_run_id"],
        "decision": digest["decision"],
        "source_commit": commit,
    }


def publish(
    bundle_dir: str | Path,
    digest_path: str | Path,
    readme_path: str | Path,
    registry_path: str | Path,
    source_commit: str,
) -> dict[str, object]:
    """Create one digest with exclusive-create semantics."""
    output = Path(digest_path)
    if output.exists():
        raise FileExistsError(f"publication digest already exists: {output}")
    digest = build_publication_digest(bundle_dir, source_commit, False)
    entry = _registry_entry(
        Path(registry_path),
        str(digest["experiment_id"]),
        str(digest["published_run_id"]),
        str(digest["decision"]),
    )
    if entry["sourceCommit"] != source_commit:
        raise ValueError("registry source commit mismatch")
    digest["readme"] = _readme_identity(Path(readme_path), digest)
    payload = _json_bytes(digest)
    output.parent.mkdir(parents=True, exist_ok=True)
    try:
        with output.open("xb") as handle:
            handle.write(payload)
    except FileExistsError:
        raise
    return digest


def verify_publication(
    bundle_dir: str | Path,
    digest_path: str | Path,
    readme_path: str | Path,
    registry_path: str | Path,
) -> dict[str, object]:
    """Read-only verification of one registered publication."""
    path = Path(digest_path)
    try:
        committed = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("malformed or missing digest") from exc
    if not isinstance(committed, dict) or committed.get("schema_version") != 2:
        raise ValueError("unsupported digest schema")
    source = committed.get("source")
    if not isinstance(source, dict) or source.get("git_dirty") is not False:
        raise ValueError("invalid digest source")
    source_commit = source.get("git_commit")
    if not isinstance(source_commit, str):
        raise ValueError("invalid digest source commit")
    rebuilt = build_publication_digest(bundle_dir, source_commit, False)
    _registry_entry(
        Path(registry_path),
        str(rebuilt["experiment_id"]),
        str(rebuilt["published_run_id"]),
        str(rebuilt["decision"]),
    )
    rebuilt["readme"] = _readme_identity(Path(readme_path), rebuilt)
    if _json_bytes(rebuilt) != _json_bytes(committed):
        raise ValueError("publication digest verification failed")
    return rebuilt


def _cli() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="mode", required=True)
    for mode in ("publish", "verify"):
        command = subparsers.add_parser(mode)
        command.add_argument("--research-root", type=Path, required=True)
        command.add_argument("--experiment-id", required=True)
        command.add_argument("--run-id", required=True)
        command.add_argument("--digest-path", type=Path, required=True)
        command.add_argument("--readme-path", type=Path, required=True)
        command.add_argument("--registry-path", type=Path, required=True)
        if mode == "publish":
            command.add_argument("--source-commit", required=True)
    args = parser.parse_args()
    bundle = args.research_root / args.experiment_id / args.run_id
    if args.mode == "publish":
        publish(
            bundle,
            args.digest_path,
            args.readme_path,
            args.registry_path,
            args.source_commit,
        )
    else:
        verify_publication(
            bundle,
            args.digest_path,
            args.readme_path,
            args.registry_path,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli())
