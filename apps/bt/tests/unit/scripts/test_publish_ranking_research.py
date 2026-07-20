from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import time

import duckdb
import pytest


SCRIPT = Path(__file__).resolve().parents[3] / "scripts/research/publish_ranking_research.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("publish_ranking_research", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


POLICY_CASES = (
    (
        "market-behavior/ranking-trend-acceleration-conditional-lift",
        "decision_gate_df",
        "CREATE TABLE decision_gate_df(recommendation VARCHAR, gate VARCHAR, passed BOOLEAN)",
        [("reject_introduction", "final_decision", True)],
        "reject_introduction",
    ),
    (
        "market-behavior/ranking-fixed-return-priority-evidence",
        "decision_gate",
        "CREATE TABLE decision_gate(decision_key VARCHAR, passed BOOLEAN, reason VARCHAR)",
        [("final_recommendation", False, "insufficient_evidence")],
        "insufficient_evidence",
    ),
    (
        "market-behavior/ranking-technical-fit-score-shape-evidence",
        "decision_gate",
        "CREATE TABLE decision_gate(decision_key VARCHAR, decision VARCHAR, sufficient_sample BOOLEAN, passed BOOLEAN)",
        [("fixed_vs_ols", "neither", True, False)],
        "neither",
    ),
)


def _bundle(
    root: Path,
    experiment_id: str,
    decision_table: str,
    decision_ddl: str,
    decision_rows: list[tuple[object, ...]],
    *,
    reverse: bool = False,
) -> tuple[Path, Path]:
    run_id = "run-v1"
    bundle = root / "research" / experiment_id / run_id
    bundle.mkdir(parents=True)
    manifest = {
        "experiment_id": experiment_id,
        "run_id": run_id,
        "git_commit": "a" * 40,
        "git_dirty": False,
        "db_fingerprint": {"exists": True, "size_bytes": 123, "mtime_ns": 456},
        "output_tables": [decision_table, "selection"],
        "result_metadata": {
            "price_projection": {
                "price_projection_sha256": "b" * 64,
                "signal_basis_row_count": 7,
                "no_stock_data_fallback": True,
            },
            "selection_audit": {"selected_count": 2, "selection_sha256": "c" * 64},
        },
    }
    (bundle / "manifest.json").write_text(
        json.dumps(manifest, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )
    (bundle / "summary.md").write_text("# deterministic summary\n", encoding="utf-8")
    conn = duckdb.connect(str(bundle / "results.duckdb"))
    try:
        conn.execute(decision_ddl)
        placeholders = ", ".join("?" for _ in decision_rows[0])
        rows = list(reversed(decision_rows)) if reverse else decision_rows
        conn.executemany(f"INSERT INTO {decision_table} VALUES ({placeholders})", rows)
        conn.execute("CREATE TABLE selection(code VARCHAR, date DATE, score DOUBLE)")
        rows2 = [("1002", "2024-01-02", 2.0), ("1001", "2024-01-01", 1.0)]
        if reverse:
            rows2.reverse()
        conn.executemany("INSERT INTO selection VALUES (?, ?, ?)", rows2)
    finally:
        conn.close()
    readme = root / "repo" / "apps/bt/docs/experiments" / experiment_id / "README.md"
    readme.parent.mkdir(parents=True)
    return bundle, readme


@pytest.mark.parametrize("experiment_id,table,ddl,rows,decision", POLICY_CASES)
def test_real_policy_builds_complete_deterministic_digest(
    tmp_path: Path,
    experiment_id: str,
    table: str,
    ddl: str,
    rows: list[tuple[object, ...]],
    decision: str,
) -> None:
    module = _load_module()
    bundle, _ = _bundle(tmp_path, experiment_id, table, ddl, rows)

    first = module.build_publication_digest(bundle, "a" * 40, False)
    second = module.build_publication_digest(bundle, "a" * 40, False)

    assert first == second
    assert first["schema_version"] == 2
    assert first["experiment_id"] == experiment_id
    assert first["published_run_id"] == "run-v1"
    assert first["decision"] == decision
    assert first["source"] == {"git_commit": "a" * 40, "git_dirty": False}
    assert [item["name"] for item in first["tables"]] == sorted(
        [table, "selection"]
    )
    assert all(len(item["content_sha256"]) == 64 for item in first["tables"])
    assert first["selection_cohort"]["selection"]["row_count"] == 2
    assert first["projection_audit"]["price_projection_sha256"] == "b" * 64
    json.dumps(first, allow_nan=False, sort_keys=True)


def test_table_digest_is_independent_of_physical_row_order(tmp_path: Path) -> None:
    module = _load_module()
    case = POLICY_CASES[1]
    left, _ = _bundle(tmp_path / "left", *case[:4])
    right, _ = _bundle(tmp_path / "right", *case[:4], reverse=True)

    left_digest = module.build_publication_digest(left, "a" * 40, False)
    right_digest = module.build_publication_digest(right, "a" * 40, False)

    assert left_digest == right_digest


@pytest.mark.parametrize("mutation", ["manifest", "summary", "row", "schema"])
def test_verify_detects_bundle_mutation(tmp_path: Path, mutation: str) -> None:
    module = _load_module()
    case = POLICY_CASES[1]
    bundle, readme = _bundle(tmp_path, *case[:4])
    readme.write_text(
        "# Readout\n\n## Published Readout\n\n"
        "Decision: `insufficient_evidence`\n"
        "Run: `run-v1`\nSource commit: `" + "a" * 40 + "`\n",
        encoding="utf-8",
    )
    digest_path = tmp_path / "digest.json"
    registry_path = tmp_path / "registry.json"
    registry_path.write_text(
        json.dumps(
            {
                case[0]: {
                    "canonicalRunId": "run-v1",
                    "canonicalDecision": "insufficient_evidence",
                    "supersededRunIds": [],
                    "digestPath": str(digest_path),
                    "readmePath": str(readme),
                    "sourceCommit": "a" * 40,
                }
            }
        ),
        encoding="utf-8",
    )
    module.publish(
        bundle,
        digest_path=digest_path,
        readme_path=readme,
        registry_path=registry_path,
        source_commit="a" * 40,
    )
    if mutation == "manifest":
        (bundle / "manifest.json").write_text("{}\n", encoding="utf-8")
    elif mutation == "summary":
        (bundle / "summary.md").write_text("mutated\n", encoding="utf-8")
    else:
        conn = duckdb.connect(str(bundle / "results.duckdb"))
        try:
            if mutation == "row":
                conn.execute("UPDATE selection SET score = 9 WHERE code = '1001'")
            else:
                conn.execute("ALTER TABLE selection ADD COLUMN extra INTEGER")
        finally:
            conn.close()

    with pytest.raises(ValueError):
        module.verify_publication(
            bundle,
            digest_path=digest_path,
            readme_path=readme,
            registry_path=registry_path,
        )


def test_publish_and_verify_are_create_only_and_read_only(tmp_path: Path) -> None:
    module = _load_module()
    case = POLICY_CASES[2]
    bundle, readme = _bundle(tmp_path, *case[:4])
    readme.write_text(
        "# Readout\n\n## Published Readout\n\nDecision: `neither`\nRun: `run-v1`\n"
        "Source commit: `" + "a" * 40 + "`\n",
        encoding="utf-8",
    )
    digest_path = tmp_path / "digest.json"
    registry_path = tmp_path / "registry.json"
    registry_path.write_text(
        json.dumps(
            {
                case[0]: {
                    "canonicalRunId": "run-v1",
                    "canonicalDecision": "neither",
                    "supersededRunIds": [],
                    "digestPath": str(digest_path),
                    "readmePath": str(readme),
                    "sourceCommit": "a" * 40,
                }
            }
        ),
        encoding="utf-8",
    )
    module.publish(
        bundle,
        digest_path=digest_path,
        readme_path=readme,
        registry_path=registry_path,
        source_commit="a" * 40,
    )
    with pytest.raises(FileExistsError):
        module.publish(
            bundle,
            digest_path=digest_path,
            readme_path=readme,
            registry_path=registry_path,
            source_commit="a" * 40,
        )
    tracked = [digest_path, readme, bundle / "manifest.json", bundle / "results.duckdb", bundle / "summary.md"]
    before = {path: (path.read_bytes(), path.stat().st_mtime_ns) for path in tracked}
    time.sleep(0.002)
    module.verify_publication(
        bundle,
        digest_path=digest_path,
        readme_path=readme,
        registry_path=registry_path,
    )
    after = {path: (path.read_bytes(), path.stat().st_mtime_ns) for path in tracked}
    assert after == before


@pytest.mark.parametrize(
    "commit,dirty,error",
    [("d" * 40, False, "source commit mismatch"), ("a" * 40, True, "dirty")],
)
def test_build_fails_closed_for_source_identity(
    tmp_path: Path, commit: str, dirty: bool, error: str
) -> None:
    module = _load_module()
    bundle, _ = _bundle(tmp_path, *POLICY_CASES[0][:4])
    with pytest.raises(ValueError, match=error):
        module.build_publication_digest(bundle, commit, dirty)


def test_build_fails_for_missing_extra_tables_and_nonfinite_values(tmp_path: Path) -> None:
    module = _load_module()
    bundle, _ = _bundle(tmp_path, *POLICY_CASES[0][:4])
    manifest_path = bundle / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["output_tables"].append("missing")
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError, match="output tables"):
        module.build_publication_digest(bundle, "a" * 40, False)

    manifest["output_tables"].remove("missing")
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    conn = duckdb.connect(str(bundle / "results.duckdb"))
    try:
        conn.execute("INSERT INTO selection VALUES ('9999', DATE '2024-01-03', 'NaN')")
    finally:
        conn.close()
    with pytest.raises(ValueError, match="finite"):
        module.build_publication_digest(bundle, "a" * 40, False)


def test_build_fails_for_malformed_sha_and_missing_decision(tmp_path: Path) -> None:
    module = _load_module()
    bundle, _ = _bundle(tmp_path, *POLICY_CASES[2][:4])
    manifest_path = bundle / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["result_metadata"]["price_projection"]["price_projection_sha256"] = "bad"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError, match="SHA-256"):
        module.build_publication_digest(bundle, "a" * 40, False)

    manifest["result_metadata"]["price_projection"]["price_projection_sha256"] = "b" * 64
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    conn = duckdb.connect(str(bundle / "results.duckdb"))
    try:
        conn.execute("DELETE FROM decision_gate")
    finally:
        conn.close()
    with pytest.raises(ValueError, match="decision"):
        module.build_publication_digest(bundle, "a" * 40, False)


def test_verify_rejects_registry_and_readme_identity_mutation(tmp_path: Path) -> None:
    module = _load_module()
    case = POLICY_CASES[1]
    bundle, readme = _bundle(tmp_path, *case[:4])
    readme.write_text(
        "## Published Readout\nDecision `insufficient_evidence` run `run-v1` source `"
        + "a" * 40
        + "`\n",
        encoding="utf-8",
    )
    digest_path = tmp_path / "digest.json"
    registry_path = tmp_path / "registry.json"
    entry = {
        "canonicalRunId": "run-v1",
        "canonicalDecision": "insufficient_evidence",
        "supersededRunIds": [],
        "digestPath": str(digest_path),
        "readmePath": str(readme),
        "sourceCommit": "a" * 40,
    }
    registry_path.write_text(json.dumps({case[0]: entry}), encoding="utf-8")
    module.publish(bundle, digest_path, readme, registry_path, "a" * 40)

    readme.write_text(readme.read_text().replace("run-v1", "run-v2"), encoding="utf-8")
    with pytest.raises(ValueError):
        module.verify_publication(bundle, digest_path, readme, registry_path)
