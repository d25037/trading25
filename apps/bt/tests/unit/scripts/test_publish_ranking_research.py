from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import time

import duckdb
import pytest


SCRIPT = Path(__file__).resolve().parents[3] / "scripts/research/publish_ranking_research.py"
SOURCE_COMMIT = "a" * 40
PROJECTION = {
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
    "price_projection_sha256": "b" * 64,
}


def _load_module():
    spec = importlib.util.spec_from_file_location("publish_ranking_research", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


POLICY_CASES = (
    (
        "market-behavior/ranking-trend-acceleration-conditional-lift",
        "reject_introduction",
        "trend_candidate_group_membership_before_outcomes_v1",
        ["date", "code", "candidate_group"],
        {
            "binary_gate_pass_count": 0,
            "binary_gate_total": 0,
            "continuous_gate_pass_count": 0,
            "continuous_gate_total": 0,
            "observation_count": 7,
            "topk_complete_row_count": 1,
            "topk_incomplete_row_count": 1,
        },
    ),
    (
        "market-behavior/ranking-fixed-return-priority-evidence",
        "insufficient_evidence",
        "fixed_return_free_scaffold_membership_before_outcomes_v1",
        ["date", "code", "scaffold_family"],
        {
            "observation_count": 7,
            "strict_value_observation_count": 3,
            "topk_complete_row_count": 1,
            "topk_incomplete_row_count": 1,
            "value_extension_observation_count": 4,
        },
    ),
    (
        "market-behavior/ranking-technical-fit-score-shape-evidence",
        "neither",
        "technical_fit_ring_membership_before_outcomes_v1",
        ["date", "code", "ring"],
        {
            "fixed_core_oos_mean_lift_pct": 1.0,
            "fixed_top5_mean_lift_pct": 3.0,
            "near1_fixed_minus_ols_mean_lift_pct": -1.0,
            "observation_count": 7,
            "ols_core_oos_mean_lift_pct": 2.0,
            "ols_top5_mean_lift_pct": 4.0,
            "topk_complete_row_count": 2,
            "topk_incomplete_row_count": 1,
        },
    ),
)


def _bundle(root: Path, case: tuple[object, ...], *, reverse: bool = False) -> Path:
    experiment_id, _, selection_policy, selection_keys, _ = case
    run_id = "run-v1"
    bundle = root / "research" / str(experiment_id) / run_id
    bundle.mkdir(parents=True)
    output_tables = ["selection"]
    metadata: dict[str, object] = {
        "observation_count": 7,
        "price_projection": PROJECTION,
        "selection_audit": {
            "policy": selection_policy,
            "key_columns": selection_keys,
            "row_count": 7,
            "sha256": "c" * 64,
        },
    }
    if "technical-fit-score" in str(experiment_id):
        metadata.pop("price_projection")
        metadata["pit_lineage"] = {
            "data_plane": "physical_market.duckdb_schema_v4",
            "stock_price_adjustment_mode": "local_projection_v2_event_time",
            "universe_source": "stock_master_daily",
            "as_of_policy": "exact_signal_date_no_latest_fallback",
            "basis_dependent_sources": ["daily_valuation", "stock_data_raw"],
            "verification_status": "verified",
            "no_service_local_recomputation": True,
            "no_basis_fallback": True,
            "price_projection": PROJECTION,
        }
    manifest = {
        "experiment_id": experiment_id,
        "run_id": run_id,
        "git_commit": SOURCE_COMMIT,
        "git_dirty": False,
        "db_fingerprint": {"exists": True, "size_bytes": 123, "mtime_ns": 456},
        "output_tables": output_tables,
        "result_metadata": metadata,
    }
    (bundle / "summary.md").write_text("# deterministic summary\n", encoding="utf-8")
    conn = duckdb.connect(str(bundle / "results.duckdb"))
    try:
        conn.execute("CREATE TABLE selection(code VARCHAR, date DATE, score DOUBLE)")
        rows = [("1002", "2024-01-02", 2.0), ("1001", "2024-01-01", 1.0)]
        conn.executemany("INSERT INTO selection VALUES (?, ?, ?)", reversed(rows) if reverse else rows)
        if "trend-acceleration" in str(experiment_id):
            conn.execute(
                "CREATE TABLE decision_gate_df(recommendation VARCHAR, gate VARCHAR, passed BOOLEAN)"
            )
            conn.execute(
                "INSERT INTO decision_gate_df VALUES ('reject_introduction', 'final_decision', TRUE)"
            )
            conn.execute("CREATE TABLE coverage_diagnostics_df(observation_count BIGINT)")
            conn.execute("INSERT INTO coverage_diagnostics_df VALUES (7)")
            conn.execute("CREATE TABLE topk_priority_lift_df(outcome_status VARCHAR)")
            conn.executemany(
                "INSERT INTO topk_priority_lift_df VALUES (?)",
                [("complete",), ("incomplete",)],
            )
            output_tables.extend(
                ["decision_gate_df", "coverage_diagnostics_df", "topk_priority_lift_df"]
            )
        elif "fixed-return-priority" in str(experiment_id):
            conn.execute(
                "CREATE TABLE decision_gate(decision_key VARCHAR, passed BOOLEAN, reason VARCHAR)"
            )
            conn.execute(
                "INSERT INTO decision_gate VALUES ('final_recommendation', FALSE, 'insufficient_evidence')"
            )
            conn.execute(
                "CREATE TABLE coverage_attrition(scaffold_family VARCHAR, observation_count BIGINT)"
            )
            conn.executemany(
                "INSERT INTO coverage_attrition VALUES (?, ?)",
                [("strict_value_long_only", 3), ("value_extension_long_only", 4)],
            )
            conn.execute("CREATE TABLE topk_priority_lift(outcome_status VARCHAR)")
            conn.executemany(
                "INSERT INTO topk_priority_lift VALUES (?)",
                [("complete",), ("incomplete",)],
            )
            output_tables.extend(
                ["decision_gate", "coverage_attrition", "topk_priority_lift"]
            )
        else:
            conn.execute(
                "CREATE TABLE decision_gate(decision_key VARCHAR, decision VARCHAR, sufficient_sample BOOLEAN, passed BOOLEAN)"
            )
            conn.execute("INSERT INTO decision_gate VALUES ('fixed_vs_ols', 'neither', TRUE, FALSE)")
            conn.execute("CREATE TABLE coverage_attrition(ring VARCHAR, observation_count BIGINT)")
            conn.executemany(
                "INSERT INTO coverage_attrition VALUES (?, ?)",
                [("core_high_high", 3), ("near_high_high_1", 4)],
            )
            conn.execute(
                "CREATE TABLE oos_fit_score_lift(is_primary BOOLEAN, horizon BIGINT, family VARCHAR, ring VARCHAR, mean_lift_pct DOUBLE)"
            )
            conn.executemany(
                "INSERT INTO oos_fit_score_lift VALUES (?, ?, ?, ?, ?)",
                [(True, 20, "fixed", "core_high_high", 1.0), (True, 20, "ols", "core_high_high", 2.0)],
            )
            conn.execute(
                "CREATE TABLE fixed_vs_ols_paired(horizon BIGINT, ring VARCHAR, fixed_minus_ols_lift_pct DOUBLE)"
            )
            conn.execute("INSERT INTO fixed_vs_ols_paired VALUES (20, 'near_high_high_1', -1.0)")
            conn.execute(
                "CREATE TABLE topk_operational_lift(horizon BIGINT, family VARCHAR, k BIGINT, topk_lift_pct DOUBLE, outcome_status VARCHAR)"
            )
            conn.executemany(
                "INSERT INTO topk_operational_lift VALUES (?, ?, ?, ?, ?)",
                [(20, "fixed", 5, 3.0, "complete"), (20, "ols", 5, 4.0, "complete"), (60, "ols", 5, 1.0, "incomplete")],
            )
            output_tables.extend(
                ["decision_gate", "coverage_attrition", "oos_fit_score_lift", "fixed_vs_ols_paired", "topk_operational_lift"]
            )
    finally:
        conn.close()
    (bundle / "manifest.json").write_text(
        json.dumps(manifest, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )
    return bundle


def _publication_files(module, root: Path, case: tuple[object, ...], bundle: Path):
    experiment_id, decision, _, _, _ = case
    digest = module.build_publication_digest(bundle, SOURCE_COMMIT, False)
    repo = root / "repo"
    readme = repo / "apps/bt/docs/experiments" / str(experiment_id) / "README.md"
    readme.parent.mkdir(parents=True)
    identity = {
        "experiment_id": experiment_id,
        "run_id": "run-v1",
        "decision": decision,
        "source_commit": SOURCE_COMMIT,
        "git_dirty": "false",
    }
    lines = ["# Readout", "", "## Publication Identity", "", "| Field | Value |", "| --- | --- |"]
    lines.extend(f"| {key} | `{value}` |" for key, value in identity.items())
    lines.extend(["", "## Published Metrics", "", "| Metric | Value |", "| --- | --- |"])
    lines.extend(
        f"| {key} | `{module._display_value(value)}` |"
        for key, value in digest["decision_metrics"].items()
    )
    readme.write_text("\n".join(lines) + "\n", encoding="utf-8")
    digest_path = repo / "digest.json"
    artifacts = digest["artifacts"]
    hashes = {
        "manifest": artifacts["manifest_sha256"],
        "results": artifacts["results_file_sha256"],
        "summary": artifacts["summary_sha256"],
    }
    entry = {
        "canonicalRunId": "run-v1",
        "canonicalDecision": decision,
        "sourceCommit": SOURCE_COMMIT,
        "bundlePath": str(bundle),
        "digestPath": str(digest_path),
        "readmePath": str(readme),
        "artifactHashes": hashes,
        "supersededRunIds": ["run-v0"],
    }
    registry = repo / "registry.json"
    registry.write_text(json.dumps({experiment_id: entry}, indent=2) + "\n", encoding="utf-8")
    catalog = repo / "catalog.toml"
    catalog.write_text(
        f'[experiments."{experiment_id}"]\n'
        f'canonicalRunId = "run-v1"\ncanonicalDecision = "{decision}"\n'
        f'sourceCommit = "{SOURCE_COMMIT}"\nbundlePath = "{bundle}"\n'
        f'digestPath = "{digest_path}"\nreadmePath = "{readme}"\n'
        'artifactHashes = { manifest = "' + str(hashes["manifest"]) + '", results = "'
        + str(hashes["results"]) + '", summary = "' + str(hashes["summary"]) + '" }\n'
        'supersededRunIds = ["run-v0"]\n',
        encoding="utf-8",
    )
    return digest_path, readme, registry, catalog, repo


@pytest.mark.parametrize("case", POLICY_CASES)
def test_real_policy_builds_complete_deterministic_digest(tmp_path: Path, case) -> None:
    module = _load_module()
    bundle = _bundle(tmp_path, case)
    first = module.build_publication_digest(bundle, SOURCE_COMMIT, False)
    second = module.build_publication_digest(bundle, SOURCE_COMMIT, False)
    assert first == second
    assert first["schema_version"] == 3
    assert first["decision"] == case[1]
    assert first["decision_metrics"] == case[4]
    assert first["selection_audit"] == {
        "policy": case[2], "key_columns": case[3], "row_count": 7, "sha256": "c" * 64
    }
    assert first["pit_contract"]


def test_table_digest_is_independent_of_physical_row_order(tmp_path: Path) -> None:
    module = _load_module()
    left = _bundle(tmp_path / "left", POLICY_CASES[1])
    right = _bundle(tmp_path / "right", POLICY_CASES[1], reverse=True)
    left_digest = module.build_publication_digest(left, SOURCE_COMMIT, False)
    right_digest = module.build_publication_digest(right, SOURCE_COMMIT, False)
    assert left_digest["tables"] == right_digest["tables"]
    assert left_digest["artifacts"]["results_semantic_sha256"] == right_digest["artifacts"]["results_semantic_sha256"]


@pytest.mark.parametrize(
    ("field", "value"),
    [("row_count", 6), ("sha256", "bad"), ("policy", "wrong"), ("key_columns", ["date", "code"])],
)
def test_build_rejects_incomplete_selection_audit(tmp_path: Path, field: str, value: object) -> None:
    module = _load_module()
    bundle = _bundle(tmp_path, POLICY_CASES[0])
    path = bundle / "manifest.json"
    manifest = json.loads(path.read_text())
    manifest["result_metadata"]["selection_audit"][field] = value
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="selection audit"):
        module.build_publication_digest(bundle, SOURCE_COMMIT, False)


@pytest.mark.parametrize(
    ("case_index", "path", "value"),
    [
        (0, ("price_projection", "no_stock_data_fallback"), False),
        (0, ("price_projection", "physical_price_source"), "stock_data"),
        (2, ("pit_lineage", "no_basis_fallback"), False),
        (2, ("pit_lineage", "data_plane"), "schema_v3"),
    ],
)
def test_build_rejects_pit_fallback_or_lineage_drift(tmp_path: Path, case_index: int, path: tuple[str, str], value: object) -> None:
    module = _load_module()
    bundle = _bundle(tmp_path, POLICY_CASES[case_index])
    manifest_path = bundle / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["result_metadata"][path[0]][path[1]] = value
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="PIT contract"):
        module.build_publication_digest(bundle, SOURCE_COMMIT, False)


def test_publish_verify_are_create_only_byte_exact_and_read_only(tmp_path: Path) -> None:
    module = _load_module()
    case = POLICY_CASES[2]
    bundle = _bundle(tmp_path, case)
    digest_path, readme, registry, catalog, repo = _publication_files(module, tmp_path, case, bundle)
    module.publish(bundle, digest_path, readme, registry, SOURCE_COMMIT, catalog, repo)
    with pytest.raises(FileExistsError):
        module.publish(bundle, digest_path, readme, registry, SOURCE_COMMIT, catalog, repo)
    tracked = [digest_path, readme, bundle / "manifest.json", bundle / "results.duckdb", bundle / "summary.md"]
    before = {path: (path.read_bytes(), path.stat().st_mtime_ns) for path in tracked}
    time.sleep(0.002)
    module.verify_publication(bundle, digest_path, readme, registry, catalog, repo)
    assert {path: (path.read_bytes(), path.stat().st_mtime_ns) for path in tracked} == before


@pytest.mark.parametrize("mutation", ["whitespace", "duplicate_key"])
def test_verify_rejects_noncanonical_or_duplicate_digest_json(tmp_path: Path, mutation: str) -> None:
    module = _load_module()
    case = POLICY_CASES[1]
    bundle = _bundle(tmp_path, case)
    digest_path, readme, registry, catalog, repo = _publication_files(module, tmp_path, case, bundle)
    module.publish(bundle, digest_path, readme, registry, SOURCE_COMMIT, catalog, repo)
    text = digest_path.read_text()
    if mutation == "whitespace":
        digest_path.write_text(text + "\n")
    else:
        digest_path.write_text(text.replace('{\n  "artifacts"', '{\n  "schema_version": 3,\n  "artifacts"'))
    with pytest.raises(ValueError):
        module.verify_publication(bundle, digest_path, readme, registry, catalog, repo)


@pytest.mark.parametrize(
    "field",
    ["canonicalDecision", "sourceCommit", "bundlePath", "digestPath", "readmePath", "artifactHashes", "supersededRunIds"],
)
def test_verify_rejects_every_registry_identity_mutation(tmp_path: Path, field: str) -> None:
    module = _load_module()
    case = POLICY_CASES[1]
    bundle = _bundle(tmp_path, case)
    digest_path, readme, registry, catalog, repo = _publication_files(module, tmp_path, case, bundle)
    module.publish(bundle, digest_path, readme, registry, SOURCE_COMMIT, catalog, repo)
    value = json.loads(registry.read_text())
    entry = value[case[0]]
    entry[field] = ["run-v1"] if field == "supersededRunIds" else ({"manifest": "0" * 64} if field == "artifactHashes" else "mutated")
    registry.write_text(json.dumps(value))
    with pytest.raises(ValueError):
        module.verify_publication(bundle, digest_path, readme, registry, catalog, repo)


def test_verify_rejects_catalog_and_readme_metric_mutation(tmp_path: Path) -> None:
    module = _load_module()
    case = POLICY_CASES[0]
    bundle = _bundle(tmp_path, case)
    digest_path, readme, registry, catalog, repo = _publication_files(module, tmp_path, case, bundle)
    module.publish(bundle, digest_path, readme, registry, SOURCE_COMMIT, catalog, repo)
    original_readme = readme.read_text()
    readme.write_text(original_readme.replace("| observation_count | `7` |", "| observation_count | `8` |"))
    with pytest.raises(ValueError, match="README published metrics"):
        module.verify_publication(bundle, digest_path, readme, registry, catalog, repo)
    readme.write_text(original_readme)
    catalog.write_text(catalog.read_text().replace('canonicalDecision = "reject_introduction"', 'canonicalDecision = "mutated"'))
    with pytest.raises(ValueError):
        module.verify_publication(bundle, digest_path, readme, registry, catalog, repo)


def test_verify_rejects_any_duplicate_canonical_or_superseded_identity(tmp_path: Path) -> None:
    module = _load_module()
    case = POLICY_CASES[0]
    bundle = _bundle(tmp_path, case)
    digest_path, readme, registry, catalog, repo = _publication_files(module, tmp_path, case, bundle)
    module.publish(bundle, digest_path, readme, registry, SOURCE_COMMIT, catalog, repo)
    value = json.loads(registry.read_text())
    value["unrelated/experiment"] = {
        "canonicalRunId": "unrelated-v1",
        "supersededRunIds": ["run-v0"],
    }
    registry.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="globally unique"):
        module.verify_publication(bundle, digest_path, readme, registry, catalog, repo)


@pytest.mark.parametrize("commit,dirty", [("d" * 40, False), (SOURCE_COMMIT, True)])
def test_build_fails_closed_for_source_identity(tmp_path: Path, commit: str, dirty: bool) -> None:
    module = _load_module()
    with pytest.raises(ValueError):
        module.build_publication_digest(_bundle(tmp_path, POLICY_CASES[0]), commit, dirty)
