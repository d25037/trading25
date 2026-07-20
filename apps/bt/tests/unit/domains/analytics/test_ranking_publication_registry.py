from __future__ import annotations

import json
from pathlib import Path
import tomllib


BT_ROOT = Path(__file__).resolve().parents[4]
FIXTURE_ROOT = BT_ROOT / "tests/fixtures/research"
CATALOG_PATH = BT_ROOT / "docs/experiments/research-catalog-metadata.toml"
REGISTRY_FIELDS = (
    "canonicalRunId",
    "canonicalDecision",
    "sourceCommit",
    "bundlePath",
    "digestPath",
    "supersededRunIds",
)


def _load_json(path: Path) -> object:
    return json.loads(path.read_text(encoding="utf-8"))


def test_ranking_publication_registry_matches_catalog_and_all_digests() -> None:
    registry = _load_json(FIXTURE_ROOT / "ranking_publication_registry.json")
    assert isinstance(registry, dict)

    catalog = tomllib.loads(CATALOG_PATH.read_text(encoding="utf-8"))
    experiments = catalog["experiments"]

    for experiment_id, expected in registry.items():
        actual_entry = experiments[experiment_id]
        actual = {field: actual_entry[field] for field in REGISTRY_FIELDS}
        assert actual == {field: expected[field] for field in REGISTRY_FIELDS}

        superseded = actual["supersededRunIds"]
        assert actual["canonicalRunId"] not in superseded
        assert len(superseded) == len(set(superseded))

        digest_path = BT_ROOT.parent.parent / actual["digestPath"]
        digest = _load_json(digest_path)
        assert isinstance(digest, dict)
        assert digest["schema_version"] == 2
        assert digest["experiment_id"] == experiment_id
        assert actual["canonicalRunId"] == digest["published_run_id"]
        assert actual["canonicalDecision"] == digest["decision"]
        assert actual["sourceCommit"] == digest["source"]["git_commit"]
        assert expected["artifactHashes"] == {
            "manifest": digest["artifacts"]["manifest_sha256"],
            "results": digest["artifacts"]["results_file_sha256"],
            "summary": digest["artifacts"]["summary_sha256"],
        }
