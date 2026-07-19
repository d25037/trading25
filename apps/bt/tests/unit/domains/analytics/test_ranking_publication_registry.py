from __future__ import annotations

import json
from pathlib import Path
import tomllib


BT_ROOT = Path(__file__).resolve().parents[4]
FIXTURE_ROOT = BT_ROOT / "tests/fixtures/research"
CATALOG_PATH = BT_ROOT / "docs/experiments/research-catalog-metadata.toml"
REGISTRY_FIELDS = ("canonicalRunId", "canonicalDecision", "supersededRunIds")
TECHNICAL_EXPERIMENT_ID = (
    "market-behavior/ranking-technical-fit-score-shape-evidence"
)


def _load_json(path: Path) -> object:
    return json.loads(path.read_text(encoding="utf-8"))


def test_ranking_publication_registry_matches_catalog_and_technical_digest() -> None:
    registry = _load_json(FIXTURE_ROOT / "ranking_publication_registry.json")
    assert isinstance(registry, dict)

    catalog = tomllib.loads(CATALOG_PATH.read_text(encoding="utf-8"))
    experiments = catalog["experiments"]

    for experiment_id, expected in registry.items():
        actual_entry = experiments[experiment_id]
        actual = {field: actual_entry[field] for field in REGISTRY_FIELDS}
        assert actual == expected

        superseded = actual["supersededRunIds"]
        assert actual["canonicalRunId"] not in superseded
        assert len(superseded) == len(set(superseded))

    digest = _load_json(
        FIXTURE_ROOT
        / "ranking_technical_fit_score_shape_evidence_published_digest.json"
    )
    assert isinstance(digest, dict)
    technical = registry[TECHNICAL_EXPERIMENT_ID]
    assert technical["canonicalRunId"] == digest["published_run_id"]
    assert technical["canonicalDecision"] == digest["decision"]
