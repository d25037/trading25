from __future__ import annotations

import pytest

from src.application.services.ingestion_pipeline import (
    passthrough_rows,
    run_ingestion_batch,
    validate_rows_required_fields,
)
from src.infrastructure.db.market.market_mutations import MarketMutationStats, SemanticDeltaResult


@pytest.mark.asyncio
async def test_run_ingestion_batch_executes_five_stages() -> None:
    stages: list[str] = []

    async def fetch_rows() -> list[dict[str, object]]:
        stages.append("fetch")
        return [
            {"code": "7203", "date": "2026-02-10", "value": 1},
            {"code": "7203", "date": "2026-02-10", "value": 2},
            {"code": "", "date": "2026-02-10", "value": 3},
        ]

    def normalize_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
        stages.append("normalize")
        return rows

    def validate_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
        stages.append("validate")
        return validate_rows_required_fields(
            rows,
            required_fields=("code", "date"),
            dedupe_keys=("code", "date"),
            stage="test",
        )

    async def publish_rows(rows: list[dict[str, object]]) -> SemanticDeltaResult:
        stages.append("publish")
        return SemanticDeltaResult(
            stats=MarketMutationStats(input=len(rows), inserted=len(rows), updated=0, unchanged=0, deleted=0)
        )

    async def index_rows(_rows: list[dict[str, object]]) -> None:
        stages.append("index")

    result = await run_ingestion_batch(
        stage="test",
        fetch=fetch_rows,
        normalize=normalize_rows,
        validate=validate_rows,
        publish=publish_rows,
        index=index_rows,
    )

    assert stages == ["fetch", "normalize", "validate", "publish", "index"]
    assert result.fetched_count == 3
    assert result.normalized_count == 3
    assert result.validated_count == 1
    assert result.published_count == 1
    assert result.rows[0]["value"] == 2


def test_validate_rows_required_fields_supports_passthrough() -> None:
    rows = [{"code": "7203", "date": "2026-02-10"}]
    assert passthrough_rows(rows) == rows


def test_validate_rows_rejects_fallback_identity_collision_before_dedupe() -> None:
    rows = [
        {
            "code": "7203",
            "statement_id": "fallback:collision",
            "disclosed_date": "2026-02-10",
            "profit": 100.0,
        },
        {
            "code": "7203",
            "statement_id": "fallback:collision",
            "disclosed_date": "2026-02-10",
            "profit": 101.0,
        },
    ]

    with pytest.raises(ValueError, match="fallback statement identity collision"):
        validate_rows_required_fields(
            rows,
            required_fields=("code", "statement_id", "disclosed_date"),
            dedupe_keys=("code", "statement_id"),
            stage="fundamentals",
        )


def test_validate_rows_dedupes_identical_fallback_identity() -> None:
    row = {
        "code": "7203",
        "statement_id": "fallback:identical",
        "disclosed_date": "2026-02-10",
        "profit": 100.0,
    }

    assert validate_rows_required_fields(
        [row, dict(row)],
        required_fields=("code", "statement_id", "disclosed_date"),
        dedupe_keys=("code", "statement_id"),
        stage="fundamentals",
    ) == [row]
