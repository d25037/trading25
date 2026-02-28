from __future__ import annotations

import pytest

from src.application.services.ingestion_pipeline import (
    passthrough_rows,
    run_ingestion_batch,
    validate_rows_required_fields,
)


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

    async def publish_rows(rows: list[dict[str, object]]) -> int:
        stages.append("publish")
        return len(rows)

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


def test_validate_rows_required_fields_supports_passthrough() -> None:
    rows = [{"code": "7203", "date": "2026-02-10"}]
    assert passthrough_rows(rows) == rows
