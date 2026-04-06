"""Published research catalog routes."""

from __future__ import annotations

from datetime import UTC, datetime

from fastapi import APIRouter, HTTPException, Query

from src.application.services.research_catalog_service import (
    get_research_publication,
    list_latest_research_publications,
)
from src.entrypoints.http.schemas.research import (
    PublishedResearchSummary,
    ResearchCatalogItem,
    ResearchCatalogResponse,
    ResearchDetailResponse,
    ResearchHighlight,
    ResearchLabelValue,
    ResearchRunReference,
    ResearchTableHighlight,
)

router = APIRouter(tags=["Analytics"])


@router.get(
    "/api/analytics/research",
    response_model=ResearchCatalogResponse,
    summary="List published analytics research bundles",
    description=(
        "List the latest available research bundle per experiment. "
        "Structured `summary.json` payloads are returned when available."
    ),
)
async def list_research_catalog() -> ResearchCatalogResponse:
    items = [
        ResearchCatalogItem(
            experimentId=item.experiment_id,
            runId=item.run_id,
            title=item.title,
            objective=item.objective,
            headline=item.headline,
            createdAt=item.created_at,
            analysisStartDate=item.analysis_start_date,
            analysisEndDate=item.analysis_end_date,
            gitCommit=item.git_commit,
            tags=list(item.tags),
            hasStructuredSummary=item.has_structured_summary,
        )
        for item in list_latest_research_publications()
    ]
    return ResearchCatalogResponse(
        items=items,
        lastUpdated=datetime.now(UTC).isoformat(),
    )


@router.get(
    "/api/analytics/research/detail",
    response_model=ResearchDetailResponse,
    summary="Get a published analytics research bundle",
    description=(
        "Fetch the latest run or a specific run for a research experiment, "
        "including the structured summary, raw markdown summary, and available runs."
    ),
)
async def get_research_detail(
    experimentId: str = Query(..., min_length=1),
    runId: str | None = Query(None, min_length=1),
) -> ResearchDetailResponse:
    try:
        publication = get_research_publication(experimentId, run_id=runId)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))

    summary = publication.summary
    return ResearchDetailResponse(
        item=ResearchCatalogItem(
            experimentId=publication.item.experiment_id,
            runId=publication.item.run_id,
            title=publication.item.title,
            objective=publication.item.objective,
            headline=publication.item.headline,
            createdAt=publication.item.created_at,
            analysisStartDate=publication.item.analysis_start_date,
            analysisEndDate=publication.item.analysis_end_date,
            gitCommit=publication.item.git_commit,
            tags=list(publication.item.tags),
            hasStructuredSummary=publication.item.has_structured_summary,
        ),
        summary=(
            PublishedResearchSummary(
                title=summary.title,
                tags=list(summary.tags),
                purpose=summary.purpose,
                method=list(summary.method),
                resultHeadline=summary.result_headline,
                resultBullets=list(summary.result_bullets),
                considerations=list(summary.considerations),
                selectedParameters=[
                    ResearchLabelValue(label=item.label, value=item.value)
                    for item in summary.selected_parameters
                ],
                highlights=[
                    ResearchHighlight(
                        label=item.label,
                        value=item.value,
                        tone=item.tone,
                        detail=item.detail,
                    )
                    for item in summary.highlights
                ],
                tableHighlights=[
                    ResearchTableHighlight(
                        name=item.name,
                        label=item.label,
                        description=item.description,
                    )
                    for item in summary.table_highlights
                ],
            )
            if summary is not None
            else None
        ),
        summaryMarkdown=publication.summary_markdown,
        outputTables=list(publication.output_tables),
        availableRuns=[
            ResearchRunReference(
                runId=item.run_id,
                createdAt=item.created_at,
                isLatest=item.is_latest,
            )
            for item in publication.available_runs
        ],
        resultMetadata=publication.result_metadata,
    )
