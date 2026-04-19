"""Published research catalog routes."""

from __future__ import annotations

from datetime import UTC, datetime

from fastapi import APIRouter, HTTPException, Query

from src.application.services.research_catalog_service import (
    PublishedResearchSummaryData,
    ResearchCatalogEntry,
    ResearchPublication,
    ResearchRunReference as ResearchRunReferenceData,
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


def _to_catalog_item(entry: ResearchCatalogEntry) -> ResearchCatalogItem:
    return ResearchCatalogItem(
        experimentId=entry.experiment_id,
        runId=entry.run_id,
        title=entry.title,
        objective=entry.objective,
        headline=entry.headline,
        docsReadmePath=entry.docs_readme_path,
        createdAt=entry.created_at,
        analysisStartDate=entry.analysis_start_date,
        analysisEndDate=entry.analysis_end_date,
        gitCommit=entry.git_commit,
        tags=list(entry.tags),
        hasStructuredSummary=entry.has_structured_summary,
    )


def _to_summary(summary: PublishedResearchSummaryData | None) -> PublishedResearchSummary | None:
    if summary is None:
        return None
    return PublishedResearchSummary(
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


def _to_run_reference(item: ResearchRunReferenceData) -> ResearchRunReference:
    return ResearchRunReference(
        runId=item.run_id,
        createdAt=item.created_at,
        isLatest=item.is_latest,
    )


def _to_detail_response(publication: ResearchPublication) -> ResearchDetailResponse:
    return ResearchDetailResponse(
        item=_to_catalog_item(publication.item),
        summary=_to_summary(publication.summary),
        summaryMarkdown=publication.summary_markdown,
        outputTables=list(publication.output_tables),
        availableRuns=[_to_run_reference(item) for item in publication.available_runs],
        resultMetadata=publication.result_metadata,
    )


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
    items = [_to_catalog_item(item) for item in list_latest_research_publications()]
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

    return _to_detail_response(publication)
