from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path
import tomllib
from typing import Any, Literal, cast

from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    list_research_bundle_infos,
    load_research_bundle_published_summary,
    resolve_research_experiment_docs_readme_path,
)

MetricTone = Literal["neutral", "accent", "success", "warning", "danger"]
ResearchDecisionStatus = Literal[
    "observed",
    "robust",
    "candidate",
    "ranking_surface",
    "strategy_draft",
    "production",
    "rejected",
]
_BT_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_RESEARCH_CATALOG_METADATA_PATH = (
    _BT_PROJECT_ROOT / "docs" / "experiments" / "research-catalog-metadata.toml"
)


@dataclass(frozen=True)
class ResearchLabelValueItem:
    label: str
    value: str


@dataclass(frozen=True)
class ResearchHighlightItem:
    label: str
    value: str
    tone: MetricTone
    detail: str | None = None


@dataclass(frozen=True)
class ResearchTableHighlightItem:
    name: str
    label: str
    description: str | None = None


@dataclass(frozen=True)
class PublishedResearchSummaryData:
    title: str
    tags: tuple[str, ...]
    family: str | None
    status: ResearchDecisionStatus
    decision: str | None
    promoted_surface: str | None
    risk_flags: tuple[str, ...]
    related_experiments: tuple[str, ...]
    purpose: str
    method: tuple[str, ...]
    result_headline: str | None
    result_bullets: tuple[str, ...]
    considerations: tuple[str, ...]
    selected_parameters: tuple[ResearchLabelValueItem, ...]
    highlights: tuple[ResearchHighlightItem, ...]
    table_highlights: tuple[ResearchTableHighlightItem, ...]


@dataclass(frozen=True)
class ResearchCatalogEntry:
    experiment_id: str
    run_id: str
    title: str
    objective: str | None
    headline: str | None
    family: str
    status: ResearchDecisionStatus
    decision: str | None
    promoted_surface: str | None
    risk_flags: tuple[str, ...]
    related_experiments: tuple[str, ...]
    docs_readme_path: str | None
    created_at: str
    analysis_start_date: str | None
    analysis_end_date: str | None
    git_commit: str | None
    tags: tuple[str, ...]
    has_structured_summary: bool


@dataclass(frozen=True)
class ResearchRunReference:
    run_id: str
    created_at: str
    is_latest: bool


@dataclass(frozen=True)
class ResearchPublication:
    item: ResearchCatalogEntry
    summary: PublishedResearchSummaryData | None
    summary_markdown: str
    output_tables: tuple[str, ...]
    available_runs: tuple[ResearchRunReference, ...]
    result_metadata: dict[str, Any]


def list_latest_research_publications() -> tuple[ResearchCatalogEntry, ...]:
    infos = list_research_bundle_infos()
    latest_by_experiment: dict[str, ResearchBundleInfo] = {}
    for info in infos:
        current = latest_by_experiment.get(info.experiment_id)
        if current is None or _info_sort_key(info) > _info_sort_key(current):
            latest_by_experiment[info.experiment_id] = info

    entries = [
        _build_catalog_entry(info)
        for info in latest_by_experiment.values()
    ]
    return tuple(
        sorted(
            entries,
            key=lambda entry: (_datetime_sort_key(entry.created_at), entry.experiment_id),
            reverse=True,
        )
    )


def get_research_publication(
    experiment_id: str,
    *,
    run_id: str | None = None,
) -> ResearchPublication:
    matching_infos = [
        info for info in list_research_bundle_infos() if info.experiment_id == experiment_id
    ]
    if not matching_infos:
        raise FileNotFoundError(f"Research bundle experiment was not found: {experiment_id}")

    sorted_infos = sorted(matching_infos, key=_info_sort_key, reverse=True)
    selected_info = sorted_infos[0]
    if run_id is not None:
        matched = next((info for info in sorted_infos if info.run_id == run_id), None)
        if matched is None:
            raise FileNotFoundError(
                f"Research bundle run was not found: experiment={experiment_id} run_id={run_id}"
            )
        selected_info = matched

    entry = _build_catalog_entry(selected_info)
    summary_markdown = selected_info.summary_path.read_text(encoding="utf-8")
    published_summary = _load_published_summary(selected_info, summary_markdown)

    return ResearchPublication(
        item=entry,
        summary=published_summary,
        summary_markdown=summary_markdown,
        output_tables=selected_info.output_tables,
        available_runs=tuple(
            ResearchRunReference(
                run_id=info.run_id,
                created_at=info.created_at,
                is_latest=index == 0,
            )
            for index, info in enumerate(sorted_infos)
        ),
        result_metadata=dict(selected_info.result_metadata),
    )


def _build_catalog_entry(info: ResearchBundleInfo) -> ResearchCatalogEntry:
    summary_markdown = info.summary_path.read_text(encoding="utf-8")
    published_summary = _load_published_summary(info, summary_markdown)
    metadata = _load_research_catalog_metadata().get(info.experiment_id, {})
    title = published_summary.title if published_summary else _extract_title(summary_markdown, info)
    objective = published_summary.purpose if published_summary else _extract_first_paragraph(summary_markdown)
    headline = (
        published_summary.result_headline
        if published_summary is not None
        else _extract_first_bullet(summary_markdown)
    )
    tags = published_summary.tags if published_summary is not None else ()
    metadata_promoted_surface = _normalize_optional_string(metadata.get("promotedSurface"))
    summary_promoted_surface = (
        published_summary.promoted_surface if published_summary is not None else None
    )
    promoted_surface = (
        metadata_promoted_surface
        or summary_promoted_surface
        or _derive_promoted_surface(info, tags)
    )
    metadata_status = _normalize_optional_status(metadata.get("status"))
    summary_status = published_summary.status if published_summary is not None else None
    status = metadata_status or summary_status or _derive_status(
        info,
        promoted_surface=promoted_surface,
    )
    risk_flags = _merge_unique_strings(
        _normalize_string_tuple(metadata.get("riskFlags")),
        published_summary.risk_flags if published_summary is not None else (),
        _derive_risk_flags(info, has_structured_summary=published_summary is not None),
    )
    metadata_family = _normalize_optional_string(metadata.get("family"))
    summary_family = published_summary.family if published_summary is not None else None
    family = metadata_family or summary_family or _derive_research_family(info, tags)
    metadata_decision = _normalize_optional_string(metadata.get("decision"))
    summary_decision = published_summary.decision if published_summary is not None else None
    decision = metadata_decision or summary_decision
    metadata_related_experiments = _normalize_string_tuple(metadata.get("relatedExperiments"))
    summary_related_experiments = (
        published_summary.related_experiments if published_summary is not None else ()
    )
    related_experiments = metadata_related_experiments or summary_related_experiments

    return ResearchCatalogEntry(
        experiment_id=info.experiment_id,
        run_id=info.run_id,
        title=title,
        objective=objective,
        headline=headline,
        family=family,
        status=status,
        decision=decision,
        promoted_surface=promoted_surface,
        risk_flags=risk_flags,
        related_experiments=related_experiments,
        docs_readme_path=resolve_research_experiment_docs_readme_path(info.experiment_id),
        created_at=info.created_at,
        analysis_start_date=info.analysis_start_date,
        analysis_end_date=info.analysis_end_date,
        git_commit=info.git_commit,
        tags=tags,
        has_structured_summary=published_summary is not None,
    )


def _load_published_summary(
    info: ResearchBundleInfo,
    summary_markdown: str,
) -> PublishedResearchSummaryData | None:
    try:
        payload = load_research_bundle_published_summary(info.bundle_dir)
    except ValueError:
        return None
    if payload is None:
        return None

    # Older bundles can use summary.json for raw result metadata. Only payloads
    # with an explicit purpose are part of the published-summary surface.
    purpose = _normalize_optional_string(payload.get("purpose"))
    if purpose is None:
        return None

    metadata = _load_research_catalog_metadata().get(info.experiment_id, {})
    title = _normalize_optional_string(payload.get("title")) or _extract_title(
        summary_markdown,
        info,
    )
    return PublishedResearchSummaryData(
        title=title,
        tags=_normalize_string_tuple(payload.get("tags")),
        family=_normalize_optional_string(metadata.get("family"))
        or _normalize_optional_string(payload.get("family")),
        status=_normalize_optional_status(metadata.get("status"))
        or _normalize_status(payload.get("status")),
        decision=_normalize_optional_string(metadata.get("decision"))
        or _normalize_optional_string(payload.get("decision")),
        promoted_surface=_normalize_optional_string(metadata.get("promotedSurface"))
        or _normalize_optional_string(payload.get("promotedSurface")),
        risk_flags=_merge_unique_strings(
            _normalize_string_tuple(metadata.get("riskFlags")),
            _normalize_string_tuple(payload.get("riskFlags")),
        ),
        related_experiments=_normalize_string_tuple(metadata.get("relatedExperiments"))
        or _normalize_string_tuple(payload.get("relatedExperiments")),
        purpose=purpose,
        method=_normalize_string_tuple(payload.get("method")),
        result_headline=_normalize_optional_string(payload.get("resultHeadline")),
        result_bullets=_normalize_string_tuple(payload.get("resultBullets")),
        considerations=_normalize_string_tuple(payload.get("considerations")),
        selected_parameters=_normalize_label_value_items(payload.get("selectedParameters")),
        highlights=_normalize_highlight_items(payload.get("highlights")),
        table_highlights=_normalize_table_highlight_items(payload.get("tableHighlights")),
    )


def _normalize_optional_string(value: Any) -> str | None:
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return None


def _normalize_optional_status(value: Any) -> ResearchDecisionStatus | None:
    normalized = _normalize_optional_string(value)
    if normalized in {
        "observed",
        "robust",
        "candidate",
        "ranking_surface",
        "strategy_draft",
        "production",
        "rejected",
    }:
        return cast(ResearchDecisionStatus, normalized)
    return None


def _normalize_status(value: Any) -> ResearchDecisionStatus:
    normalized = _normalize_optional_status(value)
    if normalized is not None:
        return normalized
    return "observed"


def _normalize_string_tuple(value: Any) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    items: list[str] = []
    for item in value:
        normalized = _normalize_optional_string(item)
        if normalized is not None:
            items.append(normalized)
    return tuple(items)


@lru_cache(maxsize=1)
def _load_research_catalog_metadata() -> dict[str, dict[str, Any]]:
    try:
        raw_payload = tomllib.loads(_RESEARCH_CATALOG_METADATA_PATH.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError):
        return {}
    if not isinstance(raw_payload, dict):
        return {}

    raw_experiments = raw_payload.get("experiments", raw_payload)
    if not isinstance(raw_experiments, dict):
        return {}

    metadata: dict[str, dict[str, Any]] = {}
    for experiment_id, value in raw_experiments.items():
        if isinstance(experiment_id, str) and isinstance(value, dict):
            metadata[experiment_id] = value
    return metadata


def _merge_unique_strings(*groups: tuple[str, ...]) -> tuple[str, ...]:
    items: list[str] = []
    seen: set[str] = set()
    for group in groups:
        for item in group:
            normalized = item.strip()
            if normalized and normalized not in seen:
                items.append(normalized)
                seen.add(normalized)
    return tuple(items)


def _derive_research_family(info: ResearchBundleInfo, tags: tuple[str, ...]) -> str:
    experiment_id = info.experiment_id.lower()
    tag_text = " ".join(tags).lower()
    haystack = f"{experiment_id} {tag_text}"

    if experiment_id.startswith("strategy-audit/"):
        return "Strategy Audit"
    if "speculative-volume-surge" in haystack:
        return "Speculative Volume Surge"
    if "falling-knife" in haystack:
        return "Falling Knife"
    if "stop-limit" in haystack:
        return "JPX Stop Limit"
    if "annual-" in experiment_id and (
        "fundamental" in haystack or "value" in haystack or "forward-per" in haystack
    ):
        return "Annual Fundamentals"
    if "topix100" in haystack and (
        "sma" in haystack or "q10" in haystack or "bounce" in haystack or "streak" in haystack
    ):
        return "TOPIX100 Regime"
    if "topix" in haystack or "nt-ratio" in haystack:
        return "Market Regime"

    return info.experiment_id.split("/", maxsplit=1)[0].replace("-", " ").title()


def _derive_promoted_surface(
    info: ResearchBundleInfo,
    tags: tuple[str, ...],
) -> str | None:
    experiment_id = info.experiment_id.lower()
    tag_text = " ".join(tags).lower()
    if "annual-value-composite-selection" in experiment_id:
        return "Ranking"
    if "strategy-audit" in experiment_id or "production" in tag_text:
        return "Strategy"
    return None


def _derive_status(
    info: ResearchBundleInfo,
    *,
    promoted_surface: str | None,
) -> ResearchDecisionStatus:
    if promoted_surface == "Ranking":
        return "ranking_surface"
    if promoted_surface == "Strategy":
        return "strategy_draft"
    if info.experiment_id.startswith("strategy-audit/"):
        return "candidate"
    return "observed"


def _derive_risk_flags(
    info: ResearchBundleInfo,
    *,
    has_structured_summary: bool,
) -> tuple[str, ...]:
    flags: list[str] = []
    if not has_structured_summary:
        flags.append("markdown-only")
    if resolve_research_experiment_docs_readme_path(info.experiment_id) is None:
        flags.append("docs-missing")
    return tuple(flags)


def _normalize_label_value_items(value: Any) -> tuple[ResearchLabelValueItem, ...]:
    if not isinstance(value, list):
        return ()
    items: list[ResearchLabelValueItem] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        label = _normalize_optional_string(item.get("label"))
        data_value = _normalize_optional_string(item.get("value"))
        if label is None or data_value is None:
            continue
        items.append(ResearchLabelValueItem(label=label, value=data_value))
    return tuple(items)


def _normalize_highlight_items(value: Any) -> tuple[ResearchHighlightItem, ...]:
    if not isinstance(value, list):
        return ()
    items: list[ResearchHighlightItem] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        label = _normalize_optional_string(item.get("label"))
        data_value = _normalize_optional_string(item.get("value"))
        if label is None or data_value is None:
            continue
        raw_tone = _normalize_optional_string(item.get("tone")) or "neutral"
        tone = cast(MetricTone, raw_tone) if raw_tone in {
            "neutral",
            "accent",
            "success",
            "warning",
            "danger",
        } else "neutral"
        items.append(
            ResearchHighlightItem(
                label=label,
                value=data_value,
                tone=tone,
                detail=_normalize_optional_string(item.get("detail")),
            )
        )
    return tuple(items)


def _normalize_table_highlight_items(value: Any) -> tuple[ResearchTableHighlightItem, ...]:
    if not isinstance(value, list):
        return ()
    items: list[ResearchTableHighlightItem] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        name = _normalize_optional_string(item.get("name"))
        label = _normalize_optional_string(item.get("label"))
        if name is None or label is None:
            continue
        items.append(
            ResearchTableHighlightItem(
                name=name,
                label=label,
                description=_normalize_optional_string(item.get("description")),
            )
        )
    return tuple(items)


def _extract_title(summary_markdown: str, info: ResearchBundleInfo) -> str:
    for line in summary_markdown.splitlines():
        stripped = line.strip()
        if stripped.startswith("# "):
            return stripped[2:].strip()
    last_segment = info.experiment_id.split("/")[-1]
    return " ".join(part.capitalize() for part in last_segment.split("-"))


def _extract_first_paragraph(summary_markdown: str) -> str | None:
    blocks = [
        block.strip()
        for block in summary_markdown.split("\n\n")
        if block.strip()
    ]
    for block in blocks:
        lines = [line.strip() for line in block.splitlines() if line.strip()]
        if not lines:
            continue
        if lines[0].startswith("#"):
            continue
        if all(line.startswith("- ") for line in lines):
            continue
        text = " ".join(lines)
        return text or None
    return None


def _extract_first_bullet(summary_markdown: str) -> str | None:
    for line in summary_markdown.splitlines():
        stripped = line.strip()
        if stripped.startswith("- "):
            bullet = stripped[2:].strip()
            return bullet or None
    return None


def _info_sort_key(info: ResearchBundleInfo) -> tuple[datetime, str]:
    return (_datetime_sort_key(info.created_at), info.run_id)


def _datetime_sort_key(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return datetime.min.replace(tzinfo=UTC)
