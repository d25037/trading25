from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import tomllib
from typing import Any, Literal, cast

from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    get_research_experiment_docs_readme_path,
    list_research_bundle_infos,
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
_DOCS_EXPERIMENTS_ROOT = _BT_PROJECT_ROOT / "docs" / "experiments"
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
class PublishedReadoutSectionData:
    title: str
    items: tuple[str, ...]


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
    readout_sections: tuple[PublishedReadoutSectionData, ...]
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
        if _load_docs_published_summary_for_experiment(info.experiment_id) is not None
    ]
    bundled_experiment_ids = set(latest_by_experiment)
    entries.extend(
        _build_docs_catalog_entry(experiment_id, readme_path)
        for experiment_id, readme_path in _list_research_docs_readmes().items()
        if experiment_id not in bundled_experiment_ids
        and _load_docs_published_summary_for_experiment(experiment_id) is not None
    )
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
        return _get_docs_research_publication(experiment_id, run_id=run_id)

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
    docs_summary = _load_docs_published_summary_with_markdown(selected_info.experiment_id)
    if docs_summary is None:
        raise FileNotFoundError(
            f"Research publication was not found: {selected_info.experiment_id}"
        )
    published_summary, summary_markdown = docs_summary

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
        result_metadata={**dict(selected_info.result_metadata), "source": "docs"},
    )


def _build_catalog_entry(info: ResearchBundleInfo) -> ResearchCatalogEntry:
    published_summary = _load_published_summary(info)
    if published_summary is None:
        raise FileNotFoundError(
            f"Research publication was not found: {info.experiment_id}"
        )
    metadata = _load_research_catalog_metadata().get(info.experiment_id, {})
    title = published_summary.title
    objective = published_summary.purpose
    headline = published_summary.result_headline
    tags = published_summary.tags
    metadata_promoted_surface = _normalize_optional_string(metadata.get("promotedSurface"))
    summary_promoted_surface = published_summary.promoted_surface
    promoted_surface = (
        metadata_promoted_surface
        or summary_promoted_surface
        or _derive_promoted_surface(info, tags)
    )
    metadata_status = _normalize_optional_status(metadata.get("status"))
    summary_status = published_summary.status
    status = metadata_status or summary_status or _derive_status(
        info,
        promoted_surface=promoted_surface,
    )
    risk_flags = _merge_unique_strings(
        _normalize_string_tuple(metadata.get("riskFlags")),
        published_summary.risk_flags,
        _derive_risk_flags(info, has_structured_summary=True),
    )
    metadata_family = _normalize_optional_string(metadata.get("family"))
    summary_family = published_summary.family
    family = metadata_family or summary_family or _derive_research_family(info, tags)
    metadata_decision = _normalize_optional_string(metadata.get("decision"))
    summary_decision = published_summary.decision
    decision = metadata_decision or summary_decision
    metadata_related_experiments = _normalize_string_tuple(metadata.get("relatedExperiments"))
    summary_related_experiments = published_summary.related_experiments
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
        has_structured_summary=True,
    )


def _build_docs_catalog_entry(experiment_id: str, readme_path: Path) -> ResearchCatalogEntry:
    summary_markdown = readme_path.read_text(encoding="utf-8")
    metadata = _load_research_catalog_metadata().get(experiment_id, {})
    published_summary = _load_markdown_published_summary(
        experiment_id,
        summary_markdown,
        metadata,
    )
    tags = (
        published_summary.tags
        if published_summary is not None
        else _normalize_string_tuple(metadata.get("tags"))
    )
    promoted_surface = (
        _normalize_optional_string(metadata.get("promotedSurface"))
        or (published_summary.promoted_surface if published_summary is not None else None)
        or _derive_promoted_surface_for_experiment(experiment_id, tags)
    )
    status = (
        _normalize_optional_status(metadata.get("status"))
        or (published_summary.status if published_summary is not None else None)
        or _derive_status_for_experiment(
            experiment_id,
            promoted_surface=promoted_surface,
        )
    )
    risk_flags = _merge_unique_strings(
        _normalize_string_tuple(metadata.get("riskFlags")),
        published_summary.risk_flags if published_summary is not None else (),
        () if published_summary is not None else ("needs-publication-summary",),
    )

    return ResearchCatalogEntry(
        experiment_id=experiment_id,
        run_id="docs",
        title=(
            published_summary.title
            if published_summary is not None
            else _normalize_optional_string(metadata.get("title"))
            or _extract_title_from_markdown(summary_markdown, experiment_id)
        ),
        objective=(
            published_summary.purpose
            if published_summary is not None
            else _extract_first_paragraph(summary_markdown)
        ),
        headline=(
            published_summary.result_headline
            if published_summary is not None
            else _extract_docs_headline(summary_markdown)
        ),
        family=_normalize_optional_string(metadata.get("family"))
        or (published_summary.family if published_summary is not None else None)
        or _derive_research_family_for_experiment(experiment_id, tags),
        status=status,
        decision=_normalize_optional_string(metadata.get("decision"))
        or (published_summary.decision if published_summary is not None else None),
        promoted_surface=promoted_surface,
        risk_flags=risk_flags,
        related_experiments=_normalize_string_tuple(metadata.get("relatedExperiments"))
        or (published_summary.related_experiments if published_summary is not None else ()),
        docs_readme_path=resolve_research_experiment_docs_readme_path(experiment_id),
        created_at=_latest_docs_modified_at(readme_path.parent),
        analysis_start_date=None,
        analysis_end_date=None,
        git_commit=None,
        tags=tags,
        has_structured_summary=published_summary is not None,
    )


def _get_docs_research_publication(
    experiment_id: str,
    *,
    run_id: str | None,
) -> ResearchPublication:
    if run_id not in (None, "docs"):
        raise FileNotFoundError(
            f"Research bundle run was not found: experiment={experiment_id} run_id={run_id}"
        )

    readme_path = get_research_experiment_docs_readme_path(experiment_id)
    if not readme_path.is_file():
        raise FileNotFoundError(f"Research bundle experiment was not found: {experiment_id}")

    entry = _build_docs_catalog_entry(experiment_id, readme_path)
    summary_markdown = readme_path.read_text(encoding="utf-8")
    published_summary = _load_markdown_published_summary(
        experiment_id,
        summary_markdown,
        _load_research_catalog_metadata().get(experiment_id, {}),
    )
    if published_summary is None:
        raise FileNotFoundError(f"Research publication was not found: {experiment_id}")
    return ResearchPublication(
        item=entry,
        summary=published_summary,
        summary_markdown=summary_markdown,
        output_tables=(),
        available_runs=(
            ResearchRunReference(
                run_id=entry.run_id,
                created_at=entry.created_at,
                is_latest=True,
            ),
        ),
        result_metadata={"source": "docs"},
    )


def _load_published_summary(
    info: ResearchBundleInfo,
) -> PublishedResearchSummaryData | None:
    return _load_docs_published_summary_for_experiment(info.experiment_id)


def _load_docs_published_summary_for_experiment(
    experiment_id: str,
) -> PublishedResearchSummaryData | None:
    loaded = _load_docs_published_summary_with_markdown(experiment_id)
    if loaded is None:
        return None
    summary, _markdown = loaded
    return summary


def _load_docs_published_summary_with_markdown(
    experiment_id: str,
) -> tuple[PublishedResearchSummaryData, str] | None:
    readme_path = get_research_experiment_docs_readme_path(experiment_id)
    if not readme_path.is_file():
        return None
    markdown = readme_path.read_text(encoding="utf-8")
    summary = _load_markdown_published_summary(
        experiment_id,
        markdown,
        _load_research_catalog_metadata().get(experiment_id, {}),
    )
    if summary is None:
        return None
    return summary, markdown


_PUBLISHED_READOUT_HEADING = "published readout"
_PUBLISHED_READOUT_REQUIRED_SECTIONS = {
    "decision",
    "main findings",
    "interpretation",
    "production implication",
    "caveats",
    "source artifacts",
}
_PUBLISHED_READOUT_SECTION_TITLES = {
    "decision": "Decision",
    "why this research was run": "Why This Research Was Run",
    "data scope / pit assumptions": "Data Scope / PIT Assumptions",
    "main findings": "Main Findings",
    "interpretation": "Interpretation",
    "production implication": "Production Implication",
    "caveats": "Caveats",
    "source artifacts": "Source Artifacts",
}
_PUBLISHED_READOUT_SECTION_ORDER = (
    "decision",
    "why this research was run",
    "data scope / pit assumptions",
    "main findings",
    "interpretation",
    "production implication",
    "caveats",
    "source artifacts",
)


def _load_markdown_published_summary(
    experiment_id: str,
    markdown: str,
    metadata: dict[str, Any],
) -> PublishedResearchSummaryData | None:
    readout_sections = _extract_published_readout_sections(markdown)
    if not _has_complete_published_readout(readout_sections):
        return None

    tags = _normalize_string_tuple(metadata.get("tags"))
    family = _normalize_optional_string(metadata.get("family"))
    promoted_surface = _normalize_optional_string(metadata.get("promotedSurface"))
    status = _normalize_optional_status(metadata.get("status")) or _derive_status_for_experiment(
        experiment_id,
        promoted_surface=promoted_surface,
    )
    decision = _normalize_optional_string(metadata.get("decision")) or _first_readout_item(
        readout_sections,
        "decision",
    )
    purpose_items = _merge_unique_strings(
        tuple(readout_sections.get("why this research was run", ())),
    )
    method = _merge_unique_strings(
        tuple(readout_sections.get("data scope / pit assumptions", ())),
    )
    canonical_readout_sections = _to_canonical_readout_sections(readout_sections)

    return PublishedResearchSummaryData(
        title=_normalize_optional_string(metadata.get("title"))
        or _extract_title_from_markdown(markdown, experiment_id),
        tags=tags,
        family=family,
        status=status,
        decision=decision,
        promoted_surface=promoted_surface,
        risk_flags=_normalize_string_tuple(metadata.get("riskFlags")),
        related_experiments=_normalize_string_tuple(metadata.get("relatedExperiments")),
        purpose=" ".join(purpose_items) or _extract_first_paragraph(markdown) or decision or "Published research.",
        method=method,
        result_headline=decision or _first_readout_item(readout_sections, "main findings"),
        readout_sections=canonical_readout_sections,
        selected_parameters=(),
        highlights=(),
        table_highlights=(),
    )


def _extract_published_readout_sections(markdown: str) -> dict[str, tuple[str, ...]]:
    sections: dict[str, list[str]] = {}
    in_readout = False
    current_section: str | None = None

    for raw_line in markdown.splitlines():
        stripped = raw_line.strip()
        if not stripped:
            continue
        if stripped.startswith("## "):
            heading = _normalize_heading_key(stripped[3:])
            in_readout = heading == _PUBLISHED_READOUT_HEADING
            current_section = None
            continue
        if not in_readout:
            continue
        if stripped.startswith("### "):
            current_section = _normalize_heading_key(stripped[4:])
            sections.setdefault(current_section, [])
            continue
        if current_section is None:
            continue

        item = _normalize_readout_line(stripped)
        if item is not None:
            sections[current_section].append(item)

    return {key: tuple(value) for key, value in sections.items()}


def _normalize_readout_line(value: str) -> str | None:
    stripped = value.strip()
    if not stripped:
        return None
    if stripped.startswith("- "):
        stripped = stripped[2:].strip()
    return stripped or None


def _to_canonical_readout_sections(
    sections: dict[str, tuple[str, ...]],
) -> tuple[PublishedReadoutSectionData, ...]:
    result: list[PublishedReadoutSectionData] = []
    seen_titles: set[str] = set()
    for key in _PUBLISHED_READOUT_SECTION_ORDER:
        items = _section_items(sections, key)
        if not items:
            continue
        title = _PUBLISHED_READOUT_SECTION_TITLES[key]
        if title in seen_titles:
            continue
        result.append(PublishedReadoutSectionData(title=title, items=items))
        seen_titles.add(title)
    return tuple(result)


def _section_items(
    sections: dict[str, tuple[str, ...]],
    key: str,
) -> tuple[str, ...]:
    return tuple(sections.get(key, ()))


def _has_complete_published_readout(sections: dict[str, tuple[str, ...]]) -> bool:
    return all(sections.get(section) for section in _PUBLISHED_READOUT_REQUIRED_SECTIONS)


def _first_readout_item(
    sections: dict[str, tuple[str, ...]],
    section_name: str,
) -> str | None:
    items = sections.get(section_name)
    if not items:
        return None
    return items[0]


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


def _normalize_string_tuple(value: Any) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    items: list[str] = []
    for item in value:
        normalized = _normalize_optional_string(item)
        if normalized is not None:
            items.append(normalized)
    return tuple(items)


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
    return _derive_research_family_for_experiment(info.experiment_id, tags)


def _derive_research_family_for_experiment(
    experiment_id: str,
    tags: tuple[str, ...],
) -> str:
    normalized_experiment_id = experiment_id.lower()
    tag_text = " ".join(tags).lower()
    haystack = f"{normalized_experiment_id} {tag_text}"

    if normalized_experiment_id.startswith("strategy-audit/"):
        return "Strategy Audit"
    if "speculative-volume-surge" in haystack:
        return "Speculative Volume Surge"
    if "falling-knife" in haystack:
        return "Falling Knife"
    if "stop-limit" in haystack:
        return "JPX Stop Limit"
    if "annual-" in normalized_experiment_id and (
        "fundamental" in haystack or "value" in haystack or "forward-per" in haystack
    ):
        return "Annual Fundamentals"
    if "topix100" in haystack and (
        "sma" in haystack or "q10" in haystack or "bounce" in haystack or "streak" in haystack
    ):
        return "TOPIX100 Regime"
    if "topix" in haystack or "nt-ratio" in haystack:
        return "Market Regime"

    return experiment_id.split("/", maxsplit=1)[0].replace("-", " ").title()


def _derive_promoted_surface(
    info: ResearchBundleInfo,
    tags: tuple[str, ...],
) -> str | None:
    return _derive_promoted_surface_for_experiment(info.experiment_id, tags)


def _derive_promoted_surface_for_experiment(
    experiment_id: str,
    tags: tuple[str, ...],
) -> str | None:
    normalized_experiment_id = experiment_id.lower()
    tag_text = " ".join(tags).lower()
    if "annual-value-composite-selection" in normalized_experiment_id:
        return "Ranking"
    if "strategy-audit" in normalized_experiment_id or "production" in tag_text:
        return "Strategy"
    return None


def _derive_status(
    info: ResearchBundleInfo,
    *,
    promoted_surface: str | None,
) -> ResearchDecisionStatus:
    return _derive_status_for_experiment(
        info.experiment_id,
        promoted_surface=promoted_surface,
    )


def _derive_status_for_experiment(
    experiment_id: str,
    *,
    promoted_surface: str | None,
) -> ResearchDecisionStatus:
    if promoted_surface == "Ranking":
        return "ranking_surface"
    if promoted_surface == "Strategy":
        return "strategy_draft"
    if experiment_id.startswith("strategy-audit/"):
        return "candidate"
    return "observed"


def _derive_risk_flags(
    info: ResearchBundleInfo,
    *,
    has_structured_summary: bool,
) -> tuple[str, ...]:
    flags: list[str] = []
    if not has_structured_summary:
        flags.append("needs-publication-summary")
    if resolve_research_experiment_docs_readme_path(info.experiment_id) is None:
        flags.append("docs-missing")
    return tuple(flags)


def _extract_title_from_markdown(summary_markdown: str, experiment_id: str) -> str:
    for line in summary_markdown.splitlines():
        stripped = line.strip()
        if stripped.startswith("# "):
            return stripped[2:].strip()
    last_segment = experiment_id.split("/")[-1]
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


def _extract_docs_headline(summary_markdown: str) -> str | None:
    current_section: str | None = None
    preferred_headline: str | None = None
    fallback_headline: str | None = None

    for line in summary_markdown.splitlines():
        stripped = line.strip()
        if stripped.startswith("## "):
            current_section = _normalize_heading_key(stripped[3:])
            continue
        if not stripped.startswith("- "):
            continue

        bullet = stripped[2:].strip()
        if not _is_informative_docs_bullet(bullet):
            continue
        if fallback_headline is None:
            fallback_headline = bullet
        if current_section in {"current findings", "current read", "findings", "results"}:
            preferred_headline = bullet
            break

    return preferred_headline or fallback_headline


def _normalize_heading_key(value: str) -> str:
    return value.strip().lower()


def _is_informative_docs_bullet(value: str) -> bool:
    stripped = value.strip()
    if not stripped:
        return False
    lowered = stripped.lower()
    if stripped.endswith(":"):
        return False
    if lowered.startswith("baseline result:"):
        return False
    if lowered in {"manifest.json", "results.duckdb", "summary.md"}:
        return False
    if "/" in stripped and stripped.startswith("`apps/"):
        return False
    return True


def _info_sort_key(info: ResearchBundleInfo) -> tuple[datetime, str]:
    return (_datetime_sort_key(info.created_at), info.run_id)


def _datetime_sort_key(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return datetime.min.replace(tzinfo=UTC)


def _list_research_docs_readmes() -> dict[str, Path]:
    if not _DOCS_EXPERIMENTS_ROOT.exists():
        return {}

    readmes: dict[str, Path] = {}
    for readme_path in sorted(_DOCS_EXPERIMENTS_ROOT.glob("*/*/README.md")):
        experiment_id = readme_path.parent.relative_to(_DOCS_EXPERIMENTS_ROOT).as_posix()
        readmes[experiment_id] = readme_path
    return readmes


def _latest_docs_modified_at(experiment_dir: Path) -> str:
    latest_mtime = experiment_dir.stat().st_mtime
    for file_path in experiment_dir.rglob("*"):
        if file_path.is_file():
            latest_mtime = max(latest_mtime, file_path.stat().st_mtime)
    return datetime.fromtimestamp(latest_mtime, tz=UTC).isoformat()
