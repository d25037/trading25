"""
TOPIX100 13:30 entry to next-session 10:45 exit conditioning research.

This follow-up study segments the 13:30 -> next-session 10:45 pattern by:

- market regime at the 13:30 entry snapshot
- sector_33_name
- previous-session 10:45 winner/loser state
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pandas as pd

from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_research_bundle_info,
    load_research_bundle_tables,
    write_bundle_artifact,
    write_research_bundle,
)
from src.domains.analytics.topix100_1330_entry_next_1045_exit import (
    DEFAULT_ENTRY_TIME,
    DEFAULT_EXIT_TIME,
    DEFAULT_INTERVAL_MINUTES,
    DEFAULT_TAIL_FRACTION,
    run_topix100_1330_entry_next_1045_exit_research,
)
from src.domains.analytics.topix100_1330_entry_next_1045_exit_conditioning_support import (
    _CURRENT_ENTRY_BUCKET_ORDER,
    _PREV_DAY_PEAK_ORDER,
    _build_current_entry_bucket_label_map,
    _build_enriched_session_level_df,
    _build_prev_day_peak_label_map,
    _build_prev_day_peak_transition_df,
    _build_regime_market_df,
    _build_segment_comparison_df,
    _build_segment_group_summary_df,
    _empty_segment_comparison_df,
    _sort_prev_day_peak_transition_df,
    _sort_segment_comparison_df,
    _sort_segment_group_summary_df,
)
from src.domains.analytics.topix100_open_relative_intraday_path import (
    SourceMode,
    TOPIX100_SCALE_CATEGORIES,
    _import_matplotlib_pyplot,
    _normalize_code_sql,
    _open_analysis_connection,
)
from src.domains.analytics.topix100_peak_winner_loser_intraday_path import (
    run_topix100_peak_winner_loser_intraday_path_research,
)

TOPIX100_1330_ENTRY_NEXT_1045_EXIT_CONDITIONING_EXPERIMENT_ID = (
    "market-behavior/topix100-1330-entry-next-1045-exit-conditioning"
)
TOPIX100_1330_ENTRY_NEXT_1045_EXIT_CONDITIONING_PLOT_FILENAME = (
    "topix100_1330_entry_next_1045_exit_conditioning_overview.png"
)
DEFAULT_PREV_DAY_PEAK_TIME = "10:45"


@dataclass(frozen=True)
class Topix1001330EntryNext1045ExitConditioningResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    interval_minutes: int
    entry_time: str
    exit_time: str
    tail_fraction: float
    prev_day_peak_time: str
    topix100_constituent_count: int
    total_entry_session_count: int
    eligible_session_count: int
    regime_day_count: int
    enriched_session_level_df: pd.DataFrame
    regime_market_df: pd.DataFrame
    regime_group_summary_df: pd.DataFrame
    regime_comparison_df: pd.DataFrame
    sector_group_summary_df: pd.DataFrame
    sector_comparison_df: pd.DataFrame
    prev_day_peak_group_summary_df: pd.DataFrame
    prev_day_peak_comparison_df: pd.DataFrame
    prev_day_peak_transition_df: pd.DataFrame

def _validate_time_label(value: str, *, argument_name: str) -> str:
    normalized = str(value).strip()
    if len(normalized) != 5 or normalized[2] != ":":
        raise ValueError(f"{argument_name} must be formatted as HH:MM")
    return normalized


def _fetch_topix100_sector_metadata_df(db_path: str) -> pd.DataFrame:
    normalized_code_sql = _normalize_code_sql("code")
    with _open_analysis_connection(db_path) as ctx:
        metadata_df = cast(
            pd.DataFrame,
            ctx.connection.execute(
                f"""
                WITH topix100_stocks AS (
                    SELECT
                        normalized_code,
                        company_name,
                        coalesce(scale_category, '') AS scale_category,
                        coalesce(sector_17_name, '') AS sector_17_name,
                        coalesce(sector_33_name, '') AS sector_33_name
                    FROM (
                        SELECT
                            {normalized_code_sql} AS normalized_code,
                            company_name,
                            scale_category,
                            sector_17_name,
                            sector_33_name,
                            ROW_NUMBER() OVER (
                                PARTITION BY {normalized_code_sql}
                                ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END, code
                            ) AS row_priority
                        FROM stocks
                        WHERE coalesce(scale_category, '') IN {cast(Any, TOPIX100_SCALE_CATEGORIES)}
                    ) stock_candidates
                    WHERE row_priority = 1
                )
                SELECT
                    normalized_code AS code,
                    company_name,
                    scale_category,
                    nullif(sector_17_name, '') AS sector_17_name,
                    nullif(sector_33_name, '') AS sector_33_name
                FROM topix100_stocks
                ORDER BY normalized_code
                """
            ).fetchdf(),
        )
    if metadata_df.empty:
        return pd.DataFrame(
            columns=["code", "company_name", "scale_category", "sector_17_name", "sector_33_name"]
        )

    metadata_df = metadata_df.copy()
    metadata_df["code"] = metadata_df["code"].astype(str)
    metadata_df["company_name"] = metadata_df["company_name"].astype(str)
    metadata_df["scale_category"] = metadata_df["scale_category"].astype(str)
    metadata_df["sector_17_name"] = metadata_df["sector_17_name"].fillna("Unknown").astype(str)
    metadata_df["sector_33_name"] = metadata_df["sector_33_name"].fillna("Unknown").astype(str)
    return metadata_df

def run_topix100_1330_entry_next_1045_exit_conditioning_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    interval_minutes: int = DEFAULT_INTERVAL_MINUTES,
    entry_time: str = DEFAULT_ENTRY_TIME,
    exit_time: str = DEFAULT_EXIT_TIME,
    tail_fraction: float = DEFAULT_TAIL_FRACTION,
    prev_day_peak_time: str = DEFAULT_PREV_DAY_PEAK_TIME,
) -> Topix1001330EntryNext1045ExitConditioningResult:
    validated_prev_day_peak_time = _validate_time_label(
        prev_day_peak_time,
        argument_name="prev_day_peak_time",
    )

    overnight_result = run_topix100_1330_entry_next_1045_exit_research(
        db_path,
        start_date=start_date,
        end_date=end_date,
        interval_minutes=interval_minutes,
        entry_time=entry_time,
        exit_time=exit_time,
        tail_fraction=tail_fraction,
    )
    peak_result = run_topix100_peak_winner_loser_intraday_path_research(
        db_path,
        start_date=start_date,
        end_date=end_date,
        interval_minutes=interval_minutes,
        anchor_candidate_times=(validated_prev_day_peak_time,),
        midday_reference_time="13:30",
        tail_fraction=tail_fraction,
    )
    sector_metadata_df = _fetch_topix100_sector_metadata_df(db_path)

    enriched_session_level_df = _build_enriched_session_level_df(
        overnight_result,
        peak_result.session_level_df,
        sector_metadata_df,
        prev_day_peak_time=validated_prev_day_peak_time,
    )
    eligible_df = enriched_session_level_df.loc[
        enriched_session_level_df["prev_close_to_entry_return"].notna()
        & enriched_session_level_df["entry_to_next_exit_return"].notna()
    ].copy()
    if eligible_df.empty:
        raise ValueError("No eligible TOPIX100 13:30 -> next 10:45 sessions were available.")

    regime_market_df = _build_regime_market_df(eligible_df)
    regime_order = {value: index for index, value in enumerate(("weak", "neutral", "strong"))}
    regime_group_summary_df = _sort_segment_group_summary_df(
        _build_segment_group_summary_df(
            eligible_df,
            segment_key_column="market_regime_bucket_key",
            segment_label_column="market_regime_bucket_label",
        ),
        segment_order=regime_order,
    )
    regime_comparison_df = _sort_segment_comparison_df(
        _build_segment_comparison_df(
            eligible_df,
            segment_key_column="market_regime_bucket_key",
            segment_label_column="market_regime_bucket_label",
        ),
        segment_order=regime_order,
    )

    sector_group_summary_df = _build_segment_group_summary_df(
        eligible_df,
        segment_key_column="sector_33_name",
        segment_label_column="sector_33_name",
    )
    if not sector_group_summary_df.empty:
        sector_all_counts = sector_group_summary_df.loc[
            sector_group_summary_df["group_label"] == "all",
            ["segment_key", "segment_sample_count", "mean_entry_to_next_exit_return"],
        ].copy()
        sector_all_counts = sector_all_counts.sort_values(
            ["segment_sample_count", "mean_entry_to_next_exit_return", "segment_key"],
            ascending=[False, False, True],
            kind="stable",
        ).reset_index(drop=True)
        sector_order = {
            str(row.segment_key): index
            for index, row in enumerate(sector_all_counts.itertuples(index=False))
        }
        sector_group_summary_df = _sort_segment_group_summary_df(
            sector_group_summary_df,
            segment_order=sector_order,
        )
        sector_comparison_df = _sort_segment_comparison_df(
            _build_segment_comparison_df(
                eligible_df,
                segment_key_column="sector_33_name",
                segment_label_column="sector_33_name",
            ),
            segment_order=sector_order,
        )
    else:
        sector_comparison_df = _empty_segment_comparison_df()

    prev_day_peak_group_summary_df = _sort_segment_group_summary_df(
        _build_segment_group_summary_df(
            eligible_df,
            segment_key_column="prev_day_peak_group_key",
            segment_label_column="prev_day_peak_group_label",
        ),
        segment_order={value: index for index, value in enumerate(_PREV_DAY_PEAK_ORDER)},
    )
    prev_day_peak_comparison_df = _sort_segment_comparison_df(
        _build_segment_comparison_df(
            eligible_df,
            segment_key_column="prev_day_peak_group_key",
            segment_label_column="prev_day_peak_group_label",
        ),
        segment_order={value: index for index, value in enumerate(_PREV_DAY_PEAK_ORDER)},
    )
    prev_day_peak_transition_df = _sort_prev_day_peak_transition_df(
        _build_prev_day_peak_transition_df(eligible_df)
    )

    return Topix1001330EntryNext1045ExitConditioningResult(
        db_path=overnight_result.db_path,
        source_mode=overnight_result.source_mode,
        source_detail=overnight_result.source_detail,
        available_start_date=overnight_result.available_start_date,
        available_end_date=overnight_result.available_end_date,
        analysis_start_date=overnight_result.analysis_start_date,
        analysis_end_date=overnight_result.analysis_end_date,
        interval_minutes=overnight_result.interval_minutes,
        entry_time=overnight_result.entry_time,
        exit_time=overnight_result.exit_time,
        tail_fraction=overnight_result.tail_fraction,
        prev_day_peak_time=validated_prev_day_peak_time,
        topix100_constituent_count=overnight_result.topix100_constituent_count,
        total_entry_session_count=overnight_result.total_entry_session_count,
        eligible_session_count=overnight_result.eligible_session_count,
        regime_day_count=int(regime_market_df["entry_date"].nunique()),
        enriched_session_level_df=enriched_session_level_df,
        regime_market_df=regime_market_df,
        regime_group_summary_df=regime_group_summary_df,
        regime_comparison_df=regime_comparison_df,
        sector_group_summary_df=sector_group_summary_df,
        sector_comparison_df=sector_comparison_df,
        prev_day_peak_group_summary_df=prev_day_peak_group_summary_df,
        prev_day_peak_comparison_df=prev_day_peak_comparison_df,
        prev_day_peak_transition_df=prev_day_peak_transition_df,
    )


def _split_result_payload(
    result: Topix1001330EntryNext1045ExitConditioningResult,
) -> tuple[dict[str, Any], dict[str, pd.DataFrame]]:
    return (
        {
            "db_path": result.db_path,
            "source_mode": result.source_mode,
            "source_detail": result.source_detail,
            "available_start_date": result.available_start_date,
            "available_end_date": result.available_end_date,
            "analysis_start_date": result.analysis_start_date,
            "analysis_end_date": result.analysis_end_date,
            "interval_minutes": result.interval_minutes,
            "entry_time": result.entry_time,
            "exit_time": result.exit_time,
            "tail_fraction": result.tail_fraction,
            "prev_day_peak_time": result.prev_day_peak_time,
            "topix100_constituent_count": result.topix100_constituent_count,
            "total_entry_session_count": result.total_entry_session_count,
            "eligible_session_count": result.eligible_session_count,
            "regime_day_count": result.regime_day_count,
        },
        {
            "enriched_session_level_df": result.enriched_session_level_df,
            "regime_market_df": result.regime_market_df,
            "regime_group_summary_df": result.regime_group_summary_df,
            "regime_comparison_df": result.regime_comparison_df,
            "sector_group_summary_df": result.sector_group_summary_df,
            "sector_comparison_df": result.sector_comparison_df,
            "prev_day_peak_group_summary_df": result.prev_day_peak_group_summary_df,
            "prev_day_peak_comparison_df": result.prev_day_peak_comparison_df,
            "prev_day_peak_transition_df": result.prev_day_peak_transition_df,
        },
    )


def _build_result_from_payload(
    metadata: dict[str, Any],
    tables: dict[str, pd.DataFrame],
) -> Topix1001330EntryNext1045ExitConditioningResult:
    return Topix1001330EntryNext1045ExitConditioningResult(
        db_path=str(metadata["db_path"]),
        source_mode=cast(SourceMode, metadata["source_mode"]),
        source_detail=str(metadata["source_detail"]),
        available_start_date=cast(str | None, metadata.get("available_start_date")),
        available_end_date=cast(str | None, metadata.get("available_end_date")),
        analysis_start_date=cast(str | None, metadata.get("analysis_start_date")),
        analysis_end_date=cast(str | None, metadata.get("analysis_end_date")),
        interval_minutes=int(metadata["interval_minutes"]),
        entry_time=str(metadata["entry_time"]),
        exit_time=str(metadata["exit_time"]),
        tail_fraction=float(metadata["tail_fraction"]),
        prev_day_peak_time=str(metadata["prev_day_peak_time"]),
        topix100_constituent_count=int(metadata["topix100_constituent_count"]),
        total_entry_session_count=int(metadata["total_entry_session_count"]),
        eligible_session_count=int(metadata["eligible_session_count"]),
        regime_day_count=int(metadata["regime_day_count"]),
        enriched_session_level_df=tables["enriched_session_level_df"],
        regime_market_df=tables["regime_market_df"],
        regime_group_summary_df=tables["regime_group_summary_df"],
        regime_comparison_df=tables["regime_comparison_df"],
        sector_group_summary_df=tables["sector_group_summary_df"],
        sector_comparison_df=tables["sector_comparison_df"],
        prev_day_peak_group_summary_df=tables["prev_day_peak_group_summary_df"],
        prev_day_peak_comparison_df=tables["prev_day_peak_comparison_df"],
        prev_day_peak_transition_df=tables["prev_day_peak_transition_df"],
    )


def _build_published_summary(
    result: Topix1001330EntryNext1045ExitConditioningResult,
) -> dict[str, Any]:
    return {
        "intervalMinutes": result.interval_minutes,
        "entryTime": result.entry_time,
        "exitTime": result.exit_time,
        "tailFraction": result.tail_fraction,
        "prevDayPeakTime": result.prev_day_peak_time,
        "analysisStartDate": result.analysis_start_date,
        "analysisEndDate": result.analysis_end_date,
        "regimeSummary": result.regime_comparison_df.to_dict(orient="records"),
        "sectorSummary": result.sector_comparison_df.to_dict(orient="records"),
        "prevDayPeakSummary": result.prev_day_peak_comparison_df.to_dict(orient="records"),
    }


def _format_pct(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"{float(value) * 100:+.4f}%"


def _build_research_bundle_summary_markdown(
    result: Topix1001330EntryNext1045ExitConditioningResult,
) -> str:
    summary_lines = [
        "# TOPIX100 13:30 Entry -> Next 10:45 Exit Conditioning",
        "",
        "## Snapshot",
        "",
        f"- Source mode: `{result.source_mode}`",
        f"- Available range: `{result.available_start_date} -> {result.available_end_date}`",
        f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
        f"- Interval minutes: `{result.interval_minutes}`",
        f"- Entry time: `{result.entry_time}`",
        f"- Exit time: `{result.exit_time}`",
        f"- Previous-day peak anchor: `{result.prev_day_peak_time}`",
        f"- Tail fraction per side: `{result.tail_fraction * 100:.1f}%`",
        f"- Current TOPIX100 constituents: `{result.topix100_constituent_count}`",
        f"- Eligible entry sessions: `{result.eligible_session_count}`",
        f"- Regime day count: `{result.regime_day_count}`",
        "",
        "## Current Read",
        "",
    ]

    if result.regime_comparison_df.empty:
        summary_lines.append("- Regime comparison rows were empty.")
    else:
        for row in result.regime_comparison_df.itertuples(index=False):
            summary_lines.append(
                f"- `{row.segment_label}`: winners-minus-losers "
                f"`{result.entry_time} -> D+1 {result.exit_time}` spread "
                f"`{_format_pct(cast(float | None, row.entry_to_next_exit_mean_spread))}` "
                f"(p=`{row.entry_to_next_exit_welch_p_value}`)."
            )

    if not result.sector_comparison_df.empty:
        best_sector = result.sector_comparison_df.sort_values(
            ["entry_to_next_exit_mean_spread", "segment_label"],
            ascending=[False, True],
            kind="stable",
        ).iloc[0]
        weakest_sector = result.sector_comparison_df.sort_values(
            ["entry_to_next_exit_mean_spread", "segment_label"],
            ascending=[True, True],
            kind="stable",
        ).iloc[0]
        summary_lines.extend(
            [
                "",
                "## Sector Read",
                "",
                (
                    f"- Largest positive winners-minus-losers sector spread: "
                    f"`{best_sector['segment_label']}` "
                    f"`{_format_pct(float(best_sector['entry_to_next_exit_mean_spread']))}`."
                ),
                (
                    f"- Largest negative winners-minus-losers sector spread: "
                    f"`{weakest_sector['segment_label']}` "
                    f"`{_format_pct(float(weakest_sector['entry_to_next_exit_mean_spread']))}`."
                ),
            ]
        )

    if not result.prev_day_peak_transition_df.empty:
        strongest_transition = result.prev_day_peak_transition_df.sort_values(
            ["mean_entry_to_next_exit_return", "sample_count"],
            ascending=[False, False],
            kind="stable",
        ).iloc[0]
        weakest_transition = result.prev_day_peak_transition_df.sort_values(
            ["mean_entry_to_next_exit_return", "sample_count"],
            ascending=[True, False],
            kind="stable",
        ).iloc[0]
        summary_lines.extend(
            [
                "",
                "## Previous-Day 10:45 Cross",
                "",
                (
                    f"- Strongest cell: `{strongest_transition['prev_day_peak_group_label']}` x "
                    f"`{strongest_transition['current_entry_bucket_label']}` "
                    f"`{_format_pct(float(strongest_transition['mean_entry_to_next_exit_return']))}` "
                    f"({int(strongest_transition['sample_count'])} sessions)."
                ),
                (
                    f"- Weakest cell: `{weakest_transition['prev_day_peak_group_label']}` x "
                    f"`{weakest_transition['current_entry_bucket_label']}` "
                    f"`{_format_pct(float(weakest_transition['mean_entry_to_next_exit_return']))}` "
                    f"({int(weakest_transition['sample_count'])} sessions)."
                ),
            ]
        )

    summary_lines.extend(
        [
            "",
            "## Artifact Plots",
            "",
            f"- `{TOPIX100_1330_ENTRY_NEXT_1045_EXIT_CONDITIONING_PLOT_FILENAME}`",
            "",
            "## Artifact Tables",
            "",
            *[
                f"- `{table_name}`"
                for table_name in _split_result_payload(result)[1].keys()
            ],
        ]
    )
    return "\n".join(summary_lines)


def _plot_grouped_regime_bars(axis: Any, regime_group_summary_df: pd.DataFrame) -> None:
    if regime_group_summary_df.empty:
        axis.text(0.5, 0.5, "No regime data", ha="center", va="center", transform=axis.transAxes)
        return
    color_map = {
        "all": "#6b7280",
        "winners": "#2563eb",
        "losers": "#dc2626",
    }
    group_order = ("all", "winners", "losers")
    regime_rows = regime_group_summary_df.loc[
        regime_group_summary_df["group_label"] == "all",
        ["segment_key", "segment_label"],
    ].copy()
    regime_rows = regime_rows.drop_duplicates().reset_index(drop=True)
    x_positions = list(range(len(regime_rows)))
    width = 0.24
    for group_index, group_label in enumerate(group_order):
        group_df = regime_group_summary_df.loc[
            regime_group_summary_df["group_label"] == group_label
        ].copy()
        heights = []
        for row in regime_rows.itertuples(index=False):
            scoped = group_df.loc[group_df["segment_key"] == row.segment_key]
            heights.append(
                float(scoped["mean_entry_to_next_exit_return"].iloc[0]) * 100.0
                if not scoped.empty
                else 0.0
            )
        shifted_positions = [
            position + (group_index - 1) * width for position in x_positions
        ]
        axis.bar(
            shifted_positions,
            heights,
            width=width,
            color=color_map[group_label],
            label=group_label.title(),
        )
    axis.axhline(0.0, color="#111827", linewidth=0.8, alpha=0.8)
    axis.set_xticks(x_positions)
    axis.set_xticklabels(regime_rows["segment_label"], fontsize=8)
    axis.set_ylabel("Mean return (%)")
    axis.set_title("Market regime split", fontsize=10)
    axis.grid(axis="y", alpha=0.2, linewidth=0.6)
    axis.legend(loc="best", frameon=False, fontsize=8)


def _plot_heatmap(
    axis: Any,
    *,
    pivot_df: pd.DataFrame,
    title: str,
    x_label: str,
    y_label: str,
    figure: Any,
) -> None:
    if pivot_df.empty:
        axis.text(0.5, 0.5, "No data", ha="center", va="center", transform=axis.transAxes)
        axis.set_title(title, fontsize=10)
        return
    image = axis.imshow(pivot_df.to_numpy(dtype=float), aspect="auto", cmap="coolwarm")
    axis.set_xticks(range(len(pivot_df.columns)))
    axis.set_xticklabels(list(pivot_df.columns), fontsize=8)
    axis.set_yticks(range(len(pivot_df.index)))
    axis.set_yticklabels(list(pivot_df.index), fontsize=7)
    axis.set_xlabel(x_label)
    axis.set_ylabel(y_label)
    axis.set_title(title, fontsize=10)
    figure.colorbar(image, ax=axis, fraction=0.025, pad=0.02, label="Mean return (%)")


def write_topix100_1330_entry_next_1045_exit_conditioning_overview_plot(
    result: Topix1001330EntryNext1045ExitConditioningResult,
    *,
    output_path: str | Path,
) -> Path:
    plt = _import_matplotlib_pyplot()
    try:
        from matplotlib import font_manager

        preferred_fonts = (
            "Hiragino Sans",
            "Yu Gothic",
            "Meiryo",
            "Noto Sans CJK JP",
        )
        available_fonts = {entry.name for entry in font_manager.fontManager.ttflist}
        configured_fonts = [
            font_name for font_name in preferred_fonts if font_name in available_fonts
        ]
        plt.rcParams["font.family"] = configured_fonts or ["sans-serif"]
    except Exception:
        plt.rcParams["font.family"] = ["sans-serif"]
    output_path = Path(output_path).expanduser()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fig, axes = plt.subplots(3, 1, figsize=(14, 14), constrained_layout=True)
    _plot_grouped_regime_bars(axes[0], result.regime_group_summary_df)

    sector_pivot_df = pd.DataFrame()
    if not result.sector_group_summary_df.empty:
        sector_plot_df = result.sector_group_summary_df.loc[
            result.sector_group_summary_df["group_label"].isin(("all", "winners", "losers"))
        ].copy()
        sector_pivot_df = (
            sector_plot_df.pivot(
                index="segment_label",
                columns="group_label",
                values="mean_entry_to_next_exit_return",
            )
            * 100.0
        )
        desired_columns = [value for value in ("all", "winners", "losers") if value in sector_pivot_df.columns]
        sector_pivot_df = sector_pivot_df.loc[:, desired_columns]
    _plot_heatmap(
        axes[1],
        pivot_df=sector_pivot_df,
        title="Sector split",
        x_label="Current 13:30 group",
        y_label="Sector 33",
        figure=fig,
    )

    transition_pivot_df = pd.DataFrame()
    if not result.prev_day_peak_transition_df.empty:
        current_entry_label_map = _build_current_entry_bucket_label_map(
            result.tail_fraction
        )
        prev_day_peak_label_map = _build_prev_day_peak_label_map(
            tail_fraction=result.tail_fraction,
            prev_day_peak_time=result.prev_day_peak_time,
        )
        transition_pivot_df = (
            result.prev_day_peak_transition_df.pivot(
                index="prev_day_peak_group_label",
                columns="current_entry_bucket_label",
                values="mean_entry_to_next_exit_return",
            )
            * 100.0
        )
        desired_columns = [
            current_entry_label_map[key]
            for key in _CURRENT_ENTRY_BUCKET_ORDER
            if current_entry_label_map[key] in transition_pivot_df.columns
        ]
        desired_index = [
            prev_day_peak_label_map[key]
            for key in _PREV_DAY_PEAK_ORDER
            if prev_day_peak_label_map[key] in transition_pivot_df.index
        ]
        transition_pivot_df = transition_pivot_df.loc[desired_index, desired_columns]
    _plot_heatmap(
        axes[2],
        pivot_df=transition_pivot_df,
        title="Previous-day 10:45 cross",
        x_label="Current 13:30 group",
        y_label="Previous-day 10:45 group",
        figure=fig,
    )

    fig.suptitle(
        f"TOPIX100 {result.entry_time} -> next-session {result.exit_time} conditioning",
        fontsize=12,
    )
    fig.savefig(output_path, dpi=160, bbox_inches="tight")
    plt.close(fig)
    return output_path


def write_topix100_1330_entry_next_1045_exit_conditioning_research_bundle(
    result: Topix1001330EntryNext1045ExitConditioningResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    metadata, tables = _split_result_payload(result)
    bundle = write_research_bundle(
        experiment_id=TOPIX100_1330_ENTRY_NEXT_1045_EXIT_CONDITIONING_EXPERIMENT_ID,
        module=__name__,
        function="run_topix100_1330_entry_next_1045_exit_conditioning_research",
        params={
            "start_date": result.analysis_start_date,
            "end_date": result.analysis_end_date,
            "interval_minutes": result.interval_minutes,
            "entry_time": result.entry_time,
            "exit_time": result.exit_time,
            "tail_fraction": result.tail_fraction,
            "prev_day_peak_time": result.prev_day_peak_time,
        },
        db_path=result.db_path,
        analysis_start_date=result.analysis_start_date,
        analysis_end_date=result.analysis_end_date,
        result_metadata=metadata,
        result_tables=tables,
        summary_markdown=_build_research_bundle_summary_markdown(result),
        published_summary=_build_published_summary(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )
    write_bundle_artifact(
        bundle,
        TOPIX100_1330_ENTRY_NEXT_1045_EXIT_CONDITIONING_PLOT_FILENAME,
        lambda output_path: write_topix100_1330_entry_next_1045_exit_conditioning_overview_plot(
            result,
            output_path=output_path,
        ),
    )
    return bundle


def load_topix100_1330_entry_next_1045_exit_conditioning_research_bundle(
    bundle_path: str | Path,
) -> Topix1001330EntryNext1045ExitConditioningResult:
    info = load_research_bundle_info(bundle_path)
    tables = load_research_bundle_tables(bundle_path)
    return _build_result_from_payload(dict(info.result_metadata), tables)


def get_topix100_1330_entry_next_1045_exit_conditioning_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        TOPIX100_1330_ENTRY_NEXT_1045_EXIT_CONDITIONING_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_topix100_1330_entry_next_1045_exit_conditioning_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        TOPIX100_1330_ENTRY_NEXT_1045_EXIT_CONDITIONING_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )
