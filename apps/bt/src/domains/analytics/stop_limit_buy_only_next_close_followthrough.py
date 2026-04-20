"""Buy-only follow-through study after stop-low intraday-range events.

Focus:

- market: Prime / Standard / Growth
- event: stop-low + intraday-range
- decision point: next-session close relative to the event-day close
- execution assumption: buy at that same next-session close
- evaluation: hold to close +3 sessions / +5 sessions

This is intentionally narrow and operational. It answers whether a buy-only
desk can still extract edge once shorting is removed and the entry is delayed
until the next close.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

import pandas as pd

from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_dataclass_research_bundle,
    write_dataclass_research_bundle,
)
from src.domains.analytics.stop_limit_daily_classification import (
    StopLimitDailyClassificationResult,
    run_stop_limit_daily_classification_research,
)

MarketName = Literal["プライム", "スタンダード", "グロース"]
NextCloseSign = Literal["minus", "flat", "plus"]
HorizonKey = Literal["next_close_to_close_3d", "next_close_to_close_5d"]

STOP_LIMIT_BUY_ONLY_NEXT_CLOSE_FOLLOWTHROUGH_EXPERIMENT_ID = (
    "market-behavior/stop-limit-buy-only-next-close-followthrough"
)
SELECTED_MARKETS: tuple[MarketName, ...] = ("プライム", "スタンダード", "グロース")
NEXT_CLOSE_SIGN_ORDER: tuple[NextCloseSign, ...] = ("minus", "flat", "plus")
HORIZON_SPECS: tuple[tuple[HorizonKey, str, str, str, str], ...] = (
    (
        "next_close_to_close_3d",
        "next_close_to_close_3d_return",
        "equal_weight_next_close_to_close_3d_return",
        "win_rate_3d",
        "next close -> close +3 sessions",
    ),
    (
        "next_close_to_close_5d",
        "next_close_to_close_5d_return",
        "equal_weight_next_close_to_close_5d_return",
        "win_rate_5d",
        "next close -> close +5 sessions",
    ),
)
TABLE_FIELD_NAMES: tuple[str, ...] = (
    "signal_event_df",
    "signal_summary_df",
    "yearly_summary_df",
    "entry_cohort_df",
    "cohort_portfolio_summary_df",
)
_EPSILON = 1e-9


@dataclass(frozen=True)
class StopLimitBuyOnlyNextCloseFollowthroughResult:
    db_path: str
    source_mode: str
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    selected_markets: tuple[MarketName, ...]
    focus_limit_side: str
    focus_intraday_state: str
    base_event_count: int
    filtered_event_count: int
    plus_signal_count: int
    minus_signal_count: int
    execution_note: str
    focus_rule_note: str
    signal_event_df: pd.DataFrame
    signal_summary_df: pd.DataFrame
    yearly_summary_df: pd.DataFrame
    entry_cohort_df: pd.DataFrame
    cohort_portfolio_summary_df: pd.DataFrame


def _empty_signal_event_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "date",
            "entry_date",
            "calendar_year",
            "code",
            "market_name",
            "limit_side",
            "intraday_state",
            "close_limit_state",
            "next_close_sign",
            "next_close_return",
            "next_close_to_close_3d_return",
            "next_close_to_close_5d_return",
        ]
    )


def _empty_signal_summary_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "market_name",
            "close_limit_state",
            "next_close_sign",
            "event_count",
            "unique_code_count",
            "mean_next_close_to_close_3d_return",
            "median_next_close_to_close_3d_return",
            "win_rate_3d",
            "mean_next_close_to_close_5d_return",
            "median_next_close_to_close_5d_return",
            "win_rate_5d",
            "first_event_date",
            "last_event_date",
        ]
    )


def _empty_yearly_summary_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "calendar_year",
            "market_name",
            "close_limit_state",
            "next_close_sign",
            "event_count",
            "unique_code_count",
            "mean_next_close_to_close_3d_return",
            "median_next_close_to_close_3d_return",
            "win_rate_3d",
            "mean_next_close_to_close_5d_return",
            "median_next_close_to_close_5d_return",
            "win_rate_5d",
        ]
    )


def _empty_entry_cohort_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "entry_date",
            "calendar_year",
            "market_name",
            "close_limit_state",
            "next_close_sign",
            "cohort_event_count",
            "cohort_unique_code_count",
            "equal_weight_next_close_to_close_3d_return",
            "equal_weight_next_close_to_close_5d_return",
        ]
    )


def _empty_cohort_portfolio_summary_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "market_name",
            "close_limit_state",
            "next_close_sign",
            "horizon_key",
            "horizon_label",
            "date_count",
            "total_signal_count",
            "avg_names_per_date",
            "median_names_per_date",
            "mean_cohort_return",
            "median_cohort_return",
            "win_rate",
            "loss_rate",
            "best_cohort_return",
            "worst_cohort_return",
            "first_entry_date",
            "last_entry_date",
        ]
    )


def _prepare_signal_event_df(
    classification_result: StopLimitDailyClassificationResult,
) -> pd.DataFrame:
    event_df = classification_result.event_df.copy()
    if event_df.empty:
        return _empty_signal_event_df()

    scoped_df = event_df[
        event_df["market_name"].astype(str).isin(SELECTED_MARKETS)
        & (event_df["limit_side"].astype(str) == "stop_low")
        & (event_df["intraday_state"].astype(str) == "intraday_range")
        & (event_df["next_close_return"].notna())
        & (event_df["next_date"].notna())
    ].copy()
    if scoped_df.empty:
        return _empty_signal_event_df()

    scoped_df["next_close_sign"] = "flat"
    scoped_df.loc[scoped_df["next_close_return"] > 0, "next_close_sign"] = "plus"
    scoped_df.loc[scoped_df["next_close_return"] < 0, "next_close_sign"] = "minus"
    scoped_df["entry_date"] = scoped_df["next_date"].astype(str)
    scoped_df["calendar_year"] = pd.to_datetime(scoped_df["entry_date"]).dt.year.astype(int)
    scoped_df["next_close_to_close_3d_return"] = (
        scoped_df["close_3d"] / scoped_df["next_close"] - 1.0
    )
    scoped_df["next_close_to_close_5d_return"] = (
        scoped_df["close_5d"] / scoped_df["next_close"] - 1.0
    )
    scoped_df.loc[scoped_df["next_close"].abs() <= _EPSILON, "next_close_to_close_3d_return"] = pd.NA
    scoped_df.loc[scoped_df["next_close"].abs() <= _EPSILON, "next_close_to_close_5d_return"] = pd.NA
    scoped_df = scoped_df[
        [
            "date",
            "entry_date",
            "calendar_year",
            "code",
            "market_name",
            "limit_side",
            "intraday_state",
            "close_limit_state",
            "next_close_sign",
            "next_close_return",
            "next_close_to_close_3d_return",
            "next_close_to_close_5d_return",
        ]
    ].sort_values(
        ["market_name", "close_limit_state", "next_close_sign", "entry_date", "code"],
        kind="stable",
    )
    return scoped_df.reset_index(drop=True)


def _mean_or_none(series: pd.Series) -> float | None:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return None
    return float(values.mean())


def _summarize_signal_group(group_df: pd.DataFrame) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "event_count": int(len(group_df)),
        "unique_code_count": int(group_df["code"].nunique()),
        "first_event_date": str(group_df["date"].min()),
        "last_event_date": str(group_df["date"].max()),
    }
    for _, return_col, _, win_rate_col, _ in HORIZON_SPECS:
        values = pd.to_numeric(group_df[return_col], errors="coerce").dropna()
        prefix = return_col.replace("_return", "")
        if values.empty:
            summary[f"mean_{prefix}_return"] = None
            summary[f"median_{prefix}_return"] = None
            summary[win_rate_col] = None
            continue
        summary[f"mean_{prefix}_return"] = float(values.mean())
        summary[f"median_{prefix}_return"] = float(values.median())
        summary[win_rate_col] = float((values > 0).mean())
    return summary


def _build_signal_summary_df(signal_event_df: pd.DataFrame) -> pd.DataFrame:
    if signal_event_df.empty:
        return _empty_signal_summary_df()

    rows: list[dict[str, Any]] = []
    grouped = signal_event_df.groupby(
        ["market_name", "close_limit_state", "next_close_sign"],
        dropna=False,
        sort=False,
    )
    for (market_name, close_limit_state, next_close_sign), group_df in grouped:
        rows.append(
            {
                "market_name": str(market_name),
                "close_limit_state": str(close_limit_state),
                "next_close_sign": str(next_close_sign),
                **_summarize_signal_group(group_df),
            }
        )
    return pd.DataFrame(rows).sort_values(
        ["market_name", "close_limit_state", "next_close_sign"],
        key=lambda series: (
            series.map({key: index for index, key in enumerate(NEXT_CLOSE_SIGN_ORDER, start=1)})
            if series.name == "next_close_sign"
            else series
        ),
        kind="stable",
    ).reset_index(drop=True)


def _build_yearly_summary_df(signal_event_df: pd.DataFrame) -> pd.DataFrame:
    if signal_event_df.empty:
        return _empty_yearly_summary_df()

    rows: list[dict[str, Any]] = []
    grouped = signal_event_df.groupby(
        ["calendar_year", "market_name", "close_limit_state", "next_close_sign"],
        dropna=False,
        sort=False,
    )
    for (calendar_year, market_name, close_limit_state, next_close_sign), group_df in grouped:
        rows.append(
            {
                "calendar_year": int(calendar_year),
                "market_name": str(market_name),
                "close_limit_state": str(close_limit_state),
                "next_close_sign": str(next_close_sign),
                **{
                    key: value
                    for key, value in _summarize_signal_group(group_df).items()
                    if key not in {"first_event_date", "last_event_date"}
                },
            }
        )
    return pd.DataFrame(rows).sort_values(
        ["calendar_year", "market_name", "close_limit_state", "next_close_sign"],
        key=lambda series: (
            series.map({key: index for index, key in enumerate(NEXT_CLOSE_SIGN_ORDER, start=1)})
            if series.name == "next_close_sign"
            else series
        ),
        kind="stable",
    ).reset_index(drop=True)


def _build_entry_cohort_df(signal_event_df: pd.DataFrame) -> pd.DataFrame:
    if signal_event_df.empty:
        return _empty_entry_cohort_df()

    grouped = signal_event_df.groupby(
        ["entry_date", "calendar_year", "market_name", "close_limit_state", "next_close_sign"],
        dropna=False,
        sort=False,
    )
    rows: list[dict[str, Any]] = []
    for group_key, group_df in grouped:
        entry_date, calendar_year, market_name, close_limit_state, next_close_sign = group_key
        rows.append(
            {
                "entry_date": str(entry_date),
                "calendar_year": int(calendar_year),
                "market_name": str(market_name),
                "close_limit_state": str(close_limit_state),
                "next_close_sign": str(next_close_sign),
                "cohort_event_count": int(len(group_df)),
                "cohort_unique_code_count": int(group_df["code"].nunique()),
                "equal_weight_next_close_to_close_3d_return": _mean_or_none(
                    group_df["next_close_to_close_3d_return"]
                ),
                "equal_weight_next_close_to_close_5d_return": _mean_or_none(
                    group_df["next_close_to_close_5d_return"]
                ),
            }
        )
    return pd.DataFrame(rows).sort_values(
        ["market_name", "close_limit_state", "next_close_sign", "entry_date"],
        key=lambda series: (
            series.map({key: index for index, key in enumerate(NEXT_CLOSE_SIGN_ORDER, start=1)})
            if series.name == "next_close_sign"
            else series
        ),
        kind="stable",
    ).reset_index(drop=True)


def _build_cohort_portfolio_summary_df(entry_cohort_df: pd.DataFrame) -> pd.DataFrame:
    if entry_cohort_df.empty:
        return _empty_cohort_portfolio_summary_df()

    rows: list[dict[str, Any]] = []
    grouped = entry_cohort_df.groupby(
        ["market_name", "close_limit_state", "next_close_sign"],
        dropna=False,
        sort=False,
    )
    for (market_name, close_limit_state, next_close_sign), group_df in grouped:
        total_signal_count = int(group_df["cohort_event_count"].sum())
        avg_names_per_date = float(group_df["cohort_event_count"].mean())
        median_names_per_date = float(group_df["cohort_event_count"].median())
        first_entry_date = str(group_df["entry_date"].min())
        last_entry_date = str(group_df["entry_date"].max())
        for horizon_key, _, cohort_return_col, _, horizon_label in HORIZON_SPECS:
            values = pd.to_numeric(group_df[cohort_return_col], errors="coerce").dropna()
            if values.empty:
                mean_cohort_return = None
                median_cohort_return = None
                win_rate = None
                loss_rate = None
                best_cohort_return = None
                worst_cohort_return = None
            else:
                mean_cohort_return = float(values.mean())
                median_cohort_return = float(values.median())
                win_rate = float((values > 0).mean())
                loss_rate = float((values < 0).mean())
                best_cohort_return = float(values.max())
                worst_cohort_return = float(values.min())
            rows.append(
                {
                    "market_name": str(market_name),
                    "close_limit_state": str(close_limit_state),
                    "next_close_sign": str(next_close_sign),
                    "horizon_key": horizon_key,
                    "horizon_label": horizon_label,
                    "date_count": int(len(group_df)),
                    "total_signal_count": total_signal_count,
                    "avg_names_per_date": avg_names_per_date,
                    "median_names_per_date": median_names_per_date,
                    "mean_cohort_return": mean_cohort_return,
                    "median_cohort_return": median_cohort_return,
                    "win_rate": win_rate,
                    "loss_rate": loss_rate,
                    "best_cohort_return": best_cohort_return,
                    "worst_cohort_return": worst_cohort_return,
                    "first_entry_date": first_entry_date,
                    "last_entry_date": last_entry_date,
                }
            )
    return pd.DataFrame(rows).sort_values(
        ["market_name", "close_limit_state", "next_close_sign", "horizon_key"],
        key=lambda series: (
            series.map({key: index for index, key in enumerate(NEXT_CLOSE_SIGN_ORDER, start=1)})
            if series.name == "next_close_sign"
            else series
        ),
        kind="stable",
    ).reset_index(drop=True)


def run_stop_limit_buy_only_next_close_followthrough_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
) -> StopLimitBuyOnlyNextCloseFollowthroughResult:
    classification_result = run_stop_limit_daily_classification_research(
        db_path,
        start_date=start_date,
        end_date=end_date,
    )
    signal_event_df = _prepare_signal_event_df(classification_result)
    signal_summary_df = _build_signal_summary_df(signal_event_df)
    yearly_summary_df = _build_yearly_summary_df(signal_event_df)
    entry_cohort_df = _build_entry_cohort_df(signal_event_df)
    cohort_portfolio_summary_df = _build_cohort_portfolio_summary_df(entry_cohort_df)
    plus_signal_count = int((signal_event_df["next_close_sign"] == "plus").sum()) if not signal_event_df.empty else 0
    minus_signal_count = int((signal_event_df["next_close_sign"] == "minus").sum()) if not signal_event_df.empty else 0
    execution_note = (
        "This study assumes the signal can be confirmed and executed at the next-session "
        "close, which should be read as a close-auction approximation rather than a "
        "guaranteed fully observed fill."
    )
    focus_rule_note = (
        "Focus only on Prime / Standard / Growth names where the original event was "
        "`stop_low` plus `intraday_range`. The key branch is whether the next-session "
        "close finished above or below the event-day close."
    )
    analysis_start_date = (
        str(signal_event_df["entry_date"].min()) if not signal_event_df.empty else None
    )
    analysis_end_date = (
        str(signal_event_df["entry_date"].max()) if not signal_event_df.empty else None
    )
    return StopLimitBuyOnlyNextCloseFollowthroughResult(
        db_path=classification_result.db_path,
        source_mode=classification_result.source_mode,
        source_detail=classification_result.source_detail,
        available_start_date=classification_result.available_start_date,
        available_end_date=classification_result.available_end_date,
        analysis_start_date=analysis_start_date,
        analysis_end_date=analysis_end_date,
        selected_markets=SELECTED_MARKETS,
        focus_limit_side="stop_low",
        focus_intraday_state="intraday_range",
        base_event_count=classification_result.total_event_count,
        filtered_event_count=int(len(signal_event_df)),
        plus_signal_count=plus_signal_count,
        minus_signal_count=minus_signal_count,
        execution_note=execution_note,
        focus_rule_note=focus_rule_note,
        signal_event_df=signal_event_df,
        signal_summary_df=signal_summary_df,
        yearly_summary_df=yearly_summary_df,
        entry_cohort_df=entry_cohort_df,
        cohort_portfolio_summary_df=cohort_portfolio_summary_df,
    )


def write_stop_limit_buy_only_next_close_followthrough_research_bundle(
    result: StopLimitBuyOnlyNextCloseFollowthroughResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=STOP_LIMIT_BUY_ONLY_NEXT_CLOSE_FOLLOWTHROUGH_EXPERIMENT_ID,
        module=__name__,
        function="run_stop_limit_buy_only_next_close_followthrough_research",
        params={
            "start_date": result.analysis_start_date,
            "end_date": result.analysis_end_date,
        },
        result=result,
        table_field_names=TABLE_FIELD_NAMES,
        summary_markdown=_build_research_bundle_summary_markdown(result),
        published_summary=_build_published_summary_payload(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_stop_limit_buy_only_next_close_followthrough_research_bundle(
    bundle_path: str | Path,
) -> StopLimitBuyOnlyNextCloseFollowthroughResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=StopLimitBuyOnlyNextCloseFollowthroughResult,
        table_field_names=TABLE_FIELD_NAMES,
    )


def get_stop_limit_buy_only_next_close_followthrough_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        STOP_LIMIT_BUY_ONLY_NEXT_CLOSE_FOLLOWTHROUGH_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_stop_limit_buy_only_next_close_followthrough_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        STOP_LIMIT_BUY_ONLY_NEXT_CLOSE_FOLLOWTHROUGH_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def _build_research_bundle_summary_markdown(
    result: StopLimitBuyOnlyNextCloseFollowthroughResult,
) -> str:
    top_5d_plus = _top_signal_rows(
        result.signal_summary_df,
        next_close_sign="plus",
        limit=8,
        sort_column="mean_next_close_to_close_5d_return",
    )
    top_portfolio_plus = _top_cohort_portfolio_rows(
        result.cohort_portfolio_summary_df,
        next_close_sign="plus",
        horizon_key="next_close_to_close_5d",
        limit=8,
    )
    yearly_focus = _top_yearly_rows(
        result.yearly_summary_df,
        next_close_sign="plus",
        limit=12,
    )
    lines = [
        "# Stop-Limit Buy-Only Next-Close Followthrough",
        "",
        "## Scope",
        "",
        "- Markets: `Prime / Standard / Growth`.",
        "- Event filter: `stop_low` + `intraday_range`.",
        "- Decision split: whether `next_close` finished above or below the event-day close.",
        "- Entry assumption: buy at that same `next_close`.",
        "- Exit windows: `close +3 sessions` / `close +5 sessions`.",
        "",
        "## Notes",
        "",
        f"- {result.execution_note}",
        f"- {result.focus_rule_note}",
        f"- Available stock-data range: `{result.available_start_date}` -> `{result.available_end_date}`.",
        f"- Signal-entry range: `{result.analysis_start_date}` -> `{result.analysis_end_date}`.",
        f"- Base stop-limit classified events: `{result.base_event_count}`.",
        f"- Filtered buy-only focus events: `{result.filtered_event_count}`.",
        f"- `next_close` plus signals: `{result.plus_signal_count}`.",
        f"- `next_close` minus signals: `{result.minus_signal_count}`.",
        "",
        "## Best Plus Buckets",
        "",
        _markdown_table(
            top_5d_plus,
            columns=(
                ("Market", "market_name"),
                ("Close State", "close_limit_state"),
                ("Sign", "next_close_sign"),
                ("Events", "event_count"),
                ("Mean 5d", "mean_next_close_to_close_5d_return"),
                ("Median 5d", "median_next_close_to_close_5d_return"),
                ("Win 5d", "win_rate_5d"),
            ),
        ),
        "",
        "## Entry-Cohort Portfolio Lens",
        "",
        _markdown_table(
            top_portfolio_plus,
            columns=(
                ("Market", "market_name"),
                ("Close State", "close_limit_state"),
                ("Sign", "next_close_sign"),
                ("Dates", "date_count"),
                ("Avg Names", "avg_names_per_date"),
                ("Mean Cohort", "mean_cohort_return"),
                ("Median Cohort", "median_cohort_return"),
                ("Win", "win_rate"),
            ),
        ),
        "",
        "## Strong Years",
        "",
        _markdown_table(
            yearly_focus,
            columns=(
                ("Year", "calendar_year"),
                ("Market", "market_name"),
                ("Close State", "close_limit_state"),
                ("Sign", "next_close_sign"),
                ("Events", "event_count"),
                ("Mean 5d", "mean_next_close_to_close_5d_return"),
                ("Median 5d", "median_next_close_to_close_5d_return"),
                ("Win 5d", "win_rate_5d"),
            ),
        ),
    ]
    return "\n".join(lines)


def _build_published_summary_payload(
    result: StopLimitBuyOnlyNextCloseFollowthroughResult,
) -> dict[str, Any]:
    return {
        "analysisStartDate": result.analysis_start_date,
        "analysisEndDate": result.analysis_end_date,
        "filteredEventCount": result.filtered_event_count,
        "plusSignalCount": result.plus_signal_count,
        "minusSignalCount": result.minus_signal_count,
        "executionNote": result.execution_note,
        "focusRuleNote": result.focus_rule_note,
        "bestPlusBuckets": _top_signal_rows(
            result.signal_summary_df,
            next_close_sign="plus",
            limit=8,
            sort_column="mean_next_close_to_close_5d_return",
        ),
        "cohortPortfolioLens": _top_cohort_portfolio_rows(
            result.cohort_portfolio_summary_df,
            next_close_sign="plus",
            horizon_key="next_close_to_close_5d",
            limit=8,
        ),
    }


def _top_signal_rows(
    signal_summary_df: pd.DataFrame,
    *,
    next_close_sign: NextCloseSign,
    limit: int,
    sort_column: str,
) -> list[dict[str, Any]]:
    if signal_summary_df.empty:
        return []
    scoped_df = signal_summary_df[
        signal_summary_df["next_close_sign"].astype(str) == next_close_sign
    ].copy()
    if scoped_df.empty:
        return []
    ranked = (
        scoped_df.sort_values(
            by=[sort_column, "event_count", "market_name"],
            ascending=[False, False, True],
            kind="stable",
        )
        .head(limit)
        .reset_index(drop=True)
    )
    return cast(list[dict[str, Any]], ranked.to_dict("records"))


def _top_cohort_portfolio_rows(
    cohort_portfolio_summary_df: pd.DataFrame,
    *,
    next_close_sign: NextCloseSign,
    horizon_key: HorizonKey,
    limit: int,
) -> list[dict[str, Any]]:
    if cohort_portfolio_summary_df.empty:
        return []
    scoped_df = cohort_portfolio_summary_df[
        (cohort_portfolio_summary_df["next_close_sign"].astype(str) == next_close_sign)
        & (cohort_portfolio_summary_df["horizon_key"].astype(str) == horizon_key)
    ].copy()
    if scoped_df.empty:
        return []
    ranked = (
        scoped_df.sort_values(
            by=["mean_cohort_return", "date_count", "market_name"],
            ascending=[False, False, True],
            kind="stable",
        )
        .head(limit)
        .reset_index(drop=True)
    )
    return cast(list[dict[str, Any]], ranked.to_dict("records"))


def _top_yearly_rows(
    yearly_summary_df: pd.DataFrame,
    *,
    next_close_sign: NextCloseSign,
    limit: int,
) -> list[dict[str, Any]]:
    if yearly_summary_df.empty:
        return []
    scoped_df = yearly_summary_df[
        (yearly_summary_df["next_close_sign"].astype(str) == next_close_sign)
        & (yearly_summary_df["event_count"] >= 5)
    ].copy()
    if scoped_df.empty:
        return []
    ranked = (
        scoped_df.sort_values(
            by=["mean_next_close_to_close_5d_return", "event_count", "calendar_year"],
            ascending=[False, False, False],
            kind="stable",
        )
        .head(limit)
        .reset_index(drop=True)
    )
    return cast(list[dict[str, Any]], ranked.to_dict("records"))


def _markdown_table(
    rows: list[dict[str, Any]],
    *,
    columns: tuple[tuple[str, str], ...],
) -> str:
    header = "| " + " | ".join(label for label, _ in columns) + " |"
    separator = "| " + " | ".join("---" for _ in columns) + " |"
    if not rows:
        return "\n".join([header, separator, "| (none) |" + " |" * (len(columns) - 1)])
    body: list[str] = []
    for row in rows:
        rendered = " | ".join(_format_markdown_cell(row.get(key)) for _, key in columns)
        body.append(f"| {rendered} |")
    return "\n".join([header, separator, *body])


def _format_markdown_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return f"{value:.6f}".rstrip("0").rstrip(".")
    return str(value).replace("|", "\\|")
