"""Selection-first contracts for ranking research outcome evaluation.

The freeze functions operate on signal-time rows only.  Outcome rows are joined
only by :func:`evaluate_frozen_selection`, after cohort membership is fixed.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import hashlib
import json
import re
from typing import Mapping, Sequence

import numpy as np
import pandas as pd


_CODE_COLUMN = "code"
_NORMALIZED_CODE_COLUMN = "__selection_normalized_code"
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class SelectionAudit:
    """Immutable identity of a complete signal-time cohort."""

    policy: str
    key_columns: tuple[str, ...]
    row_count: int
    sha256: str

    def to_manifest_payload(self) -> dict[str, object]:
        return {
            "policy": self.policy,
            "key_columns": list(self.key_columns),
            "row_count": self.row_count,
            "sha256": self.sha256,
        }


def build_relation_selection_audit(
    conn: object,
    *,
    source_name: str,
    policy: str,
    key_columns: Sequence[str],
) -> SelectionAudit:
    """Hash every unique frozen selection key before outcomes are attached."""

    if not policy or not key_columns:
        raise ValueError("selection audit policy and key columns are required")
    identifiers = (source_name, *key_columns)
    if any(_IDENTIFIER_RE.fullmatch(item) is None for item in identifiers):
        raise ValueError("selection audit identifiers must be simple SQL identifiers")
    columns = tuple(key_columns)
    select_sql = ", ".join(f'"{column}"' for column in columns)
    order_sql = ", ".join(f'"{column}"' for column in columns)
    rows = conn.execute(  # type: ignore[union-attr]
        f'SELECT {select_sql} FROM "{source_name}" ORDER BY {order_sql}'
    ).fetchall()
    if any(any(value is None for value in row) for row in rows):
        raise ValueError("selection audit keys must not contain NULL")
    if any(left == right for left, right in zip(rows, rows[1:], strict=False)):
        raise ValueError("selection audit keys contain duplicate rows")
    digest = hashlib.sha256()
    for row in rows:
        normalized = [
            value.isoformat() if isinstance(value, (date, datetime)) else str(value)
            for value in row
        ]
        digest.update(
            json.dumps(
                normalized,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode()
        )
        digest.update(b"\n")
    return SelectionAudit(
        policy=policy,
        key_columns=columns,
        row_count=len(rows),
        sha256=digest.hexdigest(),
    )


@dataclass(frozen=True)
class FrozenSignalSelection:
    """Signal-time membership frozen before any outcome frame is available."""

    candidates: pd.DataFrame
    selected: pd.DataFrame
    top: pd.DataFrame
    bottom: pd.DataFrame
    middle: pd.DataFrame
    group_columns: tuple[str, ...]
    score_columns: tuple[str, ...]
    selection_kind: str

    @property
    def key_columns(self) -> tuple[str, ...]:
        """The unique, signal-time key used when outcomes are attached."""

        return (*self.group_columns, _CODE_COLUMN)

    @property
    def buckets(self) -> Mapping[str, pd.DataFrame]:
        """Named selected cohorts, including empty cohorts where applicable."""

        return {"bottom": self.bottom, "middle": self.middle, "top": self.top}


@dataclass(frozen=True)
class EvaluatedSignalSelection:
    """Frozen membership with a separately attached outcome column."""

    frozen: FrozenSignalSelection
    candidates: pd.DataFrame
    selected: pd.DataFrame
    top: pd.DataFrame
    bottom: pd.DataFrame
    middle: pd.DataFrame
    outcome_column: str
    candidate_count: int
    candidate_outcome_count: int
    candidate_outcome_coverage_pct: float
    selected_outcome_count: int
    selected_outcome_coverage_pct: float
    outcome_status: str
    effect_metrics: Mapping[str, float] | None


def freeze_signal_topk(
    frame: pd.DataFrame,
    *,
    group_columns: Sequence[str] = (),
    score_columns: Sequence[str],
    k: int,
    ascending: Sequence[bool] | None = None,
) -> FrozenSignalSelection:
    """Freeze a deterministic fixed-size top-k cohort per signal-time group."""

    if k <= 0:
        raise ValueError("k must be positive")
    candidates, groups, scores, directions = _prepare_signal_frame(
        frame,
        group_columns=group_columns,
        score_columns=score_columns,
        ascending=ascending,
    )
    top = _select_per_group(candidates, groups, scores, directions, count=k)
    return _frozen_selection(
        candidates,
        top=top,
        bottom=_empty_like(candidates),
        middle=_empty_like(candidates),
        group_columns=groups,
        score_columns=scores,
        selection_kind="topk",
    )


def freeze_signal_tails(
    frame: pd.DataFrame,
    *,
    group_columns: Sequence[str] = (),
    score_columns: Sequence[str],
    fraction: float,
    min_side: int = 1,
    ascending: Sequence[bool] | None = None,
) -> FrozenSignalSelection:
    """Freeze disjoint fixed-size high and low signal tails per group.

    ``fraction`` determines the floor of each side's signal-time candidate
    count; ``min_side`` raises that floor.  Groups that cannot supply disjoint
    sides are retained as candidates but contribute no selected rows.
    """

    if not 0.0 < fraction <= 0.5:
        raise ValueError("fraction must be greater than 0 and at most 0.5")
    if min_side <= 0:
        raise ValueError("min_side must be positive")
    candidates, groups, scores, directions = _prepare_signal_frame(
        frame,
        group_columns=group_columns,
        score_columns=score_columns,
        ascending=ascending,
    )
    top_parts: list[pd.DataFrame] = []
    bottom_parts: list[pd.DataFrame] = []
    for group in _iter_groups(candidates, groups):
        side_count = max(min_side, int(len(group) * fraction))
        if len(group) < 2 * side_count:
            continue
        ranked = _rank_group(group, scores, directions)
        top_parts.append(ranked.head(side_count))
        bottom_parts.append(ranked.tail(side_count))
    top = _concat_or_empty(top_parts, candidates)
    bottom = _concat_or_empty(bottom_parts, candidates)
    return _frozen_selection(
        candidates,
        top=top,
        bottom=bottom,
        middle=_empty_like(candidates),
        group_columns=groups,
        score_columns=scores,
        selection_kind="tails",
    )


def freeze_signal_percentile_buckets(
    frame: pd.DataFrame,
    *,
    group_columns: Sequence[str] = (),
    percentile_column: str,
    lower_max: float = 0.2,
    upper_min: float = 0.8,
) -> FrozenSignalSelection:
    """Freeze value-based percentile buckets without recomputing percentiles.

    Boundaries are inclusive.  Therefore equal, precomputed percentile values
    remain together even when that produces buckets larger than a nominal
    fraction.
    """

    if not 0.0 <= lower_max < upper_min <= 1.0:
        raise ValueError("percentile boundaries must satisfy 0 <= lower < upper <= 1")
    candidates, groups, scores, _ = _prepare_signal_frame(
        frame,
        group_columns=group_columns,
        score_columns=(percentile_column,),
        ascending=(False,),
    )
    values = pd.to_numeric(candidates[percentile_column], errors="coerce")
    bottom = candidates.loc[values.le(lower_max)].copy()
    middle = candidates.loc[values.gt(lower_max) & values.lt(upper_min)].copy()
    top = candidates.loc[values.ge(upper_min)].copy()
    return _frozen_selection(
        candidates,
        top=_sort_by_group_and_code(top, groups),
        bottom=_sort_by_group_and_code(bottom, groups),
        middle=_sort_by_group_and_code(middle, groups),
        group_columns=groups,
        score_columns=scores,
        selection_kind="percentile_buckets",
    )


def evaluate_frozen_selection(
    frozen: FrozenSignalSelection,
    outcomes: pd.DataFrame,
    *,
    outcome_column: str,
) -> EvaluatedSignalSelection:
    """Attach unique outcomes to frozen membership without changing membership.

    Any missing selected outcome makes the evaluation incomplete and suppresses
    all effect metrics.  Missing candidate outcomes are still reported as
    coverage, but never cause lower-ranked rows to replace frozen members.
    """

    frozen_frames = (
        frozen.candidates,
        frozen.selected,
        frozen.top,
        frozen.bottom,
        frozen.middle,
    )
    if any(outcome_column in frame.columns for frame in frozen_frames):
        raise ValueError("frozen selection already carries declared outcome column")
    keys = frozen.key_columns
    _require_columns(outcomes, (*keys, outcome_column), frame_name="outcomes")
    outcome_rows = outcomes.loc[:, [*keys, outcome_column]].copy()
    _validate_unique_keys(outcome_rows, keys, frame_name="outcome")
    outcome_rows[outcome_column] = _coerce_finite_numeric(
        outcome_rows[outcome_column]
    )

    candidates = _attach_outcome(frozen.candidates, outcome_rows, keys, outcome_column)
    top = _attach_outcome(frozen.top, outcome_rows, keys, outcome_column)
    bottom = _attach_outcome(frozen.bottom, outcome_rows, keys, outcome_column)
    middle = _attach_outcome(frozen.middle, outcome_rows, keys, outcome_column)
    selected = _attach_outcome(frozen.selected, outcome_rows, keys, outcome_column)
    candidate_values = candidates[outcome_column]
    selected_values = selected[outcome_column]
    candidate_count = len(candidates)
    selected_count = len(selected)
    candidate_outcome_count = int(candidate_values.notna().sum())
    selected_outcome_count = int(selected_values.notna().sum())
    complete = bool(selected_count and selected_values.notna().all())
    return EvaluatedSignalSelection(
        frozen=frozen,
        candidates=candidates,
        selected=selected,
        top=top,
        bottom=bottom,
        middle=middle,
        outcome_column=outcome_column,
        candidate_count=candidate_count,
        candidate_outcome_count=candidate_outcome_count,
        candidate_outcome_coverage_pct=_coverage_pct(
            candidate_outcome_count, candidate_count
        ),
        selected_outcome_count=selected_outcome_count,
        selected_outcome_coverage_pct=_coverage_pct(
            selected_outcome_count, selected_count
        ),
        outcome_status="complete" if complete else "incomplete",
        effect_metrics=(
            _effect_metrics(top, bottom, middle, outcome_column) if complete else None
        ),
    )


@dataclass(frozen=True)
class FrozenTopKSelection:
    """Backward-compatible outcome-aware view of :class:`FrozenSignalSelection`."""

    candidates: pd.DataFrame
    selected: pd.DataFrame
    candidate_outcomes: pd.Series
    selected_outcomes: pd.Series
    candidate_count: int
    candidate_outcome_count: int
    candidate_outcome_coverage_pct: float
    selected_outcome_count: int
    selected_outcome_coverage_pct: float
    outcome_status: str


def select_frozen_topk(
    frame: pd.DataFrame,
    *,
    score_columns: Sequence[str],
    outcome_column: str,
    k: int,
    ascending: Sequence[bool],
) -> FrozenTopKSelection:
    """Adapt the legacy API to the selection-first freeze/evaluate boundary."""

    _require_columns(frame, (_CODE_COLUMN, *score_columns, outcome_column), frame_name="frame")
    if outcome_column in score_columns:
        raise ValueError("outcome column must not be used as a score or group column")
    frozen = freeze_signal_topk(
        frame.drop(columns=[outcome_column]),
        score_columns=score_columns,
        k=k,
        ascending=ascending,
    )
    evaluated = evaluate_frozen_selection(
        frozen,
        frame.loc[:, [_CODE_COLUMN, outcome_column]],
        outcome_column=outcome_column,
    )
    candidates = _rank_group(
        evaluated.candidates,
        tuple(score_columns),
        tuple(ascending),
    )
    candidate_outcomes = candidates[outcome_column]
    selected_outcomes = pd.to_numeric(evaluated.selected[outcome_column], errors="coerce")
    complete = bool(candidate_outcomes.notna().all() and selected_outcomes.notna().all())
    return FrozenTopKSelection(
        candidates=candidates,
        selected=evaluated.selected,
        candidate_outcomes=candidate_outcomes,
        selected_outcomes=selected_outcomes,
        candidate_count=evaluated.candidate_count,
        candidate_outcome_count=evaluated.candidate_outcome_count,
        candidate_outcome_coverage_pct=evaluated.candidate_outcome_coverage_pct,
        selected_outcome_count=evaluated.selected_outcome_count,
        selected_outcome_coverage_pct=evaluated.selected_outcome_coverage_pct,
        outcome_status="complete" if complete else "incomplete_outcomes",
    )


def _prepare_signal_frame(
    frame: pd.DataFrame,
    *,
    group_columns: Sequence[str],
    score_columns: Sequence[str],
    ascending: Sequence[bool] | None,
) -> tuple[pd.DataFrame, tuple[str, ...], tuple[str, ...], tuple[bool, ...]]:
    groups = tuple(group_columns)
    scores = tuple(score_columns)
    if not scores:
        raise ValueError("score_columns must not be empty")
    if _CODE_COLUMN in groups:
        raise ValueError("group_columns must not contain code")
    if len(set(groups)) != len(groups) or len(set(scores)) != len(scores):
        raise ValueError("group_columns and score_columns must not contain duplicates")
    _require_columns(frame, (*groups, _CODE_COLUMN, *scores), frame_name="frame")
    _reject_outcome_derived_fields((*groups, *scores))
    _validate_finite_numeric_fields(frame, scores, field_kind="signal score")
    directions = tuple(False for _ in scores) if ascending is None else tuple(ascending)
    if len(scores) != len(directions):
        raise ValueError("score_columns and ascending must have matching lengths")

    _validate_unique_keys(frame, (*groups, _CODE_COLUMN), frame_name="signal")
    candidates = frame.dropna(subset=[*groups, _CODE_COLUMN]).copy()
    return candidates, groups, scores, directions


def _reject_outcome_derived_fields(columns: Sequence[str]) -> None:
    invalid = [column for column in columns if column.casefold().startswith("forward_")]
    if invalid:
        raise ValueError(f"outcome-derived selection fields are forbidden: {sorted(invalid)}")


def _validate_finite_numeric_fields(
    frame: pd.DataFrame,
    columns: Sequence[str],
    *,
    field_kind: str,
) -> None:
    invalid: list[str] = []
    for column in columns:
        values = frame[column]
        if (
            not pd.api.types.is_numeric_dtype(values)
            or pd.api.types.is_bool_dtype(values)
        ):
            invalid.append(column)
            continue
        numeric = pd.to_numeric(values, errors="coerce")
        if numeric.isna().any() or not np.isfinite(numeric).all():
            invalid.append(column)
    if invalid:
        raise ValueError(
            f"{field_kind} fields must be finite numeric values: {sorted(invalid)}"
        )


def _require_columns(
    frame: pd.DataFrame,
    columns: Sequence[str],
    *,
    frame_name: str,
) -> None:
    missing = sorted(set(columns).difference(frame.columns))
    if missing:
        raise ValueError(f"{frame_name} is missing required columns: {missing}")


def _validate_unique_keys(
    frame: pd.DataFrame,
    key_columns: Sequence[str],
    *,
    frame_name: str,
) -> None:
    keyed = _with_normalized_code(frame, key_columns)
    normalized_keys = [
        _NORMALIZED_CODE_COLUMN if column == _CODE_COLUMN else column
        for column in key_columns
    ]
    if keyed.duplicated(normalized_keys).any():
        raise ValueError(f"duplicate {frame_name} keys")


def _with_normalized_code(frame: pd.DataFrame, key_columns: Sequence[str]) -> pd.DataFrame:
    keyed = frame.copy()
    if _CODE_COLUMN in key_columns:
        keyed[_NORMALIZED_CODE_COLUMN] = keyed[_CODE_COLUMN].map(_normalize_code)
    return keyed


def _normalize_code(value: object) -> str:
    code = str(value).strip()
    if code.endswith(".0") and code[:-2].isdigit():
        code = code[:-2]
    if code.isdigit():
        code = code.zfill(4)
        if len(code) in (5, 6) and code.endswith("0"):
            code = code[:-1]
    return code


def _iter_groups(frame: pd.DataFrame, group_columns: tuple[str, ...]) -> list[pd.DataFrame]:
    if not group_columns:
        return [frame]
    grouper: str | list[str] = (
        group_columns[0] if len(group_columns) == 1 else list(group_columns)
    )
    return [group for _, group in frame.groupby(grouper, sort=True, observed=True)]


def _rank_group(
    group: pd.DataFrame,
    score_columns: tuple[str, ...],
    ascending: tuple[bool, ...],
) -> pd.DataFrame:
    ranked = group.copy()
    ranked[_NORMALIZED_CODE_COLUMN] = ranked[_CODE_COLUMN].map(_normalize_code)
    ranked = ranked.sort_values(
        [*score_columns, _NORMALIZED_CODE_COLUMN],
        ascending=[*ascending, True],
        kind="mergesort",
    )
    return ranked.drop(columns=[_NORMALIZED_CODE_COLUMN])


def _select_per_group(
    candidates: pd.DataFrame,
    group_columns: tuple[str, ...],
    score_columns: tuple[str, ...],
    ascending: tuple[bool, ...],
    *,
    count: int,
) -> pd.DataFrame:
    selected = [
        _rank_group(group, score_columns, ascending).head(count)
        for group in _iter_groups(candidates, group_columns)
    ]
    return _concat_or_empty(selected, candidates)


def _sort_by_group_and_code(frame: pd.DataFrame, group_columns: tuple[str, ...]) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    ordered = frame.copy()
    ordered[_NORMALIZED_CODE_COLUMN] = ordered[_CODE_COLUMN].map(_normalize_code)
    ordered = ordered.sort_values(
        [*group_columns, _NORMALIZED_CODE_COLUMN],
        kind="mergesort",
    )
    return ordered.drop(columns=[_NORMALIZED_CODE_COLUMN])


def _concat_or_empty(parts: list[pd.DataFrame], template: pd.DataFrame) -> pd.DataFrame:
    return pd.concat(parts, ignore_index=True) if parts else _empty_like(template)


def _empty_like(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.iloc[0:0].copy()


def _frozen_selection(
    candidates: pd.DataFrame,
    *,
    top: pd.DataFrame,
    bottom: pd.DataFrame,
    middle: pd.DataFrame,
    group_columns: tuple[str, ...],
    score_columns: tuple[str, ...],
    selection_kind: str,
) -> FrozenSignalSelection:
    selected = _concat_or_empty([bottom, middle, top], candidates)
    _validate_unique_keys(selected, (*group_columns, _CODE_COLUMN), frame_name="selected")
    return FrozenSignalSelection(
        candidates=_sort_by_group_and_code(candidates, group_columns),
        selected=selected,
        top=top,
        bottom=bottom,
        middle=middle,
        group_columns=group_columns,
        score_columns=score_columns,
        selection_kind=selection_kind,
    )


def _attach_outcome(
    signal_frame: pd.DataFrame,
    outcomes: pd.DataFrame,
    key_columns: Sequence[str],
    outcome_column: str,
) -> pd.DataFrame:
    signal_keyed = _with_normalized_code(signal_frame, key_columns)
    outcome_keyed = _with_normalized_code(outcomes, key_columns)
    join_keys = [
        _NORMALIZED_CODE_COLUMN if column == _CODE_COLUMN else column
        for column in key_columns
    ]
    attached = signal_keyed.merge(
        outcome_keyed.loc[:, [*join_keys, outcome_column]],
        on=join_keys,
        how="left",
        sort=False,
        validate="one_to_one",
    )
    return attached.drop(columns=[_NORMALIZED_CODE_COLUMN])


def _coverage_pct(count: int, total: int) -> float:
    return count / total * 100.0 if total else float("nan")


def _coerce_finite_numeric(values: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    return numeric.where(np.isfinite(numeric), np.nan)


def _effect_metrics(
    top: pd.DataFrame,
    bottom: pd.DataFrame,
    middle: pd.DataFrame,
    outcome_column: str,
) -> Mapping[str, float]:
    metrics: dict[str, float] = {}
    for name, cohort in (("bottom", bottom), ("middle", middle), ("top", top)):
        if not cohort.empty:
            metrics[f"{name}_mean"] = float(
                pd.to_numeric(cohort[outcome_column], errors="coerce").mean()
            )
    if "top_mean" in metrics and "bottom_mean" in metrics:
        metrics["top_minus_bottom_mean"] = metrics["top_mean"] - metrics["bottom_mean"]
    return metrics
