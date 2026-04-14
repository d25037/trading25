from __future__ import annotations

from collections.abc import Sequence

import pandas as pd
import pandas.testing as pdt


def assert_frame_rows_equal_at_date(
    base_df: pd.DataFrame,
    extended_df: pd.DataFrame,
    *,
    target_date: str,
    date_col: str = "date",
    key_cols: Sequence[str] = ("code",),
    compare_columns: Sequence[str] | None = None,
) -> None:
    """Assert that rows observed on a target date are stable across two frames."""
    base_rows = (
        base_df[base_df[date_col].astype(str) == target_date]
        .sort_values(list(key_cols), kind="stable")
        .reset_index(drop=True)
    )
    extended_rows = (
        extended_df[extended_df[date_col].astype(str) == target_date]
        .sort_values(list(key_cols), kind="stable")
        .reset_index(drop=True)
    )

    if compare_columns is not None:
        base_rows = base_rows[list(compare_columns)].copy()
        extended_rows = extended_rows[list(compare_columns)].copy()

    pdt.assert_frame_equal(base_rows, extended_rows, check_like=False)
