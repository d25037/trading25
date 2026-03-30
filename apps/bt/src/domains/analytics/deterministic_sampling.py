from __future__ import annotations

import hashlib
from collections.abc import Sequence

import pandas as pd


def select_deterministic_samples(
    samples_df: pd.DataFrame,
    *,
    sample_size: int,
    partition_columns: Sequence[str],
    hash_columns: Sequence[str],
    final_order_columns: Sequence[str],
) -> pd.DataFrame:
    result = samples_df.copy()
    if result.empty:
        result["sample_rank"] = pd.Series(dtype="int64")
        return result

    hash_input = pd.Series("", index=result.index, dtype="string")
    for column in hash_columns:
        hash_input = hash_input + result[column].astype(str) + "|"

    result["_sample_sort_key"] = hash_input.map(
        lambda value: hashlib.md5(value.encode("utf-8"), usedforsecurity=False).hexdigest()
    )
    sort_columns = list(dict.fromkeys([*partition_columns, "_sample_sort_key", *hash_columns]))
    result = result.sort_values(by=sort_columns, kind="stable")
    result["sample_rank"] = result.groupby(list(partition_columns)).cumcount() + 1
    result = result.loc[result["sample_rank"] <= sample_size].copy()
    result = result.drop(columns=["_sample_sort_key"])
    return result.sort_values(by=list(final_order_columns), kind="stable").reset_index(drop=True)
