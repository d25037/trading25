"""
Walk-forward analysis helpers
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List

import pandas as pd


@dataclass
class WalkForwardSplit:
    """ウォークフォワード分割の期間情報"""

    train_start: str
    train_end: str
    test_start: str
    test_end: str


def generate_walkforward_splits(
    index: Iterable[pd.Timestamp],
    train_window: int,
    test_window: int,
    step: int | None = None,
) -> List[WalkForwardSplit]:
    """
    日付インデックスからウォークフォワード分割を生成

    Args:
        index: 日付インデックス
        train_window: 学習期間（営業日数）
        test_window: 検証期間（営業日数）
        step: ステップ幅（Noneならtest_window）
    """
    dates = pd.DatetimeIndex(index).sort_values().unique()  # type: ignore[arg-type]
    if train_window <= 0 or test_window <= 0:
        raise ValueError("train_windowとtest_windowは正の値である必要があります")

    step = step or test_window
    if step <= 0:
        raise ValueError("stepは正の値である必要があります")

    splits: List[WalkForwardSplit] = []
    start = 0
    total = len(dates)

    while start + train_window + test_window <= total:
        train_start = dates[start]
        train_end = dates[start + train_window - 1]
        test_start = dates[start + train_window]
        test_end = dates[start + train_window + test_window - 1]

        splits.append(
            WalkForwardSplit(
                train_start=train_start.date().isoformat(),
                train_end=train_end.date().isoformat(),
                test_start=test_start.date().isoformat(),
                test_end=test_end.date().isoformat(),
            )
        )

        start += step

    return splits
