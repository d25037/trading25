"""oracle 指数寄り付きギャップレジームシグナルのテスト."""

import pandas as pd
import pytest

from src.domains.strategy.signals.oracle_index_open_gap_regime import (
    oracle_index_open_gap_regime_signal,
)


@pytest.fixture
def sample_index_data() -> pd.DataFrame:
    index = pd.date_range("2024-01-01", periods=6, freq="D")
    return pd.DataFrame(
        {
            "Open": [100.0, 97.5, 98.5, 100.0, 101.5, 102.5],
            "Close": [100.0, 100.0, 100.0, 100.0, 100.0, 100.0],
        },
        index=index,
    )


def test_down_large(sample_index_data: pd.DataFrame) -> None:
    signal = oracle_index_open_gap_regime_signal(
        sample_index_data,
        gap_threshold_1_pct=1.0,
        gap_threshold_2_pct=2.0,
        regime="down_large",
    )
    assert signal.tolist() == [False, True, False, False, False, False]


def test_down_medium(sample_index_data: pd.DataFrame) -> None:
    signal = oracle_index_open_gap_regime_signal(
        sample_index_data,
        gap_threshold_1_pct=1.0,
        gap_threshold_2_pct=2.0,
        regime="down_medium",
    )
    assert signal.tolist() == [False, False, True, False, False, False]


def test_flat(sample_index_data: pd.DataFrame) -> None:
    signal = oracle_index_open_gap_regime_signal(
        sample_index_data,
        gap_threshold_1_pct=1.0,
        gap_threshold_2_pct=2.0,
        regime="flat",
    )
    assert signal.tolist() == [False, False, False, True, False, False]


def test_up_medium(sample_index_data: pd.DataFrame) -> None:
    signal = oracle_index_open_gap_regime_signal(
        sample_index_data,
        gap_threshold_1_pct=1.0,
        gap_threshold_2_pct=2.0,
        regime="up_medium",
    )
    assert signal.tolist() == [False, False, False, False, True, False]


def test_up_large(sample_index_data: pd.DataFrame) -> None:
    signal = oracle_index_open_gap_regime_signal(
        sample_index_data,
        gap_threshold_1_pct=1.0,
        gap_threshold_2_pct=2.0,
        regime="up_large",
    )
    assert signal.tolist() == [False, False, False, False, False, True]


def test_missing_required_column_raises_error() -> None:
    invalid_df = pd.DataFrame({"Close": [100.0, 101.0]})

    with pytest.raises(ValueError, match="'Open' と 'Close'"):
        oracle_index_open_gap_regime_signal(invalid_df)


def test_empty_dataframe_raises_error() -> None:
    with pytest.raises(ValueError, match="index_data が空またはNoneです"):
        oracle_index_open_gap_regime_signal(pd.DataFrame())


def test_invalid_regime_raises_error(sample_index_data: pd.DataFrame) -> None:
    with pytest.raises(ValueError, match="regime が不正です"):
        oracle_index_open_gap_regime_signal(
            sample_index_data,
            regime="invalid_regime",  # type: ignore[arg-type]
        )


def test_zero_prev_close_is_treated_as_false() -> None:
    index = pd.date_range("2024-01-01", periods=3, freq="D")
    data = pd.DataFrame(
        {
            "Open": [100.0, 99.0, 98.0],
            "Close": [100.0, 0.0, 100.0],
        },
        index=index,
    )

    signal = oracle_index_open_gap_regime_signal(
        data,
        regime="down_medium",
    )

    assert signal.tolist() == [False, True, False]
