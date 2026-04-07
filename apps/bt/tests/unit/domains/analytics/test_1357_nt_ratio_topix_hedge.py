from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import cast

import duckdb
import pandas as pd
import pandas.testing as pdt
import pytest

from src.domains.analytics import topix_close_stock_overnight_distribution as topix_analysis_module
from src.domains.analytics.hedge_1357_nt_ratio_topix import (
    HEDGE_1357_NT_RATIO_TOPIX_RESEARCH_EXPERIMENT_ID,
    _bucket_nt_ratio_return,
    _bucket_topix_close_return,
    _expected_shortfall,
    get_1357_nt_ratio_topix_hedge_bundle_path_for_run_id,
    get_1357_nt_ratio_topix_hedge_latest_bundle_path,
    load_1357_nt_ratio_topix_hedge_research_bundle,
    run_1357_nt_ratio_topix_hedge_research,
    write_1357_nt_ratio_topix_hedge_research_bundle,
)


@dataclass(frozen=True)
class SyntheticSeries:
    dates: list[str]
    topix_open: list[float]
    topix_close: list[float]
    n225_close: list[float]
    etf_open: list[float]
    etf_close: list[float]
    stock_7203_open: list[float]
    stock_7203_close: list[float]


def _build_ohlc_from_returns(
    dates: list[str],
    daily_returns: list[float],
    *,
    start_price: float,
) -> tuple[list[float], list[float], list[float], list[float]]:
    opens: list[float] = []
    highs: list[float] = []
    lows: list[float] = []
    closes: list[float] = []
    previous_close = start_price
    for index, _ in enumerate(dates):
        daily_return = daily_returns[index]
        if index == 0:
            open_price = start_price
            close_price = start_price
        else:
            overnight_return = daily_return * 0.4
            open_price = previous_close * (1.0 + overnight_return)
            close_price = previous_close * (1.0 + daily_return)
        high_price = max(open_price, close_price) * 1.01
        low_price = min(open_price, close_price) * 0.99
        opens.append(open_price)
        highs.append(high_price)
        lows.append(low_price)
        closes.append(close_price)
        previous_close = close_price
    return opens, highs, lows, closes


def _synthetic_topix_returns(count: int) -> list[float]:
    returns = [0.0]
    for index in range(1, count):
        if index in {20, 40, 60}:
            returns.append(-0.03)
        elif index in {21, 41, 61}:
            returns.append(-0.015)
        elif index < 15:
            returns.append(0.002)
        elif index < 30:
            returns.append(-0.004)
        elif index < 45:
            returns.append(0.001 if index % 2 == 0 else -0.001)
        elif index < 60:
            returns.append(-0.003)
        else:
            returns.append(-0.005)
    return returns


def _synthetic_nt_ratio_returns(count: int) -> list[float]:
    returns = [0.0] * count
    overrides = {
        20: 0.08,
        21: 0.03,
        40: -0.08,
        41: -0.03,
        60: 0.12,
        61: 0.04,
    }
    for index, value in overrides.items():
        returns[index] = value
    return returns


def _build_series() -> SyntheticSeries:
    dates = pd.bdate_range("2024-01-01", periods=80).strftime("%Y-%m-%d").tolist()
    topix_returns = _synthetic_topix_returns(len(dates))
    nt_ratio_returns = _synthetic_nt_ratio_returns(len(dates))
    topix_open, _, _, topix_close = _build_ohlc_from_returns(
        dates,
        topix_returns,
        start_price=100.0,
    )

    nt_ratio = [10.0]
    for index in range(1, len(dates)):
        nt_ratio.append(nt_ratio[-1] * (1.0 + nt_ratio_returns[index]))
    n225_close = [ratio * close for ratio, close in zip(nt_ratio, topix_close, strict=True)]

    etf_returns = [0.0]
    for index in range(1, len(dates)):
        topix_ret = topix_returns[index]
        nt_ret = nt_ratio_returns[index]
        positive_nt = max(nt_ret, 0.0)
        negative_nt = max(-nt_ret, 0.0)
        etf_returns.append((-1.7 * topix_ret) + (1.5 * positive_nt) - (1.0 * negative_nt))
    etf_open, _, _, etf_close = _build_ohlc_from_returns(
        dates,
        etf_returns,
        start_price=1000.0,
    )

    stock_7203_returns = [0.0]
    for index in range(1, len(dates)):
        topix_ret = topix_returns[index]
        nt_ret = nt_ratio_returns[index]
        positive_nt = max(nt_ret, 0.0)
        negative_nt = max(-nt_ret, 0.0)
        stock_7203_returns.append((1.15 * topix_ret) - (0.15 * negative_nt) + (0.10 * positive_nt))
    stock_7203_open, _, _, stock_7203_close = _build_ohlc_from_returns(
        dates,
        stock_7203_returns,
        start_price=200.0,
    )

    return SyntheticSeries(
        dates=dates,
        topix_open=topix_open,
        topix_close=topix_close,
        n225_close=n225_close,
        etf_open=etf_open,
        etf_close=etf_close,
        stock_7203_open=stock_7203_open,
        stock_7203_close=stock_7203_close,
    )


def _build_market_db(db_path: Path) -> tuple[str, SyntheticSeries]:
    series = _build_series()
    conn = duckdb.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE stocks (
            code TEXT PRIMARY KEY,
            company_name TEXT NOT NULL,
            company_name_english TEXT,
            market_code TEXT NOT NULL,
            market_name TEXT NOT NULL,
            sector_17_code TEXT NOT NULL,
            sector_17_name TEXT NOT NULL,
            sector_33_code TEXT NOT NULL,
            sector_33_name TEXT NOT NULL,
            scale_category TEXT,
            listed_date TEXT NOT NULL,
            created_at TEXT,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE stock_data (
            code TEXT NOT NULL,
            date TEXT NOT NULL,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT,
            adjustment_factor DOUBLE,
            created_at TEXT,
            PRIMARY KEY (code, date)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE topix_data (
            date TEXT PRIMARY KEY,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            created_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE indices_data (
            code TEXT NOT NULL,
            date TEXT NOT NULL,
            close DOUBLE,
            PRIMARY KEY (code, date)
        )
        """
    )

    conn.executemany(
        "INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("7203", "Topix100 Prime", "TOPIX100 PRIME", "0111", "プライム", "1", "A", "1", "A", "TOPIX Large70", "2000-01-01", None, None),
            ("72030", "Duplicate Topix100", "DUPLICATE TOPIX100", "0111", "プライム", "1", "A", "1", "A", "TOPIX Large70", "2000-01-01", None, None),
            ("1111", "Prime Ex", "PRIME EX", "0111", "プライム", "1", "A", "1", "A", "-", "2000-01-01", None, None),
            ("2222", "Topix500 Mid400", "TOPIX500 MID400", "0111", "プライム", "1", "A", "1", "A", "TOPIX Mid400", "2000-01-01", None, None),
            ("3333", "Growth", "GROWTH", "0113", "グロース", "1", "A", "1", "A", "-", "2000-01-01", None, None),
            ("1357", "Double Inverse ETF", "DOUBLE INVERSE ETF", "9999", "ETF", "1", "A", "1", "A", "-", "2000-01-01", None, None),
        ],
    )

    topix_returns = _synthetic_topix_returns(len(series.dates))
    nt_ratio_returns = _synthetic_nt_ratio_returns(len(series.dates))
    stock_1111_returns = [0.0]
    stock_2222_returns = [0.0]
    stock_3333_returns = [0.0]
    for index in range(1, len(series.dates)):
        topix_ret = topix_returns[index]
        nt_ret = nt_ratio_returns[index]
        positive_nt = max(nt_ret, 0.0)
        negative_nt = max(-nt_ret, 0.0)
        stock_1111_returns.append((0.85 * topix_ret) + (0.05 * negative_nt) - (0.05 * positive_nt))
        stock_2222_returns.append((1.00 * topix_ret) - (0.05 * negative_nt) + (0.02 * positive_nt))
        stock_3333_returns.append((1.30 * topix_ret) - (0.10 * negative_nt))

    stock_1111_open, _, _, stock_1111_close = _build_ohlc_from_returns(
        series.dates,
        stock_1111_returns,
        start_price=150.0,
    )
    stock_2222_open, _, _, stock_2222_close = _build_ohlc_from_returns(
        series.dates,
        stock_2222_returns,
        start_price=180.0,
    )
    stock_3333_open, _, _, stock_3333_close = _build_ohlc_from_returns(
        series.dates,
        stock_3333_returns,
        start_price=120.0,
    )

    topix_rows = [
        (
            date_value,
            series.topix_open[index],
            max(series.topix_open[index], series.topix_close[index]) * 1.01,
            min(series.topix_open[index], series.topix_close[index]) * 0.99,
            series.topix_close[index],
            None,
        )
        for index, date_value in enumerate(series.dates)
    ]
    conn.executemany("INSERT INTO topix_data VALUES (?, ?, ?, ?, ?, ?)", topix_rows)

    indices_rows = [
        (_NIKKEI_SYNTHETIC_INDEX_CODE, date_value, series.n225_close[index])
        for index, date_value in enumerate(series.dates)
    ]
    conn.executemany("INSERT INTO indices_data VALUES (?, ?, ?)", indices_rows)

    stock_rows: list[tuple[object, ...]] = []
    for index, date_value in enumerate(series.dates):
        stock_rows.extend(
            [
                ("1357", date_value, series.etf_open[index], max(series.etf_open[index], series.etf_close[index]) * 1.01, min(series.etf_open[index], series.etf_close[index]) * 0.99, series.etf_close[index], 1000, 1.0, None),
                ("7203", date_value, series.stock_7203_open[index], max(series.stock_7203_open[index], series.stock_7203_close[index]) * 1.01, min(series.stock_7203_open[index], series.stock_7203_close[index]) * 0.99, series.stock_7203_close[index], 1000, 1.0, None),
                ("72030", date_value, 50.0, 50.5, 49.5, 50.0, 1000, 1.0, None),
                ("1111", date_value, stock_1111_open[index], max(stock_1111_open[index], stock_1111_close[index]) * 1.01, min(stock_1111_open[index], stock_1111_close[index]) * 0.99, stock_1111_close[index], 1000, 1.0, None),
                ("2222", date_value, stock_2222_open[index], max(stock_2222_open[index], stock_2222_close[index]) * 1.01, min(stock_2222_open[index], stock_2222_close[index]) * 0.99, stock_2222_close[index], 1000, 1.0, None),
                ("3333", date_value, stock_3333_open[index], max(stock_3333_open[index], stock_3333_close[index]) * 1.01, min(stock_3333_open[index], stock_3333_close[index]) * 0.99, stock_3333_close[index], 1000, 1.0, None),
            ]
        )
    conn.executemany("INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", stock_rows)
    conn.close()
    return str(db_path), series


_NIKKEI_SYNTHETIC_INDEX_CODE = "N225_UNDERPX"


@pytest.fixture(scope="module")
def analytics_db(tmp_path_factory: pytest.TempPathFactory) -> tuple[str, SyntheticSeries]:
    tmp_dir = tmp_path_factory.mktemp("hedge-1357-nt-ratio-topix")
    return _build_market_db(tmp_dir / "market.duckdb")


@pytest.fixture(scope="module")
def default_result(analytics_db: tuple[str, SyntheticSeries]):
    db_path, _ = analytics_db
    return run_1357_nt_ratio_topix_hedge_research(db_path)


@pytest.fixture(scope="module")
def configured_result(analytics_db: tuple[str, SyntheticSeries]):
    db_path, _ = analytics_db
    return run_1357_nt_ratio_topix_hedge_research(
        db_path,
        selected_groups=["TOPIX100", "TOPIX500"],
        fixed_weights=[0.2, 0.4],
    )


def test_available_range_and_forward_returns_are_returned(
    analytics_db: tuple[str, SyntheticSeries],
    default_result,
) -> None:
    _, series = analytics_db
    result = default_result

    assert result.available_start_date == series.dates[0]
    assert result.available_end_date == series.dates[-1]
    assert result.analysis_start_date == series.dates[1]
    assert result.analysis_end_date == series.dates[-6]

    event_index = 20
    event_date = series.dates[event_index]
    row = result.daily_market_df[result.daily_market_df["date"] == event_date]
    assert len(row) == 1
    event_row = row.iloc[0]

    expected_next_overnight = (
        series.etf_open[event_index + 1] - series.etf_close[event_index]
    ) / series.etf_close[event_index]
    expected_next_intraday = (
        series.etf_close[event_index + 1] - series.etf_open[event_index + 1]
    ) / series.etf_open[event_index + 1]
    expected_next_close = (
        series.etf_close[event_index + 1] - series.etf_close[event_index]
    ) / series.etf_close[event_index]
    expected_forward_3d = (
        series.etf_close[event_index + 3] - series.etf_close[event_index]
    ) / series.etf_close[event_index]
    expected_forward_5d = (
        series.etf_close[event_index + 5] - series.etf_close[event_index]
    ) / series.etf_close[event_index]

    assert event_row["etf_next_overnight_return"] == pytest.approx(expected_next_overnight)
    assert event_row["etf_next_intraday_return"] == pytest.approx(expected_next_intraday)
    assert event_row["etf_next_close_to_close_return"] == pytest.approx(expected_next_close)
    assert event_row["etf_forward_3d_close_to_close_return"] == pytest.approx(expected_forward_3d)
    assert event_row["etf_forward_5d_close_to_close_return"] == pytest.approx(expected_forward_5d)


def test_bucket_boundaries_and_regime_flags_are_correct(
    analytics_db: tuple[str, SyntheticSeries],
    default_result,
) -> None:
    _, series = analytics_db
    result = default_result

    assert result.topix_close_stats is not None
    assert result.nt_ratio_stats is not None

    topix_stats = result.topix_close_stats
    nt_stats = result.nt_ratio_stats

    assert _bucket_topix_close_return(-topix_stats.threshold_2, stats=topix_stats) == "close_le_negative_threshold_2"
    assert _bucket_topix_close_return(-topix_stats.threshold_1, stats=topix_stats) == "close_negative_threshold_2_to_1"
    assert _bucket_topix_close_return(topix_stats.threshold_1, stats=topix_stats) == "close_threshold_1_to_2"
    assert _bucket_nt_ratio_return(nt_stats.lower_threshold_2, stats=nt_stats) == "return_le_mean_minus_2sd"
    assert _bucket_nt_ratio_return(nt_stats.upper_threshold_1, stats=nt_stats) == "return_mean_plus_1sd_to_plus_2sd"
    assert _bucket_nt_ratio_return(nt_stats.upper_threshold_2, stats=nt_stats) == "return_ge_mean_plus_2sd"

    stress_date = series.dates[60]
    stress_row = result.daily_market_df[result.daily_market_df["date"] == stress_date].iloc[0]
    assert bool(stress_row["shock_topix_le_negative_threshold_2"])
    assert bool(stress_row["shock_joint_adverse"])
    assert bool(stress_row["trend_ma_bearish"])
    assert bool(stress_row["hybrid_bearish_joint"])

    macd_date = series.dates[61]
    macd_row = result.daily_market_df[result.daily_market_df["date"] == macd_date].iloc[0]
    close_series = pd.Series(series.topix_close, index=series.dates, dtype=float)
    expected_fast = close_series.ewm(span=12, adjust=False, min_periods=12).mean()
    expected_slow = close_series.ewm(span=26, adjust=False, min_periods=26).mean()
    expected_macd = expected_fast - expected_slow
    expected_signal = expected_macd.ewm(span=9, adjust=False, min_periods=9).mean()
    expected_histogram = expected_macd - expected_signal
    assert macd_row["topix_macd_histogram"] == pytest.approx(
        float(expected_histogram.loc[macd_date])
    )
    assert bool(macd_row["trend_macd_negative"])
    assert result.macd_basis == "ema_adjust_false"
    assert (
        result.macd_fast_period,
        result.macd_slow_period,
        result.macd_signal_period,
    ) == (12, 26, 9)


def test_proxy_returns_and_hedge_metrics_are_consistent(
    analytics_db: tuple[str, SyntheticSeries],
    default_result,
) -> None:
    _, series = analytics_db
    result = default_result

    event_index = 20
    event_date = series.dates[event_index]
    proxy_row = result.daily_proxy_returns_df[
        (result.daily_proxy_returns_df["date"] == event_date)
        & (result.daily_proxy_returns_df["stock_group"] == "TOPIX100")
    ]
    assert len(proxy_row) == 1
    topix100_row = proxy_row.iloc[0]

    expected_long_next_close = (
        series.stock_7203_close[event_index + 1] - series.stock_7203_close[event_index]
    ) / series.stock_7203_close[event_index]
    expected_long_forward_5d = (
        series.stock_7203_close[event_index + 5] - series.stock_7203_close[event_index]
    ) / series.stock_7203_close[event_index]
    assert topix100_row["constituent_count"] == 1
    assert topix100_row["long_next_close_to_close_return"] == pytest.approx(expected_long_next_close)
    assert topix100_row["long_forward_5d_close_to_close_return"] == pytest.approx(expected_long_forward_5d)

    metrics_row = result.hedge_metrics_df[
        (result.hedge_metrics_df["stock_group"] == "TOPIX100")
        & (result.hedge_metrics_df["split"] == "overall")
        & (result.hedge_metrics_df["target_name"] == "next_close_to_close")
        & (result.hedge_metrics_df["rule_name"] == "shock_topix_le_negative_threshold_2")
        & (result.hedge_metrics_df["weight_label"] == "fixed_0.30")
    ]
    assert len(metrics_row) == 1
    metrics = metrics_row.iloc[0]

    base_df = result.daily_proxy_returns_df[
        result.daily_proxy_returns_df["stock_group"] == "TOPIX100"
    ].dropna(
        subset=[
            "long_next_close_to_close_return",
            "etf_next_close_to_close_return",
            "shock_topix_le_negative_threshold_2",
        ]
    )
    manual_hedged = base_df["long_next_close_to_close_return"] + 0.3 * base_df[
        "etf_next_close_to_close_return"
    ].where(base_df["shock_topix_le_negative_threshold_2"], 0.0)

    assert metrics["hedged_mean_return"] == pytest.approx(float(manual_hedged.mean()))
    assert metrics["expected_shortfall_improvement"] == pytest.approx(
        cast(float, _expected_shortfall(manual_hedged))
        - cast(float, _expected_shortfall(base_df["long_next_close_to_close_return"]))
    )

    etf_metrics_row = result.etf_strategy_metrics_df[
        (result.etf_strategy_metrics_df["split"] == "overall")
        & (result.etf_strategy_metrics_df["target_name"] == "next_close_to_close")
        & (result.etf_strategy_metrics_df["rule_name"] == "shock_topix_le_negative_threshold_2")
    ]
    assert len(etf_metrics_row) == 1
    etf_metrics = etf_metrics_row.iloc[0]
    manual_etf_strategy = base_df["etf_next_close_to_close_return"].where(
        base_df["shock_topix_le_negative_threshold_2"],
        0.0,
    )
    manual_active = base_df.loc[
        base_df["shock_topix_le_negative_threshold_2"],
        "etf_next_close_to_close_return",
    ]
    assert etf_metrics["strategy_mean_return"] == pytest.approx(
        float(manual_etf_strategy.mean())
    )
    assert etf_metrics["strategy_total_return"] == pytest.approx(
        float((1.0 + manual_etf_strategy).prod() - 1.0)
    )
    assert etf_metrics["mean_return_when_active"] == pytest.approx(float(manual_active.mean()))

    assert len(result.shortlist_df) <= 3
    assert set(result.shortlist_df.columns).issuperset(
        {"stock_group", "target_name", "rule_name", "weight_label", "score"}
    )
    assert not result.etf_strategy_split_comparison_df.empty
    assert not result.annual_rule_summary_df.empty


def test_hedge_bundle_roundtrip(
    configured_result,
    tmp_path: Path,
) -> None:
    result = configured_result

    bundle = write_1357_nt_ratio_topix_hedge_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260401_121500_testabcd",
    )
    reloaded = load_1357_nt_ratio_topix_hedge_research_bundle(bundle.bundle_dir)

    assert bundle.experiment_id == HEDGE_1357_NT_RATIO_TOPIX_RESEARCH_EXPERIMENT_ID
    assert bundle.summary_path.exists()
    assert (
        get_1357_nt_ratio_topix_hedge_bundle_path_for_run_id(
            bundle.run_id,
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_1357_nt_ratio_topix_hedge_latest_bundle_path(output_root=tmp_path)
        == bundle.bundle_dir
    )
    assert reloaded.topix_close_stats == result.topix_close_stats
    assert reloaded.nt_ratio_stats == result.nt_ratio_stats
    assert reloaded.fixed_weights == (0.2, 0.4)
    pdt.assert_frame_equal(
        reloaded.hedge_metrics_df,
        result.hedge_metrics_df,
        check_dtype=False,
    )


def test_snapshot_fallback_preserves_results(
    analytics_db: tuple[str, SyntheticSeries],
    default_result,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path, _ = analytics_db
    normal_result = default_result

    real_connect = topix_analysis_module._connect_duckdb
    call_count = {"value": 0}

    def flaky_connect(path: str, *, read_only: bool = True):
        call_count["value"] += 1
        if call_count["value"] == 1:
            raise RuntimeError("Could not set lock on file: conflicting lock is held")
        return real_connect(path, read_only=read_only)

    monkeypatch.setattr(topix_analysis_module, "_connect_duckdb", flaky_connect)
    snapshot_result = run_1357_nt_ratio_topix_hedge_research(db_path)

    assert snapshot_result.source_mode == "snapshot"
    pdt.assert_frame_equal(
        normal_result.daily_market_df.reset_index(drop=True),
        snapshot_result.daily_market_df.reset_index(drop=True),
    )
    pdt.assert_frame_equal(
        normal_result.hedge_metrics_df.sort_values(
            ["split", "stock_group", "target_name", "rule_name", "weight_label"]
        ).reset_index(drop=True),
        snapshot_result.hedge_metrics_df.sort_values(
            ["split", "stock_group", "target_name", "rule_name", "weight_label"]
        ).reset_index(drop=True),
    )
