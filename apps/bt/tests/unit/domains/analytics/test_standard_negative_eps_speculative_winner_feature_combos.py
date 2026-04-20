from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pytest

import src.domains.analytics.standard_negative_eps_speculative_winner_feature_combos as study_mod
from src.domains.analytics.standard_negative_eps_speculative_winner_feature_combos import (
    DEFAULT_SCOPE_NAME,
    STANDARD_NEGATIVE_EPS_SPECULATIVE_WINNER_FEATURE_COMBOS_EXPERIMENT_ID,
    StandardNegativeEpsSpeculativeWinnerFeatureCombosResult,
    _apply_sector_bucket_collapse,
    _attach_winner_labels,
    _bucket_cfo_margin,
    _bucket_entry_adv,
    _bucket_entry_market_cap,
    _bucket_entry_open,
    _bucket_equity_ratio,
    _bucket_pre_entry_volatility_20d,
    _bucket_prior_20d_return,
    _bucket_prior_63d_return,
    _bucket_prior_252d_return,
    _bucket_profit_margin,
    _bucket_volume_ratio_20d,
    _build_group_comparison_df,
    _build_summary_markdown,
    _build_top_examples_df,
    _build_winner_threshold_df,
    _compute_pre_entry_volatility_20d,
    _compute_prior_return_pct,
    _compute_volume_ratio_20d,
    _empty_result_df,
    _fmt_num,
    _locate_date_index,
    _matching_combo_rows,
    _normalize_market,
    _ratio_pct,
    _series_stat,
    _top_summary_rows,
    get_standard_negative_eps_speculative_winner_feature_combos_bundle_path_for_run_id,
    get_standard_negative_eps_speculative_winner_feature_combos_latest_bundle_path,
    load_standard_negative_eps_speculative_winner_feature_combos_bundle,
    run_standard_negative_eps_speculative_winner_feature_combos,
    write_standard_negative_eps_speculative_winner_feature_combos_bundle,
)
from src.domains.analytics.standard_negative_eps_right_tail_decomposition import (
    StandardNegativeEpsRightTailResult,
)


def _append_price_rows(
    stock_rows: list[tuple[object, ...]],
    *,
    code: str,
    pre_closes: list[float],
    pre_volumes: list[int],
    entry_open: float,
    exit_close: float,
    include_exit_row: bool = True,
) -> None:
    pre_dates = pd.bdate_range(end="2024-05-10", periods=len(pre_closes)).strftime("%Y-%m-%d")
    for date_value, close_value, volume_value in zip(pre_dates, pre_closes, pre_volumes, strict=True):
        stock_rows.append(
            (
                code,
                str(date_value),
                float(close_value),
                float(close_value) + 0.5,
                float(close_value) - 0.5,
                float(close_value),
                int(volume_value),
                1.0,
                None,
            )
        )
    stock_rows.append(
        (
            code,
            "2024-05-13",
            float(entry_open),
            float(entry_open) + 0.5,
            float(entry_open) - 0.5,
            float(entry_open),
            int(pre_volumes[-1]),
            1.0,
            None,
        )
    )
    if include_exit_row:
        stock_rows.append(
            (
                code,
                "2025-05-09",
                float(exit_close),
                float(exit_close) + 0.5,
                float(exit_close) - 0.5,
                float(exit_close),
                int(pre_volumes[-1]),
                1.0,
                None,
            )
        )


def _statement_row(
    code: str,
    disclosed_date: str,
    *,
    eps: float | None,
    forecast_eps: float | None,
    next_year_forecast_eps: float | None,
    profit: float | None,
    equity: float | None,
    operating_cash_flow: float | None,
    sales: float | None,
    total_assets: float | None,
    period: str,
) -> tuple[object, ...]:
    return (
        code,
        disclosed_date,
        eps,
        profit,
        equity,
        period,
        None,
        next_year_forecast_eps,
        None,
        sales,
        None,
        None,
        operating_cash_flow,
        None,
        None,
        None,
        None,
        None,
        None,
        forecast_eps,
        None,
        None,
        None,
        total_assets,
        10_000_000.0,
        None,
    )


def _build_market_db(db_path: Path) -> str:
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
        CREATE TABLE statements (
            code TEXT NOT NULL,
            disclosed_date TEXT NOT NULL,
            earnings_per_share DOUBLE,
            profit DOUBLE,
            equity DOUBLE,
            type_of_current_period TEXT,
            type_of_document TEXT,
            next_year_forecast_earnings_per_share DOUBLE,
            bps DOUBLE,
            sales DOUBLE,
            operating_profit DOUBLE,
            ordinary_profit DOUBLE,
            operating_cash_flow DOUBLE,
            dividend_fy DOUBLE,
            forecast_dividend_fy DOUBLE,
            next_year_forecast_dividend_fy DOUBLE,
            payout_ratio DOUBLE,
            forecast_payout_ratio DOUBLE,
            next_year_forecast_payout_ratio DOUBLE,
            forecast_eps DOUBLE,
            investing_cash_flow DOUBLE,
            financing_cash_flow DOUBLE,
            cash_and_equivalents DOUBLE,
            total_assets DOUBLE,
            shares_outstanding DOUBLE,
            treasury_shares DOUBLE,
            PRIMARY KEY (code, disclosed_date)
        )
        """
    )

    cohort_a = [
        ("1001", "A Winner", "Services", 80.0, 200.0),
        ("1002", "A Runner", "Services", 82.0, 115.0),
        ("1003", "A Flat", "Services", 84.0, 92.0),
        ("1004", "A Loser", "Services", 86.0, 69.0),
    ]
    cohort_b = [
        ("2001", "B Winner", "Retail Trade", 70.0, 224.0),
        ("2002", "B Runner", "Retail Trade", 72.0, 94.0),
        ("2003", "B Flat", "Retail Trade", 74.0, 70.0),
        ("2004", "B Loser", "Retail Trade", 76.0, 46.0),
        ("2099", "B No Next FY", "Retail Trade", 78.0, 78.0),
    ]
    stocks = [
        (
            code,
            name,
            None,
            "0112",
            "Standard",
            "1",
            sector,
            "1",
            sector,
            "-",
            "2000-01-01",
            None,
            None,
        )
        for code, name, sector, _, _ in [*cohort_a, *cohort_b]
    ]
    conn.executemany(
        "INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        stocks,
    )

    price_rows: list[tuple[object, ...]] = []
    pre_closes_a = [float(value) for value in range(40, 80)]
    pre_volumes_a = [1_000] * 20 + [10_000] * 20
    pre_closes_b = [float(148 - 2 * idx) for idx in range(40)]
    pre_volumes_b = [10_000] * 20 + [3_000] * 20

    for code, _, _, entry_open, exit_close in cohort_a:
        _append_price_rows(
            price_rows,
            code=code,
            pre_closes=pre_closes_a,
            pre_volumes=pre_volumes_a,
            entry_open=entry_open,
            exit_close=exit_close,
        )
    for code, _, _, entry_open, exit_close in cohort_b:
        _append_price_rows(
            price_rows,
            code=code,
            pre_closes=pre_closes_b,
            pre_volumes=pre_volumes_b,
            entry_open=entry_open,
            exit_close=exit_close,
            include_exit_row=(code != "2099"),
        )
    conn.executemany("INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", price_rows)

    statement_rows: list[tuple[object, ...]] = []
    for code, _, _, _, _ in cohort_a:
        statement_rows.append(
            _statement_row(
                code,
                "2024-02-10",
                eps=None,
                forecast_eps=None,
                next_year_forecast_eps=None,
                profit=None,
                equity=None,
                operating_cash_flow=None,
                sales=None,
                total_assets=None,
                period="3Q",
            )
        )
        statement_rows.append(
            _statement_row(
                code,
                "2024-05-10",
                eps=-10.0,
                forecast_eps=5.0,
                next_year_forecast_eps=5.0,
                profit=-20.0,
                equity=700.0,
                operating_cash_flow=150.0,
                sales=1_000.0,
                total_assets=1_000.0,
                period="FY",
            )
        )
        statement_rows.append(
            _statement_row(
                code,
                "2025-05-12",
                eps=10.0,
                forecast_eps=12.0,
                next_year_forecast_eps=12.0,
                profit=60.0,
                equity=720.0,
                operating_cash_flow=180.0,
                sales=1_050.0,
                total_assets=1_020.0,
                period="FY",
            )
        )
    for code, _, _, _, _ in cohort_b:
        statement_rows.append(
            _statement_row(
                code,
                "2024-02-10",
                eps=None,
                forecast_eps=None,
                next_year_forecast_eps=None,
                profit=None,
                equity=None,
                operating_cash_flow=None,
                sales=None,
                total_assets=None,
                period="3Q",
            )
        )
        statement_rows.append(
            _statement_row(
                code,
                "2024-05-10",
                eps=-10.0,
                forecast_eps=None,
                next_year_forecast_eps=None,
                profit=-40.0,
                equity=250.0,
                operating_cash_flow=-50.0,
                sales=1_000.0,
                total_assets=1_000.0,
                period="FY",
            )
        )
        if code != "2099":
            statement_rows.append(
                _statement_row(
                    code,
                    "2025-05-12",
                    eps=10.0,
                    forecast_eps=12.0,
                    next_year_forecast_eps=12.0,
                    profit=40.0,
                    equity=300.0,
                    operating_cash_flow=20.0,
                    sales=980.0,
                    total_assets=990.0,
                    period="FY",
                )
            )
    conn.executemany(
        "INSERT INTO statements VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        statement_rows,
    )

    conn.close()
    return str(db_path)


@pytest.fixture
def analytics_db_path(tmp_path: Path) -> str:
    return _build_market_db(tmp_path / "market.duckdb")


def test_bucket_boundaries() -> None:
    assert _bucket_entry_market_cap(4.9) == "<5b"
    assert _bucket_entry_market_cap(5.0) == "5b-20b"
    assert _bucket_entry_market_cap(20.0) == "20b-50b"
    assert _bucket_entry_market_cap(50.0) == "50b-200b"
    assert _bucket_entry_market_cap(200.0) == ">=200b"

    assert _bucket_entry_adv(4_999_999.0) == "<5m"
    assert _bucket_entry_adv(5_000_000.0) == "5m-20m"
    assert _bucket_entry_adv(20_000_000.0) == "20m-100m"
    assert _bucket_entry_adv(100_000_000.0) == "100m-500m"
    assert _bucket_entry_adv(500_000_000.0) == ">=500m"

    assert _bucket_entry_open(99.0) == "<100"
    assert _bucket_entry_open(100.0) == "100-300"
    assert _bucket_entry_open(300.0) == "300-1000"
    assert _bucket_entry_open(1000.0) == ">=1000"

    assert _bucket_prior_20d_return(-40.0) == "<=-30%"
    assert _bucket_prior_20d_return(-5.0) == "-30% to 0%"
    assert _bucket_prior_20d_return(1.0) == ">0%"
    assert _bucket_prior_63d_return(-60.0) == "<=-50%"
    assert _bucket_prior_63d_return(-15.0) == "-50% to -10%"
    assert _bucket_prior_63d_return(5.0) == ">-10%"
    assert _bucket_prior_252d_return(-60.0) == "-80% to -50%"
    assert _bucket_prior_252d_return(-30.0) == "-50% to -20%"
    assert _bucket_prior_252d_return(-10.0) == ">-20%"

    assert _bucket_volume_ratio_20d(0.5) == "<0.7"
    assert _bucket_volume_ratio_20d(1.0) == "0.7-1.5"
    assert _bucket_volume_ratio_20d(1.6) == ">1.5"
    assert _bucket_pre_entry_volatility_20d(20.0) == "low"
    assert _bucket_pre_entry_volatility_20d(50.0) == "mid"
    assert _bucket_pre_entry_volatility_20d(100.0) == "high"
    assert _bucket_equity_ratio(20.0) == "<30%"
    assert _bucket_equity_ratio(40.0) == "30-50%"
    assert _bucket_equity_ratio(60.0) == ">=50%"
    assert _bucket_profit_margin(-1.0) == "<=0%"
    assert _bucket_profit_margin(3.0) == "0-5%"
    assert _bucket_profit_margin(8.0) == ">5%"
    assert _bucket_cfo_margin(-1.0) == "<=0%"
    assert _bucket_cfo_margin(5.0) == "0-10%"
    assert _bucket_cfo_margin(12.0) == ">10%"


def test_helper_edge_cases_and_empty_paths() -> None:
    assert _empty_result_df(["a", "b"]).empty
    assert _normalize_market(" standard ") == "standard"
    with pytest.raises(ValueError):
        _normalize_market("prime")

    empty_series = pd.Series(dtype=float)
    assert _series_stat(empty_series, "mean") is None
    assert _series_stat(pd.Series([1.0, 3.0]), "median") == pytest.approx(2.0)
    with pytest.raises(ValueError):
        _series_stat(pd.Series([1.0]), "q99")

    assert _ratio_pct(None, 1.0) is None
    assert _ratio_pct(1.0, 0.0) is None
    assert _ratio_pct(2.0, 4.0) == pytest.approx(50.0)

    price_frame = pd.DataFrame(
        {
            "date": ["2024-05-09", "2024-05-10", "2024-05-13"],
            "close": [10.0, 12.0, 11.0],
            "volume": [100, 100, 100],
        }
    )
    assert _locate_date_index(pd.DataFrame(), "2024-05-10") is None
    assert _locate_date_index(price_frame, "2024-05-14") is None
    assert _locate_date_index(price_frame, "2024-05-10") == 1

    assert _compute_prior_return_pct(price_frame, entry_idx=1, prior_sessions=5) == (None, 0)
    assert _compute_volume_ratio_20d(price_frame, entry_idx=0) == (None, 0)
    assert _compute_pre_entry_volatility_20d(price_frame, entry_idx=1) == (None, 0)
    assert _fmt_num(None) == "-"
    assert _fmt_num(3) == "3"

    assert _build_winner_threshold_df(pd.DataFrame(), winner_quantile=0.9).empty
    assert _attach_winner_labels(pd.DataFrame(), threshold_df=pd.DataFrame()).empty
    assert _apply_sector_bucket_collapse(pd.DataFrame(), sparse_sector_min_event_count=2).empty
    assert _build_group_comparison_df(
        pair_combo_summary_df=pd.DataFrame(),
        triplet_combo_summary_df=pd.DataFrame(),
    ).empty
    assert _top_summary_rows(pd.DataFrame(), cohort_key="forecast_positive__cfo_positive").empty


def test_matching_and_top_examples_empty_paths() -> None:
    event_row = pd.Series(
        {
            "cohort_key": "forecast_positive__cfo_positive",
            "entry_adv_bucket": "<5m",
            "entry_open_bucket": "<100",
        }
    )
    assert _matching_combo_rows(event_row, pd.DataFrame()).empty
    summary_df = pd.DataFrame(
        [
            {
                "cohort_key": "forecast_missing__cfo_non_positive",
                "feature_1_key": "entry_adv",
                "feature_1_bucket_label": "<5m",
                "feature_2_key": None,
                "feature_2_bucket_label": None,
                "feature_3_key": None,
                "feature_3_bucket_label": None,
            }
        ]
    )
    assert _matching_combo_rows(event_row, summary_df).empty

    unmatched_event_df = pd.DataFrame(
        [
            {
                "cohort_key": "forecast_positive__cfo_positive",
                "cohort_label": "Forecast > 0 / CFO > 0",
                "code": "1001",
                "company_name": "Example",
                "disclosed_date": "2024-05-10",
                "entry_date": "2024-05-13",
                "exit_date": "2025-05-09",
                "event_return_pct": 100.0,
                "winner_cutoff_pct": 50.0,
                "is_winner": True,
                "entry_market_cap_bucket": "<5b",
                "entry_adv_bucket": "<5m",
                "entry_open_bucket": "<100",
                "prior_20d_return_bucket": "missing",
                "prior_63d_return_bucket": "missing",
                "prior_252d_return_bucket": "missing",
                "volume_ratio_20d_bucket": "missing",
                "pre_entry_volatility_20d_bucket": "missing",
                "profit_margin_bucket": "missing",
                "cfo_margin_bucket": "missing",
                "equity_ratio_bucket": "missing",
                "sector_bucket": "other",
            }
        ]
    )
    assert _build_top_examples_df(
        unmatched_event_df,
        pair_combo_summary_df=pd.DataFrame(),
        triplet_combo_summary_df=pd.DataFrame(),
        top_examples_limit=4,
    ).empty


def test_summary_markdown_empty_result() -> None:
    empty = _empty_result_df(["x"])
    result = StandardNegativeEpsSpeculativeWinnerFeatureCombosResult(
        db_path="/tmp/market.duckdb",
        selected_market="standard",
        source_mode="live",
        source_detail="live",
        available_start_date=None,
        available_end_date=None,
        analysis_start_date=None,
        analysis_end_date=None,
        scope_name=DEFAULT_SCOPE_NAME,
        adv_window=20,
        winner_quantile=0.9,
        min_event_count=15,
        min_winner_count=3,
        top_examples_limit=20,
        sparse_sector_min_event_count=15,
        winner_threshold_df=pd.DataFrame(),
        feature_bucket_def_df=empty.copy(),
        event_feature_df=empty.copy(),
        single_feature_summary_df=empty.copy(),
        pair_combo_summary_df=empty.copy(),
        triplet_combo_summary_df=empty.copy(),
        group_comparison_df=empty.copy(),
        top_examples_df=empty.copy(),
    )

    summary = _build_summary_markdown(result)

    assert "No realized events were available" in summary


def _base_result_with_event_ledger(event_ledger_df: pd.DataFrame) -> StandardNegativeEpsRightTailResult:
    empty = pd.DataFrame()
    return StandardNegativeEpsRightTailResult(
        db_path="/tmp/market.duckdb",
        selected_market="standard",
        source_mode="live",
        source_detail="live",
        available_start_date=None,
        available_end_date=None,
        analysis_start_date=None,
        analysis_end_date=None,
        scope_name=DEFAULT_SCOPE_NAME,
        adv_window=20,
        liquidity_split_method="median",
        event_summary_df=empty.copy(),
        portfolio_daily_df=empty.copy(),
        portfolio_summary_df=empty.copy(),
        tail_concentration_df=empty.copy(),
        liquidity_thresholds_df=empty.copy(),
        top_winner_events_df=empty.copy(),
        event_ledger_df=event_ledger_df,
    )


def test_run_study_validation_and_empty_source_paths(monkeypatch) -> None:
    with pytest.raises(ValueError):
        run_standard_negative_eps_speculative_winner_feature_combos("/tmp/market.duckdb", market="prime")
    with pytest.raises(ValueError):
        run_standard_negative_eps_speculative_winner_feature_combos("/tmp/market.duckdb", adv_window=0)
    with pytest.raises(ValueError):
        run_standard_negative_eps_speculative_winner_feature_combos(
            "/tmp/market.duckdb",
            winner_quantile=1.0,
        )
    with pytest.raises(ValueError):
        run_standard_negative_eps_speculative_winner_feature_combos(
            "/tmp/market.duckdb",
            min_event_count=0,
        )
    with pytest.raises(ValueError):
        run_standard_negative_eps_speculative_winner_feature_combos(
            "/tmp/market.duckdb",
            min_winner_count=0,
        )
    with pytest.raises(ValueError):
        run_standard_negative_eps_speculative_winner_feature_combos(
            "/tmp/market.duckdb",
            top_examples_limit=0,
        )
    with pytest.raises(ValueError):
        run_standard_negative_eps_speculative_winner_feature_combos(
            "/tmp/market.duckdb",
            sparse_sector_min_event_count=0,
        )

    monkeypatch.setattr(
        study_mod,
        "run_standard_negative_eps_right_tail_decomposition",
        lambda db_path, market, adv_window: _base_result_with_event_ledger(pd.DataFrame()),
    )
    empty_result = run_standard_negative_eps_speculative_winner_feature_combos("/tmp/market.duckdb")
    assert empty_result.winner_threshold_df.empty
    assert empty_result.event_feature_df.empty

    non_target_ledger = pd.DataFrame(
        [
            {
                "status": "realized",
                "group_key": "forecast_missing__cfo_missing",
                "disclosed_date": "2024-05-10",
                "exit_date": "2025-05-09",
                "code": "1001",
            }
        ]
    )
    monkeypatch.setattr(
        study_mod,
        "run_standard_negative_eps_right_tail_decomposition",
        lambda db_path, market, adv_window: _base_result_with_event_ledger(non_target_ledger),
    )
    no_target_result = run_standard_negative_eps_speculative_winner_feature_combos("/tmp/market.duckdb")
    assert no_target_result.winner_threshold_df.empty
    assert no_target_result.event_feature_df.empty


def test_run_study_builds_winners_and_combo_tables(analytics_db_path: str) -> None:
    result = run_standard_negative_eps_speculative_winner_feature_combos(
        analytics_db_path,
        min_event_count=2,
        min_winner_count=1,
        top_examples_limit=4,
        sparse_sector_min_event_count=2,
    )

    assert result.selected_market == "standard"
    assert result.scope_name == "standard / FY actual EPS < 0"
    assert set(result.winner_threshold_df["cohort_key"]) == {
        "forecast_positive__cfo_positive",
        "forecast_missing__cfo_non_positive",
    }

    threshold_a = result.winner_threshold_df[
        result.winner_threshold_df["cohort_key"] == "forecast_positive__cfo_positive"
    ].iloc[0]
    threshold_b = result.winner_threshold_df[
        result.winner_threshold_df["cohort_key"] == "forecast_missing__cfo_non_positive"
    ].iloc[0]
    assert int(threshold_a["realized_event_count"]) == 4
    assert int(threshold_b["realized_event_count"]) == 4
    assert int(threshold_a["winner_event_count"]) == 1
    assert int(threshold_b["winner_event_count"]) == 1

    assert set(result.event_feature_df["code"]) == {
        "1001",
        "1002",
        "1003",
        "1004",
        "2001",
        "2002",
        "2003",
        "2004",
    }
    assert "2099" not in set(result.event_feature_df["code"])

    winner_codes = set(
        result.event_feature_df[result.event_feature_df["is_winner"]]["code"].astype(str)
    )
    assert winner_codes == {"1001", "2001"}

    row_1001 = result.event_feature_df[result.event_feature_df["code"] == "1001"].iloc[0]
    assert row_1001["prior_20d_return_pct"] == pytest.approx((79.0 / 60.0 - 1.0) * 100.0)
    assert row_1001["prior_20d_return_bucket"] == ">0%"
    assert row_1001["prior_252d_return_bucket"] == "missing"
    assert row_1001["volume_ratio_20d_bucket"] == ">1.5"
    assert row_1001["cfo_margin_bucket"] == ">10%"
    assert row_1001["equity_ratio_bucket"] == ">=50%"
    assert row_1001["sector_bucket"] == "Services"

    row_2001 = result.event_feature_df[result.event_feature_df["code"] == "2001"].iloc[0]
    assert row_2001["prior_20d_return_bucket"] == "<=-30%"
    assert row_2001["volume_ratio_20d_bucket"] == "<0.7"
    assert row_2001["cfo_margin_bucket"] == "<=0%"
    assert row_2001["equity_ratio_bucket"] == "<30%"
    assert row_2001["sector_bucket"] == "Retail Trade"

    assert not result.pair_combo_summary_df.empty
    assert (result.pair_combo_summary_df["event_count"] >= 2).all()
    assert result.pair_combo_summary_df["eligible_for_triplet_expansion"].any()

    assert not result.triplet_combo_summary_df.empty
    eligible_pair_keys = set(
        result.pair_combo_summary_df[
            result.pair_combo_summary_df["eligible_for_triplet_expansion"]
        ]["combo_key"].astype(str)
    )
    for row in result.triplet_combo_summary_df.to_dict(orient="records"):
        parent_keys = {part.strip() for part in str(row["parent_pair_combo_keys"]).split("||")}
        assert parent_keys & eligible_pair_keys

    assert not result.group_comparison_df.empty
    assert "shared" in set(result.group_comparison_df["strength_class"].astype(str))
    assert not result.top_examples_df.empty
    assert set(result.top_examples_df["code"].astype(str)) == {"1001", "2001"}


def test_bundle_roundtrip(analytics_db_path: str, tmp_path: Path) -> None:
    result = run_standard_negative_eps_speculative_winner_feature_combos(
        analytics_db_path,
        min_event_count=2,
        min_winner_count=1,
        top_examples_limit=4,
        sparse_sector_min_event_count=2,
    )
    bundle = write_standard_negative_eps_speculative_winner_feature_combos_bundle(
        result,
        output_root=tmp_path,
        run_id="unit-test-run",
    )

    assert bundle.experiment_id == STANDARD_NEGATIVE_EPS_SPECULATIVE_WINNER_FEATURE_COMBOS_EXPERIMENT_ID
    assert get_standard_negative_eps_speculative_winner_feature_combos_latest_bundle_path(
        output_root=tmp_path
    ) == bundle.bundle_dir
    assert (
        get_standard_negative_eps_speculative_winner_feature_combos_bundle_path_for_run_id(
            "unit-test-run",
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )

    loaded = load_standard_negative_eps_speculative_winner_feature_combos_bundle(bundle.bundle_dir)
    pd.testing.assert_frame_equal(
        loaded.winner_threshold_df,
        result.winner_threshold_df,
        check_dtype=False,
        check_like=True,
    )
    pd.testing.assert_frame_equal(
        loaded.pair_combo_summary_df,
        result.pair_combo_summary_df,
        check_dtype=False,
        check_like=True,
    )
