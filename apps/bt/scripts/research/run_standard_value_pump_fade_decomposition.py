#!/usr/bin/env python3
"""Runner-first entrypoint for Standard value pump/fade decomposition."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

import pandas as pd


def _ensure_bt_root_on_path() -> Path:
    bt_root = Path(__file__).resolve().parents[2]
    bt_root_str = str(bt_root)
    if bt_root_str not in sys.path:
        sys.path.insert(0, bt_root_str)
    return bt_root


_BT_ROOT = _ensure_bt_root_on_path()

from scripts.research.common import (  # noqa: E402
    add_bundle_output_arguments,
    emit_bundle_payload,
    ensure_bt_workdir,
)
from src.application.services.ranking_service import RankingService  # noqa: E402
from src.domains.analytics.annual_fundamental_confounder_analysis import (  # noqa: E402
    POSITIVE_RATIO_ONLY_COLUMNS,
)
from src.domains.analytics.annual_value_breakout_periodic_rebalance import (  # noqa: E402
    run_annual_value_breakout_periodic_rebalance,
)
from src.domains.analytics.standard_value_pump_fade_decomposition import (  # noqa: E402
    DEFAULT_FORWARD_HORIZONS,
    DEFAULT_TOP_RANKS,
    run_standard_value_pump_fade_decomposition_from_frames,
    write_standard_value_pump_fade_decomposition_bundle,
)
from src.infrastructure.db.market.market_reader import MarketDbReader  # noqa: E402
from src.shared.paths.resolver import get_data_dir  # noqa: E402


def _default_db_path() -> str:
    return str((get_data_dir() / "market-timeseries" / "market.duckdb").resolve())


def _parse_csv_ints(value: str) -> tuple[int, ...]:
    try:
        return tuple(int(part.strip()) for part in value.split(",") if part.strip())
    except ValueError as exc:
        raise argparse.ArgumentTypeError("value must be comma-separated integers") from exc


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Decompose PIT Standard value+breakout candidates into ordinary value "
            "versus pump/fade-like technical/liquidity buckets."
        )
    )
    parser.add_argument(
        "--db-path",
        default=_default_db_path(),
        help="market.duckdb path. Defaults to the active XDG market snapshot.",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=2018,
        help="First calendar year for PIT rebalance snapshots. Default: 2018.",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=None,
        help="Last calendar year. Defaults to the latest available year.",
    )
    parser.add_argument(
        "--selection-count",
        type=int,
        default=100,
        help="Top-N candidates per PIT rebalance snapshot. Default: 100.",
    )
    parser.add_argument(
        "--rebalance-months",
        type=int,
        default=3,
        help="PIT rebalance cadence reused from value+breakout research. Default: 3.",
    )
    parser.add_argument(
        "--forward-horizons",
        type=_parse_csv_ints,
        default=DEFAULT_FORWARD_HORIZONS,
        help="Comma-separated forward horizons in sessions. Default: 20,60.",
    )
    parser.add_argument(
        "--top-ranks",
        type=_parse_csv_ints,
        default=DEFAULT_TOP_RANKS,
        help="Comma-separated rank buckets to retain in metadata. Default: 25,50,100.",
    )
    parser.add_argument(
        "--include-current-ranking-snapshot",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Append the current production Ranking snapshot as current examples. "
            "Historical outcome rows still come from PIT rebalance snapshots."
        ),
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def _selected_events_to_snapshot_frame(selected_event_df: pd.DataFrame) -> pd.DataFrame:
    if selected_event_df.empty:
        return pd.DataFrame()
    frame = selected_event_df.copy()
    result = pd.DataFrame(
        {
            "snapshot_date": frame["signal_date"].astype(str),
            "rank": pd.to_numeric(frame["selection_rank"], errors="coerce"),
            "code": frame["code"].astype(str),
            "company_name": frame["company_name"].astype(str),
            "market_code": frame["market_code"].astype(str),
            "score": pd.to_numeric(frame["composite_score"], errors="coerce"),
            "score_before_boost": pd.to_numeric(
                frame["value_composite_score"],
                errors="coerce",
            ),
            "breakout_boost": pd.to_numeric(frame["composite_score"], errors="coerce")
            - pd.to_numeric(frame["value_composite_score"], errors="coerce"),
            "pbr": pd.to_numeric(frame["pbr"], errors="coerce"),
            "forward_per": pd.to_numeric(frame["forward_per"], errors="coerce"),
            "market_cap_bil_jpy": pd.to_numeric(frame["market_cap_bil_jpy"], errors="coerce"),
            "avg_trading_value_60d_mil_jpy": pd.to_numeric(
                frame["avg_trading_value_60d_mil_jpy"],
                errors="coerce",
            ),
        }
    )
    return result.dropna(subset=["snapshot_date", "rank", "code"]).reset_index(drop=True)


def _append_current_ranking_snapshot(
    reader: MarketDbReader,
    snapshot_df: pd.DataFrame,
    *,
    limit: int,
) -> pd.DataFrame:
    service = RankingService(reader)
    response = service.get_value_composite_ranking(
        date=None,
        limit=limit,
        markets="standard",
        profile_id="standard_breakout_120d20",
        forward_eps_mode="latest",
        apply_liquidity_filter=True,
    )
    rows: list[dict[str, Any]] = []
    for item in response.model_dump(mode="json")["items"]:
        metrics = item.get("technicalMetrics") or {}
        rows.append(
            {
                "snapshot_date": response.date,
                "rank": item.get("rank"),
                "code": item.get("code"),
                "company_name": item.get("companyName"),
                "market_code": item.get("marketCode"),
                "score": item.get("score"),
                "score_before_boost": item.get("scoreBeforeBoost"),
                "breakout_boost": item.get("breakoutBoost"),
                "pbr": item.get("pbr"),
                "forward_per": item.get("forwardPer"),
                "market_cap_bil_jpy": item.get("marketCapBilJpy"),
                "avg_trading_value_60d_mil_jpy": metrics.get(
                    "avgTradingValue60dMilJpy"
                ),
            }
        )
    current_df = pd.DataFrame.from_records(rows)
    if snapshot_df.empty:
        return current_df
    return pd.concat([snapshot_df, current_df], ignore_index=True)


def _load_price_history(
    reader: MarketDbReader,
    *,
    codes: list[str],
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    if not codes:
        return pd.DataFrame(columns=["code", "date", "open", "high", "low", "close", "volume"])
    placeholders = ",".join("?" for _ in codes)
    sql = f"""
        WITH normalized AS (
            SELECT
                regexp_replace(code, '[^0-9A-Za-z]', '', 'g') AS code,
                date,
                open,
                high,
                low,
                close,
                volume,
                row_number() over (
                    partition by regexp_replace(code, '[^0-9A-Za-z]', '', 'g'), date
                    order by case when length(code) = 4 then 0 else 1 end, code
                ) AS rn
            FROM stock_data
            WHERE date >= ?
              AND date <= ?
              AND regexp_replace(code, '[^0-9A-Za-z]', '', 'g') IN ({placeholders})
        )
        SELECT code, date, open, high, low, close, volume
        FROM normalized
        WHERE rn = 1
        ORDER BY code, date
    """
    rows = [dict(row) for row in reader.query(sql, (start_date, end_date, *codes))]
    return pd.DataFrame.from_records(rows)


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)

    base_result = run_annual_value_breakout_periodic_rebalance(
        args.db_path,
        markets=("standard",),
        rebalance_months=(args.rebalance_months,),
        selection_counts=(args.selection_count,),
        score_methods=("prime_size_tilt",),
        liquidity_scenarios=("adv10m",),
        breakout_policies=("breakout_additive",),
        breakout_windows=(120,),
        breakout_lookback_sessions=(20,),
        breakout_score_boost=0.10,
        start_year=args.start_year,
        end_year=args.end_year,
        required_positive_columns=POSITIVE_RATIO_ONLY_COLUMNS,
        include_incomplete_last_period=True,
        skip_portfolio_curves=True,
    )
    snapshot_df = _selected_events_to_snapshot_frame(base_result.selected_event_df)

    reader = MarketDbReader(args.db_path, read_only=True)
    try:
        if args.include_current_ranking_snapshot:
            snapshot_df = _append_current_ranking_snapshot(
                reader,
                snapshot_df,
                limit=args.selection_count,
            )
        min_snapshot = pd.to_datetime(snapshot_df["snapshot_date"]).min()
        max_snapshot = pd.to_datetime(snapshot_df["snapshot_date"]).max()
        start_date = (min_snapshot - pd.DateOffset(years=2, months=2)).strftime("%Y-%m-%d")
        end_date = (max_snapshot + pd.DateOffset(months=6)).strftime("%Y-%m-%d")
        codes = sorted(set(snapshot_df["code"].astype(str)))
        price_history_df = _load_price_history(
            reader,
            codes=codes,
            start_date=start_date,
            end_date=end_date,
        )
    finally:
        reader.close()

    result = run_standard_value_pump_fade_decomposition_from_frames(
        db_path=args.db_path,
        ranking_snapshot_df=snapshot_df,
        price_history_df=price_history_df,
        forward_horizons=args.forward_horizons,
        top_ranks=args.top_ranks,
        score_profile="standard_breakout_120d20_pit_periodic_rebalance",
    )
    bundle = write_standard_value_pump_fade_decomposition_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
