from __future__ import annotations

from dataclasses import replace
from datetime import date
from pathlib import Path

import duckdb
import pytest

from src.domains.analytics.daily_ranking_consumer_support import (
    DailyValuationPsrPercentileFeaturesRequest,
    build_daily_valuation_psr_percentile_features,
)
from src.domains.analytics.daily_ranking_research_base import (
    DailyRankingPanelRequest,
    build_daily_ranking_research_base,
)
from tests.unit.domains.analytics.test_ranking_short_red_evidence import (
    _build_short_red_db,
)


def test_forward_psr_builder_matches_actual_and_forecast_percent_rank(
    tmp_path: Path,
) -> None:
    conn, source = _build_psr_source(tmp_path / "market.duckdb", namespace="psr_a")

    features = build_daily_valuation_psr_percentile_features(
        conn,
        DailyValuationPsrPercentileFeaturesRequest(
            source=source,
            authority=source,
            namespace="crowded_forward_psr",
        ),
    )

    rows = conn.execute(
        f"""
        SELECT code, psr, psr_percentile, forecast_psr, forecast_psr_percentile
        FROM {features.name}
        WHERE date = DATE '2024-03-01'
        ORDER BY code
        """
    ).fetchall()
    assert rows
    assert rows[0][2] == pytest.approx(0.0)
    assert rows[-1][2] == pytest.approx(1.0)
    assert rows[0][4] == pytest.approx(1.0)
    assert rows[-1][4] == pytest.approx(0.0)
    assert all(row[1] is None or row[1] > 0 for row in rows)
    assert all(row[3] is None or row[3] > 0 for row in rows)
    conn.close()


def test_forward_psr_builder_rejects_copied_and_cross_generation_refs(
    tmp_path: Path,
) -> None:
    conn, source_a = _build_psr_source(tmp_path / "market.duckdb", namespace="psr_a")
    relations_b = build_daily_ranking_research_base(
        conn,
        DailyRankingPanelRequest(
            namespace="psr_b",
            analysis_start_date=date(2024, 3, 1),
            analysis_end_date=date(2024, 3, 5),
            horizons=(5,),
            market_scopes=("prime",),
            percentile_features=(),
        ),
    )
    source_b = relations_b.ranked_signals

    with pytest.raises(ValueError, match="exact issued|current|trusted"):
        build_daily_valuation_psr_percentile_features(
            conn,
            DailyValuationPsrPercentileFeaturesRequest(
                source=replace(source_a),
                authority=source_a,
                namespace="copied_psr",
            ),
        )
    with pytest.raises(ValueError, match="generation/capability mismatch"):
        build_daily_valuation_psr_percentile_features(
            conn,
            DailyValuationPsrPercentileFeaturesRequest(
                source=source_a,
                authority=source_b,
                namespace="cross_generation_psr",
            ),
        )
    conn.close()


@pytest.mark.parametrize("mutation", ["duplicate", "missing"])
def test_forward_psr_builder_fails_closed_on_valuation_key_drift(
    tmp_path: Path,
    mutation: str,
) -> None:
    conn, source = _build_psr_source(tmp_path / "market.duckdb", namespace="psr_a")
    code, valuation_date, basis_id = conn.execute(
        f"SELECT code, date, valuation_basis_id FROM {source.name} LIMIT 1"
    ).fetchone()
    if mutation == "duplicate":
        conn.execute(
            """
            INSERT INTO daily_valuation
            SELECT * FROM daily_valuation
            WHERE code = ? AND CAST(date AS DATE) = ? AND basis_version = ?
            LIMIT 1
            """,
            [code, valuation_date, basis_id],
        )
    else:
        conn.execute(
            """
            DELETE FROM daily_valuation
            WHERE code = ? AND CAST(date AS DATE) = ? AND basis_version = ?
            """,
            [code, valuation_date, basis_id],
        )

    with pytest.raises(RuntimeError, match="exact valuation key"):
        build_daily_valuation_psr_percentile_features(
            conn,
            DailyValuationPsrPercentileFeaturesRequest(
                source=source,
                authority=source,
                namespace=f"{mutation}_psr",
            ),
        )
    conn.close()


def _build_psr_source(db_path: Path, *, namespace: str):
    _build_short_red_db(db_path)
    conn = duckdb.connect(str(db_path))
    conn.execute("ALTER TABLE daily_valuation ADD COLUMN psr DOUBLE")
    conn.execute("ALTER TABLE daily_valuation ADD COLUMN forward_psr DOUBLE")
    conn.execute(
        """
        UPDATE daily_valuation
        SET psr = CAST(code AS INTEGER),
            forward_psr = 10000.0 - CAST(code AS INTEGER)
        """
    )
    relations = build_daily_ranking_research_base(
        conn,
        DailyRankingPanelRequest(
            namespace=namespace,
            analysis_start_date=date(2024, 3, 1),
            analysis_end_date=date(2024, 3, 5),
            horizons=(5,),
            market_scopes=("prime",),
            percentile_features=(),
        ),
    )
    source = relations.ranked_signals
    return conn, source
