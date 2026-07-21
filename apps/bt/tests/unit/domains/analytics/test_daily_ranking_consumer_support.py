from __future__ import annotations

from dataclasses import replace
from datetime import date
from pathlib import Path

import duckdb
import pytest

from src.domains.analytics.daily_ranking_consumer_support import (
    DailyValuationPsrPercentileFeaturesRequest,
    build_daily_valuation_psr_percentile_features,
    compose_daily_ranking_signal_features,
)
from src.domains.analytics.daily_ranking_feature_builders import (
    AtrFeaturesRequest,
    build_atr_features,
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
    valuation_key = conn.execute(
        f"SELECT code, date FROM {source.name} LIMIT 1"
    ).fetchone()
    assert valuation_key is not None
    code, valuation_date = valuation_key
    if mutation == "duplicate":
        conn.execute(
            """
            INSERT INTO daily_valuation
            SELECT * FROM daily_valuation
            WHERE code = ? AND CAST(date AS DATE) = ?
              AND CAST(price_basis_date AS DATE) = ?
            LIMIT 1
            """,
            [code, valuation_date, valuation_date],
        )
    else:
        conn.execute(
            """
            DELETE FROM daily_valuation
            WHERE code = ? AND CAST(date AS DATE) = ?
              AND CAST(price_basis_date AS DATE) = ?
            """,
            [code, valuation_date, valuation_date],
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


def test_feature_composer_preserves_exact_source_membership_and_row_count(
    tmp_path: Path,
) -> None:
    conn, source = _build_psr_source(
        tmp_path / "market.duckdb",
        namespace="compose_membership",
    )
    atr = build_atr_features(
        conn,
        AtrFeaturesRequest(source=source, namespace="compose_membership_atr"),
    )
    psr = build_daily_valuation_psr_percentile_features(
        conn,
        DailyValuationPsrPercentileFeaturesRequest(
            source=source,
            authority=source,
            namespace="compose_membership_psr",
        ),
    )

    composed = compose_daily_ranking_signal_features(
        conn,
        source=source,
        features=(atr, psr),
        namespace="compose_membership",
    )

    key_sql = ", ".join(source.key_columns)
    assert composed.row_count == source.row_count
    assert conn.execute(
        f"SELECT {key_sql} FROM {composed.name} ORDER BY {key_sql}"
    ).fetchall() == conn.execute(
        f"SELECT {key_sql} FROM {source.name} ORDER BY {key_sql}"
    ).fetchall()
    conn.close()


def test_feature_composer_fails_closed_on_duplicate_overlay_columns(
    tmp_path: Path,
) -> None:
    conn, source = _build_psr_source(
        tmp_path / "market.duckdb",
        namespace="compose_duplicate",
    )
    first = build_atr_features(
        conn,
        AtrFeaturesRequest(source=source, namespace="compose_duplicate_first"),
    )
    second = build_atr_features(
        conn,
        AtrFeaturesRequest(source=source, namespace="compose_duplicate_second"),
    )

    with pytest.raises(ValueError, match="duplicate composed feature column: atr20_pct"):
        compose_daily_ranking_signal_features(
            conn,
            source=source,
            features=(first, second),
            namespace="compose_duplicate",
        )
    conn.close()


@pytest.mark.parametrize("mutation", ("missing_key", "duplicate_key", "payload"))
def test_feature_composer_rejects_post_issuance_feature_mutation(
    tmp_path: Path,
    mutation: str,
) -> None:
    conn, source = _build_psr_source(
        tmp_path / "market.duckdb",
        namespace=f"compose_mutation_{mutation}",
    )
    feature = build_atr_features(
        conn,
        AtrFeaturesRequest(
            source=source,
            namespace=f"compose_mutation_{mutation}_atr",
        ),
    )
    key_columns = ", ".join(feature.key_columns)
    key_values = conn.execute(
        f"SELECT {key_columns} FROM {feature.name} LIMIT 1"
    ).fetchone()
    assert key_values is not None
    key_predicate = " AND ".join(f"{column} = ?" for column in feature.key_columns)
    if mutation == "missing_key":
        conn.execute(
            f"DELETE FROM {feature.name} WHERE {key_predicate}",
            key_values,
        )
    elif mutation == "duplicate_key":
        conn.execute(f"INSERT INTO {feature.name} SELECT * FROM {feature.name} LIMIT 1")
    else:
        conn.execute(
            f"UPDATE {feature.name} "
            "SET atr20_pct = coalesce(atr20_pct, 0.0) + 1.0 "
            f"WHERE {key_predicate}",
            key_values,
        )

    with pytest.raises(RuntimeError, match="changed|fingerprint|current|membership"):
        compose_daily_ranking_signal_features(
            conn,
            source=source,
            features=(feature,),
            namespace=f"compose_reject_{mutation}",
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
