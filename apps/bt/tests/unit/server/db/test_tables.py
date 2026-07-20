"""
Contract Integrity Tests for tables.py

tables.py のテーブル定義が contracts/ JSON スキーマ契約と一致することを検証する。
Codex 指摘 #6 対応: 列名・型・nullable・PK/FK/UNIQUE/INDEX まで検証。
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from sqlalchemy import CheckConstraint, ForeignKeyConstraint, Integer, Text, UniqueConstraint
from sqlalchemy.types import REAL

from src.infrastructure.db.market import tables as market_tables
from src.infrastructure.db.market.tables import (
    index_master,
    indices_data,
    jobs,
    market_margin_data,
    market_statements,
    market_meta,
    portfolio_items,
    portfolio_meta,
    portfolio_metadata,
    portfolios,
    stock_data_raw,
    stock_data,
    stocks,
    sync_metadata,
    topix_data,
    watchlist_items,
    watchlists,
)

CONTRACTS_DIR = Path(__file__).resolve().parents[6] / "contracts"


def _load_contract(filename: str) -> dict:
    path = CONTRACTS_DIR / filename
    with open(path) as f:
        return json.load(f)


def _sa_type_name(col_type: object) -> str:
    """SQLAlchemy 型を contract JSON の型名に変換"""
    if isinstance(col_type, Text):
        return "text"
    if isinstance(col_type, REAL):
        return "real"
    if isinstance(col_type, Integer):
        return "integer"
    type_name = type(col_type).__name__.upper()
    if type_name == "TEXT":
        return "text"
    if type_name == "REAL":
        return "real"
    if type_name in ("INTEGER", "INT"):
        return "integer"
    return type_name.lower()


# ===========================================================================
# market-db-schema-v1.json 契約テスト
# ===========================================================================

class TestMarketDbContract:
    """market.duckdb テーブル定義が market-db-schema-v1.json と一致"""

    @pytest.fixture(autouse=True)
    def _load(self) -> None:
        self.contract = _load_contract("market-db-schema-v1.json")
        self.tables = self.contract["properties"]["tables"]["properties"]

    def test_stocks_columns(self) -> None:
        spec = self.tables["stocks"]["properties"]["columns"]["properties"]
        for col_name, col_spec in spec.items():
            col = stocks.c[col_name]
            expected = col_spec["const"]
            assert _sa_type_name(col.type) == expected["type"], f"stocks.{col_name} type mismatch"
            assert col.nullable == expected["nullable"], f"stocks.{col_name} nullable mismatch"

    def test_stocks_primary_key(self) -> None:
        pk_cols = [c.name for c in stocks.primary_key.columns]
        assert pk_cols == self.tables["stocks"]["properties"]["primary_key"]["const"]

    def test_stock_data_columns(self) -> None:
        spec = self.tables["stock_data"]["properties"]["columns"]["properties"]
        for col_name, col_spec in spec.items():
            col = stock_data.c[col_name]
            expected = col_spec["const"]
            assert _sa_type_name(col.type) == expected["type"], f"stock_data.{col_name} type mismatch"
            assert col.nullable == expected["nullable"], f"stock_data.{col_name} nullable mismatch"

    def test_stock_data_primary_key(self) -> None:
        pk_cols = [c.name for c in stock_data.primary_key.columns]
        assert pk_cols == self.tables["stock_data"]["properties"]["primary_key"]["const"]

    def test_topix_data_columns(self) -> None:
        spec = self.tables["topix_data"]["properties"]["columns"]["properties"]
        for col_name, col_spec in spec.items():
            col = topix_data.c[col_name]
            expected = col_spec["const"]
            assert _sa_type_name(col.type) == expected["type"], f"topix_data.{col_name} type mismatch"
            assert col.nullable == expected["nullable"], f"topix_data.{col_name} nullable mismatch"

    def test_topix_data_primary_key(self) -> None:
        pk_cols = [c.name for c in topix_data.primary_key.columns]
        assert pk_cols == self.tables["topix_data"]["properties"]["primary_key"]["const"]

    def test_indices_data_columns(self) -> None:
        spec = self.tables["indices_data"]["properties"]["columns"]["properties"]
        for col_name, col_spec in spec.items():
            col = indices_data.c[col_name]
            expected = col_spec["const"]
            assert _sa_type_name(col.type) == expected["type"], f"indices_data.{col_name} type mismatch"
            assert col.nullable == expected["nullable"], f"indices_data.{col_name} nullable mismatch"

    def test_indices_data_primary_key(self) -> None:
        pk_cols = [c.name for c in indices_data.primary_key.columns]
        assert pk_cols == self.tables["indices_data"]["properties"]["primary_key"]["const"]

    def test_market_meta_tracks_v5_tables(self) -> None:
        assert "daily_valuation" in market_meta.tables
        assert "stock_adjustment_events" in market_meta.tables
        assert "statement_metrics_adjusted" in market_meta.tables
        assert "stock_adjustment_bases" not in market_meta.tables
        assert "stock_adjustment_basis_segments" not in market_meta.tables

    def test_sync_metadata_structure(self) -> None:
        assert sync_metadata.c.key.primary_key
        assert not sync_metadata.c.value.nullable

    def test_index_master_structure(self) -> None:
        assert index_master.c.code.primary_key
        assert not index_master.c.name.nullable
        assert not index_master.c.category.nullable


# ===========================================================================
# market-db-schema-v2.json 契約テスト
# ===========================================================================

class TestMarketDbContractV2:
    """market.duckdb statements + margin_data + stock_data_raw 契約（v2 minor update）"""

    @pytest.fixture(autouse=True)
    def _load(self) -> None:
        self.contract = _load_contract("market-db-schema-v2.json")
        self.tables = self.contract["properties"]["tables"]["properties"]

    def test_statements_columns(self) -> None:
        spec = self.tables["statements"]["properties"]["columns"]["properties"]
        for col_name, col_spec in spec.items():
            col = market_statements.c[col_name]
            expected = col_spec["const"]
            assert _sa_type_name(col.type) == expected["type"], f"market.statements.{col_name} type mismatch"
            assert col.nullable == expected["nullable"], f"market.statements.{col_name} nullable mismatch"

    def test_v2_contract_documents_historical_statements_primary_key(self) -> None:
        assert self.tables["statements"]["properties"]["primary_key"]["const"] == [
            "code",
            "disclosed_date",
        ]

    def test_statements_indexes(self) -> None:
        expected_indexes = self.tables["statements"]["properties"]["indexes"]["const"]
        actual_indexes = {idx.name: [c.name for c in idx.columns] for idx in market_statements.indexes}
        for idx in expected_indexes:
            assert idx["name"] in actual_indexes
            assert actual_indexes[idx["name"]] == idx["columns"]

    def test_margin_data_columns(self) -> None:
        spec = self.tables["margin_data"]["properties"]["columns"]["properties"]
        for col_name, col_spec in spec.items():
            col = market_margin_data.c[col_name]
            expected = col_spec["const"]
            assert _sa_type_name(col.type) == expected["type"], f"market.margin_data.{col_name} type mismatch"
            assert col.nullable == expected["nullable"], f"market.margin_data.{col_name} nullable mismatch"

    def test_margin_data_primary_key(self) -> None:
        pk_cols = [c.name for c in market_margin_data.primary_key.columns]
        assert pk_cols == self.tables["margin_data"]["properties"]["primary_key"]["const"]

    def test_margin_data_indexes(self) -> None:
        expected_indexes = self.tables["margin_data"]["properties"]["indexes"]["const"]
        actual_indexes = {idx.name: [c.name for c in idx.columns] for idx in market_margin_data.indexes}
        for idx in expected_indexes:
            assert idx["name"] in actual_indexes
            assert actual_indexes[idx["name"]] == idx["columns"]

    def test_stock_data_raw_columns(self) -> None:
        spec = self.tables["stock_data_raw"]["properties"]["columns"]["properties"]
        for col_name, col_spec in spec.items():
            col = stock_data_raw.c[col_name]
            expected = col_spec["const"]
            assert _sa_type_name(col.type) == expected["type"], f"market.stock_data_raw.{col_name} type mismatch"
            assert col.nullable == expected["nullable"], f"market.stock_data_raw.{col_name} nullable mismatch"

    def test_stock_data_raw_primary_key(self) -> None:
        pk_cols = [c.name for c in stock_data_raw.primary_key.columns]
        assert pk_cols == self.tables["stock_data_raw"]["properties"]["primary_key"]["const"]

    def test_v2_contract_keeps_additive_backward_compatibility(self) -> None:
        assert set(self.contract["properties"]["schema_version"]["enum"]) == {"2.0.0", "2.1.0", "2.2.0"}
        required_tables = set(self.contract["properties"]["tables"]["required"])
        assert "margin_data" not in required_tables
        assert "margin_data" in self.tables
        assert "stock_data_raw" not in required_tables
        assert "stock_data_raw" in self.tables


class TestMarketDbContractV3:
    """market.duckdb schema v3 contract documents PIT master and consumer metric SoTs."""

    @pytest.fixture(autouse=True)
    def _load(self) -> None:
        self.contract = _load_contract("market-db-schema-v3.json")
        self.tables = self.contract["properties"]["tables"]["properties"]

    def test_v3_contract_requires_current_sot_tables(self) -> None:
        assert self.contract["properties"]["schema_version"]["const"] == "3.0.0"
        required_tables = set(self.contract["properties"]["tables"]["required"])
        assert {
            "market_schema_version",
            "stock_master_daily",
            "stock_master_intervals",
            "stocks_latest",
            "stock_data_raw",
            "stock_data",
            "stock_data_minute_raw",
            "topix_data",
            "indices_data",
            "index_master",
            "index_membership_daily",
            "statements",
            "statement_metrics_adjusted",
            "daily_valuation",
            "margin_data",
            "options_225_data",
            "sync_metadata",
            "stock_adjustment_bases",
            "stock_adjustment_basis_segments",
        }.issubset(required_tables)

    def test_v3_contract_defines_pit_universe_and_adjusted_metric_keys(self) -> None:
        assert self.tables["stock_master_daily"]["properties"]["primary_key"]["const"] == [
            "date",
            "code",
        ]
        assert self.tables["index_membership_daily"]["properties"]["primary_key"]["const"] == [
            "date",
            "index_code",
            "code",
        ]
        assert self.tables["statement_metrics_adjusted"]["properties"]["primary_key"]["const"] == [
            "code",
            "disclosed_date",
            "period_end",
            "period_type",
            "basis_version",
        ]
        assert self.tables["daily_valuation"]["properties"]["primary_key"]["const"] == [
            "code",
            "date",
            "basis_version",
        ]

    def test_v3_contract_documents_historical_event_time_adjustment_basis_tables(self) -> None:
        assert self.tables["stock_adjustment_bases"]["properties"]["primary_key"]["const"] == [
            "code",
            "basis_id",
        ]
        assert self.tables["stock_adjustment_bases"]["properties"]["unique_constraints"][
            "const"
        ] == [{"name": "uq_stock_adjustment_bases_code_valid_from", "columns": ["code", "valid_from"]}]
        assert self.tables["stock_adjustment_basis_segments"]["properties"]["primary_key"][
            "const"
        ] == ["code", "basis_id", "source_date_from"]



class TestMarketDbContractV4:
    """Contract v4 describes the breaking provider-adjusted Market v5 plane."""

    @pytest.fixture(autouse=True)
    def _load(self) -> None:
        self.contract = _load_contract("market-db-schema-v4.json")
        self.tables = self.contract["properties"]["tables"]["properties"]

    def test_v4_contract_requires_v5_relations_without_retained_bases(self) -> None:
        assert self.contract["properties"]["schema_version"]["const"] == "4.0.0"
        assert self.contract["properties"]["physical_schema_version"]["const"] == 5
        assert self.contract["properties"]["stock_price_adjustment_mode"]["const"] == (
            "provider_adjusted_v1"
        )
        required = set(self.contract["properties"]["tables"]["required"])
        assert "stock_adjustment_events" in required
        assert "statement_metrics_adjusted" in required
        assert "daily_valuation" in required
        assert "stock_adjustment_bases" not in required
        assert "stock_adjustment_basis_segments" not in required

    def test_v4_raw_and_event_columns_are_exact(self) -> None:
        assert list(
            self.tables["stock_data_raw"]["properties"]["columns"]["properties"]
        ) == [column.name for column in stock_data_raw.columns]
        assert list(
            self.tables["stock_adjustment_events"]["properties"]["columns"]["properties"]
        ) == [column.name for column in market_tables.stock_adjustment_events.columns]
        assert self.tables["stock_adjustment_events"]["properties"]["primary_key"][
            "const"
        ] == ["code", "date"]
        checks = {
            constraint.name: str(constraint.sqltext)
            for constraint in market_tables.stock_adjustment_events.constraints
            if isinstance(constraint, CheckConstraint)
        }
        assert checks == {
            "ck_stock_adjustment_events_non_unit_positive_factor": (
                "adjustment_factor > 0 AND adjustment_factor <> 1"
            )
        }
        assert self.tables["stock_adjustment_events"]["properties"]["checks"][
            "const"
        ] == [
            {
                "name": "ck_stock_adjustment_events_non_unit_positive_factor",
                "expression": "adjustment_factor > 0 AND adjustment_factor <> 1",
            }
        ]

    def test_v4_current_basis_statement_key_has_no_basis_dimension(self) -> None:
        assert self.tables["statement_metrics_adjusted"]["properties"]["primary_key"][
            "const"
        ] == ["code", "statement_id"]
        columns = self.tables["statement_metrics_adjusted"]["properties"]["columns"][
            "properties"
        ]
        assert list(columns) == [
            column.name for column in market_tables.statement_metrics_adjusted.columns
        ]
        assert "basis_version" not in columns

    def test_v4_daily_valuation_is_a_view_without_basis_dimension(self) -> None:
        valuation = self.tables["daily_valuation"]
        assert valuation["properties"]["relation_type"]["const"] == "view"
        assert "primary_key" not in valuation["properties"]
        assert "basis_version" not in valuation["properties"]["columns"]["properties"]

    def test_v4_contract_requires_provider_metadata_keys(self) -> None:
        assert self.tables["sync_metadata"]["properties"]["required_keys"]["const"] == [
            "provider_plan",
            "provider_as_of",
            "provider_coverage_start",
            "provider_coverage_end",
            "provider_source_fingerprint",
            "fundamentals_adjustment_basis_date",
        ]


class TestDatasetDbContractV3:
    """Dataset v3 is the breaking event-time PIT snapshot contract."""

    @pytest.fixture(autouse=True)
    def _load(self) -> None:
        self.contract = _load_contract("dataset-db-schema-v3.json")
        self.tables = self.contract["properties"]["tables"]["properties"]

    def test_requires_all_writer_tables_including_event_time_pit_graph(self, tmp_path: Path) -> None:
        assert self.contract["properties"]["schema_version"]["const"] == "3.0.0"
        from src.infrastructure.db.dataset_io.dataset_writer import DatasetWriter

        writer = DatasetWriter(str(tmp_path / "snapshot"))
        actual_tables = {
            row[0]
            for row in writer._duckdb_store._conn.execute("SHOW TABLES").fetchall()  # noqa: SLF001
        }
        writer.close()
        assert set(self.contract["properties"]["tables"]["required"]) == actual_tables
        assert set(self.tables) == actual_tables

    def test_primary_keys_and_basis_foreign_keys_are_exact(self) -> None:
        expected_primary_keys = {
            "stock_data_raw": ["code", "date"],
            "stock_master_daily": ["date", "code"],
            "stock_adjustment_bases": ["code", "basis_id"],
            "stock_adjustment_basis_segments": ["code", "basis_id", "source_date_from"],
            "statement_metrics_adjusted": [
                "code", "disclosed_date", "period_end", "period_type", "basis_version"
            ],
            "daily_valuation": ["code", "date", "basis_version"],
        }
        for table, primary_key in expected_primary_keys.items():
            assert self.tables[table]["properties"]["primary_key"]["const"] == primary_key

        basis_fk = {
            "columns": ["code", "basis_id"],
            "references_table": "stock_adjustment_bases",
            "references_columns": ["code", "basis_id"],
            "on_delete": "NO ACTION",
        }
        metric_fk = basis_fk | {"columns": ["code", "basis_version"]}
        assert self.tables["stock_adjustment_basis_segments"]["properties"]["foreign_keys"]["const"] == [basis_fk]
        assert self.tables["statement_metrics_adjusted"]["properties"]["foreign_keys"]["const"] == [metric_fk]
        assert self.tables["daily_valuation"]["properties"]["foreign_keys"]["const"] == [metric_fk]

    def test_contract_primary_keys_match_dataset_writer(self, tmp_path: Path) -> None:
        from src.infrastructure.db.dataset_io.dataset_writer import DatasetWriter

        writer = DatasetWriter(str(tmp_path / "snapshot"))
        conn = writer._duckdb_store._conn  # noqa: SLF001
        for table, table_contract in self.tables.items():
            actual_pk = [
                row[1]
                for row in sorted(
                    (row for row in conn.execute(f"PRAGMA table_info('{table}')").fetchall() if row[5]),
                    key=lambda row: row[5],
                )
            ]
            assert actual_pk == table_contract["properties"]["primary_key"]["const"]
        writer.close()


# ===========================================================================
# portfolio-db-schema-v2.json 契約テスト
# ===========================================================================

class TestPortfolioDbContract:
    """portfolio.db テーブル定義が portfolio-db-schema-v2.json と一致"""

    @pytest.fixture(autouse=True)
    def _load(self) -> None:
        self.contract = _load_contract("portfolio-db-schema-v2.json")
        self.tables = self.contract["properties"]["tables"]["properties"]
        self.defs = self.contract["$defs"]

    def _resolve_type(self, col_spec: dict) -> dict:
        if "const" in col_spec:
            return col_spec["const"]
        if "$ref" in col_spec:
            ref_name = col_spec["$ref"].split("/")[-1]
            ref_def = self.defs[ref_name]
            if "allOf" in ref_def:
                for part in ref_def["allOf"]:
                    if "properties" in part:
                        type_val = part["properties"].get("type", {}).get("const")
                        nullable_val = part["properties"].get("nullable", {}).get("const")
                        if type_val is not None and nullable_val is not None:
                            return {"type": type_val, "nullable": nullable_val}
        if "allOf" in col_spec:
            for part in col_spec["allOf"]:
                if "$ref" in part:
                    return self._resolve_type(part)
        return col_spec

    def _verify_columns(self, table_obj: object, table_name: str) -> None:
        from sqlalchemy import Table as SATable

        assert isinstance(table_obj, SATable)
        spec = self.tables[table_name]["properties"]["columns"]["properties"]
        for col_name, col_spec in spec.items():
            col = table_obj.c[col_name]
            expected = self._resolve_type(col_spec)
            assert _sa_type_name(col.type) == expected["type"], f"{table_name}.{col_name} type mismatch"
            assert col.nullable == expected["nullable"], f"{table_name}.{col_name} nullable mismatch"

    def _verify_primary_key(self, table_obj: object, table_name: str) -> None:
        from sqlalchemy import Table as SATable

        assert isinstance(table_obj, SATable)
        pk_cols = [c.name for c in table_obj.primary_key.columns]
        assert pk_cols == self.tables[table_name]["properties"]["primary_key"]["const"]

    def _verify_indexes(self, table_obj: object, table_name: str) -> None:
        from sqlalchemy import Table as SATable

        assert isinstance(table_obj, SATable)
        expected_indexes = self.tables[table_name]["properties"]["indexes"]["const"]
        actual_indexes = {idx.name: [c.name for c in idx.columns] for idx in table_obj.indexes}
        for idx_spec in expected_indexes:
            assert idx_spec["name"] in actual_indexes, f"{table_name} missing index {idx_spec['name']}"
            assert actual_indexes[idx_spec["name"]] == idx_spec["columns"], (
                f"{table_name} index {idx_spec['name']} columns mismatch"
            )

    def _verify_unique_constraints(self, table_obj: object, table_name: str) -> None:
        from sqlalchemy import Table as SATable

        assert isinstance(table_obj, SATable)
        expected = self.tables[table_name]["properties"].get("unique_constraints", {}).get("const", [])
        actual_uniques: dict[str, list[str]] = {}
        for constraint in table_obj.constraints:
            if isinstance(constraint, UniqueConstraint) and constraint.name:
                actual_uniques[str(constraint.name)] = [c.name for c in constraint.columns]
        for uq_spec in expected:
            assert uq_spec["name"] in actual_uniques, f"{table_name} missing unique {uq_spec['name']}"
            assert actual_uniques[uq_spec["name"]] == uq_spec["columns"]

    def _verify_foreign_keys(self, table_obj: object, table_name: str) -> None:
        from sqlalchemy import Table as SATable

        assert isinstance(table_obj, SATable)
        expected = self.tables[table_name]["properties"].get("foreign_keys", {}).get("const", [])
        actual_fks: list[dict] = []
        for constraint in table_obj.constraints:
            if isinstance(constraint, ForeignKeyConstraint):
                actual_fks.append({
                    "columns": [c.name for c in constraint.columns],
                    "references_table": list(constraint.elements)[0].column.table.name,
                    "references_columns": [e.column.name for e in constraint.elements],
                    "on_delete": constraint.ondelete or "NO ACTION",
                })
        # Also check single-column ForeignKey
        for col in table_obj.columns:
            for fk in col.foreign_keys:
                # Check if already captured
                already = any(
                    d["columns"] == [col.name] and d["references_table"] == fk.column.table.name
                    for d in actual_fks
                )
                if not already:
                    actual_fks.append({
                        "columns": [col.name],
                        "references_table": fk.column.table.name,
                        "references_columns": [fk.column.name],
                        "on_delete": fk.ondelete or "NO ACTION",
                    })
        for fk_spec in expected:
            found = False
            for actual in actual_fks:
                if (
                    actual["columns"] == fk_spec["columns"]
                    and actual["references_table"] == fk_spec["references_table"]
                    and actual["references_columns"] == fk_spec["references_columns"]
                    and actual["on_delete"] == fk_spec["on_delete"]
                ):
                    found = True
                    break
            assert found, f"{table_name} missing FK {fk_spec}"

    # --- portfolio_metadata ---
    def test_portfolio_metadata_columns(self) -> None:
        self._verify_columns(portfolio_metadata, "portfolio_metadata")

    def test_portfolio_metadata_pk(self) -> None:
        self._verify_primary_key(portfolio_metadata, "portfolio_metadata")

    # --- portfolios ---
    def test_portfolios_columns(self) -> None:
        self._verify_columns(portfolios, "portfolios")

    def test_portfolios_pk(self) -> None:
        self._verify_primary_key(portfolios, "portfolios")

    def test_portfolios_unique(self) -> None:
        self._verify_unique_constraints(portfolios, "portfolios")

    # --- portfolio_items ---
    def test_portfolio_items_columns(self) -> None:
        self._verify_columns(portfolio_items, "portfolio_items")

    def test_portfolio_items_pk(self) -> None:
        self._verify_primary_key(portfolio_items, "portfolio_items")

    def test_portfolio_items_indexes(self) -> None:
        self._verify_indexes(portfolio_items, "portfolio_items")

    def test_portfolio_items_unique(self) -> None:
        self._verify_unique_constraints(portfolio_items, "portfolio_items")

    def test_portfolio_items_fk(self) -> None:
        self._verify_foreign_keys(portfolio_items, "portfolio_items")

    # --- watchlists ---
    def test_watchlists_columns(self) -> None:
        self._verify_columns(watchlists, "watchlists")

    def test_watchlists_pk(self) -> None:
        self._verify_primary_key(watchlists, "watchlists")

    def test_watchlists_unique(self) -> None:
        self._verify_unique_constraints(watchlists, "watchlists")

    # --- watchlist_items ---
    def test_watchlist_items_columns(self) -> None:
        self._verify_columns(watchlist_items, "watchlist_items")

    def test_watchlist_items_pk(self) -> None:
        self._verify_primary_key(watchlist_items, "watchlist_items")

    def test_watchlist_items_indexes(self) -> None:
        self._verify_indexes(watchlist_items, "watchlist_items")

    def test_watchlist_items_unique(self) -> None:
        self._verify_unique_constraints(watchlist_items, "watchlist_items")

    def test_watchlist_items_fk(self) -> None:
        self._verify_foreign_keys(watchlist_items, "watchlist_items")

    # --- jobs ---
    def test_jobs_columns(self) -> None:
        self._verify_columns(jobs, "jobs")

    def test_jobs_pk(self) -> None:
        self._verify_primary_key(jobs, "jobs")

    def test_jobs_indexes(self) -> None:
        self._verify_indexes(jobs, "jobs")

    def test_portfolio_meta_has_6_tables(self) -> None:
        assert len(portfolio_meta.tables) == 6


# ===========================================================================
# Drizzle ラウンドトリップテスト
# ===========================================================================

class TestDrizzleRoundTrip:
    """SQLAlchemy Core で Drizzle 互換テーブルを作成し、読み書きできることを検証"""

    def test_portfolio_create_all_and_insert(self, tmp_path: Path) -> None:
        from sqlalchemy import create_engine, insert, select

        db_path = tmp_path / "test_portfolio.db"
        engine = create_engine(f"sqlite:///{db_path}")

        # Drizzle 互換テーブルを作成
        portfolio_meta.create_all(engine)

        with engine.begin() as conn:
            # PRAGMA 設定
            conn.execute(text_sql("PRAGMA foreign_keys=ON"))

            # portfolio 作成
            conn.execute(insert(portfolios).values(name="test", description="Test Portfolio"))
            result = conn.execute(select(portfolios)).fetchall()
            assert len(result) == 1
            assert result[0].name == "test"

            # portfolio_item 追加
            conn.execute(
                insert(portfolio_items).values(
                    portfolio_id=1,
                    code="7203",
                    company_name="Toyota",
                    quantity=100,
                    purchase_price=2500.0,
                    purchase_date="2024-01-15",
                )
            )
            items = conn.execute(select(portfolio_items)).fetchall()
            assert len(items) == 1
            assert items[0].code == "7203"

        engine.dispose()

    def test_market_create_all_and_insert(self, tmp_path: Path) -> None:
        from sqlalchemy import create_engine, insert, select

        db_path = tmp_path / "test_market.duckdb"
        engine = create_engine(f"sqlite:///{db_path}")

        market_meta.create_all(engine)

        with engine.begin() as conn:
            conn.execute(
                insert(stocks).values(
                    code="7203",
                    company_name="トヨタ自動車",
                    market_code="0111",
                    market_name="プライム",
                    sector_17_code="6",
                    sector_17_name="自動車・輸送機",
                    sector_33_code="3700",
                    sector_33_name="輸送用機器",
                    listed_date="1949-05-16",
                )
            )
            result = conn.execute(select(stocks)).fetchall()
            assert len(result) == 1
            assert result[0].code == "7203"

        engine.dispose()

def text_sql(sql: str):  # noqa: ANN201
    """sqlalchemy text() ラッパー"""
    from sqlalchemy import text

    return text(sql)
