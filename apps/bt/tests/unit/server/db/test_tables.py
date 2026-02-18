"""
Contract Integrity Tests for tables.py

tables.py のテーブル定義が contracts/ JSON スキーマ契約と一致することを検証する。
Codex 指摘 #6 対応: 列名・型・nullable・PK/FK/UNIQUE/INDEX まで検証。
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from sqlalchemy import ForeignKeyConstraint, Integer, Text, UniqueConstraint
from sqlalchemy.types import REAL

from src.lib.market_db.tables import (
    dataset_info,
    dataset_meta,
    ds_indices_data,
    ds_stock_data,
    ds_stocks,
    ds_topix_data,
    index_master,
    indices_data,
    market_statements,
    margin_data,
    market_meta,
    portfolio_items,
    portfolio_meta,
    portfolio_metadata,
    portfolios,
    statements,
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


def _resolve_column_spec(spec: dict) -> dict:
    """$ref を解決せず、const 直接またはネストした const を取得"""
    if "const" in spec:
        return spec["const"]
    # dataset-db-schema-v2 uses $ref but we already validated via allOf
    # For direct const (market-db-schema-v1), use as-is
    return spec.get("const", spec)


# ===========================================================================
# market-db-schema-v1.json 契約テスト
# ===========================================================================

class TestMarketDbContract:
    """market.db テーブル定義が market-db-schema-v1.json と一致"""

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

    def test_market_meta_has_7_tables(self) -> None:
        assert len(market_meta.tables) == 7

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
    """market.db statements 追加契約（v2）"""

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

    def test_statements_primary_key(self) -> None:
        pk_cols = [c.name for c in market_statements.primary_key.columns]
        assert pk_cols == self.tables["statements"]["properties"]["primary_key"]["const"]

    def test_statements_indexes(self) -> None:
        expected_indexes = self.tables["statements"]["properties"]["indexes"]["const"]
        actual_indexes = {idx.name: [c.name for c in idx.columns] for idx in market_statements.indexes}
        for idx in expected_indexes:
            assert idx["name"] in actual_indexes
            assert actual_indexes[idx["name"]] == idx["columns"]


# ===========================================================================
# dataset-db-schema-v2.json 契約テスト
# ===========================================================================

class TestDatasetDbContract:
    """dataset.db テーブル定義が dataset-db-schema-v2.json と一致"""

    @pytest.fixture(autouse=True)
    def _load(self) -> None:
        self.contract = _load_contract("dataset-db-schema-v2.json")
        self.tables = self.contract["properties"]["tables"]["properties"]
        # $defs から型を解決するヘルパー
        self.defs = self.contract["$defs"]

    def _resolve_type(self, col_spec: dict) -> dict:
        """$ref を解決して {type, nullable} を返す"""
        if "const" in col_spec:
            return col_spec["const"]
        if "$ref" in col_spec:
            ref_name = col_spec["$ref"].split("/")[-1]
            # $defs は allOf で定義されているので、2番目の要素から const を取得
            ref_def = self.defs[ref_name]
            if "allOf" in ref_def:
                for part in ref_def["allOf"]:
                    if "properties" in part:
                        type_val = part["properties"].get("type", {}).get("const")
                        nullable_val = part["properties"].get("nullable", {}).get("const")
                        if type_val is not None and nullable_val is not None:
                            return {"type": type_val, "nullable": nullable_val}
        # allOf wrapper pattern
        if "allOf" in col_spec:
            for part in col_spec["allOf"]:
                if "$ref" in part:
                    return self._resolve_type(part)
        return col_spec

    def _verify_table_columns(self, table_obj: object, table_name: str) -> None:
        spec = self.tables[table_name]["properties"]["columns"]["properties"]
        from sqlalchemy import Table as SATable

        assert isinstance(table_obj, SATable)
        for col_name, col_spec in spec.items():
            col = table_obj.c[col_name]
            expected = self._resolve_type(col_spec)
            assert _sa_type_name(col.type) == expected["type"], f"{table_name}.{col_name} type mismatch"
            assert col.nullable == expected["nullable"], f"{table_name}.{col_name} nullable mismatch"

    def _verify_primary_key(self, table_obj: object, table_name: str) -> None:
        from sqlalchemy import Table as SATable

        assert isinstance(table_obj, SATable)
        pk_cols = [c.name for c in table_obj.primary_key.columns]
        expected_pk = self.tables[table_name]["properties"]["primary_key"]["const"]
        assert pk_cols == expected_pk, f"{table_name} PK mismatch: {pk_cols} != {expected_pk}"

    def _verify_indexes(self, table_name: str) -> None:
        expected_indexes = self.tables[table_name]["properties"]["indexes"]["const"]
        # dataset テーブルのインデックスは ds_ プレフィックス付きで定義されている
        meta_indexes = {idx.name: [c.name for c in idx.columns] for idx in dataset_meta.tables[table_name].indexes}
        for idx_spec in expected_indexes:
            # 実際のインデックス名は ds_ プレフィックス or 元名
            found = False
            for idx_name, idx_cols in meta_indexes.items():
                if idx_cols == idx_spec["columns"]:
                    found = True
                    break
            assert found, f"{table_name} index {idx_spec['name']} with columns {idx_spec['columns']} not found"

    def test_stocks_columns(self) -> None:
        self._verify_table_columns(ds_stocks, "stocks")

    def test_stocks_primary_key(self) -> None:
        self._verify_primary_key(ds_stocks, "stocks")

    def test_stocks_indexes(self) -> None:
        self._verify_indexes("stocks")

    def test_stock_data_columns(self) -> None:
        self._verify_table_columns(ds_stock_data, "stock_data")

    def test_stock_data_primary_key(self) -> None:
        self._verify_primary_key(ds_stock_data, "stock_data")

    def test_stock_data_indexes(self) -> None:
        self._verify_indexes("stock_data")

    def test_topix_data_columns(self) -> None:
        self._verify_table_columns(ds_topix_data, "topix_data")

    def test_topix_data_primary_key(self) -> None:
        self._verify_primary_key(ds_topix_data, "topix_data")

    def test_topix_data_indexes(self) -> None:
        self._verify_indexes("topix_data")

    def test_indices_data_columns(self) -> None:
        self._verify_table_columns(ds_indices_data, "indices_data")

    def test_indices_data_primary_key(self) -> None:
        self._verify_primary_key(ds_indices_data, "indices_data")

    def test_indices_data_indexes(self) -> None:
        self._verify_indexes("indices_data")

    def test_dataset_info_columns(self) -> None:
        self._verify_table_columns(dataset_info, "dataset_info")

    def test_dataset_info_primary_key(self) -> None:
        self._verify_primary_key(dataset_info, "dataset_info")

    def test_margin_data_columns(self) -> None:
        self._verify_table_columns(margin_data, "margin_data")

    def test_margin_data_primary_key(self) -> None:
        self._verify_primary_key(margin_data, "margin_data")

    def test_margin_data_indexes(self) -> None:
        self._verify_indexes("margin_data")

    def test_statements_columns(self) -> None:
        self._verify_table_columns(statements, "statements")

    def test_statements_primary_key(self) -> None:
        self._verify_primary_key(statements, "statements")

    def test_statements_indexes(self) -> None:
        self._verify_indexes("statements")

    def test_dataset_meta_has_7_tables(self) -> None:
        assert len(dataset_meta.tables) == 7


# ===========================================================================
# portfolio-db-schema-v1.json 契約テスト
# ===========================================================================

class TestPortfolioDbContract:
    """portfolio.db テーブル定義が portfolio-db-schema-v1.json と一致"""

    @pytest.fixture(autouse=True)
    def _load(self) -> None:
        self.contract = _load_contract("portfolio-db-schema-v1.json")
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
                actual_uniques[constraint.name] = [c.name for c in constraint.columns]
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

    def test_portfolio_meta_has_5_tables(self) -> None:
        assert len(portfolio_meta.tables) == 5


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

        db_path = tmp_path / "test_market.db"
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

    def test_dataset_create_all_and_insert(self, tmp_path: Path) -> None:
        from sqlalchemy import create_engine, insert, select

        db_path = tmp_path / "test_dataset.db"
        engine = create_engine(f"sqlite:///{db_path}")

        dataset_meta.create_all(engine)

        with engine.begin() as conn:
            conn.execute(insert(dataset_info).values(key="schema_version", value="2.0.0"))
            result = conn.execute(select(dataset_info)).fetchall()
            assert len(result) == 1
            assert result[0].value == "2.0.0"

        engine.dispose()


def text_sql(sql: str):  # noqa: ANN201
    """sqlalchemy text() ラッパー"""
    from sqlalchemy import text

    return text(sql)
