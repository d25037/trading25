"""MarketDbReader concurrency tests."""

from concurrent.futures import ThreadPoolExecutor

from src.infrastructure.db.market.market_reader import MarketDbReader


def test_query_one_is_thread_safe(market_db_path: str) -> None:
    """複数スレッドの同時 query_one で例外が発生しないことを確認。"""
    reader = MarketDbReader(market_db_path)
    try:
        stock_codes = ["72030", "67580"] * 100

        def fetch_company_name(code: str) -> str:
            row = reader.query_one(
                "SELECT company_name FROM stocks WHERE code = ?",
                (code,),
            )
            assert row is not None
            return str(row["company_name"])

        with ThreadPoolExecutor(max_workers=16) as executor:
            names = list(executor.map(fetch_company_name, stock_codes))

        assert len(names) == len(stock_codes)
        assert set(names) == {"トヨタ自動車", "ソニーグループ"}
    finally:
        reader.close()


def test_query_and_query_one_mixed_concurrency(market_db_path: str) -> None:
    """query/query_one の混在実行でも結果整合が崩れないことを確認。"""
    reader = MarketDbReader(market_db_path)
    try:
        def worker(i: int) -> tuple[str, int]:
            code = "72030" if i % 2 == 0 else "67580"
            row = reader.query_one("SELECT code FROM stocks WHERE code = ?", (code,))
            assert row is not None
            rows = reader.query(
                "SELECT date FROM stock_data WHERE code = ? ORDER BY date",
                (code,),
            )
            return str(row["code"]), len(rows)

        with ThreadPoolExecutor(max_workers=20) as executor:
            results = list(executor.map(worker, range(200)))

        assert len(results) == 200
        assert all(code in {"72030", "67580"} for code, _ in results)
        assert all(count == 3 for _, count in results)
    finally:
        reader.close()
