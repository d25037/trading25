"""Portfolio ルートのテスト"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from src.server.app import create_app
from src.lib.market_db.portfolio_db import PortfolioDb


@pytest.fixture()
def pdb(tmp_path: Path) -> PortfolioDb:
    db = PortfolioDb(str(tmp_path / "portfolio.db"))
    yield db  # type: ignore[misc]
    db.close()


@pytest.fixture()
def client(pdb: PortfolioDb) -> TestClient:
    app = create_app()
    app.state.portfolio_db = pdb
    return TestClient(app, raise_server_exceptions=False)


# ===== List Portfolios =====


class TestListPortfolios:
    def test_empty(self, client: TestClient) -> None:
        resp = client.get("/api/portfolio")
        assert resp.status_code == 200
        assert resp.json()["portfolios"] == []

    def test_with_items(self, client: TestClient, pdb: PortfolioDb) -> None:
        pdb.create_portfolio("p1", "desc1")
        pdb.add_item(1, "7203", "トヨタ", 100, 2500.0, "2024-01-15")
        resp = client.get("/api/portfolio")
        assert resp.status_code == 200
        data = resp.json()["portfolios"]
        assert len(data) == 1
        assert data[0]["name"] == "p1"
        assert data[0]["stockCount"] == 1
        assert data[0]["totalShares"] == 100
        assert "createdAt" in data[0]


# ===== Create Portfolio =====


class TestCreatePortfolio:
    def test_create(self, client: TestClient) -> None:
        resp = client.post("/api/portfolio", json={"name": "Test", "description": "My portfolio"})
        assert resp.status_code == 201
        data = resp.json()
        assert data["name"] == "Test"
        assert data["description"] == "My portfolio"
        assert "id" in data

    def test_create_no_description(self, client: TestClient) -> None:
        resp = client.post("/api/portfolio", json={"name": "Test"})
        assert resp.status_code == 201
        assert resp.json()["description"] is None

    def test_create_empty_name(self, client: TestClient) -> None:
        resp = client.post("/api/portfolio", json={"name": ""})
        assert resp.status_code == 422

    def test_create_duplicate(self, client: TestClient) -> None:
        client.post("/api/portfolio", json={"name": "Test"})
        resp = client.post("/api/portfolio", json={"name": "Test"})
        assert resp.status_code == 409
        assert "already exists" in resp.json()["message"]


# ===== Get Portfolio =====


class TestGetPortfolio:
    def test_get(self, client: TestClient, pdb: PortfolioDb) -> None:
        pdb.create_portfolio("Test")
        pdb.add_item(1, "7203", "トヨタ", 100, 2500.0, "2024-01-15")
        resp = client.get("/api/portfolio/1")
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "Test"
        assert len(data["items"]) == 1
        assert data["items"][0]["code"] == "7203"
        assert data["items"][0]["portfolioId"] == 1
        assert data["items"][0]["quantity"] == 100
        assert data["items"][0]["purchasePrice"] == 2500.0

    def test_not_found(self, client: TestClient) -> None:
        resp = client.get("/api/portfolio/999")
        assert resp.status_code == 404


# ===== Update Portfolio =====


class TestUpdatePortfolio:
    def test_update_name(self, client: TestClient, pdb: PortfolioDb) -> None:
        pdb.create_portfolio("Old")
        resp = client.put("/api/portfolio/1", json={"name": "New"})
        assert resp.status_code == 200
        assert resp.json()["name"] == "New"

    def test_update_description_to_null(self, client: TestClient, pdb: PortfolioDb) -> None:
        pdb.create_portfolio("Test", "desc")
        resp = client.put("/api/portfolio/1", json={"description": None})
        assert resp.status_code == 200
        assert resp.json()["description"] is None

    def test_not_found(self, client: TestClient) -> None:
        resp = client.put("/api/portfolio/999", json={"name": "New"})
        assert resp.status_code == 404

    def test_duplicate_name(self, client: TestClient, pdb: PortfolioDb) -> None:
        pdb.create_portfolio("A")
        pdb.create_portfolio("B")
        resp = client.put("/api/portfolio/2", json={"name": "A"})
        assert resp.status_code == 409


# ===== Delete Portfolio =====


class TestDeletePortfolio:
    def test_delete(self, client: TestClient, pdb: PortfolioDb) -> None:
        pdb.create_portfolio("Test")
        resp = client.delete("/api/portfolio/1")
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is True
        assert "deleted" in data["message"].lower()

    def test_not_found(self, client: TestClient) -> None:
        resp = client.delete("/api/portfolio/999")
        assert resp.status_code == 404


# ===== Add Item =====


class TestAddItem:
    def test_add(self, client: TestClient, pdb: PortfolioDb) -> None:
        pdb.create_portfolio("Test")
        resp = client.post(
            "/api/portfolio/1/items",
            json={
                "code": "7203",
                "companyName": "トヨタ",
                "quantity": 100,
                "purchasePrice": 2500.0,
                "purchaseDate": "2024-01-15",
            },
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["code"] == "7203"
        assert data["quantity"] == 100

    def test_portfolio_not_found(self, client: TestClient) -> None:
        resp = client.post(
            "/api/portfolio/999/items",
            json={
                "code": "7203",
                "companyName": "トヨタ",
                "quantity": 100,
                "purchasePrice": 2500.0,
                "purchaseDate": "2024-01-15",
            },
        )
        assert resp.status_code == 404

    def test_duplicate_stock(self, client: TestClient, pdb: PortfolioDb) -> None:
        pdb.create_portfolio("Test")
        pdb.add_item(1, "7203", "トヨタ", 100, 2500.0, "2024-01-15")
        resp = client.post(
            "/api/portfolio/1/items",
            json={
                "code": "7203",
                "companyName": "トヨタ",
                "quantity": 50,
                "purchasePrice": 2600.0,
                "purchaseDate": "2024-02-01",
            },
        )
        assert resp.status_code == 409

    def test_invalid_code(self, client: TestClient, pdb: PortfolioDb) -> None:
        pdb.create_portfolio("Test")
        resp = client.post(
            "/api/portfolio/1/items",
            json={
                "code": "invalid",
                "companyName": "Test",
                "quantity": 100,
                "purchasePrice": 100.0,
                "purchaseDate": "2024-01-15",
            },
        )
        assert resp.status_code == 422


# ===== Update Item =====


class TestUpdateItem:
    def test_update(self, client: TestClient, pdb: PortfolioDb) -> None:
        pdb.create_portfolio("Test")
        pdb.add_item(1, "7203", "トヨタ", 100, 2500.0, "2024-01-15")
        resp = client.put("/api/portfolio/1/items/1", json={"quantity": 200})
        assert resp.status_code == 200
        assert resp.json()["quantity"] == 200

    def test_not_found(self, client: TestClient, pdb: PortfolioDb) -> None:
        pdb.create_portfolio("Test")
        resp = client.put("/api/portfolio/1/items/999", json={"quantity": 200})
        assert resp.status_code == 404


# ===== Delete Item =====


class TestDeleteItem:
    def test_delete(self, client: TestClient, pdb: PortfolioDb) -> None:
        pdb.create_portfolio("Test")
        pdb.add_item(1, "7203", "トヨタ", 100, 2500.0, "2024-01-15")
        resp = client.delete("/api/portfolio/1/items/1")
        assert resp.status_code == 200
        assert resp.json()["success"] is True

    def test_not_found(self, client: TestClient, pdb: PortfolioDb) -> None:
        pdb.create_portfolio("Test")
        resp = client.delete("/api/portfolio/1/items/999")
        assert resp.status_code == 404


# ===== Upsert Stock =====


class TestUpsertStock:
    def test_upsert_new(self, client: TestClient) -> None:
        resp = client.put(
            "/api/portfolio/MyPort/stocks/7203",
            json={
                "companyName": "トヨタ",
                "quantity": 100,
                "purchasePrice": 2500.0,
                "purchaseDate": "2024-01-15",
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["code"] == "7203"
        assert data["quantity"] == 100

    def test_upsert_update(self, client: TestClient, pdb: PortfolioDb) -> None:
        pdb.create_portfolio("MyPort")
        pdb.add_item(1, "7203", "トヨタ", 100, 2500.0, "2024-01-15")
        resp = client.put(
            "/api/portfolio/MyPort/stocks/7203",
            json={
                "companyName": "トヨタ自動車",
                "quantity": 200,
                "purchasePrice": 2600.0,
                "purchaseDate": "2024-02-01",
            },
        )
        assert resp.status_code == 200
        assert resp.json()["quantity"] == 200


# ===== Delete Stock =====


class TestDeleteStock:
    def test_delete(self, client: TestClient, pdb: PortfolioDb) -> None:
        pdb.create_portfolio("MyPort")
        pdb.add_item(1, "7203", "トヨタ", 100, 2500.0, "2024-01-15")
        resp = client.delete("/api/portfolio/MyPort/stocks/7203")
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is True
        assert data["deletedItem"]["code"] == "7203"

    def test_portfolio_not_found(self, client: TestClient) -> None:
        resp = client.delete("/api/portfolio/NonExistent/stocks/7203")
        assert resp.status_code == 404

    def test_stock_not_found(self, client: TestClient, pdb: PortfolioDb) -> None:
        pdb.create_portfolio("MyPort")
        resp = client.delete("/api/portfolio/MyPort/stocks/9999")
        assert resp.status_code == 404


# ===== Get Codes =====


class TestGetCodes:
    def test_get_codes(self, client: TestClient, pdb: PortfolioDb) -> None:
        pdb.create_portfolio("MyPort")
        pdb.add_item(1, "7203", "トヨタ", 100, 2500.0, "2024-01-15")
        pdb.add_item(1, "6758", "ソニー", 50, 1500.0, "2024-01-15")
        resp = client.get("/api/portfolio/MyPort/codes")
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "MyPort"
        assert set(data["codes"]) == {"7203", "6758"}

    def test_empty(self, client: TestClient) -> None:
        resp = client.get("/api/portfolio/NonExistent/codes")
        assert resp.status_code == 200
        assert resp.json()["codes"] == []


# ===== DB Not Initialized =====


class TestDbNotInitialized:
    def test_no_db(self) -> None:
        app = create_app()
        app.state.portfolio_db = None
        c = TestClient(app, raise_server_exceptions=False)
        resp = c.get("/api/portfolio")
        assert resp.status_code == 422
