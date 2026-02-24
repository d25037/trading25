"""Watchlist ルートのテスト"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from src.entrypoints.http.app import create_app
from src.infrastructure.db.market.portfolio_db import PortfolioDb


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


# ===== List Watchlists =====


class TestListWatchlists:
    def test_empty(self, client: TestClient) -> None:
        resp = client.get("/api/watchlist")
        assert resp.status_code == 200
        assert resp.json()["watchlists"] == []

    def test_with_items(self, client: TestClient, pdb: PortfolioDb) -> None:
        pdb.create_watchlist("wl1", "desc")
        pdb.add_watchlist_item(1, "7203", "トヨタ")
        resp = client.get("/api/watchlist")
        assert resp.status_code == 200
        data = resp.json()["watchlists"]
        assert len(data) == 1
        assert data[0]["name"] == "wl1"
        assert data[0]["stockCount"] == 1


# ===== Create Watchlist =====


class TestCreateWatchlist:
    def test_create(self, client: TestClient) -> None:
        resp = client.post("/api/watchlist", json={"name": "Tech", "description": "Tech stocks"})
        assert resp.status_code == 201
        data = resp.json()
        assert data["name"] == "Tech"
        assert data["description"] == "Tech stocks"

    def test_create_duplicate(self, client: TestClient) -> None:
        client.post("/api/watchlist", json={"name": "Tech"})
        resp = client.post("/api/watchlist", json={"name": "Tech"})
        assert resp.status_code == 409

    def test_create_empty_name(self, client: TestClient) -> None:
        resp = client.post("/api/watchlist", json={"name": ""})
        assert resp.status_code == 422


# ===== Get Watchlist =====


class TestGetWatchlist:
    def test_get(self, client: TestClient, pdb: PortfolioDb) -> None:
        pdb.create_watchlist("Tech")
        pdb.add_watchlist_item(1, "7203", "トヨタ", memo="注目")
        resp = client.get("/api/watchlist/1")
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "Tech"
        assert len(data["items"]) == 1
        assert data["items"][0]["code"] == "7203"
        assert data["items"][0]["memo"] == "注目"
        assert data["items"][0]["watchlistId"] == 1

    def test_not_found(self, client: TestClient) -> None:
        resp = client.get("/api/watchlist/999")
        assert resp.status_code == 404


# ===== Update Watchlist =====


class TestUpdateWatchlist:
    def test_update(self, client: TestClient, pdb: PortfolioDb) -> None:
        pdb.create_watchlist("Old")
        resp = client.put("/api/watchlist/1", json={"name": "New"})
        assert resp.status_code == 200
        assert resp.json()["name"] == "New"

    def test_not_found(self, client: TestClient) -> None:
        resp = client.put("/api/watchlist/999", json={"name": "New"})
        assert resp.status_code == 404

    def test_duplicate_name(self, client: TestClient, pdb: PortfolioDb) -> None:
        pdb.create_watchlist("A")
        pdb.create_watchlist("B")
        resp = client.put("/api/watchlist/2", json={"name": "A"})
        assert resp.status_code == 409


# ===== Delete Watchlist =====


class TestDeleteWatchlist:
    def test_delete(self, client: TestClient, pdb: PortfolioDb) -> None:
        pdb.create_watchlist("Test")
        resp = client.delete("/api/watchlist/1")
        assert resp.status_code == 200
        assert resp.json()["success"] is True

    def test_not_found(self, client: TestClient) -> None:
        resp = client.delete("/api/watchlist/999")
        assert resp.status_code == 404


# ===== Add Watchlist Item =====


class TestAddWatchlistItem:
    def test_add(self, client: TestClient, pdb: PortfolioDb) -> None:
        pdb.create_watchlist("Tech")
        resp = client.post(
            "/api/watchlist/1/items",
            json={"code": "7203", "companyName": "トヨタ", "memo": "注目"},
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["code"] == "7203"
        assert data["memo"] == "注目"

    def test_watchlist_not_found(self, client: TestClient) -> None:
        resp = client.post(
            "/api/watchlist/999/items",
            json={"code": "7203", "companyName": "トヨタ"},
        )
        assert resp.status_code == 404

    def test_duplicate_stock(self, client: TestClient, pdb: PortfolioDb) -> None:
        pdb.create_watchlist("Tech")
        pdb.add_watchlist_item(1, "7203", "トヨタ")
        resp = client.post(
            "/api/watchlist/1/items",
            json={"code": "7203", "companyName": "トヨタ"},
        )
        assert resp.status_code == 409

    def test_invalid_code(self, client: TestClient, pdb: PortfolioDb) -> None:
        pdb.create_watchlist("Tech")
        resp = client.post(
            "/api/watchlist/1/items",
            json={"code": "bad", "companyName": "Test"},
        )
        assert resp.status_code == 422


# ===== Delete Watchlist Item =====


class TestDeleteWatchlistItem:
    def test_delete(self, client: TestClient, pdb: PortfolioDb) -> None:
        pdb.create_watchlist("Tech")
        pdb.add_watchlist_item(1, "7203", "トヨタ")
        resp = client.delete("/api/watchlist/1/items/1")
        assert resp.status_code == 200
        assert resp.json()["success"] is True

    def test_watchlist_not_found(self, client: TestClient) -> None:
        resp = client.delete("/api/watchlist/999/items/1")
        assert resp.status_code == 404

    def test_item_not_found(self, client: TestClient, pdb: PortfolioDb) -> None:
        pdb.create_watchlist("Tech")
        resp = client.delete("/api/watchlist/1/items/999")
        assert resp.status_code == 404
