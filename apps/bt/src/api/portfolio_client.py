"""API client for portfolio operations (using existing API)."""

from __future__ import annotations

from typing import Any

import pandas as pd

from src.api.client import BaseAPIClient


class PortfolioAPIClient(BaseAPIClient):
    """API client for portfolio operations.

    This client provides access to portfolio management functionality
    through the existing localhost:3001 API.

    Endpoints used:
        - GET /api/portfolio - List all portfolios
        - GET /api/portfolio/{id} - Get portfolio details
        - GET /api/portfolio/{id}/items - Get portfolio items
        - GET /api/portfolio/{name}/codes - Get stock codes

    Usage:
        ```python
        with PortfolioAPIClient() as client:
            portfolios = client.get_portfolio_list()
            items = client.get_portfolio_items("my_portfolio")
        ```
    """

    # =========================================================================
    # Portfolio Read Methods
    # =========================================================================

    def get_portfolio_list(self) -> pd.DataFrame:
        """Get list of all portfolios.

        Returns:
            DataFrame with portfolio information
        """
        data = self._get("/api/portfolio")

        if not data:
            return pd.DataFrame()

        # APIレスポンスは {"portfolios": [...]} 形式
        portfolios = data.get("portfolios", []) if isinstance(data, dict) else data
        return pd.DataFrame(portfolios)

    def get_portfolio(self, portfolio_id: int) -> dict[str, Any]:
        """Get portfolio details by ID.

        Args:
            portfolio_id: Portfolio ID

        Returns:
            Dictionary with portfolio details including items
        """
        data = self._get(f"/api/portfolio/{portfolio_id}")
        return data if isinstance(data, dict) else {}

    def get_portfolio_by_name(self, name: str) -> dict[str, Any]:
        """Get portfolio details by name.

        Args:
            name: Portfolio name

        Returns:
            Dictionary with portfolio details
        """
        # First, get all portfolios and find by name
        data = self._get("/api/portfolio")

        if not data:
            return {}

        # APIレスポンスは {"portfolios": [...]} 形式
        portfolios = data.get("portfolios", []) if isinstance(data, dict) else data

        for p in portfolios:
            if p.get("name") == name:
                portfolio_id = p.get("id")
                if portfolio_id is not None:
                    return self.get_portfolio(int(portfolio_id))

        return {}

    # =========================================================================
    # Portfolio Items Methods
    # =========================================================================

    def get_portfolio_items(self, portfolio_id: int) -> pd.DataFrame:
        """Get all items in a portfolio.

        Args:
            portfolio_id: Portfolio ID

        Returns:
            DataFrame with portfolio items
        """
        data = self._get(f"/api/portfolio/{portfolio_id}")

        if not data or not isinstance(data, dict):
            return pd.DataFrame()

        items = data.get("items", [])
        return pd.DataFrame(items) if items else pd.DataFrame()

    def get_portfolio_items_by_name(self, name: str) -> pd.DataFrame:
        """Get all items in a portfolio by portfolio name.

        Args:
            name: Portfolio name

        Returns:
            DataFrame with portfolio items
        """
        portfolio = self.get_portfolio_by_name(name)

        if not portfolio:
            return pd.DataFrame()

        items = portfolio.get("items", [])
        return pd.DataFrame(items) if items else pd.DataFrame()

    # =========================================================================
    # Convenience Methods
    # =========================================================================

    def get_portfolio_codes(self, name: str) -> list[str]:
        """Get list of stock codes in a portfolio.

        Args:
            name: Portfolio name

        Returns:
            List of stock codes
        """
        items = self.get_portfolio_items_by_name(name)

        if items.empty:
            return []

        return items["code"].tolist()

    def get_portfolio_summary(self, portfolio_id: int) -> dict[str, Any]:
        """Get portfolio summary with calculated values.

        Args:
            portfolio_id: Portfolio ID

        Returns:
            Dictionary with summary information
        """
        portfolio = self.get_portfolio(portfolio_id)

        if not portfolio:
            return {}

        items = portfolio.get("items", [])
        total_value = sum(
            item.get("quantity", 0) * item.get("purchase_price", 0)
            for item in items
        )

        return {
            "name": portfolio.get("name"),
            "description": portfolio.get("description"),
            "item_count": len(items),
            "total_invested": total_value,
            "stock_codes": [item.get("code") for item in items],
        }
