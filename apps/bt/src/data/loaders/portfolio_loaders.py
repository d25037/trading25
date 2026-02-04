"""
ポートフォリオデータローダー

localhost:3001 API経由でportfolio管理とmarket.dbを統合して、
ポートフォリオ分析用のデータを提供します。
"""

from __future__ import annotations

from typing import Optional, List, Dict
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
from loguru import logger

from src.api.portfolio_client import PortfolioAPIClient
from src.api.market_client import MarketAPIClient
from ...models.portfolio import Portfolio, PortfolioItem, PortfolioSummary


def _convert_portfolio_code_to_market_code(code: str) -> str:
    """
    portfolio.dbの銘柄コードをmarket.dbの銘柄コードに変換

    Args:
        code: portfolio.dbの銘柄コード（例: "2207", "285A"）

    Returns:
        str: market.dbの銘柄コード（例: "22070", "285A0"）

    Note:
        4文字の銘柄コードは末尾に"0"を追加して5桁にする
        （数字のみ・英数字混在問わず）
    """
    # 4文字の場合は末尾に"0"を追加
    if len(code) == 4:
        return code + "0"
    # それ以外はそのまま返す
    return code


def load_portfolio_list() -> pd.DataFrame:
    """
    ポートフォリオ一覧を取得

    Returns:
        pd.DataFrame: ポートフォリオ一覧
    """
    with PortfolioAPIClient() as client:
        df = client.get_portfolio_list()

    return df


def load_portfolio_code_name_mapping(portfolio_name: str) -> Dict[str, str]:
    """
    ポートフォリオ銘柄のコード→会社名マッピングを取得

    Args:
        portfolio_name: ポートフォリオ名

    Returns:
        Dict[str, str]: {銘柄コード: 会社名} の辞書
    """
    summary = load_portfolio_summary(portfolio_name)
    return {item.code: item.company_name for item in summary.items}


def load_portfolio_summary(portfolio_name: str) -> PortfolioSummary:
    """
    ポートフォリオサマリーを取得

    Args:
        portfolio_name: ポートフォリオ名

    Returns:
        PortfolioSummary: ポートフォリオサマリー

    Raises:
        ValueError: ポートフォリオが見つからない場合
    """
    with PortfolioAPIClient() as client:
        portfolio_data = client.get_portfolio_by_name(portfolio_name)

        if not portfolio_data:
            raise ValueError(f"Portfolio '{portfolio_name}' not found")

        # Portfolio オブジェクトを作成
        portfolio = Portfolio(
            id=int(portfolio_data["id"]),
            name=str(portfolio_data["name"]),
            description=str(portfolio_data.get("description", ""))
            if portfolio_data.get("description")
            else None,
            created_at=pd.to_datetime(portfolio_data["createdAt"]),
            updated_at=pd.to_datetime(portfolio_data["updatedAt"]),
        )

        # 保有銘柄情報取得
        items_list = portfolio_data.get("items", [])
        items: List[PortfolioItem] = []
        total_cost = 0.0

        for item_data in items_list:
            item = PortfolioItem(
                id=int(item_data["id"]),
                portfolio_id=int(item_data["portfolioId"]),
                code=str(item_data["code"]),
                company_name=str(item_data["companyName"]),
                quantity=int(item_data["quantity"]),
                purchase_price=float(item_data["purchasePrice"]),
                purchase_date=pd.to_datetime(item_data["purchaseDate"]).date(),
                account=str(item_data.get("account", ""))
                if item_data.get("account")
                else None,
                notes=str(item_data.get("notes", ""))
                if item_data.get("notes")
                else None,
                created_at=pd.to_datetime(item_data["createdAt"]),
                updated_at=pd.to_datetime(item_data["updatedAt"]),
            )
            items.append(item)
            total_cost += item.total_cost

    return PortfolioSummary(
        portfolio=portfolio,
        items=items,
        total_stocks=len(items),
        total_cost=total_cost,
    )


def load_portfolio_stock_data(
    portfolio_name: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    lookback_days: int = 252,
) -> Dict[str, pd.DataFrame]:
    """
    ポートフォリオ保有銘柄の株価データを一括取得

    Args:
        portfolio_name: ポートフォリオ名
        start_date: 開始日 (YYYY-MM-DD)。指定しない場合は lookback_days から計算
        end_date: 終了日 (YYYY-MM-DD)。指定しない場合は現在日
        lookback_days: 遡る営業日数（デフォルト252日 ≈ 1年）

    Returns:
        Dict[str, pd.DataFrame]: {銘柄コード: OHLCVデータ(DatetimeIndex)} の辞書

    Raises:
        ValueError: ポートフォリオが見つからない場合
    """
    # 銘柄コード一覧取得
    with PortfolioAPIClient() as portfolio_client:
        codes = portfolio_client.get_portfolio_codes(portfolio_name)

    if not codes:
        raise ValueError(f"No stocks found in portfolio '{portfolio_name}'")

    logger.info(
        f"Loading stock data for {len(codes)} stocks in portfolio '{portfolio_name}'"
    )

    # 日付範囲の設定
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")
    if start_date is None:
        # lookback_days 分遡る（営業日換算で余裕を持たせる）
        start_dt = datetime.now() - timedelta(days=int(lookback_days * 1.5))
        start_date = start_dt.strftime("%Y-%m-%d")

    # 各銘柄のデータを取得
    stock_data: Dict[str, pd.DataFrame] = {}
    with MarketAPIClient() as market_client:
        for code in codes:
            try:
                # portfolio.dbのコードをmarket.dbのコードに変換
                market_code = _convert_portfolio_code_to_market_code(code)
                logger.debug(f"Converting code: {code} -> {market_code}")

                df = market_client.get_stock_ohlcv(market_code, start_date, end_date)
                if df.empty:
                    logger.warning(
                        f"No data found for stock {code} (market code: {market_code})"
                    )
                    continue

                # DatetimeIndex に変換（VectorBT用）- APIクライアントで既に変換済み
                df.sort_index(inplace=True)

                # カラム名を統一（大文字化）- APIクライアントで既に変換済み
                # df.columns = df.columns.str.capitalize()

                # 元のportfolio.dbのコードをキーとして保存
                stock_data[code] = df

            except Exception as e:
                logger.error(f"Failed to load data for stock {code}: {e}")
                continue

    logger.info(
        f"Successfully loaded data for {len(stock_data)}/{len(codes)} stocks"
    )
    return stock_data


def create_portfolio_returns_matrix(
    stock_data: Dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """
    ポートフォリオ銘柄の日次リターン行列を作成

    Args:
        stock_data: {銘柄コード: OHLCVデータ} の辞書

    Returns:
        pd.DataFrame: 日次リターン行列（行: 日付, 列: 銘柄コード）
                     各値は日次リターン（Close価格の変化率）
    """
    returns_dict: Dict[str, pd.Series[float]] = {}

    for code, df in stock_data.items():
        if "Close" not in df.columns:
            logger.warning(f"Stock {code} has no 'Close' column, skipping")
            continue

        # 日次リターン計算（pct_change）
        returns: pd.Series[float] = df["Close"].pct_change()
        returns_dict[code] = returns

    # DataFrame に変換
    returns_df = pd.DataFrame(returns_dict)

    # NaN/Inf を除去（初日のリターンはNaN、計算エラーはInf）
    returns_df = returns_df.replace([np.inf, -np.inf], np.nan)
    returns_df = returns_df.dropna(how="all")  # 全てNaNの行を削除

    logger.info(
        f"Created returns matrix: {returns_df.shape[0]} days × {returns_df.shape[1]} stocks"
    )

    return returns_df


def create_portfolio_price_matrix(
    stock_data: Dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """
    ポートフォリオ銘柄の終値行列を作成

    Args:
        stock_data: {銘柄コード: OHLCVデータ} の辞書

    Returns:
        pd.DataFrame: 終値行列（行: 日付, 列: 銘柄コード）
    """
    price_dict: Dict[str, pd.Series[float]] = {}

    for code, df in stock_data.items():
        if "Close" not in df.columns:
            logger.warning(f"Stock {code} has no 'Close' column, skipping")
            continue

        price_dict[code] = df["Close"]

    price_df = pd.DataFrame(price_dict)
    logger.info(
        f"Created price matrix: {price_df.shape[0]} days × {price_df.shape[1]} stocks"
    )

    return price_df
