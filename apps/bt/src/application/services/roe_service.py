"""
ROE Calculation Service

JQuants API から取得した財務諸表データを基に ROE を計算する。
Hono shared/fundamental-analysis/roe.ts と同等のロジック。
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from src.infrastructure.external_api.clients.jquants_client import JQuantsAsyncClient
from src.entrypoints.http.schemas.analytics_roe import (
    ROEMetadata,
    ROEResponse,
    ROEResultItem,
    ROESummary,
)

# 四半期の年換算乗数
_QUARTER_MULTIPLIER: dict[str, float] = {
    "1Q": 4.0,
    "2Q": 2.0,
    "3Q": 4.0 / 3.0,
}


def _normalize_period_type(cur_per_type: str) -> str:
    """CurPerType を正規化する"""
    if not cur_per_type:
        return "FY"
    t = cur_per_type.strip()
    if t in ("FY", "1Q", "2Q", "3Q"):
        return t
    # JQuants の TypeOfCurrentPeriod
    upper = t.upper()
    if "1Q" in upper or "Q1" in upper:
        return "1Q"
    if "2Q" in upper or "Q2" in upper or "HALF" in upper:
        return "2Q"
    if "3Q" in upper or "Q3" in upper:
        return "3Q"
    return "FY"


def _is_quarterly(period_type: str) -> bool:
    return period_type in ("1Q", "2Q", "3Q")


def _is_consolidated_doc(doc_type: str | None) -> bool:
    if not doc_type:
        return True  # default consolidated
    return "consolidated" in doc_type.lower()


def _extract_accounting_standard(doc_type: str | None) -> str | None:
    if not doc_type:
        return "JGAAP"
    lower = doc_type.lower()
    if "ifrs" in lower:
        return "IFRS"
    if "us" in lower and "gaap" in lower:
        return "US GAAP"
    return "JGAAP"


def _calculate_single_roe(
    stmt: dict[str, Any],
    annualize: bool = True,
    prefer_consolidated: bool = True,
    min_equity: float = 1000,
) -> ROEResultItem | None:
    """単一の財務諸表から ROE を計算する"""
    # Profit / Equity 抽出
    if prefer_consolidated:
        net_profit = stmt.get("NP") or stmt.get("NCNP")
        equity = stmt.get("Eq") or stmt.get("NCEq")
    else:
        net_profit = stmt.get("NCNP") or stmt.get("NP")
        equity = stmt.get("NCEq") or stmt.get("Eq")

    if net_profit is None or equity is None:
        return None
    if abs(equity) < min_equity or equity <= 0:
        return None

    period_type = _normalize_period_type(stmt.get("CurPerType", ""))

    adjusted_profit = net_profit
    is_annualized = False
    if annualize and _is_quarterly(period_type):
        multiplier = _QUARTER_MULTIPLIER.get(period_type, 1.0)
        adjusted_profit = net_profit * multiplier
        is_annualized = True

    roe = (adjusted_profit / equity) * 100

    code = str(stmt.get("Code", ""))[:4]

    return ROEResultItem(
        roe=round(roe, 4),
        netProfit=adjusted_profit,
        equity=equity,
        metadata=ROEMetadata(
            code=code,
            periodType=period_type,
            periodEnd=stmt.get("CurPerEn", ""),
            isConsolidated=_is_consolidated_doc(stmt.get("DocType")),
            accountingStandard=_extract_accounting_standard(stmt.get("DocType")),
            isAnnualized=is_annualized,
        ),
    )


def _should_prefer(new_stmt: dict[str, Any], current_stmt: dict[str, Any]) -> bool:
    """新しい statement を優先すべきかどうか"""
    new_type = _normalize_period_type(new_stmt.get("CurPerType", ""))
    cur_type = _normalize_period_type(current_stmt.get("CurPerType", ""))

    if new_type == "FY" and cur_type != "FY":
        return True
    if new_type != "FY" and cur_type == "FY":
        return False

    new_end = new_stmt.get("CurPerEn", "")
    cur_end = current_stmt.get("CurPerEn", "")
    return new_end > cur_end


class ROEService:
    """ROE 計算サービス"""

    def __init__(self, client: JQuantsAsyncClient) -> None:
        self._client = client

    async def calculate_roe(
        self,
        code: str | None = None,
        date: str | None = None,
        annualize: bool = True,
        prefer_consolidated: bool = True,
        min_equity: float = 1000,
        sort_by: str = "roe",
        limit: int = 50,
    ) -> ROEResponse:
        """ROE を計算して返す

        code または date のいずれかが必須。
        """
        # コード指定の場合
        if code:
            codes = [c.strip() for c in code.split(",")]
            all_stmts: list[dict[str, Any]] = []
            for c in codes:
                body = await self._client.get("/fins/summary", {"code": c})
                stmts = body.get("data", [])
                all_stmts.extend(stmts)
        elif date:
            # 日付指定: 全銘柄の特定日の statements
            normalized_date = date.replace("-", "")
            body = await self._client.get("/fins/summary", {"date": normalized_date})
            all_stmts = body.get("data", [])
        else:
            all_stmts = []

        # 銘柄ごとに最適な statement を選択
        best: dict[str, dict[str, Any]] = {}
        for stmt in all_stmts:
            stmt_code = str(stmt.get("Code", ""))[:4]
            if stmt_code in best:
                if _should_prefer(stmt, best[stmt_code]):
                    best[stmt_code] = stmt
            else:
                best[stmt_code] = stmt

        # ROE 計算
        results: list[ROEResultItem] = []
        for stmt in best.values():
            item = _calculate_single_roe(stmt, annualize, prefer_consolidated, min_equity)
            if item:
                results.append(item)

        # ソート
        if sort_by == "code":
            results.sort(key=lambda r: r.metadata.code)
        elif sort_by == "date":
            results.sort(key=lambda r: r.metadata.periodEnd, reverse=True)
        else:
            results.sort(key=lambda r: r.roe, reverse=True)

        # リミット
        results = results[:limit]

        # サマリー
        if results:
            roes = [r.roe for r in results]
            summary = ROESummary(
                averageROE=round(sum(roes) / len(roes), 4),
                maxROE=round(max(roes), 4),
                minROE=round(min(roes), 4),
                totalCompanies=len(results),
            )
        else:
            summary = ROESummary(averageROE=0, maxROE=0, minROE=0, totalCompanies=0)

        return ROEResponse(
            results=results,
            summary=summary,
            lastUpdated=datetime.now(UTC).isoformat(),
        )
