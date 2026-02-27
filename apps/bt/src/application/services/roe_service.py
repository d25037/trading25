"""
ROE Calculation Service

JQuants API から取得した財務諸表データを基に ROE を計算する。
Hono shared/fundamental-analysis/roe.ts と同等のロジック。
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from src.infrastructure.external_api.clients.jquants_client import JQuantsAsyncClient
from src.domains.fundamentals.roe import (
    calculate_single_roe as _calculate_single_roe_domain,
    should_prefer as _should_prefer,
)
from src.entrypoints.http.schemas.analytics_roe import (
    ROEMetadata,
    ROEResponse,
    ROEResultItem,
    ROESummary,
)


def _to_response_item(result: Any) -> ROEResultItem:
    return ROEResultItem(
        roe=result.roe,
        netProfit=result.net_profit,
        equity=result.equity,
        metadata=ROEMetadata(
            code=result.metadata.code,
            periodType=result.metadata.period_type,
            periodEnd=result.metadata.period_end,
            isConsolidated=result.metadata.is_consolidated,
            accountingStandard=result.metadata.accounting_standard,
            isAnnualized=result.metadata.is_annualized,
        ),
    )


def _calculate_single_roe(
    stmt: dict[str, Any],
    annualize: bool = True,
    prefer_consolidated: bool = True,
    min_equity: float = 1000,
) -> ROEResultItem | None:
    result = _calculate_single_roe_domain(
        stmt,
        annualize=annualize,
        prefer_consolidated=prefer_consolidated,
        min_equity=min_equity,
    )
    if result is None:
        return None
    return _to_response_item(result)


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
