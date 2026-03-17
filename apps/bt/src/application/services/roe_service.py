"""
ROE Calculation Service

local market.duckdb の statements を基に ROE を計算する。
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Callable, cast

from src.application.services.analytics_data_provider import (
    AnalyticsDataProvider,
    MarketAnalyticsDataProvider,
)
from src.application.services.analytics_provenance import build_market_provenance
from src.domains.fundamentals.roe import (
    calculate_single_roe as _calculate_single_roe_domain,
    should_prefer as _should_prefer,
)
from src.entrypoints.http.schemas.analytics_common import ResponseDiagnostics
from src.entrypoints.http.schemas.analytics_roe import (
    ROEMetadata,
    ROEResponse,
    ROEResultItem,
    ROESummary,
)
from src.infrastructure.db.market.market_reader import MarketDbReader


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


def _statement_row_to_roe_input(stmt: dict[str, Any]) -> dict[str, Any]:
    return {
        "Code": stmt.get("Code", ""),
        "CurPerType": stmt.get("CurPerType", ""),
        "CurPerEn": stmt.get("CurPerEn", "") or stmt.get("DiscDate", ""),
        "DocType": stmt.get("DocType", ""),
        "NP": stmt.get("NP"),
        "Eq": stmt.get("Eq"),
    }


class ROEService:
    """ROE 計算サービス"""

    def __init__(self, provider: AnalyticsDataProvider) -> None:
        self._provider = provider

    def close(self) -> None:
        close = getattr(self._provider, "close", None)
        if callable(close):
            close()

    def _get_statements_for_codes(self, codes: list[str]) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for code in codes:
            df = self._provider.get_statements(
                code,
                period_type="all",
                actual_only=False,
            )
            if df.empty:
                continue
            for disclosed_at, row in df.sort_index().iterrows():
                results.append(
                    {
                        "Code": row.get("code", code),
                        "DiscDate": str(disclosed_at)[:10],
                        "DocType": row.get("typeOfDocument", ""),
                        "CurPerType": row.get("typeOfCurrentPeriod", ""),
                        "CurPerEn": row.get("periodEnd", row.get("curPerEn", str(disclosed_at)[:10])),
                        "NP": row.get("profit"),
                        "Eq": row.get("equity"),
                    }
                )
        return results

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
        if code:
            codes = [c.strip() for c in code.split(",") if c.strip()]
            all_stmts = self._get_statements_for_codes(codes)
        elif date:
            normalized_date = date.replace("-", "")
            disclosed_date = f"{normalized_date[:4]}-{normalized_date[4:6]}-{normalized_date[6:8]}"
            getter = cast(
                Callable[[str], list[dict[str, Any]]] | None,
                getattr(self._provider, "get_statements_by_date", None),
            )
            all_stmts = getter(disclosed_date) if callable(getter) else []
        else:
            all_stmts = []

        best: dict[str, dict[str, Any]] = {}
        for stmt in all_stmts:
            stmt_code = str(stmt.get("Code", ""))[:4]
            if stmt_code in best:
                if _should_prefer(stmt, best[stmt_code]):
                    best[stmt_code] = stmt
            else:
                best[stmt_code] = stmt

        results: list[ROEResultItem] = []
        for stmt in best.values():
            normalized_stmt = _statement_row_to_roe_input(stmt)
            result = _calculate_single_roe_domain(
                normalized_stmt,
                annualize=annualize,
                prefer_consolidated=prefer_consolidated,
                min_equity=min_equity,
            )
            if result is not None:
                results.append(_to_response_item(result))

        if sort_by == "code":
            results.sort(key=lambda r: r.metadata.code)
        elif sort_by == "date":
            results.sort(key=lambda r: r.metadata.periodEnd, reverse=True)
        else:
            results.sort(key=lambda r: r.roe, reverse=True)

        results = results[:limit]
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

        diagnostics = ResponseDiagnostics(
            missing_required_data=[] if all_stmts else ["statements"],
            used_fields=["statements.profit", "statements.equity", "statements.type_of_current_period"],
            effective_period_type="mixed",
        )

        return ROEResponse(
            results=results,
            summary=summary,
            lastUpdated=datetime.now(UTC).isoformat(),
            provenance=build_market_provenance(
                reference_date=date,
                loaded_domains=("statements",),
            ),
            diagnostics=diagnostics,
        )


def create_market_roe_service(reader: MarketDbReader | None) -> ROEService:
    return ROEService(MarketAnalyticsDataProvider(reader=reader))
