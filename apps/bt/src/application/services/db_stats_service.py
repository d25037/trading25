"""
DB Stats Service

GET /api/db/stats のビジネスロジック。
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from src.application.services.listed_market_targets import (
    build_fundamentals_coverage,
    build_fundamentals_target_map,
    normalize_frontier_date,
    resolve_frontier_cache_codes,
)
from src.application.services.intraday_schedule import build_intraday_freshness
from src.infrastructure.db.market.time_series_store import TimeSeriesInspection
from src.infrastructure.db.market.market_db import METADATA_KEYS
from src.entrypoints.http.schemas.db import (
    AdjustedMetricsStats,
    DateRange,
    FundamentalsStats,
    IndicesStats,
    IntradayFreshness,
    ListedMarketCoverage,
    MarginStats,
    MarketStatsResponse,
    MarketSchemaStats,
    Options225Stats,
    StockMasterCoverageStats,
    StockDataStats,
    StockMinuteDataStats,
    StorageStats,
    StockStats,
    TopixStats,
)

_STALE_STORAGE_ARTIFACT_LIMIT = 20
_STALE_STORAGE_ARTIFACT_SUFFIXES = (".tmp", ".bak", ".backup", ".old")


class MarketDbStatsLike(Protocol):
    def is_initialized(self) -> bool: ...
    def get_sync_metadata(self, key: str) -> str | None: ...
    def get_stats(self) -> dict[str, int]: ...
    def get_market_schema_version(self) -> int | None: ...
    def is_market_schema_current(self) -> bool: ...
    def get_stock_master_coverage(self) -> dict[str, Any]: ...
    def get_stock_count_by_market(self) -> dict[str, int]: ...
    def get_fundamentals_target_stock_rows(self) -> list[dict[str, str]]: ...
    def get_index_master_category_counts(self) -> dict[str, int]: ...
    def get_adjusted_metrics_snapshot(self) -> dict[str, Any]: ...


class TimeSeriesStoreStatsLike(Protocol):
    def inspect(self) -> TimeSeriesInspection: ...
    def get_storage_stats(self) -> object: ...


def _resolve_duckdb_size_bytes(time_series_store: object) -> int:
    duckdb_path = getattr(time_series_store, "_duckdb_path", None)
    if not isinstance(duckdb_path, Path):
        return 0
    try:
        return int(duckdb_path.stat().st_size) if duckdb_path.exists() else 0
    except OSError:
        return 0


def _resolve_parquet_size_bytes(time_series_store: object) -> int:
    parquet_dir = getattr(time_series_store, "_parquet_dir", None)
    if not isinstance(parquet_dir, Path):
        return 0
    try:
        if not parquet_dir.exists():
            return 0
        total = 0
        for file_path in parquet_dir.rglob("*.parquet"):
            if not file_path.is_file():
                continue
            total += int(file_path.stat().st_size)
        return total
    except OSError:
        return 0


def _is_stale_storage_artifact(path: Path) -> bool:
    name = path.name.lower()
    return any(name.endswith(suffix) for suffix in _STALE_STORAGE_ARTIFACT_SUFFIXES)


def _resolve_stale_storage_artifacts(
    *,
    duckdb_path: Path | None,
    parquet_dir: Path | None,
    limit: int = _STALE_STORAGE_ARTIFACT_LIMIT,
) -> tuple[int, list[str]]:
    artifacts: list[str] = []
    try:
        if duckdb_path is not None and duckdb_path.parent.exists():
            for path in duckdb_path.parent.iterdir():
                if not path.is_file() or not _is_stale_storage_artifact(path):
                    continue
                artifacts.append(path.name)
        if parquet_dir is not None and parquet_dir.exists():
            for path in parquet_dir.rglob("*"):
                if not path.is_file() or not _is_stale_storage_artifact(path):
                    continue
                rel_path = path.relative_to(parquet_dir)
                artifacts.append(f"{parquet_dir.name}/{rel_path.as_posix()}")
    except OSError:
        return 0, []

    unique_artifacts = sorted(set(artifacts))
    return len(unique_artifacts), unique_artifacts[:limit]


def _resolve_storage_stats(time_series_store: object) -> StorageStats:
    get_storage_stats = getattr(time_series_store, "get_storage_stats", None)
    if callable(get_storage_stats):
        stats = get_storage_stats()
        duckdb_bytes = getattr(stats, "duckdb_bytes", None)
        parquet_bytes = getattr(stats, "parquet_bytes", None)
        duckdb_blocks_total = getattr(stats, "duckdb_blocks_total", 0)
        duckdb_blocks_used = getattr(stats, "duckdb_blocks_used", 0)
        duckdb_blocks_free = getattr(stats, "duckdb_blocks_free", 0)
        duckdb_bytes_free = getattr(stats, "duckdb_bytes_free", 0)
        duckdb_wal_bytes = getattr(stats, "duckdb_wal_bytes", 0)
        temp_directory = getattr(stats, "temp_directory", None)
        temp_bytes = getattr(stats, "temp_bytes", 0)
        stale_artifact_count = getattr(stats, "stale_artifact_count", 0)
        stale_artifacts = getattr(stats, "stale_artifacts", [])
        if isinstance(duckdb_bytes, int) and isinstance(parquet_bytes, int):
            return StorageStats(
                duckdbBytes=duckdb_bytes,
                parquetBytes=parquet_bytes,
                totalBytes=duckdb_bytes + parquet_bytes,
                duckdbBlocksTotal=duckdb_blocks_total
                if isinstance(duckdb_blocks_total, int)
                else 0,
                duckdbBlocksUsed=duckdb_blocks_used
                if isinstance(duckdb_blocks_used, int)
                else 0,
                duckdbBlocksFree=duckdb_blocks_free
                if isinstance(duckdb_blocks_free, int)
                else 0,
                duckdbBytesFree=duckdb_bytes_free
                if isinstance(duckdb_bytes_free, int)
                else 0,
                duckdbWalBytes=duckdb_wal_bytes
                if isinstance(duckdb_wal_bytes, int)
                else 0,
                tempDirectory=temp_directory
                if isinstance(temp_directory, str)
                else None,
                tempBytes=temp_bytes if isinstance(temp_bytes, int) else 0,
                staleArtifactCount=stale_artifact_count
                if isinstance(stale_artifact_count, int)
                else 0,
                staleArtifacts=[
                    str(artifact)
                    for artifact in stale_artifacts
                    if isinstance(artifact, str)
                ][:_STALE_STORAGE_ARTIFACT_LIMIT],
            )

    duckdb_path = getattr(time_series_store, "_duckdb_path", None)
    parquet_dir = getattr(time_series_store, "_parquet_dir", None)
    duckdb_bytes = _resolve_duckdb_size_bytes(time_series_store)
    parquet_bytes = _resolve_parquet_size_bytes(time_series_store)
    stale_artifact_count, stale_artifacts = _resolve_stale_storage_artifacts(
        duckdb_path=duckdb_path if isinstance(duckdb_path, Path) else None,
        parquet_dir=parquet_dir if isinstance(parquet_dir, Path) else None,
    )
    return StorageStats(
        duckdbBytes=duckdb_bytes,
        parquetBytes=parquet_bytes,
        totalBytes=duckdb_bytes + parquet_bytes,
        staleArtifactCount=stale_artifact_count,
        staleArtifacts=stale_artifacts,
    )


def get_market_stats(
    market_db: MarketDbStatsLike,
    *,
    time_series_store: TimeSeriesStoreStatsLike,
) -> MarketStatsResponse:
    """DuckDB 時系列 SoT と market metadata を統合した統計情報を返す。"""
    initialized = market_db.is_initialized()
    last_sync = market_db.get_sync_metadata(METADATA_KEYS["LAST_SYNC_DATE"])
    last_intraday_sync = market_db.get_sync_metadata(METADATA_KEYS["LAST_INTRADAY_SYNC"])
    inspection = time_series_store.inspect()

    # Metadata / reference data (DuckDB metadata tables)
    basic = market_db.get_stats()
    master_coverage = market_db.get_stock_master_coverage()
    by_market = market_db.get_stock_count_by_market()
    index_master_by_category = market_db.get_index_master_category_counts()
    statement_codes = set(inspection.statement_codes)
    latest_disclosed_date = inspection.latest_statement_disclosed_date
    fundamentals_target_map = build_fundamentals_target_map(
        market_db.get_fundamentals_target_stock_rows()
    )
    fundamentals_frontier = normalize_frontier_date(
        market_db.get_sync_metadata(METADATA_KEYS["FUNDAMENTALS_LAST_DISCLOSED_DATE"])
        or latest_disclosed_date
    )
    fundamentals_empty_skipped_codes = resolve_frontier_cache_codes(
        market_db.get_sync_metadata(METADATA_KEYS["FUNDAMENTALS_EMPTY_CODES"]),
        fundamentals_frontier,
    )
    fundamentals_coverage = build_fundamentals_coverage(
        fundamentals_target_map,
        statement_codes,
        empty_skipped_codes=fundamentals_empty_skipped_codes,
        limit_missing=0,
        limit_empty=0,
    )
    storage = _resolve_storage_stats(time_series_store)
    adjusted_metrics = _build_adjusted_metrics_stats(
        market_db.get_adjusted_metrics_snapshot(),
        source_stock_count=inspection.stock_count,
        source_statement_count=inspection.statements_count,
        latest_stock_date=inspection.stock_max,
    )

    # Topix
    topix = TopixStats(
        count=inspection.topix_count,
        dateRange=DateRange(min=inspection.topix_min, max=inspection.topix_max)
        if inspection.topix_min and inspection.topix_max
        else None,
    )

    # Stocks
    stocks_stats = StockStats(
        total=basic.get("stocks", 0),
        byMarket=by_market,
    )

    # Stock data
    stock_data = StockDataStats(
        count=inspection.stock_count,
        dateCount=inspection.stock_date_count,
        dateRange=DateRange(min=inspection.stock_min, max=inspection.stock_max)
        if inspection.stock_min and inspection.stock_max
        else None,
        averageStocksPerDay=(
            round(inspection.stock_count / inspection.stock_date_count, 2)
            if inspection.stock_date_count > 0
            else 0.0
        ),
    )
    stock_minute_data = StockMinuteDataStats(
        count=inspection.stock_minute_count,
        uniqueStockCount=inspection.stock_minute_code_count,
        dateCount=inspection.stock_minute_date_count,
        dateRange=DateRange(
            min=inspection.stock_minute_min,
            max=inspection.stock_minute_max,
        )
        if inspection.stock_minute_min and inspection.stock_minute_max
        else None,
        latestTime=inspection.latest_stock_minute_time,
        averageBarsPerDay=(
            round(inspection.stock_minute_count / inspection.stock_minute_date_count, 2)
            if inspection.stock_minute_date_count > 0
            else 0.0
        ),
    )

    # Indices
    indices = IndicesStats(
        masterCount=basic.get("index_master", 0),
        dataCount=inspection.indices_count,
        dateCount=inspection.indices_date_count,
        dateRange=DateRange(min=inspection.indices_min, max=inspection.indices_max)
        if inspection.indices_min and inspection.indices_max
        else None,
        byCategory=index_master_by_category,
    )

    options_225 = Options225Stats(
        count=inspection.options_225_count,
        dateCount=inspection.options_225_date_count,
        dateRange=DateRange(min=inspection.options_225_min, max=inspection.options_225_max)
        if inspection.options_225_min and inspection.options_225_max
        else None,
    )

    margin = MarginStats(
        count=inspection.margin_count,
        uniqueStockCount=len(inspection.margin_codes),
        dateCount=inspection.margin_date_count,
        dateRange=DateRange(min=inspection.margin_min, max=inspection.margin_max)
        if inspection.margin_min and inspection.margin_max
        else None,
    )

    fundamentals = FundamentalsStats(
        count=inspection.statements_count,
        uniqueStockCount=len(statement_codes),
        latestDisclosedDate=latest_disclosed_date,
        listedMarketCoverage=ListedMarketCoverage(
            listedMarketStocks=int(fundamentals_coverage.get("targetCount", 0) or 0),
            coveredStocks=int(fundamentals_coverage.get("coveredCount", 0) or 0),
            missingStocks=int(fundamentals_coverage.get("missingCount", 0) or 0),
            coverageRatio=float(fundamentals_coverage.get("coverageRatio", 0.0) or 0.0),
            issuerAliasCoveredCount=int(
                fundamentals_coverage.get("issuerAliasCoveredCount", 0) or 0
            ),
            emptySkippedCount=int(
                fundamentals_coverage.get("emptySkippedCount", 0) or 0
            ),
        ),
    )
    intraday_freshness_snapshot = build_intraday_freshness(
        latest_date=inspection.stock_minute_max,
        latest_time=inspection.latest_stock_minute_time,
        last_intraday_sync=last_intraday_sync,
    )

    return MarketStatsResponse(
        initialized=initialized,
        lastSync=last_sync,
        lastIntradaySync=last_intraday_sync,
        timeSeriesSource=inspection.source,
        databaseSize=storage.duckdbBytes,
        storage=storage,
        schema_=MarketSchemaStats(
            version=market_db.get_market_schema_version(),
            current=market_db.is_market_schema_current(),
        ),
        stockMaster=StockMasterCoverageStats(
            dailyCount=int(master_coverage.get("dailyCount", 0) or 0),
            intervalCount=int(master_coverage.get("intervalCount", 0) or 0),
            latestCount=int(master_coverage.get("latestCount", 0) or 0),
            indexMembershipDailyCount=int(master_coverage.get("indexMembershipDailyCount", 0) or 0),
            dateRange=DateRange(
                min=str(master_coverage.get("dateMin")),
                max=str(master_coverage.get("dateMax")),
            )
            if master_coverage.get("dateMin") and master_coverage.get("dateMax")
            else None,
            dateCount=int(master_coverage.get("dateCount", 0) or 0),
            codeCount=int(master_coverage.get("codeCount", 0) or 0),
            missingTopixDatesCount=int(master_coverage.get("missingTopixDatesCount", 0) or 0),
            missingTopixDates=[str(d) for d in master_coverage.get("missingTopixDates", [])],
        ),
        topix=topix,
        stocks=stocks_stats,
        stockData=stock_data,
        stockMinuteData=stock_minute_data,
        indices=indices,
        options225=options_225,
        margin=margin,
        fundamentals=fundamentals,
        adjustedMetrics=adjusted_metrics,
        intradayFreshness=IntradayFreshness(
            status=intraday_freshness_snapshot.status,
            expectedDate=intraday_freshness_snapshot.expected_date,
            latestDate=intraday_freshness_snapshot.latest_date,
            latestTime=intraday_freshness_snapshot.latest_time,
            lastIntradaySync=intraday_freshness_snapshot.last_intraday_sync,
            readyTimeJst=intraday_freshness_snapshot.ready_time_jst,
            evaluatedAtJst=intraday_freshness_snapshot.evaluated_at_jst,
            calendarBasis=intraday_freshness_snapshot.calendar_basis,
        ),
        lastUpdated=datetime.now(UTC).isoformat(),
    )


def _build_adjusted_metrics_stats(
    snapshot: dict[str, Any],
    *,
    source_stock_count: int,
    source_statement_count: int,
    latest_stock_date: str | None,
) -> AdjustedMetricsStats:
    statement_rows = int(snapshot.get("statementRows", 0) or 0)
    daily_rows = int(snapshot.get("dailyValuationRows", 0) or 0)
    daily_technical_rows = int(snapshot.get("dailyTechnicalMetricRows", 0) or 0)
    daily_valuation_latest_date = snapshot.get("dailyValuationLatestDate")
    daily_valuation_latest_code_count = int(
        snapshot.get("dailyValuationLatestCodeCount", 0) or 0
    )
    daily_valuation_previous_code_count = int(
        snapshot.get("dailyValuationPreviousCodeCount", 0) or 0
    )
    price_basis_date = snapshot.get("priceBasisDate")
    basis_version = snapshot.get("basisVersion")
    basis_version_count = int(snapshot.get("basisVersionCount", 0) or 0)
    retained_basis_count = int(snapshot.get("retainedBasisCount", 0) or 0)
    ready_basis_count = int(snapshot.get("readyBasisCount", 0) or 0)
    invalid_basis_count = int(snapshot.get("invalidBasisCount", 0) or 0)
    active_coverage_frontier = snapshot.get("activeCoverageFrontier")
    under_covered_active_basis_count = int(
        snapshot.get("underCoveredActiveBasisCount", 0) or 0
    )
    overlapping_basis_count = int(snapshot.get("overlappingBasisCount", 0) or 0)
    orphan_adjusted_statement_rows = int(
        snapshot.get("orphanAdjustedStatementRows", 0) or 0
    )
    orphan_daily_valuation_rows = int(
        snapshot.get("orphanDailyValuationRows", 0) or 0
    )
    has_raw_source = source_stock_count > 0 or source_statement_count > 0
    if invalid_basis_count > 0 or overlapping_basis_count > 0:
        status = "invalid_lineage"
    elif orphan_adjusted_statement_rows > 0 or orphan_daily_valuation_rows > 0:
        status = "orphan_rows"
    elif statement_rows <= 0 and daily_rows <= 0 and not has_raw_source:
        status = "empty_source"
    elif statement_rows <= 0 or daily_rows <= 0:
        status = "missing"
    elif (
        isinstance(daily_valuation_latest_date, str)
        and latest_stock_date is not None
        and daily_valuation_latest_date < latest_stock_date
    ):
        status = "stale"
    elif (
        daily_valuation_previous_code_count > 0
        and daily_valuation_latest_code_count
        < max(1, int(daily_valuation_previous_code_count * 0.5))
    ):
        status = "stale"
    elif under_covered_active_basis_count > 0:
        status = "incomplete_coverage"
    else:
        status = "ready"
    return AdjustedMetricsStats(
        statementRows=statement_rows,
        dailyValuationRows=daily_rows,
        dailyTechnicalMetricRows=daily_technical_rows,
        dailyValuationLatestDate=(
            str(daily_valuation_latest_date)
            if daily_valuation_latest_date is not None
            else None
        ),
        dailyValuationLatestCodeCount=daily_valuation_latest_code_count,
        dailyValuationPreviousCodeCount=daily_valuation_previous_code_count,
        priceBasisDate=str(price_basis_date) if price_basis_date is not None else None,
        basisVersion=str(basis_version) if basis_version is not None else None,
        basisVersionCount=basis_version_count,
        retainedBasisCount=retained_basis_count,
        readyBasisCount=ready_basis_count,
        invalidBasisCount=invalid_basis_count,
        activeCoverageFrontier=(
            str(active_coverage_frontier)
            if active_coverage_frontier is not None
            else None
        ),
        underCoveredActiveBasisCount=under_covered_active_basis_count,
        overlappingBasisCount=overlapping_basis_count,
        orphanAdjustedStatementRows=orphan_adjusted_statement_rows,
        orphanDailyValuationRows=orphan_daily_valuation_rows,
        status=status,
    )
