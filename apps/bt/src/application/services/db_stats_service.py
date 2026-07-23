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
from src.shared.provider_stock_window import validate_provider_plan
from src.shared.contracts import market_maintenance as maintenance_contracts
from src.infrastructure.db.market.market_maintenance_evidence import (
    read_market_maintenance_evidence,
)
from src.application.contracts.market_data_plane import (
    DateRange,
    FundamentalsStats,
    IndicesStats,
    IntradayFreshness,
    ListedMarketCoverage,
    MarginStats,
    MarketStatsResponse,
    MarketSchemaStats,
    Options225Stats,
    ProviderVintageStats,
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
    def get_status_counts(self) -> dict[str, int]: ...
    def get_market_schema_version(self) -> int | None: ...
    def is_market_schema_current(self) -> bool: ...
    def get_stock_master_coverage(self) -> dict[str, Any]: ...
    def get_stock_count_by_market(self) -> dict[str, int]: ...
    def get_fundamentals_target_stock_rows(self) -> list[dict[str, str]]: ...
    def get_index_master_category_counts(self) -> dict[str, int]: ...
    def get_adjusted_metrics_status_snapshot(self) -> dict[str, Any]: ...
    def get_adjusted_metrics_source_diagnostics(self) -> dict[str, int]: ...
    def get_provider_vintage_status_snapshot(self) -> dict[str, Any]: ...
    def get_adjustment_events_count(self) -> int: ...


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


def _resolve_maintenance_evidence(
    time_series_store: object,
) -> maintenance_contracts.MarketMaintenanceRecord:
    duckdb_path = getattr(time_series_store, "_duckdb_path", None)
    if not isinstance(duckdb_path, Path):
        return maintenance_contracts.MarketMaintenanceRecord.never_run()
    return read_market_maintenance_evidence(duckdb_path.parent)


def get_market_stats(
    market_db: MarketDbStatsLike,
    *,
    time_series_store: TimeSeriesStoreStatsLike,
) -> MarketStatsResponse:
    """DuckDB 時系列 SoT と market metadata を統合した統計情報を返す。"""
    initialized = market_db.is_initialized()
    last_sync = market_db.get_sync_metadata(METADATA_KEYS["LAST_SYNC_DATE"])
    last_intraday_sync = market_db.get_sync_metadata(
        METADATA_KEYS["LAST_INTRADAY_SYNC"]
    )
    inspection = time_series_store.inspect()

    # Metadata / reference data (DuckDB metadata tables)
    basic = market_db.get_status_counts()
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
    maintenance = _resolve_maintenance_evidence(time_series_store)
    provider_vintage = _build_provider_vintage_stats(
        {
            **market_db.get_adjusted_metrics_status_snapshot(),
            **market_db.get_provider_vintage_status_snapshot(),
            **market_db.get_adjusted_metrics_source_diagnostics(),
        },
        source_stock_count=inspection.stock_count,
        source_statement_count=inspection.statements_count,
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
        dateRange=DateRange(
            min=inspection.options_225_min, max=inspection.options_225_max
        )
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
        maintenance=maintenance,
        schema_=MarketSchemaStats(
            version=market_db.get_market_schema_version(),
            current=market_db.is_market_schema_current(),
        ),
        stockMaster=StockMasterCoverageStats(
            dailyCount=int(master_coverage.get("dailyCount", 0) or 0),
            intervalCount=int(master_coverage.get("intervalCount", 0) or 0),
            latestCount=int(master_coverage.get("latestCount", 0) or 0),
            indexMembershipDailyCount=int(
                master_coverage.get("indexMembershipDailyCount", 0) or 0
            ),
            dateRange=DateRange(
                min=str(master_coverage.get("dateMin")),
                max=str(master_coverage.get("dateMax")),
            )
            if master_coverage.get("dateMin") and master_coverage.get("dateMax")
            else None,
            dateCount=int(master_coverage.get("dateCount", 0) or 0),
            codeCount=int(master_coverage.get("codeCount", 0) or 0),
            missingTopixDatesCount=int(
                master_coverage.get("missingTopixDatesCount", 0) or 0
            ),
            missingTopixDates=[
                str(d) for d in master_coverage.get("missingTopixDates", [])
            ],
        ),
        topix=topix,
        stocks=stocks_stats,
        stockData=stock_data,
        stockMinuteData=stock_minute_data,
        indices=indices,
        options225=options_225,
        margin=margin,
        fundamentals=fundamentals,
        providerVintage=provider_vintage,
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


def _build_provider_vintage_stats(
    snapshot: dict[str, Any],
    *,
    source_stock_count: int,
    source_statement_count: int,
    provider_plan: str | None = None,
) -> ProviderVintageStats:
    del provider_plan
    current_basis_statement_count = int(
        snapshot.get("currentBasisStatementCount", 0) or 0
    )
    current_basis_state_count = int(snapshot.get("currentBasisStateCount", 0) or 0)
    invalid_current_basis_state_count = int(
        snapshot.get("invalidCurrentBasisStateCount", 0) or 0
    )
    fundamentals_adjustment_basis_date = snapshot.get(
        "fundamentalsAdjustmentBasisDate"
    )
    provider_window_count = int(snapshot.get("providerWindowCount", 0) or 0)
    ready_provider_window_count = int(
        snapshot.get("readyProviderWindowCount", 0) or 0
    )
    pending_current_basis_code_count = int(
        snapshot.get("pendingCurrentBasisCodeCount", 0) or 0
    )
    orphan_adjusted_statement_rows = int(
        snapshot.get("orphanAdjustedStatementRows", 0) or 0
    )
    provider_window_fingerprint_count = int(
        snapshot.get("providerWindowFingerprintCount", 0) or 0
    )
    invalid_provider_window_count = int(
        snapshot.get("invalidProviderWindowCount", 0) or 0
    )
    adjustment_event_fingerprint_count = int(
        snapshot.get("adjustmentEventFingerprintCount", 0) or 0
    )
    invalid_adjustment_event_count = int(
        snapshot.get("invalidAdjustmentEventCount", 0) or 0
    )
    provider_adjusted_mismatch_count = int(
        snapshot.get("providerAdjustedMismatchCount", 0) or 0
    )
    provider_as_of = snapshot.get("providerAsOf")
    provider_as_of_min = snapshot.get("providerAsOfMin")
    provider_as_of_max = snapshot.get("providerAsOfMax")
    coverage_start = snapshot.get("effectiveCoverageStart")
    coverage_end = snapshot.get("effectiveCoverageEnd")
    source_fingerprint = snapshot.get("providerSourceFingerprint")
    provider_window_coherent = bool(snapshot.get("providerWindowCoherent", False))
    adjustment_event_count = int(snapshot.get("adjustmentEventCount", 0) or 0)
    source_statement_key_count = int(snapshot.get("sourceStatementKeyCount", 0) or 0)
    expected_adjusted_statement_rows = int(
        snapshot.get("expectedAdjustedStatementRows", 0) or 0
    )
    missing_adjusted_statement_rows = int(
        snapshot.get("missingAdjustedStatementRows", 0) or 0
    )
    extra_adjusted_statement_rows = int(
        snapshot.get("extraAdjustedStatementRows", 0) or 0
    )
    stale_adjusted_statement_rows = int(
        snapshot.get("staleAdjustedStatementRows", 0) or 0
    )
    wrong_basis_adjusted_statement_rows = int(
        snapshot.get("wrongBasisAdjustedStatementRows", 0) or 0
    )
    has_raw_source = source_stock_count > 0 or source_statement_count > 0
    is_empty_source = not any(
        (
            has_raw_source,
            provider_window_count,
            adjustment_event_count,
            current_basis_state_count,
            current_basis_statement_count,
            invalid_current_basis_state_count,
            pending_current_basis_code_count,
            source_statement_key_count,
            expected_adjusted_statement_rows,
            missing_adjusted_statement_rows,
            extra_adjusted_statement_rows,
            stale_adjusted_statement_rows,
            wrong_basis_adjusted_statement_rows,
            orphan_adjusted_statement_rows,
            invalid_provider_window_count,
            invalid_adjustment_event_count,
            provider_adjusted_mismatch_count,
        )
    )
    provider_plan = snapshot.get("providerPlan")
    provider_plan_valid = is_empty_source
    if not is_empty_source:
        try:
            provider_plan = validate_provider_plan(provider_plan)
            provider_plan_valid = True
        except ValueError:
            provider_plan_valid = False
    provider_metadata_valid = bool(
        provider_window_coherent
        and provider_as_of_min
        and provider_as_of_max
        and coverage_start
        and coverage_end
        and source_fingerprint
        and coverage_start <= coverage_end
        and provider_as_of_min >= coverage_end
    )
    if is_empty_source:
        status = "empty_source"
    elif (
        invalid_current_basis_state_count > 0
        or wrong_basis_adjusted_statement_rows > 0
        or invalid_provider_window_count > 0
        or invalid_adjustment_event_count > 0
        or provider_adjusted_mismatch_count > 0
        or not provider_plan_valid
        or (provider_window_count > 0 and not provider_metadata_valid)
    ):
        status = "invalid"
    elif pending_current_basis_code_count > 0 or ready_provider_window_count < provider_window_count:
        status = "pending"
    elif missing_adjusted_statement_rows > 0:
        status = "missing"
    elif (
        stale_adjusted_statement_rows > 0
        or extra_adjusted_statement_rows > 0
        or orphan_adjusted_statement_rows > 0
    ):
        status = "stale"
    elif provider_window_count <= 0 or (source_statement_count > 0 and current_basis_statement_count <= 0):
        status = "missing"
    else:
        status = "ready"
    return ProviderVintageStats(
        providerPlan=provider_plan,
        providerAsOf=provider_as_of,
        providerAsOfRange=DateRange(min=provider_as_of_min, max=provider_as_of_max)
        if provider_as_of_min and provider_as_of_max
        else None,
        effectiveCoverage=DateRange(min=coverage_start, max=coverage_end)
        if coverage_start and coverage_end
        else None,
        sourceFingerprint=source_fingerprint,
        providerWindowCoherent=provider_window_coherent,
        providerWindowCount=provider_window_count,
        readyProviderWindowCount=ready_provider_window_count,
        providerWindowFingerprintCount=provider_window_fingerprint_count,
        invalidProviderWindowCount=invalid_provider_window_count,
        adjustmentEventCount=adjustment_event_count,
        adjustmentEventFingerprintCount=adjustment_event_fingerprint_count,
        invalidAdjustmentEventCount=invalid_adjustment_event_count,
        providerAdjustedMismatchCount=provider_adjusted_mismatch_count,
        currentBasisStatementCount=current_basis_statement_count,
        currentBasisStateCount=current_basis_state_count,
        invalidCurrentBasisStateCount=invalid_current_basis_state_count,
        fundamentalsAdjustmentBasisDate=(
            str(fundamentals_adjustment_basis_date)
            if fundamentals_adjustment_basis_date is not None
            else None
        ),
        pendingCurrentBasisCodeCount=pending_current_basis_code_count,
        orphanAdjustedStatementRows=orphan_adjusted_statement_rows,
        sourceStatementKeyCount=source_statement_key_count,
        expectedAdjustedStatementRows=expected_adjusted_statement_rows,
        missingAdjustedStatementRows=missing_adjusted_statement_rows,
        extraAdjustedStatementRows=extra_adjusted_statement_rows,
        staleAdjustedStatementRows=stale_adjusted_statement_rows,
        wrongBasisAdjustedStatementRows=wrong_basis_adjusted_statement_rows,
        status=status,
        recoveryStage="market_db_sync"
        if status in {"missing", "stale", "pending", "invalid"}
        else None,
    )
