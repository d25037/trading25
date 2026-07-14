# Fundamentals Event-Time PIT Data Plane Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Market Data Plane v4 that retains per-code corporate-action regime bases and make both Fundamentals APIs consume one fail-closed event-time PIT snapshot.

**Architecture:** The write side materializes one origin basis plus one basis per valid split/reverse-split frontier, retains every ready regime, and publishes catalog, projection segments, adjusted statements, and valuation atomically. Dataset bundles copy the same basis graph. The read side resolves one containing basis and exact stock-master date inside a DuckDB read snapshot; Fundamentals never recomputes adjustments, writes during a request, or falls back to current tables.

**Tech Stack:** Python 3.12, DuckDB, pandas, Pydantic v2, FastAPI, pytest, Ruff, Pyright, TypeScript, Bun, OpenAPI-generated contracts.

## Global Constraints

- Follow the approved design at `docs/superpowers/specs/2026-07-14-fundamentals-event-time-pit-data-plane-design.md`.
- `statement_metrics_adjusted` and `daily_valuation` remain the consumer-facing adjusted fundamentals SoTs.
- Materialization runs only in initial/incremental sync, stock refresh, or the explicit adjusted-metrics job; analytics requests are read-only.
- Basis identity is `event-pit-v1:{normalized_code}:{valid_from}` and `basis_version` must equal that `basis_id`.
- Retain every source-derived basis. Delete only orphaned lineage after source correction/deletion or during full reset.
- Invalid/non-finite/zero/negative adjustment factors invalidate lineage; never coerce them to `1` in the event-time path.
- Market schema v3 and adjustment mode `local_projection_v1` are incompatible. Support only Market schema v4 with `local_projection_v2_event_time` after reset.
- Do not add ALTER backfills, dual reads, migration shims, deprecated aliases, request-time writes, current/latest fallback, dataset fallback, or frontend financial calculations.
- `to` is the knowledge/event cutoff. `from` is a response display lower bound and must not change basis or latest selection.
- A pre-cutoff forecast remains eligible even when its fiscal period ends after the cutoff.
- GET and POST Fundamentals endpoints must share validation, empty-result behavior, error mapping, and payload semantics.
- Preserve `manifest.v2.json` as the physical dataset filename required by the repository path contract, but require payload `schemaVersion: 3`; schemaVersion 2 is unsupported.
- Ingestion-vintage/bitemporal reconstruction is out of scope; corrected source facts replace the affected event-time lineage.

---

### Task 1: Establish the hard Market Data Plane v4 boundary

**Files:**
- Create: `contracts/market-db-schema-v3.json`
- Modify: `contracts/README.md`
- Modify: `apps/bt/src/infrastructure/db/market/market_schema.py`
- Modify: `apps/bt/src/infrastructure/db/market/market_db.py`
- Modify: `apps/bt/src/infrastructure/db/market/tables.py`
- Modify: `apps/bt/src/application/services/sync_service.py`
- Modify: `apps/bt/src/application/services/db_validation_service.py`
- Test: `apps/bt/tests/unit/server/db/test_market_db.py`
- Test: `apps/bt/tests/unit/server/db/test_tables.py`
- Test: `apps/bt/tests/unit/server/services/test_sync_service.py`
- Test: `apps/bt/tests/unit/server/services/test_db_validation_service.py`

**Interfaces:**
- Produces: `MARKET_SCHEMA_VERSION = 4`.
- Produces: `LOCAL_STOCK_PRICE_ADJUSTMENT_MODE = "local_projection_v2_event_time"`.
- Produces: physical tables `stock_adjustment_bases` and `stock_adjustment_basis_segments`.
- Produces: breaking stable contract `market-db-schema-v3.json`; physical schema number and contract major number are intentionally distinct.

- [ ] **Step 1: Add failing v4 compatibility tests**

```python
def test_existing_v3_is_not_partially_upgraded(tmp_path: Path) -> None:
    db = _open_versioned_market_db(tmp_path, version=3, mode="local_projection_v1")
    db.ensure_schema()
    assert db.get_market_schema_version() == 3
    assert not db._table_exists("stock_adjustment_bases")
    assert not db._table_exists("stock_adjustment_basis_segments")


def test_fresh_market_db_uses_event_time_mode(tmp_path: Path) -> None:
    db = MarketDb(str(tmp_path / "market.duckdb"))
    assert db.get_market_schema_version() == 4
    assert db.get_stock_price_adjustment_mode() == "local_projection_v2_event_time"
```

Extend validation/sync assertions so v3 recommends only initial sync with `resetBeforeSync=true` and a reset-created v4 DB is accepted.

- [ ] **Step 2: Run the tests and confirm the red state**

Run:

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/db/test_market_db.py \
  tests/unit/server/db/test_tables.py \
  tests/unit/server/services/test_sync_service.py \
  tests/unit/server/services/test_db_validation_service.py -q
```

Expected: failures report schema version `3`, mode `local_projection_v1`, and missing basis tables/contract definitions.

- [ ] **Step 3: Define the fresh-only v4 schema**

Add these constants and DDL shapes in `market_schema.py`:

```python
MARKET_SCHEMA_VERSION = 4
LOCAL_STOCK_PRICE_ADJUSTMENT_MODE = "local_projection_v2_event_time"
STOCK_ADJUSTMENT_BASIS_STATUSES = ("building", "ready", "invalid")
```

```sql
CREATE TABLE stock_adjustment_bases (
  code TEXT,
  basis_id TEXT,
  valid_from TEXT NOT NULL,
  valid_to_exclusive TEXT,
  adjustment_through_date TEXT NOT NULL,
  source_fingerprint TEXT NOT NULL,
  materialized_through_date TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('building', 'ready', 'invalid')),
  created_at TEXT,
  updated_at TEXT,
  PRIMARY KEY (code, basis_id),
  UNIQUE (code, valid_from)
)
```

```sql
CREATE TABLE stock_adjustment_basis_segments (
  code TEXT,
  basis_id TEXT,
  source_date_from TEXT,
  source_date_to_exclusive TEXT,
  cumulative_factor DOUBLE NOT NULL,
  PRIMARY KEY (code, basis_id, source_date_from)
)
```

Preflight `market_schema_version` before executing any v4 DDL. If an existing version is not `4`, `ensure_market_schema()` and then `MarketDb.ensure_schema()` return before table creation, ALTER helpers, index-membership reconstruction, or metadata writes.

- [ ] **Step 4: Publish the breaking JSON contract**

Create `contracts/market-db-schema-v3.json` with `schema_version: 3.0.0`, the existing required tables, both new basis tables, and the existing multi-basis primary keys for adjusted statements and valuation. Update `contracts/README.md` to mark v3 as current and v2 as superseded rather than runtime-compatible.

- [ ] **Step 5: Run focused verification**

Run the Step 2 pytest command, then:

```bash
uv run --directory apps/bt ruff check \
  src/infrastructure/db/market/market_schema.py \
  src/infrastructure/db/market/market_db.py \
  src/infrastructure/db/market/tables.py \
  src/application/services/sync_service.py \
  src/application/services/db_validation_service.py
uv run --directory apps/bt pyright \
  src/infrastructure/db/market/market_schema.py \
  src/infrastructure/db/market/market_db.py \
  src/application/services/sync_service.py \
  src/application/services/db_validation_service.py
```

Expected: all commands pass; opening v3 performs no writes.

- [ ] **Step 6: Commit Task 1**

```bash
git add contracts/market-db-schema-v3.json contracts/README.md \
  apps/bt/src/infrastructure/db/market \
  apps/bt/src/application/services/sync_service.py \
  apps/bt/src/application/services/db_validation_service.py \
  apps/bt/tests/unit/server/db/test_market_db.py \
  apps/bt/tests/unit/server/db/test_tables.py \
  apps/bt/tests/unit/server/services/test_sync_service.py \
  apps/bt/tests/unit/server/services/test_db_validation_service.py
git commit -m "feat(bt): require market data plane v4"
```

### Task 2: Build corporate-action regime lineage and its repository

**Files:**
- Create: `apps/bt/src/domains/fundamentals/adjustment_basis.py`
- Create: `apps/bt/src/infrastructure/db/market/adjustment_basis_queries.py`
- Create: `apps/bt/src/infrastructure/db/market/adjustment_basis_writers.py`
- Modify: `apps/bt/src/infrastructure/db/market/market_db.py`
- Test: `apps/bt/tests/unit/domains/fundamentals/test_adjustment_basis.py`
- Test: `apps/bt/tests/unit/server/db/test_adjustment_basis_repository.py`

**Interfaces:**
- Produces: `RawAdjustmentPoint`, `StockAdjustmentBasis`, `StockAdjustmentBasisSegment`, and `StockAdjustmentLineage` frozen dataclasses.
- Produces: `build_stock_adjustment_lineage(code, rows) -> StockAdjustmentLineage`.
- Produces: basis repository methods on `MarketDb`.

- [ ] **Step 1: Write failing pure lineage tests**

```python
def test_build_lineage_creates_origin_and_split_regimes() -> None:
    rows = [
        RawAdjustmentPoint("72030", "2024-01-04", 1.0),
        RawAdjustmentPoint("7203", "2024-06-28", 0.5),
        RawAdjustmentPoint("7203", "2025-03-31", 2.0),
    ]
    lineage = build_stock_adjustment_lineage("72030", rows)
    assert [b.valid_from for b in lineage.bases] == [
        "2024-01-04", "2024-06-28", "2025-03-31"
    ]
    assert lineage.bases[0].valid_to_exclusive == "2024-06-28"
    assert lineage.bases[1].basis_id == "event-pit-v1:7203:2024-06-28"
    assert lineage.bases[-1].valid_to_exclusive is None


def test_invalid_factor_invalidates_forward_lineage() -> None:
    lineage = build_stock_adjustment_lineage(
        "7203", [RawAdjustmentPoint("7203", "2024-06-28", 0.0)]
    )
    assert lineage.bases[-1].status == "invalid"
```

Also assert cumulative segment factors use only events in `(source_date, adjustment_through_date]`.

- [ ] **Step 2: Run pure tests and confirm missing interfaces**

Run:

```bash
uv run --directory apps/bt pytest \
  tests/unit/domains/fundamentals/test_adjustment_basis.py -q
```

Expected: import failure for `src.domains.fundamentals.adjustment_basis`.

- [ ] **Step 3: Implement the pure lineage model**

Use these exact signatures:

```text
@dataclass(frozen=True)
class RawAdjustmentPoint:
    code: str
    date: str
    adjustment_factor: float | None


@dataclass(frozen=True)
class StockAdjustmentBasis:
    code: str
    basis_id: str
    valid_from: str
    valid_to_exclusive: str | None
    adjustment_through_date: str
    source_fingerprint: str
    materialized_through_date: str
    status: Literal["building", "ready", "invalid"]


build_stock_adjustment_lineage(
    code: str,
    rows: Sequence[RawAdjustmentPoint],
) -> StockAdjustmentLineage
```

Normalize aliases before grouping, sort by ISO date, reject overlapping source dates with conflicting factors, and calculate a deterministic SHA-256 source fingerprint from normalized `(date, factor)` pairs.

- [ ] **Step 4: Add failing repository and projection tests**

```python
def test_ready_basis_resolution_requires_interval_and_coverage(market_db: MarketDb) -> None:
    _publish_two_regimes(market_db)
    assert market_db.get_ready_adjustment_basis("72030", "2024-06-27")["valid_from"] == "2024-01-04"
    assert market_db.get_ready_adjustment_basis("7203", "2024-06-28")["valid_from"] == "2024-06-28"
    assert market_db.get_ready_adjustment_basis("7203", "2026-01-01") is None


def test_basis_projection_never_reads_current_stock_data(market_db: MarketDb) -> None:
    _seed_raw_prices_and_segments(market_db)
    rows = market_db.get_basis_adjusted_stock_data(
        "7203", "event-pit-v1:7203:2024-06-28", end="2024-06-28"
    )
    assert rows[0]["close"] == 500.0
    assert rows[0]["volume"] == 200
```

- [ ] **Step 5: Implement repository methods and atomic lineage publish**

Add these methods:

```text
MarketDb.load_raw_adjustment_points(codes: list[str] | None = None)
    -> list[dict[str, Any]]
MarketDb.get_ready_adjustment_basis(code: str, effective_market_date: str)
    -> dict[str, Any] | None
MarketDb.get_adjustment_basis_segments(code: str, basis_id: str)
    -> list[dict[str, Any]]
MarketDb.get_basis_adjusted_stock_data(
    code: str, basis_id: str, *, start: str | None = None, end: str | None = None
) -> list[dict[str, Any]]
MarketDb.publish_stock_adjustment_lineages(
    lineages: Sequence[StockAdjustmentLineage],
    *, remove_basis_ids: Mapping[str, Sequence[str]]
) -> None
```

`publish_stock_adjustment_lineages` registers staging relations, validates non-overlapping intervals/segments, opens one DuckDB transaction, removes only the supplied orphan IDs, upserts catalog and segments, and rolls back on any exception.

- [ ] **Step 6: Verify and commit Task 2**

Run:

```bash
uv run --directory apps/bt pytest \
  tests/unit/domains/fundamentals/test_adjustment_basis.py \
  tests/unit/server/db/test_adjustment_basis_repository.py -q
uv run --directory apps/bt ruff check \
  src/domains/fundamentals/adjustment_basis.py \
  src/infrastructure/db/market/adjustment_basis_queries.py \
  src/infrastructure/db/market/adjustment_basis_writers.py \
  src/infrastructure/db/market/market_db.py
uv run --directory apps/bt pyright \
  src/domains/fundamentals/adjustment_basis.py \
  src/infrastructure/db/market/adjustment_basis_queries.py \
  src/infrastructure/db/market/adjustment_basis_writers.py
git add apps/bt/src/domains/fundamentals/adjustment_basis.py \
  apps/bt/src/infrastructure/db/market \
  apps/bt/tests/unit/domains/fundamentals/test_adjustment_basis.py \
  apps/bt/tests/unit/server/db/test_adjustment_basis_repository.py
git commit -m "feat(bt): add event-time adjustment basis repository"
```

### Task 3: Reconcile and atomically publish all adjusted metric regimes

**Files:**
- Modify: `apps/bt/src/application/services/adjusted_metrics_materializer.py`
- Modify: `apps/bt/src/infrastructure/db/market/valuation_writers.py`
- Modify: `apps/bt/src/infrastructure/db/market/valuation_queries.py`
- Modify: `apps/bt/src/infrastructure/db/market/market_db.py`
- Test: `apps/bt/tests/unit/server/services/test_adjusted_metrics_materializer.py`
- Create: `apps/bt/tests/unit/server/db/test_adjusted_metrics_atomic_publish.py`

**Interfaces:**
- Consumes: Task 2 lineage/repository.
- Produces: `AdjustedMetricsMaterializer.reconcile(codes=None)` while preserving `rebuild_all()` and `rebuild_codes()` entrypoints.
- Produces: explicit-basis adjusted metrics and valuation queries.

- [ ] **Step 1: Replace prune expectations with failing retention/reconciliation tests**

```python
def test_rebuild_retains_closed_basis_and_appends_active_basis(market_db: MarketDb) -> None:
    materializer = AdjustedMetricsMaterializer(market_db)
    materializer.rebuild_all()
    closed_before = _basis_rows(market_db, "7203", status="ready")[0]
    _append_price_without_adjustment(market_db, "7203", "2025-01-06")
    materializer.rebuild_all()
    assert _basis_row(market_db, closed_before["basis_id"])["updated_at"] == closed_before["updated_at"]
    assert len(_basis_rows(market_db, "7203", status="ready")) == 2


def test_publish_failure_keeps_previous_ready_snapshot(market_db: MarketDb, monkeypatch) -> None:
    before = _ready_snapshot(market_db, "7203")
    monkeypatch.setattr(market_db, "_commit_basis_publish", _raise_injected)
    with pytest.raises(RuntimeError, match="injected"):
        AdjustedMetricsMaterializer(market_db).rebuild_codes(["7203"])
    assert _ready_snapshot(market_db, "7203") == before
```

Add event-add/correction/deletion, statement correction, raw price correction, invalid factor, and exact-basis query cases. Delete the old test that requires prior basis pruning.

- [ ] **Step 2: Run the materializer tests and confirm old behavior fails**

Run:

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_adjusted_metrics_materializer.py \
  tests/unit/server/db/test_adjusted_metrics_atomic_publish.py -q
```

Expected: failures show old `adjusted-v1:*` selection, old-basis deletion, and current `stock_data` reads.

- [ ] **Step 3: Implement aggregate reconciliation**

Keep caller compatibility only at the method-name level:

```python
@dataclass(frozen=True)
class AdjustedMetricsBuildResult:
    basis_count: int
    ready_basis_count: int
    statement_rows: int
    daily_valuation_rows: int
    daily_technical_metric_rows: int
    daily_valuation_latest_date: str | None
    active_price_basis_date: str | None
    active_basis_version: str | None


def rebuild_all(self) -> AdjustedMetricsBuildResult:
    return self.reconcile(codes=None)


def rebuild_codes(self, codes: list[str]) -> AdjustedMetricsBuildResult:
    return self.reconcile(codes=normalize_codes(codes))
```

`reconcile` compares stored fingerprints with rebuilt lineages. Append only the active basis for ordinary price additions. Rebuild from the first changed event forward for event changes. Fan statement and raw-price corrections into every observable basis. Closed bases end at the market day before `valid_to_exclusive`.

- [ ] **Step 4: Make valuation materialization basis-explicit**

Add explicit-basis SQL beside the existing implicit readers so Task 3 remains
buildable until Task 5 migrates every consumer:

```text
get_adjusted_statement_metrics_for_basis(
    table_exists: TableExists, fetchall_dicts: FetchAllDicts, code: str,
    *, basis_id: str, as_of_date: str | None = None
) -> list[dict[str, Any]]
get_daily_valuation_for_basis(
    table_exists: TableExists, fetchall_dicts: FetchAllDicts, code: str,
    *, basis_id: str, start: str | None = None, end: str | None = None
) -> list[dict[str, Any]]
```

Rewrite daily valuation insertion to join `stock_data_raw` with the chosen basis segments. Require every inserted adjusted/valuation row to carry the exact basis ID and valid nested disclosure dates.

- [ ] **Step 5: Remove pruning and publish atomically**

Delete `prune_adjusted_metric_basis_versions` from `MarketDb`, `valuation_writers.py`, materializer calls, diagnostics, and tests. Add:

```text
MarketDb.publish_adjusted_basis_materialization(
    plan: AdjustedBasisMaterializationPlan
) -> AdjustedBasisPublishResult
```

The plan contains catalog, segments, adjusted statements, valuation rows, and explicit orphan IDs. One transaction publishes all rows and flips catalog status to `ready` after coverage/provenance validation.

- [ ] **Step 6: Verify and commit Task 3**

Run the Step 2 tests plus:

```bash
uv run --directory apps/bt ruff check \
  src/application/services/adjusted_metrics_materializer.py \
  src/infrastructure/db/market/valuation_writers.py \
  src/infrastructure/db/market/valuation_queries.py \
  src/infrastructure/db/market/market_db.py
uv run --directory apps/bt pyright \
  src/application/services/adjusted_metrics_materializer.py \
  src/infrastructure/db/market/valuation_writers.py \
  src/infrastructure/db/market/valuation_queries.py
git add apps/bt/src/application/services/adjusted_metrics_materializer.py \
  apps/bt/src/infrastructure/db/market \
  apps/bt/tests/unit/server/services/test_adjusted_metrics_materializer.py \
  apps/bt/tests/unit/server/db/test_adjusted_metrics_atomic_publish.py
git commit -m "feat(bt): reconcile event-time adjusted metric bases"
```

### Task 4: Integrate the explicit `adjusted_metrics_pit` write stage

**Files:**
- Modify: `apps/bt/src/application/services/sync_strategies.py`
- Modify: `apps/bt/src/application/services/sync_service.py`
- Modify: `apps/bt/src/application/services/stock_refresh_service.py`
- Modify: `apps/bt/src/entrypoints/http/routes/db.py`
- Modify: `apps/bt/src/entrypoints/http/schemas/db.py`
- Test: `apps/bt/tests/unit/server/services/test_sync_strategies.py`
- Test: `apps/bt/tests/unit/server/services/test_sync_service.py`
- Test: `apps/bt/tests/unit/server/services/test_stock_refresh_service.py`
- Test: `apps/bt/tests/unit/server/test_routes_db_sync.py`

**Interfaces:**
- Consumes: Task 3 materializer.
- Produces: required `adjusted_metrics_pit` stage for initial/incremental sync and successful stock refresh codes.
- Preserves: explicit `/api/db/adjusted-metrics/materialize` job as the recovery path.

- [ ] **Step 1: Write failing workflow-order tests**

```python
@pytest.mark.parametrize("mode", ["initial", "incremental"])
async def test_sync_materializes_pit_after_publish_and_before_complete(mode: str) -> None:
    events = await _run_sync_with_event_spies(mode)
    assert events.index("stock_indexed") < events.index("adjusted_metrics_pit")
    assert events.index("adjusted_metrics_pit") < events.index("sync_complete")


async def test_repair_does_not_refresh_adjustment_basis() -> None:
    events = await _run_sync_with_event_spies("repair")
    assert "adjusted_metrics_pit" not in events
```

Add assertions that a materializer exception fails sync/refresh and that refresh passes only successfully normalized codes to `rebuild_codes`.

- [ ] **Step 2: Run focused workflow tests**

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_sync_strategies.py \
  tests/unit/server/services/test_sync_service.py \
  tests/unit/server/services/test_stock_refresh_service.py \
  tests/unit/server/test_routes_db_sync.py -q
```

Expected: the current workflows either omit materialization or expose the old `materialize` stage/result fields.

- [ ] **Step 3: Wire the stage and update its result contract**

Initial/incremental sync invokes `AdjustedMetricsMaterializer.rebuild_all()` after raw stock/statements publish/index. Stock refresh invokes `rebuild_codes(successful_normalized_codes)`. Do not catch and downgrade failures.

Change the explicit job progress stage to `adjusted_metrics_pit` and return aggregate fields:

```python
class AdjustedMetricsMaterializeResult(BaseModel):
    success: bool
    basisCount: int
    readyBasisCount: int
    statementRows: int
    dailyValuationRows: int
    dailyTechnicalMetricRows: int
    dailyValuationLatestDate: str | None
    activePriceBasisDate: str | None
    activeBasisVersion: str | None
```

Remove the old ambiguous `priceBasisDate`/`basisVersion` compatibility fields from this job response.

- [ ] **Step 4: Verify and commit Task 4**

Run the Step 2 tests, Ruff/Pyright for the changed service/route/schema files, then:

```bash
git add apps/bt/src/application/services/sync_strategies.py \
  apps/bt/src/application/services/sync_service.py \
  apps/bt/src/application/services/stock_refresh_service.py \
  apps/bt/src/entrypoints/http/routes/db.py \
  apps/bt/src/entrypoints/http/schemas/db.py \
  apps/bt/tests/unit/server/services/test_sync_strategies.py \
  apps/bt/tests/unit/server/services/test_sync_service.py \
  apps/bt/tests/unit/server/services/test_stock_refresh_service.py \
  apps/bt/tests/unit/server/test_routes_db_sync.py
git commit -m "feat(bt): integrate PIT materialization workflows"
```

### Task 5: Migrate existing analytics readers to explicit target-date bases

**Files:**
- Modify: `apps/bt/src/infrastructure/data_access/clients.py`
- Modify: `apps/bt/src/application/services/ranking_fundamental_queries.py`
- Modify: `apps/bt/src/application/services/ranking_service.py`
- Modify: `apps/bt/src/application/services/ranking_valuation.py`
- Modify: `apps/bt/src/application/services/ranking_value_composite_metrics.py`
- Modify: `apps/bt/src/application/services/screening_statement_loader.py`
- Modify: `apps/bt/src/application/services/db_stats_service.py`
- Modify: `apps/bt/src/application/services/db_validation_service.py`
- Test: `apps/bt/tests/unit/server/services/test_ranking_service.py`
- Test: `apps/bt/tests/unit/server/services/test_screening_market_loader.py`
- Test: `apps/bt/tests/unit/server/services/test_db_validation_service.py`
- Test: `apps/bt/tests/unit/server/db/test_market_adjusted_metrics.py`

**Interfaces:**
- Consumes: Task 2 basis resolver and Task 3 exact-basis valuation queries.
- Produces: target-date basis selection for every existing ranking/screening analytics reader.
- Produces: separate retained/ready/active basis diagnostics rather than ambiguous total-row latest selection.

- [ ] **Step 1: Add failing multi-basis contamination tests**

```python
def test_ranking_target_date_selects_the_containing_basis(reader: MarketDbReader) -> None:
    _seed_two_basis_valuations(reader)
    frame = load_adjusted_daily_valuation_frame(
        reader,
        codes=["7203"],
        target_date="2024-06-27",
    )
    assert set(frame["basis_version"]) == {"event-pit-v1:7203:2024-01-04"}


def test_screening_never_uses_post_reference_basis(reader: MarketDbReader) -> None:
    _seed_two_basis_statement_metrics(reader)
    rows = load_screening_statement_metrics(
        reader,
        codes=["7203"],
        reference_date="2024-06-27",
    )
    assert {row["basis_version"] for row in rows} == {
        "event-pit-v1:7203:2024-01-04"
    }
```

Add fail-closed assertions for missing, building, and under-covered bases; duplicate rows from two bases must never be silently deduplicated.

- [ ] **Step 2: Run the affected analytics tests**

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_ranking_service.py \
  tests/unit/server/services/test_screening_market_loader.py \
  tests/unit/server/services/test_db_validation_service.py \
  tests/unit/server/db/test_market_adjusted_metrics.py -q
```

Expected: current readers choose the newest `price_basis_date` globally or return duplicate multi-basis rows.

- [ ] **Step 3: Require target dates at every PIT reader boundary**

Add a shared batch resolver:

```text
resolve_ready_adjustment_bases(
    reader: MarketDbReader,
    codes: Sequence[str],
    effective_market_date: str,
) -> dict[str, StockAdjustmentBasis]
```

Ranking uses its resolved ranking date. Screening uses its strict reference date. Each SQL query joins `(code, basis_version)` to the resolved basis map before any latest-row/window selection. Missing or incomplete basis raises the existing analytics data-unavailable path; there is no current-basis fallback.

After the last consumer uses `get_adjusted_statement_metrics_for_basis` and
`get_daily_valuation_for_basis`, delete the old implicit reader methods and
their latest-`price_basis_date` SQL in this same task. No compatibility reader
survives the Task 5 commit.

- [ ] **Step 4: Separate DB observability by basis state**

Return/report retained basis count, ready basis count, invalid basis count, active coverage frontier, and orphan adjusted/valuation row counts. Delete the diagnostic recommending old-basis pruning. Validation warns with recovery `adjusted_metrics_pit` when active coverage is incomplete and errors on invalid/overlapping lineage.

- [ ] **Step 5: Verify and commit Task 6**

Run Step 2, Ruff/Pyright for all changed readers/services, then:

```bash
git add apps/bt/src/infrastructure/data_access/clients.py \
  apps/bt/src/application/services/ranking_fundamental_queries.py \
  apps/bt/src/application/services/ranking_service.py \
  apps/bt/src/application/services/ranking_valuation.py \
  apps/bt/src/application/services/ranking_value_composite_metrics.py \
  apps/bt/src/application/services/screening_statement_loader.py \
  apps/bt/src/application/services/db_stats_service.py \
  apps/bt/src/application/services/db_validation_service.py \
  apps/bt/tests/unit/server/services/test_ranking_service.py \
  apps/bt/tests/unit/server/services/test_screening_market_loader.py \
  apps/bt/tests/unit/server/services/test_db_validation_service.py \
  apps/bt/tests/unit/server/db/test_market_adjusted_metrics.py
git commit -m "fix(bt): resolve analytics against event-time bases"
```

### Task 6: Persist the event-time basis graph in Dataset snapshots

**Files:**
- Create: `contracts/dataset-db-schema-v3.json`
- Modify: `contracts/README.md`
- Modify: `apps/bt/src/infrastructure/db/dataset_io/dataset_writer.py`
- Replace: `apps/bt/tests/unit/server/db/test_dataset_adjusted_metrics_snapshot.py`
- Create: `apps/bt/tests/unit/server/db/test_dataset_event_time_basis_snapshot.py`
- Modify: `apps/bt/tests/unit/server/test_dataset_writer.py`
- Modify: `apps/bt/tests/unit/server/db/test_tables.py`

**Interfaces:**
- Consumes: Market v4 basis catalog, segments, raw prices, daily master, adjusted metrics, and valuation.
- Produces: `copy_event_time_pit_from_source` returning `EventTimePitCopyResult`.

- [ ] **Step 1: Write failing atomic-copy tests**

```python
def test_copy_event_time_pit_retains_origin_and_split_bases(tmp_path: Path) -> None:
    source = _build_v4_market_with_two_regimes(tmp_path)
    writer = DatasetWriter(str(tmp_path / "snapshot"))
    result = writer.copy_event_time_pit_from_source(
        source_duckdb_path=str(source),
        normalized_codes=["7203"],
        date_from="2024-01-01",
        date_to="2024-12-31",
    )
    assert result.basis_rows == 2
    assert result.segment_rows > 0
    assert set(_basis_versions(writer.duckdb_path)) == {
        "event-pit-v1:7203:2024-01-04",
        "event-pit-v1:7203:2024-06-28",
    }


@pytest.mark.parametrize("fault", ["market_v3", "missing_segments", "building_basis"])
def test_copy_preflight_fails_before_partial_insert(tmp_path: Path, fault: str) -> None:
    writer = _writer_with_existing_sentinel(tmp_path)
    with pytest.raises(DatasetSnapshotError):
        writer.copy_event_time_pit_from_source(**_faulty_source_args(tmp_path, fault))
    assert _destination_tables(writer.duckdb_path) == {"sentinel"}
```

- [ ] **Step 2: Run dataset writer tests and confirm missing v4 copy support**

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/db/test_dataset_event_time_basis_snapshot.py \
  tests/unit/server/test_dataset_writer.py \
  tests/unit/server/db/test_tables.py -q
```

Expected: missing copy method/tables and permissive missing-table behavior fail the new assertions.

- [ ] **Step 3: Add required dataset tables and copy contract**

Create destination tables/parquet exports for:

```text
stock_data_raw
stock_master_daily
stock_adjustment_bases
stock_adjustment_basis_segments
statement_metrics_adjusted
daily_valuation
```

Define:

```text
@dataclass(frozen=True)
class EventTimePitCopyResult:
    raw_price_rows: int
    stock_master_rows: int
    basis_rows: int
    segment_rows: int
    statement_metric_rows: int
    daily_valuation_rows: int


copy_event_time_pit_from_source(
    *,
    source_duckdb_path: str,
    normalized_codes: list[str],
    date_from: str | None,
    date_to: str | None,
) -> EventTimePitCopyResult
```

Preflight source schema `4`, mode `local_projection_v2_event_time`, all required tables, ready/intersecting bases, interval integrity, and materialized coverage. Copy aliases only after normalization and publish all tables in one transaction. Remove `copy_adjusted_metrics_from_source` and every missing-table-return-zero branch.

- [ ] **Step 4: Add the breaking dataset DB contract**

Create `contracts/dataset-db-schema-v3.json` with the six required PIT tables and exact primary keys/FK relationships. Update `contracts/README.md`. Do not edit an older contract in place.

- [ ] **Step 5: Verify and commit Task 5**

Run the Step 2 tests plus Ruff/Pyright for `dataset_writer.py`, then:

```bash
git add contracts/dataset-db-schema-v3.json contracts/README.md \
  apps/bt/src/infrastructure/db/dataset_io/dataset_writer.py \
  apps/bt/tests/unit/server/db/test_dataset_event_time_basis_snapshot.py \
  apps/bt/tests/unit/server/test_dataset_writer.py \
  apps/bt/tests/unit/server/db/test_tables.py
git rm apps/bt/tests/unit/server/db/test_dataset_adjusted_metrics_snapshot.py
git commit -m "feat(dataset): persist event-time PIT basis data"
```

### Task 7: Require schemaVersion 3 manifests and reject pre-v4 bundles

**Files:**
- Create: `contracts/dataset-snapshot-manifest-v3.schema.json`
- Modify: `contracts/README.md`
- Modify: `apps/bt/src/infrastructure/db/market/dataset_snapshot_reader.py`
- Modify: `apps/bt/src/application/services/dataset_builder_service.py`
- Modify: `apps/bt/src/application/services/dataset_resolver.py`
- Test: `apps/bt/tests/unit/server/test_dataset_snapshot_reader.py`
- Test: `apps/bt/tests/unit/server/test_dataset_resolver.py`
- Test: `apps/bt/tests/unit/server/test_dataset_builder_service_branches.py`
- Test fixtures: `apps/bt/tests/unit/server/test_dataset_service.py`
- Test fixtures: `apps/bt/tests/unit/server/test_routes_dataset.py`
- Test fixtures: `apps/bt/tests/unit/server/test_routes_dataset_data.py`
- Test fixtures: `apps/bt/tests/unit/server/test_routes_dataset_jobs.py`

**Interfaces:**
- Produces: physical `manifest.v2.json` with required payload `schemaVersion: 3`.
- Produces: strict v3 manifest validation and cheap resolver compatibility preflight.

- [ ] **Step 1: Add failing strict-manifest tests**

```python
def test_schema_version_two_manifest_is_unsupported(snapshot_dir: Path) -> None:
    _write_manifest(snapshot_dir, schema_version=2)
    with pytest.raises(UnsupportedDatasetSnapshotError):
        resolve_dataset_snapshot(snapshot_dir)


def test_v3_manifest_requires_market_v4_lineage(snapshot_dir: Path) -> None:
    _write_manifest(
        snapshot_dir,
        schema_version=3,
        source={"marketSchemaVersion": 4},
    )
    with pytest.raises(DatasetManifestValidationError, match="stockPriceAdjustmentMode"):
        resolve_dataset_snapshot(snapshot_dir)
```

Add missing catalog/segment/count, checksum mismatch, overlapping interval, dangling basis FK, and insufficient coverage cases.

- [ ] **Step 2: Run reader/resolver tests and confirm v2 acceptance fails expectations**

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/test_dataset_snapshot_reader.py \
  tests/unit/server/test_dataset_resolver.py \
  tests/unit/server/test_dataset_builder_service_branches.py -q
```

Expected: current `Literal[2]`, legacy zero-count allowances, and v2 resolver paths fail.

- [ ] **Step 3: Implement manifest v3 while preserving the physical filename**

```python
class DatasetManifestV3(BaseModel):
    schemaVersion: Literal[3] = 3
    source: DatasetSourceV3
    dataset: DatasetDescriptorV3
    logicalCounts: DatasetLogicalCountsV3


class DatasetSourceV3(BaseModel):
    backend: Literal["duckdb-parquet"]
    marketSchemaVersion: Literal[4]
    stockPriceAdjustmentMode: Literal["local_projection_v2_event_time"]
```

Require counts for all six PIT tables. Remove `_LEGACY_ZERO_ONLY_LOGICAL_COUNT_FIELDS`, schemaVersion 2 parsing, missing-count defaults, and legacy checksum acceptance. `inspect_dataset_snapshot_duckdb` validates ready bases, unique/non-overlapping intervals, basis FKs, and coverage.

- [ ] **Step 4: Make resolver discovery strict**

`DatasetResolver.exists/list` reads `manifest.v2.json` and cheaply rejects any payload other than schemaVersion 3 + Market v4/event-time mode. Full resolution performs file checksum, logical checksum, and DuckDB integrity validation. Unsupported bundles are not advertised.

- [ ] **Step 5: Update every fixture and commit Task 7**

Replace fixture payloads with schemaVersion 3 and required source/count fields, then run Step 2 plus route fixture suites. Commit:

```bash
git add contracts/dataset-snapshot-manifest-v3.schema.json contracts/README.md \
  apps/bt/src/infrastructure/db/market/dataset_snapshot_reader.py \
  apps/bt/src/application/services/dataset_builder_service.py \
  apps/bt/src/application/services/dataset_resolver.py \
  apps/bt/tests/unit/server
git commit -m "feat(dataset): require v4 PIT snapshot bundles"
```

### Task 8: Add basis-aware Dataset snapshot reads

**Files:**
- Modify: `apps/bt/src/infrastructure/db/market/dataset_snapshot_reader.py`
- Test: `apps/bt/tests/unit/server/test_dataset_snapshot_reader.py`
- Test: `apps/bt/tests/unit/server/db/test_dataset_event_time_basis_snapshot.py`

**Interfaces:**
- Consumes: Tasks 2 and 6 basis semantics.
- Produces: basis resolution, raw-price projection, and exact-basis metrics for archived snapshots.

- [ ] **Step 1: Write failing before/after-split reader tests**

```python
def test_reader_selects_containing_basis_and_never_mixes_versions(snapshot_dir: Path) -> None:
    reader = DatasetSnapshotReader(snapshot_dir)
    origin = reader.resolve_adjustment_basis("7203", "2024-06-27")
    post_split = reader.resolve_adjustment_basis("7203", "2024-06-28")
    assert origin.basis_id != post_split.basis_id
    assert {
        row["basis_version"]
        for row in reader.get_daily_valuation("7203", basis_id=origin.basis_id)
    } == {origin.basis_id}
```

Add missing, multiple, building, under-covered, and projected OHLCV cases.

- [ ] **Step 2: Run focused reader tests**

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/test_dataset_snapshot_reader.py \
  tests/unit/server/db/test_dataset_event_time_basis_snapshot.py -q
```

Expected: current reader has no basis resolver and selects latest rows without an explicit basis.

- [ ] **Step 3: Implement exact basis-aware interfaces**

```text
DatasetSnapshotReader.resolve_adjustment_basis(
    code: str, effective_market_date: str
) -> StockAdjustmentBasis
DatasetSnapshotReader.get_basis_adjusted_stock_ohlcv(
    code: str, *, basis_id: str,
    start: str | None = None, end: str | None = None
) -> pd.DataFrame
DatasetSnapshotReader.get_adjusted_statement_metrics(
    code: str, *, basis_id: str, as_of_date: str | None = None
) -> list[dict[str, Any]]
DatasetSnapshotReader.get_daily_valuation(
    code: str, *, basis_id: str,
    start: str | None = None, end: str | None = None
) -> list[dict[str, Any]]
```

Use the shared interval/coverage predicates from Task 2. Remove broad exception-to-empty conversion from these paths.

- [ ] **Step 4: Verify and commit Task 8**

Run Step 2, Ruff/Pyright for the reader, then:

```bash
git add apps/bt/src/infrastructure/db/market/dataset_snapshot_reader.py \
  apps/bt/tests/unit/server/test_dataset_snapshot_reader.py \
  apps/bt/tests/unit/server/db/test_dataset_event_time_basis_snapshot.py
git commit -m "feat(dataset): add basis-aware PIT reads"
```

### Task 9: Wire complete PIT data into Dataset creation

**Files:**
- Modify: `apps/bt/src/application/services/dataset_builder_copy_stages.py`
- Modify: `apps/bt/src/application/services/dataset_builder_service.py`
- Test: `apps/bt/tests/unit/server/test_dataset_builder_service.py`
- Test: `apps/bt/tests/unit/server/test_dataset_builder_service_branches.py`

**Interfaces:**
- Consumes: Task 6 writer and Task 7 manifest.
- Produces: one dedicated PIT copy stage after the dataset date range and normalized code set are fixed.

- [ ] **Step 1: Add a failing end-to-end two-regime build test**

```python
@pytest.mark.asyncio
async def test_builder_publishes_complete_event_time_bundle(tmp_path: Path) -> None:
    source = _build_v4_market_with_two_regimes(tmp_path)
    result = await _build_dataset(source, tmp_path / "dataset")
    manifest = json.loads((result.snapshot_dir / "manifest.v2.json").read_text())
    assert manifest["schemaVersion"] == 3
    assert manifest["source"]["marketSchemaVersion"] == 4
    reader = DatasetSnapshotReader(result.snapshot_dir)
    assert reader.resolve_adjustment_basis("7203", "2024-06-27").valid_from == "2024-01-04"
    assert reader.resolve_adjustment_basis("7203", "2024-06-28").valid_from == "2024-06-28"
```

Add cancellation coverage proving no manifest is written before copy/export/close completes.

- [ ] **Step 2: Run builder tests and confirm the old adjusted-only stage fails**

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/test_dataset_builder_service.py \
  tests/unit/server/test_dataset_builder_service_branches.py -q
```

- [ ] **Step 3: Replace the permissive adjusted copy stage**

After stock range and normalized codes are known, call `copy_event_time_pit_from_source`. Require Market v4 before any destination writes. Write dataset source schema/mode and schemaVersion 3 manifest only after DuckDB close and all Parquet exports/checksums complete.

- [ ] **Step 4: Verify and commit Task 9**

Run all Task 6-9 dataset tests plus:

```bash
uv run --directory apps/bt ruff check \
  src/infrastructure/db/dataset_io \
  src/infrastructure/db/market/dataset_snapshot_reader.py \
  src/application/services/dataset_builder_service.py \
  src/application/services/dataset_builder_copy_stages.py \
  src/application/services/dataset_resolver.py
uv run --directory apps/bt pyright \
  src/infrastructure/db/dataset_io \
  src/infrastructure/db/market/dataset_snapshot_reader.py \
  src/application/services/dataset_builder_service.py \
  src/application/services/dataset_builder_copy_stages.py \
  src/application/services/dataset_resolver.py
git add apps/bt/src/application/services/dataset_builder_copy_stages.py \
  apps/bt/src/application/services/dataset_builder_service.py \
  apps/bt/tests/unit/server/test_dataset_builder_service.py \
  apps/bt/tests/unit/server/test_dataset_builder_service_branches.py
git commit -m "feat(dataset): build complete v4 PIT snapshots"
```

### Task 10: Resolve one canonical Fundamentals PIT snapshot

**Files:**
- Create: `apps/bt/src/application/contracts/fundamentals_pit.py`
- Create: `apps/bt/src/infrastructure/data_access/fundamentals_pit_reader.py`
- Modify: `apps/bt/src/infrastructure/db/market/market_reader.py`
- Modify: `apps/bt/src/infrastructure/data_access/clients.py`
- Test: `apps/bt/tests/unit/server/db/test_fundamentals_pit_reader.py`
- Test: `apps/bt/tests/unit/server/db/test_market_reader.py`

**Interfaces:**
- Consumes: Market v4 basis repository and exact-date master data.
- Produces: one immutable `FundamentalsPitSnapshot` and typed fail-closed reasons.
- Produces: `DirectMarketClient.get_fundamentals_pit_snapshot(symbol, cutoff_date)` and the matching `DirectMarketDataClient` delegate.

- [ ] **Step 1: Write failing single-snapshot tests**

```python
def test_snapshot_resolves_weekend_to_one_basis_and_exact_master(v4_market: Path) -> None:
    client = _direct_client_for_path(v4_market)
    snapshot = client.get_fundamentals_pit_snapshot(
        "7203", date.fromisoformat("2024-06-30")
    )
    assert snapshot.knowledge_cutoff_date == date(2024, 6, 30)
    assert snapshot.effective_market_date == date(2024, 6, 28)
    assert snapshot.basis_id == "event-pit-v1:7203:2024-06-28"
    assert snapshot.stock_info.code[:4] == "7203"
    assert snapshot.stock_master_snapshot_date == date(2024, 6, 28)


def test_snapshot_rejects_future_nested_provenance(v4_market: Path) -> None:
    _set_forward_disclosure(v4_market, row_date="2024-06-28", disclosed="2024-07-01")
    with pytest.raises(FundamentalsPitSnapshotError) as exc_info:
        _client(v4_market).get_fundamentals_pit_snapshot("7203", date(2024, 6, 30))
    assert exc_info.value.reason == "pit_snapshot_inconsistent"
```

Add missing basis, under-covered basis, missing exact master, not-listed symbol, mixed basis, future statement, future OHLCV, and no-cutoff-current-frontier cases.

- [ ] **Step 2: Run the new reader tests**

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/db/test_fundamentals_pit_reader.py \
  tests/unit/server/db/test_market_reader.py -q
```

Expected: missing snapshot contract/reader and lack of a protected multi-query read snapshot.

- [ ] **Step 3: Add typed snapshot contracts**

```python
FundamentalsPitReason = Literal[
    "stock_not_listed_as_of",
    "historical_adjustment_basis_required",
    "stock_master_snapshot_required",
    "pit_snapshot_inconsistent",
]


@dataclass(frozen=True)
class FundamentalsPitSnapshot:
    requested_cutoff_date: date | None
    knowledge_cutoff_date: date
    effective_market_date: date
    stock_master_snapshot_date: date
    basis_id: str
    adjustment_through_date: date
    materialized_through_date: date
    stock_info: StockInfo
    statements: pd.DataFrame
    adjusted_statement_metrics: Sequence[dict[str, Any]]
    daily_valuation: Sequence[dict[str, Any]]
    ohlcv: pd.DataFrame
    prime_liquidity_panel: pd.DataFrame


class FundamentalsPitSnapshotError(RuntimeError):
    def __init__(self, reason: FundamentalsPitReason, message: str) -> None:
        self.reason = reason
        super().__init__(message)
```

- [ ] **Step 4: Add a protected DuckDB read transaction**

Implement `MarketDbReader.read_snapshot()` as a context manager around one thread connection and `BEGIN TRANSACTION`/`ROLLBACK`, with nested-use rejection. Only the reader owns these control statements; public `query()` remains read-only SQL constrained.

```python
with reader.read_snapshot():
    snapshot = resolve_fundamentals_pit_snapshot(reader, symbol, cutoff_date)
```

- [ ] **Step 5: Implement the canonical resolver**

```text
resolve_fundamentals_pit_snapshot(
    reader: MarketDbReader,
    symbol: str,
    cutoff_date: date | None,
) -> FundamentalsPitSnapshot
```

Resolve `knowledge_cutoff_date`, global effective market session, containing ready/covered basis, exact master, bounded statements, exact-basis adjusted metrics/valuation/OHLCV, and exact-date Prime panel in that order. The Prime panel must use exact `stock_master_daily`, same-basis valuation free-float market cap, and basis-adjusted OHLCV; delete its use of `stocks_latest`, raw statements, current `stock_data`, and independently loaded adjustment events. Validate every date/basis/provenance field before returning.

- [ ] **Step 6: Expose exactly one client method**

```python
def get_fundamentals_pit_snapshot(
    self,
    symbol: str,
    cutoff_date: date | None,
) -> FundamentalsPitSnapshot:
    return resolve_fundamentals_pit_snapshot(
        _resolve_market_reader(), symbol, cutoff_date
    )
```

Add the same signature to `DirectMarketDataClient` and delegate to
`self._market.get_fundamentals_pit_snapshot`; this is the object injected into
`FundamentalsService`.

- [ ] **Step 7: Verify and commit Task 10**

Run Step 2, Ruff/Pyright for the new contracts/reader/client files, then:

```bash
git add apps/bt/src/application/contracts/fundamentals_pit.py \
  apps/bt/src/infrastructure/data_access/fundamentals_pit_reader.py \
  apps/bt/src/infrastructure/db/market/market_reader.py \
  apps/bt/src/infrastructure/data_access/clients.py \
  apps/bt/tests/unit/server/db/test_fundamentals_pit_reader.py \
  apps/bt/tests/unit/server/db/test_market_reader.py
git commit -m "feat(bt): resolve canonical fundamentals PIT snapshots"
```

### Task 11: Cut FundamentalsService over to the PIT snapshot

**Files:**
- Modify: `apps/bt/src/application/services/fundamentals_service.py`
- Modify: `apps/bt/src/entrypoints/http/schemas/fundamentals.py`
- Create: `apps/bt/tests/unit/server/services/test_fundamentals_event_time_pit.py`
- Modify: `apps/bt/tests/unit/server/services/test_fundamentals_adjusted_sot.py`
- Modify: `apps/bt/tests/unit/server/services/test_fundamentals_liquidity_profile.py`
- Modify: `apps/bt/tests/server/services/test_fundamentals_service.py`

**Interfaces:**
- Consumes: Task 10 snapshot only.
- Produces: response calculations based on one basis with `from` applied only after calculations.
- Produces: `date | None` request fields and required `asOfDate` response field used by the service.

- [ ] **Step 1: Write failing service isolation and future-sentinel tests**

```python
def test_service_uses_only_one_pit_snapshot() -> None:
    client = SnapshotOnlyClient(_pit_snapshot())
    result = FundamentalsService(client).compute_fundamentals(_request(to="2024-06-30"))
    assert client.calls == [("get_fundamentals_pit_snapshot", "7203", date(2024, 6, 30))]
    assert result.asOfDate == "2024-06-28"
    assert result.provenance.reference_date == "2024-06-30"


def test_post_cutoff_revision_and_split_do_not_change_response() -> None:
    clean = FundamentalsService(SnapshotOnlyClient(_pit_snapshot())).compute_fundamentals(_request())
    poisoned = FundamentalsService(
        SnapshotOnlyClient(_pit_snapshot_with_unreachable_future_sentinels())
    ).compute_fundamentals(_request())
    assert poisoned.model_dump() == clean.model_dump()
```

Add tests proving `from` changes only returned `data`/`dailyValuation`, pre-cutoff future-period forecasts remain, and current metadata getters are never called.

- [ ] **Step 2: Run the service suites and confirm independent getters are used**

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_fundamentals_event_time_pit.py \
  tests/unit/server/services/test_fundamentals_adjusted_sot.py \
  tests/unit/server/services/test_fundamentals_liquidity_profile.py \
  tests/server/services/test_fundamentals_service.py -q
```

Expected: missing `asOfDate`, multiple market getter calls, current adjustment application, and current liquidity reads.

- [ ] **Step 3: Type the shared service request/response dates**

Change `from_date` and `to_date` to `date | None`, validate
`from_date <= to_date`, and add required `asOfDate: str` to
`FundamentalsComputeResponse`. Keep JSON aliases unchanged. Calculator/display
helpers receive `.isoformat()` strings only at their boundary.

- [ ] **Step 4: Replace independent loads with one snapshot call**

At the start of `compute_fundamentals`, call only:

```python
snapshot = self.market_client.get_fundamentals_pit_snapshot(
    request.symbol,
    request.to_date,
)
```

Convert bounded statements once. Use snapshot history for calculations and crop only response `data` by fiscal period `from_date` and response `dailyValuation` by market-date `from_date` after latest/revision/rolling computations.

- [ ] **Step 5: Remove service-local basis work**

Delete the execution path through `_get_stock_adjustment_events`, `_apply_share_adjustments`, current `_get_stock_info`, current `_get_daily_stock_ohlcv`, implicit `_get_adjusted_statement_metrics`, implicit `_get_adjusted_daily_valuation`, and latest-price fallback. `_build_prime_liquidity_profile` receives `snapshot.prime_liquidity_panel` and never calls the client.

Set:

```python
asOfDate=snapshot.effective_market_date.isoformat()
priceBasisDate=snapshot.adjustment_through_date.isoformat()
provenance=build_market_provenance(
    reference_date=snapshot.knowledge_cutoff_date.isoformat(),
    loaded_domains=(
        "stock_adjustment_bases",
        "stock_master_daily",
        "statements",
        "statement_metrics_adjusted",
        "daily_valuation",
        "stock_data_raw",
    ),
)
```

- [ ] **Step 6: Verify and commit Task 11**

Run Step 2 plus Ruff/Pyright for `fundamentals_service.py`, then:

```bash
git add apps/bt/src/application/services/fundamentals_service.py \
  apps/bt/src/entrypoints/http/schemas/fundamentals.py \
  apps/bt/tests/unit/server/services/test_fundamentals_event_time_pit.py \
  apps/bt/tests/unit/server/services/test_fundamentals_adjusted_sot.py \
  apps/bt/tests/unit/server/services/test_fundamentals_liquidity_profile.py \
  apps/bt/tests/server/services/test_fundamentals_service.py
git commit -m "refactor(bt): consume fundamentals PIT snapshots"
```

### Task 12: Unify both Fundamentals HTTP contracts and errors

**Files:**
- Create: `apps/bt/src/entrypoints/http/routes/fundamentals_error_mapping.py`
- Modify: `apps/bt/src/entrypoints/http/routes/fundamentals.py`
- Modify: `apps/bt/src/entrypoints/http/routes/analytics_market.py`
- Test: `apps/bt/tests/server/routes/test_fundamentals.py`
- Test: `apps/bt/tests/unit/server/test_routes_analytics_fundamentals.py`
- Test: `apps/bt/tests/unit/server/test_openapi.py`

**Interfaces:**
- Consumes: Task 10 typed errors and Task 11 result.
- Produces: required `asOfDate`, typed ISO dates, common 404/409/422 mapping, and identical empty results.

- [ ] **Step 1: Add parameterized GET/POST parity tests**

```python
@pytest.mark.parametrize("endpoint", ["post_compute", "get_analytics"])
def test_listed_symbol_without_disclosure_returns_200_empty(endpoint: str, client: TestClient) -> None:
    response = _call(endpoint, client, result=_empty_pit_response())
    assert response.status_code == 200
    assert response.json()["data"] == []


@pytest.mark.parametrize(
    ("reason", "status"),
    [
        ("stock_not_listed_as_of", 404),
        ("historical_adjustment_basis_required", 409),
        ("stock_master_snapshot_required", 409),
        ("pit_snapshot_inconsistent", 409),
    ],
)
def test_routes_map_same_pit_error(reason: str, status: int, client: TestClient) -> None:
    for endpoint in ("post_compute", "get_analytics"):
        response = _call_with_pit_error(endpoint, client, reason)
        assert response.status_code == status
        assert {"field": "reason", "message": reason} in response.json()["details"]
```

Add impossible dates (`2024-02-30`), malformed dates, reversed range, and distinct `asOfDate`/`priceBasisDate`/provenance date assertions.

- [ ] **Step 2: Run route/OpenAPI tests and confirm current divergence**

```bash
uv run --directory apps/bt pytest \
  tests/server/routes/test_fundamentals.py \
  tests/unit/server/test_routes_analytics_fundamentals.py \
  tests/unit/server/test_openapi.py -q
```

Expected: GET still converts empty data to 404, GET/POST error mapping differs,
and GET does not yet reuse the typed request validation path.

- [ ] **Step 3: Reuse the typed request model from both routes**

GET query parameters use `date | None` and construct the same
`FundamentalsComputeRequest` already used by POST. FastAPI/Pydantic therefore
return the same 422 response for malformed/impossible/reversed ranges.

- [ ] **Step 4: Share one error mapper and remove divergent empty handling**

```python
def raise_fundamentals_http_error(exc: FundamentalsPitSnapshotError) -> NoReturn:
    status = 404 if exc.reason == "stock_not_listed_as_of" else 409
    recovery = None if status == 404 else "adjusted_metrics_pit"
    raise build_structured_http_exception(
        status,
        str(exc),
        reason=exc.reason,
        recovery=recovery,
    ) from exc
```

Both routes catch only the shared typed error, call this mapper, and return the service result without an empty-data override. Remove `DailyValuationRequiredError` and its old recovery mapping.

- [ ] **Step 5: Verify and commit Task 12**

Run Step 2 plus Ruff/Pyright for schemas/routes, then:

```bash
git add apps/bt/src/entrypoints/http/routes/fundamentals_error_mapping.py \
  apps/bt/src/entrypoints/http/routes/fundamentals.py \
  apps/bt/src/entrypoints/http/routes/analytics_market.py \
  apps/bt/tests/server/routes/test_fundamentals.py \
  apps/bt/tests/unit/server/test_routes_analytics_fundamentals.py \
  apps/bt/tests/unit/server/test_openapi.py
git commit -m "feat(bt): expose fundamentals PIT API semantics"
```

### Task 13: Synchronize OpenAPI/TypeScript, document the new SoT, and run final verification

**Files:**
- Generated: `apps/ts/packages/contracts/openapi/bt-openapi.json`
- Generated: `apps/ts/packages/contracts/src/clients/backtest/generated/bt-api-types.ts`
- Modify: `apps/ts/packages/contracts/src/types/api-types.ts`
- Modify: `apps/ts/packages/api-clients/src/backtest/fundamentals-types.ts`
- Modify: `apps/ts/packages/api-clients/src/analytics/types.ts`
- Modify: `apps/ts/packages/api-clients/src/analytics/AnalyticsClient.ts`
- Test: `apps/ts/packages/api-clients/src/analytics/AnalyticsClient.test.ts`
- Test: `apps/ts/packages/contracts/src/types/api-response-types.test.ts`
- Test: `apps/ts/packages/web/src/hooks/useFundamentals.test.tsx`
- Modify: `AGENTS.md`
- Modify: `docs/architecture-sot-matrix.md`
- Modify: `.codex/skills/bt-financial-analysis/SKILL.md`
- Modify: `.codex/skills/bt-database-management/SKILL.md`
- Modify: `.codex/skills/ts-financial-analysis/SKILL.md`

**Interfaces:**
- Consumes: all backend contract changes from Tasks 4 and 12.
- Produces: synchronized generated contracts, minimally aligned handwritten types, API-client parameter forwarding, and current architecture documentation.

- [ ] **Step 1: Add failing TypeScript contract/client tests**

```typescript
it('requires the fundamentals market as-of date', () => {
  const response: ApiFundamentalsResponse = Object.assign({}, fundamentalsFixture, {
    asOfDate: '2024-06-28',
  });
  expect(response.asOfDate).toBe('2024-06-28');
});

it('forwards the complete PIT query', async () => {
  await client.getFundamentals({
    symbol: '7203',
    from: '2020-01-01',
    to: '2024-06-30',
    periodType: 'FY',
    preferConsolidated: false,
  });
  expect(fetchSpy.mock.calls.at(-1)?.[0]).toContain(
    'from=2020-01-01&to=2024-06-30&periodType=FY&preferConsolidated=false'
  );
});
```

- [ ] **Step 2: Regenerate the backend OpenAPI contract**

```bash
bun --cwd="$PWD/apps/ts" run --filter @trading25/contracts bt:sync
```

Expected diff: Market DB materialization job aggregate fields, Fundamentals date schemas/descriptions, `asOfDate`, and no unrelated endpoint changes.

- [ ] **Step 3: Align current handwritten types without doing the deferred ownership migration**

Add required `asOfDate: string` to `ApiFundamentalsResponse` and the backtest `FundamentalsComputeResponse`. Extend analytics parameters exactly as follows:

```typescript
export interface FundamentalsParams {
  symbol: string;
  from?: string;
  to?: string;
  periodType?: 'all' | 'FY' | '1Q' | '2Q' | '3Q';
  preferConsolidated?: boolean;
  tradingValuePeriod?: number;
  forecastEpsLookbackFyCount?: number;
}
```

Forward every property in `AnalyticsClient.getFundamentals`. Update fixtures with `asOfDate`. Do not alias or migrate the wider Fundamentals contract family in this task.

- [ ] **Step 4: Update architecture and skill truth**

Document:

- Market schema v4 and `local_projection_v2_event_time`;
- basis catalog/segments and retained regime semantics;
- `adjusted_metrics_pit` sync/materialization stage;
- strict Fundamentals `from`/`to`, GET/POST, and error behavior;
- dataset payload schemaVersion 3 in physical `manifest.v2.json` and pre-v4 rejection;
- current `stock_data` remains convenience-only and is forbidden in cutoff-aware Fundamentals.

Run:

```bash
python3 scripts/skills/refresh_skill_references.py --check
python3 scripts/skills/validate_skills.py
```

- [ ] **Step 5: Run focused TypeScript verification**

```bash
bun --cwd="$PWD/apps/ts" run --filter @trading25/contracts bt:check
bun --cwd="$PWD/apps/ts" run --filter @trading25/contracts test
bun --cwd="$PWD/apps/ts" run --filter @trading25/api-clients test
bun --cwd="$PWD/apps/ts" run --filter @trading25/web test -- useFundamentals
bun --cwd="$PWD/apps/ts" run quality:typecheck
bun --cwd="$PWD/apps/ts" run quality:deps:audit
```

- [ ] **Step 6: Run full backend and repository verification**

```bash
uv run --directory apps/bt pytest tests/ -q
uv run --directory apps/bt ruff check src/
uv run --directory apps/bt pyright src/
bun --cwd="$PWD/apps/ts" run workspace:test
bun --cwd="$PWD/apps/ts" run quality:lint
git diff --check
git status --short
```

Expected: every command passes; only intentional files are modified; no v3 market compatibility, old-basis prune, latest/current fallback, schemaVersion 2 reader, or service-local adjustment path remains.

- [ ] **Step 7: Request two-stage review before the final commit**

Dispatch one spec-compliance reviewer against the approved design and this plan, then one code-quality reviewer against the complete diff. Resolve every finding and rerun the smallest affected tests plus Step 6.

- [ ] **Step 8: Commit Task 13**

```bash
git add apps/ts/packages/contracts \
  apps/ts/packages/api-clients \
  apps/ts/packages/web/src/hooks/useFundamentals.test.tsx \
  AGENTS.md docs/architecture-sot-matrix.md .codex/skills
git commit -m "chore: sync event-time PIT contracts and docs"
```

## Execution Order and Review Gates

1. Tasks 1-5 are Data Plane Slice A. Run the full focused Data Plane suite and complete a spec-compliance review before Task 6.
2. Tasks 6-9 are Dataset propagation. Do not begin them until the v4 repository/materializer interfaces are stable.
3. Tasks 10-12 are Fundamentals Slice B. Do not begin service work until both Market and Dataset basis readers pass.
4. Task 13 is contract synchronization, documentation, full verification, and final review.
5. Every task uses a fresh implementation subagent followed by a spec-compliance review and code-quality review before advancing.
