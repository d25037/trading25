# Market v5 full-rebuild cutover runbook

## Scope and non-negotiable contract

This is the only supported operator path from an older Market data plane to
Market schema 5 with `stock_price_adjustment_mode=provider_adjusted_v1`.
There is no in-place migration, dual read, compatibility alias, or promotion
of a retained Market v4 rehearsal. A retained v4 tree is ineligible as a v5
candidate and must not be rebuilt or activated. Market v4 is retained only as
an immutable v4 backup for exact rollback.

The workflow performs two independent full builds:

1. `rehearse` builds and smokes a fresh isolated root.
2. `cutover` revalidates that report, builds a fresh staging root again,
   verifies Market schema 5, `providerVintage`, and Dataset schema 4, then
   atomically activates the staged `market-timeseries` tree.

All artifacts are create-only beneath
`$TRADING25_DATA_DIR/operations/market-v5-cutover/`. The tool never falls back
to the former v4 operation namespace.

## Preconditions

- Stop FastAPI, sync workers, backtest workers, and any process that can open
  the active Market root. Do not continue while jobs are running.
- Use a clean, immutable git commit; tracked changes make the command fail.
- Load the existing J-Quants credentials without printing them. Set
  `JQUANTS_PLAN` explicitly to `free`, `light`, `standard`, or `premium`.
- Ensure enough free space for the active tree, immutable backup, isolated
  rehearsal, fresh cutover staging tree, Parquet exports, and quarantine.
- Select a liquid symbol and a production smoke strategy that are valid for
  the configured provider plan and coverage.
- Use new IDs. Reports and backups are immutable and IDs are never reused.

Example operator shell:

```bash
cd apps/bt
set -a
source ~/.config/trading25/config.env
set +a

export JQUANTS_PLAN=standard
export REHEARSAL_ID=market-v5-rehearsal-20260721-r1
export BACKUP_ID=market-v4-before-v5-20260721-r1
export CUTOVER_ID=market-v5-active-20260721-r1
export SMOKE_SYMBOL=7203
export SMOKE_STRATEGY=production/cutover_smoke
```

Do not pass a different data root between phases. If `TRADING25_DATA_DIR` is
set, keep it unchanged for preflight, backup, rehearsal, cutover, and restore.

## 1. Stop writers and preflight

Stop FastAPI and all workers first, then run:

```bash
uv run bt market-cutover preflight
```

Preflight must prove the operation lease is exclusive, owned runtimes are
quiescent, DuckDB can checkpoint, the WAL is absent or empty, the active tree
is descriptor-confined, and disk capacity is sufficient. A v4 active database
is permitted here because it is the source of the rollback backup; it is not a
candidate for activation.

## 2. Create and verify the immutable backup

```bash
uv run bt market-cutover backup "$BACKUP_ID"
```

The backup contains the exact active `market.duckdb` and Parquet file set plus
size and SHA-256 evidence. It is made read-only after verification. Confirm:

```bash
test -f "$TRADING25_DATA_DIR/operations/market-v5-cutover/backups/$BACKUP_ID/manifest.json"
```

Do not edit, chmod, replace, or delete the backup. The cutover command verifies
its checksums and source-root fingerprint again before it starts rebuilding.

## 3. Run the isolated full rehearsal

```bash
uv run bt market-cutover rehearse "$REHEARSAL_ID" \
  --symbol "$SMOKE_SYMBOL" \
  --strategy "$SMOKE_STRATEGY" \
  --dataset-preset primeMarket
```

The owned server uses a fresh isolated root and runs initial sync. Because the
staging database is newly created, no active database is reset or migrated.
The passing report must contain both phases:

- `initial_sync_and_provider_vintage`
- `semantic_smoke`

Inspect the create-only report:

```bash
python -m json.tool \
  "$TRADING25_DATA_DIR/operations/market-v5-cutover/reports/$REHEARSAL_ID/report.json"
```

Do not proceed unless all of these are true:

- `phase` is `rehearsal`, `status` is `passed`, and `rehearsalMode` is
  `full_rebuild`.
- `serverProcessJoined` and `workerProcessJoined` are true.
- `schemaCoverage.schemaVersion` is 5 and
  `schemaCoverage.stockPriceAdjustmentMode` is `provider_adjusted_v1`.
- `schemaCoverage.providerVintage` has positive provider-window and current
  basis counts and zero invalid, missing, stale, wrong-basis, and orphan
  counters.
- The smoke exercised stats, validation, fundamentals parity, screening,
  ranking, dataset creation, Dataset schema 4 info, and sample reads.

The rehearsal tree is evidence only. It is never a promotion source.

## 4. Run the canonical full rebuild and atomic cutover

Keep all writers stopped and run:

```bash
uv run bt market-cutover cutover "$CUTOVER_ID" \
  --rehearsal-report-id "$REHEARSAL_ID" \
  --backup-id "$BACKUP_ID" \
  --symbol "$SMOKE_SYMBOL" \
  --strategy "$SMOKE_STRATEGY" \
  --dataset-preset primeMarket
```

Before mutation the command rechecks the exact report ID, git identity, smoke
configuration, root/configuration fingerprint, immutable backup, quiescence,
checkpoint, WAL, and disk space. It then fetches and validates a new Market v5
candidate in staging. The candidate must prove:

- Market schema 5 and `provider_adjusted_v1`;
- coherent provider plan, provider-as-of range, effective coverage, canonical
  source fingerprint, provider-adjusted/raw equality, event-ledger integrity,
  and current-basis fundamentals freshness through `providerVintage`;
- Dataset schema 4 with exactly matching `providerPlan`, `providerAsOf`,
  `providerCoverageStart`, `providerCoverageEnd`,
  `providerSourceFingerprint`, and `fundamentalsAdjustmentBasisDate`;
- semantic reads through fundamentals, screening, ranking, and dataset APIs.

Only after those checks and owned-process joins does an atomic directory
exchange activate the rebuilt tree. The active tree is smoked again before a
passing report is published.

## 5. Verify the active data plane

Read the cutover report first:

```bash
python -m json.tool \
  "$TRADING25_DATA_DIR/operations/market-v5-cutover/reports/$CUTOVER_ID/report.json"
```

Then start FastAPI and run a read-only smoke with a new operation ID:

```bash
uv run bt server --port 3002

uv run bt market-cutover smoke \
  --operation-id "$CUTOVER_ID.post" \
  --symbol "$SMOKE_SYMBOL" \
  --strategy "$SMOKE_STRATEGY" \
  --dataset-preset primeMarket
```

Also inspect `GET /api/db/stats` and `GET /api/db/validate`. The schema must be
current at 5, validation must be healthy, and `providerVintage.status` must be
`ready`. Do not accept a v4 schema, an old adjustment mode, absent coverage,
or a Dataset v3 bundle as a partial success.

Existing Dataset v3/Market v4 snapshots are unsupported. Delete or recreate
them as separate operator work; cutover does not inherit their price rows.

## Failure behavior and exact rollback

- Failure before atomic activation leaves the active Market tree unchanged.
- Failure after activation triggers automatic restore from the exact immutable
  backup when the owned server and worker are proven joined.
- If an owned child is not proven joined, the operation lease remains fenced
  and automatic restore is deferred. Resolve process ownership first; do not
  alter the operation lock, report, staging, or quarantine files manually.
- Failed staging trees, reports, logs, and quarantined trees remain for
  diagnosis. Do not treat them as activation candidates.

For an operator-requested exact rollback, stop FastAPI and workers and run:

```bash
uv run bt market-cutover restore "$BACKUP_ID"
```

Restore verifies the immutable manifest, copies to a fresh stage, verifies the
exact file set and SHA-256 values, quarantines the failed active tree, and
atomically installs the backup. It never deletes or mutates the backup. After
rollback, Market v4 remains incompatible with normal v5 reads and sync; service
availability requires another successful v5 full rebuild.

## Benchmark evidence

The repository benchmark is local and fixture-driven. It does not open the
active database or call J-Quants:

```bash
BENCH_WORKSPACE=$(mktemp -d /tmp/market-v5-sync.XXXXXX)
uv run python scripts/benchmark_market_v5_sync.py \
  --fixture benchmarks/fixtures/market-v5-sync.json \
  --workspace "$BENCH_WORKSPACE/work" \
  --output "$BENCH_WORKSPACE/result.json"
python -m json.tool "$BENCH_WORKSPACE/result.json"
```

The JSON records wall time, CPU time, peak RSS, request/page counts, affected
codes, new rows, row mutations, storage growth, checksums, and
`allCodeMaterializerInvocations` for no-op, one-day, fundamentals-only,
split/drift, and the old all-code/local-projection baseline fixture. Scaling
claims use measured work counters, not timing thresholds.

Recorded fixture evidence is
`apps/bt/benchmarks/market-v5-sync-fixture-evidence.json`. Its
`representativeEvidence` is `unavailable`: no safe representative local v5
database and pre-authorized no-cost credential fixture was used for this task,
so no live or paid requests were made.
