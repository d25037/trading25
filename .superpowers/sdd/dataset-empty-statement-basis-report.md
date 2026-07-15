# Dataset empty-statement basis TDD report

## Scope

Dataset v3 event-time copy and snapshot validation now accept a ready adjustment
basis with complete price, segment, and valuation lineage when the Market v4
source has no statement disclosures for that basis. A source statement still
requires its exact adjusted metric identity for every applicable basis.

## RED

Command:

```text
uv run --directory apps/bt pytest tests/unit/server/db/test_dataset_event_time_basis_snapshot.py -k 'accepts_basis_without_source_statements or rejects_missing_metric_for_source_statement' -q
```

Observed before the production change: 1 failed, 1 passed. The statementless
basis regression failed in `DatasetWriter._validate_staged_event_time_pit` with
`DatasetSnapshotError: adjusted metric coverage is empty`.

## GREEN

Focused Dataset writer, reader, and builder verification:

```text
uv run --directory apps/bt pytest tests/unit/server/db/test_dataset_event_time_basis_snapshot.py tests/unit/server/test_dataset_snapshot_reader.py tests/unit/server/test_dataset_writer.py tests/unit/server/test_dataset_builder_service.py tests/unit/server/test_dataset_builder_service_branches.py -q
```

Result: 118 passed.

Static verification:

```text
uv run --directory apps/bt ruff check src/infrastructure/db/dataset_io/dataset_writer.py src/infrastructure/db/market/dataset_snapshot_reader.py tests/unit/server/db/test_dataset_event_time_basis_snapshot.py tests/unit/server/test_dataset_snapshot_reader.py
uv run --directory apps/bt pyright src/infrastructure/db/dataset_io/dataset_writer.py src/infrastructure/db/market/dataset_snapshot_reader.py
git diff --check
```

Result: Ruff passed; Pyright reported 0 errors, 0 warnings, 0 informations;
`git diff --check` passed.

## Commit

The implementation and this report are committed together as
`fix(bt): allow statementless PIT bases in datasets`.

## Concerns

No known correctness concerns. The writer derives expected identities from the
raw `statements` SoT using the same normalized, column-wise 4/5-digit alias
COALESCE stage used by the actual statement copy. It then applies
`period_end=disclosed_date`, normalized period type, the requested snapshot
cutoff, and basis applicability semantics from adjusted metrics materialization.
The snapshot reader applies the same invariant against copied statements.
Existing incomplete, orphan, wrong-basis, and provenance rejection tests remain
green.

## Review fix wave

Two independent RED regressions reproduced the review findings:

- A canonical 4-digit statement with NULL `type_of_current_period` and a 5-digit
  alias with `FY` was rejected because expected identities selected one whole
  source row instead of using the statement copy's column-wise COALESCE merge.
- A statement disclosed on the requested weekend cutoff after the last Friday
  raw price was ignored by the reader because it inferred cutoff from
  `max(stock_data_raw.date)`.

The statement copy and PIT expectation builder now share one normalized staging
implementation. The writer persists the requested `date_to` under the shared
`event_time_pit_date_to` snapshot-contract key in the same transaction as the
PIT graph. The reader requires a valid ISO cutoff from that key and never infers
it from trading rows. A missing metric at the weekend cutoff is rejected by both
writer and reader; the complete metric snapshot opens successfully.

Review-wave GREEN results:

- 3 focused alias/cutoff regressions passed.
- 121 Dataset writer/reader/builder tests passed.
- 218 broader Dataset resolver/service/API/writer/reader/builder tests passed.
- Ruff passed.
- Pyright reported 0 errors, 0 warnings, 0 informations.
- `git diff --check` passed.

The review fix wave and this report update are committed together as
`fix(bt): align dataset statement identity cutoff`.

## Strict cutoff review wave

Three RED regressions captured the remaining review findings:

- A real Dataset builder run physically copied a disclosure after the persisted
  cutoff, and both `get_statements(end=None)` and
  `get_statements_batch(end=None)` exposed it.
- A schemaVersion 3 bundle without `dataset_info.event_time_pit_date_to` was
  reported by resolver `exists`/`list_datasets` as supported before `resolve`
  failed.
- Application preflight accepted a Market v4 source without `statements`, while
  the writer rejected it later.

The builder now passes its exact stock/PIT `date_to` through every statement
batch. `DatasetWriter.copy_statements_from_source` requires that cutoff, verifies
it against immutable persisted metadata before writing, and stages only
disclosures at or before it. Snapshot integrity also rejects any physically
stored post-cutoff statement. Resolver support preflight opens `dataset.duckdb`
read-only and requires the strict ISO cutoff invariant, so an old v3 bundle is
unsupported rather than migrated or accepted. Application and writer source
preflights share `MARKET_V4_EVENT_TIME_REQUIRED_TABLES`, including `statements`.

Strict-wave verification:

- 3 focused regressions passed after failing for the intended reasons.
- 155 Dataset builder/writer/reader/resolver tests passed.
- 221 broader Dataset resolver/service/API/builder/writer/reader tests passed.
- Ruff passed.
- Pyright reported 0 errors, 0 warnings, 0 informations.
- `git diff --check` passed.

The strict cutoff wave and this report update are committed together as
`fix(bt): enforce dataset statement cutoff contract`.

## All time-indexed families and support preflight wave

RED coverage showed that a real builder omitted future statements but still
published post-cutoff TOPIX, index, and margin rows, while resolver discovery
advertised checksum-mismatched, missing-table, and invalid-lineage bundles.

The exact builder cutoff is now required by every source copy API:
`stock_data`, `topix_data`, `indices_data`, `margin_data`, and `statements`.
Every query filters `date <= cutoff`. Stock copy runs before PIT publication and
therefore rejects a conflicting persisted cutoff when present; PIT publication
then persists/matches the exact value transactionally. All later families
require an exact persisted match. Snapshot integrity rejects physical rows past
the cutoff across every time-indexed table.

Discovery and runtime resolution now share
`validate_supported_dataset_snapshot`. It verifies the full manifest and file
checksums, exact required tables via DuckDB inspection, PIT lineage, logical
counts/coverage/date range, and the required cutoff. Therefore `exists`,
`list_datasets`, and `resolve` agree for malformed bundles without adding a
compatibility path.

Verification:

- 4 focused builder/preflight RED regressions passed.
- 158 core Dataset tests passed after fixture contract updates.
- 224 broader Dataset resolver/service/API/builder/writer/reader tests passed.
- Ruff passed.
- Pyright reported 0 errors, 0 warnings, 0 informations.
- `git diff --check` passed.

This wave and report update are committed together as
`fix(bt): enforce cutoff across dataset time series`.

## Cached support proof and exhaustive physical cutoff wave

Resolver support validation now uses a typed `DatasetValidationProof`. Its cheap
fingerprint includes the manifest content SHA-256 plus path, device, inode, size,
and nanosecond mtime for the manifest, DuckDB, and every manifest-declared
Parquet artifact. A cache miss performs full checksum/inspection validation once
between identical before/after fingerprints. A cache hit serves
`exists`/`list`/`resolve` without rehashing. A changed fingerprint invalidates
the stale reader before revalidation; concurrent replacement fails closed.
Resolver constructs a reader only through the typed proof, while public
`DatasetSnapshotReader(...)` still performs complete validation.

Cutoff validation now runs before any sparse-PIT early return and covers every
physical dated/disclosure table: `stock_data`, `topix_data`, `indices_data`,
`margin_data`, `statements`, `stock_data_raw`, `stock_master_daily`,
`statement_metrics_adjusted`, and `daily_valuation`. Statementless bases remain
valid because zero statements/adjusted metrics do not violate the physical
cutoff or expected-identity invariant.

Verification:

- Cache tests prove one full validation on first resolve, zero rehashes for
  repeated calls, revalidation/reader eviction after stat change, and
  fail-closed concurrent mutation.
- 10 parameterized physical-family/sparse cutoff regressions passed.
- 237 broader Dataset resolver/service/API/builder/writer/reader tests passed.
- Ruff passed.
- Pyright reported 0 errors, 0 warnings, 0 informations.
- `git diff --check` passed.

This wave and report update are committed together as
`fix(bt): cache dataset support validation proofs`.

## Proof binding and immutable artifact identity wave

Validation proofs are now bound to reader use. Each reader retains its exact
artifact fingerprint and rebuilds it immediately before and after every new
read-only DuckDB connection. A replacement in either window fails closed, and
the after-open check closes the just-opened connection before raising. Resolver
also performs a final fingerprint comparison before returning either a new or
cached reader; a concurrent change retires that reader and revalidates instead
of returning stale state.

Snapshot roots, manifests, DuckDB files, and every declared Parquet artifact
must be canonical regular paths rather than symlinks. Validation-cache identity
is the explicit normalized requested dataset name plus canonical snapshot path;
it is never inferred from a resolved path basename. Automatically invalidated
readers are retired without closing them under the resolver lock, so an active
query is not interrupted. Explicit `evict` and `close_all` perform the eventual
close outside the global lock.

Verification:

- Root, manifest, DuckDB, and Parquet symlink regressions passed.
- Before-open and after-open proof checks passed, including closing the newly
  opened connection on an after-open mismatch.
- A controlled final-return race proved a changed cached reader is never
  returned.
- A concurrent active-query regression proved automatic invalidation retires
  without closing; `close_all` later closes the reader.
- 162 Dataset resolver/builder/reader/event-time snapshot tests passed.
- Ruff passed.
- Pyright reported 0 errors, 0 warnings, 0 informations.
- `git diff --check` passed.

Final review hardening replaced recursive resolver re-entry with at most two
iterative resolve attempts. Every final fingerprint mismatch invalidates its
proof; a previously returned reader is retired, while a never-returned candidate
is discarded before cache installation. A second mismatch returns `None`
fail-closed. Persistent artifact churn therefore cannot cause a `RecursionError`
or unbounded reader retirement across repeated resolve calls. The
declared `parquet/` directory itself is also checked with `lstat` and rejected
when it is a symlink, including links whose target remains inside the snapshot
root.

The bounded-retry review fix and this report update are committed together as
`fix(bt): bound dataset proof retries`.
