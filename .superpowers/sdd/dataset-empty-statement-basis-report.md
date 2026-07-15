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
