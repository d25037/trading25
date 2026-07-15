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
raw `statements` SoT using the same code normalization, alias priority,
`period_end=disclosed_date`, period type normalization, date cutoff, and basis
applicability semantics as adjusted metrics materialization. The snapshot reader
applies the same invariant against copied statements. Existing incomplete,
orphan, wrong-basis, and provenance rejection tests remain green.
