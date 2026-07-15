# Retained Market v4 Rehearsal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a fail-closed `bt market-cutover rehearse-retained` workflow that reuses the completed r10 isolated Market v4 data plane, runs the current full semantic smoke without sync, and emits exact current-code rehearsal evidence accepted by cutover.

**Architecture:** `MarketV4CutoverService` remains the operation owner for this pre-split change. A retained run derives its source root solely from a source rehearsal ID, acquires the source root lease, starts a new current-code owned server/runtime, invokes the existing `smoke()` method, proves the Market DB did not change, and writes a provenance-rich rehearsal report. The cutover gate accepts only the new explicit report contract; it does not add aliases, report upgrades, or manually adopted evidence.

**Tech Stack:** Python 3.12, Typer, FastAPI runtime adapter, DuckDB adapter, `MarketOperationLease`, pytest, Ruff, Pyright.

## Global Constraints

- Do not call sync, reset, repair, stock refresh, or adjusted-metric materialization from a retained rehearsal.
- Source roots are derived only from `operations/market-v4-cutover/rehearsals/<source-id>/root`; no path option is supported.
- Market schema must be exactly v4 and adjustment mode exactly `local_projection_v2_event_time`.
- Source Market DB and Parquet files are read-only inputs and must be identity-equal before and after smoke.
- A retained run uses a distinct report ID, runtime directory, Dataset smoke name, and server log.
- No legacy report alias, dual reader, force option, or handwritten report adoption is allowed.
- A passing report requires current code identity, unchanged active-root fingerprint, matching smoke config, `serverProcessJoined=true`, and `workerProcessJoined=true`.
- Preserve the user-owned untracked `.codex/config.toml`.

---

## File Map

- Modify `apps/bt/src/application/services/market_v4_cutover.py`: report contract, retained-source eligibility, retained runtime lifecycle, immutable Market identity, and cutover eligibility.
- Modify `apps/bt/src/entrypoints/cli/market_cutover.py`: `rehearse-retained` Typer command; it must not request J-Quants rebuild credentials.
- Modify `apps/bt/tests/unit/server/services/test_market_v4_cutover.py`: service TDD for provenance, no-sync execution, cleanup, mutation detection, and cutover gate.
- Modify `apps/bt/tests/unit/cli_bt/test_market_cutover_cli.py`: command listing, exact argument mapping, and environment independence.
- Modify `docs/runbooks/market-v4-cutover.md`: operator command and evidence rules.
- Modify `AGENTS.md` only if the repository-level cutover contract needs the new canonical command recorded.

### Task 1: Make Rehearsal Evidence Explicit and Fail Closed

**Files:**
- Modify: `apps/bt/src/application/services/market_v4_cutover.py:3319-3378`
- Modify: `apps/bt/src/application/services/market_v4_cutover.py:2825-2852`
- Test: `apps/bt/tests/unit/server/services/test_market_v4_cutover.py`

**Interfaces:**
- Produces: `_operation_report(..., rehearsal_mode: str | None = None, source_rehearsal_report_id: str | None = None, source_rehearsal_code_version: str | None = None, source_retained_root_fingerprint: str | None = None, source_market_identity_before: dict[str, object] | None = None, source_market_identity_after: dict[str, object] | None = None) -> dict[str, object]`.
- Produces: normal `rehearse()` reports with `rehearsalMode="full_rebuild"` and explicit successful join booleans.
- Consumes: existing `SmokeConfig`, `_read_report`, `root_fingerprint`, and exact code identity.

- [ ] **Step 1: Write RED tests for the new report contract**

Add assertions to the normal passing rehearsal test and a new cutover-gate test:

```python
assert report["rehearsalMode"] == "full_rebuild"
assert report["serverProcessJoined"] is True
assert report["workerProcessJoined"] is True

legacy = dict(report)
legacy.pop("rehearsalMode")
write_report(data_root, "legacy-rehearsal", legacy)
with pytest.raises(CutoverSafetyError, match="exact passing rehearsal"):
    service.cutover(
        "active-reject-legacy",
        rehearsal_report_id="legacy-rehearsal",
        backup_id="verified-backup",
        config=smoke_config,
        inherited_environment=rebuild_environment,
    )
```

Also parameterize removal/false values for both join booleans. Expected: every malformed report is rejected before backup verification or staging creation.

- [ ] **Step 2: Run the RED tests**

Run:

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_market_v4_cutover.py \
  -k 'rehearsal_report or cutover_rejects' -q
```

Expected: failures because passing reports omit `rehearsalMode`/join fields and the cutover gate does not require them.

- [ ] **Step 3: Extend `_operation_report` and normal rehearsal calls**

Add keyword-only optional provenance arguments and emit them only when supplied:

```python
if rehearsal_mode is not None:
    report["rehearsalMode"] = rehearsal_mode
if source_rehearsal_report_id is not None:
    report["sourceRehearsalReportId"] = source_rehearsal_report_id
if source_rehearsal_code_version is not None:
    report["sourceRehearsalCodeVersion"] = source_rehearsal_code_version
if source_retained_root_fingerprint is not None:
    report["sourceRetainedRootFingerprint"] = source_retained_root_fingerprint
if source_market_identity_before is not None:
    report["sourceMarketIdentityBefore"] = source_market_identity_before
if source_market_identity_after is not None:
    report["sourceMarketIdentityAfter"] = source_market_identity_after
```

Every normal rehearsal success/failure report call passes
`rehearsal_mode="full_rebuild"`. A normal successful report explicitly passes
both join booleans as `True`; failure paths retain their actual values.

- [ ] **Step 4: Harden the cutover report predicate**

Replace the shallow mode-independent predicate with an exact mode-aware check:

```python
mode = rehearsal.get("rehearsalMode")
common_valid = (
    rehearsal.get("phase") == "rehearsal"
    and rehearsal.get("status") == "passed"
    and rehearsal.get("reportId") == rehearsal_report_id
    and mode in {"full_rebuild", "retained_market_smoke"}
    and rehearsal.get("serverProcessJoined") is True
    and rehearsal.get("workerProcessJoined") is True
    and expected_root_fingerprint == self.root_fingerprint(self.data_root)
    and rehearsal.get("codeVersion") == code_version
    and rehearsal.get("smokeConfig") == expected_smoke_config
)
```

For `retained_market_smoke`, additionally require non-empty source ID/code/root
fingerprint and equal before/after Market identities. Do not accept missing
fields.

- [ ] **Step 5: Run focused tests and commit**

Run:

```bash
uv run --directory apps/bt pytest tests/unit/server/services/test_market_v4_cutover.py -q
uv run --directory apps/bt ruff check src/application/services/market_v4_cutover.py tests/unit/server/services/test_market_v4_cutover.py
uv run --directory apps/bt pyright src/application/services/market_v4_cutover.py
```

Expected: all pass.

Commit:

```bash
git add apps/bt/src/application/services/market_v4_cutover.py \
  apps/bt/tests/unit/server/services/test_market_v4_cutover.py
git commit -m "fix(bt): require explicit rehearsal evidence"
```

### Task 2: Implement the Retained Rehearsal Service Lifecycle

**Files:**
- Modify: `apps/bt/src/application/services/market_v4_cutover.py`
- Test: `apps/bt/tests/unit/server/services/test_market_v4_cutover.py`

**Interfaces:**
- Consumes: Task 1 report fields and mode-aware cutover gate.
- Produces: `MarketV4CutoverService.rehearse_retained(report_id: str, *, source_rehearsal_report_id: str, config: SmokeConfig, inherited_environment: dict[str, str]) -> OperationResult`.
- Produces: `_market_tree_identity(root_fd: int) -> dict[str, object]`, a stable identity for `market-timeseries/market.duckdb` and its Parquet regular files.
- Produces: `_prepare_retained_runtime(retained_root: Path, *, runtime_name: str) -> None`, which creates only a new runtime subtree and copies the retained root's config/strategies without touching Market data.
- Reuses unchanged: `runtime.start`, `runtime.stop`, `runtime.cancel_owned_work`, `smoke`, `_isolated_environment`, `_write_report`, and `MarketOperationLease`.

- [ ] **Step 1: Add RED eligibility tests**

Build a source report/root fixture and parameterize:

```python
@pytest.mark.parametrize("mutation", [
    "missing_source_report",
    "same_report_id",
    "wrong_smoke_config",
    "active_fingerprint_drift",
    "source_status_cleanup_deferred",
    "source_server_unjoined",
    "source_worker_unjoined",
    "source_root_symlink",
    "configuration_drift",
    "schema_v3",
    "wrong_adjustment_mode",
])
def test_rehearse_retained_rejects_ineligible_source(...):
    with pytest.raises(CutoverSafetyError):
        service.rehearse_retained(...)
    runtime.start.assert_not_called()
    assert not new_report_dir.exists()
```

The source report may have `status="failed"` only when both join booleans are
true. Its error string is arbitrary and must not be inspected.

- [ ] **Step 2: Add the RED success/no-sync test**

Use an owned fake runtime and API. Make `service.smoke` return a known
`SmokeResult`. Assert:

```python
result = service.rehearse_retained(
    "retained-r12",
    source_rehearsal_report_id="market-v4-rehearsal-20260715-r10",
    config=smoke_config,
    inherited_environment={},
)
assert result.report_id == "retained-r12"
runtime.start.assert_called_once()
runtime.stop.assert_called_once()
assert api.request_calls_do_not_contain("/api/db/sync")
assert api.request_calls_do_not_contain("/api/db/materialize")
```

Assert the report has current `codeVersion`, source provenance, identical
before/after Market identities, `retained_market_smoke` timing, complete smoke
checks, and both join booleans true.

- [ ] **Step 3: Add RED mutation/cleanup tests**

Cover Market DB or Parquet mutation after smoke, current code drift, active
fingerprint drift, smoke failure, runtime stop failure, and worker shutdown
failure. For mutation:

```python
def mutate_market_after_smoke(*_args, **_kwargs):
    market_db.write_bytes(market_db.read_bytes() + b"changed")
    return smoke_result

with pytest.raises(CutoverSafetyError, match="retained Market tree changed"):
    service.rehearse_retained(...)
report = read_report("retained-mutated")
assert report["status"] == "failed"
assert report["sourceMarketIdentityBefore"] != report["sourceMarketIdentityAfter"]
```

- [ ] **Step 4: Implement source derivation and eligibility**

Add a private resolver that never accepts a path:

```python
def _retained_rehearsal_root(self, source_report_id: str) -> Path:
    source_report_id = self._validate_id(source_report_id, label="source rehearsal report")
    root = self.operations_root / "rehearsals" / source_report_id / "root"
    self._require_confined_real_directory(root, self.operations_root / "rehearsals")
    return root
```

Read the source report, require matching ID/phase/smoke config/target fingerprint,
status in `{"passed", "failed"}`, and clean joins. Compare source and active
configuration fingerprints before starting runtime.

- [ ] **Step 5: Implement stable Market tree identity**

Use existing managed/openat helpers. The identity payload is deterministic:

```python
{
    "marketDuckdb": {"device": ..., "inode": ..., "size": ..., "sha256": ...},
    "parquetSha256": {relative_path: sha256, ...},
}
```

Reject symlinks and special files. Hash only the Market DB and files beneath
`market-timeseries/parquet`; Dataset outputs are outside this identity.

- [ ] **Step 6: Implement the retained runtime subtree**

Add `_prepare_retained_runtime`. It must fail if the new runtime name exists,
create `<retained-root>/market-timeseries/<runtime-name>/{datasets,backtest,config}`,
copy `<retained-root>/config/default.yaml`, and copy
`<retained-root>/strategies` with the existing managed filesystem helpers. It
must not call `_prepare_isolated_root`, because the retained root and Market DB
already exist.

- [ ] **Step 7: Implement retained runtime and smoke**

Under the retained root exclusive lease:

```python
code_version = self._require_code_identity()
active_fingerprint = self.root_fingerprint(self.data_root)
before = self._market_tree_identity(lease.root_fd)
api = self.runtime.start(... current code/new runtime/new log ...)
smoke_started = time.monotonic()
smoke_result = self.smoke(
    api,
    config,
    operation_id=report_id,
    market_root=retained_root / "market-timeseries",
    market_directory_fd=market_fd,
    guard_lease_fd=lease.fd,
)
phases = ({"name": "retained_market_smoke", "durationSeconds": ...},)
self.runtime.stop(api)
after = self._market_tree_identity(lease.root_fd)
```

Then require `before == after`, unchanged code, unchanged active fingerprint,
and write the Task 1 report. Follow the existing failure cleanup structure;
never unlock after an unjoined process.

- [ ] **Step 8: Run service tests and commit**

Run:

```bash
uv run --directory apps/bt pytest tests/unit/server/services/test_market_v4_cutover.py -q
uv run --directory apps/bt ruff check src/application/services/market_v4_cutover.py tests/unit/server/services/test_market_v4_cutover.py
uv run --directory apps/bt pyright src/application/services/market_v4_cutover.py
git diff --check
```

Expected: all pass.

Commit:

```bash
git add apps/bt/src/application/services/market_v4_cutover.py \
  apps/bt/tests/unit/server/services/test_market_v4_cutover.py
git commit -m "feat(bt): revalidate retained market rehearsal"
```

### Task 3: Add the CLI and Operator Contract

**Files:**
- Modify: `apps/bt/src/entrypoints/cli/market_cutover.py`
- Modify: `apps/bt/tests/unit/cli_bt/test_market_cutover_cli.py`
- Modify: `docs/runbooks/market-v4-cutover.md`
- Modify: `AGENTS.md` if required by the existing cutover command list

**Interfaces:**
- Consumes: Task 2 `rehearse_retained` signature.
- Produces: Typer command `bt market-cutover rehearse-retained`.

- [ ] **Step 1: Write RED CLI tests**

Update command listing and fake service:

```python
def rehearse_retained(self, report_id: str, **kwargs: object) -> str:
    calls.append((report_id, kwargs))
    return "ok"

result = runner.invoke(app, [
    "market-cutover", "rehearse-retained", "retained-r12",
    "--source-rehearsal-id", "market-v4-rehearsal-20260715-r10",
    "--symbol", "7203",
    "--strategy", "production/cutover_smoke",
    "--dataset-preset", "primeMarket",
])
assert result.exit_code == 0
assert calls == [("retained-r12", {
    "source_rehearsal_report_id": "market-v4-rehearsal-20260715-r10",
    "config": SmokeConfig(...),
    "inherited_environment": {},
})]
```

Monkeypatch `_required_rebuild_environment` to raise if called; retained CLI must
not require `JQUANTS_API_KEY` or `JQUANTS_PLAN`.

- [ ] **Step 2: Run RED CLI tests**

Run:

```bash
uv run --directory apps/bt pytest tests/unit/cli_bt/test_market_cutover_cli.py -q
```

Expected: command absent/fake method not called.

- [ ] **Step 3: Implement Typer command**

Add:

```python
@market_v4_cutover_app.command("rehearse-retained")
def rehearse_retained_command(
    report_id: str = typer.Argument(..., help="Unique retained rehearsal report ID."),
    source_rehearsal_id: str = typer.Option(..., "--source-rehearsal-id"),
    symbol: str = typer.Option(..., "--symbol"),
    strategy: str = typer.Option(..., "--strategy"),
    dataset_preset: str = typer.Option("primeMarket", "--dataset-preset"),
    data_root: Path | None = DataRootOption,
) -> None:
    _fail_closed(lambda: _service(data_root).rehearse_retained(
        report_id,
        source_rehearsal_report_id=source_rehearsal_id,
        config=_smoke_config(symbol, strategy, dataset_preset),
        inherited_environment={},
    ))
```

- [ ] **Step 4: Update runbook and repository contract**

Document:

```bash
uv run bt market-cutover rehearse-retained market-v4-retained-20260715-r12 \
  --source-rehearsal-id market-v4-rehearsal-20260715-r10 \
  --symbol 7203 \
  --strategy production/cutover_smoke \
  --dataset-preset primeMarket
```

State that this command is valid only after a downstream smoke/code correction;
data-plane changes still require `rehearse`. Record that it makes zero J-Quants
requests and rejects legacy reports.

- [ ] **Step 5: Run CLI/docs gates and commit**

Run:

```bash
uv run --directory apps/bt pytest tests/unit/cli_bt/test_market_cutover_cli.py -q
uv run --directory apps/bt ruff check src/entrypoints/cli/market_cutover.py tests/unit/cli_bt/test_market_cutover_cli.py
uv run --directory apps/bt pyright src/entrypoints/cli/market_cutover.py
python3 scripts/skills/refresh_skill_references.py --check
git diff --check
```

Expected: all pass.

Commit:

```bash
git add apps/bt/src/entrypoints/cli/market_cutover.py \
  apps/bt/tests/unit/cli_bt/test_market_cutover_cli.py \
  docs/runbooks/market-v4-cutover.md AGENTS.md
git commit -m "feat(bt): expose retained rehearsal command"
```

### Task 4: Independent Review and Operational Acceptance

**Files:**
- Inspect: all Task 1--3 diffs
- Runtime evidence: `~/.local/share/trading25/operations/market-v4-cutover/`

**Interfaces:**
- Consumes: current HEAD retained command and r10 source ID.
- Produces: passing retained report used by active cutover.

- [ ] **Step 1: Run focused regression suite**

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_market_v4_cutover.py \
  tests/unit/cli_bt/test_market_cutover_cli.py \
  tests/unit/server/test_dataset_builder_service.py \
  tests/unit/server/test_dataset_builder_service_branches.py \
  tests/unit/server/db/test_dataset_event_time_basis_snapshot.py \
  tests/unit/server/test_dataset_snapshot_reader.py -q
```

Expected: all pass.

- [ ] **Step 2: Run independent two-stage subagent review**

One reviewer checks spec compliance and evidence semantics. A second reviewer
checks code quality, path confinement, lease/process cleanup, and no-sync
guarantees. Fix every Important/Critical finding with TDD and re-review.

- [ ] **Step 3: Run retained rehearsal as a one-shot owned LaunchAgent**

Use a new ID, for example `market-v4-retained-20260715-r12`, and source r10.
Do not load a J-Quants secret. Capture launcher status and self-remove the
LaunchAgent.

- [ ] **Step 4: Verify operational evidence**

Require:

```text
status = passed
phase = rehearsal
rehearsalMode = retained_market_smoke
sourceRehearsalReportId = market-v4-rehearsal-20260715-r10
codeVersion = exact current HEAD
serverProcessJoined = true
workerProcessJoined = true
sourceMarketIdentityBefore = sourceMarketIdentityAfter
apiChecks include dataset_create_info_open
server log contains no /api/db/sync and no JQuants fetch
```

- [ ] **Step 5: Update the main maintenance plan state**

Mark retained rehearsal complete and proceed directly to immutable backup and
active cutover. Do not run another initial sync rehearsal unless data-plane code
changes after this report.

---

## Post-Acceptance Sequence

After Task 4 succeeds, continue the already approved original plan in this
order:

1. Create and verify `market-v3-pre-v4-20260715` immutable active backup.
2. Run active v4 cutover using the exact retained rehearsal report and backup.
3. Run post-active standalone smoke and remove the owned smoke strategy.
4. Implement the Python 3.12 maintainability fail-fast and CI snapshot gate.
5. Move DB/sync application DTOs out of HTTP schemas with zero OpenAPI semantic
   diff.
6. Split `market_v4_cutover.py` into focused modules with no compatibility
   re-exports.
7. Run full backend, TypeScript, contracts, architecture, privacy, and
   maintainability gates.
8. Perform requirement-by-requirement completion audit before marking the goal
   complete.
