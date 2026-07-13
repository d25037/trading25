# Contract Sync Parallel Safety Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `check-contract-sync.sh` portable across GNU and BSD/macOS `mktemp`, safe for concurrent execution, and read-only with respect to tracked generated files.

**Architecture:** Each run allocates one unique temporary directory and keeps all generated JSON inside it. TypeScript contract validation uses `openapi-typescript --check` through the existing package script, eliminating the shared generated-file write and the follow-up Git diff.

**Tech Stack:** Bash, Python 3.12/pytest, Bun 1.3, openapi-typescript 7.13

## Global Constraints

- Use exactly one `mktemp -d "${TMPDIR:-/tmp}/bt-contract-sync.XXXXXX"` allocation per run.
- Register cleanup immediately and remove the complete run directory with the `EXIT` trap.
- Do not write the committed OpenAPI snapshot or generated TypeScript file during a check.
- Use `bun run --filter @trading25/contracts bt:generate-types -- --check`.
- Preserve stale-OpenAPI and stale-TypeScript failure guidance.
- Do not add a lock, package dependency, manifest change, lockfile change, or CI job.
- Limit production changes to `scripts/check-contract-sync.sh`.

---

### Task 1: Add the Portable Read-Only Contract Check

**Files:**

- Modify: `scripts/check-contract-sync.sh`
- Create: `apps/bt/tests/unit/scripts/test_check_contract_sync.py`
- Modify: `scripts/ci/test_targets.py`
- Modify: `apps/bt/tests/unit/scripts/test_test_targets.py`

**Interfaces:**

- Produces: a run-local temporary directory and non-mutating TypeScript check
- Consumes: the existing OpenAPI export script and contracts package script
- Preserves: all existing user-facing failure messages and `[contract] PASS`

- [ ] **Step 1: Add failing script-contract tests**

Create `test_check_contract_sync.py` with repository-root resolution and these assertions:

```python
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[5]
SCRIPT = REPO_ROOT / "scripts" / "check-contract-sync.sh"


def _script_text() -> str:
    return SCRIPT.read_text()


def test_contract_sync_uses_one_portable_run_local_directory() -> None:
    script = _script_text()

    assert 'mktemp -d "${TMPDIR:-/tmp}/bt-contract-sync.XXXXXX"' in script
    assert 'trap \'rm -rf "${tmp_dir}"\' EXIT' in script
    assert 'tmp_openapi="${tmp_dir}/bt-openapi.json"' in script
    assert 'tmp_openapi_norm="${tmp_dir}/bt-openapi-normalized.json"' in script
    assert 'tmp_snapshot_norm="${tmp_dir}/bt-openapi-snapshot-normalized.json"' in script
    assert "XXXXXX.json" not in script


def test_contract_sync_checks_types_without_writing_generated_file() -> None:
    script = _script_text()

    assert "bt:generate-types -- --check" in script
    assert "generated_types_path=" not in script
    assert 'git -C "${repo_root}" diff --exit-code' not in script
    assert "Run: bun run --filter @trading25/contracts bt:sync" in script
```

Add `tests/unit/scripts/test_check_contract_sync.py` to
`BT_PRODUCT_SCRIPT_TESTS`. Extend `test_test_targets.py` with:

```python
def test_contract_sync_script_test_is_in_product_script_targets() -> None:
    assert "tests/unit/scripts/test_check_contract_sync.py" in test_targets.BT_PRODUCT_SCRIPT_TESTS
```

- [ ] **Step 2: Run RED**

```bash
./scripts/bt-pytest.sh \
  tests/unit/scripts/test_check_contract_sync.py \
  tests/unit/scripts/test_test_targets.py
```

Expected: the new script-contract assertions fail against suffix-style
`mktemp`, generated-file writes, and the missing target entry.

- [ ] **Step 3: Replace three temporary files with one run directory**

Use:

```bash
tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/bt-contract-sync.XXXXXX")"
trap 'rm -rf "${tmp_dir}"' EXIT

tmp_openapi="${tmp_dir}/bt-openapi.json"
tmp_openapi_norm="${tmp_dir}/bt-openapi-normalized.json"
tmp_snapshot_norm="${tmp_dir}/bt-openapi-snapshot-normalized.json"
```

Remove the three suffix-style allocations and their three-file cleanup trap.

- [ ] **Step 4: Replace the shared TypeScript write with check mode**

Replace regeneration plus Git diff with:

```bash
echo "[contract] Check generated TypeScript types against committed snapshot"
if ! (
  cd "${repo_root}/apps/ts"
  bun run --filter @trading25/contracts bt:generate-types -- --check
); then
  echo "[contract] Generated types are not up to date. Run: bun run --filter @trading25/contracts bt:sync" >&2
  exit 1
fi
```

Remove `generated_types_path`; keep `snapshot_path` and JSON comparison intact.

- [ ] **Step 5: Run GREEN and static checks**

```bash
./scripts/bt-pytest.sh \
  tests/unit/scripts/test_check_contract_sync.py \
  tests/unit/scripts/test_test_targets.py
bash -n scripts/check-contract-sync.sh
UV_CACHE_DIR=/tmp/trading25-uv-cache uv run --directory apps/bt ruff check \
  tests/unit/scripts/test_check_contract_sync.py \
  tests/unit/scripts/test_test_targets.py \
  ../../scripts/ci/test_targets.py
```

Expected: all tests pass, Bash syntax is valid, and Ruff is clean. If Ruff
does not accept the repository-relative script path from `apps/bt`, run Ruff
from the repository root against the two Python test files and
`scripts/ci/test_targets.py` using the project configuration.

- [ ] **Step 6: Run real single and concurrent checks**

Run one check normally, then run two overlapping processes with separate log
files and require both statuses to be zero:

```bash
./scripts/check-contract-sync.sh

tmp_logs="$(mktemp -d "${TMPDIR:-/tmp}/bt-contract-sync-test.XXXXXX")"
./scripts/check-contract-sync.sh >"${tmp_logs}/first.log" 2>&1 & first_pid=$!
./scripts/check-contract-sync.sh >"${tmp_logs}/second.log" 2>&1 & second_pid=$!
wait "${first_pid}"; first_status=$?
wait "${second_pid}"; second_status=$?
test "${first_status}" -eq 0
test "${second_status}" -eq 0
grep -q '\[contract\] PASS' "${tmp_logs}/first.log"
grep -q '\[contract\] PASS' "${tmp_logs}/second.log"
rm -rf "${tmp_logs}"
```

Then verify generated artifacts and the worktree:

```bash
git diff --exit-code -- \
  apps/ts/packages/contracts/openapi/bt-openapi.json \
  apps/ts/packages/contracts/src/clients/backtest/generated/bt-api-types.ts
git diff --check
```

- [ ] **Step 7: Commit Task 1**

```bash
git add scripts/check-contract-sync.sh scripts/ci/test_targets.py \
  apps/bt/tests/unit/scripts/test_check_contract_sync.py \
  apps/bt/tests/unit/scripts/test_test_targets.py
git commit -m "fix(ci): make contract sync checks parallel-safe"
```

---

### Task 2: Whole-Slice Verification

**Files:**

- Verify only; no planned production changes

**Interfaces:**

- Consumes: Task 1
- Produces: fresh single/concurrent portability evidence and final review

- [ ] **Step 1: Re-run unit, taxonomy, and syntax checks**

Run the Task 1 GREEN command, `python3 scripts/skills/refresh_skill_references.py --check`, and `git diff --check` from clean HEAD.

- [ ] **Step 2: Re-run real concurrency verification**

Run one real check and two concurrent real checks. Confirm all three exit zero,
both concurrent logs contain `[contract] PASS`, the generated artifacts have
zero diff, and no `bt-contract-sync.*` directory remains under the selected
temporary root.

- [ ] **Step 3: Request whole-slice review**

Review the design, plan, TDD report, tests, complete diff, cleanup behavior,
cross-platform template, check-mode invocation, CI target inclusion, and real
concurrency evidence. Fix and re-review all Critical or Important findings.

