# Contract Sync Parallel Safety Design

## Context

`scripts/check-contract-sync.sh` creates three files with templates such as
`/tmp/bt-openapi-generated.XXXXXX.json`. GNU `mktemp` accepts a suffix after
the `X` run, while BSD/macOS `mktemp` replaces only trailing `X` characters.
On macOS the current template becomes a fixed filename, so two concurrent
checks race and one exits with `File exists`.

The script also regenerates the committed TypeScript output before checking
`git diff`. Even after fixing the JSON temporary files, concurrent checks
would still write the same generated file.

## Decision

Make each check fully isolated and read-only:

1. Allocate one run directory with
   `mktemp -d "${TMPDIR:-/tmp}/bt-contract-sync.XXXXXX"`.
2. Register one recursive cleanup trap immediately after allocation.
3. Store exported and normalized JSON files under that directory.
4. Replace generated-TypeScript writes plus `git diff` with
   `bt:generate-types -- --check`.

No lock is introduced. A portable lock would add stale-lock handling and is
unnecessary once the command has no shared writable output.

## Script Flow

```text
unique temporary directory
        ↓
export OpenAPI to run-local JSON
        ↓
normalize run-local export and committed snapshot
        ↓
compare normalized JSON
        ↓
openapi-typescript --check against committed generated file
        ↓
cleanup run directory
```

The committed OpenAPI snapshot and generated TypeScript file are read-only
inputs. A successful run leaves the worktree and temporary root unchanged.

## Portability and Cleanup

The `XXXXXX` sequence is the final part of the template, which works with GNU
and BSD/macOS `mktemp`. `TMPDIR` is honored when present and `/tmp` remains the
fallback.

Only one allocation occurs. The `EXIT` trap removes the entire run directory
on success, comparison failure, export failure, or TypeScript check failure.
Cleanup after `SIGKILL` is outside shell guarantees and is not addressed.

## TypeScript Contract Check

Use the existing package script and forward `--check`:

```bash
bun run --filter @trading25/contracts bt:generate-types -- --check
```

The pinned `openapi-typescript` version supports `--check`. It compares the
generated content with the output file without rewriting it. The old
`generated_types_path` variable and post-generation `git diff` block become
unnecessary.

Failure guidance remains `bun run --filter @trading25/contracts bt:sync`.

## Tests

Add `apps/bt/tests/unit/scripts/test_check_contract_sync.py` and include it in
the `bt-product-scripts` test target.

The tests execute a copied script in a fixture repository with fake `mktemp`,
`uv`, `bun`, and `git` commands. They verify:

- a BSD-compatible `mktemp` accepts the trailing-`X` directory template;
- two overlapping runs receive different directories and both exit zero;
- a failing export still removes the run directory;
- Bun receives `bt:generate-types -- --check`;
- TypeScript check failure makes the script fail;
- the generated fixture content and modification time remain unchanged;
- the temporary root is empty after each run.

The pre-change RED must fail because the suffix-style file templates are not
accepted by the BSD-compatible fake `mktemp` or because overlapping runs
collide.

Final verification runs the unit tests, shell syntax check, one real contract
check, and two real checks concurrently. Both concurrent processes must exit
zero and generated files must have zero diff.

## Scope

Modify only:

- `scripts/check-contract-sync.sh`
- `apps/bt/tests/unit/scripts/test_check_contract_sync.py`
- `scripts/ci/test_targets.py`
- `apps/bt/tests/unit/scripts/test_test_targets.py`

Do not change OpenAPI snapshots, generated TypeScript, package manifests,
lockfiles, FastAPI code, or CI job topology.

## Completion Criteria

- Single and concurrent real checks pass on macOS.
- The command is read-only with respect to tracked generated files.
- Temporary directories are removed on success and ordinary failure.
- The new test is part of `bt-product-scripts`.
- Ruff, Pyright, shell syntax, test taxonomy, and `git diff --check` pass.

