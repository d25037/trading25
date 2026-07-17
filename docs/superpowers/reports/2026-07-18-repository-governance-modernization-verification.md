# Repository Governance Modernization: Task 7 Verification

**Date:** 2026-07-18

**Branch:** `codex/repo-governance-modernization`

**Merge base:** `53b6adf3b586cea7ecacf44e8c654cec33afc038` (`main`)

**Scope:** Fresh completion audit of Tasks 1-6 against the design and implementation plan. Prior task reports were treated as context, not as verification evidence.

## Outcome

All mandatory Task 7 checks passed. The audited branch now enforces the intended CI test selection, TypeScript build and coverage gates, FastAPI-only TypeScript surface, Git-aware secret scanning, workflow reproducibility controls, repository skill contracts, and current Market v4 guidance.

Two fresh checks initially failed and exposed final artifact drift. They were corrected before this report:

1. `scripts/check-privacy-leaks.py` found a checkout-local absolute path in the tracked Task 6 report.
2. `scripts/maintainability_snapshot.py --check` found that the tracked snapshot had not incorporated the new Python audit source and the deleted TypeScript helper.

The failing commands served as the regression checks. The path was made repository-relative and both maintainability artifacts were regenerated to the exact current values. The same commands then passed. The focused fix is commit `77929a2f` (`fix(governance): close final artifact guardrail gaps`).

## Verification evidence

### Skills, governance, privacy, and maintainability

| Command | Result |
|---|---|
| `python3 scripts/skills/audit_skills.py --strict-legacy` | PASS: repository skill audit passed with strict legacy checks enabled. |
| `python3 scripts/skills/verify_react_catalog.py` | PASS: installed-source React catalog provenance and inventory matched. |
| `python3 scripts/skills/verify_react_catalog.py --offline` | PASS: repository-owned offline React inventory matched. |
| `python3 scripts/skills/refresh_skill_references.py --check` | PASS: generated skill references were current. |
| `python3 scripts/check-privacy-leaks.py` | PASS after the tracked Task 6 report path fix. |
| `uv run --project apps/bt python scripts/maintainability_snapshot.py --root . --json-out docs/maintainability-snapshot-latest.json --md-out docs/maintainability-snapshot-latest.md --check` | PASS after refreshing the tracked JSON and Markdown snapshots. Current totals: 10,144 functions, 346,381 total lines, 272,208 code lines, and 235 files at the 500-line warning threshold. |
| `uv run --directory apps/bt pytest -q tests/unit/scripts/test_audit_skills.py tests/unit/scripts/test_ci_workflow.py tests/unit/scripts/test_ci_changed_scope.py tests/unit/scripts/test_test_targets.py tests/unit/scripts/test_test_taxonomy.py tests/unit/scripts/test_check_privacy_leaks.py tests/unit/scripts/test_maintainability_snapshot.py` | PASS: 239 passed; one warning. This covers workflow security/build assertions, taxonomy, target selection, skills, privacy, and maintainability self-tests. |

### Newly selected BT suites

The suites omitted by the previous taxonomy were run directly:

```text
uv run --directory apps/bt pytest -q \
  tests/unit/contracts \
  tests/unit/domains/fundamentals \
  tests/unit/domains/strategy/runtime \
  tests/unit/domains/analytics/test_market_bubble_footprint_monitor.py
```

Result: PASS, 45 passed; one warning.

The broader repository-owned gates also passed:

| Command | Result |
|---|---|
| `env CI_DEPS_READY=1 BT_UNIT_TEST_SHARDS=3 ./scripts/test-packages.sh` | PASS. BT core shard: 1,234 passed. BT server shard: 4,329 passed and one skipped. The remaining BT analytics/scripts shard and all TS package suites passed. |
| `env CI_DEPS_READY=1 ./scripts/test-apps.sh` | PASS. Web: 1,437 tests. BT app/API/integration selection: 609 passed. |
| `./scripts/test-golden-regression.sh` | PASS: 29 passed; one warning. |

### Lint, types, contracts, builds, tests, and coverage

| Command | Result |
|---|---|
| `./scripts/lint.sh` | PASS. Dependency-direction audit reported 91 allowed edges, no violations, and no stale rules; Biome checked 498 files; Ruff passed. |
| `./scripts/typecheck.sh` | PASS. Generated TS types, all TS packages/apps, dependency declarations, and Pyright passed; Pyright reported zero errors and warnings. |
| `./scripts/check-contract-sync.sh` | PASS. OpenAPI export and generated TS comparison matched; the wire-contract duplicate check covered 337 schemas across 21 files. |
| `bun run --filter @trading25/contracts bt:sync` from `apps/ts` | PASS. The generated contract snapshot was already current and produced no tracked diff. |
| `bun run quality:deps:audit` from `apps/ts` | PASS across six manifests. |
| `bun run workspace:build` from `apps/ts` | PASS. The Vite production build completed after transforming 2,295 modules. |
| `bun run extension:build` from `apps/ts` | PASS. |
| `bun run workspace:test` from `apps/ts` | PASS. Root scripts, contracts, utils, api-clients, extension, and web suites all passed; web ran 1,437 tests. |
| `env CI_DEPS_READY=1 ./scripts/coverage-gate.sh` | PASS. All declared TS LCOV inputs met their thresholds. The full BT suite collected 7,406 tests and exited successfully; included backend product coverage was 89%, above the 70% minimum. |

Observed TS coverage summaries included:

- contracts: 98.03% lines and 86.96% functions;
- utils: 96.01% lines and 95.00% functions;
- web: 91.62% lines and 89.84% functions;
- api-clients and Shikiho extension: their newly required LCOV files were present and passed the repository threshold checker.

### Retired surface and active-guidance searches

Fresh active-source searches confirmed all of the following:

- `apps/ts/packages/utils/src/utils/dataset-paths.ts` and its test no longer exist;
- active non-test TypeScript contains none of `DEFAULT_DATASET_DIR`, `getDefaultDatasetDir`, `ensureDatasetDir`, `resolveDatasetPath`, `ensureDbExtension`, `normalizeDatasetPath`, `getMarketDbPath`, `getPortfolioDbPath`, or `getDatasetPath`;
- active source contains no `@trading25/utils/utils/...` deep import;
- `@trading25/utils` no longer declares the `./utils/*` wildcard export;
- active AGENTS and skill guidance contains no unsupported `promote-retained` source/force/copy/J-Quants option form and no language that reclassifies full-rebuild `market-cutover cutover` as canonical retained promotion.

The strict skill audit provides a second executable check over the repository-specific legacy and retained-promotion rules. Historical plans and specifications remain excluded from active-contract bans, as required by the design.

### Secret scanning

Gitleaks 8.30.1 was downloaded from the official release and verified against its published Darwin arm64 archive checksum before use:

```text
b40ab0ae55c505963e365f271a8d3846efbc170aa17f2607f13df610a9aeb6a5
```

The canonical local scans passed:

- current tracked tree, reconstructed with `git archive HEAD` and scanned from its repository-relative root with `.gitleaks.toml`;
- Git event range from the merge base through final branch `HEAD`, using `gitleaks git` and the repository configuration.

An exploratory raw-worktree directory scan saw two ignored pytest bytecode files containing synthetic fixture material. That result was not treated as a pass and did not change the allowlist. Scanning the tracked tree from its relative root correctly exercised the narrow path/rule/line allowlist and passed. Ignored caches are not part of the commit or event range.

## Final diff and artifact audit

- The worktree was clean before Task 7 changes, so there were no pre-existing user edits to overwrite or absorb.
- `git diff --check` passed.
- `git status --short --branch` showed no uncommitted tracked files before this report was added.
- The pre-report branch delta was 85 files and matched the planned areas: CI/workflow tests, taxonomy, TypeScript utils removal/build/coverage controls, gitleaks configuration, skill audit/catalog/guidance, AGENTS guidance, governance evidence, and maintainability artifacts.
- No dependency lockfile, OpenAPI contract, generated TS contract, runtime trading implementation, or local user data changed during the audit.
- Build, test, and coverage products remained ignored; no accidental generated artifact was staged.
- The final report itself is the only new Task 7 deliverable. Final privacy, skill, maintainability, diff, tracked-tree gitleaks, and Git-range gitleaks checks are run again after adding it.

## Environment-only limitation

Docker was unavailable in the local verification environment, so the workflow's exact digest-pinned OCI invocation could not be executed locally. This did not weaken completion criteria:

- workflow unit tests passed and verify the immutable image digest, version pin, Git-aware command, full-history checkout, least-privilege permissions, and disabled checkout credentials;
- the same pinned gitleaks version was executed locally as an official checksum-verified release binary;
- both the tracked current tree and the relevant Git event range were scanned with the repository configuration and passed.

No other mandatory verification was skipped.
