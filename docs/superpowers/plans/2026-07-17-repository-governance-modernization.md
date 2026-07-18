# Repository Governance Modernization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make CI, repository skills, and governance enforce the current FastAPI-only, Market v4, and TypeScript workspace architecture after the large refactor.

**Architecture:** Extend existing repository-owned taxonomy, audit, build, coverage, and security gates instead of adding parallel systems. Remove unused legacy APIs, make active documentation executable and current, and add regression tests for every previously identified gap.

**Tech Stack:** Python 3.12, pytest, GitHub Actions, Bash, Bun 1.3, TypeScript 6, Vitest/Bun test, gitleaks, Codex SKILL.md.

## Global Constraints

- Preserve the user's unrelated `.gitignore` change.
- FastAPI on port 3002 remains the only backend.
- TypeScript must not access DuckDB, SQLite, or dataset files directly.
- Market v4 retained promotion uses `bt market-cutover promote-retained REPORT_ID --retained-report-id ... --backup-id ... --symbol ... --strategy ...`; full rebuild remains `bt market-cutover cutover`.
- Unknown changed paths continue to fail closed into product CI.
- Security, build, test, and coverage gates may not be weakened to gain speed.

---

### Task 1: Complete CI test selection

**Files:**
- Modify: `scripts/ci/test_targets.py`
- Modify: `scripts/ci/test_taxonomy.py`
- Test: `apps/bt/tests/unit/scripts/test_test_targets.py`
- Test: `apps/bt/tests/unit/scripts/test_ci_changed_scope.py`

**Interfaces:**
- Consumes: existing `TARGET_GROUPS` and `classify_changed_paths()`.
- Produces: complete production pytest groups and explicit classification for `domains/lab_agent` and HTTP-backed analytics modules.

- [ ] Add failing assertions that `tests/unit/contracts`, `tests/unit/domains/fundamentals`, `tests/unit/domains/strategy/runtime`, and CI self-tests are selected.
- [ ] Add failing scope assertions for `domains/lab_agent` and `market_bubble_footprint_monitor.py`.
- [ ] Run `uv run --directory apps/bt pytest tests/unit/scripts/test_test_targets.py tests/unit/scripts/test_ci_changed_scope.py -q` and confirm the new assertions fail.
- [ ] Extend the canonical groups and taxonomy with the exact missing paths.
- [ ] Re-run the focused tests and all newly included suites.

### Task 2: Enforce production builds and complete coverage

**Files:**
- Modify: `.github/workflows/ci.yml`
- Modify: `apps/ts/scripts/check-coverage.ts`
- Test: `apps/bt/tests/unit/scripts/test_ci_workflow.py`
- Test: `apps/ts/scripts/dependency-audit.coverage.test.ts`

**Interfaces:**
- Consumes: `bun run workspace:build`, `bun run extension:build`, package LCOV outputs.
- Produces: required build steps and thresholds for contracts, utils, api-clients, extension, and web.

- [ ] Add workflow assertions requiring both production build commands before tests complete.
- [ ] Add coverage-gate assertions for api-clients and Shikiho LCOV paths and thresholds.
- [ ] Run the Python and Bun focused tests and confirm failure.
- [ ] Add build steps and complete `thresholds`/`coverageFiles` entries.
- [ ] Run focused tests, `bun run workspace:build`, and `bun run extension:build`.

### Task 3: Remove retired TypeScript Data Plane APIs

**Files:**
- Delete: `apps/ts/packages/utils/src/utils/dataset-paths.ts`
- Delete: `apps/ts/packages/utils/src/utils/dataset-paths.test.ts`
- Modify: `apps/ts/packages/utils/src/index.ts`
- Modify: `apps/ts/scripts/dependency-audit.test.ts`

**Interfaces:**
- Consumes: current FastAPI-only architecture.
- Produces: a utils package with no database or dataset filesystem API.

- [ ] Add a failing source audit asserting the retired module and public exports do not exist.
- [ ] Run `bun test scripts/dependency-audit.test.ts` and confirm failure.
- [ ] Remove the module, tests, and exports.
- [ ] Re-run utils/root tests, dependency audit, and typecheck.

### Task 4: Harden secret scanning and workflow reproducibility

**Files:**
- Modify: `.github/workflows/ci.yml`
- Modify: `.github/workflows/nautilus-smoke.yml`
- Modify: `.gitleaks.toml`
- Modify: `apps/bt/tests/unit/scripts/test_ci_workflow.py`
- Modify: `apps/bt/tests/unit/scripts/test_ci_changed_scope.py`

**Interfaces:**
- Consumes: GitHub checkout history and repository gitleaks configuration.
- Produces: always-required Git-aware secret/privacy scanning, least-privilege workflows, immutable action references, fixed tool versions, and scoped Nautilus execution.

- [ ] Add failing workflow tests for docs-only security checks, `permissions: contents: read`, `persist-credentials: false`, Git-aware gitleaks, pinned uv/Bun/gitleaks, and Nautilus path scope.
- [ ] Run the workflow tests and confirm failure.
- [ ] Add a narrow gitleaks allowlist for the synthetic indicator registry key.
- [ ] Implement the workflow hardening without suppressing real findings.
- [ ] Re-run workflow tests and a local gitleaks-equivalent scan.

### Task 5: Modernize repository skills and skill audit

**Files:**
- Modify: `scripts/skills/audit_skills.py`
- Modify: `apps/bt/tests/unit/scripts/test_audit_skills.py`
- Modify: `.codex/skills/*/SKILL.md`
- Refresh: `.codex/skills/ts-vercel-react-best-practices/rules/*.md`
- Modify: `.codex/skills/ts-vercel-react-best-practices/AGENTS.md`

**Interfaces:**
- Consumes: current skill catalog rules and installed bundled React catalog.
- Produces: discovery-compliant frontmatter, executable verification guidance, retained-promotion contract, and detectable upstream rule drift.

- [ ] Add failing audit tests for `Use when...`, name/frontmatter constraints, non-placeholder verification commands, retired Data Plane surfaces, and React catalog provenance/file inventory.
- [ ] Run `uv run --directory apps/bt pytest tests/unit/scripts/test_audit_skills.py -q` and confirm failure.
- [ ] Update workflow skills and refresh the React catalog from the installed source.
- [ ] Replace the unpinned remote design-guideline source with pinned provenance.
- [ ] Re-run the skill audit and focused tests.

### Task 6: Refresh AGENTS and active contract guidance

**Files:**
- Modify: `apps/bt/AGENTS.md`
- Modify: `apps/ts/AGENTS.md`
- Modify: `.codex/skills/bt-market-sync-strategies/SKILL.md`
- Modify: `.codex/skills/bt-database-management/SKILL.md`
- Modify: `.codex/skills/bt-strategy-config/SKILL.md`
- Modify: `.codex/skills/ts-financial-analysis/SKILL.md`
- Modify: `.codex/skills/ts-dataset-management/SKILL.md`

**Interfaces:**
- Consumes: `pyproject.toml`, `package.json`, OpenAPI, CLI implementation, and root AGENTS contract.
- Produces: active guidance with correct versions, categories, request/response semantics, and retained-promotion recovery rules.

- [ ] Add or extend semantic audit assertions for the corrected contracts.
- [ ] Update the documents, preferring stable symbols and paths over volatile line numbers/counts.
- [ ] Run skill audit, OpenAPI contract check, and documentation searches for banned active guidance.

### Task 7: Full verification and completion audit

**Files:**
- Verify all modified files and generated references.

**Interfaces:**
- Consumes: deliverables from Tasks 1-6.
- Produces: evidence that every audited gap is closed.

- [ ] Run repository skill audit, privacy check, maintainability check, lint, typecheck, contract sync, TS builds/tests/coverage, BT newly selected tests, and workflow/taxonomy tests.
- [ ] Search active source for retired TypeScript database helpers and stale Market cutover guidance.
- [ ] Inspect `git diff --check`, `git status --short`, and the final diff for accidental generated or user-file changes.
- [ ] Record any environment-only limitation without weakening the completion criteria.
