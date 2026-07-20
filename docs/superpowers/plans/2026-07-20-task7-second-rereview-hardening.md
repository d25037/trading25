# Task 7 Second Re-review Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the five remaining Daily Ranking builder review findings without weakening event-time, provenance, or compatibility guarantees.

**Architecture:** Extend the architecture scanner's binding model, restore legacy sector breadth denominator behavior while retaining null overlays, align ROE alias validation with the filtered FY natural key, replace Python row streaming with bounded DB-side commutative fingerprints plus synchronous validation scopes, and make legacy cleanup use exact prefix matching. Production edits remain limited to the Daily Ranking base/builder modules; regression tests remain in their existing focused and architecture suites.

**Tech Stack:** Python 3.12, DuckDB, pytest, AST, Ruff, Pyright.

## Global Constraints

- Write and observe a failing focused test before each production behavior change.
- `RelationRef` remains exact-object, exact-connection, immutable-provenance, and physical-OID bound across calls.
- Fingerprints transfer a fixed number of aggregate rows independent of relation size and contain no `ORDER BY`.
- PSR/ROE remain exact `valuation_basis_id` consumers with no latest/current fallback.
- Sector output preserves every source key and leaves unavailable per-key features NULL.
- Compatibility cleanup preserves unrelated similarly named temporary tables.
- Keep `.superpowers/sdd/task-7-report.md` ignored and uncommitted.

---

### Task 1: AST dotted-import binding and rebind ratchet

**Files:**
- Modify: `apps/bt/tests/unit/architecture/test_layer_boundaries.py`

**Interfaces:**
- Consumes: `_scan_daily_ranking_private_edges(tree, importer_module, experiment_modules)`
- Produces: full dotted-prefix module bindings and rebind-sensitive edge/call-scope tuples

- [x] Add a synthetic RED case for `import src.domains.analytics.synthetic_private_owner` followed by `src.domains.analytics.synthetic_private_owner._private()`.
- [x] Add paired pre/post-rebind cases proving call scopes disappear or move when the bound name is replaced or aliased.
- [x] Run the exact synthetic tests and confirm the dotted case fails before the scanner change.
- [x] Preserve the full imported dotted module as a binding and resolve each prefix attribute until the private symbol is reached.
- [x] Run the synthetic tests and full architecture file; recompute the pinned inventory only if real repository edges change.

### Task 2: Sector mixed-completeness breadth semantics

**Files:**
- Modify: `apps/bt/src/domains/analytics/daily_ranking_feature_builders.py`
- Modify: `apps/bt/tests/unit/domains/analytics/test_daily_ranking_feature_builders.py`

**Interfaces:**
- Consumes: `build_sector_strength_features`
- Produces: legacy `AVG(CASE positive THEN 1.0 ELSE 0.0 END)` denominator semantics with complete sector-state gating

- [x] Add fixture data with one sector/date containing one complete positive constituent and one missing-return constituent.
- [x] Assert every source key remains present, the missing constituent's own sector features remain NULL, and the complete row's literal breadth/ranks/scores/buckets use the full two-member denominator.
- [x] Run the focused sector test and observe the breadth mismatch RED.
- [x] Restore `ELSE 0.0` inside the sector aggregate while deriving availability separately and joining the completed sector state only to eligible source rows.
- [x] Run focused sector and sector-owner tests GREEN.

### Task 3: ROE FY alias natural key

**Files:**
- Modify: `apps/bt/src/domains/analytics/daily_ranking_feature_builders.py`
- Modify: `apps/bt/tests/unit/domains/analytics/test_daily_ranking_feature_builders.py`

**Interfaces:**
- Consumes: `statement_metrics_adjusted`
- Produces: alias consistency over FY-only `(code,basis_version,disclosed_date,period_end,period_type)` rows

- [x] Add RED cases where identical 4/5-digit aliases have both FY and quarterly rows and must be accepted, plus conflicting FY rows for the same exact period that must fail.
- [x] Run focused ROE tests and observe the mixed-period false conflict.
- [x] Extend alias consistency with a validated static predicate and include `period_type` in the natural key; call it with `upper(coalesce(period_type,'')) = 'FY'`.
- [x] Run focused ROE and full ROE-owner tests GREEN.

### Task 4: Bounded DB-side fingerprints and synchronous validation scope

**Files:**
- Modify: `apps/bt/src/domains/analytics/daily_ranking_research_base.py`
- Modify: `apps/bt/src/domains/analytics/daily_ranking_feature_builders.py`
- Modify: `apps/bt/tests/unit/domains/analytics/test_daily_ranking_feature_builders.py`

**Interfaces:**
- Consumes: `_assert_ref_current`, public builder validation/publication calls
- Produces: fixed-row aggregate fingerprints and a private synchronous validation token/handle

- [x] Add an instrumented RED scale test for 1x/2x data asserting a fixed currentness-query count, fixed aggregate result-row count, no fingerprint `ORDER BY`, and one validation each for sector source/population.
- [x] Preserve forge/copy/mutation/drop-recreate regressions in the same run.
- [x] Replace Python `fetchmany` row streaming with one DuckDB aggregate row containing count plus at least two independent commutative row-hash aggregates for keys and full content.
- [x] Add a private context-local validation scope whose token caches only exact ref identities for one synchronous builder/publication call; a new public call creates a new scope and revalidates staleness.
- [x] Ensure publication reuses the source validation and sector source/population are each validated once.
- [x] Run focused security/scale tests GREEN at both sizes.

### Task 5: Exact legacy cleanup prefix

**Files:**
- Modify: `apps/bt/src/domains/analytics/daily_ranking_feature_builders.py`
- Modify: `apps/bt/tests/unit/domains/analytics/test_daily_ranking_feature_builders.py`

**Interfaces:**
- Consumes: `_legacy_intermediate_names`
- Produces: exact `str.startswith('legacy_feature_g_')` cleanup selection

- [x] Add an unrelated temporary table whose name matches wildcard underscores but not the literal prefix; assert success and failure wrappers preserve it.
- [x] Run the wrapper test and observe deletion RED.
- [x] Query all temporary table names and filter with exact Python prefix matching.
- [x] Run repeat/failure/atomic cleanup tests GREEN.

### Task 6: Verification, report, and commit

**Files:**
- Append without staging: `.superpowers/sdd/task-7-report.md`

**Interfaces:**
- Consumes: all five completed fixes
- Produces: one reviewed commit and clean tracked worktree

- [x] Run focused builders/security/parity and full architecture.
- [x] Run full sector and ROE owner files; run any other affected owner subset needed by fingerprint/cleanup changes, then the required owner gate.
- [x] Run Ruff on every modified Python file, Pyright on modified source, research guardrails, and `git diff --check`.
- [x] Append exact RED/GREEN evidence and final command results to the ignored report.
- [x] Stage only intended tracked files and commit with a scoped `fix(bt): ...` message.
