# Repository Guidance Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make current repository guidance executable and Market v5-safe, fix the exposed watchlist validation bug, and move P3 cleanup into GitHub Issues.

**Architecture:** Root `AGENTS.md` remains the cross-project SoT; child guidance contains local deltas; READMEs provide executable entry points; skills encode current workflows; living research surfaces reject pre-v5 evidence. Work is split into focused documentation, skill-contract, and UI validation gates.

**Tech Stack:** Markdown, Python guard scripts, React 19, TypeScript, Vitest, Bun, GitHub CLI.

## Global Constraints

- Fix P0-P2 now; create GitHub Issues for P3 and lower findings.
- Do not delete or bulk-move historical artifacts.
- Label retained Market v3/v4 material non-current and rerun-required.
- Do not restart the full suite for an isolated failure.
- Preserve unrelated changes and stage exact in-scope paths.

---

### Task 1: Invalidate Pre-v5 Living Research Guidance

**Files:**
- Modify: `apps/bt/docs/experiments/market-behavior/pre-disclosure-flow-volatility/README.md`
- Modify: `apps/bt/docs/experiments/market-behavior/annual-first-open-last-close-fundamental-panel/README.md`
- Modify: `apps/bt/docs/experiments/market-behavior/ranking-fixed-return-priority-evidence/README.md`
- Modify: `apps/bt/docs/experiments/market-behavior/ranking-trend-acceleration-conditional-lift/README.md`
- Modify: `apps/bt/docs/experiments/market-behavior/ranking-technical-fit-score-shape-evidence/README.md`
- Modify: `apps/bt/docs/experiments/README.md`

**Interfaces:**
- Consumes: Market v5 rules from root `AGENTS.md` and `docs/research-pit-invalidation-register.md`.
- Produces: readouts classified as historical and rerun-required.

- [ ] Add a prominent banner to each affected readout stating that Market v3/v4 evidence must not drive production, thresholds, or ranking decisions before a Market v5 `provider_adjusted_v1` rerun.
- [ ] Change current/adopted language to historical candidate language while preserving measurements, dates, and provenance.
- [ ] Mark affected experiment-index entries `historical_archive` / `rerun_required` and remove obsolete “published v11” identity.
- [ ] Run `python scripts/check-research-guardrails.py` and targeted searches. Expect the guard to pass and every pre-v5 claim to be adjacent to invalidation text.

---

### Task 2: Correct AGENTS and Skill Contracts

**Files:**
- Modify: `AGENTS.md`
- Modify: `apps/bt/AGENTS.md`
- Modify: `apps/ts/AGENTS.md`
- Modify: `.codex/skills/ts-financial-analysis/SKILL.md`
- Modify: `.codex/skills/bt-financial-analysis/SKILL.md`
- Modify: `.codex/skills/bt-market-sync-strategies/SKILL.md`
- Modify: `.codex/skills/trading25-dependabot-maintenance/SKILL.md`
- Modify: `.codex/skills/ts-portfolio-management/SKILL.md`

**Interfaces:**
- Consumes: current source behavior and OpenAPI contracts.
- Produces: safe current workflows for agents and maintainers.

- [ ] Remove deleted `src/api/client.py`; describe middleware runtime order as `CORSMiddleware -> CorrelationIdMiddleware -> RequestLoggerMiddleware`, verified through `app.user_middleware`.
- [ ] Replace unshipped strategy examples with `buy_and_hold` / `sma_cross`, or explicitly state the XDG prerequisite. Mark TS references to root scripts as repository-root commands.
- [ ] Replace `priceBasisDate` with `fundamentalsAdjustmentBasisDate`; keep calculation SoT in `src/domains/*` and services as I/O/orchestration.
- [ ] Correct auto sync: when `last_sync_date` is absent, inspect DuckDB; choose incremental for a non-empty snapshot and initial for an empty snapshot.
- [ ] Make Dependabot maintenance record its initial local/remote boundary, push only current-batch commits, and preserve pre-existing dirty/ahead state.
- [ ] Describe watchlist codes with backend pattern `^\d[0-9A-Z]\d[0-9A-Z]$`, including `7203` and `130A`.
- [ ] Run strict skill audit, generated-reference freshness check, and targeted stale-text searches. Expect no living stale instruction.

---

### Task 3: Repair README, Contract, Strategy, and Runbook Entry Points

**Files:**
- Modify: `README.md`
- Modify: `apps/bt/README.md`
- Modify: `docs/ts-cli-scope.md`
- Modify: `contracts/README.md`
- Modify: `docs/README.md`
- Modify: `apps/bt/docs/README.md`
- Replace content: `apps/bt/docs/strategies.md`

**Interfaces:**
- Consumes: current CLI help, shipped strategies, Market v5 runbook, strict OpenAPI source export.
- Produces: executable navigation and strategy guidance.

- [ ] Retain prepared corrections for TS packages, GitHub Issues, non-price-only repair, and repository-root test commands.
- [ ] State that `bt:sync` fails on source-export failure and never uses a running server or stale snapshot; retain `bt:generate-offline` as the explicit snapshot-only path.
- [ ] Link `docs/runbooks/market-v5-cutover.md` from root README, docs index, and bt docs index as the only supported pre-v5-to-v5 operator path.
- [ ] Replace the stale strategy guide with: Sources of truth; Shipped examples; List/validate/run; YAML/backend validation; Signal registry/metadata; Optimization block; Adding a strategy or signal; Verification.
- [ ] Use only current `bt list`, `bt validate`, `bt backtest`, and `bt migrate-optimization-specs`; do not publish fixed strategy counts or performance multipliers.
- [ ] Run a local Markdown-link checker across root, apps, docs, contracts, and skills. Expect zero missing targets.

---

### Task 4: Accept Alphanumeric Watchlist Codes with TDD

**Files:**
- Modify: `apps/ts/packages/web/src/components/Watchlist/WatchlistDetail.test.tsx`
- Modify: `apps/ts/packages/web/src/components/Watchlist/WatchlistDetail.tsx`

**Interfaces:**
- Consumes: `normalizeStockCode(code)` and backend pattern `^\d[0-9A-Z]\d[0-9A-Z]$`.
- Produces: add behavior accepting `7203` and `130A`, rejecting free text and invalid shapes.

- [ ] Add a failing test that types lowercase `130a`, submits, and expects payload `{ code: '130A', companyName: '130A' }`.
- [ ] Run `cd apps/ts && bun run --filter @trading25/web test -- src/components/Watchlist/WatchlistDetail.test.tsx`. Expect RED because Add is disabled.
- [ ] Replace `/^\d{4}$/` with `/^\d[0-9A-Z]\d[0-9A-Z]$/` against `normalizedCode`.
- [ ] Rerun the focused test, web typecheck, and focused Biome check. Expect all to pass.

---

### Task 5: Create P3 Issues, Verify, Commit, and Push

**Files:**
- External state: six grouped GitHub Issues from the approved design.

**Interfaces:**
- Consumes: P3 findings and final diffs.
- Produces: Issue URLs, verified scoped commits, and pushed `main`.

- [ ] Create issues for index/history drift, closed roadmap items, old doc paths, semantic freshness metadata, Dependabot skill size/history, and external guideline offline verification. Include affected paths and acceptance criteria; do not implement P3 docs.
- [ ] Run skill audit, generated-reference check, research guard, privacy check, focused watchlist test, Markdown-link checker, and `git diff --check`. Expect all exit 0.
- [ ] Confirm every P1/P2 has a change and every P3 has an Issue URL.
- [ ] Commit coherent groups by staging exact paths for research, guidance/skills, watchlist, and `.gitignore` cleanup.
- [ ] Fetch first, inspect remote movement, require a safe integration, and push verified commits to `origin/main`.
