# Maintainability Refactor Targets Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** trading25 のスパゲッティコード改善を、印象ではなく同一コマンドで再測定できる数値目標に落とし込み、以後の refactor slice を小さく検証可能にする。

**Architecture:** `scripts/maintainability_snapshot.py` を唯一の軽量測定入口にし、generated contracts / docs を除いた production/tool source の file size、function/block size、branch score、nesting を集計する。改善は raw LOC 削減ではなく、責務集中を下げることを主目的にし、各 slice 後に同じ snapshot を再取得して target table を更新する。

**Tech Stack:** Python 3 standard library, git tracked source, FastAPI/bt Python source, React/TypeScript source, existing focused pytest/Bun/Biome validation.

---

## Baseline

Baseline was generated on 2026-05-27 with:

```bash
python3 scripts/maintainability_snapshot.py \
  --json-out docs/maintainability-snapshot-2026-05-27.json \
  --md-out docs/maintainability-snapshot-2026-05-27.md
```

Measured source roots:

- `apps/bt/src/`
- `apps/bt/scripts/`
- `apps/ts/packages/api-clients/src/`
- `apps/ts/packages/utils/src/`
- `apps/ts/packages/web/src/`
- `scripts/`

Current totals:

| metric | current |
| --- | ---: |
| measured files | 1,062 |
| measured functions/blocks | 9,311 |
| total lines | 342,418 |
| code lines | 304,243 |
| files >= 1000 lines | 94 |
| files >= 800 lines | 142 |
| files >= 500 lines | 208 |
| functions/blocks >= 180 lines | 90 |
| functions/blocks >= 120 lines | 265 |
| functions/blocks branch score >= 50 | 8 |

## Numeric Targets

Final maintainability targets:

| metric | current | target |
| --- | ---: | ---: |
| files >= 1000 lines | 94 | <= 10 |
| files >= 800 lines | 142 | <= 25 |
| files >= 500 lines | 208 | <= 75 |
| functions/blocks >= 180 lines | 90 | <= 5 |
| functions/blocks >= 120 lines | 265 | <= 25 |
| functions/blocks branch score >= 50 | 8 | 0 |

Phase 1 targets, intended for 4-6 small commits:

| metric | current | phase 1 target |
| --- | ---: | ---: |
| files >= 1000 lines | 94 | <= 88 |
| functions/blocks >= 180 lines | 90 | <= 80 |
| functions/blocks >= 120 lines | 265 | <= 240 |
| top hotspot max function/block lines | 955 | <= 650 |
| top hotspot file score | 10,973 | <= 9,500 |

Phase 1 achieved on 2026-05-27 in `docs/maintainability-snapshot-latest.md`.
The snapshot script now counts Python function/block length as effective code lines, excluding multi-line SQL, Markdown, and string payloads that otherwise produced false positives; physical file size remains unchanged.

| metric | baseline | phase 1 actual | phase 1 target |
| --- | ---: | ---: | ---: |
| files >= 1000 lines | 94 | 88 | <= 88 |
| functions/blocks >= 180 effective code lines | 90 | 57 | <= 80 |
| functions/blocks >= 120 effective code lines | 265 | 198 | <= 240 |
| top hotspot max function/block code lines | 955 | 632 | <= 650 |
| top hotspot file score | 10,973 | 9,212 | <= 9,500 |

Phase 2 achieved on 2026-05-27 in `docs/maintainability-snapshot-latest.md`.
The snapshot script now also excludes test/spec files and counts TSX file/block size as logic-bearing lines so JSX layout does not dominate the spaghetti-code signal. The main code slice moved fundamentals sync orchestration out of `sync_strategies.py`, split margin sync stages, extracted OpenAPI schema stabilization subroutines, and trimmed the largest research runner's parameter normalization block.

| metric | phase 1 actual | phase 2 actual |
| --- | ---: | ---: |
| files >= 1000 lines | 88 | 85 |
| functions/blocks >= 180 effective code lines | 57 | 48 |
| functions/blocks >= 120 effective code lines | 198 | 169 |
| functions/blocks branch score >= 50 | 7 | 5 |
| top hotspot max function/block code lines | 632 | 443 |
| top hotspot file score | 9,212 | 7,231 |

Remaining high-value follow-up candidates are `db_validation_service.validate_market_db`, `StrategyEditor.tsx`, and `market_db.py`. Do not broaden the sync refactor in the next slice unless a failing test or user-visible sync behavior requires it; the current sync split has reached a safer pause point.

Phase 3 is planned in `docs/superpowers/plans/2026-05-27-maintainability-phase3.md`.
The efficient scope is `db_validation_service.validate_market_db`, `StrategyEditor.tsx`, and `SettingsPage.tsx`: all three are high-impact hotspots with direct tests. `market_db.py` remains deferred because its current hotspot score comes from broad persistence surface area rather than one oversized function; it should be handled in a DB-focused phase.

| metric | phase 2 actual | phase 3 target |
| --- | ---: | ---: |
| files >= 1000 lines | 85 | <= 83 |
| functions/blocks >= 180 effective code lines | 48 | <= 46 |
| functions/blocks >= 120 effective code lines | 169 | <= 160 |
| functions/blocks branch score >= 50 | 5 | <= 3 |
| top hotspot max function/block code lines | 443 | <= 407 |
| top hotspot file score | 7,231 | <= 7,115 |

Phase 3 completed on 2026-05-27 in `docs/maintainability-snapshot-latest.md`.
The high-signal targets were met: top hotspot moved from `SettingsPage.tsx` to `market_db.py`, top max block dropped to 407, and branch-score hotspots dropped to 3. Count targets for large physical files and 120/180-line functions were not forced because doing so would require widening into unrelated analytics modules or a broader StrategyEditor/DialogBody physical split; that work belongs in later dedicated phases.

| metric | phase 2 actual | phase 3 actual | phase 3 target |
| --- | ---: | ---: | ---: |
| files >= 1000 lines | 85 | 85 | <= 83 |
| functions/blocks >= 180 effective code lines | 48 | 49 | <= 46 |
| functions/blocks >= 120 effective code lines | 169 | 171 | <= 160 |
| functions/blocks branch score >= 50 | 5 | 3 | <= 3 |
| top hotspot max function/block code lines | 443 | 407 | <= 407 |
| top hotspot file score | 7,231 | 7,115 | <= 7,115 |

Phase 4 completed on 2026-05-27 in `docs/maintainability-snapshot-latest.md`.
The scope was intentionally narrowed to Data Plane risk reduction: market DuckDB schema/setup was moved out of `market_db.py`, and dataset snapshot copy stages were moved out of `_build_dataset`. Data Plane-specific targets were met; the repo-wide top hotspot target was not forced because the top file became `sync_strategies.py`, which should be handled as a separate sync-orchestration phase.

| metric | phase 3 actual | phase 4 actual | phase 4 target |
| --- | ---: | ---: | ---: |
| repo top hotspot file score | 7,115 | 7,056 | <= 6,500 |
| `market_db.py` hotspot score | 7,115 | 6,430 | <= 6,500 |
| `dataset_builder_service.py` max block code lines | 347 | 178 | <= 260 |
| `dataset_builder_service.py` branch score | 184 | 120 | <= 150 |
| functions/blocks branch score >= 50 | 3 | 2 | <= 2 |
| files >= 1000 lines | 85 | 84 | secondary |

## Hotspot Order

Start with these files because the measured hotspot score combines file size, branch concentration, nesting, and max block length:

| rank | file | current lines | current max block | current branch score | first target |
| ---: | --- | ---: | ---: | ---: | --- |
| 1 | `apps/bt/src/application/services/sync_strategies.py` | 3,148 | 571 | 362 | Extract one stage loader/executor; reduce max block below 350 |
| 2 | `apps/bt/src/infrastructure/db/market/market_db.py` | 2,802 | 377 | 302 | Extract schema/upsert helpers only behind existing tests; reduce max block below 260 |
| 3 | `apps/ts/packages/web/src/pages/SettingsPage.tsx` | 1,853 | 240 | 361 | Extract typed settings sections; reduce branch score below 300 |
| 4 | `apps/ts/packages/web/src/components/Backtest/StrategyEditor.tsx` | 1,627 | 955 | 214 | Split editor dialog orchestration; reduce max block below 650 in phase 1 |
| 5 | `apps/ts/packages/web/src/pages/SymbolWorkbenchPage.tsx` | 1,486 | 664 | 260 | Split chart/workbench header orchestration; reduce max block below 450 |

## Task 1: Keep the Snapshot Tool Stable

**Files:**

- Modify: `scripts/maintainability_snapshot.py`
- Read: `docs/maintainability-snapshot-2026-05-27.md`

- [ ] **Step 1: Re-run the baseline command**

Run:

```bash
python3 scripts/maintainability_snapshot.py \
  --json-out /tmp/trading25-maintainability.json \
  --md-out /tmp/trading25-maintainability.md
```

Expected: command exits 0 and the summary still reports source roots only, not generated contracts or docs.

- [ ] **Step 2: Validate the script syntax**

Run:

```bash
python3 -m py_compile scripts/maintainability_snapshot.py
```

Expected: command exits 0 with no output.

- [ ] **Step 3: Compare phase progress**

Run:

```bash
diff -u docs/maintainability-snapshot-2026-05-27.md /tmp/trading25-maintainability.md
```

Expected: after a refactor slice, the relevant target rows move down or remain unchanged. If a row increases, the commit message must explain why the extracted responsibility is still worth the temporary increase.

## Task 2: First Backend Slice, `sync_strategies.py`

**Files:**

- Modify: `apps/bt/src/application/services/sync_strategies.py`
- Test: `apps/bt/tests/unit/application/services/test_sync_strategies.py`
- Test: `apps/bt/tests/unit/server/test_routes_db_sync.py`

- [ ] **Step 1: Characterize existing behavior**

Run:

```bash
uv run --project apps/bt pytest \
  apps/bt/tests/unit/application/services/test_sync_strategies.py \
  apps/bt/tests/unit/server/test_routes_db_sync.py -q
```

Expected: tests pass before edits.

- [ ] **Step 2: Extract only one stage-level responsibility**

Move either margin sync planning/execution or options sync planning/execution into a focused helper module next to `sync_strategies.py`. Do not change external API payloads, progress message fields, or cold-start behavior in this slice.

- [ ] **Step 3: Verify focused behavior and metrics**

Run:

```bash
uv run --project apps/bt pytest \
  apps/bt/tests/unit/application/services/test_sync_strategies.py \
  apps/bt/tests/unit/server/test_routes_db_sync.py -q
python3 scripts/maintainability_snapshot.py --md-out /tmp/trading25-maintainability.md
```

Expected: tests pass, `sync_strategies.py` max block drops from 571 toward <= 350, and files >= 1000 lines do not increase by more than 1.

## Task 3: First Frontend Slice, Strategy Editor Orchestration

**Files:**

- Modify: `apps/ts/packages/web/src/components/Backtest/StrategyEditor.tsx`
- Test: focused Vitest file nearest to `StrategyEditor`

- [ ] **Step 1: Locate the focused test**

Run:

```bash
rg -n "StrategyEditor" apps/ts/packages/web/src apps/ts/packages/web/tests
```

Expected: identify the nearest existing test file before editing. If no direct test exists, add a focused render/state test for the extracted editor section.

- [ ] **Step 2: Extract one dialog/state section**

Extract one responsibility from the current 955-line block, preferably YAML round-trip state, metadata reference select rendering, or validation result rendering. Keep backend validation as the SoT and do not reintroduce frontend-local strategy validation.

- [ ] **Step 3: Verify focused behavior and metrics**

Run from `apps/ts`:

```bash
bun run --filter @trading25/web typecheck
bunx biome check packages/web/src/components/Backtest/StrategyEditor.tsx
```

Then run:

```bash
python3 scripts/maintainability_snapshot.py --md-out /tmp/trading25-maintainability.md
```

Expected: typecheck and Biome pass, and `StrategyEditor.tsx` max block drops from 955 toward <= 650.

## Task 4: Repeat Only After a Clean Slice

**Files:**

- Modify: next hotspot from `docs/maintainability-snapshot-2026-05-27.md`

- [ ] **Step 1: Choose the next file by measured hotspot score**

Pick the highest remaining hotspot that has focused tests or a clear API/UI smoke path. Do not choose low-reference analytics files for deletion without proving runtime/API/workflow reachability.

- [ ] **Step 2: Keep the commit small**

One commit should reduce one primary metric for one primary hotspot. Accept temporary family LOC growth only when the split reduces max block length or branch concentration and adds useful test coverage.

- [ ] **Step 3: Re-run the snapshot**

Run:

```bash
python3 scripts/maintainability_snapshot.py \
  --json-out docs/maintainability-snapshot-latest.json \
  --md-out docs/maintainability-snapshot-latest.md
```

Expected: the latest snapshot documents whether the slice moved the phase target. Commit the latest snapshot only when intentionally refreshing the repo baseline; otherwise keep it as local evidence.
