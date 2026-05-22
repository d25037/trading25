# Ranking Valuation Coloring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and apply research-backed, percentile-based coloring for `PER`, `Fwd PER`, `Fwd P/OP`, `PBR`, and `流動性Z` on Ranking page `Individual Stocks`.

**Updated direction:** Do not extend the heavy recent-return threshold runner for this UI task. Use the dedicated fast `ranking-color-evidence` runner, because the UI evidence only needs `stock_data`, `topix_data`, `stock_master_daily`/`stocks`, and `daily_valuation`. Color-tier judgment is Prime-only for the first production pass.

---

## Files

- Added: `apps/bt/src/domains/analytics/ranking_color_evidence.py`
  - Fast daily panel using `daily_valuation` as valuation SoT.
  - Emits `ranking_color_evidence_df`, `forward_per_pop_interaction_df`, and `liquidity_regime_evidence_df`.
- Added: `apps/bt/scripts/research/run_ranking_color_evidence.py`
  - Bundle runner for `market-behavior/ranking-color-evidence`.
- Added: `apps/bt/tests/unit/domains/analytics/test_ranking_color_evidence.py`
  - Proves the fast path does not require `statements`.
- Added: `apps/bt/docs/experiments/market-behavior/ranking-color-evidence/README.md`
  - Canonical published readout.
- Modified: `apps/bt/docs/experiments/research-catalog-metadata.toml`
  - Adds research catalog metadata.
- Modify next: `apps/bt/src/entrypoints/http/schemas/ranking.py`
  - Add optional valuation percentile fields.
- Modify next: `apps/bt/src/application/services/ranking_service.py`
  - Compute target-date market-relative valuation percentiles.
- Modify next: TS contracts and web/API client types after OpenAPI sync.
- Modify next: `apps/ts/packages/web/src/components/Ranking/EquityRankingTable.tsx`
  - Color cells from backend percentile fields and liquidity regime.

## Task 1: Dedicated Fast Research Runner

- [x] Write failing test for `run_ranking_color_evidence_research`.
- [x] Implement lightweight DuckDB panel using `daily_valuation`, not `statements`.
- [x] Add runner script and `--help` verification.
- [x] Run Prime-only full-period research:

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_ranking_color_evidence.py \
  --start-date 2016-04-01 \
  --end-date 2026-05-14 \
  --markets prime \
  --horizons 20 \
  --output-root /tmp/trading25-research \
  --run-id 20260522_ranking_color_evidence_prime_regime_v2 \
  --min-observations 1000 \
  --notes ranking-color-prime-production-regime-names
```

Result: completed in about 8 seconds. Bundle:

`/tmp/trading25-research/market-behavior/ranking-color-evidence/20260522_ranking_color_evidence_prime_regime_v2`

## Task 2: Evidence Interpretation

- [x] Publish readout to `apps/bt/docs/experiments/market-behavior/ranking-color-evidence/README.md`.
- [x] Keep absolute PER/PBR/Fwd PER/Fwd P/OP thresholds out of the design.
- [x] Record that PBR high percentile is the strongest bad/red evidence in Prime.
- [x] Record that low-valuation green is Prime-relative UI evidence, not an absolute alpha claim.
- [x] Record that `流動性Z` is a production regime/crowding diagnostic, not "higher is better"; `crowded_rerating` is caution, not green.

## Task 3: Backend Contract

- [ ] Add optional fields to Ranking item schema:

```python
perPercentile: float | None
forwardPerPercentile: float | None
forwardPOpPercentile: float | None
pbrPercentile: float | None
```

- [ ] Compute percentiles over the full target-date market universe in backend, before applying frontend/table display limits.
- [ ] Use target-date market scope. Do not compute percentiles from visible rows.
- [ ] Add unit tests in `apps/bt/tests/unit/server/services/test_ranking_service.py`.
- [ ] Run OpenAPI sync:

```bash
bun run --filter @trading25/contracts bt:sync
```

## Task 4: Frontend Coloring

- [ ] Remove current `forwardPer > per` / `forwardPer < per` color logic.
- [ ] Add a small evidence-tier helper near `EquityRankingTable`.
- [ ] Color desktop table cells and mobile cards consistently.
- [ ] Add PBR to mobile valuation metrics so all target metrics are visible.
- [ ] Preserve existing liquidity state chip colors; only color the numeric `流動性Z` cell.

## Task 5: Validation

- [ ] Run focused backend tests:

```bash
uv run --project apps/bt pytest \
  apps/bt/tests/unit/domains/analytics/test_ranking_color_evidence.py \
  apps/bt/tests/unit/server/services/test_ranking_service.py -q
```

- [ ] Run frontend tests/typecheck:

```bash
bun run --filter @trading25/web test
bun run --filter @trading25/web typecheck
```

- [ ] Browser-check `/ranking?dailyView=stocks` on the local app after server restart if backend contract changes are in play.

## Non-Goals

- No absolute valuation thresholds.
- No portfolio score-method change.
- No sector-relative calibration in this first pass.
- No claim that UI colors are standalone alpha rules.
