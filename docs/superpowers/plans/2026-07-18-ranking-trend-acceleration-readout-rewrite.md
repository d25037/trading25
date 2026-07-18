# Ranking Trend Acceleration Readout Rewrite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rewrite the canonical Published Readout so the adoption decision and its evidence are understandable without decoding internal research-table or gate names.

**Architecture:** This is a presentation-only change to one canonical experiment README. Existing v2 bundle values remain authoritative; the rewrite reorganizes them into separate continuous, badge, Top-K, and adoption-condition sections, then verifies every retained value and publication contract without changing research logic or production surfaces.

**Tech Stack:** Markdown, DuckDB CLI/Python verification, repository research guardrails.

## Global Constraints

- Modify only `apps/bt/docs/experiments/market-behavior/ranking-trend-acceleration-conditional-lift/README.md` for the published readout.
- Do not change any metric, confidence interval, date range, PIT assumption, bundle path, provenance field, catalog decision, or production recommendation.
- Universe remains PIT Prime-equivalent only: `0101` before market restructuring and `0111` afterward; Standard and Growth remain excluded.
- Keep `Decision`, `Main Findings`, `Interpretation`, `Production Implication`, `Caveats`, and `Source Artifacts` in `## Published Readout`.
- Preserve the v2 bundle as the numerical source of truth; do not introduce new calculations.
- Preserve the user's unrelated `.gitignore` change.

---

### Task 1: Rewrite the canonical Published Readout

**Files:**
- Modify: `apps/bt/docs/experiments/market-behavior/ranking-trend-acceleration-conditional-lift/README.md`

**Interfaces:**
- Consumes: the approved rewrite design and v2 `results.duckdb`, `manifest.json`, and `summary.md`.
- Produces: a decision-first Japanese Published Readout with separately explained continuous, badge, Top-K, adoption-condition, validation, caveat, and provenance evidence.

- [ ] **Step 1: Record the existing published facts**

Run:

```bash
sed -n '1,280p' apps/bt/docs/experiments/market-behavior/ranking-trend-acceleration-conditional-lift/README.md
```

Expected: the current readout contains the v2 decision, exact tables, validation details, caveats, source paths, provenance, telemetry, and runner command.

- [ ] **Step 2: Verify table schemas and any values needed by the new layout**

Run:

```bash
uv run --directory apps/bt python - <<'PY'
import duckdb

path = "/Users/mirage/.local/share/trading25/research/market-behavior/ranking-trend-acceleration-conditional-lift/20260718_prime_pit_conditional_lift_v2/results.duckdb"
with duckdb.connect(path, read_only=True) as connection:
    for table in ("continuous_rank_lift_df", "conditional_binary_lift_df", "topk_priority_lift_df", "decision_gate_df"):
        print(table, connection.execute(f'DESCRIBE SELECT * FROM "{table}"').fetchall())
PY
```

Expected: schemas expose the already-published metrics required by the approved tables; no derived or unpublished metric is needed.

- [ ] **Step 3: Rewrite the README**

Use `apply_patch` to implement the approved section order and wording:

```text
Decision → Why This Research Was Run → Data Scope / PIT Assumptions →
What Was Compared → Main Findings (continuous / badge / Top-K / adoption conditions) →
Interpretation → Production Implication → Validation Details → Caveats → Source Artifacts
```

Expected: the first screen states non-adoption, failed cross-family replication, and unchanged fixed 20D/60D; continuous and binary evidence no longer share one table.

- [ ] **Step 4: Inspect the resulting diff for presentation-only scope**

Run:

```bash
git diff -- apps/bt/docs/experiments/market-behavior/ranking-trend-acceleration-conditional-lift/README.md
```

Expected: only wording, headings, and table organization change; all research facts and operational conclusions remain unchanged.

### Task 2: Verify publication integrity and readability

**Files:**
- Verify: `apps/bt/docs/experiments/market-behavior/ranking-trend-acceleration-conditional-lift/README.md`
- Verify unchanged: `apps/bt/docs/experiments/research-catalog-metadata.toml`

**Interfaces:**
- Consumes: the rewritten README from Task 1.
- Produces: verified publication-contract compliance and an independent evidence/readability review.

- [ ] **Step 1: Compare retained numerical claims with the v2 artifacts**

Run targeted read-only DuckDB queries for the two primary families, 20D continuous/binary rows, four Top-K rows, segment rows, coverage rows, and decision rows; compare their printed values to the README.

Expected: every displayed value, confidence interval, count, and date range matches the v2 artifacts or the previously published README exactly.

- [ ] **Step 2: Run repository publication checks**

Run:

```bash
python3 scripts/check-research-guardrails.py
python3 scripts/skills/audit_skills.py --strict-legacy
python3 - <<'PY'
import tomllib
from pathlib import Path

tomllib.loads(Path("apps/bt/docs/experiments/research-catalog-metadata.toml").read_text())
print("research catalog TOML: ok")
PY
git diff --check
```

Expected: every command exits 0; the catalog parses without modification; the diff has no whitespace errors.

- [ ] **Step 3: Request independent evidence/readability review**

Ask a reviewer to verify that the readout answers the four reader questions early, keeps Prime-equivalent PIT scope explicit, separates incomparable metrics, and preserves all exact evidence and provenance.

Expected: reviewer returns PASS or concrete corrections, which are applied and reverified.

- [ ] **Step 4: Commit the readout rewrite**

Run:

```bash
git add apps/bt/docs/experiments/market-behavior/ranking-trend-acceleration-conditional-lift/README.md docs/superpowers/plans/2026-07-18-ranking-trend-acceleration-readout-rewrite.md
git commit -m "docs: clarify trend acceleration research readout"
```

Expected: commit succeeds without staging `.gitignore`.
