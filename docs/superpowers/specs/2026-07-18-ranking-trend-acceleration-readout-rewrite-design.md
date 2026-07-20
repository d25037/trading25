# Ranking Trend Acceleration Published Readout Rewrite Design

## Goal

Rewrite the canonical `ranking-trend-acceleration-conditional-lift` Published Readout so a Ranking user can understand the decision, evidence, and production implication without first decoding internal gate or table names.

The rewrite changes presentation only. It must not change any metric, confidence interval, date range, PIT assumption, bundle path, provenance field, catalog decision, or production recommendation.

## Editorial Principle

Answer the reader's question before explaining the research machinery:

1. Did OLS trend acceleration improve ordering inside existing long candidates?
2. Did the three-condition badge improve outcomes?
3. Did Top-K prioritization rescue the result?
4. Why does the evidence fail the adoption threshold?

Internal names such as `decision_gate_df`, `1/7`, `0/7`, and bootstrap completeness belong after the plain-language conclusion, not in the first explanation of the result.

## Required Structure

### Decision

Lead with three plain-language statements:

- Do not add the continuous Ranking columns or binary badge.
- The result did not reproduce across the two independent long-candidate families.
- Keep fixed 20D/60D and all production Ranking surfaces unchanged.

Mention the formal `reject_introduction` verdict only after those statements.

### Why This Research Was Run

Explain in one short paragraph that the research tests incremental ordering within candidates selected by existing information. State explicitly that OLS features do not select the population.

### Data Scope / PIT Assumptions

Keep the current Prime-equivalent PIT contract and after-close timing. Shorten the explanation while preserving `0101`, `0111`, exact-date `stock_master_daily`, primary 20D TOPIX-excess outcome, and incomplete-forward exclusion.

### What Was Compared

Add a compact definition table:

| Reader-facing label | Definition |
| --- | --- |
| Continuous ordering | Rank candidates by `slope20 - slope60` |
| Three-condition badge | `slope20 > 0`, `slope60 > 0`, and `slope20 > slope60` |
| Independent long groups | `core_long_only` and `momentum_value_only` |
| Primary outcome | 20D close-to-close TOPIX-excess return |

Define `core_long_only` and `momentum_value_only` in one plain-language sentence each. Keep exact identifiers for traceability.

### Main Findings

Use four conclusion-first subsections.

1. `連続順位は2つのlong候補群で改善しなかった`
   - One table with family, eligible dates, observations, mean 20D lift, 95% CI, and plain result.
   - Keep IC as supporting prose or a secondary column only when it helps explain why a positive IC did not justify adoption.

2. `3条件badgeは片方だけ良かったが、採用条件を満たさなかった`
   - Separate table with family, paired dates, median lift, positive-date rate, 95% CI, median badge candidates, and plain result.
   - State that `momentum_value_only` was positive but its CI crossed zero, median candidates were three, and the other independent family did not reproduce it.

3. `Top-Kに絞っても改善は確認できなかった`
   - Retain the four primary 20D Top-K rows.
   - Explain in one sentence that every CI crossed zero.

4. `採用条件を総合すると導入できない`
   - Replace the gate-count-first table with a plain-language requirement table: cross-family reproduction, CI lower bound, candidate count, time-segment reproduction, severe-loss non-deterioration, and coverage.
   - Preserve the formal continuous `1/7`, binary `0/7`, and final verdict as a short audit note below the table.

### Interpretation

Explain why isolated positive signals do not override the pre-registered replication gate. Avoid repeating every numeric result.

### Production Implication

Keep this section short and operational:

- do not add the columns;
- do not add the badge;
- do not change fixed 20D/60D or existing Ranking semantics;
- do not start an API/materialization/UI follow-on from this research.

### Validation Details

Move lower-priority audit information here:

- three time segments and why 2024+ is not a holdout;
- `aggressive_rerating` sample insufficiency;
- Top-K bootstrap 42/42 completeness;
- feature coverage;
- horizon-dependent paired-date endpoints.

### Caveats and Source Artifacts

Preserve all material caveats, v2 bundle paths, source commit, dirty provenance, result-table inventory, telemetry, and runner command. Remove duplicated explanation already stated earlier.

## Table and Language Rules

- Each table answers one question and uses comparable metrics across rows.
- Do not combine continuous IC rows and binary paired-lift rows in one table.
- Split dates and observations into separate columns.
- Prefer `改善せず`, `一部改善だが不十分`, and `採用条件を満たさず` over internal gate jargon.
- Define every unavoidable technical term before first use.
- Keep exact metric values and precision from the current canonical README or v2 bundle.
- Do not introduce a new calculation or infer an unpublished value. Use `—` when a reader-facing table would otherwise require a value not already verified.

## Verification

- Compare every retained number and date range against the current README and v2 `results.duckdb`.
- Confirm required Published Readout sections remain present.
- Confirm catalog decision remains unchanged.
- Run research guardrails, strict skill audit, TOML parse, and `git diff --check`.
- Request an independent evidence/readability review before completion.
