---
name: trading25-research-semantic-layer
description: Use when answering Data Analytics questions from trading25 research results, especially apps/bt experiment readouts, runner bundles, research catalog metadata, Ranking evidence, PIT safety, and next-research prioritization.
---

# Trading25 Research Semantic Layer

Use this skill when the user wants Data Analytics to use this repository's
research as the analytical source of truth.

## Start Here

1. Read `references/semantic-layer.md`.
2. Treat `apps/bt/docs/experiments/*/*/README.md` `## Published Readout` as the publication source of truth.
3. Use `apps/bt/docs/experiments/research-catalog-metadata.toml` as the compact index for experiment family, status, decision, risk flags, and related experiments.
4. Verify calculations with the runner output `summary.md` / `results.duckdb` when the bundle exists or when the user asks for numerical confirmation.
5. If a `/tmp` bundle is missing, rerun the relevant script under `apps/bt/scripts/research/` instead of trusting stale paths.

## References

- `references/semantic-layer.md`: source priority, metrics, experiment surfaces, and gotchas.
- `references/source-inventory.md`: sources checked and missing lanes.
- `references/evidence.md`: provenance for the layer itself.

## Answering Rules

- Prefer repo research readouts over generic external analytics assumptions.
- Distinguish published conclusions from exploratory bundle tables.
- Treat PIT or future-leak invalidation as blocking for production, Ranking, Screening, or strategy-selection evidence.
- When sources disagree, state which source controls the answer and why.
- Do not treat `summary.json`, legacy digest fields, or chat-only conclusions as publication evidence.
