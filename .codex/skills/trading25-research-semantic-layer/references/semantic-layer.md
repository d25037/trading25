# Trading25 Research Semantic Layer

## Quick Reference

- Area: trading25 research results and research-backed product decisions.
- Intended users: Codex/Data Analytics answering the user's research, Ranking, Screening, strategy, and next-analysis questions in this repository.
- Coverage level: Strong for source hierarchy and publication rules; Directional for experiment-specific claims until the relevant readout and artifacts are read.
- Source inventory: `references/source-inventory.md`
- Last synthesized: 2026-06-09
- Freshness expectations: reread repo files in the current workflow; rerun research only when numerical verification is needed or durable bundle artifacts are missing.
- Default date and time zone rules: use repo/runtime dates from `market.duckdb` and runner manifests; do not infer "latest" without checking current local artifacts.

## Source Priority

| Priority | Source | Use It For | Caveats |
| ---: | --- | --- | --- |
| 1 | `apps/bt/docs/experiments/*/*/README.md` `## Published Readout` | Published conclusions, decisions, interpretation, caveats, source artifacts | README must have a complete Published Readout; old sections below it are lower priority |
| 2 | `docs/research-pit-invalidation-register.md` | Whether old or current readouts are invalidated, rerun-required, or PIT-safe | Treat invalidated/rerun-required status as blocking for production evidence |
| 3 | `apps/bt/docs/experiments/research-catalog-metadata.toml` | Search/index by family, status, decision, risk flags, related experiments | Compact summary only; README controls full nuance |
| 4 | `apps/bt/scripts/research/<runner>.py` and `apps/bt/src/domains/analytics/*` | Reproducibility, table names, metric construction, PIT logic | Code can change after publication; compare with bundle/README |
| 5 | Runner bundle `manifest.json + summary.md + results.duckdb` | Detailed numerical confirmation and table-level diagnostics | `/tmp` bundles are ephemeral; regenerate missing bundles |

## Research Surfaces

| Surface | Meaning | Source Rule |
| --- | --- | --- |
| Published Readout | Durable decision and interpretation | Read first and cite as the controlling source |
| Source Artifacts | Runner, domain, tests, bundle, table names | Use to locate reproducible evidence |
| Research Catalog | Compact navigation and risk labels | Use for discovery and related work |
| Bundle Tables | Detailed diagnostics and recalculation evidence | Use after readout, not as publication replacement |
| Baseline / old sections | Historical context | Do not override Published Readout |

## Standard Filters And Dimensions

| Filter Or Dimension | Default Logic | Override When | Applies To | Sources |
| --- | --- | --- | --- | --- |
| Publication validity | Require complete `## Published Readout` or explicit PIT-safe replacement | User asks for historical context only | All research conclusions | README, PIT register |
| PIT safety | Use signal-date/as-of universe and PIT helpers; reject latest-membership leakage | Readout explicitly labels proxy or historical context | Ranking, Screening, strategy, market behavior research | PIT register, runner/domain code |
| Ranking evidence | Treat as UI evidence layer unless readout promotes production use | Runner includes portfolio construction and production implication | Daily Ranking colors, sector overlays, long/short candidates | Ranking README family, catalog risk flags |
| Portfolio evidence | Require portfolio lens for position sizing, turnover, cost, sector cap, or capacity claims | User only asks for observation-level forward response | Strategy and production candidates | README caveats, bundle tables |

## Key Metrics And Terms

| Metric Or Term | Meaning | Canonical Source | Caveats |
| --- | --- | --- | --- |
| `median_forward_topix_excess_return_pct` | Median future stock return minus TOPIX over the horizon | Runner `results.duckdb` and Published Readout tables | Observation-level unless readout says portfolio |
| `win_rate_pct` | Share of observations with positive forward TOPIX excess return | Runner `results.duckdb` | Does not capture tail severity alone |
| `severe_loss_rate_pct` | Share of observations below the runner severe-loss threshold | Runner params and tables | Threshold is runner-specific |
| `bank_observation_share_pct` | Share of observations from the bank sector | Ranking sector research readouts | Concentration diagnostic, not necessarily a bug |
| `future_top5_sector_share_pct` | Ex-post sector winner diagnostic | Relevant runner readout | Must not be used as signal input |
| `Published Readout` | Repo publication SoT for long-lived research | Experiment README | Chat-only summaries do not replace it |

## Query Patterns

- Pattern: Answer "what did this research conclude?"
  - Read `research-catalog-metadata.toml` for discovery.
  - Open the experiment README and use `## Published Readout`.
  - Mention risk flags and caveats when they affect production use.

- Pattern: Answer "is this result real or stale?"
  - Check the README `Source Artifacts`.
  - Check `docs/research-pit-invalidation-register.md`.
  - If bundle path is missing or under `/tmp`, rerun the runner with an explicit `/tmp` or requested output root.
  - Compare regenerated `summary.md` / `results.duckdb` to the README before changing interpretation.

- Pattern: Decide "what should we analyze next?"
  - Use README `Production Implication` and `Caveats`.
  - Look at related experiments in `research-catalog-metadata.toml`.
  - Prefer the next lens that would change a production or UI decision, such as portfolio construction, sector cap, PIT safety, turnover, or cost.

## Gotchas

- Gotcha: `summary.json` and legacy digest fields are not publication sources.
  - Impact: future agents may revive stale summaries.
  - How to avoid: use README `Published Readout` and runner bundle artifacts only.
  - Source: `apps/bt/docs/experiments/README.md`, `bt-research-workflow`.

- Gotcha: `/tmp` bundle paths often disappear.
  - Impact: a missing bundle is not evidence that the research is invalid.
  - How to avoid: rerun the runner and compare against the canonical README.
  - Source: runner-first workflow and current inspection.

- Gotcha: observation-level forward response is not portfolio performance.
  - Impact: production sizing, turnover, cost, capacity, and sector concentration can be overstated.
  - How to avoid: require portfolio lens before production adoption.
  - Source: experiment caveats and catalog risk flags.

- Gotcha: PIT invalidation overrides attractive headline results.
  - Impact: invalidated readouts must not support production or strategy-selection evidence.
  - How to avoid: check `docs/research-pit-invalidation-register.md`.
  - Source: PIT invalidation register.

## Related Docs And Skills

| Source | Use It For | Caveats |
| --- | --- | --- |
| `.codex/skills/bt-research-workflow/SKILL.md` | Research runner workflow, publication contract, verification commands | Skill instructions, not experiment evidence |
| `apps/bt/docs/experiments/research-publication-contract.md` | Required Published Readout structure | Publication rules only |
| `scripts/check-research-guardrails.py` | Guardrail regression checks | Does not validate investment interpretation |

## Open Questions

- Should a recurring polling automation watch the research README/catalog files?
  - Why it matters: future Data Analytics runs could know when the semantic layer is stale.
  - Best owner or source to check next: user preference; default is no automation.
