# Source Inventory

## Coverage

- Coverage level: Strong for repo research source selection and publication rules; Directional for individual experiment details until the relevant README and runner artifacts are read in the current workflow.
- Sources checked: repo experiment index, research catalog metadata, PIT invalidation register, bt research workflow skill, and current research runner conventions.
- Missing high-value lanes: no external data warehouse, Slack, or BI dashboard source is configured for this semantic layer. This is intentional unless a future workflow asks for external context.
- Rejected or lower-confidence candidates: `/tmp` research bundles are treated as ephemeral and must not be the durable source of truth.

## Sources

| Source | Type | Locator | Connector Or Tool | Permission Status | Last Checked | Supports | Gaps Or Caveats | Automation Eligible | Update Boundary |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Experiment README Published Readouts | Canonical docs | `apps/bt/docs/experiments/*/*/README.md` | Local repo filesystem | Available | 2026-06-09 | Published decisions, findings, interpretation, production implication, caveats, source artifacts | Must read relevant README in the current workflow; incomplete readouts are not publication evidence | Yes | Report changed files; do not rewrite conclusions automatically |
| Experiment index | Repo docs | `apps/bt/docs/experiments/README.md` | Local repo filesystem | Available | 2026-06-09 | Experiment conventions and navigation | Index can lag specific README details | Yes | Report changes |
| Research catalog metadata | Repo metadata | `apps/bt/docs/experiments/research-catalog-metadata.toml` | Local repo filesystem | Available | 2026-06-09 | Compact family, status, decision, risk flags, related experiments | Summary-level only; README controls full interpretation | Yes | Report changes |
| PIT invalidation register | Repo docs | `docs/research-pit-invalidation-register.md` | Local repo filesystem | Available | 2026-06-09 | Future-leak and PIT safety status | Only covers registered invalidation classes | Yes | Report changes |
| Research runner scripts | Reproducible execution | `apps/bt/scripts/research/` | Local repo filesystem / `uv run --project apps/bt` | Available | 2026-06-09 | Recompute missing bundles, inspect CLI args, reproduce tables | Runtime market DB freshness can change numerical output | No | Manual rerun only |
| Analytics domain code | Calculation logic | `apps/bt/src/domains/analytics/` | Local repo filesystem | Available | 2026-06-09 | Metric definitions, PIT helpers, table construction | Code can be ahead of published readout; compare with README and bundle | Yes | Report changes |
| Research bundles | Result artifacts | `~/.local/share/trading25/research/...` or explicit bundle paths such as `/tmp/...` | Local filesystem / DuckDB | Conditional | 2026-06-09 | `manifest.json`, `summary.md`, `results.duckdb` detailed tables | `/tmp` paths are ephemeral; missing bundles should be regenerated | No | Manual rerun only |
