# Evidence Register

| Fact Or Claim | Source Type | Source Link Or Path | Retrieved Or Observed | Confidence | Notes |
| --- | --- | --- | --- | --- | --- |
| Published research source of truth is experiment README `## Published Readout`. | Repo docs / local skill | `apps/bt/docs/experiments/README.md`, `.codex/skills/bt-research-workflow/SKILL.md` | 2026-06-09 | High | `summary.json` and legacy digest fields are not publication sources. |
| PIT invalidation status must be checked before using research as production evidence. | Repo docs | `docs/research-pit-invalidation-register.md` | 2026-06-09 | High | Invalidated and rerun-required entries cannot support production/Ranking/Screening evidence. |
| Research catalog provides compact family/status/decision/risk/related-experiment metadata. | Repo metadata | `apps/bt/docs/experiments/research-catalog-metadata.toml` | 2026-06-09 | High | Use for discovery; README controls interpretation. |
| Runner bundles contain `manifest.json`, `summary.md`, and `results.duckdb`; `/tmp` bundles may be missing. | Repo docs and observed run | `apps/bt/docs/experiments/README.md`, `/tmp/trading25-research-refresh/...` | 2026-06-09 | High | Rerun missing ephemeral bundles instead of treating absence as invalidation. |
