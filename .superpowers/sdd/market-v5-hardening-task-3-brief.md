# Market v5 Hardening Task 3: persist exact provider lineage per stock window

Implement exactly Task 3 in `docs/superpowers/plans/2026-07-21-market-v5-review-hardening.md` after Task 2 head `485e22e88e14349d6a8963cabc61b34772e53528` was independently approved.

## Scope and indivisible contract

- Introduce immutable validated `ProviderStockStage(provider_plan, provider_as_of, provider_codes)`.
- Add non-null `provider_plan` to `stock_provider_windows` in physical schema, table contracts, canonical JSON contract, readers, writers, and explicit test fixtures.
- Make stock publish/flush and stock refresh require an explicit stage/plan/frontier/code scope.
- The provider frontier is the request/TOPIX frontier, never inferred from max returned stock row date.
- Advance same-plan in-scope suspended/no-row symbols to the request frontier without inventing price coverage; a partial refresh must not relabel untouched windows.
- Dataset selection/copy, fingerprints, diagnostics, PIT readers, valuation/ranking readers must derive plan and as-of from exact per-window lineage. Global `sync_metadata.provider_plan` is not lineage authority.
- Reject mixed plans, blank plans, malformed frontiers, and incoherent/missing windows before destructive overwrite or consumer fallback.
- Keep schema v5/provider-adjusted behavior fail-closed; no compatibility aliases, auto migration, global metadata fallback, or latest/current fallback.

## Owned files

Use the exact Task 3 file list in the approved plan, plus only test fixtures that explicitly create/insert `stock_provider_windows`, the task report, and these necessary production files omitted from the plan's file list.

The two unavoidable propagation call sites are:

- `apps/bt/src/application/services/sync_stock_data_fetch.py`
- `apps/bt/src/entrypoints/http/routes/db.py`

Those call sites may only propagate their authoritative provider plan and request/TOPIX frontier into the new required `refresh_stocks` interface. They must not infer authority from returned row maxima, current/latest state, or global metadata. Do not absorb Task 4 or later corporate-action/projection/journal work.

The per-window diagnostics implementation also necessarily includes:

- `apps/bt/src/application/services/db_stats_service.py`

That file may only expose the exact per-window provider-plan diagnostic already produced by the Market v5 inspection snapshot; it must not restore global metadata as lineage authority.

## Required process

Follow all Task 3 RED/GREEN steps. Start with stage/store tests, then propagation, then Dataset/diagnostics/PIT consumers. Run the exact final focused suite from the plan, Ruff, Pyright, and `./scripts/check-contract-sync.sh`. Do not run the repository-wide suite. Write `.superpowers/sdd/market-v5-hardening-task-3-report.md` with RED/GREEN evidence, migrations deliberately not provided, files, commands, and residual risks. Commit the indivisible Task 3 change only; do not push.
