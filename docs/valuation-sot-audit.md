# Valuation SoT Audit

## Product SoT

- Product valuation fields (`PER`, `forward PER`, `PBR`, `marketCap`) are rooted in `apps/bt/src/domains/fundamentals/FundamentalsCalculator.calculate_latest_valuation()`.
- Low-level semantics live in `apps/bt/src/domains/fundamentals/valuation_primitives.py`:
  - non-positive denominators return `None`
  - non-positive prices return `None`
  - market cap requires positive price and shares
- `chart_service.get_sector_stocks()` and value-composite ranking now enrich rows through the fundamentals resolver instead of inline service math.
- `apps/bt/tests/unit/domains/fundamentals/test_valuation_sot_inventory.py` prevents product services from reintroducing direct PER/PBR/market-cap formulas.

## Batch And Signal Surfaces

- `apps/bt/src/domains/strategy/signals/fundamental_valuation.py` does not call the per-symbol product resolver because signal inputs are already prepared daily series. It now uses the shared vectorized valuation-ratio primitive for PER/PBR/PEG denominator semantics.
- `apps/bt/src/application/services/screening_market_loader.py` and `apps/bt/src/infrastructure/data_access/loaders/data_preparation.py` remain vectorized data-preparation paths. They already share `resolve_adjusted_shared_baseline_shares()` from `statements_loaders.py`, which uses `adjust_share_count_to_price_basis()`.

## Research Inventory

Research modules intentionally keep experiment-specific valuation snapshots when the timing contract is part of a published bundle. Do not bulk rewrite them in a product UI fix.

Reusable or active panel providers to migrate first in a follow-up:

- `apps/bt/src/domains/analytics/forward_eps_trade_archetype_decomposition.py`
- `apps/bt/src/domains/analytics/annual_large_universe_factor_family.py`
- `apps/bt/src/domains/analytics/annual_market_fundamental_divergence.py`
- `apps/bt/src/domains/analytics/annual_value_periodic_rebalance.py`
- `apps/bt/src/domains/analytics/annual_value_breakout_periodic_rebalance.py`

Published experiment modules should preserve bundle semantics unless a dedicated rerun is part of the task.
