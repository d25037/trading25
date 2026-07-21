# Market v5 hardening — Task 0 integration report

## Outcome

Daily Ranking and the merged ranking-research execution path now require Market
schema v5 with `stock_price_adjustment_mode=provider_adjusted_v1`. Signal and
forward-return prices come from `stock_data_raw.adjusted_*`; the consumer
`stock_data` projection, provider window bounds/as-of/fingerprint, adjustment
event ledger, and current fundamentals lineage are verified before use. No
compatibility gate or dual-read fallback remains in the migrated paths.

## Invariant mapping

| Previous invariant | Market v5 invariant |
|---|---|
| ready basis/cardinality | exactly one provider window with exact raw bounds, valid as-of, and SHA-256 fingerprint |
| basis segment cardinality/factor | adjustment-event count, factor, date, and provider fingerprint match raw provider events |
| local reprojection | provider `adjusted_*` values used directly and exact canonical `stock_data` equality verified |
| valuation basis identity | exact current fundamentals basis date and source fingerprint for the consumed signal date |

`provider_as_of` is the request frontier. A suspended symbol may have
`coverage_end < provider_as_of`; `coverage_end` must equal the actual raw maximum,
and every consumed date must be on or before `provider_as_of`.

Current fundamentals lineage fails closed unless each symbol has exactly one
`current_basis_fundamentals_state`, no pending recomputation, a basis date equal
to provider `coverage_end`, nonblank source fingerprint/materialization time,
matching raw and adjusted statement counts and identities, and a unique
signal-date `daily_valuation` row with the same basis and fingerprint.

## Other preserved behavior

- Normalized consumer aliases prefer the four-digit code; the selected canonical
  projection must still match provider-adjusted values exactly.
- Equal normalized raw aliases are accepted. Conflicting OHLC or adjustment
  factors fail closed.
- Signal-date universe membership remains `stock_master_daily` based.
- Next-open outcomes use the stock's next session. A missing same-date TOPIX
  endpoint leaves excess return null.
- Split and reverse-split completion outcomes remain provider-adjusted.
- Incomplete forward horizons remain null and are never backfilled with a later
  cohort member.
- Ranking recovery guidance is `market_db_sync`.
- Exact-date materialized daily technical metrics are retained; codes without a
  materialized row fall back to calculation from the verified provider-adjusted
  event relation.
- Market Bubble valuation joins use current fundamentals lineage; the selected
  price-history provider vintage is propagated by code/date.

## Verification evidence

- Daily Ranking service: **107 passed** (1 warning, 12.83s).
- Daily Ranking event-time/provider tests: **34 passed**.
- Daily Ranking research-base tests: **20 passed**.
- Trend acceleration research: **27 passed** (1 warning, 150.33s).
- Fixed-return research: **31 passed**.
- Technical-fit research: **96 passed, 1 skipped** (267.38s); its focused
  provider/current-basis lineage set is **13 passed**.
- Market Bubble: **4 passed** (1 warning, 223.18s).
- ATR expansion, sector strength, short red, and short sector strength:
  **24 passed** (1 warning, 327.56s).
- Analytics route recovery mapping: **1 passed**, returning `market_db_sync`.
- Changed Task 0 Python sources/tests pass Ruff.
- Technical-fit production/test files pass Pyright with zero errors and warnings.
- `create_app()` import smoke passes.
- The compatibility-symbol scan across application and test code returns no
  matches.
