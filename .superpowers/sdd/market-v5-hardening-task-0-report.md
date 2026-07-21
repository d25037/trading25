# Market v5 hardening — Task 0 integration report

## Outcome

Daily Ranking and the merged ranking-research execution path now require Market
schema v5 with `stock_price_adjustment_mode=provider_adjusted_v1`. Signal and
forward-return prices come from `stock_data_raw.adjusted_*`; the consumer
`stock_data` projection, provider window bounds/as-of/fingerprint, and adjustment
event ledger are verified before use. There is no Market v4 compatibility gate or
dual-read fallback in the migrated paths.

## Invariant mapping

| Market v4 invariant | Market v5 invariant |
|---|---|
| ready basis/cardinality | exactly one provider window with exact raw bounds, valid as-of, and SHA-256 fingerprint |
| basis segment cardinality/factor | adjustment-event count, factor, date, and provider fingerprint match raw provider events |
| local reprojection | provider `adjusted_*` values used directly and exact `stock_data` equality verified |
| valuation `basis_version` | exact `price_basis_date` plus selected provider-vintage ID from price history |

`provider_as_of` is treated as the request frontier. A suspended symbol may have
`coverage_end < provider_as_of`; `coverage_end` must equal the actual raw maximum,
and `provider_as_of` must be on or after the signal/completion date being consumed.

## Other preserved behavior

- Signal-date universe membership remains `stock_master_daily` based.
- Incomplete forward horizons remain null and are never backfilled with a later
  cohort member.
- Ranking recovery guidance is `market_db_sync`.
- Exact-date materialized daily technical metrics are retained; codes without a
  materialized row fall back to calculation from the verified provider-adjusted
  event relation.
- Market Bubble valuation joins no longer require removed `basis_version`; the
  selected price-history provider vintage is propagated by code/date.

## Verification evidence

- RED: application import initially failed on removed
  `require_market_v4_compatibility`; the first v5 price test initially failed
  collection before the provider contract was implemented.
- GREEN: Daily Ranking event-time/provider tests: **20 passed**.
- GREEN: Daily Ranking research-base tests: **20 passed**.
- GREEN: focused combined Ranking/research/route run: **43 passed**.
- GREEN: focused fixed-return runner and technical-fit runner both execute against
  v5 provider fixtures.
- GREEN: analytics route recovery mapping test returns `market_db_sync`.
- GREEN: Ruff passed for all changed Task 0 Python sources/tests.
- GREEN: `create_app()` import smoke passed.
- GREEN: `rg require_market_v4_compatibility apps/bt/src apps/bt/tests` returned
  no matches.

## Remaining aggregate risks

- The full Ranking service file progressed through 40 tests before the next
  fixture-port issue: its mixed 4/5-digit consumer-row case creates conflicting
  v5 projection aliases and is correctly rejected by exact projection validation.
  The controlled fixture still needs canonical provider-window construction for
  that alias scenario; production validation was not weakened.
- Several legacy research tests still mutate Market v4 basis/segment tables.
  Their v5 runner happy paths pass, but those mutation assertions must be rewritten
  around provider-window/event/projection corruption before the entire historical
  research test files are green.
