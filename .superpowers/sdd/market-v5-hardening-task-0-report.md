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
signal-date `daily_valuation` row whose `price_basis_date` is the signal date and
whose basis and fingerprint match the current fundamentals state. Stale and
current valuation rows cannot satisfy this check jointly.

## Other preserved behavior

- Normalized consumer aliases prefer the four-digit code; the selected canonical
  projection must still match provider-adjusted values exactly.
- Equal normalized raw aliases are accepted. Conflicting OHLC or adjustment
  factors fail closed.
- Signal-date universe membership remains `stock_master_daily` based.
- Next-open outcomes use the stock's next session. A missing same-date TOPIX
  endpoint leaves excess return null.
- Split and reverse-split completion outcomes remain provider-adjusted.
- Forward-outcome hashes cover TOPIX and N225 excess-return columns. The
  aggregate price-projection hash also commits to the next-open outcome hash.
- Incomplete forward horizons remain null and are never backfilled with a later
  cohort member.
- Ranking recovery guidance is `market_db_sync`.
- Exact-date materialized daily technical metrics are retained; codes without a
  materialized row fall back to calculation from the verified provider-adjusted
  event relation. The event-time provider projection is always constructed and
  validated before any materialized overlay is consumed.
- Market Bubble valuation joins use current fundamentals lineage; the selected
  price-history provider vintage is propagated by code/date. Standalone and live
  paths with no explicit price-history relation also use this verified Market v5
  projection instead of the `stock_data` convenience table. Each consumed price
  row now requires exactly one valuation row whose `date` and `price_basis_date`
  both equal the price date and whose fundamentals basis/fingerprint match the
  current state on that same row. Future/stale price-basis rows are ignored;
  duplicate and split-witness valuation lineage fails closed.
- Standalone and rerating Bubble results publish the same deduplicated 12-table
  Market v5 dependency set: raw and convenience prices, provider windows/events,
  current fundamentals state/pending work, daily master/valuation, raw and
  adjusted statements, TOPIX, and indices.
- Ranking analytics fixtures that previously created legacy statement tables or
  populated only the convenience price projection now seed canonical raw and
  adjusted statement identities, `stock_data_raw`, exact provider fingerprints,
  and current-basis statement counts.

## Verification evidence

- Daily Ranking service: **108 passed**; service plus ranking contracts:
  **115 passed** (1 warning, 12.97s).
- Daily Ranking event-time/provider tests: **37 passed**.
- Daily Ranking research-base tests: **21 passed**.
- Daily Ranking feature builders: **68 passed**.
- Trend acceleration research: **27 passed** (1 warning, 150.33s).
- Fixed-return research: **31 passed**.
- Technical-fit research: **96 passed, 1 skipped** (267.38s); its focused
  provider/current-basis lineage set is **13 passed**.
- Market Bubble: **9 passed** (1 warning, 490.22s), including the reviewer
  reproduction where a stale `price_basis_date=2099-12-31` row previously moved
  `expensive_mcap_share_pct` from **44.883986** to **99.998841**. The metric now
  remains at the verified baseline. Bubble monitor: **3 passed** (1 warning,
  0.04s).
- ATR expansion, sector strength, short red, and short sector strength:
  **24 passed** (1 warning, 327.56s).
- PSR and short-value Market v5 fixtures: **2 passed** and **5 passed**,
  respectively.
- Repository suite (`uv run pytest tests/`): **7695 passed, 3 skipped, 1 failed**
  in 2608.06s. The sole failure was
  `TestRankingSymbolSnapshot::test_200_latest_symbol_snapshot`: its route fixture
  declared Market v5 provider windows but left `stock_data_raw` empty, so the
  event-time cohort correctly produced no ranked item. After seeding all 900 raw
  provider rows and computed provider fingerprints, targeted verification is
  green: exact failing test **1 passed**, symbol route class **4 passed**,
  containing analytics route file **61 passed**, and ranking service/contracts
  **115 passed**. Per the approved efficient verification policy, the entire
  43-minute suite was not rerun after this fixture-only repair or the subsequent
  focused Bubble review fixes.
- Analytics route recovery mapping remains covered by the 61-test route file and
  returns `market_db_sync` for provider-lineage recovery.
- Changed Task 0 Python sources/tests pass Ruff.
- Changed route fixture and Technical-fit production/test files pass Pyright
  with zero errors and warnings.
- `create_app()` import smoke passes.
- The migrated ranking/research production-module scan returns no obsolete
  basis-table, `basis_version`, or local-projection-mode references.
