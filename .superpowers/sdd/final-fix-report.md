# Final Ranking Review Fix Report

## Scope

- Fixed the value-composite technical-metrics query in
  `apps/bt/src/application/services/ranking_value_composite_features.py`.
- Preserved the pre-optimization full-history meaning of 20/120-session new-high
  detection and `daysSinceNewHigh`.
- Kept the provider-price relation bounded before normalization/deduplication by
  restricting it to the exact normalized candidate code set and `target_date`
  upper bound.
- Did not change the value-composite formulas, profile path, response contracts,
  API routes, or frontend behavior.

## Root cause and data flow

Before `d2334ded`, `load_value_composite_technical_metrics()` built a canonical
provider-price relation over the available table and then restricted both
`stock_history` and `signal_history` to the requested candidate codes and PIT
cutoff. The window expressions therefore saw every candidate session up to the
cutoff.

`d2334ded` moved a lower date bound for the latest 253 sessions into
`provider_price`. That bound was sufficient for the latest 252-session return,
but the same relation also feeds:

- the 20- and 120-session prior-high windows;
- full-history `signal_row_number`;
- `latest_breakout` aggregation; and
- `days_since_new_high_*` subtraction.

As a result, the first rows after the lower bound lacked their true predecessors,
and a genuine latest breakout before the lower bound disappeared entirely. This
was a semantic change, not just a query-plan change.

The corrected flow is:

```text
normalized requested codes
  -> current-basis/provider-window batch validation at target_date
  -> provider_price source filtered by normalized_code IN (candidates)
                               AND price.date <= target_date
  -> unchanged 4-digit-preferred per-code/date deduplication
  -> unchanged full candidate history window formulas
  -> unchanged latest technical/breakout response mapping
```

The provider relation therefore avoids windowing unrelated market codes while
retaining all history required by the legacy formulas. The existing outer code
and date filters remain in place, and positional parameters follow SQL occurrence
order: provider candidates, provider cutoff, stock-history cutoff/candidates,
then signal-history cutoff/candidates.

## Strict TDD evidence

### Baseline

Before adding the regression:

```text
uv run pytest tests/unit/server/services/test_ranking_service.py -k 'value_composite' -q
21 passed, 102 deselected
```

### RED

Two tests were added before production code changed:

1. A SQL/parameter regression requiring the provider-price source predicate to
   contain the exact normalized candidate set and target-date upper bound before
   `ROW_NUMBER()` deduplication, without a lower date bound.
2. A DuckDB-backed 400-session semantic regression. Its only genuine 20/120-day
   breakout is at session index 100, before the former 253-session lower bound.
   The latest signal is 298 sessions after that breakout. The fixture also places
   conflicting 4-digit/5-digit rows on the latest signal date so the established
   4-digit precedence remains observable.

The focused RED run failed for the intended reasons:

```text
uv run pytest tests/unit/server/services/test_ranking_service.py \
  -k 'value_composite_provider_price_is_candidate_bounded or value_composite_technical_metrics_preserve_full_history_breakout' -q

FAILED test_value_composite_provider_price_is_candidate_bounded_before_deduplication
  ValueError: substring not found

FAILED test_value_composite_technical_metrics_preserve_full_history_breakout_semantics
  assert None == 298

2 failed, 123 deselected
```

The first failure proved that the inner provider source had no candidate-code
bound. The second proved that the 253-session cap removed the legacy breakout.

### GREEN

The minimal production change removed the 253-session start-date lookup and
changed the inner predicate to normalized candidate codes plus the target-date
upper bound. The window formulas and response mapping were not modified.

```text
uv run pytest tests/unit/server/services/test_ranking_service.py \
  -k 'value_composite_provider_price_is_candidate_bounded or value_composite_technical_metrics_preserve_full_history_breakout' -q

2 passed, 123 deselected
```

The semantic regression now returns `298` for both 20-day and 120-day
`days_since_new_high`, while the conflicting 5-digit latest-session spike is
ignored in favor of the 4-digit row.

## Verification evidence

```text
uv run pytest tests/unit/server/services/test_ranking_service.py -q
125 passed

uv run pytest tests/unit/server/test_routes_analytics_fundamentals.py \
  tests/unit/server/routes/test_analytics_complex.py -q
100 passed

uv run ruff check src/application/services \
  tests/unit/server/services/test_ranking_service.py
All checks passed!

uv run pyright src/application/services
0 errors, 0 warnings, 0 informations

git diff --check
exit 0
```

## Invariants reviewed

- Full-history new-high and days-since semantics match the pre-optimization query.
- All price observations remain PIT-bounded by `price.date <= target_date`.
- `resolve_provider_windows()` still validates the same normalized candidate set
  before price calculation; no current/latest fallback was added.
- `normalized_code_sql()` is used in the inner predicate, so 4/5-digit inputs map
  to the same candidate identity.
- The unchanged provider CTE continues to prefer a 4-digit row over a 5-digit row
  for the same normalized code/date.
- SQL positional parameter order is asserted for a normalized, deduplicated
  multi-code input set.
- The profile-specific lookback path is unchanged because its bounded recency
  calculation predates the optimization and is not the reported 253-session
  regression.
- No response schema, OpenAPI, route, or TypeScript generation change is needed.

## Self-review

- The fix targets the source of the semantic regression instead of extending the
  date cap with another guessed constant.
- Candidate scoping occurs before the expensive provider deduplication window, so
  unrelated all-market rows no longer participate.
- There is no dynamic SQL value interpolation: candidate values remain positional
  parameters; only the placeholder count and static normalization expression are
  interpolated.
- Existing unrelated modifications to `.superpowers/sdd/task-3-report.md` through
  `task-6-report.md` were preserved and excluded from the scoped fix.
- No remaining material concern was found in the scoped diff.

## Commit

- Intended message: `fix(bt): preserve value composite breakout history`
- The resulting hash is recorded in the task handoff because a commit cannot
  embed its own hash.
