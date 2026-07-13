# TOPIX100 Streak Point-in-Time Audit

Date: `2026-04-10`

## Current Decision

The tradeable fixed-3/53 research family was deleted on 2026-07-13. Its
parameter selection used forward-return evidence, its former event-row coercion
was future-conditioned, and stock-selection studies also used current
membership across historical dates. Deleted results are not active evidence and
have no rerun promise.

`topix100_streak_353_transfer` remains only as a retrospective event study. It
must not be used as a tradeable ranking or production-evidence surface.

## Point-in-Time State

The neutral daily state implementation is
[`topix_streak_state.py`](../apps/bt/src/domains/analytics/topix_streak_state.py).
For each requested date it uses only history available through that date.
Future rows must not change an already-computed state.

The retained retrospective transfer helpers may explain completed streak
events, but event rows keyed by `segment_end_date` must never be coerced into
a daily signal panel.

## Guardrails

- A state at date `t` must be reproducible from data truncated at `t`.
- Future OHLCV, disclosures, or membership must not change past states.
- Historical membership must resolve at the signal date with no latest/current
  fallback.
- Forward returns and labels must not enter feature construction or parameter
  selection.
