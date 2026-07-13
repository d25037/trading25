# TOPIX100 Streak Point-in-Time Audit

Date: `2026-04-10`

## Current Decision

The tradeable fixed-3/53 research family was deleted on 2026-07-13. Its
parameter selection used forward-return evidence, its former event-row coercion
was future-conditioned, and stock-selection studies also used current
membership across historical dates. Deleted results are not active evidence and
have no rerun promise.

The fixed-`3/53` transfer study was also deleted. Calling it retrospective did
not remove the future-derived parameter choice, so preserving it would keep the
contaminated result available as an implicit compatibility surface.

## Point-in-Time State

No fixed-`3/53` domain, runner, helper, test, publication, or UI fixture remains.
Future-extension stability alone is insufficient when the fixed parameter was
selected from future-return evidence.

## Guardrails

- Any new state at date `t` must be reproducible from data truncated at `t`.
- Future OHLCV, disclosures, or membership must not change past states.
- Historical membership must resolve at the signal date with no latest/current
  fallback.
- Forward returns and labels must not enter feature construction or parameter
  selection.
