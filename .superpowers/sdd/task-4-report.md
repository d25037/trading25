# Task 4 report: instrument acquisition and production background routing

## Status

Implemented Task 4 with TDD, production routing, terminal trace persistence, self-review fixes, and extension validation.

## RED evidence

- Added acquisition lifecycle tests before production changes.
- Initial focused run: 45 passed / 2 failed because acquisition still sent the legacy request and did not finish broker attempts.
- Added broker latest-progress terminalization tests before the broker merge.
- Initial broker run: 12 passed / 2 failed because timeout/error finishes discarded accepted progress metrics.
- Added a nonterminal content-trace rejection test during self-review.
- Initial focused run failed because acquisition incorrectly resolved a nonterminal trace.

## Implementation

- Removed the compile-only legacy `capture_now` request/response shape.
- Added one attempt ID per selected tab acquisition while retaining a fresh request ID for every receiver message.
- Registered broker attempts before the first capture message.
- Preserved the absolute 25-second deadline and 100 ms receiver retry cadence.
- Recorded receiver attempts/readiness and passed them to the exact instrumented bridge.
- Validated response tab, request ID, attempt ID, code, result, and compatible terminal trace semantics.
- Finished terminal success/partial/diagnostic/error/timeout exactly once before owned lease release.
- Limited abandonment without a terminal trace to exact-tab fallback and navigation/code replacement.
- Synthesized minimal timeout/error terminal traces in acquisition.
- Extended the broker internally to retain the latest accepted progress and merge its trusted metrics/field state with synthesized terminal semantics without exposing provisional content to acquisition.
- Created one production broker, strictly parsed internal `capture_progress`, required `sender.tab.id`, and accepted only Ports named `shikiho-capture-progress-v1`.
- Added repository trace reads to public resolution and localhost bridge responses.
- Preserved an existing repository snapshot when a forced refresh acquisition fails, while returning the persisted terminal trace.
- Forwarded terminal traces and trace-storage changes through the localhost bridge without promoting provisional candidates.

## Justified Task 2/3 integration extensions

- `capture-progress.ts` and its tests were extended because timeout/error terminalization must merge the broker's latest accepted progress while acquisition remains limited to the four lifecycle methods.
- `shikiho-tab-bridge.ts` removed the Task 3 compile-only legacy sender compatibility now that Task 4 owns the instrumented sender.
- `localhost-content.ts` and its test were updated so the Task 4 `{snapshot, diagnostic, trace}` production response is not rejected or dropped before Task 5 consumes it.

## Self-review

- Fixed nonterminal or result-incompatible content traces being accepted as completed captures.
- Fixed synthesized timeout/error persistence to retain latest accepted DOM field state and navigation metadata, not only counters.
- Split receiver retry and runtime-response parsing helpers to satisfy Biome complexity checks.
- Confirmed exact user tabs are never released, closed, or adopted by the warm-tab lease manager.
- Confirmed broker cleanup occurs before trace persistence, so late progress cannot survive terminal cleanup.
- No material unresolved correctness findings.

## Validation

- Focused Task 4 tests: 63 passed, 0 failed.
- Full extension tests: 220 passed, 0 failed, 776 expectations.
- Extension TypeScript typecheck: exit 0.
- Extension build: exit 0.
- Scoped Biome check across all 10 touched source/test files: clean.
- `git diff --check`: clean.

## Concerns

- No blocking concerns.
- Runtime Chrome/Atlas Port behavior is covered by broker and build/type contracts; live Atlas validation remains outside this implementation task.
