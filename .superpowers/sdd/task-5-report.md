# Task 5 Report: Stream progress through the localhost bridge

## Implemented

- Added a Chrome runtime Port adapter using the exact `shikiho-capture-progress-v1` name.
- Subscribed the long-lived localhost bridge Port to the current four-digit code after each valid `get_snapshot` page request.
- Strictly parsed internal progress envelopes and forwarded them as public `capture_progress` responses mapped to the current page `requestId`.
- Advanced each page request through trusted attempts in one direction, while rejecting wrong-code, retired-attempt, stale-sequence, and malformed progress.
- Reset sequence state for a new trusted attempt, and reset all attempt state when the page request/code changes or Chrome disconnects the Port.
- Enforced both current-window source and exact approved localhost origin before accepting page requests.
- Removed Port listeners on runtime disconnect, reconnected on the next valid request, and disconnected/cleaned the Port when the bridge stops.
- Stopped forwarding progress after a terminal response for the current request.

## TDD evidence

- RED: `bun test extensions/shikiho/src/localhost-content.test.ts` failed because `SHIKIHO_CAPTURE_PROGRESS_PORT_NAME` and Port streaming did not exist.
- GREEN: added focused tests for exact Port naming, subscriptions, request-ID mapping, request/code replacement, stale and malformed messages, source/origin restrictions, disconnect/reconnect, terminal cleanup, and bridge stop cleanup.
- Review RED: exact-user-tab attempt progress followed by owned-tab fallback progress produced only one event because a new attempt ID was incorrectly treated as stale.
- Review GREEN: an unseen attempt now replaces the current attempt with sequence reset; the replaced attempt ID is retired and cannot become current again.

## Verification

- `bun test extensions/shikiho/src/localhost-content.test.ts extensions/shikiho/src/contract.test.ts` — 43 pass, 0 fail.
- `bun run --filter @trading25/shikiho-extension test` — 235 pass, 0 fail.
- `bun run --filter @trading25/shikiho-extension typecheck` — pass.
- `bunx biome check extensions/shikiho/src/localhost-content.ts extensions/shikiho/src/localhost-content.test.ts` — pass.
- `git diff --check` — pass.

## Scope and boundaries

- No network, cookie, backend, OpenAPI, database, manifest permission, or host permission changes.
- No provisional content is persisted; the localhost bridge only forwards in-memory progress from the validated Chrome Port.
