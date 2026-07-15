# Shikiho one-shot owned tabs design

## Goal

Return extension-created Company Shikiho tabs to one-shot lifecycle semantics. A tab created by the extension exists only for one capture attempt and is closed after that attempt finishes. Reuse must not remain as a capture path.

## Ownership boundary

- An exact Shikiho tab that was already open outside the extension remains user-owned. The extension may read its rendered DOM but must not navigate, reload, or close it.
- A tab created by the extension remains extension-owned until capture cleanup or until existing ownership-safety hooks abandon it because the user interacted with it.
- Ownership metadata is retained only to make cleanup race-safe. It is not permission to reuse a tab.

## Lifecycle

1. When no exact user-owned tab is available, create a new inactive Shikiho tab for the selected code.
2. Capture the rendered DOM with the existing progressive capture pipeline.
3. Persist the terminal snapshot, diagnostic, and trace before releasing ownership.
4. Close the extension-owned tab and remove its lease after success, partial success, diagnostic, timeout, or error.
5. A later capture creates another inactive tab. It never navigates or reuses the prior owned tab.
6. On extension startup/reconciliation, close and remove any legacy idle reusable lease left by an older build.

## Compatibility

- Keep fast memoized DOM extraction, progressive diagnostics, the 25-second absolute deadline, alphanumeric stock-code support, and local-only storage unchanged.
- Keep legacy lease parsing and alarm handling only as needed to safely clean up state created by older builds.
- Existing user-owned exact-tab capture remains unchanged.

## Tests

- A successful owned capture closes the created tab and clears the lease without creating an idle alarm.
- Two sequential owned captures create two different tabs and never call tab navigation/update.
- Reconciliation closes a legacy idle lease immediately.
- Diagnostics, timeout, and error continue to close owned tabs.
- Exact user-owned tabs are not closed or navigated.
- Full extension tests, typecheck, build, Biome, privacy checks, and Chrome acceptance must pass.

## Documentation

Update the active Chrome operator instructions to state that extension-created tabs are one-shot and closed after capture. Historical design records remain unchanged.
