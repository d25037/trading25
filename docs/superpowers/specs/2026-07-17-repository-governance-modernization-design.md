# Repository Governance Modernization Design

## Objective

Bring repository skills, CI, security checks, test selection, build verification, and project governance back into alignment with the post-refactor architecture. Completion means the documented FastAPI-only and Market v4 contracts are enforced by executable checks, not merely described.

## Scope

The work covers four connected areas:

1. CI correctness and completeness: every product unit-test domain, CI self-test, production TypeScript build, extension build, and declared coverage suite must be enforced.
2. Security and reproducibility: secret/privacy checks must cover documentation and Git history, workflow permissions must be least-privilege, checkout credentials must not persist, and tool versions must be centrally pinned.
3. Post-refactor cleanup: unused TypeScript filesystem helpers that recreate the retired direct Data Plane contract must be removed.
4. Skills and governance: Market v4 retained promotion, analytics requests, strategy categories, React guidance, dependency versions, and verification commands must reflect current source contracts.

Historical design and plan documents remain historical records and are not rewritten solely because they mention retired paths.

## Architecture

### CI taxonomy

`scripts/ci/test_targets.py` remains the canonical pytest grouping layer, but its groups must cover every production test directory and CI/governance self-test. Tests must assert coverage of known directories and critical scripts so a future split cannot silently fall outside CI.

`scripts/ci/test_taxonomy.py` remains fail-closed for unknown paths. Current product paths such as `domains/lab_agent` and HTTP-backed analytics modules must be classified explicitly, while research-only modules retain targeted research CI.

### Build and coverage gates

The required product workflow will execute both `bun run workspace:build` and `bun run extension:build`. Coverage generation and enforcement must refer to the same package set; api-clients and the Shikiho extension receive explicit thresholds rather than producing unchecked reports.

### Security workflow

Secret and privacy checks run for every non-empty change set, including documentation-only changes. Gitleaks uses Git-aware scanning over repository history relevant to the checkout. Known synthetic test fixtures are excluded narrowly through `.gitleaks.toml`, using path/rule/line matching instead of broad suppression.

Workflows declare `permissions: contents: read`, disable checkout credential persistence, and use immutable action SHAs with readable version comments. Bun, uv, and gitleaks versions are defined once in workflow environment variables or repository-owned setup paths and kept aligned with repository documentation.

### TypeScript Data Plane boundary

`packages/utils/src/utils/dataset-paths.ts` and its tests are removed because they have no production consumer and expose retired `.db` and direct XDG database access. The utils public index stops exporting those functions. A dependency or source guard test prevents the API from returning.

### Skills and governance

Repository workflow skills use discovery-focused `Use when...` descriptions. The skill audit validates frontmatter size/name/description rules, runnable verification commands, and repository-specific banned legacy surfaces. Market cutover skills explicitly distinguish full rebuild `cutover` from canonical retained `promote-retained` and encode no-sync/no-JQuants/recovery requirements.

The vendored React rule catalog is refreshed from the installed bundled catalog and carries version/provenance metadata plus a drift test. Remote `main` guidance is replaced with a pinned source. AGENTS files avoid volatile test counts and line ranges where a stable symbol/path is available.

## Error Handling and Compatibility

- Security scanner exceptions are minimal and documented; scanner failure remains blocking.
- Unknown CI paths continue to trigger full product CI.
- No compatibility alias is retained for the removed TypeScript database helpers because there are no production consumers and the APIs violate the current architecture.
- Historical documents are excluded from active-contract bans when they are clearly stored under `docs/superpowers/specs` or `docs/superpowers/plans`.

## Verification

Completion requires:

- repository skill audit and its unit tests;
- CI taxonomy/workflow tests, including tests proving previously omitted suites are selected;
- all newly included BT tests;
- TypeScript dependency audit, typecheck, workspace tests, production workspace build, and extension build;
- contract sync, maintainability, privacy, and gitleaks-equivalent checks;
- lint and targeted/full test suites proportionate to the changed files;
- a final search proving retired TypeScript database helpers are absent from active source exports.

## Non-Goals

- Changing trading or analytics behavior.
- Rewriting historical implementation plans.
- Introducing a second backend or compatibility path.
- Optimizing CI by weakening required checks.
