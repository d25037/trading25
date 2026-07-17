# Task 6 Report: Refresh AGENTS and active contract guidance

## Scope

Implemented Task 6 only in `/Users/mirage/dev/trading25/.worktrees/repo-governance-modernization`.

The pre-existing shared-worktree changes below were preserved and excluded from the Task 6 commits:

- `.superpowers/sdd/task-3-report.md`
- `.superpowers/sdd/task-5-report.md`
- `docs/superpowers/plans/2026-07-17-repository-governance-modernization.md`
- `docs/superpowers/specs/2026-07-17-repository-governance-modernization-design.md`

## Source-of-truth audit

The active guidance was checked against repository-owned sources before editing:

- `.github/workflows/ci.yml`: Bun `1.3.14`.
- `apps/ts/package.json`: `@biomejs/biome` `^2.5.3`.
- `apps/bt/pyproject.toml` and `apps/bt/uv.lock`: `vectorbt>=1.1.0` / lock `1.1.0`, `fastapi>=0.139.0` / lock `0.139.0`, and `uvicorn[standard]>=0.51.0` / lock `0.51.0`.
- `apps/bt/src/shared/paths/constants.py`: `experimental` / `production` / `legacy` are external categories, `reference` is project-owned, and `SEARCH_ORDER` is experimental → production → reference → legacy.
- `apps/ts/packages/contracts/openapi/bt-openapi.json`: fundamentals GET query fields `from`, `to`, `periodType`, `preferConsolidated`, `tradingValuePeriod`, and `forecastEpsLookbackFyCount` are optional; the POST body requires only `symbol`; response `asOfDate` is required.
- `apps/bt/src/entrypoints/cli/market_cutover.py` and `apps/bt/src/application/services/market_v4_cutover/`: `cutover` is the explicit full-rebuild family; `promote-retained` promotes exact retained provenance without rebuilding and is bound to exact IDs and recovery evidence.

## TDD and writing-skills evidence

Contract/drift tests were added before any AGENTS or skill edit.

```bash
uv run --directory apps/bt pytest tests/unit/scripts/test_audit_skills.py -q
```

RED: **106 collected, 7 failed, 99 passed**. The failures were the intended baseline:

1. stale bt/ts dependency and runtime versions;
2. `.py:line-range` references and volatile signal/stock/line counts;
3. missing strategy category ownership wording and the incorrect “3 layers” label;
4. incomplete retained-promotion recovery and immutability guidance in both market skills;
5. fundamentals optional request fields conflated with required response `asOfDate`;
6. missing TypeScript filesystem Data Plane prohibition.

GREEN with the same retrieval/contract scenarios: **106 passed, 1 warning in 0.39s**.

The tests derive values directly from manifests, lock data, CI, runtime constants, and OpenAPI where stable, so future source changes require the active guidance to move with them.

## Implementation

- Replaced stale bt dependency ranges with current manifest/lock versions and ts tooling with Bun `1.3.14` / Biome `^2.5.3`.
- Replaced source line ranges with stable symbols and removed volatile signal inventory, stock-universe result, and deleted-line counts.
- Corrected strategy ownership to three external XDG categories plus project-owned `reference`, while retaining resolver fallback guidance.
- Marked XDG paths in ts guidance as backend-owned reference information; TypeScript dataset filesystem/path helpers and direct Data Plane reads/writes are prohibited.
- The two market skills now distinguish explicit full rebuild from exact retained promotion and include provenance, create-only immutable backup, atomic exchange, process-local continuation authorization, same-ID recovery, joined exact rollback, unjoined dual-lease deferred fencing, retained backup/quarantine immutability, and journal-bound post-commit cleanup.
- Retained promotion explicitly prohibits sync, reset, repair, stock refresh, intraday sync, adjusted-metric materialization, rebuild, and J-Quants calls; success requires `noSync: true` / `noJQuants: true`, exact identity, semantic smoke, and join evidence.
- Financial guidance now distinguishes optional GET/POST request options from required response `asOfDate`.

## Files

- `.codex/skills/bt-database-management/SKILL.md`
- `.codex/skills/bt-market-sync-strategies/SKILL.md`
- `.codex/skills/bt-strategy-config/SKILL.md`
- `.codex/skills/ts-dataset-management/SKILL.md`
- `.codex/skills/ts-financial-analysis/SKILL.md`
- `apps/bt/AGENTS.md`
- `apps/bt/tests/unit/scripts/test_audit_skills.py`
- `apps/ts/AGENTS.md`
- `.superpowers/sdd/task-6-report.md`

## Verification

```bash
uv run --directory apps/bt pytest tests/unit/scripts/test_audit_skills.py -q
uv run --directory apps/bt pytest tests/unit/scripts -q
python3 scripts/skills/audit_skills.py --strict-legacy
bun --cwd="$PWD/apps/ts" run --filter @trading25/contracts bt:check
uv run --directory apps/bt ruff check tests/unit/scripts/test_audit_skills.py
rg -n '3層構造|35種類シグナル|\.py:[0-9]+(?:-[0-9]+)?|Bun 1\.3\.8|Biome 2\.4\.15|vectorbt.*>=1\.0\.0|fastapi.*>=0\.136\.1|uvicorn.*>=0\.47\.0' apps/bt/AGENTS.md apps/ts/AGENTS.md .codex/skills/bt-market-sync-strategies/SKILL.md .codex/skills/bt-database-management/SKILL.md .codex/skills/bt-strategy-config/SKILL.md .codex/skills/ts-financial-analysis/SKILL.md .codex/skills/ts-dataset-management/SKILL.md
git diff --check
```

Results:

- focused skill audit tests: **106 passed, 1 warning in 0.39s**;
- full scripts unit suite: **510 passed, 1 warning in 9.89s**;
- strict skill audit: **Skill audit passed**;
- OpenAPI source/snapshot/generated-type and handwritten-wire-DTO drift gate: **PASS**;
- Ruff: **All checks passed**;
- banned active-guidance search: no matches (expected `rg` exit 1);
- whitespace/diff check: passed.

## Commit

Implementation:

```text
3f523a00a9b3cd3b926406f1a3d30e470fbea849
docs(governance): refresh active repository contracts
```
