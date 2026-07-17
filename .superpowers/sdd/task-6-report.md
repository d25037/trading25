# Task 6 Report: Refresh AGENTS and active contract guidance

## Scope

Implemented Task 6 only in the isolated `repo-governance-modernization` worktree.

The pre-existing shared-worktree changes below were preserved and excluded from the Task 6 commits:

- `.superpowers/sdd/task-3-report.md`
- `.superpowers/sdd/task-5-report.md`
- `docs/superpowers/plans/2026-07-17-repository-governance-modernization.md`
- `docs/superpowers/specs/2026-07-17-repository-governance-modernization-design.md`

## Source-of-truth audit

The active guidance was checked against repository-owned sources before editing:

- `.github/workflows/ci.yml`: Bun `1.3.14`.
- `apps/ts/package.json`: `@biomejs/biome` `^2.5.3`.
- `apps/bt/pyproject.toml` and `apps/bt/uv.lock`: `vectorbt>=1.1.0` / lock `1.1.0`, `pydantic>=2.13.4` / lock `2.13.4`, `fastapi>=0.139.0` / lock `0.139.0`, and `uvicorn[standard]>=0.51.0` / lock `0.51.0`.
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

### Review-fix RED/GREEN

The four review findings received new tests before their fixes. The first review RED was **122 collected, 7 failed, 115 passed**:

- the manifest-derived dependency test found stale `pydantic>=2.0.0`;
- the stable-guidance test found `100倍以上の高速化`;
- AST-derived `SEARCH_ORDER` was absent from the strategy skill;
- both market skills passed after structural promotion clauses were removed;
- both market skills accepted manual lock/journal/staging mutation allowance language.

The dependency test now discovers every single-package Core bullet backed by a manifest `>=` requirement and checks its exact manifest requirement and lock version; it has no selective vectorbt/FastAPI/uvicorn tuple. Category ownership and search order are parsed from `constants.py` with `ast.parse` / `ast.literal_eval` and compared with both AGENTS and the strategy skill.

The first review GREEN was **122 passed**. REFACTOR cases then proved the contradiction detector still accepted reverse word order such as “sync is allowed,” “J-Quants calls are permitted,” “rebuild is allowed,” and “manual lock changes are allowed”: **130 collected, 8 failed, 122 passed**. The final detector handles allowance-before and allowance-after forms. Final review GREEN: **130 passed, 1 warning in 0.29s**.

### Remaining-P2 RED/GREEN

The remaining three review findings also received failing tests before implementation. RED was **148 collected, 15 failed, 133 passed**:

- removing any of `retained report provenance`, `source root`, `command 内で`, `create-only immutable backup`, or `atomic exchange` from either market skill was not rejected;
- affirmative Japanese guidance for `sync を実行する`, `J-Quants を呼び出す`, `rebuild する`, and `lock / journal / staging を手動変更する` was accepted in both market skills;
- the performance heuristic missed ASCII/full-width `x`, English `times`, full-width percent, and Japanese performance/velocity wording.

GREEN with the same cases was **148 passed, 1 warning in 0.42s**. Separate acceptance cases confirm that the exact Japanese negative forms (`実行しない`, `呼び出さない`, `しない`, and `手動変更しない`) remain valid.

### Final-P2 RED/GREEN

The final operator non-mutation review finding was reproduced by mutating the actual contract sentence in each market skill rather than appending synthetic guidance. RED was **152 collected, 4 failed, 148 passed**:

- deleting `lock / journal / staging を手動変更せず` from `bt-database-management` was not rejected;
- deleting `operator は lock / journal / staging を手動変更しない` from `bt-market-sync-strategies` was not rejected;
- reversing either actual negative fragment to `lock / journal / staging を手動変更する` was not rejected as a contradiction.

GREEN was **152 passed, 1 warning in 0.31s**. The required clause matches the lock/journal/staging object sequence plus explicit negative manual-modification semantics without depending on a subject or sentence order. The affirmative reversal detector is likewise independent of a preceding `promote-retained` subject on the same line.

## Implementation

- Replaced stale bt dependency ranges with current manifest/lock versions and ts tooling with Bun `1.3.14` / Biome `^2.5.3`.
- The Core dependency drift test now derives all documented single-package lower bounds from the manifest and lock, including pydantic.
- Replaced source line ranges with stable symbols and removed volatile signal inventory, stock-universe result, and deleted-line counts.
- Removed the quantitative VectorBT speedup claim. The audit heuristic rejects ASCII/full-width decimal numbers paired with `x` / `X` / `ｘ` / `Ｘ`, `time` / `times`, `倍`, or `%` / `％` when they occur within 24 non-sentence-breaking characters of the recognized performance vocabulary `faster`, `speed`, `speedup`, `performance`, `improve` / `improved` / `improvement`, `高速`, `高速化`, `性能`, `速度`, `改善`, or `向上`. This is intentionally a bounded pattern check, not a claim to understand all natural-language performance statements; fixed configuration numbers without performance vocabulary, including display scale and fixed multiplier/percentage settings, remain allowed.
- Corrected strategy ownership to three external XDG categories plus project-owned `reference`, while retaining resolver fallback guidance.
- Strategy ownership and search-order tests now AST-parse the source constants instead of comparing hardcoded source literals.
- Marked XDG paths in ts guidance as backend-owned reference information; TypeScript dataset filesystem/path helpers and direct Data Plane reads/writes are prohibited.
- The two market skills now distinguish explicit full rebuild from exact retained promotion and include provenance, create-only immutable backup, atomic exchange, process-local continuation authorization, same-ID recovery, joined exact rollback, unjoined dual-lease deferred fencing, retained backup/quarantine immutability, and journal-bound post-commit cleanup.
- Retained promotion explicitly prohibits sync, reset, repair, stock refresh, intraday sync, adjusted-metric materialization, rebuild, and J-Quants calls; success requires `noSync: true` / `noJQuants: true`, exact identity, semantic smoke, and join evidence.
- The strict skill audit now validates the exact retained CLI, retained-report provenance and source-root resolution, command-local create-only immutable backup and atomic exchange, four identity fields, smoke/join evidence, process-local same-ID recovery, joined rollback, unjoined dual-lease fencing, journal-bound cleanup, the actual operator lock/journal/staging non-mutation clause, immutable backup/quarantine, and explicit operation/J-Quants prohibitions. It rejects positive allowance language for sync/J-Quants/rebuild and manual state mutation in either English word order, plus the scoped affirmative Japanese forms. The Japanese lock/journal/staging affirmative reversal is detected independently of line subject/order, while the exact negative forms remain valid.
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
- `scripts/skills/audit_skills.py`
- `.superpowers/sdd/task-6-report.md`

## Verification

```bash
uv run --directory apps/bt pytest tests/unit/scripts/test_audit_skills.py -q
uv run --directory apps/bt pytest tests/unit/scripts -q
python3 scripts/skills/audit_skills.py --strict-legacy
bun --cwd="$PWD/apps/ts" run --filter @trading25/contracts bt:check
uv run --directory apps/bt ruff check ../../scripts/skills/audit_skills.py tests/unit/scripts/test_audit_skills.py
rg -n '3層構造|35種類シグナル|\.py:[0-9]+(?:-[0-9]+)?|Bun 1\.3\.8|Biome 2\.4\.15|vectorbt.*>=1\.0\.0|fastapi.*>=0\.136\.1|uvicorn.*>=0\.47\.0' apps/bt/AGENTS.md apps/ts/AGENTS.md .codex/skills/bt-market-sync-strategies/SKILL.md .codex/skills/bt-database-management/SKILL.md .codex/skills/bt-strategy-config/SKILL.md .codex/skills/ts-financial-analysis/SKILL.md .codex/skills/ts-dataset-management/SKILL.md
git diff --check
```

Results:

- focused skill audit tests: **152 passed, 1 warning in 0.31s**;
- full scripts unit suite: **556 passed, 1 warning in 10.27s**;
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

Initial verification report:

```text
6d8c9303598e56b7335333c323a24b3c05f83a20
docs(governance): record task 6 verification
```

Review fixes:

```text
c24cd60524f8f6c0d14427a8d53ee462e643a9bd
fix(governance): harden active guidance drift checks

34610d36d5fa215d3608d772979dee62b0d31946
docs(governance): record task 6 review fixes

afea0071d37af85efac13136b50e806b21cc2114
fix(governance): close retained guidance audit gaps

a7362a04c293141ab8204ed76f04fe2fc5286fd9
docs(governance): record remaining task 6 fixes

e03ee72fbd70f75c1aece86ed6602f2ab160e751
fix(governance): require operator non-mutation contract
```
