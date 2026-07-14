# Task 5 Report: TypeScript Factor Regression Contract Convergence

## Status

Complete. Stock and portfolio Factor Regression wire responses now use stable aliases derived from the committed generated OpenAPI types. The stock and portfolio index-match shapes remain distinct. Only the two local request parameter interfaces remain handwritten in the analytics client.

## Files

- `apps/ts/packages/contracts/src/types/api-response-types.ts`
- `apps/ts/packages/contracts/src/types/api-response-types.test.ts`
- `apps/ts/packages/contracts/src/types/api-types.ts`
- `apps/ts/packages/api-clients/src/analytics/types.ts`
- `apps/ts/packages/api-clients/src/analytics/index.ts`
- `apps/ts/packages/api-clients/type-tests/factor-regression-contracts.ts`
- `apps/ts/packages/web/src/hooks/useFactorRegression.ts`
- `apps/ts/packages/web/src/hooks/useFactorRegression.test.tsx`
- `apps/ts/packages/web/src/components/Chart/FactorRegressionPanel.tsx`
- `apps/ts/packages/web/src/components/Chart/FactorRegressionPanel.test.tsx`

`apps/ts/packages/api-clients/tsconfig.type-tests.json` was not changed because its existing `type-tests/**/*.ts` include already places the new negative type-test in normal `typecheck:tests` execution.

## RED

- `bun run --filter @trading25/contracts test`: 32 passed; runtime fixtures were accepted because Bun does not typecheck test sources.
- `bun run --filter @trading25/api-clients typecheck:tests`: failed as intended with TS2305 for the missing canonical aliases and TS2578 on the then-insensitive `@ts-expect-error`.

## Generated Identity

- OpenAPI snapshot HEAD/worktree object: `4870b569260495d95774dcbd4cce4dbe671f1f2f` / identical.
- Generated `bt-api-types.ts` HEAD/worktree object: `645c879f4e9ce80d38acfdd31c8e1743805b338b` / identical.
- `bun run --filter @trading25/contracts bt:check`: passed and left both generated artifacts unchanged.

## Negative Type Sensitivity Proof

Temporarily changed `PortfolioFactorRegressionIndexMatch` to `FactorRegressionIndexMatch`. `bun run --filter @trading25/api-clients typecheck:tests` then failed with:

- TS2353 for portfolio-only `code`;
- TS1360 because the response's portfolio match array did not satisfy the temporary stock alias;
- TS2578 because assigning a stock match to the deliberately wrong portfolio alias no longer errored.

The temporary change was reverted. The same command then passed.

## GREEN Gates

- contracts tests: 32 passed, 0 failed, 71 expectations.
- api-clients tests: 87 passed, 0 failed, 214 expectations.
- api-clients source + dedicated type-tests: passed.
- focused web Factor Regression tests: 2 files, 9 tests passed.
- workspace `quality:typecheck`: passed, including regenerated contract types, root, api-clients, extension, and web checks.
- `quality:deps:audit`: passed, 6 manifests checked.
- focused Biome check: 9 files checked, no fixes required.
- skill reference audit: passed.

## Diff and Generated Status

- Handwritten `ApiIndexMatch`, `ApiFactorRegressionResponse`, `ApiPortfolioWeight`, `ApiExcludedStock`, and `ApiPortfolioFactorRegressionResponse` were removed.
- Handwritten analytics `IndexMatch`, response, weight, and excluded-stock DTOs were removed.
- No deprecated aliases, runtime conversion, field renaming, OpenAPI snapshot changes, or generated TypeScript changes remain.
- `git diff --check`: passed.

## Commit

Commit message: `refactor(ts): converge factor regression contracts` (this task commit; no push).

## Self-review

- All eight required stable aliases are exported from contracts and re-exported by the analytics client.
- Nested aliases derive from response fields, avoiding collision-qualified generated component names.
- Stock consumers use the stock match alias; portfolio types preserve the generated `{ code, name, rSquared }` match shape.
- Existing hook and panel tests were strengthened in place; no duplicate web fixture/test file was added.
- Request parameters remain local and endpoint/runtime behavior is unchanged.

## Concerns

None.
