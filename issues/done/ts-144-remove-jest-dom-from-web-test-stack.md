---
id: ts-144
title: "web テスト基盤から jest-dom 依存を段階除去する"
status: done
priority: medium
labels: [testing, dependencies, cleanup, vitest]
project: ts
created: 2026-03-24
updated: 2026-03-24
depends_on: []
blocks: []
parent: null
---

# ts-144 web テスト基盤から jest-dom 依存を段階除去する

## 目的
- `apps/ts/packages/web` のテスト基盤で使っている `@testing-library/jest-dom` を依存から外し、Vitest + Testing Library + 必要最小限のローカル helper だけで回せる状態へ移行する。
- matcher 依存を縮小し、Jest 由来の前提を減らして test stack を単純化する。

## 受け入れ条件
- `apps/ts/packages/web/package.json` から `@testing-library/jest-dom` が削除されている。
- `apps/ts/packages/web/src/test-setup.ts` から `import '@testing-library/jest-dom'` が除去されている。
- 既存 test が `jest-dom` matcher に依存せず通る。
- 置換後も test の可読性が大きく劣化しないよう、必要な範囲の local helper / matcher が整備されている。
- `bun run --filter @trading25/web test` と `bun run --filter @trading25/web typecheck` が通る。

## 実施内容
- [x] `jest-dom` matcher の実使用を分類し、置換方針を `direct DOM check` と `local helper/matcher` に分ける。
- [x] 使用頻度の高い matcher (`toBeInTheDocument`, `toBeDisabled`, `toHaveAttribute`, `toHaveClass`, `toHaveTextContent`) の置換ガイドを決める。
- [x] 可読性維持のため、必要なら `src/test-utils` 配下に薄い assert helper または Vitest matcher extension を追加する。
- [x] test file を段階置換し、`jest-dom` import を外しても green になる状態まで進める。
- [x] `package.json` と `test-setup.ts` から依存を除去し、lockfile と test policy を更新する。

## 結果
- `apps/ts/packages/web/src/test-dom-matchers.ts` に web test suite 向けの薄い local matcher 群を追加し、`toBeInTheDocument` / `toBeDisabled` / `toBeEnabled` / `toHaveAttribute` / `toHaveClass` / `toHaveTextContent` / `toBeChecked` / `toHaveValue` / `toHaveStyle` / `toBeEmptyDOMElement` を Vitest `expect.extend` で提供するようにした。
- `apps/ts/packages/web/src/types/vitest-dom-matchers.d.ts` で matcher 型を追加し、`apps/ts/packages/web/src/test-setup.ts` から local matcher を登録する形に切り替えた。
- `apps/ts/packages/web/package.json` から `@testing-library/jest-dom` を削除し、`apps/ts/bun.lock` と `apps/ts/TESTING.md` を更新した。
- 検証:
  - `bun run --filter @trading25/web typecheck`
  - `bun run --filter @trading25/web test`

## 補足
- 2026-03-24 時点では `jest-dom` はまだ使用中。
  - 定義: `apps/ts/packages/web/package.json`
  - 読み込み: `apps/ts/packages/web/src/test-setup.ts`
- 使用規模は小さくない。
  - DOM matcher を使う test file は 138 件中 85 件
  - 主な使用回数:
    - `toBeInTheDocument`: 896
    - `toBeDisabled`: 33
    - `toHaveAttribute`: 22
    - `toHaveClass`: 15
    - `toHaveTextContent`: 11
    - `toBeChecked`: 8
    - `toHaveValue`: 5
- そのため、単純な一括削除ではなく、以下の段階移行が前提:
  1. 高頻度 matcher の代替方針を固定する
  2. helper / matcher extension を必要最小限だけ内製化する
  3. 主要 matcher の利用箇所を置換する
  4. 最後に dependency を削除する
- `jest-dom` を消すこと自体が目的化しないよう、test の可読性と保守性を維持することを優先する。
