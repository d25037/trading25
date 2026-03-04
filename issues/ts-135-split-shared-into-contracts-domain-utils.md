---
id: ts-135
title: "packages/shared を責務別3パッケージへ一括分割（core中間なし）"
status: open
priority: high
labels: [refactor, architecture, packages, breaking-change, openapi]
project: ts
created: 2026-03-04
updated: 2026-03-04
depends_on: []
blocks: []
parent: null
---

# ts-135 packages/shared を責務別3パッケージへ一括分割（core中間なし）

## 目的
- `apps/ts/packages/shared` を中間段階なしで最終構成へ移行し、依存境界を明確化する。
- `@trading25/core` を経由せず、`contracts` / `domain` / `utils` の3パッケージへ一括分割する。
- 後方互換レイヤ（`@trading25/shared` の re-export など）は導入しない。

## 受け入れ条件
- `apps/ts/packages/shared` が削除され、以下3パッケージが追加される。
  - `apps/ts/packages/contracts`
  - `apps/ts/packages/domain`
  - `apps/ts/packages/utils`
- `@trading25/shared` と `@trading25/core` の参照がリポジトリ内から解消される。
- `bt:sync` と OpenAPI 型整合チェックが `@trading25/contracts` に集約される。
- `apps/ts/package.json` の workspaces / scripts / tasks が新パッケージ構成に一致する。
- `web` / `cli` / `api-clients` の import と dependencies が新パッケージ参照に更新される。
- `bun run quality:typecheck` / `bun run workspace:test` / `bun run workspace:build` が成功する。

## 実施内容
- [ ] `packages/contracts` を作成し、OpenAPI generated 型・API response 型・型整合チェックを移設
- [ ] `packages/domain` を作成し、dataset/portfolio/watchlist/portfolio-performance を移設
- [ ] `packages/utils` を作成し、logger/env/date/path など汎用 utility を移設
- [ ] `apps/ts/packages/shared` を削除し、残存参照を完全解消
- [ ] `apps/ts/package.json` の workspace / scripts を新パッケージ構成へ更新
- [ ] `apps/ts/scripts/tasks.ts` の task 名称・filter・説明を新構成へ更新
- [ ] `apps/ts/tsconfig.json` と package 個別 `tsconfig` の `paths` を新構成へ更新
- [ ] `apps/ts/packages/*`（`web`, `cli`, `api-clients`）の import specifier / dependencies を一括置換
- [ ] `bun.lock` と関連設定を再生成し、依存解決の不整合を解消
- [ ] README / AGENTS / docs の import 例と運用手順を新パッケージ名へ更新
- [ ] 検証コマンドを実行
  - [ ] `bun run quality:typecheck`
  - [ ] `bun run workspace:test`
  - [ ] `bun run workspace:build`
  - [ ] `bun run quality:lint`
  - [ ] `bun run --filter @trading25/contracts bt:sync`

## 結果
- 未着手

## 補足
- このIssueは breaking change を許容する前提で実施する（同時移行・一括置換）。
- `ts-136` と `ts-137` の段階分割案は本Issueへ統合し superseded とする。
- `contracts/` の SoT は維持し、OpenAPI 契約更新が発生した場合は `bt:sync` とセットで反映する。
