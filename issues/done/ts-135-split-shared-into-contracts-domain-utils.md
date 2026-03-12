---
id: ts-135
title: "packages/shared を責務別3パッケージへ一括分割（core中間なし）"
status: done
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
- [x] `packages/contracts` を作成し、OpenAPI generated 型・API response 型・型整合チェックを移設
- [x] `packages/domain` を作成し、dataset/portfolio/watchlist/portfolio-performance を移設
- [x] `packages/utils` を作成し、logger/env/date/path など汎用 utility を移設
- [x] `apps/ts/packages/shared` を削除し、残存参照を完全解消
- [x] `apps/ts/package.json` の workspace / scripts を新パッケージ構成へ更新
- [x] `apps/ts/scripts/tasks.ts` の task 名称・filter・説明を新構成へ更新
- [x] `apps/ts/tsconfig.json` と package 個別 `tsconfig` の `paths` を新構成へ更新
- [x] `apps/ts/packages/*`（`web`, `cli`, `api-clients`）の import specifier / dependencies を一括置換
- [x] `bun.lock` と関連設定を再生成し、依存解決の不整合を解消
- [x] README / AGENTS / docs の import 例と運用手順を新パッケージ名へ更新
- [x] 検証コマンドを実行
  - [x] `bun run quality:typecheck`
  - [x] `bun run workspace:test`
  - [x] `bun run workspace:build`
  - [x] `bun run quality:lint`
  - [x] `bun run --filter @trading25/contracts bt:sync`

## 結果
- `apps/ts/packages/contracts` / `apps/ts/packages/domain` / `apps/ts/packages/utils` を新設し、`shared` の責務を分割移設
- `apps/ts/packages/shared` を削除し、active code/docs/scripts から `@trading25/shared` 参照を解消（archive / done issues を除く）
- `apps/ts` の workspace 設定・task runner・tsconfig paths・web alias/dependencies を新パッケージ構成に更新
- ルート運用スクリプト（`scripts/check-contract-sync.sh` / `scripts/check-dep-direction.sh` / `scripts/dep-direction-allowlist.txt`）と skills 参照生成を新パスへ更新
- 実行結果:
  - `bun run quality:typecheck` ✅
  - `bun run workspace:test` ✅
  - `bun run workspace:build` ✅
  - `bun run quality:lint` ✅（warning 3件は既存ルール警告、exit 0）
  - `bun run --filter @trading25/contracts bt:sync` ✅（ローカル生成不可時は snapshot fallback で継続）

## 補足
- このIssueは breaking change を許容する前提で実施する（同時移行・一括置換）。
- `ts-136` と `ts-137` の段階分割案は本Issueへ統合し superseded とする。
- `contracts/` の SoT は維持し、OpenAPI 契約更新が発生した場合は `bt:sync` とセットで反映する。
