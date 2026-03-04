---
id: ts-135
title: "packages/shared を packages/core へ一括リネーム"
status: open
priority: high
labels: [refactor, packages, breaking-change, openapi]
project: ts
created: 2026-03-04
updated: 2026-03-04
depends_on: []
blocks: []
parent: null
---

# ts-135 packages/shared を packages/core へ一括リネーム

## 目的
- `apps/ts/packages/shared` の責務実態に合わせ、パッケージ名と配置を `core` に統一する。
- 後方互換レイヤ（`@trading25/shared` の re-export など）は導入せず、一度で参照先を置換する。

## 受け入れ条件
- ディレクトリが `apps/ts/packages/core` に移行され、`apps/ts/packages/shared` が存在しない。
- パッケージ名が `@trading25/core` に変更され、`@trading25/shared` 参照がリポジトリ内から解消される。
- workspace 設定、build/test/typecheck/lint スクリプト、CI 参照が新パス・新名に一致する。
- OpenAPI 型生成フロー（`bt:sync`）と型整合チェックが `core` 前提で成功する。
- `bun run workspace:test` と `bun run quality:typecheck` が成功する。

## 実施内容
- [ ] `apps/ts/packages/shared` を `apps/ts/packages/core` にリネーム
- [ ] `apps/ts/packages/core/package.json` の `name` を `@trading25/core` に更新
- [ ] `apps/ts/package.json` の workspace / scripts / filter 指定を `core` へ更新
- [ ] `apps/ts/packages/*`（`web`, `cli`, `api-clients` など）の import specifier を `@trading25/core` に一括置換
- [ ] OpenAPI 生成・型チェック関連（`bt:sync`, `check-bt-types`, generated path 参照）を `core` 配下へ更新
- [ ] `bun.lock` と関連設定を再生成し、依存解決の不整合を解消
- [ ] README / docs / issue テンプレート内の `shared` 記述を `core` に更新（必要箇所のみ）
- [ ] 検証コマンドを実行
  - [ ] `bun run workspace:test`
  - [ ] `bun run quality:typecheck`
  - [ ] `bun run quality:lint`
  - [ ] `bun run --filter @trading25/shared bt:sync` 旧指定の廃止確認
  - [ ] `bun run --filter @trading25/core bt:sync` 実行確認

## 結果
- 未着手

## 補足
- このIssueは breaking change を許容する前提で実施する。
- `contracts/` の SoT は維持し、OpenAPI 契約更新が発生した場合は `bt:sync` とセットで反映する。
