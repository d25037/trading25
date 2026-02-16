---
id: ts-130
title: "ts CLI 縮退と Web 移植の段階計画"
status: done
priority: medium
labels: [cli, web, migration, ops]
project: ts
created: 2026-02-16
updated: 2026-02-16
depends_on: []
blocks: []
parent: null
---

# ts-130 ts CLI 縮退と Web 移植の段階計画

## 目的
必要機能の frontend 移植が進んだ現状に合わせて `apps/ts/packages/cli` の責務を再定義し、  
「残す CLI（運用・自動化）」と「Web へ移す/廃止する CLI」を段階的に整理する。

## 受け入れ条件
- 残存させる最小 CLI セットが明文化される（`db` / `dataset` / `jquants fetch` / 自動化向け `backtest`・`analysis`）
- Web 側へ移植する対象コマンドが明文化される（優先度・段階付き）
- 廃止対象 CLI を削除し、代替導線（Web）を明示する
- `jquants auth` の API 契約不整合（CLI 側型・コマンド定義との齟齬）を解消する計画が含まれる
- ドキュメント（README / 運用手順）に CLI と Web の責務境界が反映される

## 実施内容
- CLI 全コマンドの現状棚卸しと「Keep / Migrate / Deprecate」分類
- Keep 対象の保守（JSON/CSV 出力、exit code、運用スクリプト互換）を明確化
- Migrate 対象の Web 実装チケット分解
- Migrate 完了済み対象（portfolio/watchlist）の CLI を削除し Web 導線へ統一
- `jquants auth` の契約差分修正（OpenAPI SoT に一致させる）

## 結果
- `portfolio` / `watchlist` CLI コマンド群を削除
  - 削除: `apps/ts/packages/cli/src/commands/portfolio/*`
  - 削除: `apps/ts/packages/cli/src/commands/watchlist/*`
  - 更新: `apps/ts/packages/cli/src/index.ts`（ルーティング/ヘルプから除去）
- CLI 契約整合を実施
  - `jquants auth` を API key ステータス前提に修正（OpenAPI SoT 整合）
  - `refresh-tokens` は互換エイリアス化し、`status` へ誘導
- 責務境界ドキュメントを追加
  - 追加: `docs/ts-cli-scope.md`（Keep / Removed / Guardrails）
- README を更新
  - 更新: `apps/ts/README.md`（CLIスコープと例を現行化）
  - 更新: `README.md`（portfolio/watchlist は web 導線に統一）
- 検証
  - `bun run --filter @trading25/cli typecheck` 成功
  - `bun run --filter @trading25/cli test` 成功（53 pass / 0 fail）

## 補足
- 現時点の暫定分類:
  - Keep: `db`, `dataset`(sample/search/info), `jquants fetch`, ヘッドレス用途の `backtest/analysis`
  - Migrate/Deprecate候補: `portfolio`, `watchlist`, 日常操作としての `backtest/analysis`
- Proxy 本体は `bt` FastAPI (`/api/jquants/*`) であり、`ts/cli` はクライアント層
