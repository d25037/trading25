---
id: bt-026
title: "Phase 4: Python ドメインパッケージ分離"
status: open
priority: medium
labels: [architecture, refactor]
project: bt
created: 2026-02-09
updated: 2026-02-09
depends_on: []
blocks: []
parent: null
---

# bt-026 Phase 4: Python ドメインパッケージ分離

## 目的
`apps/bt/src` の DB/指標/バックテスト責務を明確な境界へ再配置し、`server` と CLI を thin adapter 化する。

## 受け入れ条件
- `apps/bt/src/lib/market_db`, `dataset_io`, `indicators`, `backtest_core`, `strategy_runtime` の境界が作成される
- `apps/bt/src/server` と `apps/bt/src/cli_*` が新境界を経由して依存する
- API/CLI の既存挙動に回帰がない（既存テスト + 追加回帰テストで確認）
- lint/typecheck/test が通る

## 実施内容
- Phase 4C: `src/server/db` と dataset I/O の再配置
- 指標計算・backtest 実行・strategy runtime の責務分割
- import 依存とモジュール境界の整理
- 段階移行中の互換 import 方針を定義

## 結果

## 補足
- 参照: `docs/unified-roadmap.md` Phase 4（再ベースライン）
