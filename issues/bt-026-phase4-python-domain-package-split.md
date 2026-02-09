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

## 進捗
- [x] Step1: `apps/bt/src/lib/market_db`, `apps/bt/src/lib/dataset_io` を作成
- [x] Step1: `src/server/db/*` と `dataset_writer` を新境界へ実体移管
- [x] Step1: `src/server` 側 import を `src.lib.*` へ切替
- [x] Step1: `src/server/db/*` に互換 re-export を配置（段階移行）
- [ ] Step2: `apps/bt/src/lib/indicators` 分離
- [ ] Step2: `apps/bt/src/lib/backtest_core` 分離
- [ ] Step2: `apps/bt/src/lib/strategy_runtime` 分離

## 結果
- 2026-02-09: Phase 4C Step1（DB + dataset I/O 分離）を完了
- `src/server/db` の実装本体を `src/lib/market_db` へ移管し、`DatasetWriter` を `src/lib/dataset_io` へ移管
- `src/server/routes` / `src/server/services` / `src/server/app.py` の参照先を新境界へ切替
- 互換レイヤとして `src/server/db/*.py` は re-export facade として維持
- 検証結果
  - `uv run ruff check src tests`: passed
  - `uv run pyright src`: 0 errors（既存 warning 1）
  - `uv run pytest tests/unit/server/db tests/unit/server/services tests/unit/server/routes`: 465 passed
  - `uv run pytest tests/server tests/integration`: 387 passed

## 補足
- 参照: `docs/unified-roadmap.md` Phase 4（再ベースライン）
