---
id: bt-042
title: "Worker runtime と durable execution control を導入"
status: open
priority: high
labels: [jobs, worker, execution, cancellation, bt]
project: bt
created: 2026-03-08
updated: 2026-03-08
depends_on: [bt-039]
blocks: [bt-044]
parent: bt-037
---

# bt-042 Worker runtime と durable execution control を導入

## 目的
- `FastAPI + asyncio.create_task + ThreadPoolExecutor` 依存から脱し、API plane と execution plane を分離する。
- cancel を status control ではなく durable execution control に近づける。

## 受け入れ条件
- [ ] backtest / optimize / lab の長時間処理が API プロセス外 worker で実行される。
- [ ] job lease / startedAt / finishedAt / timeout / cancel state が durable に管理される。
- [ ] worker 側で cooperative cancel もしくは process-level stop が成立する。
- [ ] SSE / status API が worker 実行モデルに追従する。

## 実施内容
- [ ] worker 実行方式を選定し実装する。
- [ ] job persistence と lease モデルを定義する。
- [ ] cancellation / timeout / retry のライフサイクルを統一する。
- [ ] observability と metrics を新実行モデルへ接続する。

## 結果
- 未着手

## 補足
- 参照: `docs/backtest-greenfield-rebuild.md` Section 2.2, 4.1, 10

