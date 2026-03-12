---
id: bt-042
title: "Worker runtime と durable execution control を導入"
status: done
priority: high
labels: [jobs, worker, execution, cancellation, bt]
project: bt
created: 2026-03-08
updated: 2026-03-09
depends_on: [bt-039]
blocks: [bt-044]
parent: bt-037
---

# bt-042 Worker runtime と durable execution control を導入

## 目的
- `FastAPI + asyncio.create_task + ThreadPoolExecutor` 依存から脱し、API plane と execution plane を分離する。
- cancel を status control ではなく durable execution control に近づける。

## 受け入れ条件
- [x] backtest / optimize / lab の長時間処理が API プロセス外 worker で実行される。
- [x] job lease / startedAt / finishedAt / timeout / cancel state が durable に管理される。
- [x] worker 側で cooperative cancel もしくは process-level stop が成立する。
- [x] SSE / status API が worker 実行モデルに追従する。

## 実施内容
- [x] worker 実行方式を選定し実装する。
- [x] job persistence と lease モデルを定義する。
- [x] cancellation / timeout / retry のライフサイクルを統一する。
- [x] observability と metrics を新実行モデルへ接続する。

## 結果
- `portfolio.db.jobs` に `lease_owner` / `lease_expires_at` / `last_heartbeat_at` / `cancel_requested_at` / `cancel_reason` / `timeout_at` を追加し、`JobManager` が durable に hydrate / persist できるようにした。
- backtest / optimize / lab / screening の job status response に `execution_control` を追加し、single-writer 前提の in-process 実行でも future worker runtime に必要な control state を観測できるようにした。
- startup で orphaned `pending/running` job を回収し、shutdown で active job を durable cancel してから DB close する順序へ修正した。startup reconciliation は lease heartbeat が新しい job を即時回収しないようにし、外部 worker 継続の余地を残した。
- shutdown 時の cancel は terminal 化ではなく `cancel_requested_at` を durable に残す方式へ寄せ、attached task があれば best-effort cancel し、未収束 job は次回 startup reconciliation で回収する形にした。
- `backtest` は first slice として API プロセス外 subprocess worker へ切り出した。API 側は watcher task で subprocess を監視し、worker 側は `claim/heartbeat/set_result/complete/fail` を `portfolio.db` に書き込む。
- `optimize` も同じ worker envelope へ移し、`best_score / best_params / worst_score / total_combinations / html_path` と raw payload を durable に保存するようにした。
- `lab generate/evolve/optimize/improve` も同じ worker envelope へ移し、raw payload と completion message override を durable に保存するようにした。
- `JobManager.get_job/list_jobs` は DB 優先 refresh に寄せ、API 再起動後でも `status` polling が external worker の durable state を追従できるようにした。
- worker claim 時に job timeout を設定し、worker heartbeat が `timeout_at` を監視して `worker_timed_out` へ遷移できるようにした。retry は自動再試行を行わず durable terminal state を優先する方針へ統一した。
- SSE generator は idle 時にも `reload_job_from_storage(notify=True)` を定期実行し、API 再起動後に新しく接続した stream でも external worker の durable state 更新を追従できるようにした。
- external worker は `job_lifecycle` structured log と `metrics_recorder.record_job_duration(...)` で completion/failure を観測できるようにした。

## 補足
- 参照: `docs/backtest-greenfield-rebuild.md` Section 2.2, 4.1, 10
