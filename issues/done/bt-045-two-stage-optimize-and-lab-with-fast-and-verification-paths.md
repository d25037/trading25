---
id: bt-045
title: "Optimize / Lab を fast path と verification path の二段実行へ移行"
status: done
priority: medium
labels: [optimize, lab, vectorbt, nautilus, bt]
project: bt
created: 2026-03-08
updated: 2026-03-12
depends_on: [bt-041, bt-044]
blocks: []
parent: bt-037
---

# bt-045 Optimize / Lab を fast path と verification path の二段実行へ移行

## 目的
- optimize / lab を単一 engine 前提から外し、`vectorbt` 一次探索と `Nautilus` 再検証を組み合わせられるようにする。
- 高速探索と高忠実度検証の trade-off を product として明示する。

## 受け入れ条件
- [x] optimize trial が engine policy を選択できる。
- [x] lab candidate の上位候補を verification queue に回せる。
- [x] result UI / API が fast path と verification path の差分を表示できる。
- [x] verification 不一致時の扱いが定義される。

## 実施内容
- [x] optimize/lab orchestration を engine-aware にする。
- [x] ranking / candidate selection に verification 状態を追加する。
- [x] best/worst だけでなく verification 結果との差分を保存する。
- [x] web/API 表示項目を更新する。

## 結果
- `OptimizationRequest`、`LabGenerateRequest`、`LabEvolveRequest`、`LabOptimizeRequest` に `engine_policy`（`fast_only` / `fast_then_verify`、`verification_top_k`）を追加し、optimize と lab generate/evolve/optimize を二段実行へ移行した。
- backend では verification orchestrator と durable child `backtest` run を導入し、`vectorbt` fast path の上位候補を `Nautilus` で直列 verification できるようにした。親 job は verification 完了まで `running` を維持し、`fast_candidates` と `verification` を raw/canonical result に保存する。
- mismatch policy は `demote` に固定し、`authoritative_candidate_id`、candidate delta、mismatch reasons を API で返すようにした。verified candidate が全件 mismatch/failed の場合は保存を抑止する。
- web では Optimization / Lab form に `Fast only` / `Fast + Nautilus verify` と `Top K` を追加し、progress/history/result で fast stage と verification stage を分離表示するようにした。
- OpenAPI と TS contracts を `bt:sync` で同期し、manual types / runtime schemas も verification payload に追従させた。
- scoped `aicheck` を完了し、backend verification/worker 対象 coverage は `verification_orchestrator 96%`、`lab_worker 92%`、`optimization_worker 94%`、targeted backend suite は `192 passed`、targeted web tests は `42 passed`、`bun run quality:typecheck` も通過した。

## 補足
- 参照: `docs/backtest-greenfield-rebuild.md` Section 8, 10
