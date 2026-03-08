---
id: bt-039
title: "RunSpec / CanonicalExecutionResult と experiment registry を定義"
status: open
priority: high
labels: [architecture, contracts, artifacts, runs, bt]
project: bt
created: 2026-03-08
updated: 2026-03-08
depends_on: []
blocks: [bt-040, bt-041, bt-042, bt-043, bt-044, bt-045, bt-046]
parent: bt-037
---

# bt-039 RunSpec / CanonicalExecutionResult と experiment registry を定義

## 目的
- engine 実装に依存しない execution contract を定義する。
- job 管理を experiment registry へ拡張し、再現性・比較・ lineage を保持できるようにする。

## 受け入れ条件
- [ ] `RunSpec`、`CompiledStrategyIR` 入力要件、`CanonicalExecutionResult`、artifact index のスキーマが定義される。
- [ ] run metadata に `dataset_snapshot_id`、`engine_family`、`execution_policy_version`、`parent_run_id` を保持できる。
- [ ] backtest / optimize / lab / attribution が同じ canonical result schema に正規化可能になる。
- [ ] OpenAPI / contracts / docs が更新される。

## 実施内容
- [ ] domain model と persistence schema を設計する。
- [ ] `job_manager` / result summary / artifact resolver の将来置換方針を決める。
- [ ] schema versioning と compatibility policy を決める。
- [ ] 必要な contract test を追加する。

## 結果
- 未着手

## 補足
- 参照: `docs/backtest-greenfield-rebuild.md` Section 5.3, 6, 7

