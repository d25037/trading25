---
id: bt-039
title: "RunSpec / CanonicalExecutionResult と experiment registry を定義"
status: done
priority: high
labels: [architecture, contracts, artifacts, runs, bt]
project: bt
created: 2026-03-08
updated: 2026-03-09
depends_on: []
blocks: [bt-040, bt-041, bt-042, bt-043, bt-044, bt-045, bt-046]
parent: bt-037
---

# bt-039 RunSpec / CanonicalExecutionResult と experiment registry を定義

## 目的
- engine 実装に依存しない execution contract を定義する。
- job 管理を experiment registry へ拡張し、再現性・比較・ lineage を保持できるようにする。

## 受け入れ条件
- [x] `RunSpec`、`CompiledStrategyIR` 入力要件、`CanonicalExecutionResult`、artifact index のスキーマが定義される。
- [x] run metadata に `dataset_snapshot_id`、`engine_family`、`execution_policy_version`、`parent_run_id` を保持できる。
- [x] backtest / optimize / lab / attribution が同じ canonical result schema に正規化可能になる。
- [x] OpenAPI / contracts / docs が更新される。

## 実施内容
- [x] domain model と persistence schema を設計する。
- [x] `job_manager` / result summary / artifact resolver の将来置換方針を決める。
- [x] schema versioning と compatibility policy を決める。
- [x] 必要な contract test を追加する。

## 結果
- 初手として `RunSpec` / `RunMetadata` / `CanonicalExecutionResult` / `ArtifactIndex` を実装し、`job_manager` と `portfolio.db jobs` 永続化、backtest/optimize/lab API response へ接続した。
- `apps/ts` の OpenAPI snapshot と generated types を更新し、手書き client types へ新契約を反映した。
- `run_contracts` を切り出し、backtest / optimization / attribution / screening / lab family の submit path で `RunSpec` を明示生成して job 作成時点から dataset / resolved parameters を保持するようにした。
- `pyright` を通したうえで run contract helper と service submit path のテストを追加し、registry 契約の最低限の回帰防止を入れた。
- artifact kind を attribution JSON / strategy YAML / history YAML まで拡張し、artifact index が persisted artifact を辿れるようにした。
- `run_registry` を追加し、backtest summary / attribution result の read path を `artifact_index -> canonical_result -> legacy columns` に寄せた。
- `contracts/README.md` と `docs/backtest-greenfield-rebuild.md` に schema versioning / compatibility policy / registry reader 優先順位を明文化した。

## 補足
- 参照: `docs/backtest-greenfield-rebuild.md` Section 5.3, 6, 7
