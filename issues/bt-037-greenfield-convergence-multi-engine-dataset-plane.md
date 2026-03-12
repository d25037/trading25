---
id: bt-037
title: "Greenfield convergence: multi-engine execution と dataset plane 統合"
status: open
priority: high
labels: [greenfield, architecture, backtest, dataset, engine, bt]
project: bt
created: 2026-03-08
updated: 2026-03-12
depends_on: []
blocks: [bt-038, bt-039, bt-040, bt-041, bt-042, bt-043, bt-044, bt-045, bt-046]
parent: null
---

# bt-037 Greenfield convergence: multi-engine execution と dataset plane 統合

## 目的
- `docs/backtest-greenfield-rebuild.md` で整理した方針を、実装可能な workstream と依存関係へ落とし込む。
- `market.duckdb + parquet` に進んだ market plane と、legacy `dataset.db` に残っている dataset plane を一体で再設計する。
- 将来の `vectorbt + Nautilus` 併存を前提に、engine-neutral な execution contract へ収束させる。

## 受け入れ条件
- [x] 本 issue に紐づく子 issue が作成済みで、依存順が明示されている。
- [x] `dataset plane migration` と `engine abstraction` が同じプログラムとして管理される。
- [x] `main` と別 worktree を使う範囲と、段階的に `main` へ戻す判断基準が明文化されている。
- [ ] 全 child issue 完了後に `docs/backtest-greenfield-rebuild.md` と AGENTS.md の方針差分が解消される。

## 実施内容
- [x] tracking issue として child issue の完了状態を管理する。
- [x] 実装中に設計変更が出た場合は child issue と依存関係を更新する。
- [x] greenfield program 用の専用 worktree を維持し、`bt-039`/`bt-038`/`bt-043`/`bt-042` のような基盤整備は child issue 単位で段階的に `main` へ戻す。
- [x] `bt-041`/`bt-044`/`bt-045`/`bt-046` のような高 churn な engine/runtime 変更は、契約境界と snapshot resolver が安定するまで専用 worktree で隔離する。
- [x] OpenAPI / contracts / dataset snapshot manifest の互換が崩れる期間は `main` へ直接流さず、互換回復または移行手順確定後に統合する。
- [ ] 完了時に最終アーキテクチャと移行結果を docs へ反映する。

## 結果
- 2026-03-09 までに基盤整備の 4 本である `bt-039` / `bt-038` / `bt-043` / `bt-042` を child issue 単位で完了し、RunSpec / canonical result / artifact index、dataset snapshot SoT、snapshot resolver、durable worker runtime を先に安定化した。
- 2026-03-10 に `bt-040` が完了し、`CompiledStrategyIR` / availability model が strategy validation、signal processing、screening 判定、signal reference まで通る形で導入された。
- 2026-03-10 に `bt-041` が完了し、`VectorbtAdapter` と `ExecutionPortfolioProtocol` への移行によって domain surface から `vbt.Portfolio` を除去した。
- 2026-03-10 に `bt-046` が完了し、simulation と report rendering / artifact generation が分離され、canonical result と core artifacts を HTML 非依存で再解決できる状態になった。
- 2026-03-12 時点の child issue 進捗は 9 本中 7 本完了で、未完了は `bt-044` と `bt-045` の 2 本のみである。
- 依存関係も整理され、現在のクリティカルパスは `bt-044 -> bt-045` に収束した。`bt-044` は `bt-040` / `bt-042` / `bt-043` 完了により着手可能で、`bt-045` は `bt-041` 完了済みのため `bt-044` のみが blocker になっている。

## Child Issue 状態

### 完了済み
- [x] `bt-039` RunSpec / CanonicalExecutionResult と experiment registry を定義
- [x] `bt-038` dataset snapshot SoT を `dataset.duckdb + parquet` へ移行
- [x] `bt-043` market / dataset snapshot resolver を共通化
- [x] `bt-042` worker runtime と durable execution control を導入
- [x] `bt-040` CompiledStrategyIR と availability model を導入
- [x] `bt-041` VectorbtAdapter を抽出し domain から `vbt.Portfolio` を除去
- [x] `bt-046` Simulation と report rendering / artifact generation を分離

### 未完了
- [ ] `bt-044` NautilusAdapter を verification engine として追加
- [ ] `bt-045` Optimize / Lab を fast path と verification path の二段実行へ移行

## 現在の判断
- dataset plane migration、execution contract、snapshot resolver、worker runtime、compiled strategy、VectorBT adapter 抽出、artifact 分離までが完了し、program は「基盤整備フェーズ」から「multi-engine verification フェーズ」へ移った。
- 当初は `bt-040` 仕上げ前に `bt-041` / `bt-046` を残していたが、実際には `bt-041` と `bt-046` まで先行して閉じられたため、残課題は verification engine (`bt-044`) とその product integration (`bt-045`) に絞られた。
- したがって現在の主要リスクは dataset plane や execution contract ではなく、`RunSpec` / snapshot / compiled strategy を Nautilus verification path にどう接続し、その差分を optimize/lab と UI/API にどう露出するかである。

## Worktree運用計画
- 専用の長寿命 worktree は引き続き `bt-037` program の統合検証用として維持するが、用途は基盤整備の溜め込みではなく、`bt-044` / `bt-045` の high churn 変更を隔離することに絞る。
- 初期フェーズで想定していた `bt-039` / `bt-038` / `bt-043` / `bt-042` に加え、`bt-040` / `bt-041` / `bt-046` も issue 管理上は完了済みであり、現在の隔離対象は `bt-044` と `bt-045` のみである。
- dataset plane の移行線 `ts-125 -> bt-028 -> bt-038 -> bt-043` は完了済みとみなし、以後は snapshot contract を壊さない限り `main` を基準に進める。
- execution/control plane の実績線は `bt-039 -> bt-042 -> bt-040 -> bt-041 -> bt-046` まで完了しており、残る拡張線は `bt-044 -> bt-045` である。
- 次の実行順は専用 worktree で `bt-044` を最優先とし、Nautilus verification path を最小スコープで成立させたうえで `bt-045` の fast/verification 二段化へ接続する。
- `main` へ戻す条件は引き続き「child issue 単位で完結」「OpenAPI/contracts 更新反映済み」「既存 UI/CLI の後方互換または明示的移行手順あり」「bt/ts の主要テスト通過」の 4 点とする。
- したがって、残る高 churn 変更は worktree に隔離し続けるが、`bt-037` 全体完了を待たず、引き続き child issue 単位で順次 `main` へ戻す。

## 今後の予定
- 次の本丸は `bt-044` とし、`RunSpec` / worker runtime / snapshot resolver / compiled strategy を前提に Nautilus verification engine を最小構成で成立させる。
- `bt-044` 完了後に `bt-045` を進め、optimize/lab を fast path と verification path の二段実行へ移行する。
- `bt-045` では verification 結果との差分保存、API/UI 表示、queueing policy をまとめて片付ける。
- 全 child issue 完了後に `docs/backtest-greenfield-rebuild.md` と AGENTS.md の program 差分を解消し、本 issue を close する。

## 補足
- 参照: `docs/backtest-greenfield-rebuild.md`
