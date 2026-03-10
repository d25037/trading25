---
id: bt-037
title: "Greenfield convergence: multi-engine execution と dataset plane 統合"
status: open
priority: high
labels: [greenfield, architecture, backtest, dataset, engine, bt]
project: bt
created: 2026-03-08
updated: 2026-03-10
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
- 2026-03-09 までに基盤整備の 4 本である `bt-039` / `bt-038` / `bt-043` / `bt-042` を child issue 単位で完了し、段階的に `main` へ戻した。
- `RunSpec` / canonical result / artifact index、dataset snapshot SoT、snapshot resolver、durable worker runtime が先に安定したことで、engine/runtime 置換の前提条件が揃った。
- 2026-03-10 時点で専用 worktree は `bt-040` の仕上げとレビューに使っており、`CompiledStrategyIR` / availability model の導入は issue 上 `done` に到達した。
- 実行順は当初の計画から少し前後し、基盤は `bt-039 -> bt-038 -> bt-043 -> bt-042` を先に `main` へ戻し、その後に同一 worktree 上で `bt-040` を深掘りした。
- したがって現時点の program 状態は「基盤 5 本のうち 4 本は `main` 反映済み、`bt-040` は worktree 上で最終確認中、残る高 churn issue は未着手」という段階にある。

## Child Issue 状態

### 完了済み
- [x] `bt-039` RunSpec / CanonicalExecutionResult と experiment registry を定義
- [x] `bt-038` dataset snapshot SoT を `dataset.duckdb + parquet` へ移行
- [x] `bt-043` market / dataset snapshot resolver を共通化
- [x] `bt-042` worker runtime と durable execution control を導入
- [x] `bt-040` CompiledStrategyIR と availability model を導入

### 未完了
- [ ] `bt-041` VectorbtAdapter を抽出し domain から `vbt.Portfolio` を除去
- [ ] `bt-044` NautilusAdapter を verification engine として追加
- [ ] `bt-045` Optimize / Lab を fast path と verification path の二段実行へ移行
- [ ] `bt-046` Simulation と report rendering / artifact generation を分離

## 現在の判断
- 基盤の child issue は、専用 worktree で統合検証した後に child issue 単位で `main` へ戻す、という program 方針どおりに運用できている。
- ズレているのは順序の細部で、`bt-042` を `bt-040` より先に固めた点と、`bt-040` を shadow compile だけでなく execution path まで広げた点である。
- ただしこのズレは program の意図に反しておらず、むしろ `bt-041` 以降の adapter/engine 置換に必要な契約境界を先に安定化する方向で吸収できている。

## Worktree運用計画
- 専用の長寿命 worktree は引き続き `bt-037` program の統合検証用として維持するが、今後は新しい基盤整備を溜め込む場所ではなく、`bt-040` 以降の高 churn 変更を隔離する場所として使う。
- 初期フェーズで想定していた `bt-039` / `bt-038` / `bt-043` / `bt-042` はすでに `main` へ戻したため、現在の隔離対象は `bt-040` の最終化と `bt-041` / `bt-044` / `bt-045` / `bt-046` である。
- dataset plane の移行線 `ts-125 -> bt-028 -> bt-038 -> bt-043` は完了済みとみなし、以後は snapshot contract を壊さない限り `main` を基準に進める。
- execution/control plane は実績ベースで `bt-039 -> bt-042 -> bt-040 -> bt-041 -> bt-046` を主線とし、`bt-044` は `bt-040/042/043` を前提に別線で進め、`bt-045` は `bt-041 + bt-044` 完了後に着手する。
- 次の実行順は `bt-040` を commit/review/merge したあと、専用 worktree で `bt-041` を最優先とする。その後に `bt-046` と `bt-044` を状況に応じて並行または連続で進め、`bt-045` を最後に閉じる。
- `main` へ戻す条件は引き続き「child issue 単位で完結」「OpenAPI/contracts 更新反映済み」「既存 UI/CLI の後方互換または明示的移行手順あり」「bt/ts の主要テスト通過」の 4 点とする。
- したがって、残る高 churn 変更は worktree に隔離し続けるが、安定化した child issue まで `bt-037` 全体を待つのではなく、引き続き child issue 単位で順次 `main` へ戻す。

## 今後の予定
- `bt-040` の最終レビュー・commit・PR 作成を先に完了する。
- 次の本丸は `bt-041` とし、`vectorbt` を domain surface から引き剥がして adapter 境界を確立する。
- `bt-041` 後に `bt-046` を進め、simulation と report rendering / artifact generation を分離して canonical result を HTML 依存から切り離す。
- `bt-044` は `RunSpec` / worker runtime / snapshot resolver を前提に verification engine として追加し、`bt-045` で optimize/lab の fast/verification 二段化へつなぐ。
- 全 child issue 完了後に `docs/backtest-greenfield-rebuild.md` と AGENTS.md の program 差分を解消し、本 issue を close する。

## 補足
- 参照: `docs/backtest-greenfield-rebuild.md`
