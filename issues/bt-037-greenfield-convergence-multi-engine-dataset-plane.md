---
id: bt-037
title: "Greenfield convergence: multi-engine execution と dataset plane 統合"
status: open
priority: high
labels: [greenfield, architecture, backtest, dataset, engine, bt]
project: bt
created: 2026-03-08
updated: 2026-03-09
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
- [ ] 本 issue に紐づく子 issue が作成済みで、依存順が明示されている。
- [ ] `dataset plane migration` と `engine abstraction` が同じプログラムとして管理される。
- [ ] `main` と別 worktree を使う範囲と、段階的に `main` へ戻す判断基準が明文化されている。
- [ ] 全 child issue 完了後に `docs/backtest-greenfield-rebuild.md` と AGENTS.md の方針差分が解消される。

## 実施内容
- [ ] tracking issue として child issue の完了状態を管理する。
- [ ] 実装中に設計変更が出た場合は child issue と依存関係を更新する。
- [ ] greenfield program 用の専用 worktree を維持し、`bt-039`/`bt-038`/`bt-043`/`bt-042` のような基盤整備は child issue 単位で段階的に `main` へ戻す。
- [ ] `bt-041`/`bt-044`/`bt-045`/`bt-046` のような高 churn な engine/runtime 変更は、契約境界と snapshot resolver が安定するまで専用 worktree で隔離する。
- [ ] OpenAPI / contracts / dataset snapshot manifest の互換が崩れる期間は `main` へ直接流さず、互換回復または移行手順確定後に統合する。
- [ ] 完了時に最終アーキテクチャと移行結果を docs へ反映する。

## 結果
- 未着手

## Worktree運用計画
- 専用の長寿命 worktree を `bt-037` program の統合検証用として確保し、`main` で日常開発を止めない。
- 初期フェーズでは `bt-039` を最優先とし、`RunSpec` / `CanonicalExecutionResult` / artifact index の契約が固まるまで `main` とは分離して進める。
- dataset plane は `ts-125 -> bt-028 -> bt-038 -> bt-043` を 1 本の移行線として扱い、互換 reader を残した状態で child issue 単位に `main` へ戻す。
- execution/control plane は `bt-039 -> bt-040 / bt-042 -> bt-041 -> bt-046` を基本順とし、`vectorbt` 境界変更と worker runtime 切替は統合検証が通るまで worktree 内でまとめて扱う。
- `bt-044` と `bt-045` は verification engine 追加後の差分が大きいため、`main` へ急がず専用 worktree で API / artifact / UI の整合を取ってから戻す。
- `main` へ戻す条件は「child issue 単位で完結している」「OpenAPI/contracts の更新が反映済み」「既存 UI/CLI の後方互換または明示的移行手順がある」「bt/ts の主要テストが通る」の 4 点とする。
- したがって、依存 issue が全て終わるまで全変更を閉じ込めるのではなく、基盤が安定したものから順次 `main` へ戻し、高 churn な統合変更のみを最後まで worktree に残す。

## 補足
- 参照: `docs/backtest-greenfield-rebuild.md`
