---
id: bt-037
title: "Greenfield convergence: multi-engine execution と dataset plane 統合"
status: open
priority: high
labels: [greenfield, architecture, backtest, dataset, engine, bt]
project: bt
created: 2026-03-08
updated: 2026-03-08
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
- [ ] 全 child issue 完了後に `docs/backtest-greenfield-rebuild.md` と AGENTS.md の方針差分が解消される。

## 実施内容
- [ ] tracking issue として child issue の完了状態を管理する。
- [ ] 実装中に設計変更が出た場合は child issue と依存関係を更新する。
- [ ] 完了時に最終アーキテクチャと移行結果を docs へ反映する。

## 結果
- 未着手

## 補足
- 参照: `docs/backtest-greenfield-rebuild.md`

