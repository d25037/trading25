---
id: bt-046
title: "Simulation と report rendering / artifact generation を分離"
status: open
priority: medium
labels: [artifacts, reports, marimo, execution, bt]
project: bt
created: 2026-03-08
updated: 2026-03-08
depends_on: [bt-039, bt-041]
blocks: []
parent: bt-037
---

# bt-046 Simulation と report rendering / artifact generation を分離

## 目的
- `BacktestRunner -> MarimoExecutor` に近い現行構造を解き、simulation を report export から分離する。
- HTML を presentation artifact に限定し、canonical result を先に確定させる。

## 受け入れ条件
- [ ] simulation 完了時点で canonical result と core artifacts が確定する。
- [ ] HTML / notebook render は後段 renderer として動作する。
- [ ] result summary は HTML 依存なしで再解決できる。
- [ ] report 生成失敗時も simulation result 自体は保持される。

## 実施内容
- [ ] `BacktestRunner` から simulation と report rendering を分離する。
- [ ] artifact writer / renderer の役割を分ける。
- [ ] `result.html`, `metrics.json`, `manifest.json` の責務を再定義する。
- [ ] 回帰テストと smoke フローを更新する。

## 結果
- 未着手

## 補足
- 参照: `docs/backtest-greenfield-rebuild.md` Section 2.2, 7, 9.2

