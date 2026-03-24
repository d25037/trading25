---
id: ts-145
title: "Backtest workspace を Analyst Desk に揃える"
status: open
priority: medium
labels: [frontend, design, ui, backtest]
project: ts
created: 2026-03-24
updated: 2026-03-24
depends_on: []
blocks: []
parent: ts-143
---

# ts-145 Backtest workspace を Analyst Desk に揃える

## 目的
- `Charts` / `Ranking` / `Screening` / `Indices` / `Portfolio` までは `Analyst Desk` の results-first language に揃ったが、`Backtest` はまだ旧来の shell / section hierarchy が残っている。
- 実行、履歴、結果、editor の優先順位を整理し、routine analytics workspace として読みやすい first viewport に揃える。

## 受け入れ条件
- `Backtest` の top-level shell と first viewport が既存の `Analyst Desk` primitive に揃っている。
- control chrome よりも run status / history / result workspace の優先順位が明確になっている。
- strategy editor、run、history、result 表示、lab/optimization への導線を壊さない。
- touched components で decorative gradient / unnecessary glass treatment が削減されている。
- `bun run --filter @trading25/web test` と `bun run --filter @trading25/web typecheck` が通る。

## 実施内容
- [ ] `Backtest` の page intro / mode bar / section hierarchy を compact に整理する。
- [ ] strategy / run / history / result の chrome を quieter にし、results-first の workspace 比率へ見直す。
- [ ] editor / result / history panel の surface を shared primitive に寄せ、不要な card / glass 表現を削減する。
- [ ] 既存 route state、job polling、editor flow、navigation のロジックを維持したまま UI だけを整理する。
- [ ] 必要な test 更新と browser smoke を行う。

## 結果
- 未着手

## 補足
- `ts-143` から切り出した follow-up issue。
