---
id: ts-145
title: "Backtest workspace を Analyst Desk に揃える"
status: done
priority: medium
labels: [frontend, design, ui, backtest]
project: ts
created: 2026-03-24
updated: 2026-03-26
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
- [x] `Backtest` の page intro / mode bar / section hierarchy を compact に整理する。
- [x] strategy / run / history / result の chrome を quieter にし、results-first の workspace 比率へ見直す。
- [x] editor / result / history panel の surface を shared primitive に寄せ、不要な card / glass 表現を削減する。
- [x] 既存 route state、job polling、editor flow、navigation のロジックを維持したまま UI だけを整理する。
- [x] 必要な test 更新と browser smoke を行う。

## 結果
- `apps/ts/packages/web/src/pages/BacktestPage.tsx` の shell を Analyst Desk の intro + quieter navigation に揃え、first viewport で view / strategy / focus を把握できるようにした。
- `BacktestRunner` / `BacktestResults` / `BacktestStrategies` / `BacktestAttribution` / result browsers / job progress surfaces を shared workspace primitive ベースへ整理し、control rail より status / history / preview が先に読める比率に寄せた。
- `BacktestRunner` は strategy summary + `Run Status` / `Optimization Status` / sticky `Control Panel` の 3 列 workspace に再編し、操作より実行状態を先に読む構成へ寄せた。
- `BacktestStrategies` は `lg` 以上で list main + right detail rail を維持する layout に戻し、strategy workspace が下段へ落ちないよう整理した。
- `ResultHtmlViewer` の二重 chrome を削減し、preview surface 側に操作を寄せた。
- test 更新:
  - `bun run --filter @trading25/web typecheck`
  - `bun run --filter @trading25/web test`
  - `bun run --filter @trading25/web test:coverage`
  - `./scripts/prepush-ci.sh --skip-install`
- browser smoke:
  - `PLAYWRIGHT_WEB_PORT=4273 PLAYWRIGHT_BASE_URL=http://127.0.0.1:4273 bunx playwright test e2e/backtest-optimize-popup.smoke.spec.ts`

## 補足
- `ts-143` から切り出した follow-up issue。
