---
id: ts-146
title: "Analyst Desk 移行後の web UI polish と smoke QA を行う"
status: migrated
original_status: open
github_issue: https://github.com/d25037/trading25/issues/362
migrated_at: 2026-04-22
priority: low
labels: [frontend, qa, polish, ui]
project: ts
created: 2026-03-24
updated: 2026-03-24
depends_on: [ts-145]
blocks: []
parent: ts-143
---

# ts-146 Analyst Desk 移行後の web UI polish と smoke QA を行う

## 目的
- `ts-143` とその follow-up で主要 workspace の shell は揃ってきたため、残る見た目の不整合や responsive regression を横断的に洗い出して抑える。
- 実装 issue を増やし続けず、仕上げの smoke QA と軽微な polish をまとめて扱う。

## 受け入れ条件
- 更新済みページで、first viewport / internal scroll / rail と results の比率に明白な回帰がない。
- touched workflow に残る `glass-panel` / decorative treatment / oversized header の残差が必要に応じて整理されている。
- browser smoke の確認結果が issue に記録され、必要なら新規 bug issue に分割されている。
- `bun run --filter @trading25/web test` と `bun run --filter @trading25/web typecheck` が通る。

## 実施内容
- [ ] `Charts` / `Ranking` / `Screening` / `Indices` / `Portfolio` / `N225 Options` / `Market DB` の browser smoke を行う。
- [ ] desktop 幅と narrow desktop 幅で responsive spot check を行う。
- [ ] copy / spacing / selection state / panel height の軽微な polish を必要範囲で入れる。
- [ ] 新たに見つかった regressions は別 issue に分割する。

## 結果
- 未着手

## 補足
- `ts-143` の残差整理用 follow-up issue。
