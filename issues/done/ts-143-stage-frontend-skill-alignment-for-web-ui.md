---
id: ts-143
title: "ts/web を frontend-skill 準拠へ段階移行する"
status: done
priority: medium
labels: [frontend, design, ui, refactor, phased]
project: ts
created: 2026-03-24
updated: 2026-03-24
depends_on: []
blocks: []
parent: null
---

# ts-143 ts/web を frontend-skill 準拠へ段階移行する

## 目的
- 現在の `apps/ts/packages/web` は、情報設計と機能面では一定の整理が進んでいるが、`frontend-skill` が要求する app UI の visual direction / hierarchy / typography / motion / cardless discipline までは満たしていない。
- 既存機能を壊さず、全面改装ではなく段階的に `frontend-skill` のルールを充足できる状態へ移行する。

## 受け入れ条件
- `frontend-skill` の app UI ルールに対応する `ts/web` 向け visual thesis / content plan / interaction thesis が明文化されている。
- タイポグラフィ、色、surface、accent の設計がトークン化され、既定の app shell で一貫して使われている。
- routine product UI での decorative gradient / hero-like banner / unnecessary card treatment が削減され、主要画面が layout-first に読める。
- `Charts` / `Ranking` / `Screening` / `Portfolio` / `Market DB` / `N225 Options` の少なくとも主要 first viewport が utility copy を優先した app UI になっている。
- hover / reveal / transition が場当たり的な `transition-*` と spinner 依存ではなく、2-3 個の意図的な motion pattern に整理されている。
- responsive behavior と既存機能を維持し、既存テストと必要な追加テストで regressions を抑制できている。

## 実施内容
- [x] Stage 1: `ts/web` 向け visual thesis / content plan / interaction thesis を定義し、app shell に必要な typography・spacing・surface 原則を決める。
- [x] Stage 2: `index.css` と layout primitives を見直し、default font stack 依存・purple-first accent・`glass-panel` 常用を減らせる token / primitive に置き換える。
- [x] Stage 3: `Header` と top-level page headers を app UI 基準で再設計し、marketing-like hero / chip strip / decorative gradient を utility-first header へ整理する。
- [x] Stage 4: `Ranking` / `Screening` / `Portfolio` / `Market DB` / `N225 Options` / `Indices` の surface を cardless layout 優先へ置き換え、`Card` / `glass-panel` の必要箇所だけを残す。
- [x] Stage 5: touched pages の hover / selection / panel sizing を results-first の interaction language に寄せ、残る motion/polish は follow-up issue へ分離した。
- [x] Stage 6: visual regression と usability を確認し、移行方針に沿わない残差を follow-up issue へ分割した。

## 結果
- `apps/ts/packages/web/src/index.css` と layout primitives を更新し、`Analyst Desk` の visual thesis に沿った typography / surface / accent token と page intro primitive を導入した。
- `Header` と app shell を calmer な utility navigation に整理し、active state は style class 依存ではなく semantic state を基準に見直した。
- `N225 Options` と `Market DB` の first viewport を gradient hero から utility-first header + restrained surface へ移行した。
- `Ranking` / `Screening` / `Indices` を results-first に再設計し、filter rail・results workspace・internal scroll の比率を調整した。
- `Portfolio / Watchlist` を compact header + results-first workspace に揃え、list/detail hierarchy を整理した。
- `Charts` の panel sizing と shell を調整し、main chart と sub-chart の表示優先度を立て直した。
- 検証:
  - `bun run --filter @trading25/web test`
  - `bun run --filter @trading25/web typecheck`

## 補足
- 当初スコープに対して、実装中に判明した残件は `ts-145` と `ts-146` に分離した。
- `Backtest` の workspace alignment は別 issue とし、`ts-143` は主要 workspace の土台整備と page-level 移行完了をもって close する。
