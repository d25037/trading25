---
id: ts-143
title: "ts/web を frontend-skill 準拠へ段階移行する"
status: open
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
- [ ] Stage 1: `ts/web` 向け visual thesis / content plan / interaction thesis を定義し、app shell に必要な typography・spacing・surface 原則を決める。
- [ ] Stage 2: `index.css` と layout primitives を見直し、default font stack 依存・purple-first accent・`glass-panel` 常用を減らせる token / primitive に置き換える。
- [ ] Stage 3: `Header` と top-level page headers を app UI 基準で再設計し、marketing-like hero / chip strip / decorative gradient を utility-first header へ整理する。
- [ ] Stage 4: `Ranking` / `Screening` / `Portfolio` / `Market DB` / `N225 Options` / `Indices` の surface を cardless layout 優先へ置き換え、`Card` / `glass-panel` の必要箇所だけを残す。
- [ ] Stage 5: hover / selection / panel reveal / route-level transition を 2-3 個の motion pattern に統一し、generic `transition-*` と ornamental scale effects を整理する。
- [ ] Stage 6: visual regression と usability を確認し、移行方針に沿わない残差を follow-up issue へ分割する。

## 結果
- 未着手

## 補足
- 現状は `frontend-skill` を**部分的には満たすが、全体としては未充足**と判断する。
- 主なギャップ:
  - `apps/ts/packages/web/src/index.css`
    - 専用タイポグラフィ設計がなく、font family token も未定義。
    - `glass-panel` / `gradient-primary` が app surface の既定表現として残っている。
  - `apps/ts/packages/web/src/components/Layout/Header.tsx`
    - app shell は機能するが、brand hierarchy と navigation treatment が generic segmented pill に寄っており、visual thesis が弱い。
  - `apps/ts/packages/web/src/pages/N225OptionsPage.tsx`
    - `Derivatives Explorer` hero は product UI より marketing/landing 的で、`frontend-skill` の utility copy 原則とずれる。
  - `apps/ts/packages/web/src/pages/SettingsPage.tsx`
    - `MarketDbHero` は ops page に対して decorative gradient と chip strip が強く、routine product UI としては装飾過多。
  - `apps/ts/packages/web/src/components/Ranking/RankingFilters.tsx`
    - filters が generic card 依存で、layout-first より component-first に見える。
  - `apps/ts/packages/web/src/components/Portfolio/PortfolioList.tsx`
    - selected state が `gradient-primary + shadow-lg` に依存し、app UI の restrained hierarchy から外れ気味。
- `frontend-skill` の app UI 観点では、特に以下を避ける必要がある:
  - stacked cards / card mosaics
  - decorative gradients behind routine product UI
  - multiple competing accents
  - utility copy ではなく marketing-like headers
- まずは shell / token / header の基盤から着手し、その後に page-level cleanup を進めるのが安全。
