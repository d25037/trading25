---
id: ts-142
title: "TS 依存整理と構造簡素化のフォローアップ計画"
status: open
priority: medium
labels: [planning, dependencies, refactor, tooling]
project: ts
created: 2026-03-20
updated: 2026-03-23
depends_on: [ts-138]
blocks: []
parent: null
---

# ts-142 TS 依存整理と構造簡素化のフォローアップ計画

## 目的
- `apps/ts` の依存整理を「削除」だけでなく state boundary・テスト基盤・補助 UI ライブラリ・タスク実行構造まで含めて棚卸しする
- 既存 issue (`ts-138` / `ts-139` / `ts-140` / `ts-141`) を補完し、次の実装順を明確にする

## 背景
既存の dependency audit と policy により、未使用依存や pin drift の検出基盤は整っている。一方で、次のような「依存自体は使われているが、もっとシンプルにできる」論点が残っている。

1. route search state と persisted zustand state の責務が画面によって分散している
2. Vitest で `happy-dom` を標準にしつつ、一部だけ `jsdom` を使っており DOM 実装が二重化している
3. `react-json-view-lite` が単発利用に近く、内部 JSON viewer への置換余地がある
4. YAML parse / normalize / validation が複数箇所に散っている
5. `scripts/tasks.ts` に workspace orchestration が集まり、package script と責務が重なっている

## 受け入れ条件
- 依存整理の対象を「manifest から消せるもの」と「構造の簡素化で負債を減らすもの」に分けて管理できる
- zustand / route search params / query cache の責務分担が issue レベルで明文化されている
- DOM テスト基盤について `happy-dom` / `jsdom` の維持方針が決まっている
- 単発 UI helper dependency と YAML utility 重複の評価結果が残っている
- `scripts/tasks.ts` の縮小または現状維持の判断基準が整理されている

## 実施内容

### A. state boundary の整理
- [ ] `ts-140` を基準に、URL で表現できる state と session-local state を再分類する
- [ ] `analysisStore` の job result / history persist を query cache + API 再解決へ寄せられるか評価する
- [ ] `backtestStore` は active job tracking 中心に限定し、localStorage 直アクセス箇所を共通化できるか確認する

### B. テスト基盤の依存整理
- [ ] `happy-dom` を標準に据えたまま `jsdom` 依存を削減できるか調査する
- [ ] `@vitest-environment jsdom` を使うテストごとに必要 DOM API を棚卸しする
- [ ] 完全統一できない場合も、「なぜ二重運用なのか」をテスト基盤ドキュメントへ残す

### C. 単発利用 dependency の見直し
- [ ] `react-json-view-lite` の実利用機能を整理する
- [ ] read-only tree view で十分なら内部 `JsonTreeView` コンポーネントへの置換案を作る
- [ ] 置換しない場合は、保持理由（深いネスト可視化・開閉体験など）を記録する

### D. YAML utility の共通化
- [x] `authoringUtils.ts` / `strategyValidation.ts` / `optimizationGridParams.ts` の YAML 処理を共通 API に寄せる
- [ ] parse / stringify / validation payload normalization の責務を分割する
- [ ] Monaco editor から直接 `js-yaml` を触る箇所を減らせるか確認する

### E. task runner の簡素化
- [ ] `scripts/tasks.ts` の task を「単純委譲」と「独自 orchestration」に分類する
- [ ] env file 注入や optional sync のような付加価値がない task は package script 直呼びへ戻せるか評価する
- [ ] README のコマンド説明と task 実装のズレを解消する

## 実装順の提案
1. `ts-139` で manifest 上の明確な不要物と pin drift を先に解消する
2. `ts-140` で route search state と zustand の境界をさらに整理する
3. 本 issue で DOM test / 単発 dependency / YAML utility / task runner の簡素化を順に評価する
4. 再発防止は `ts-141` の audit guardrail と README policy に寄せる

## 結果
- 2026-03-23: `yamlUtils.ts` を追加し、Backtest authoring 周辺の YAML parse/dump を共通化した
- 2026-03-23: `DefaultConfigEditor` の dump fallback も共通 util へ寄せ、重複実装を削除した

## 補足
- `@tanstack/react-query` / `@tanstack/react-router` / `lightweight-charts` / `@monaco-editor/react` + `monaco-editor` は現時点では責務が明確で、優先的な削除対象ではない
- `zustand` は完全撤去ではなく縮小方針を維持する
- この issue は依存削除だけでなく「依存が必要でも構造を簡素化できる箇所」を追跡するための計画 issue とする
