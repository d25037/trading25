---
id: ts-105
title: "Backtest UI 進捗バーの実数表示"
status: done
priority: medium
labels: []
project: ts
created: 2026-01-30
updated: 2026-02-13
depends_on: []
blocks: []
parent: null
---

# ts-105 Backtest UI 進捗バーの実数表示

## 目的
進捗バーを実数値と連動させる。

## 受け入れ条件
- `progress` 値に応じたバーが表示される

## 実施内容

- `apps/ts/packages/web/src/components/Backtest/BacktestAttribution.tsx` のジョブカードに
  `progress` 値連動の determinate progress bar を追加
- 進捗表示に `%` 表示、経過時間表示（`started_at`/`created_at` 基準）、`aria` 属性を追加
- `progress` が未提供の場合は indeterminate bar にフォールバック
- `apps/ts/packages/web/src/components/Backtest/BacktestAttribution.test.tsx` を更新し
  `50.0%` 表示と `aria-valuenow=50` を検証
- `progress=null` 時の indeterminate 表示、および `progress>1` のクランプ表示（100%）をテスト追加
- 追加テスト実行時の対象カバレッジ: `BacktestAttribution.tsx` line 98.7% / branch 87.39%

## 結果

- `progress` 値に応じた進捗バー表示を実装し、受け入れ条件を満たした。

## 補足
