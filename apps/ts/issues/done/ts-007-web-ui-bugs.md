---
id: ts-007
title: "Web/UI 既知バグ修正"
status: done
priority: medium
labels: []
project: ts
created: 2026-01-30
updated: 2026-01-30
depends_on: []
blocks: []
parent: null
---

# ts-007 Web/UI 既知バグ修正

## 目的
Tailwind 動的クラス/React ref など既知の UI バグを修正する。

## 受け入れ条件
- `StockChart` の色クラスが正しく反映
- `SearchSuggestions` が ref を正しく受け取る

## 実施内容

### StockChart 色クラスバグ修正
- `getPriceColorClass` が Tailwind 動的クラス (`text-[${CHART_COLORS.UP}]`) を生成していたが、Tailwind CSS v4 の JIT コンパイラはビルド時に動的クラスを検出できないため色が反映されなかった
- `getPriceColor` に改名し、色文字列を直接返すように変更
- OHLCOverlay で `className` ではなく `style={{ color }}` でインラインスタイル適用に修正

### SearchSuggestions ref バグ
- コードベースに `SearchSuggestions` コンポーネントが存在しない（削除済みまたは未実装）
- 既存の `Input` コンポーネントは `forwardRef` を正しく使用しており問題なし

## 結果
- typecheck, lint 共にパス
- StockChart の OHLC オーバーレイで価格の上昇/下落色が正しく表示される

## 補足
- 変更ファイル: `packages/web/src/components/Chart/StockChart.tsx`
