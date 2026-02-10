---
id: ts-126
title: "Phase 5B: Web シグナルマーカー表示"
status: open
priority: medium
labels: [web, signals, phase5]
project: ts
created: 2026-02-10
updated: 2026-02-10
depends_on: [bt-029]
blocks: []
parent: null
---

# ts-126 Phase 5B: Web シグナルマーカー表示

## 目的
チャート上にシグナル発火点（▲/▼）を表示し、バックテストの entry/exit と連動できる UI を提供する。

## 受け入れ条件
- チャート上にシグナルマーカーが表示される
- entry/exit ポイントと連動できる
- API レスポンスに基づく型安全な描画実装になっている
- テストとドキュメントが更新される

## 実施内容
- シグナル取得フック / UI コンポーネント追加
- チャート描画ロジック統合
- 回帰テスト追加

## 結果
（完了後に記載）

## 補足
- 参照: `docs/archive/unified-roadmap-2026-02-10.md` Phase 5B
