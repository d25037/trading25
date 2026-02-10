---
id: bt-029
title: "Phase 5A: Signal Overlay API 実装"
status: open
priority: medium
labels: [api, signals, phase5]
project: bt
created: 2026-02-10
updated: 2026-02-10
depends_on: []
blocks: [ts-126]
parent: null
---

# bt-029 Phase 5A: Signal Overlay API 実装

## 目的
チャート重畳用のシグナル判定 API を FastAPI に追加し、Web 側のマーカー表示基盤を提供する。

## 受け入れ条件
- `POST /api/indicators/signals` が実装される
- boolean 配列 + トリガー日付リストを返すレスポンス定義が実装される
- 34 シグナルを扱える
- OpenAPI / テスト / ドキュメントが更新される

## 実施内容
- ルート / スキーマ / サービスの実装
- シグナル実装との接続
- 結合テストと性能確認

## 結果
（完了後に記載）

## 補足
- 参照: `docs/archive/unified-roadmap-2026-02-10.md` Phase 5A
