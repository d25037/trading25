---
id: bt-028
title: "Dataset snapshot reader とスキーマ検証の実装"
status: open
priority: medium
labels: [contracts, dataset, roadmap-migration]
project: bt
created: 2026-02-10
updated: 2026-02-10
depends_on: [ts-125]
blocks: []
parent: null
---

# bt-028 Dataset snapshot reader とスキーマ検証の実装

## 目的
`apps/bt` 側で snapshot manifest を読み取り、利用前に schema version / checksum 検証を行う。

## 受け入れ条件
- manifest reader が実装される
- 読込前に schema version と table checksum を検証できる
- 失敗時に統一エラー形式で返却/通知できる
- テストとドキュメントが更新される

## 実施内容
- manifest reader / validator 実装
- dataset loading 経路への検証統合
- 異常系（version mismatch, checksum mismatch）テスト追加

## 結果
（完了後に記載）

## 補足
- 参照: `docs/archive/unified-roadmap-2026-02-10.md` Phase 2A 延期項目
