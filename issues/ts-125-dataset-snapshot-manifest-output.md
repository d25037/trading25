---
id: ts-125
title: "Dataset snapshot manifest 出力の実装"
status: open
priority: medium
labels: [contracts, dataset, roadmap-migration]
project: ts
created: 2026-02-10
updated: 2026-02-10
depends_on: []
blocks: [bt-028]
parent: null
---

# ts-125 Dataset snapshot manifest 出力の実装

## 目的
`apps/ts` 側で dataset snapshot の `manifest.json` を出力し、bt 側の読込前検証を可能にする。

## 受け入れ条件
- manifest 出力機能が実装される
- manifest に `schema_version`, `table_checksums`, `created_at`, `generator` を含む
- 生成物と契約（`contracts/*-schema*.json`）の整合が確認できる
- テストとドキュメントが更新される

## 実施内容
- snapshot/manifest 生成処理の実装
- checksum 算出と schema version 連携
- 検証テスト追加

## 結果
（完了後に記載）

## 補足
- 参照: `docs/archive/unified-roadmap-2026-02-10.md` Phase 2A 延期項目
