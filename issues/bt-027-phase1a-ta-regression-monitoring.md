---
id: bt-027
title: "Phase 1A: TA 回帰監視基盤の実装"
status: open
priority: medium
labels: [quality, monitoring, ta]
project: bt
created: 2026-02-10
updated: 2026-02-10
depends_on: []
blocks: []
parent: null
---

# bt-027 Phase 1A: TA 回帰監視基盤の実装

## 目的
TA 計算結果と性能の回帰を継続監視し、FastAPI 一本化後の品質を定常運用で担保する。

## 受け入れ条件
- 11 指標 × 代表銘柄 3-5 銘柄の定期差分テストが実装される
- P95 レイテンシ監視（閾値 800ms）を測定できる
- 週次サンプル比較スクリプトが実装され、不一致率 < 0.1% / API エラー率 < 1% を判定できる
- 実行手順がドキュメント化される

## 実施内容
- 回帰監視テスト/スクリプトの実装
- 実行コマンドとしきい値判定の標準化
- CI/定期実行の導入可否を整理

## 結果
（完了後に記載）

## 補足
- 参照: `docs/archive/unified-roadmap-2026-02-10.md` Phase 1A
