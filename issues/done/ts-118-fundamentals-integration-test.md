---
id: ts-118
title: "FundamentalsDataService 統合テスト追加"
status: done
priority: medium
labels: [test]
project: ts
created: 2026-02-02
updated: 2026-02-02
closed: 2026-02-02
depends_on: []
blocks: [ts-117]
---

# ts-118 FundamentalsDataService 統合テスト追加

## 現状
- `packages/api/src/services/fundamentals-data.ts` のカバレッジは **4.43%**
- 既存テストはルートハンドラレベル (`routes/__tests__/fundamentals.test.ts`) のみで、サービスを完全にモックしている
- `FundamentalsDataService` の計算ロジック（ROE, PER, PBR, EPS, FCF, daily valuation 等）が未テスト

## 目標
- 外部API依存の層であるため、**統合テスト**でカバーする
- JQuants API レスポンスをモックし、`FundamentalsDataService.getFundamentals()` の計算結果を検証

## テスト対象メソッド
- `getFundamentals()` — メインの公開メソッド
- `calculateAllMetrics()` — ROE, PER, PBR, EPS 等の財務指標計算
- `calculateDailyValuation()` — 日次 PER/PBR 時系列計算
- `enhanceLatestMetrics()` — forecast EPS, 前期キャッシュフローによる補強
- `annotateLatestFYWithRevision()` — 四半期決算からの修正予想反映
- `getForecastEps()` — 予想 EPS 変化率の算出

## アプローチ
- `BaseJQuantsService` の外部API呼び出しをモックし、固定のstatements/pricesデータで計算結果を検証
- エッジケース（データ欠損、単一期のみ、forecast未公表）もカバー

## 変更対象
- `packages/api/src/services/__tests__/fundamentals-data.test.ts` — 新規作成

## 受け入れ条件
- `bun run test` 全パス
- `fundamentals-data.ts` のカバレッジが大幅に改善される

## 結果
- `packages/api/src/services/fundamentals-data.test.ts` を新規作成（39テスト、7 describe グループ）
- カバレッジ: 4.43% → **94.87%**（branch 100%）
- API パッケージ全体: 418 pass, 0 fail
- モック戦略: mock.module() + spread passthrough で外部依存のみモック、shared計算ロジックは実行
