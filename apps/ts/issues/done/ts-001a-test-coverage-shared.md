---
id: ts-001a
title: "Test Coverage: shared パッケージ"
status: done
priority: medium
labels: []
project: ts
created: 2026-01-30
updated: 2026-01-31
depends_on: []
blocks: []
parent: "ts-001"
---

# ts-001a Test Coverage: shared パッケージ

## 目的
shared パッケージの重要ロジックに単体/統合テストを追加し、最低カバレッジ 60% を達成する。

## 受け入れ条件
- `bun run --filter @trading25/shared test` が安定して通る
- shared のカバレッジが 60% 以上
- 重要な失敗系（JSON 破損/通信失敗/タイムアウト）のテストがある

## 実施内容
19個のテストファイルを新規作成し、純粋関数を中心にテストカバレッジを向上させた。

### 新規テストファイル
1. `src/portfolio-performance/__tests__/calculations.test.ts` — P&L計算、フォーマット関数
2. `src/portfolio-performance/__tests__/benchmark.test.ts` — ベンチマーク比較
3. `src/factor-regression/__tests__/regression.test.ts` — OLS回帰、残差計算
4. `src/factor-regression/__tests__/returns.test.ts` — 日次リターン計算
5. `src/utils/timeout-utils.test.ts` — タイムアウト管理
6. `src/utils/error-helpers.test.ts` — エラー分類ヘルパー
7. `src/errors/__tests__/index.test.ts` — カスタムエラークラス
8. `src/config/__tests__/index.test.ts` — 設定管理
9. `src/ta/bollinger.test.ts` — ボリンジャーバンド
10. `src/ta/atr-support.test.ts` — ATRサポート
11. `src/ta/n-bar-support.test.ts` — N日安値サポート
12. `src/ta/trading-value-ma.test.ts` — 売買代金MA
13. `src/watchlist/__tests__/types.test.ts` — Watchlistエラー階層
14. `src/dataset/__tests__/utils.test.ts` — データセットユーティリティ
15. `src/screening/volume-utils.test.ts` — 出来高分析
16. `src/ta/relative/utils.test.ts` — 相対OHLC関数
17. `src/db/query-builder-helpers.test.ts` — SQLクエリヘルパー
18. `src/test-utils/array-helpers.test.ts` — 配列型ガード
19. `src/test-utils/fetch-mock.test.ts` — モックレスポンス

### 変更ファイル
- `scripts/check-coverage.ts` — shared閾値を 0.5 → 0.6 に更新

## 結果
- **テスト数**: 927 pass, 0 fail (63ファイル)
- **カバレッジ (lcov)**: Lines 60.32%, Functions 75.57%
- **カバレッジゲート**: 通過 (`bun run check:coverage`)
- **Lint/Typecheck**: shared パッケージ通過

## 補足
- `CODE_REVIEW_ISSUES.md` の Issue #1, #3, #5, #6, #13 に関連
