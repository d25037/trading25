# Phase 2.5 並走検証レポート

**日付**: 2026-02-03
**目的**: apps/bt/ API (Python/vectorbt) と apps/ts/ (TypeScript) のインジケータ計算結果の一致を検証し、Phase 3 移行の安全性を確認する。

---

## 1. 精度比較サマリー

### 判定基準

| 指標 | 閾値 |
|------|------|
| 不一致率 (暖気期間除外後) | < 0.1% |
| P95 レイテンシ | < 800ms |
| API エラー率 | < 1% |

### SMA系インジケータ（完全一致が期待される）

| インジケータ | パラメータ | 不一致数 | 比較数 | 結果 |
|---|---|---|---|---|
| SMA | period=20 | 0 | 225 | PASS |
| Bollinger Bands | period=20, std=2.0 | 0 | 675 | PASS |
| N-Bar Support | period=20 | 0 | 225 | PASS |
| Volume Comparison | short=20, long=100 | 0 | 433 | PASS |
| Trading Value MA | period=20 (スケーリング補正) | 0 | 225 | PASS |

### EMA系インジケータ（暖気期間後の収束を検証）

| インジケータ | パラメータ | 暖気バー数 | 収束後不一致 | 結果 |
|---|---|---|---|---|
| EMA | period=20 | 100 | 0 | PASS |
| PPO | 12/26/9 | 150 | 0 | PASS |
| ATR | period=14 | 100 | 0 | PASS |
| ATR Support | lookback=20, mult=2.0 | 120 | 0 | PASS |

### アルゴリズム差異（非収束）

| インジケータ | apps/bt/ アルゴリズム | TS アルゴリズム | 差異の原因 |
|---|---|---|---|
| RSI | vbt.RSI (Wilder's smoothing, 1/N decay) | EMA-based (2/(N+1) decay) | smoothing方式が根本的に異なる |
| MACD | vbt.MACD (pandas ewm adjust=True) | SMA初期化 EMA | EMA初期化差異がMACD line で増幅される |

**対応方針**: フロントエンド（apps/ts/web）ではTS側の計算結果を使用する。apps/bt/ APIの RSI/MACD は戦略バックテスト用途のみで使用し、表示値はTS計算に依存する。

### 信用指標

信用指標はTS側とapps/bt/側でアルゴリズムが異なる（TS: 取引日ベースのルックバック、apps/bt/: pandas rolling）。Golden Dataset生成は完了しているが、apps/bt/ APIとの直接比較はスコープ外（信用指標はTS側計算をフロントエンドで使用）。

---

## 2. ATR Support 修正

### 変更内容

| 要素 | 修正前 (apps/bt/) | 修正後 (apps/bt/) | TS実装 |
|---|---|---|---|
| ATR計算 | `rolling().mean()` (SMA) | `vbt.MA.run(ewm=True)` (EMA) | `ema(trueRange, period)` |
| 最高値 | `high.rolling().max()` | `close.rolling().max()` | `highestClose(closes, period)` |

**変更ファイル**: `apps/bt/src/utils/indicators.py` - `compute_atr_support_line()`

**既存テスト**: 全19テスト合格（テストはアルゴリズム非依存の振る舞い検証）

---

## 3. レイテンシサマリー

計測日: 2026-02-03 / データソース: market (apps/ts/ API経由)

| シナリオ | P50 | P95 | P99 | Mean | エラー率 | 結果 |
|---|---|---|---|---|---|---|
| 4インジケータ × 100回 | 23ms | 24ms | 109ms | 24ms | 0.0% | PASS |
| 全11インジケータ × 10回 | 48ms | 61ms | 61ms | 49ms | 0.0% | PASS |

全シナリオで P95 < 800ms、エラー率 < 1% の基準をクリア。

---

## 4. Phase 3 Go/No-Go 判定

### チェックリスト

- [x] SMA系インジケータ（5種）: 完全一致
- [x] EMA系インジケータ（4種）: 暖気期間後に収束
- [x] RSI/MACDのアルゴリズム差異: 文書化済み、フロントエンドはTS計算使用
- [x] ATR Support: Pine Script準拠に修正済み
- [x] NaN位置: 全インジケータで一致
- [x] Golden Datasetテスト: 15/15 PASS
- [x] レイテンシ: P95 24ms / 61ms (< 800ms)
- [x] エラー率: 0.0% (< 1%)

### 判定: **Go**

全チェック項目をクリア。Phase 3（旧コード削除）への移行を推奨する。

**既知の制約**:
- RSI/MACDはvbt内部アルゴリズムとTS側で異なる値を返す。Phase 3移行後もapps/bt/ APIのRSI/MACDはバックテスト用途のみ。フロントエンド表示はTS側計算に依存。
- EMA系は244バーのフィクスチャデータで100-150バーの暖気が必要。実運用データ（1年以上）では問題なく収束。

---

## 5. 成果物一覧

| ファイル | 用途 |
|---------|------|
| `apps/ts/.../generate-golden-datasets.ts` | Golden Dataset生成スクリプト |
| `apps/ts/.../golden/*.json` (16ファイル) | Golden Data (11テクニカル + 3信用 + 2入力) |
| `apps/bt/tests/server/test_indicator_golden.py` | 精度比較テスト (15テスト) |
| `apps/bt/scripts/measure_indicator_latency.py` | レイテンシ計測 |
| `apps/bt/src/utils/indicators.py` | ATR Support修正 |
| `docs/archive/reports/phase2_5_verification_report.md` | 本レポート |
