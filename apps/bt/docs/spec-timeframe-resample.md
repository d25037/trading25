# Timeframe Resample & Relative OHLC 仕様書

本文書は、trading25プロジェクトにおけるOHLCVデータのTimeframe変換（週足/月足リサンプル）およびRelative OHLC（ベンチマーク相対）変換の**正式仕様**を定義する。

## 目的

- apps/bt/（Python/pandas）とapps/ts/（TypeScript）間の仕様差異を解消
- apps/bt/をSingle Source of Truthとして確立
- 移行時の互換性テスト基準を明確化

---

## 1. Timeframe変換（Resample）

### 1.1 対応Timeframe

| Timeframe | 説明 | 周期 |
|-----------|------|------|
| `daily` | 日次（変換なし） | 1営業日 |
| `weekly` | 週次 | 月曜〜金曜 |
| `monthly` | 月次 | 月初〜月末 |

### 1.2 週足（Weekly）の境界定義

**週の開始**: 月曜日（ISO週に準拠）

**週の終了**: 金曜日（取引所営業日ベース）

**週キーの形式**: `YYYY-WXX`（例: `2024-W01`）

**境界計算ロジック**:
```python
# pandas実装
freq = "W"  # pandas.resample()ではW = 週末（日曜）アンカー
# 結果は週の最終営業日の日付をインデックスとする
```

```typescript
// ts実装
// 月曜（weekday=1）を週開始として計算
const dayOfWeek = date.getUTCDay();
const daysToMonday = dayOfWeek === 0 ? 6 : dayOfWeek - 1;
weekStart.setUTCDate(date.getUTCDate() - daysToMonday);
```

**統一仕様**:
- 週のグルーピングはISO週（月曜開始）に従う
- 出力される日付は**週開始日（月曜）**とする
- apps/bt/の`pandas.resample("W")`は日曜アンカーのため、インデックスを月曜に調整する必要あり

### 1.3 月足（Monthly）の境界定義

**月の開始**: 1日

**月の終了**: 月末最終日

**月キーの形式**: `YYYY-MM`（例: `2024-01`）

**境界計算ロジック**:
```python
# pandas実装
freq = "ME"  # Month End（月末アンカー）
```

**統一仕様**:
- 月のグルーピングはカレンダー月に従う
- 出力される日付は**月初日（1日）**とする
- apps/bt/の`pandas.resample("ME")`は月末アンカーのため、インデックスを月初に調整する必要あり

### 1.4 OHLC集約ルール

| フィールド | 集約方法 | 説明 |
|-----------|----------|------|
| `Open` | `first` | 期間最初の始値 |
| `High` | `max` | 期間中の最高値 |
| `Low` | `min` | 期間中の最安値 |
| `Close` | `last` | 期間最後の終値 |
| `Volume` | `sum` | 期間の出来高合計 |

**pandas実装**:
```python
resampled = df.resample(freq).agg({
    "Open": "first",
    "High": "max",
    "Low": "min",
    "Close": "last",
    "Volume": "sum",
})
```

### 1.5 欠損データの扱い

**原則**: 欠損日（祝日・休場日）はスキップし、実データのみで集約

**完全性基準**（includeIncomplete=false時）:
- **週足**: 2日以上のデータで有効
- **月足**: 10日以上のデータで有効

**dropna条件**:
- `Close`がNaNの期間は除外: `dropna(subset=["Close"])`

### 1.6 タイムゾーン

**統一仕様**: UTC（協定世界時）

- 日付文字列はISO 8601形式: `YYYY-MM-DD`
- 内部計算はUTCで統一
- 日本市場の取引日は`Asia/Tokyo`だが、日付文字列としてはUTC日付を使用

---

## 2. Relative OHLC（相対OHLC）

### 2.1 概要

株価OHLCVをベンチマーク（例: TOPIX）に対する相対値に変換する。

**計算式**:
```
relative_open = stock_open / benchmark_open
relative_high = stock_high / benchmark_high
relative_low = stock_low / benchmark_low
relative_close = stock_close / benchmark_close
volume = stock_volume  # Volumeはそのまま保持
```

### 2.2 対応ベンチマーク

| コード | 名称 | データソース |
|--------|------|------------|
| `topix` | 東証株価指数 | apps/ts/api market TOPIX |

### 2.3 日付アライメント

**原則**: 銘柄とベンチマークの共通日付のみで計算

**ロジック**:
```python
common_dates = stock_df.index.intersection(benchmark_df.index)
stock_aligned = stock_df.loc[common_dates]
bench_aligned = benchmark_df.loc[common_dates]
```

**欠損日（アライメント不可）の扱い**: スキップ

### 2.4 ゼロ除算の扱い

ベンチマークのOHLCいずれかが0の場合の処理:

| オプション | 動作 |
|-----------|------|
| `skip`（デフォルト） | その日を除外 |
| `zero` | 相対値を0.0とする |
| `null` | 相対値をNaNとする |

### 2.5 計算順序（重要）

**統一仕様**: Relative OHLC変換 → Timeframe Resample

```
Daily Stock OHLCV + Daily Benchmark OHLCV
    ↓
Relative OHLC計算（daily）
    ↓
Timeframe Resample（daily → weekly/monthly）
    ↓
Relative Weekly/Monthly OHLCV
```

**理由**:
- 日次の相対強度を維持したまま週足/月足に集約
- 先にresampleしてからrelative計算すると、週/月単位の平均的な相対強度になり意味が異なる

---

## 3. API仕様

### 3.1 POST /api/ohlcv/resample

**用途**: OHLCVデータのTimeframe変換

**Request**:
```json
{
  "stock_code": "7203",
  "source": "market",
  "timeframe": "weekly",
  "start_date": "2024-01-01",
  "end_date": "2025-02-01",
  "benchmark_code": "topix",  // optional
  "relative_options": {       // optional
    "handle_zero_division": "skip"
  }
}
```

**Response**:
```json
{
  "stock_code": "7203",
  "timeframe": "weekly",
  "benchmark_code": "topix",
  "meta": {
    "source_bars": 260,
    "resampled_bars": 52
  },
  "data": [
    {
      "date": "2024-01-01",
      "open": 2500.0,
      "high": 2600.0,
      "low": 2450.0,
      "close": 2580.0,
      "volume": 1000000
    }
  ]
}
```

### 3.2 既存API拡張: POST /api/indicators/compute

**output: "ohlcv"オプション追加**

インジケータを計算せず、変換後のOHLCVのみを返却:

```json
{
  "stock_code": "7203",
  "source": "market",
  "timeframe": "weekly",
  "benchmark_code": "topix",
  "output": "ohlcv",
  "indicators": []  // 空でもOK
}
```

---

## 4. データ検証

### 4.1 OHLC整合性チェック

**必須条件**:
- `high >= max(open, close, low)`
- `low <= min(open, close, high)`
- `volume >= 0`
- 全値が有限値（NaN/Inf不可）

**違反時**: 該当バーをスキップ（警告ログ出力）

### 4.2 時系列順序

**必須条件**: 日付は昇順であること

**違反時**: エラー（apps/ts/側はthrow、apps/bt/側はValueError）

---

## 5. 互換性テスト仕様

### 5.1 ゴールデンデータ

固定のOHLCVデータセットを用意し、apps/ts/とapps/bt/の出力を比較:

| テストケース | 入力 | 期待出力 |
|-------------|------|----------|
| 週足変換 | 20営業日の日次データ | 4週分の週足 |
| 月足変換 | 60営業日の日次データ | 3ヶ月分の月足 |
| 相対OHLC | 銘柄+TOPIX 20日分 | 相対OHLCV 20日分 |
| 相対+週足 | 銘柄+TOPIX 60日分 | 相対週足 12週分 |

### 5.2 許容誤差

- **OHLCV値**: 完全一致（0誤差）
- **日付**: 完全一致
- **レコード数**: 完全一致

### 5.3 差異発生時の対応

1. 差異内容をログ出力
2. apps/bt/の実装を本仕様書に合わせて調整
3. テスト再実行で完全一致を確認

---

## 6. 実装差異と統一方針

### 6.1 週足の出力日付

| 実装 | 現状 | 統一後 |
|------|------|--------|
| apps/ts/ | 週開始日（月曜） | 週開始日（月曜） |
| apps/bt/ (pandas) | 週終了日（日曜） | 週開始日（月曜）に調整 |

**apps/bt/調整方法**:
```python
# resample後にインデックスを週開始日に変換
resampled.index = resampled.index - pd.Timedelta(days=6)
```

### 6.2 月足の出力日付

| 実装 | 現状 | 統一後 |
|------|------|--------|
| apps/ts/ | 月初日（1日） | 月初日（1日） |
| apps/bt/ (pandas) | 月末日 | 月初日に調整 |

**apps/bt/調整方法**:
```python
# resample後にインデックスを月初日に変換
resampled.index = resampled.index.to_period('M').to_timestamp()
```

### 6.3 Relative OHLC handleZeroDivisionデフォルト

| 実装 | 現状 | 統一後 |
|------|------|--------|
| apps/ts/ | `zero` | `skip` |
| apps/bt/ | `skip` | `skip` |

**理由**: ゼロ除算データを含めるより除外する方が安全

---

## 7. 変更履歴

| 日付 | 変更内容 |
|------|---------|
| 2025-02-03 | 初版作成 |
