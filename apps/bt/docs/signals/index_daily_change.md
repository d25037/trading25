# 指数前日比シグナル（Index Daily Change Signal）

## 概要

指数（TOPIX等）の前日比に基づいてシグナルを生成するフィルター機能です。

**目的**:
- 短期スイングトレードで市場の過熱を避ける
- 翌日の利益確定売りを回避する
- 超短期の逆張り状態を目指す

**実装日**: 2025-10-20

## 使用方法

### エントリーフィルター（市場過熱回避）

市場が大きく上昇していない日にエントリーする設定：

```yaml
entry_filter_params:
  index_daily_change:
    enabled: true
    max_daily_change_pct: 1.0  # +1.0%以下の日のみエントリー
    direction: "below"
```

**効果**:
- 市場が+1.0%以下の日のみエントリー
- 市場が過熱している日（+1.0%超）はエントリーしない
- 翌日の利益確定売りリスクを軽減

### エグジットトリガー（市場急騰利確）

市場が大きく上昇した日に利益確定する設定：

```yaml
exit_trigger_params:
  index_daily_change:
    enabled: true
    max_daily_change_pct: 1.5  # +1.5%超の日にエグジット
    direction: "above"
```

**効果**:
- 市場が+1.5%を超える日にエグジット
- 市場急騰時に利益を確定
- 急落リスクを回避

## パラメータ

### IndexDailyChangeSignalParams

| パラメータ | 型 | デフォルト | 範囲 | 説明 |
|-----------|-----|-----------|------|------|
| `enabled` | bool | `false` | - | シグナル有効フラグ |
| `max_daily_change_pct` | float | `1.0` | -10.0〜10.0 | 前日比閾値（%単位） |
| `direction` | str | `"below"` | `"below"`/`"above"` | 判定方向 |

### direction パラメータの意味

- **`"below"`**: 前日比が閾値以下の日にTrue（エントリーフィルター用）
  - 例: `max_daily_change_pct=1.0`, `direction="below"` → 前日比 ≤ +1.0%の日にエントリー
- **`"above"`**: 前日比が閾値を超える日にTrue（エグジットトリガー用）
  - 例: `max_daily_change_pct=1.5`, `direction="above"` → 前日比 > +1.5%の日にエグジット

## ベンチマークデータ

このシグナルを使用する場合、自動的にベンチマークデータ（TOPIX等）がロードされます。

### ベンチマーク設定

```yaml
shared_config:
  benchmark_table: "topix"  # TOPIXをベンチマークとして使用
```

## 使用例

### 例1: 市場過熱回避戦略

市場が+1.0%以下の日のみエントリーし、+2.0%超の日にエグジット：

```yaml
entry_filter_params:
  index_daily_change:
    enabled: true
    max_daily_change_pct: 1.0
    direction: "below"

exit_trigger_params:
  index_daily_change:
    enabled: true
    max_daily_change_pct: 2.0
    direction: "above"
```

### 例2: 下落相場フィルター

市場が下落している日のみエントリー（逆張り戦略）：

```yaml
entry_filter_params:
  index_daily_change:
    enabled: true
    max_daily_change_pct: -0.5  # -0.5%以下（下落）の日にエントリー
    direction: "below"
```

### 例3: 他のシグナルとの組み合わせ

レンジブレイク + 市場環境フィルター：

```yaml
entry_filter_params:
  # レンジブレイク（主要シグナル）
  period_breakout:
    enabled: true
    period: 200
    direction: "high"
    condition: "break"
    lookback_days: 5

  # 市場環境フィルター
  index_daily_change:
    enabled: true
    max_daily_change_pct: 1.0
    direction: "below"

  # 出来高フィルター
  volume:
    enabled: true
    direction: "surge"
    threshold: 1.5
    short_period: 20
    long_period: 100
```

## 注意事項

### 浮動小数点誤差対策

前日比の計算では浮動小数点誤差が発生する可能性があるため、内部で小数点以下4桁に丸めています（0.0001%の精度）。

### 初日のシグナル

初日は前日比が計算できないため、常に`False`になります。

### ベンチマークデータの自動ロード

このシグナルが有効な場合、自動的にベンチマークデータ（TOPIX等）がロードされます。ベンチマークデータが存在しない場合、シグナルはスキップされます。

## 関連ファイル

- **実装**: `src/strategies/signals/index_daily_change.py`
- **パラメータモデル**: `src/models/signals.py` (`IndexDailyChangeSignalParams`)
- **レジストリ**: `src/strategies/signals/registry.py`
- **テスト**: `tests/unit/strategies/signals/test_index_daily_change.py`
- **設定例**: `config/strategies/experimental/example_index_daily_change.yaml`

## テスト状況

✅ 全12テストケースが合格
- 基本機能テスト（direction="below"/"above"）
- 異なる閾値のテスト
- エラーハンドリングテスト
- エッジケーステスト
- 負の閾値テスト
- NaN値テスト

## 統合システム対応

- ✅ 統一シグナルシステムに統合
- ✅ レジストリシステムに登録
- ✅ ベンチマークデータ自動ロード対応
- ✅ エントリー・エグジット両用設計
- ✅ YAML完全制御対応

## 実装日

2025年10月20日
