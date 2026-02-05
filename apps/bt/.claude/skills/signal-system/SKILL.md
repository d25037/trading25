# 統一Signalsシステム

## 設計思想

### 概念統一の原則

#### Entry Filters（絞り込み）
- **役割**: 戦略の基本エントリー条件を**AND条件で絞り込む**
- **YAML管理**: `entry_filter_params` として設定
- **例**: `period_breakout`, `bollinger_bands`, `volume_surge`, `fundamental.per`, `beta`
- **処理**: 基本エントリーシグナル ∩ フィルター条件（AND結合）
- **コンセプト**: "Filter"は条件を満たすものだけを**通過させる**

#### Exit Triggers（発火）
- **役割**: 戦略の基本エグジット条件に**OR条件で追加発火**
- **YAML管理**: `exit_trigger_params` として設定
- **例**: `atr_support_break`, `volume_drop`, `crossover`, `bollinger_bands`
- **処理**: 基本エグジットシグナル ∪ トリガー条件（OR結合）
- **コンセプト**: "Trigger"は追加の売却条件を**発火させる**

## 統一アーキテクチャ

### モデル定義
- **models/signals.py**: 全シグナルパラメータの統一管理（Signals + SignalParams）
  - **Signalsクラス**: 型安全なエントリー・エグジットシグナル管理（pd.Series[bool]バリデーション）
  - **SignalParamsクラス**: 統一シグナルパラメータ（旧FilterParams + TriggerParams統合完了）

### シグナル実装（35種類）

**実装箇所**: `strategies/signals/`

1. **breakout.py**: ブレイクアウトシグナル（期間ブレイク・MAブレイク・リトレースメント統合）
2. **buy_and_hold.py**: Buy&Holdシグナル
3. **crossover.py**: クロスオーバーシグナル（SMA/RSI/MACD/EMA）
4. **mean_reversion.py**: 平均回帰シグナル（乖離・回復統合・deviation/recovery無効化対応）
5. **rsi_threshold.py**: RSI閾値シグナル（買われすぎ・売られすぎ判定）
6. **rsi_spread.py**: RSIスプレッドシグナル（短期・長期RSI差分判定）
7. **risk_adjusted.py**: リスク調整リターンシグナル（シャープ/ソルティノレシオ）
8. **beta.py**: β値シグナル（Numba最適化・負のβ値対応・ベンチマークデータ自動ロード）
9. **fundamental.py**: 財務指標シグナル（PER/PBR/ROE/Forward EPS成長/EPS成長（実績）/PEG/営業利益率/配当利回り/営業CF/簡易FCF/CFO利回り/簡易FCF利回り/時価総額）
10. **margin.py**: 信用残高シグナル
11. **volatility.py**: ボラティリティシグナル（ボリンジャーバンド）
12. **volume.py**: 出来高シグナル（surge/drop両方向・SMA/EMA選択可能）
13. **trading_value.py**: 売買代金シグナル（surge/drop方向制御・絶対閾値指定）
14. **trading_value_range.py**: 売買代金レンジシグナル（範囲指定フィルター）
15. **index_daily_change.py**: 指数前日比シグナル（市場環境フィルター）
16. **index_macd_histogram.py**: INDEXヒストグラムシグナル（市場モメンタムフィルター）
17. **performance.py**: 相対パフォーマンスシグナル
18. **sector.py**: セクターシグナル
19. **sector_strength.py**: セクター強度シグナル（ランキング・ローテーション位相・ボラティリティレジーム）
20. **registry.py**: シグナルレジストリシステム（35シグナル統合管理）
21. **processor.py**: SignalProcessor（統一シグナル処理・AND/OR結合制御・セクターデータ注入）

### SignalProcessor

**エントリーAND結合・エグジットOR結合制御**

```python
# エントリーシグナル = 基本エントリー ∩ Filter1 ∩ Filter2 ∩ ...
entry_signal = base_entry & filter1 & filter2

# エグジットシグナル = 基本エグジット ∪ Trigger1 ∪ Trigger2 ∪ ...
exit_signal = base_exit | trigger1 | trigger2
```

### 両用設計

同一シグナル関数でエントリー・エグジット両対応（direction/condition切り替え）

```python
def volume_signal(
    data: pd.DataFrame,
    params: VolumeSignalParams,
    direction: str = "surge",  # "surge" or "drop"
    condition: str = "entry"    # "entry" or "exit"
) -> pd.Series[bool]:
    """出来高シグナル（エントリー・エグジット両用）"""
    ...
```

## シグナルパラメータ（19種類統合）

### OHLCV系シグナル
- **VolumeSignalParams**: surge/drop方向制御・ma_type追加（SMA/EMA選択可能）
- **TradingValueSignalParams**: surge/drop方向制御・絶対閾値指定（億円単位）
- **PeriodBreakoutParams**: 期間ブレイクアウトシグナル
- **MABreakoutParams**: 移動平均線ブレイクアウトシグナル
- **BollingerBandsSignalParams**: ボリンジャーバンドシグナル

### テクニカル系シグナル
- **CrossoverSignalParams**: クロスオーバーシグナル（SMA/RSI/MACD/EMA）
- **MeanReversionSignalParams**: 平均回帰シグナル（deviation/recovery無効化対応）
- **RSIThresholdSignalParams**: RSI閾値シグナル（買われすぎ・売られすぎ判定）
- **RSISpreadSignalParams**: RSIスプレッドシグナル（短期・長期RSI差分判定）
- **RiskAdjustedReturnSignalParams**: リスク調整リターンシグナル（シャープ/ソルティノレシオ）
- **ATRSupportBreakParams**: ATRサポートラインブレイクシグナル
- **RetracementSignalParams**: リトレースメントシグナル（フィボナッチ下落率ベース）

### ファンダメンタル系シグナル
- **FundamentalSignalParams**: 財務指標シグナル（14種類: PER・PBR・ROE・Profit成長・Sales成長・PEG・Forward EPS・営業利益率・配当利回り・営業CF・簡易FCF・CFO利回り・簡易FCF利回り・時価総額）
  - **condition: above | below**: 全サブパラメータに対応（閾値との比較方向を柔軟に設定可能）
  - デフォルト: PER/PBR/PEG=`below`、ROE/営業利益率/配当利回り/営業CF/成長率系/利回り系=`above`、時価総額=`above`
  - **consecutive_periods**: 営業CF・簡易FCFで「直近N回連続で条件を満たす」チェック（デフォルト=1で従来動作）
  - **use_floating_shares**: CFO利回り・簡易FCF利回り・時価総額で使用。True=流通株式（発行済み-自己株式）、False=発行済み全体
  - **market_cap**: 時価総額閾値（億円単位）。大型株フィルター（entry: above）や小型株除外（exit: below）に利用
- **BetaSignalParams**: β値シグナル（負のβ値対応: min_beta範囲-2.0〜5.0）

### 財務データのperiod_type設定

財務諸表データのロード時に決算期間を指定可能（`load_statements_data()`）:

| period_type | 対象 | 推奨シグナル |
|-------------|------|-------------|
| `"FY"` (デフォルト) | 本決算のみ | PER, PBR, ROE, Forward EPS成長, EPS成長（実績）, 配当利回り, PEG |
| `"2Q"` | 中間決算のみ | 営業キャッシュフロー（中間決算でも発表） |
| `"all"` | 全四半期 | 売上高, 営業利益率（最新情報優先時） |

**FYのみで利用可能なデータ**: BPS, DividendFY, NextYearForecastEPS

### 市場環境系シグナル
- **MarginSignalParams**: 信用残高シグナル
- **VolatilitySignalParams**: ボラティリティシグナル
- **IndexDailyChangeSignalParams**: 指数前日比シグナル（市場環境フィルター）
- **IndexMACDHistogramSignalParams**: INDEXヒストグラムシグナル（市場モメンタムフィルター）

### セクター強度系シグナル
- **SectorStrengthRankingParams**: セクター強度ランキング（複合スコアで上位Nセクターのみエントリー許可）
- **SectorRotationPhaseParams**: セクターローテーション位相（RRG的4象限分類: leading/weakening）
- **SectorVolatilityRegimeParams**: セクターボラティリティレジーム（低ボラ/高ボラ環境フィルタ）

### その他
- **BuyAndHoldSignalParams**: Buy&Holdシグナル

## YAML設定例

```yaml
entry_filter_params:
  period_breakout:
    period: 100
  volume_surge:
    short_window: 5
    long_window: 20
    surge_threshold: 1.5
  beta:
    min_beta: 0.8
    max_beta: 1.5
  # ファンダメンタル系（condition指定例）
  fundamental:
    enabled: true
    per:
      enabled: true
      threshold: 20.0
      condition: above  # PER > 20（割高株を選定）
    roe:
      enabled: true
      threshold: 5.0
      condition: below  # ROE < 5%（低ROE株を選定）

exit_trigger_params:
  atr_support_break:
    atr_period: 14
    atr_multiplier: 2.0
  volume_drop:
    short_window: 5
    long_window: 20
    drop_threshold: 0.7
```

## レガシー削除完了

- `models/filters.py` → 完全削除
- `models/triggers.py` → 完全削除
- `horizontal_price_action.py` → `breakout.py`に統合

## 関連ファイル

- `src/models/signals.py`
- `src/strategies/signals/processor.py`
- `src/strategies/signals/registry.py`
- `src/strategies/signals/*.py`（21種類のシグナル実装）
- `src/server/services/signal_reference_service.py` — シグナルリファレンス構築サービス
- `src/server/schemas/signal_reference.py` — レスポンススキーマ
- `src/server/routes/signal_reference.py` — APIエンドポイント
- `config/strategies/*/entry_filter_params`
- `config/strategies/*/exit_trigger_params`

## シグナル追加時のチェックリスト

新しいシグナルを追加する際は、以下の全ファイルを更新すること：

### 必須（バックエンド）
1. `src/strategies/signals/<signal_name>.py` - シグナル関数実装
2. `src/models/signals.py` - Pydanticモデル追加 + SignalParamsにフィールド追加
3. `src/strategies/signals/registry.py` - インポート追加 + SIGNAL_REGISTRY登録（`category`/`description`/`param_key`フィールド必須）
4. `src/strategies/signals/__init__.py` - エクスポート追加
5. `tests/unit/strategies/signals/test_<signal_name>.py` - テスト作成

### 必須（最適化機能）
6. `src/optimization/param_builder.py` - `SIGNAL_PARAM_CLASSES`辞書にパラメータクラス登録

**注意**: optimize機能への登録を忘れると、パラメータ最適化でシグナルが使えない

### フロントエンド（apps/ts/）
フロントエンドは apps/ts/web/ に移行済み。新シグナル追加時はapps/ts/側のUI更新も必要。

## パフォーマンス：ベクトル化必須

### 重要性
シグナル関数は**必ずベクトル化**すること。非ベクトル化コードは約7倍以上遅くなる。

### 避けるべきパターン（低速）
```python
# ❌ iterrows - 非常に遅い
for _, row in df.iterrows():
    result.append(some_calculation(row))

# ❌ apply - 遅い
df.apply(lambda row: some_calculation(row), axis=1)

# ❌ Pythonループ
for i in range(len(df)):
    result[i] = some_calculation(df.iloc[i])
```

### 推奨パターン（高速）
```python
# ✅ pandas/numpy ベクトル演算
result = df['A'] > df['B'] * threshold

# ✅ pandas rolling（ループより約7倍高速）
result = returns.rolling(window=period).std()

# ✅ VectorBTインジケータ（内部でベクトル化済み）
ma = vbt.indicators.MA.run(close, period).ma

# ✅ numpy ベクトル演算
result = np.where(condition, value_if_true, value_if_false)
```

### 参考事例
- `risk_adjusted.py:17-42`: apply() → rolling().std() で7倍高速化
- `volume.py`: VectorBTのMAインジケータ使用（ベクトル化済み）
- `beta.py`: Numba JIT最適化でさらなる高速化
