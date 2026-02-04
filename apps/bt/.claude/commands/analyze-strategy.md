# /analyze-strategy

バックテスト結果を詳細分析し、投資戦略パラメータの改善を提案します。

**使用方法**: `/analyze-strategy <strategy_name>`
- 例: `/analyze-strategy range_break_v5`
- 例: `/analyze-strategy macd_cross`

---

## Step 1: 戦略設定ファイルの読み込み

指定された戦略名から、YAML設定ファイルを検索・読み込みます：

1. `config/strategies/production/<strategy_name>.yaml` を探す
2. 見つからなければ `config/strategies/experimental/<strategy_name>.yaml` を探す
3. どちらも見つからない場合はエラーを報告し、利用可能な戦略一覧を表示

### 読み込む情報
- **strategy_params**: 戦略基本パラメータ
- **entry_filter_params**: エントリーフィルター設定（AND条件絞り込み）
- **exit_trigger_params**: エグジットトリガー設定（OR条件発火）

---

## Step 2: 最新実行結果ipynbの読み込み

`notebooks/generated/<strategy_name>/` ディレクトリから最新のバックテスト結果を取得します：

1. ディレクトリ内の全ipynbファイルをリストアップ
2. ファイル名のタイムスタンプ（`all_YYYYMMDD_HHMMSS.ipynb`）でソート
3. 最新のipynbファイルを読み込み

### 抽出すべき情報（ipynbのセルから）

#### パフォーマンス指標
- Total Return [%]
- Benchmark Return [%]
- Win Rate [%]
- Profit Factor
- Expectancy
- Total Trades（取引回数）
- 同時保有ポジション最大数（資金効率の評価）

#### リスク指標
- Max Drawdown [%]
- Max Drawdown Duration
- Sharpe Ratio
- Sortino Ratio
- Calmar Ratio
- Omega Ratio
- Annualized Volatility

#### 取引統計
- Total Closed Trades / Total Open Trades
- Best Trade [%] / Worst Trade [%]
- Avg Winning Trade [%] / Avg Losing Trade [%]
- Avg Winning Trade Duration / Avg Losing Trade Duration
- Max Win Streak / Max Loss Streak

**注意**: ipynbはJSONフォーマットです。セル出力から数値を抽出する際は、text出力部分を解析してください。

---

## Step 3: パフォーマンス分析

Step 1・2で取得した情報を総合的に分析し、以下の観点で評価します：

### 3.1 収益性評価
- **Total Return vs Benchmark**: ベンチマーク（TOPIX等）を上回っているか
- **Win Rate**: 勝率が40%以上か（低い場合は要改善）
- **Profit Factor**: 1.5以上が理想（1.0未満は赤字）
- **Expectancy**: 1トレードあたりの期待収益が十分か

### 3.2 リスク評価
- **Sharpe Ratio**: 1.0以上が理想（リスク調整後リターンの評価）
- **Sortino Ratio**: 下方リスクのみを考慮した評価
- **Max Drawdown**: 30%未満が理想（大きすぎる場合は資金管理要改善）
- **Drawdown Duration**: 回復期間が長すぎないか

### 3.3 取引効率評価
- **取引頻度**: 多すぎる（手数料負担大）／少なすぎる（機会損失）
- **平均保有期間**: 短期すぎる（ノイズトレード）／長期すぎる（資金固定化）
- **勝ち/負けトレード比**: バランスが偏りすぎていないか
- **同時保有ポジション数**: 資金効率が低い（1-2銘柄のみ）場合は分散不足

### 3.4 問題点の特定
以下のパターンに該当する場合は具体的に指摘：
- **高勝率・低利益**: 利食いが早すぎる（exit条件見直し）
- **低勝率・高利益**: 損切りが早すぎる（entry条件見直し）
- **高ドローダウン**: リスク管理不足（ストップロス追加）
- **低取引回数**: フィルターが厳しすぎる（条件緩和）
- **高取引回数**: フィルターが緩すぎる（条件強化）
- **低資金効率**: 分散が不足（フィルター緩和・銘柄数増加）

---

## Step 4: 改善提案の生成

Step 3の分析結果に基づき、具体的な改善策を優先度付きで提案します。

### 4.1 パラメータ最適化
現在のYAML設定を見て、調整すべきパラメータを提案：

#### Entry Filter Params
- **period_breakout**:
  - `lookback_days`が短すぎる/長すぎる場合の調整案
  - `period`の最適値提案
- **bollinger_bands**:
  - `alpha`（標準偏差倍率）の調整（1.0σ → 1.5σ等）
  - `position`条件の変更提案
- **volume**:
  - `threshold`（出来高倍率）の調整
  - `short_period`, `long_period`の最適化
- **crossover**:
  - `fast_period`, `slow_period`, `signal_period`の調整
  - `lookback_days`（クロス検出期間）の最適化

#### Exit Trigger Params
- **atr_support_break**:
  - `atr_multiplier`の調整（利確・損切りバランス）
  - `lookback_period`の最適化
- **volume**:
  - `drop_threshold`の調整（出来高減少判定）
- **crossover**:
  - デッドクロス条件の調整

### 4.2 フィルター追加・有効化
現在無効（`enabled: false`）のフィルターで、追加すべきものを提案：

- **fundamental**: PER/ROE/EPS成長率でファンダメンタル絞り込み
- **beta**: β値フィルターで市場感応度制御
- **volatility**: ボラティリティフィルターで高リスク銘柄除外
- **relative_performance**: ベンチマーク比較で強い銘柄に絞る
- **margin**: 信用残高で需給バランス判定

**提案基準**:
- Win Rateが低い → entryフィルター強化（fundamental, beta等）
- 取引回数が少ない → entryフィルター緩和（volume閾値下げ等）
- Drawdownが大きい → リスク管理フィルター追加（volatility, beta等）

### 4.3 エグジット条件強化
損切り・利確ロジックの改善提案：

- **ATRベースストップロス**: `atr_support_break`のパラメータ調整
- **利確レベル追加**: 一定利益で確定売りする条件追加
- **時間ベース手仕舞い**: 長期保有銘柄の自動手仕舞い条件

### 4.4 リスク管理改善
ポートフォリオレベルの改善提案：

- **ポジションサイズ**: `cash_sharing`, `group_by`設定の最適化
- **分散強化**: フィルター緩和で保有銘柄数増加
- **ドローダウン対策**: 最大損失制限・一時停止条件の追加

### 4.5 YAML修正例の提供
上記提案を反映した、具体的なYAML設定変更例を提示：

```yaml
# 改善案1: XXXパラメータ調整
entry_filter_params:
  period_breakout:
    lookback_days: 15  # 10 → 15に変更（理由：XXX）
    period: 150        # 200 → 150に変更（理由：XXX）

# 改善案2: ファンダメンタルフィルター追加
entry_filter_params:
  fundamental:
    per_enabled: true        # 有効化
    per_threshold: 15.0      # PER 15倍以下
    roe_enabled: true        # 有効化
    roe_threshold: 10        # ROE 10%以上

# 改善案3: エグジット条件強化
exit_trigger_params:
  atr_support_break:
    atr_multiplier: 2.5  # 3.0 → 2.5に変更（早期損切り）
```

---

## 出力フォーマット

分析結果をMarkdown形式で以下のように出力してください：

```markdown
# 📊 戦略分析レポート: <strategy_name>

## 現在の設定サマリー
- **戦略名**: <strategy_name>
- **設定ファイル**: config/strategies/<production|experimental>/<strategy_name>.yaml
- **最新実行結果**: notebooks/generated/<strategy_name>/<filename>.ipynb
- **データ期間**: <start_date> ~ <end_date>

### Entry Filter設定
- 有効フィルター: <enabled filters>
- 主要パラメータ: <key parameters>

### Exit Trigger設定
- 有効トリガー: <enabled triggers>
- 主要パラメータ: <key parameters>

---

## 📈 バックテスト結果分析

### 収益性指標
| 指標 | 値 | 評価 |
|------|-----|------|
| Total Return | XX.XX% | ⭐⭐⭐ |
| Benchmark Return | XX.XX% | - |
| Win Rate | XX.XX% | ⭐⭐ |
| Profit Factor | X.XX | ⭐⭐⭐ |
| Expectancy | XX,XXX | ⭐⭐ |

### リスク指標
| 指標 | 値 | 評価 |
|------|-----|------|
| Sharpe Ratio | X.XX | ⭐⭐⭐ |
| Sortino Ratio | X.XX | ⭐⭐⭐ |
| Max Drawdown | XX.XX% | ⭐⭐ |
| Calmar Ratio | X.XX | ⭐⭐⭐ |

### 取引統計
- **総取引回数**: XXX回
- **同時保有ポジション最大数**: XX銘柄
- **平均保有期間**: XX日
- **勝ちトレード平均**: +XX.XX%
- **負けトレード平均**: -XX.XX%

---

## ⚠️ 検出された問題点

1. **[問題カテゴリ]**: 具体的な問題内容
   - 原因: XXX
   - 影響: XXX

2. **[問題カテゴリ]**: 具体的な問題内容
   - 原因: XXX
   - 影響: XXX

---

## 🎯 改善提案（優先度順）

### 【優先度：高】改善案1: <タイトル>
**目的**: XXX
**期待効果**: XXX

#### 具体的変更
\```yaml
# config/strategies/<production|experimental>/<strategy_name>.yaml
entry_filter_params:
  xxx:
    enabled: true
    threshold: X.X
\```

**理由**: XXX

---

### 【優先度：中】改善案2: <タイトル>
...

---

### 【優先度：低】改善案3: <タイトル>
...

---

## 💡 次のステップ

1. 提案された改善案から優先度の高いものを選択
2. YAML設定ファイルを編集
3. `uv run execute-notebook run <strategy_name>` で再実行
4. 結果を比較して効果を確認
5. 必要に応じてパラメータを微調整

---

## 📝 補足情報

- **データセット**: <db_path>
- **初期資金**: <initial_cash>
- **手数料**: <fees>
- **ベンチマーク**: <benchmark_table>
```

---

## エラーハンドリング

以下の場合は処理を中断し、ユーザーに確認してください：

1. **戦略名が見つからない**:
   - `config/strategies/`内の利用可能な戦略をリスト表示
   - 正しい戦略名を再入力してもらう

2. **実行結果ipynbが存在しない**:
   - `notebooks/generated/<strategy_name>/` に結果がないことを報告
   - 先に `uv run execute-notebook run <strategy_name>` を実行するよう促す

3. **ipynbの解析に失敗**:
   - どのセルでエラーが発生したかを報告
   - 手動でNotebookを確認するよう促す

4. **YAML設定が不正**:
   - バリデーションエラーの詳細を報告
   - 設定ファイルの修正を促す

---

## 注意事項

- ipynbファイルはJSON形式です。`nbformat`ライブラリまたは直接JSONパースで読み込んでください
- 数値抽出時は、セル出力の`text`部分をパースしてください（プロット画像は無視）
- 複数の改善案を提示する際は、優先度を明確にしてください
- YAML例は実際の設定ファイル構造に合わせて正確に記述してください
- 提案は具体的で実行可能なものにしてください（抽象的な提案は避ける）
