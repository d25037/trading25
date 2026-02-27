# Trading25-BT 戦略システムドキュメント

## 📊 概要

**統一Signalsシステム**ベースの高速戦略バックテストプラットフォーム。VectorBTによる100倍以上の高速化と、柔軟なYAML設定システム、MarimoベースのHTML実行環境を組み合わせた次世代投資戦略ツールです。

## 🚀 主要特徴

### **統一Signalsアーキテクチャ**
- **SignalProcessor**: エントリーAND結合・エグジットOR結合による統合処理
- **10種類シグナル**: 出来高・財務・β値・プライスアクション・信用残高・ボラティリティ・相対パフォーマンス・セクター等
- **Entry/Exit両用設計**: 同一シグナルでエントリー絞り込み・エグジット発火両対応

### **高速実行システム**
- **VectorBT基盤**: 100倍以上の高速化実現
- **Marimo実行**: Pythonテンプレートを静的HTMLとして自動生成
- **マルチアセット対応**: 大規模ポートフォリオ一括処理

### **柔軟な設定システム**
- **YAML設定**: 戦略パラメータ・シグナル条件の柔軟な調整
- **型安全性**: Pydantic統合による実行時バリデーション
- **テンプレートベース**: 一貫した分析フローの自動生成

## 📋 利用可能戦略一覧

現在14戦略が実装・動作確認済みです：

| 戦略名 | 説明 | 主要特徴 |
|--------|------|----------|
| **buy_and_hold** | Buy&Hold戦略 | ベンチマーク・長期投資戦略 |
| **sma_cross** | SMAクロス戦略 | ゴールデン/デッドクロス戦略 |
| **sma_break** | SMAブレイク戦略 | SMAブレイクアウト戦略 |
| **rsi_cross** | RSIクロス戦略 | RSI過買い/過売り戦略 |
| **range_break** | レンジブレイク戦略 | ボラティリティブレイクアウト |
| **range_break_v4** | レンジブレイクv4 | 最高値未更新エグジット版 |
| **range_break_v5** | レンジブレイクv5 | 長期・短期高値一致+ATR |
| **bnf_mean_reversion** | BNF平均回帰戦略 | SMA乖離平均回帰 |
| **bnf_mean_reversion_v2** | BNF平均回帰v2 | 改良版平均回帰 |
| **bnf_mean_reversion_v3** | BNF平均回帰v3 | ボリンジャーバンドベース |
| **macd_cross** | MACDクロス戦略 | MACDクロスオーバー |
| **macd_cross_v2** | MACDクロスv2 | ボリンジャーバンド統合版 |
| **macd_deadcross** | MACDデッドクロス戦略 | MACDデッドクロス戦略 |
| **atr_support_break** | ATRサポートブレイク戦略 | ATR基準サポートブレイク |

## 🛠️ 基本使用方法

### **戦略一覧表示**
```bash
uv run bt list
```

### **基本戦略実行**
```bash
# シンプル実行（全銘柄・デフォルト設定）
uv run bt backtest buy_and_hold
uv run bt backtest sma_cross
uv run bt backtest range_break

# 短縮形（run コマンド）
uv run bt run buy_and_hold

# 設定ファイル指定実行
uv run bt backtest sma_cross --config config/strategies/sma_cross.yaml

# 出力先指定実行
uv run bt backtest range_break --output-dir custom_results
```

### **設定ファイル検証**
```bash
uv run bt validate sma_cross
```

### **古いHTML削除**
```bash
uv run bt cleanup --days 7
```

## ⚙️ 設定システム

### **YAML設定ファイル構造**
```yaml
# config/strategies/example.yaml
shared_config:
  initial_cash: 10000000      # 初期資金
  fees: 0.001                 # 手数料
  stock_codes: ["all"]        # 対象銘柄
  direction: "longonly"       # 取引方向

strategy_params:
  name: "example_strategy"    # 戦略名

entry_signal_params:          # エントリーシグナル（AND結合）
  volume:
    surge_enabled: true       # 出来高急増フィルター
    surge_threshold: 1.5      # 急増閾値
  fundamental:
    per:
      enabled: true           # PERフィルター
      threshold: 15.0         # PER閾値

exit_signal_params:           # エグジットシグナル（OR結合）
  volume:
    drop_enabled: true        # 出来高減少トリガー
    drop_threshold: 0.7       # 減少閾値
```

### **利用可能シグナル種類**

#### **エントリーシグナル（絞り込み）**
- **volume**: 出来高急増による絞り込み
- **fundamental**: 財務指標（PER・ROE・EPS成長率・PEG Ratio）
- **beta**: 市場感応度β値（Numba最適化で10-50倍高速）
- **price_action**: プライスアクション（サポート維持・ブレイク）
- **margin**: 信用残高（需給判定）
- **volatility**: ボラティリティ（ベンチマーク比較）
- **trend**: トレンド（EMA傾きベース）

#### **エグジットシグナル（発火）**
- **volume**: 出来高減少による損切り・急増による利確
- **fundamental**: 財務悪化による警告
- **beta**: 市場相関変化による警告
- **price_action**: サポート割れ・抵抗接近警告

## 🏗️ アーキテクチャ詳細

### **統一Signalsシステム**
```
src/strategies/signals/
├── processor.py              # SignalProcessor統合処理クラス
├── volume.py                 # 出来高シグナル（両用）
├── fundamental.py            # 財務指標シグナル
├── beta.py                   # β値シグナル（Numba最適化）
├── horizontal_price_action.py # プライスアクションシグナル
├── margin.py                 # 信用残高シグナル
├── volatility.py             # ボラティリティシグナル
├── performance.py            # 相対パフォーマンスシグナル
└── sector.py                 # セクターシグナル
```

### **実行フロー**
1. **設定読み込み**: YAML→Pydanticモデル変換・バリデーション
2. **データロード**: マルチアセット株価・財務・信用データロード
3. **戦略実行**: 各戦略固有のエントリー・エグジットロジック実行
4. **シグナル統合**: SignalProcessorによるエントリー（AND）・エグジット（OR）結合
5. **VectorBTバックテスト**: 高速ベクトル化バックテスト実行
6. **結果出力**: HTML形式での詳細分析結果生成

### **パフォーマンス最適化**
- **VectorBT基盤**: 全データ一括処理による100倍以上高速化
- **Numba最適化**: β値計算等で10-50倍個別最適化
- **メモリ効率**: 大規模データセット対応最適化
- **並行処理**: マルチアセット並行シグナル処理

## 📊 戦略別詳細

### **SMAクロス戦略**
**基本概念**: 短期・長期移動平均のクロスオーバーによる売買判定
- **エントリー**: ゴールデンクロス（短期SMA > 長期SMA）
- **エグジット**: デッドクロス（短期SMA < 長期SMA）
- **シグナル拡張**: 出来高・財務フィルターで精度向上

### **レンジブレイク戦略**
**基本概念**: ボラティリティブレイクアウトによる順張り戦略
- **エントリー**: 過去N日間の高値/安値ブレイク
- **エグジット**: 逆方向ブレイクまたはATR基準ストップ
- **バリエーション**: v2-v5で段階的改良・最適化

### **平均回帰戦略**
**基本概念**: 価格の統計的平均回帰特性を活用
- **エントリー**: SMA/ボリンジャーバンドからの過度乖離
- **エグジット**: 平均回帰完了または損切り条件
- **BNF版**: 特定の乖離率・期間設定による最適化

## 🧪 テスト・検証

### **品質保証**
- **ユニットテスト**: 各シグナル・戦略の個別テスト
- **統合テスト**: エンドツーエンド戦略実行テスト
- **型安全性**: Pydantic・pyright・mypyによる型チェック

### **パフォーマンステスト**
- **実行時間**: 戦略実行時間の継続監視
- **メモリ使用量**: 大規模データセット処理の効率性
- **正確性**: 従来実装との数値一致検証

## 📚 参考ドキュメント

- **[CLAUDE.md](../CLAUDE.md)**: プロジェクト統合ドキュメント
- **[migration-roadmap.md](refactoring/migration-roadmap.md)**: アーキテクチャ移行完了レポート
- **[VectorBTドキュメント](vectorbt/)**: VectorBT使用方法・最適化技法
- **設定ファイル例**: `config/strategies/` 内の各戦略YAML

## 🔧 開発・拡張

### **新戦略追加**
1. `src/strategies/implementations/` に戦略クラス実装
2. `config/strategies/` に設定YAMLファイル作成
3. `src/domains/strategy/core/factory.py` に戦略登録
4. テストケース作成・動作確認

### **新シグナル追加**
1. `src/strategies/signals/` に新シグナルファイル実装
2. `src/models/signals.py` にパラメータモデル追加
3. `SignalProcessor` に統合処理ロジック追加
4. 設定YAML・テストケース対応

---

## 🎯 まとめ

trading25-btは**統一Signalsシステム**により、高速・柔軟・拡張可能な戦略バックテストプラットフォームを実現しています。VectorBTの高速化、YAML設定の柔軟性、MarimoによるHTML分析出力を組み合わせ、プロフェッショナルな投資戦略開発をサポートします。
