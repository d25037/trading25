# VectorBT ピラミッディング実装ガイド

## 概要
このドキュメントでは、VectorBTを使用したピラミッディング戦略（既存ポジションに追加投資）の実装方法について説明します。

## ピラミッディングとは
ピラミッディングは、既に保有しているポジションが利益を出している時に、同じ銘柄に対して追加投資を行うトレード技法です。

### メリット
- 利益の最大化：利益が出ているポジションに追加投資することで、さらなる利益を狙える
- リスク分散：段階的にポジションを追加するため、一度に大きなリスクを取らない

### デメリット
- リスクの増大：ポジションサイズが大きくなるため、反転時の損失も大きくなる
- 複雑な管理：複数のエントリーポイントを管理する必要がある

## VectorBTでのピラミッディング実装方法

### 基本的な実装アプローチ

VectorBTでピラミッディングを実装する場合、以下の3つのアプローチがあります：

1. **from_signals() + accumulate=True** (制限あり)
2. **from_orders()** (中級者向け)
3. **from_order_func()** (最も柔軟、推奨)

### 1. from_signals() + accumulate=True
```python
# 制限：同じシグナルでのみ追加投資可能
portfolio = vbt.Portfolio.from_signals(
    data=close_prices,
    entries=buy_signals,
    exits=sell_signals,
    accumulate=True,  # 複数エントリーを許可
    init_cash=100000
)
```

**制限事項**：
- 新規エントリーと追加投資で同じシグナルを使用
- 異なる条件でのピラミッディングは不可

### 2. from_orders()
```python
# 事前にオーダーサイズを計算してシリーズを作成
order_sizes = calculate_pyramid_sizes(data, signals)

portfolio = vbt.Portfolio.from_orders(
    data=close_prices,
    size=order_sizes,
    fees=0.001
)
```

### 3. from_order_func() (推奨)
```python
def pyramid_order_func(c):
    """ピラミッディング対応のオーダー関数"""
    current_position = c.last_position[c.col]
    current_price = close_prices.iloc[c.i]
    
    # 新規エントリー
    if current_position == 0 and new_entry_conditions[c.i]:
        return base_size  # 初回投資
    
    # ピラミッディング
    if current_position > 0 and pyramid_conditions[c.i]:
        return additional_size  # 追加投資
    
    # 決済
    if current_position > 0 and exit_conditions[c.i]:
        return -current_position  # 全決済
    
    return 0  # 何もしない

portfolio = vbt.Portfolio.from_order_func(
    close=close_prices,
    order_func=pyramid_order_func,
    size=base_size,
    init_cash=100000
)
```

## 実践例：Range Break V3戦略

当プロジェクトのRange Break V3戦略では、以下の仕様でピラミッディングを実装しています：

### ピラミッディング仕様
- **初期ポジション**: 1U (基準単位)
- **ATR20をN**として以下の追加投資:
  - エントリー価格から **+0.5N** → 1U追加 (合計2U)
  - エントリー価格から **+1.0N** → 1U追加 (合計3U)
  - エントリー価格から **+1.5N** → 1U追加 (合計4U、最大)

### 実装コード例
```python
def pyramid_order_func(c):
    current_idx = c.i
    current_col = c.col
    current_position = c.last_position[current_col]
    
    # 売りシグナルチェック
    if current_position > 0 and exit_signals.iloc[current_idx]:
        return -current_position  # 全決済
    
    # 新規エントリー
    if current_position == 0 and entry_signals.iloc[current_idx]:
        return 1.0  # 初回エントリー（1U）
    
    # ピラミッディング
    if current_position > 0 and current_position < 4:
        entry_price = get_entry_price(current_idx)  # エントリー価格取得
        current_price = close.iloc[current_idx]
        current_atr = atr_values.iloc[current_idx]
        
        # 現在のポジション数に応じた閾値判定
        pyramid_level = int(current_position) - 1
        if pyramid_level < len(pyramid_atr_multipliers):
            threshold = entry_price + (pyramid_atr_multipliers[pyramid_level] * current_atr)
            
            if current_price >= threshold:
                return 1.0  # 1U追加
    
    return 0.0
```

## ピラミッディング設定のベストプラクティス

### 1. 段階的なサイズ設定
```python
# 等量ピラミッド（推奨）
pyramid_sizes = [1.0, 1.0, 1.0, 1.0]  # 各段階で同量

# 逓減ピラミッド（リスク管理重視）
pyramid_sizes = [2.0, 1.5, 1.0, 0.5]  # 段階的に減量
```

### 2. 適切な閾値設定
```python
# ATRベース（ボラティリティ対応）
pyramid_thresholds = [0.5, 1.0, 1.5]  # ATRの倍数

# 固定パーセンテージ
pyramid_thresholds = [0.02, 0.04, 0.06]  # 2%, 4%, 6%
```

### 3. 最大ポジション制限
```python
MAX_POSITION_UNITS = 4  # 最大4Uまで

if current_position >= MAX_POSITION_UNITS:
    return 0  # これ以上の追加投資は行わない
```

## リスク管理

### 1. ストップロス設定
```python
# 全体ポジションに対するストップロス
if current_price <= stop_loss_price:
    return -current_position  # 全決済
```

### 2. 部分決済
```python
# 段階的な利益確定
if profit_percentage > 0.1:  # 10%利益
    return -1.0  # 1U分のみ決済
```

### 3. 最大損失制限
```python
# ポジション全体の最大損失
max_loss_per_position = initial_cash * 0.02  # 初期資金の2%
current_loss = calculate_unrealized_loss(current_position, entry_price, current_price)

if current_loss >= max_loss_per_position:
    return -current_position  # 損切り
```

## パフォーマンス最適化

### 1. 状態管理の効率化
```python
# エントリー価格の効率的な管理
entry_prices = {}  # コラムごとのエントリー価格を保持

def get_entry_price(col, current_idx):
    if col not in entry_prices:
        # エントリー価格を逆算
        for i in range(current_idx, -1, -1):
            if entry_signals.iloc[i, col]:
                entry_prices[col] = close.iloc[i, col]
                break
    return entry_prices.get(col, 0)
```

### 2. ベクトル化処理
```python
# 可能な限りベクトル化された処理を使用
pyramid_conditions = (
    (current_prices > entry_prices + 0.5 * atr_values) &
    (positions == 1)
)
```

## よくある実装上の注意点

### 1. インデックス範囲チェック
```python
if current_idx >= len(close) or current_idx >= len(atr_values):
    return 0.0  # 範囲外の場合は何もしない
```

### 2. ポジション状態の正確な管理
```python
# ポジション状態を正確に把握
current_position = c.last_position[current_col]
if current_position < 0:
    current_position = 0  # 負のポジションは0として扱う
```

### 3. 価格データの一貫性
```python
# 価格データがNaNの場合の処理
if pd.isna(current_price) or pd.isna(current_atr):
    return 0.0  # データが不完全な場合は何もしない
```

## まとめ

VectorBTでのピラミッディング実装は、`from_order_func()`を使用することで最も柔軟に行えます。重要なポイント：

1. **適切なリスク管理**：最大ポジション制限・ストップロス設定
2. **効率的な状態管理**：エントリー価格・ポジション数の正確な追跡
3. **段階的な投資**：一度に大きなポジションを取らない
4. **バックテスト検証**：実装後は十分なバックテストで検証

この実装方法により、リスクを管理しながら利益を最大化するピラミッディング戦略を構築できます。