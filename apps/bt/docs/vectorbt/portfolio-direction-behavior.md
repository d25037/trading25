# VectorBT Portfolio.from_signals Direction パラメータ挙動ガイド

## 概要

VectorBTの`Portfolio.from_signals()`における`direction`パラメータは、エントリー・エグジットシグナルの解釈方法を制御する重要なパラメータです。このドキュメントでは、`direction`の各設定値と`entries`、`exits`、`short_entries`、`short_exits`パラメータの組み合わせによる具体的な挙動を詳細に解説します。

## direction パラメータの基本概念

### 設定値
- **`'longonly'` (0)**: ロングポジションのみ許可
- **`'shortonly'` (1)**: ショートポジションのみ許可
- **`'both'` (2)**: ロング・ショート両方向のポジション許可（デフォルト）

### 2つのシグナル指定方法

VectorBTでは、シグナルを指定する方法として以下の2つのアプローチがあります：

1. **シンプル方式**: `entries`と`exits`のみ使用、`direction`で方向制御
2. **明示的方式**: `entries`、`exits`、`short_entries`、`short_exits`で各方向を個別指定

## 詳細な挙動解析

### ケース1: entries/exitsのみ使用 + direction指定

#### direction='longonly'
```python
portfolio = vbt.Portfolio.from_signals(
    close=price_data,
    entries=buy_signals,      # ロングエントリーとして解釈
    exits=sell_signals,       # ロングエグジットとして解釈
    direction='longonly'
)
```

**挙動**:
- `entries=True` → ロングポジション開始（買い）
- `exits=True` → ロングポジション終了（売り）
- ショートポジションは一切取らない
- `short_entries`、`short_exits`は無視される

#### direction='shortonly'
```python
portfolio = vbt.Portfolio.from_signals(
    close=price_data,
    entries=entry_signals,    # ショートエントリーとして解釈
    exits=exit_signals,       # ショートエグジットとして解釈
    direction='shortonly'
)
```

**挙動**:
- `entries=True` → ショートポジション開始（売り）
- `exits=True` → ショートポジション終了（買戻し）
- ロングポジションは一切取らない
- `short_entries`、`short_exits`は無視される

#### direction='both'
```python
portfolio = vbt.Portfolio.from_signals(
    close=price_data,
    entries=signals,          # ロングエントリーとして解釈
    exits=exit_signals,       # 現在ポジションのエグジットとして解釈
    direction='both'
)
```

**挙動**:
- `entries=True` → ロングポジション開始（買い）
- `exits=True` → 現在のポジション終了
- ショートエントリーは発生しない（`short_entries`未設定のため）

### ケース2: 明示的シグナル指定（推奨）

#### 完全な方向指定
```python
portfolio = vbt.Portfolio.from_signals(
    close=price_data,
    entries=long_entries,         # ロングエントリー
    exits=long_exits,            # ロングエグジット
    short_entries=short_entries,  # ショートエントリー
    short_exits=short_exits,     # ショートエグジット
    direction='both'             # 通常は'both'を使用
)
```

**挙動**:
- `entries=True` → ロングポジション開始
- `exits=True` → ロングポジション終了
- `short_entries=True` → ショートポジション開始
- `short_exits=True` → ショートポジション終了
- 各シグナルは独立して動作

#### direction制限との組み合わせ
```python
# ロングオンリー戦略でshort_entriesを設定
portfolio = vbt.Portfolio.from_signals(
    close=price_data,
    entries=long_entries,
    exits=long_exits,
    short_entries=short_entries,  # 動作は要検証
    short_exits=short_exits,     # 動作は要検証
    direction='longonly'
)
```

**挙動** (要検証):
- `direction='longonly'`時の`short_entries`/`short_exits`の扱いは公式ドキュメントで明確でない
- 実装テストによる検証が必要

## シグナル解釈表

| direction | entries | exits | short_entries | short_exits | 実際の挙動 |
|-----------|---------|-------|---------------|-------------|------------|
| `'longonly'` | ✓ | ✓ | ❓ (要検証) | ❓ (要検証) | ロング売買のみ |
| `'shortonly'` | ショート | ショート | ❓ (要検証) | ❓ (要検証) | ショート売買のみ |
| `'both'` (entries/exits のみ) | ロング | ロング | - | - | ロング売買のみ |
| `'both'` (全指定) | ロング | ロング | ショート | ショート | 両方向売買 |

## 実装例

### 例1: 単純なSMAクロス戦略（ロングオンリー）
```python
import vectorbt as vbt
import pandas as pd

def sma_crossover_long_only(close, fast=10, slow=30):
    """SMAクロス戦略（ロングオンリー）"""
    fast_sma = vbt.MA.run(close, fast)
    slow_sma = vbt.MA.run(close, slow)

    entries = fast_sma.ma_crossed_above(slow_sma.ma)
    exits = fast_sma.ma_crossed_below(slow_sma.ma)

    return vbt.Portfolio.from_signals(
        close=close,
        entries=entries,
        exits=exits,
        direction='longonly',  # ロングオンリー指定
        init_cash=10000,
        fees=0.001
    )
```

### 例2: 同一戦略でのロング・ショート切り替え
```python
def sma_crossover_directional(close, fast=10, slow=30, direction='both'):
    """SMAクロス戦略（方向指定可能）"""
    fast_sma = vbt.MA.run(close, fast)
    slow_sma = vbt.MA.run(close, slow)

    # 基本シグナル
    uptrend = fast_sma.ma_crossed_above(slow_sma.ma)
    downtrend = fast_sma.ma_crossed_below(slow_sma.ma)

    if direction == 'longonly':
        entries = uptrend
        exits = downtrend
        return vbt.Portfolio.from_signals(
            close, entries, exits, direction='longonly',
            init_cash=10000, fees=0.001
        )

    elif direction == 'shortonly':
        entries = downtrend  # ショートエントリーとして解釈
        exits = uptrend      # ショートエグジットとして解釈
        return vbt.Portfolio.from_signals(
            close, entries, exits, direction='shortonly',
            init_cash=10000, fees=0.001
        )

    else:  # direction == 'both'
        # 明示的にlong/short分離
        return vbt.Portfolio.from_signals(
            close=close,
            entries=uptrend,          # ロングエントリー
            exits=downtrend,          # ロングエグジット
            short_entries=downtrend,  # ショートエントリー
            short_exits=uptrend,      # ショートエグジット
            direction='both',
            init_cash=10000,
            fees=0.001
        )
```

### 例3: 複雑な両方向戦略
```python
def advanced_long_short_strategy(close):
    """高度なロング・ショート戦略"""
    # 異なるインジケーターでロング・ショート判定
    rsi = vbt.RSI.run(close, 14)
    bb = vbt.BBANDS.run(close, 20, 2)

    # ロングシグナル: RSI過売り + BBバンド下限タッチ
    long_entries = (rsi.rsi < 30) & (close < bb.lowerband)
    long_exits = rsi.rsi > 70

    # ショートシグナル: RSI過買い + BBバンド上限タッチ
    short_entries = (rsi.rsi > 70) & (close > bb.upperband)
    short_exits = rsi.rsi < 30

    return vbt.Portfolio.from_signals(
        close=close,
        entries=long_entries,        # ロング専用
        exits=long_exits,           # ロング専用
        short_entries=short_entries, # ショート専用
        short_exits=short_exits,    # ショート専用
        direction='both',           # 両方向許可
        init_cash=10000,
        fees=0.001
    )
```

## ベストプラクティス

### 1. 戦略の目的に応じた方法選択

**シンプルな単方向戦略**:
```python
# direction パラメータ使用
portfolio = vbt.Portfolio.from_signals(
    close, entries, exits,
    direction='longonly'  # または 'shortonly'
)
```

**複雑な両方向戦略**:
```python
# 明示的シグナル分離使用
portfolio = vbt.Portfolio.from_signals(
    close, long_entries, long_exits,
    short_entries=short_entries,
    short_exits=short_exits
)
```

### 2. 方向別パフォーマンス比較

```python
def compare_directions(close, strategy_func):
    """同一戦略での方向別パフォーマンス比較"""
    results = {}

    for direction in ['longonly', 'shortonly', 'both']:
        pf = strategy_func(close, direction=direction)
        results[direction] = {
            'total_return': pf.total_return(),
            'sharpe_ratio': pf.sharpe_ratio(),
            'max_drawdown': pf.max_drawdown(),
            'win_rate': pf.trades.win_rate()
        }

    return pd.DataFrame(results).T

# 使用例
comparison = compare_directions(close_data, sma_crossover_directional)
print(comparison)
```

## 注意事項・制限事項

### 1. ポジションサイジングとdirection
- `direction='shortonly'`と`size_type='percent'`の組み合わせで問題が発生する場合がある
- ショートオンリー戦略では固定サイズ（`size_type='shares'`）推奨

### 2. シグナル競合時の動作
- 同時に`entries=True`と`short_entries=True`の場合の動作は`DirectionConflictMode`で制御
- デフォルトではロングシグナルが優先される場合が多い

### 3. ブロードキャスト対応
```python
# 銘柄別に異なるdirection設定
direction = ['longonly', 'shortonly', 'both']  # 3銘柄用
portfolio = vbt.Portfolio.from_signals(
    multi_asset_data, entries, exits,
    direction=direction  # 銘柄ごとに適用
)
```

### 4. 現在のコードベースへの統合

現在の`backtest_executor_mixin.py`では：
```python
# 現在の実装
portfolio = vbt.Portfolio.from_signals(
    close=close_data,
    entries=all_entries,
    exits=all_exits,
    # direction パラメータ未設定 = デフォルト'both'
)

# 改善案: 設定ファイルからdirection制御
portfolio = vbt.Portfolio.from_signals(
    close=close_data,
    entries=all_entries,
    exits=all_exits,
    direction=self.config.get('direction', 'both'),  # 設定から取得
    # ... その他パラメータ
)
```

## まとめ

- **単純戦略**: `direction`パラメータでシンプルに制御
- **複雑戦略**: 明示的な`short_entries`/`short_exits`で柔軟性確保
- **パフォーマンス比較**: 同一戦略での方向別バックテストで最適化
- **統合方針**: YAML設定ファイルからdirection制御で拡張性確保

この理解により、VectorBTの`direction`パラメータを適切に活用し、効果的なロング・ショート戦略を実装できます。