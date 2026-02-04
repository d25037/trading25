# Pandas FillNA FutureWarning対応ガイド

## 概要

pandas 2.2.0以降で以下の警告が発生する問題への対応方法をまとめる。

```
FutureWarning: Downcasting object dtype arrays on .fillna, .ffill, .bfill is deprecated and will change in a future version. Call result.infer_objects(copy=False) instead. To opt-in to the future behavior, set `pd.set_option('future.no_silent_downcasting', True)`
```

## 警告の原因

### 1. 自動ダウンキャスト機能の廃止
- pandas 2.2.0でobject型配列の**自動ダウンキャスト**が非推奨化
- `fillna()`, `ffill()`, `bfill()`で発生する**予測困難な型変換**を排除
- 将来のpandasバージョンでは自動ダウンキャストが完全に削除予定

### 2. 影響を受けるメソッド
- `DataFrame.fillna()`
- `Series.fillna()`
- `DataFrame.ffill()` / `Series.ffill()`
- `DataFrame.bfill()` / `Series.bfill()`
- `DataFrame.replace()` / `Series.replace()`

## 解決方法

### 方法1: 明示的な型変換（推奨）

```python
# 修正前（警告発生）
df = pd.DataFrame({'col': [1, None, 3]})
result = df.fillna(0)  # FutureWarning

# 修正後（推奨）
df = pd.DataFrame({'col': [1, None, 3]})
result = df.fillna(0).infer_objects(copy=False)
```

### 方法2: 事前の型指定

```python
# boolean データの場合
entries = pd.Series([True, None, False], dtype='object')
entries_filled = entries.astype('boolean').fillna(False)

# 数値データの場合
prices = pd.Series([100.0, None, 200.0])
prices_filled = prices.astype('float64').fillna(0.0)
```

### 方法3: パンダスオプション設定（一時的）

```python
import pandas as pd

# グローバル設定（非推奨：隠れた問題の原因となる可能性）
pd.set_option('future.no_silent_downcasting', True)

# コンテキスト内での一時的な設定（推奨）
with pd.option_context('future.no_silent_downcasting', True):
    result = df.fillna(0)
```

## 廃止済み引数への対応

### method引数の廃止（pandas 1.4.0〜）

```python
# 修正前（既に廃止済み）
df.fillna(method='ffill')  # FutureWarning
df.fillna(method='bfill')  # FutureWarning

# 修正後
df.ffill()  # 前方補間
df.bfill()  # 後方補間
```

## プロジェクト内での対応方針

### 1. 高優先度（必須修正）
- **廃止済みmethod引数**：即座に新しい構文に変更
- セキュリティ上の理由で放置不可

### 2. 中優先度（警告対応）
- **自動ダウンキャスト警告**：`.infer_objects(copy=False)`追加
- 既存機能に影響なし、将来対応のため

### 3. 良好な実装例（修正不要）
```python
# 既に適切な実装
signal.fillna(False)  # boolean型への明示的変換
df.astype('float64').fillna(0.0)  # 事前型指定
```

## 検証方法

```python
# 警告の確認
import warnings
warnings.filterwarnings('error', category=FutureWarning)

# テストケース
try:
    result = df.fillna(0)
    print("警告なし")
except FutureWarning as e:
    print(f"警告発生: {e}")
```

## 参考資料

- [pandas 2.2.0 Release Notes](https://pandas.pydata.org/docs/whatsnew/v2.2.0.html)
- [pandas.DataFrame.fillna Documentation](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.fillna.html)
- [GitHub Issue #40988: Deprecate downcast keyword for fillna](https://github.com/pandas-dev/pandas/issues/40988)

## 更新履歴

- 2025-01-09: 初版作成、プロジェクト内対応方針決定