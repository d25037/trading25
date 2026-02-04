# VectorBT Positions分析ガイド

## 同時保有ポジション数の取得

### VectorBT positions.records_readableの正確なカラム構造

```python
# ❌ 間違ったカラム名（エラーの原因）
'Start Timestamp', 'End Timestamp'

# ✅ 正しいカラム名  
'Entry Timestamp', 'Exit Timestamp'

# 完全なカラム一覧
['Position Id', 'Column', 'Size', 'Entry Timestamp', 'Avg Entry Price', 'Entry Fees', 
 'Exit Timestamp', 'Avg Exit Price', 'Exit Fees', 'PnL', 'Return', 'Direction', 'Status']
```

### 正しい同時ポジション数計算実装

```python
def get_max_concurrent_positions(self) -> int:
    """同時保有ポジションの最大数を取得"""
    positions = portfolio.positions
    position_matrix = positions.records_readable
    
    # ✅ 正しいカラム名を使用
    entry_times = pd.to_datetime(position_matrix['Entry Timestamp'])
    exit_times = pd.to_datetime(position_matrix['Exit Timestamp'])
    
    # 効率的な期間重複計算
    position_periods = list(zip(entry_times, exit_times))
    return self._calculate_max_overlapping_periods(position_periods)

def _calculate_max_overlapping_periods(self, periods: list) -> int:
    """スイープライン法による効率的な重複計算"""
    if not periods:
        return 0
    
    # イベントリスト作成: (時刻, +1=開始/-1=終了)
    events = []
    for start, end in periods:
        events.append((start, 1))    # ポジション開始
        events.append((end, -1))     # ポジション終了
    
    # 時刻順ソート → スイープライン法で最大重複数計算
    events.sort()
    current_count = 0
    max_count = 0
    
    for _, delta in events:
        current_count += delta
        max_count = max(max_count, current_count)
    
    return max_count
```

## Positionsオブジェクトの属性

### Records基本属性
- `positions.records`: 生のNumPy構造化配列
- `positions.records_readable`: 人間可読形式のDataFrame
- `positions.count()`: ポジション総数
- `positions.apply_mask()`: 条件フィルタリング

### Position専用属性  
- `positions.size`: ポジションサイズ
- `positions.pnl`: 損益
- `positions.duration`: 保有期間
- `positions.entry_price`: エントリー価格
- `positions.exit_price`: エグジット価格

## デバッグ時の重要ポイント

1. **コンソール出力**: Jupyter環境では`debug`ログが表示されないため`print()`を使用
2. **単一銘柄テスト**: API構造調査は単一銘柄でも可能
3. **カラム名確認**: `list(position_matrix.columns)`で必ず実際の構造を確認
4. **型確認**: `type(positions)`でオブジェクトクラスを確認

## 実証結果例

- **単一銘柄（17190）**: 最大同時ポジション数 = 1（3ポジション非重複）
- **398銘柄ポートフォリオ**: 最大同時ポジション数 = **77銘柄**
- **期間重複**: スイープライン法により正確な重複期間計算

## トラブルシューティング

### よくあるエラー
```python
# エラー: 'Start Timestamp' キーが存在しない
KeyError: 'Start Timestamp'

# 解決: 正しいカラム名を使用
entry_times = pd.to_datetime(position_matrix['Entry Timestamp'])
```

### 属性確認方法
```python
# positionsオブジェクトの利用可能属性を調査
print([attr for attr in dir(positions) if not attr.startswith('_')])

# records_readableのカラム構造確認
print(list(positions.records_readable.columns))
```