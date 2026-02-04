# パラメータ最適化システム

## 概要

パラメータ最適化システムは、戦略のパラメータをグリッドサーチで最適化し、**Kelly基準による2段階資金配分最適化**を自動実行します。

## 基本使用方法

### 1. グリッドYAMLファイルを作成

`config/optimization/{strategy_name}_grid.yaml`:

```yaml
parameter_ranges:
  entry_filter_params:
    period_breakout:
      period: [50, 100, 200]  # 最適化対象パラメータ
    volume:
      threshold: [1.5, 2.0, 2.5]
```

### 2. 最適化実行

```bash
# 基本実行
uv run python -m src.cli.optimize run range_break_v6

# 詳細ログ出力
uv run python -m src.cli.optimize run range_break_v6 --verbose
```

## Kelly基準2段階最適化

各パラメータ組み合わせに対して：
1. 全銘柄等分配でバックテスト実行
2. Kelly基準計算
3. Kelly配分率でバックテスト実行
4. Min-Max正規化後の複合スコア算出

詳細: `docs/kelly-criterion-allocation.md`
