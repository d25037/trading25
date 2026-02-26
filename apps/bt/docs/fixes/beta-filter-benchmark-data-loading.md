# β値フィルター修正: ベンチマークデータロード問題

## 問題の概要

β値フィルターが有効でも、ベンチマークデータがロードされず、フィルターが全く効かない問題が発生していました。

### 症状

- β値フィルター設定: `min_beta: 2.0, max_beta: 3.0`（市場の2-3倍のボラティリティ）
- 期待動作: トレード数が大幅に減少
- 実際の動作: トレード数が全く減らない（2,444件）

## 根本原因

### 問題の構造

1. **β値シグナルの要件**:
   - ベンチマークデータ（市場指数）が必須
   - `src/strategies/signals/registry.py:139-141` でベンチマークデータの存在をチェック

2. **ベンチマークデータのロード欠陥**:
   - `BacktestExecutorMixin.run_multi_backtest()` では、`relative_mode=True` の場合のみ `load_benchmark_data()` が呼び出される
   - 通常モード（`relative_mode=False`）では `self.benchmark_data` が `None` のまま

3. **シグナルスキップのメカニズム**:
   ```python
   # registry.py:139-141
   data_checker=lambda d: "benchmark_data" in d
   and d["benchmark_data"] is not None
   and not d["benchmark_data"].empty,
   ```
   - `data_checker` がベンチマークデータ不在を検出
   - `SignalProcessor._apply_unified_signal()` で早期リターン
   - **β値シグナルが完全にスキップされる**

### コールスタック

```
BacktestExecutorMixin.run_multi_backtest()
  ↓ (relative_mode=Falseの場合、benchmark_data未ロード)
  ↓
YamlConfigurableStrategy.generate_signals()
  ↓ (benchmark_data=None)
  ↓
SignalProcessor.generate_signals()
  ↓
SignalProcessor.apply_signals()
  ↓
SignalProcessor._apply_unified_signal()
  ↓ data_checker(data_sources) → False
  ↓ return (β値シグナルスキップ)
```

## 修正内容

### 1. `BacktestExecutorMixin._should_load_benchmark()` を追加

β値シグナルが有効かチェックするヘルパーメソッド：

```python
def _should_load_benchmark(self: "StrategyProtocol") -> bool:
    """
    ベンチマークデータのロードが必要かチェック

    β値シグナルが有効な場合はベンチマークデータが必要

    Returns:
        bool: ベンチマークデータロードが必要な場合True
    """
    # エントリーフィルターでβ値シグナルが有効かチェック
    if (
        hasattr(self, "entry_filter_params")
        and self.entry_filter_params is not None
        and hasattr(self.entry_filter_params, "beta")
        and self.entry_filter_params.beta.enabled
    ):
        return True

    # エグジットトリガーでβ値シグナルが有効かチェック
    if (
        hasattr(self, "exit_trigger_params")
        and self.exit_trigger_params is not None
        and hasattr(self.exit_trigger_params, "beta")
        and self.exit_trigger_params.beta.enabled
    ):
        return True

    return False
```

### 2. `BacktestExecutorMixin.run_multi_backtest()` を修正

β値シグナル有効時にベンチマークデータを自動ロード：

```python
# β値シグナルが有効な場合はベンチマークデータをロード
if self._should_load_benchmark():
    self._log("β値シグナル有効 - ベンチマークデータロード開始", "info")
    try:
        self.load_benchmark_data()
        if self.benchmark_data is not None and not self.benchmark_data.empty:
            self._log(
                f"✅ ベンチマークデータロード完了: {len(self.benchmark_data)}レコード",
                "info",
            )
        else:
            self._log(
                "⚠️  ベンチマークデータが空またはNone - β値シグナルがスキップされます",
                "warning",
            )
    except Exception as e:
        self._log(
            f"⚠️  ベンチマークデータロード失敗: {e} - β値シグナルがスキップされます",
            "warning",
        )
```

### 3. `SignalProcessor._apply_unified_signal()` にデバッグログ追加

データ不足でスキップされる場合の詳細ログ：

```python
# 2. 必須データチェック
if signal_def.data_checker and not signal_def.data_checker(data_sources):
    # データ不足でスキップ - デバッグログ出力
    logger.warning(
        f"⚠️  {signal_def.name}シグナル: 必須データ不足によりスキップ "
        f"(ベンチマークデータ: {'有' if data_sources.get('benchmark_data') is not None else '無'})"
    )
    return
```

## 検証結果

### 修正前

- HTML: `~/.local/share/trading25/backtest/results/bnf_mean_reversion_v3/all_20251010_061423.html`
- β値フィルター設定: `min_beta: 2.0, max_beta: 3.0`
- トレード数: **2,444件**（フィルターが効いていない）

### 修正後

- HTML: `~/.local/share/trading25/backtest/results/bnf_mean_reversion_v3/all_20251010_062017.html`
- β値フィルター設定: `min_beta: 2.0, max_beta: 3.0`
- トレード数: **1件**（フィルターが正しく機能！）

β値フィルターが正常に動作し、市場の2-3倍のボラティリティを持つ銘柄のみに絞り込まれました。

## テストケース追加

将来の回帰を防ぐため、以下のテストケースを追加：

- `tests/unit/strategies/mixins/test_backtest_executor_mixin.py`
  - エントリーフィルターでβ値シグナル有効時のベンチマークロードチェック
  - エグジットトリガーでβ値シグナル有効時のベンチマークロードチェック
  - β値シグナル無効時のベンチマークロード不要チェック
  - パラメータ未設定時のベンチマークロード不要チェック
  - 両方でβ値シグナル有効時のベンチマークロードチェック

全195テストがパス。

## 影響範囲

- **修正ファイル**:
  - `src/strategies/core/mixins/backtest_executor_mixin.py`
  - `src/strategies/signals/processor.py`

- **追加ファイル**:
  - `tests/unit/strategies/mixins/test_backtest_executor_mixin.py`

- **既存機能への影響**: なし（全テストパス）

## 関連Issue

- ユーザー報告: β値フィルターが効かない問題

## 修正日時

2025-10-10
