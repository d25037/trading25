# パラメータ最適化システム実装計画

## 概要

戦略のハイパーパラメータを自動最適化し、インタラクティブな3D可視化HTMLを生成するシステム。

**設計方針**:
- ✅ **既存アーキテクチャを完全保持**: YAML完全制御・SignalParams構造を一切変更しない
- ✅ **YAML形式でパラメータグリッド定義**: 再利用性・管理性重視
- ✅ **既存default.yaml活用**: 新ディレクトリ不要・共通設定を継承
- ✅ **CLI並列処理**: リアルタイム進捗出力・高速化
- ✅ **インタラクティブ可視化**: 3D散布図・ヒートマップ等を含むHTML自動生成

---

## システムフロー

```
1. パラメータグリッド定義（YAML）
   ↓
2. CLI並列最適化実行
   ├─ ベース戦略YAML読み込み（シグナル有効化・固定パラメータ）
   ├─ グリッドYAMLマージ（最適化パラメータのみ上書き）
   ├─ SignalParams動的構築（ベース設定 + 最適化パラメータ）
   ├─ 並列バックテスト
   │  ├─ 初回等分配バックテスト（全銘柄均等配分）
   │  ├─ Kelly基準資金配分計算
   │  └─ Kelly配分バックテスト（最終評価）
   └─ リアルタイム進捗出力
   ↓
3. 最適化結果HTML生成
   ├─ 複合スコアリングランキング順ソート
   ├─ 3D可視化HTML自動生成
   └─ インタラクティブ分析環境
```

---

## 1. パラメータグリッド定義（YAML形式）

### ディレクトリ構成

```
config/
└── optimization/
    ├── template_grid.yaml  # テンプレート
    ├── range_break_v6_grid.yaml
    ├── sma_cross_grid.yaml
    └── bnf_mean_reversion_grid.yaml
```

### YAML形式仕様

**設計思想**:
- **最小限の記述**: パラメータ範囲のみ記述
- **自動推測**: ファイル名から戦略名・ベース設定を自動推測（`range_break_v6_grid.yaml` → `range_break_v6.yaml`）
- **デフォルト活用**: 最適化設定・共通設定はdefault.yamlから読み込み
- **規約ベース出力**: 結果ファイルは戦略名から自動生成

#### 設計原則: 戦略YAML vs グリッドYAMLの責任分離

**戦略YAML (`config/strategies/experimental/range_break_v6.yaml`) の役割**:
- ✅ シグナルの有効化（`enabled: true/false`）
- ✅ シグナルの方向・条件（`direction: "high"`, `condition: "break"`）
- ✅ 固定パラメータ（最適化しないパラメータ）
- ✅ 戦略の基本構造・ロジック

**グリッドYAML (`config/optimization/range_break_v6_grid.yaml`) の役割**:
- ✅ **最適化対象パラメータの範囲のみ**
- ✅ `parameter_ranges`配下のパラメータ値リスト

**統合ルール**:
1. ベース戦略YAMLを読み込み（全シグナル設定）
2. グリッドYAMLで指定されたパラメータ**のみ**上書き
3. その他の設定（`enabled`, `direction`, `condition`等）はベース戦略YAMLから継承

**例**: レンジブレイク戦略の場合
```yaml
# config/strategies/experimental/range_break_v6.yaml（ベース戦略）
entry_filter_params:
  period_extrema_break:
    enabled: true        # ← 戦略YAMLで管理
    direction: "high"    # ← 戦略YAMLで管理
    lookback_days: 10    # ← グリッドYAMLで最適化
    period: 100          # ← グリッドYAMLで最適化

  volume_ratio_above:
    enabled: true        # ← 戦略YAMLで管理
    ratio_threshold: 2.0  # ← グリッドYAMLで最適化
```

```yaml
# config/optimization/range_break_v6_grid.yaml（最適化パラメータのみ）
parameter_ranges:
  entry_filter_params:
    period_extrema_break:
      lookback_days: [5, 10, 15, 20]  # 最適化対象
      period: [30, 50, 100, 200]      # 最適化対象

    volume_ratio_above:
      ratio_threshold: [1.5, 2.0, 2.5, 3.0]  # 最適化対象
```

```yaml
# config/optimization/range_break_v6_grid.yaml

description: "レンジブレイクv6戦略のパラメータ最適化グリッド"  # オプション

# ===== 最適化するパラメータ範囲 =====
parameter_ranges:
  # entry_filter_params配下のパラメータ
  entry_filter_params:

    # Period Extrema Breakシグナル
    period_extrema_break:
      lookback_days: [5, 10, 15, 20]  # 短期最高値期間
      period: [30, 50, 100, 200]      # 長期最高値期間

    # Bollinger Positionシグナル
    bollinger_position:
      window: [10, 20, 30]  # BB期間
      alpha: [1.0, 1.5, 2.0]  # 標準偏差倍率

    # Volume Ratio Aboveシグナル
    volume_ratio_above:
      ratio_threshold: [1.5, 2.0, 2.5, 3.0]  # 出来高倍率
      short_period: [10, 20]  # 短期平均期間
      long_period: [50, 100]  # 長期平均期間

  # exit_trigger_params配下のパラメータ
  exit_trigger_params:

    # ATR Support Positionシグナル
    atr_support_position:
      lookback_period: [10, 20, 30]  # サポートライン期間
      atr_multiplier: [2.0, 2.5, 3.0]  # ATR倍率
```

**自動推測される設定**:
- **ベース戦略YAML**: `config/strategies/experimental/range_break_v6.yaml`（ファイル名から）
- **最適化設定**: `config/default.yaml`の`parameter_optimization`セクションから読み込み
- **共通設定**: `config/default.yaml`の`shared_config`セクションから読み込み
- **出力ファイル** (戦略別ディレクトリ + タイムスタンプ):
  - 可視化HTML: `~/.local/share/trading25/backtest/optimization/range_break_v6/20250112_143052.html`


### テンプレート

```yaml
# config/optimization/your_strategy_grid.yaml

description: "戦略の説明"  # オプション

parameter_ranges:
  entry_filter_params:
    # your_signal:
    #   param1: [value1, value2, value3]
    #   param2: [value1, value2]

  exit_trigger_params:
    # your_signal:
    #   param1: [value1, value2]
```

**注意**:
- ファイル名を`{戦略名}_grid.yaml`の形式にすることで、自動的に戦略設定が推測されます
- 最適化設定（method, n_jobs, scoring_weights等）は`config/default.yaml`で管理
- 可視化HTMLは`~/.local/share/trading25/backtest/optimization/{戦略名}/`に自動生成されます（タイムスタンプ付き）
- 生成されたHTMLには3D散布図、ヒートマップ、パラメータ相関等が含まれます

---

## 2. CLI使用方法

### 基本的な使い方

同名(*_grid)の戦略をconfig/optimization/から検索

```bash
# パラメータ最適化実行
uv run bt optimize range_break_v6 \
```

### 実行時の出力例

```
🚀 パラメータ最適化開始
戦略: range_break_v6
組み合わせ数: 96
並列処理数: 4

[1/96] lb=5, period=30, bb_win=10, vol_th=1.5: Sharpe=1.23, Calmar=0.89, Return=45.2%, Score=1.12
[2/96] lb=5, period=30, bb_win=10, vol_th=2.0: Sharpe=1.45, Calmar=1.02, Return=52.1%, Score=1.32
[3/96] lb=5, period=30, bb_win=10, vol_th=2.5: Sharpe=1.31, Calmar=0.95, Return=48.7%, Score=1.19
...
[96/96] lb=20, period=200, bb_win=30, vol_th=3.0: Sharpe=0.87, Calmar=0.65, Return=28.3%, Score=0.78

✅ 最適化完了!

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📊 最適化結果（複合スコアランキング上位10件）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🥇 Rank 1 - 複合スコア: 1.65
  entry_filter_params.period_extrema_break.lookback_days: 10
  entry_filter_params.period_extrema_break.period: 100
  entry_filter_params.bollinger_position.window: 20
  entry_filter_params.volume_ratio_above.ratio_threshold: 2.0
  → Sharpe: 1.87, Calmar: 1.35, Return: 68.5%

🥈 Rank 2 - 複合スコア: 1.58
  entry_filter_params.period_extrema_break.lookback_days: 10
  entry_filter_params.period_extrema_break.period: 100
  entry_filter_params.bollinger_position.window: 30
  entry_filter_params.volume_ratio_above.ratio_threshold: 2.0
  → Sharpe: 1.79, Calmar: 1.28, Return: 65.3%

🥉 Rank 3 - 複合スコア: 1.52
  entry_filter_params.period_extrema_break.lookback_days: 15
  entry_filter_params.period_extrema_break.period: 100
  entry_filter_params.bollinger_position.window: 20
  entry_filter_params.volume_ratio_above.ratio_threshold: 2.5
  → Sharpe: 1.72, Calmar: 1.22, Return: 62.1%

...

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📊 可視化HTML生成中...

✅ 可視化HTML生成完了!
  📓 ~/.local/share/trading25/backtest/optimization/range_break_v6/20250112_143052.html

次のステップ:
  open ~/.local/share/trading25/backtest/optimization/range_break_v6/20250112_143052.html
```

## 3. 実装詳細

### ファイル構成

```
src/
├── cli/
│   └── optimize.py  # 新規CLIコマンド
├── optimization/
│   ├── __init__.py
│   ├── engine.py  # 最適化エンジン
│   ├── param_builder.py  # SignalParams動的構築
│   ├── evaluator.py  # 評価指標計算
│   └── optimization_report_renderer.py  # 可視化HTML自動生成
config/
└── optimization/  # パラメータグリッド定義（新規）
    ├── template_grid.yaml
    ├── range_break_v6_grid.yaml
    ├── sma_cross_grid.yaml
    └── bnf_mean_reversion_grid.yaml
docs/
└── parameter-optimization-system.md  # このドキュメント
```

### 主要クラス設計

#### ParameterOptimizer（最適化エンジン）

```python
# src/optimization/engine.py

class ParameterOptimizer:
    """
    パラメータ最適化エンジン

    YAMLグリッド定義に基づき、並列最適化を実行
    """

    def __init__(self, strategy_name: str, grid_config_path: str):
        """
        Args:
            strategy_name: 戦略名（例: "range_break_v6"）
            grid_config_path: グリッドYAMLファイルパス
        """
        self.strategy_name = strategy_name

        # グリッドYAML読み込み
        with open(grid_config_path) as f:
            grid_config = yaml.safe_load(f)

        self.parameter_ranges = grid_config["parameter_ranges"]
        self.description = grid_config.get("description", "")

        # default.yaml読み込み
        default_config = self._load_default_config()

        # 最適化設定
        self.optimization = default_config["parameter_optimization"]

        # 共通設定
        self.shared_config = default_config["shared_config"]

        # ベース戦略YAML（グリッドで指定 or ファイル名から推測）
        self.base_config_path = grid_config.get(
            "base_config",
            self._infer_base_config_path(strategy_name)
        )

        # ベース戦略YAML読み込み（シグナル有効化・固定パラメータ）
        with open(self.base_config_path) as f:
            base_strategy_config = yaml.safe_load(f)

        # ベースSignalParamsを構築（最適化しない設定の基準値）
        self.base_entry_params = SignalParams(
            **base_strategy_config.get("entry_filter_params", {})
        )
        self.base_exit_params = SignalParams(
            **base_strategy_config.get("exit_trigger_params", {})
        )

        # 出力HTMLパス（規約ベース自動生成）
        self.output_html_path = self._generate_html_path(strategy_name)

    def optimize(self) -> OptimizationResult:
        """
        最適化実行

        Returns:
            OptimizationResult: 最適化結果
        """
        # 1. パラメータ組み合わせ生成
        combinations = self._generate_combinations()

        # 2. 並列最適化実行
        results = self._run_parallel_optimization(combinations)

        # 3. 複合スコアランキング順にソート
        sorted_results = sorted(
            results,
            key=lambda x: x["score"],
            reverse=True  # 降順（スコア高い順）
        )

        # 4. 可視化HTML生成
        html_path = self._generate_visualization_report(sorted_results)

        return OptimizationResult(
            best_params=sorted_results[0]["params"],
            best_score=sorted_results[0]["score"],
            all_results=sorted_results,
            html_path=html_path
        )

    def _load_default_config(self) -> Dict:
        """
        default.yamlから設定読み込み

        Returns:
            Dict: {
                "parameter_optimization": {...},
                "shared_config": {...}
            }
        """
        with open("config/default.yaml") as f:
            config = yaml.safe_load(f)

        return {
            "parameter_optimization": config["default"]["parameters"]["shared_config"]["parameter_optimization"],
            "shared_config": config["default"]["parameters"]["shared_config"]
        }

    def _infer_base_config_path(self, strategy_name: str) -> str:
        """
        戦略名からベース設定YAMLパスを推測

        Args:
            strategy_name: 戦略名（例: "range_break_v6"）

        Returns:
            str: ベース戦略YAMLパス

        推測ルール:
            1. config/strategies/experimental/{strategy_name}.yaml
            2. config/strategies/production/{strategy_name}.yaml
            3. 見つからない場合はエラー
        """
        candidates = [
            f"config/strategies/experimental/{strategy_name}.yaml",
            f"config/strategies/production/{strategy_name}.yaml"
        ]

        for path in candidates:
            if os.path.exists(path):
                return path

        raise FileNotFoundError(
            f"Base config not found for strategy '{strategy_name}'. "
            f"Searched: {candidates}"
        )

    def _generate_html_path(self, strategy_name: str) -> str:
        """
        戦略名から可視化HTMLパスを生成（戦略別ディレクトリ + タイムスタンプ）

        Args:
            strategy_name: 戦略名

        Returns:
            str: 可視化HTMLのパス

        出力形式:
            ~/.local/share/trading25/backtest/optimization/{strategy_name}/{timestamp}.html

        例:
            range_break_v6 → ~/.local/share/trading25/backtest/optimization/range_break_v6/20250112_143052.html
        """
        from datetime import datetime

        output_dir = f"~/.local/share/trading25/backtest/optimization/{strategy_name}"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        return f"{output_dir}/{timestamp}.html"

    def _generate_visualization_report(self, sorted_results: List[Dict]) -> str:
        """
        最適化結果から可視化HTMLを自動生成

        Args:
            sorted_results: 複合スコア順にソートされた最適化結果

        Returns:
            str: 生成されたHTMLのパス

        生成される可視化:
            - 複合スコアランキング表（上位20件）
            - 3D散布図（パラメータ vs スコア）
            - パラメータ感度分析（ヒートマップ）
            - 指標別分布図（Sharpe/Calmar/Return）
            - パラメータ相関行列
            - 最適パラメータ詳細表
        """
        from .optimization_report_renderer import generate_optimization_report

        html_path = generate_optimization_report(
            results=sorted_results,
            output_path=self.output_html_path,
            strategy_name=self.strategy_name,
            parameter_ranges=self.parameter_ranges,
            scoring_weights=self.optimization["scoring_weights"]
        )

        return html_path

    def _generate_combinations(self) -> List[Dict]:
        """パラメータ組み合わせ生成"""
        # parameter_rangesからデカルト積を生成
        pass

    def _run_parallel_optimization(
        self,
        combinations: List[Dict]
    ) -> List[Dict]:
        """並列最適化実行"""
        n_jobs = self.optimization["n_jobs"]

        with ProcessPoolExecutor(max_workers=n_jobs) as executor:
            futures = {
                executor.submit(
                    self._evaluate_single_params,
                    combo
                ): combo
                for combo in combinations
            }

            results = []
            for future in tqdm(as_completed(futures), total=len(futures)):
                result = future.result()

                # リアルタイム出力
                self._print_progress(result)
                results.append(result)

        return results

    def _evaluate_single_params(self, params: Dict) -> Dict:
        """
        単一パラメータ組み合わせを評価（2段階Kelly基準評価）

        評価プロセス:
            1. ベース戦略YAML設定 + グリッド最適化パラメータをマージ
            2. 初回バックテスト（全銘柄等分配）
            3. Kelly基準による最適資金配分計算
            4. Kelly配分バックテスト（最終評価）
        """
        # 1. SignalParamsを動的構築（ベース設定 + 最適化パラメータ）
        entry_params = build_signal_params(
            params,
            section="entry_filter_params",
            base_signal_params=self.base_entry_params  # ベース戦略YAMLから継承
        )
        exit_params = build_signal_params(
            params,
            section="exit_trigger_params",
            base_signal_params=self.base_exit_params  # ベース戦略YAMLから継承
        )

        # 2. YamlConfigurableStrategyを初期化
        strategy = YamlConfigurableStrategy(
            shared_config=SharedConfig(**self.shared_config),
            entry_filter_params=entry_params,
            exit_trigger_params=exit_params
        )

        # 3. 初回バックテスト実行（等分配）
        # Kelly基準計算用に全銘柄均等配分でバックテスト
        portfolio_equal = strategy.run_multi_backtest()

        # 4. Kelly基準による最適資金配分計算
        kelly_allocations = strategy.calculate_kelly_allocations(
            portfolio_equal,
            kelly_fraction=self.shared_config["kelly_fraction"]
        )

        # 5. Kelly配分バックテスト実行（最終評価）
        # 最適配分でバックテストを再実行
        portfolio_kelly = strategy.run_multi_backtest(
            custom_allocations=kelly_allocations
        )

        # 6. 評価指標計算（Kelly配分結果を使用）
        metrics = self._calculate_metrics(portfolio_kelly, strategy)

        # 7. 複合スコア計算
        score = self._calculate_composite_score(metrics)

        return {
            "params": params,
            "score": score,
            "metrics": metrics,
            "portfolio": portfolio_kelly,  # Kelly配分結果を保存
            "kelly_allocations": kelly_allocations  # 配分比率も保存
        }
```

#### SignalParamsBuilder（ベース設定マージ機能）

```python
# src/optimization/param_builder.py

def build_signal_params(
    params: Dict,
    section: str,
    base_signal_params: SignalParams
) -> SignalParams:
    """
    パラメータ辞書からSignalParamsを動的構築（ベース設定マージ）

    設計思想:
        - ベース戦略YAML（enabled, direction等）を継承
        - グリッドYAMLで指定されたパラメータのみ上書き
        - その他の設定は全てベース戦略YAMLから引き継ぐ

    Args:
        params: {"entry_filter_params.period_extrema_break.lookback_days": 10, ...}
        section: "entry_filter_params" or "exit_trigger_params"
        base_signal_params: ベース戦略YAMLから読み込んだSignalParams

    Returns:
        SignalParams: ベース設定 + 最適化パラメータをマージしたSignalParams

    Example:
        # ベース戦略YAML (range_break_v6.yaml)
        entry_filter_params:
          period_extrema_break:
            enabled: true           # ← ベース設定から継承
            direction: "high"       # ← ベース設定から継承
            lookback_days: 10       # ← グリッドで上書き
            period: 100             # ← グリッドで上書き

        # グリッドYAML (range_break_v6_grid.yaml)
        parameter_ranges:
          entry_filter_params:
            period_extrema_break:
              lookback_days: [5, 10, 15, 20]  # 最適化対象
              period: [30, 50, 100, 200]      # 最適化対象

        # 結果: enabled=True, direction="high"は継承
        #       lookback_days=10, period=100はグリッドから設定
    """
    # 1. ベース設定をdictに変換（継承用）
    base_params_dict = base_signal_params.model_dump()

    # 2. セクション抽出（グリッドから最適化パラメータのみ）
    section_params = {
        k.replace(f"{section}.", ""): v
        for k, v in params.items()
        if k.startswith(f"{section}.")
    }

    # 3. シグナル別にグルーピング
    # {"period_extrema_break.lookback_days": 10}
    # → {"period_extrema_break": {"lookback_days": 10}}
    grid_params_dict = {}
    for key, value in section_params.items():
        signal_name, param_name = key.split(".", 1)
        if signal_name not in grid_params_dict:
            grid_params_dict[signal_name] = {}
        grid_params_dict[signal_name][param_name] = value

    # 4. ベース設定とグリッド設定をマージ
    merged_params = {}
    for signal_name, base_signal_config in base_params_dict.items():
        if base_signal_config is None:
            continue

        # ベース設定を辞書化
        if hasattr(base_signal_config, "model_dump"):
            base_signal_dict = base_signal_config.model_dump()
        else:
            base_signal_dict = base_signal_config

        # グリッド設定で上書き
        if signal_name in grid_params_dict:
            base_signal_dict.update(grid_params_dict[signal_name])

        merged_params[signal_name] = base_signal_dict

    # 5. SignalParams構築（マージ結果から）
    kwargs = {}

    # Period Extrema Break
    if "period_extrema_break" in merged_params:
        kwargs["period_extrema_break"] = PeriodExtremaBreakSignalParams(
            **merged_params["period_extrema_break"]
        )

    # Bollinger Position
    if "bollinger_position" in merged_params:
        kwargs["bollinger_position"] = BollingerPositionSignalParams(
            **merged_params["bollinger_position"]
        )

    # Volume Ratio Above
    if "volume_ratio_above" in merged_params:
        kwargs["volume_ratio_above"] = VolumeRatioAboveSignalParams(
            **merged_params["volume_ratio_above"]
        )

    # ... 他のシグナルも同様（ベース設定 + グリッド設定をマージ）

    return SignalParams(**kwargs)
```

---

## 4. 既存コードへの影響

### 変更なし（完全な後方互換性）

- ✅ `src/strategies/core/yaml_configurable_strategy.py`
- ✅ `src/strategies/signals/processor.py`
- ✅ `src/models/signals.py`
- ✅ 全YAML設定ファイル（`config/strategies/`）

### 新規追加のみ

- `src/cli/optimize.py`（新規CLIコマンド）
- `src/optimization/`（新規パッケージ）
  - `engine.py`（最適化エンジン）
  - `param_builder.py`（SignalParams動的構築）
  - `evaluator.py`（評価指標計算）
  - `optimization_report_renderer.py`（可視化HTML自動生成）
- `config/optimization/`（新規ディレクトリ）
- `~/.local/share/trading25/backtest/optimization/`（可視化HTML出力先・戦略別サブディレクトリ）
- `docs/parameter-optimization-system.md`（このドキュメント）

---

## 5. 実装例

### 例1: SMAクロス戦略の最適化

```yaml
# config/optimization/sma_cross_grid.yaml
parameter_ranges:
  entry_filter_params:
    crossover:
      indicator_type: ["sma"]  # 固定
      short_period: [5, 10, 15, 20]
      long_period: [50, 100, 150, 200]
```

```bash
uv run bt optimize sma_cross
```

### 例2: BNF平均回帰戦略の最適化

```yaml
# config/optimization/bnf_mean_reversion_grid.yaml
parameter_ranges:
  entry_filter_params:
    baseline_deviation:
      baseline_type: ["sma", "ema"]
      baseline_period: [10, 20, 30, 50]
      deviation_threshold: [0.15, 0.2, 0.25]

    rsi_threshold:
      period: [7, 14, 21]
      oversold: [20, 25, 30]
```

```bash
uv run bt optimize bnf_mean_reversion
```

---

## 6. ベストプラクティス

### パラメータ範囲の設定

```yaml
# ❌ 悪い例: 極端すぎる範囲
parameter_ranges:
  entry_filter_params:
    period_extrema_break:
      lookback_days: [1, 2, 3, 500]  # 極端
      period: [5, 10, 1000]  # 極端

# ✅ 良い例: 常識的な範囲
parameter_ranges:
  entry_filter_params:
    period_extrema_break:
      lookback_days: [5, 10, 15, 20]  # 妥当
      period: [30, 50, 100, 200]  # 妥当
```

### 最適化設定のカスタマイズ

最適化設定（並列処理数、スコアリング重み等）は`config/default.yaml`で管理します：

```yaml
# config/default.yaml
default:
  parameters:
    shared_config:
      parameter_optimization:
        enabled: true
        method: "grid_search"  # "grid_search" or "random_search"
        n_jobs: 4  # 並列処理数（-1で全CPUコア）

        # 複合スコアリング設定
        scoring_weights:
          sharpe_ratio: 0.6  # リスク調整後リターン
          calmar_ratio: 0.3  # ドローダウン考慮
          total_return: 0.1  # 絶対リターン
```

**設計思想**: 複数の指標を組み合わせた複合スコアリングにより、リスク・リターン・ドローダウンをバランス良く評価し、ロバストな最適化を実現します。

---

## 7. トラブルシューティング

### Q1: 最適化が遅い

**原因**: パラメータ組み合わせ数が多い

**解決策**: `config/default.yaml`で並列処理数を調整
```yaml
# config/default.yaml
default:
  parameters:
    shared_config:
      parameter_optimization:
        n_jobs: 8  # CPUコア数に応じて並列処理数を増やす
        max_combinations: 50  # 最大組み合わせ数を制限
```

### Q2: 最適パラメータがTest期間で機能しない

**原因**: 過学習（Overfitting）

**解決策**:
- データ分割（Train/Test）を必ず行う
- パラメータ範囲を広げる
- 複合スコアリングでロバスト性を重視

---

## 8. まとめ

### 2つの重要設計原則（必読）

#### 原則1: 戦略YAML vs グリッドYAMLの責任分離

**設計思想**:
- **戦略YAML** (`config/strategies/experimental/range_break_v6.yaml`):
  - ✅ シグナルの有効化（`enabled: true/false`）
  - ✅ シグナルの方向・条件（`direction`, `condition`）
  - ✅ 固定パラメータ（最適化しないパラメータ）
  - ✅ 戦略の基本構造・ロジック

- **グリッドYAML** (`config/optimization/range_break_v6_grid.yaml`):
  - ✅ **最適化対象パラメータの範囲のみ**

**実装箇所**:
- `ParameterOptimizer.__init__()`: ベース戦略YAML読み込み（lines 308-318）
- `build_signal_params()`: ベース設定 + グリッド設定のマージロジック（lines 543-646）
- マージルール: `enabled`, `direction`, `condition`等はベース戦略YAMLから継承、最適化パラメータのみグリッドYAMLで上書き

#### 原則2: 2段階Kelly基準評価プロセス

**評価フロー**:
1. **初回等分配バックテスト**: 全銘柄均等配分で実行
2. **Kelly基準資金配分計算**: 期待リターン・勝率・リスクから最適配分比率を算出
3. **Kelly配分バックテスト**: 最適配分でバックテスト再実行（最終評価）
4. **複合スコア計算**: Kelly配分結果をもとに評価指標を算出

**実装箇所**:
- `_evaluate_single_params()`: 2段階Kelly基準評価の完全実装（lines 478-535）
- `run_multi_backtest()`: 等分配 → Kelly配分の2回実行
- 最終評価は必ずKelly配分結果を使用（リスク調整後の最適配分）

**意義**:
- 等分配では評価できない銘柄間リスク差を適切に反映
- オーバーフィッティング防止（リスク調整後の安定配分）
- 実運用に近い資金配分でパラメータ最適化

---

### システムの特徴

| 項目 | 説明 |
|------|------|
| **設計方針** | 既存アーキテクチャを完全保持 |
| **責任分離** | 戦略YAML（構造）vs グリッドYAML（最適化パラメータ） |
| **Kelly基準評価** | 2段階評価（等分配 → Kelly配分）によるリスク調整 |
| **パラメータ定義** | YAML形式（再利用性・管理性） |
| **共通設定管理** | 既存default.yamlを活用（新ディレクトリ不要） |
| **並列処理** | ProcessPoolExecutor（高速化） |
| **リアルタイム出力** | 進捗・評価指標をリアルタイム表示 |
| **可視化** | 3D散布図・ヒートマップ等のインタラクティブHTML自動生成 |
| **破壊的変更** | ゼロ（完全な後方互換性） |

### 次のステップ

1. ✅ この計画ドキュメントの確認・承認
2. パラメータグリッドYAML形式の最終決定
3. 最適化エンジン実装
4. CLIコマンド実装
5. 可視化HTML自動生成機能実装
6. テスト・検証

---

**🚀 このシステムにより、データドリブンな戦略パフォーマンス最適化が可能になります！**
