"""
StrategyGenerator のユニットテスト
"""

from src.agent.models import GeneratorConfig, StrategyCandidate
from src.agent.strategy_generator import (
    AVAILABLE_SIGNALS,
    SIGNAL_CONSTRAINTS_MAP,
    StrategyGenerator,
)


class TestStrategyGenerator:
    """StrategyGenerator のテスト"""

    def test_available_signals_defined(self):
        """利用可能シグナルが定義されていることを確認"""
        assert len(AVAILABLE_SIGNALS) > 0
        assert len(SIGNAL_CONSTRAINTS_MAP) > 0
        assert "fundamental" in SIGNAL_CONSTRAINTS_MAP

    def test_signal_constraints_have_required_fields(self):
        """シグナル制約が必須フィールドを持つことを確認"""
        for signal in AVAILABLE_SIGNALS:
            assert signal.name is not None
            assert signal.usage in ["entry", "exit", "both"]

    def test_generator_initialization_default(self):
        """デフォルト設定で初期化できることを確認"""
        generator = StrategyGenerator()
        assert generator.config.n_strategies == 100
        assert len(generator.entry_signals) > 0
        assert len(generator.exit_signals) > 0

    def test_generator_initialization_custom_config(self):
        """カスタム設定で初期化できることを確認"""
        config = GeneratorConfig(
            n_strategies=50,
            entry_signal_min=3,
            entry_signal_max=4,
            seed=42,
        )
        generator = StrategyGenerator(config=config)
        assert generator.config.n_strategies == 50

    def test_generate_creates_candidates(self):
        """戦略候補を生成できることを確認"""
        config = GeneratorConfig(n_strategies=5, seed=42)
        generator = StrategyGenerator(config=config)
        candidates = generator.generate()

        assert len(candidates) == 5
        for candidate in candidates:
            assert isinstance(candidate, StrategyCandidate)
            assert candidate.strategy_id is not None
            assert isinstance(candidate.entry_filter_params, dict)
            assert isinstance(candidate.exit_trigger_params, dict)

    def test_generate_with_different_seed_produces_different_results(self):
        """異なるシードで異なる結果が生成されることを確認"""
        config1 = GeneratorConfig(n_strategies=3, seed=42)
        config2 = GeneratorConfig(n_strategies=3, seed=123)

        generator1 = StrategyGenerator(config=config1)
        generator2 = StrategyGenerator(config=config2)

        candidates1 = generator1.generate()
        candidates2 = generator2.generate()

        # 少なくとも1つは異なるはず（確率的だが高確率）
        ids1 = {c.strategy_id for c in candidates1}
        ids2 = {c.strategy_id for c in candidates2}
        # strategy_idはUUIDベースなので必ず異なる
        assert ids1 != ids2

    def test_generate_respects_signal_count_range(self):
        """シグナル数の範囲が守られることを確認"""
        config = GeneratorConfig(
            n_strategies=10,
            entry_signal_min=2,
            entry_signal_max=3,
            exit_signal_min=2,
            exit_signal_max=2,
            seed=42,
        )
        generator = StrategyGenerator(config=config)
        candidates = generator.generate()

        for candidate in candidates:
            enabled_entry = sum(
                1
                for params in candidate.entry_filter_params.values()
                if isinstance(params, dict) and params.get("enabled", False)
            )
            enabled_exit = sum(
                1
                for params in candidate.exit_trigger_params.values()
                if isinstance(params, dict) and params.get("enabled", False)
            )

            assert 2 <= enabled_entry <= 3
            assert enabled_exit == 2

    def test_generate_excludes_signals(self):
        """除外シグナルが生成から除外されることを確認"""
        config = GeneratorConfig(
            n_strategies=10,
            exclude_signals=["volume", "beta"],
            seed=42,
        )
        generator = StrategyGenerator(config=config)
        candidates = generator.generate()

        for candidate in candidates:
            assert "volume" not in candidate.entry_filter_params
            assert "beta" not in candidate.entry_filter_params

    def test_generate_from_template(self):
        """テンプレートからバリエーションを生成できることを確認"""
        generator = StrategyGenerator()
        template = {
            "entry": ["period_breakout", "volume"],
            "exit": ["atr_support_break"],
        }

        variations = generator.generate_from_template(template, n_variations=5)

        assert len(variations) == 5
        for v in variations:
            assert "period_breakout" in v.entry_filter_params
            assert "volume" in v.entry_filter_params
            assert "atr_support_break" in v.exit_trigger_params

    def test_candidate_has_metadata(self):
        """生成された候補がメタデータを持つことを確認"""
        config = GeneratorConfig(n_strategies=1, seed=42)
        generator = StrategyGenerator(config=config)
        candidates = generator.generate()

        candidate = candidates[0]
        assert "generation_index" in candidate.metadata
        assert "entry_signals" in candidate.metadata
        assert "exit_signals" in candidate.metadata

    def test_generate_randomizes_parameters(self):
        """generate()がパラメータをランダム化することを確認"""
        config1 = GeneratorConfig(n_strategies=5, seed=42)
        config2 = GeneratorConfig(n_strategies=5, seed=123)

        gen1 = StrategyGenerator(config=config1)
        gen2 = StrategyGenerator(config=config2)

        candidates1 = gen1.generate()
        candidates2 = gen2.generate()

        # 数値パラメータの値を収集
        numeric_values1: list[float] = []
        numeric_values2: list[float] = []

        for c in candidates1:
            for params in c.entry_filter_params.values():
                if isinstance(params, dict):
                    for key, value in params.items():
                        if key != "enabled" and isinstance(value, (int, float)):
                            numeric_values1.append(value)

        for c in candidates2:
            for params in c.entry_filter_params.values():
                if isinstance(params, dict):
                    for key, value in params.items():
                        if key != "enabled" and isinstance(value, (int, float)):
                            numeric_values2.append(value)

        # 異なるシードで値が異なることを確認
        assert numeric_values1 != numeric_values2

    def test_generate_parameters_within_valid_ranges(self):
        """生成されたパラメータが有効範囲内であることを確認"""
        from src.agent.parameter_evolver import ParameterEvolver

        config = GeneratorConfig(n_strategies=10, seed=42)
        generator = StrategyGenerator(config=config)
        candidates = generator.generate()

        for candidate in candidates:
            # entry_filter_params のチェック
            for signal_name, params in candidate.entry_filter_params.items():
                if signal_name in ParameterEvolver.PARAM_RANGES:
                    ranges = ParameterEvolver.PARAM_RANGES[signal_name]
                    for param_name, (min_val, max_val, _) in ranges.items():
                        if param_name in params:
                            value = params[param_name]
                            assert min_val <= value <= max_val, (
                                f"{signal_name}.{param_name}={value} "
                                f"is outside range [{min_val}, {max_val}]"
                            )

            # exit_trigger_params のチェック
            for signal_name, params in candidate.exit_trigger_params.items():
                if signal_name in ParameterEvolver.PARAM_RANGES:
                    ranges = ParameterEvolver.PARAM_RANGES[signal_name]
                    for param_name, (min_val, max_val, _) in ranges.items():
                        if param_name in params:
                            value = params[param_name]
                            assert min_val <= value <= max_val, (
                                f"{signal_name}.{param_name}={value} "
                                f"is outside range [{min_val}, {max_val}]"
                            )

    def test_generate_same_seed_produces_same_parameters(self):
        """同じシードで同じパラメータ値が生成されることを確認（再現性）"""
        # 注: random はグローバルステートのため、シード設定直後に generate() を呼ぶ必要あり

        # 1回目
        config1 = GeneratorConfig(n_strategies=5, seed=42)
        gen1 = StrategyGenerator(config=config1)
        candidates1 = gen1.generate()

        # 2回目（同じシードで新規インスタンス）
        config2 = GeneratorConfig(n_strategies=5, seed=42)
        gen2 = StrategyGenerator(config=config2)
        candidates2 = gen2.generate()

        # 同じ位置の候補で、同じシグナルのパラメータが一致することを確認
        for c1, c2 in zip(candidates1, candidates2):
            for signal_name in c1.entry_filter_params:
                if signal_name in c2.entry_filter_params:
                    assert c1.entry_filter_params[signal_name] == c2.entry_filter_params[signal_name]
            for signal_name in c1.exit_trigger_params:
                if signal_name in c2.exit_trigger_params:
                    assert c1.exit_trigger_params[signal_name] == c2.exit_trigger_params[signal_name]

    def test_allowed_categories_filters_signals(self):
        """allowed_categories で候補シグナルが絞り込まれる"""
        config = GeneratorConfig(
            n_strategies=3,
            seed=42,
            allowed_categories=["fundamental"],
            entry_signal_min=1,
            entry_signal_max=1,
        )
        generator = StrategyGenerator(config=config)

        assert [s.name for s in generator.entry_signals] == ["fundamental"]
        assert generator.exit_signals == []

    def test_entry_filter_only_generates_no_exit_signals(self):
        """entry_filter_only=True のとき exit_trigger_params は空"""
        config = GeneratorConfig(
            n_strategies=3,
            seed=42,
            entry_filter_only=True,
            allowed_categories=["fundamental"],
            entry_signal_min=1,
            entry_signal_max=1,
        )
        generator = StrategyGenerator(config=config)
        candidates = generator.generate()

        assert len(candidates) == 3
        for candidate in candidates:
            assert candidate.exit_trigger_params == {}
            assert "fundamental" in candidate.entry_filter_params

    def test_fundamental_params_have_parent_and_child_enabled(self):
        """fundamental 生成時に親 enabled と子シグナル enabled が設定される"""
        config = GeneratorConfig(
            n_strategies=1,
            seed=42,
            allowed_categories=["fundamental"],
            entry_signal_min=1,
            entry_signal_max=1,
            entry_filter_only=True,
        )
        generator = StrategyGenerator(config=config)
        candidate = generator.generate()[0]

        fundamental = candidate.entry_filter_params["fundamental"]
        assert fundamental["enabled"] is True
        child_enabled = [
            key
            for key, value in fundamental.items()
            if isinstance(value, dict) and value.get("enabled")
        ]
        assert len(child_enabled) >= 1
