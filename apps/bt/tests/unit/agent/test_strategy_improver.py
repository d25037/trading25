"""strategy_improver.py のテスト"""

from unittest.mock import MagicMock

import pandas as pd

from src.domains.lab_agent.models import Improvement, WeaknessReport
from src.domains.lab_agent.strategy_improver import StrategyImprover


# ===== Helpers =====


def _make_mock_portfolio(
    max_dd=0.1,
    drawdown_values=None,
    trades_data=None,
    returns_data=None,
):
    """テスト用モックポートフォリオ"""
    portfolio = MagicMock()
    portfolio.max_drawdown.return_value = max_dd

    # drawdown series
    if drawdown_values is None:
        drawdown_values = [0.0, -0.02, -0.05, -0.1, -0.03, 0.0]
    dd_series = pd.Series(drawdown_values, index=pd.date_range("2025-01-01", periods=len(drawdown_values)))
    portfolio.drawdown.return_value = dd_series

    # trades
    if trades_data is None:
        trades_data = pd.DataFrame(
            {
                "Return": [0.05, -0.03, 0.02, -0.01, 0.04],
                "Entry Timestamp": pd.date_range("2025-01-01", periods=5),
                "Exit Timestamp": pd.date_range("2025-01-06", periods=5),
            }
        )
    portfolio.trades.records_readable = trades_data

    # returns
    if returns_data is None:
        returns_data = pd.Series([0.01, -0.005, 0.02, -0.01, 0.005])
    portfolio.returns.return_value = returns_data

    return portfolio


# ===== _analyze_portfolio =====


class TestAnalyzePortfolio:
    def test_basic_analysis(self):
        improver = StrategyImprover()
        portfolio = _make_mock_portfolio(max_dd=0.15)
        report = improver._analyze_portfolio(portfolio, "test_strategy")
        assert isinstance(report, WeaknessReport)
        assert report.strategy_name == "test_strategy"
        assert report.max_drawdown == 0.15

    def test_high_drawdown(self):
        improver = StrategyImprover()
        portfolio = _make_mock_portfolio(max_dd=0.4)
        report = improver._analyze_portfolio(portfolio, "test")
        assert report.max_drawdown == 0.4

    def test_nan_drawdown_fallback(self):
        improver = StrategyImprover()
        portfolio = _make_mock_portfolio()
        portfolio.max_drawdown.return_value = float("nan")
        report = improver._analyze_portfolio(portfolio, "test")
        assert report.max_drawdown == 0.0

    def test_drawdown_period_analysis(self):
        improver = StrategyImprover()
        dd_values = [0.0, -0.01, -0.05, -0.1, -0.08, 0.0]
        portfolio = _make_mock_portfolio(drawdown_values=dd_values)
        report = improver._analyze_portfolio(portfolio, "test")
        assert report.max_drawdown_duration_days is not None
        assert report.max_drawdown_duration_days >= 0

    def test_consecutive_losses_detected(self):
        improver = StrategyImprover()
        trades = pd.DataFrame(
            {
                "Return": [-0.01, -0.02, -0.03, -0.04, 0.05],
                "Entry Timestamp": pd.date_range("2025-01-01", periods=5),
                "Exit Timestamp": pd.date_range("2025-01-06", periods=5),
            }
        )
        portfolio = _make_mock_portfolio(trades_data=trades)
        report = improver._analyze_portfolio(portfolio, "test")
        consecutive = [p for p in report.losing_trade_patterns if p.get("type") == "consecutive_losses"]
        assert len(consecutive) == 1
        assert consecutive[0]["count"] == 4

    def test_worst_trade_detected(self):
        improver = StrategyImprover()
        trades = pd.DataFrame(
            {
                "Return": [0.05, -0.1, 0.03],
                "Entry Timestamp": pd.date_range("2025-01-01", periods=3),
                "Exit Timestamp": pd.date_range("2025-01-04", periods=3),
            }
        )
        portfolio = _make_mock_portfolio(trades_data=trades)
        report = improver._analyze_portfolio(portfolio, "test")
        worst = [p for p in report.losing_trade_patterns if p.get("type") == "worst_trade"]
        assert len(worst) == 1
        assert worst[0]["return"] == -0.1

    def test_market_condition_analysis(self):
        improver = StrategyImprover()
        returns = pd.Series([0.02, 0.01, -0.03, -0.01, 0.005])
        portfolio = _make_mock_portfolio(returns_data=returns)
        report = improver._analyze_portfolio(portfolio, "test")
        assert "bull_market_avg" in report.performance_by_market_condition
        assert "bear_market_avg" in report.performance_by_market_condition
        assert "bull_ratio" in report.performance_by_market_condition

    def test_empty_trades(self):
        improver = StrategyImprover()
        empty_trades = pd.DataFrame(columns=["Return", "Entry Timestamp", "Exit Timestamp"])
        portfolio = _make_mock_portfolio(trades_data=empty_trades)
        report = improver._analyze_portfolio(portfolio, "test")
        assert report.losing_trade_patterns == []

    def test_drawdown_exception_handled(self):
        improver = StrategyImprover()
        portfolio = _make_mock_portfolio()
        portfolio.max_drawdown.side_effect = Exception("fail")
        report = improver._analyze_portfolio(portfolio, "test")
        assert report.max_drawdown == 0.0

    def test_returns_exception_handled(self):
        improver = StrategyImprover()
        portfolio = _make_mock_portfolio()
        portfolio.returns.side_effect = Exception("fail")
        report = improver._analyze_portfolio(portfolio, "test")
        assert report.performance_by_market_condition == {}


# ===== _generate_improvement_suggestions =====


class TestGenerateImprovementSuggestions:
    def test_high_drawdown_suggestion(self):
        improver = StrategyImprover()
        report = WeaknessReport(strategy_name="test", max_drawdown=0.4)
        config = {"entry_filter_params": {}, "exit_trigger_params": {}}
        suggestions = improver._generate_improvement_suggestions(report, config)
        assert any("ドローダウン" in s for s in suggestions)

    def test_consecutive_loss_suggestion(self):
        improver = StrategyImprover()
        report = WeaknessReport(
            strategy_name="test",
            losing_trade_patterns=[{"type": "consecutive_losses", "count": 6}],
        )
        config = {"entry_filter_params": {}, "exit_trigger_params": {}}
        suggestions = improver._generate_improvement_suggestions(report, config)
        assert any("連続" in s for s in suggestions)

    def test_bear_market_suggestion(self):
        improver = StrategyImprover()
        report = WeaknessReport(
            strategy_name="test",
            performance_by_market_condition={"bear_market_avg": -0.05},
        )
        config = {"entry_filter_params": {}, "exit_trigger_params": {}}
        suggestions = improver._generate_improvement_suggestions(report, config)
        assert any("下落相場" in s for s in suggestions)

    def test_max_5_suggestions(self):
        improver = StrategyImprover()
        report = WeaknessReport(
            strategy_name="test",
            max_drawdown=0.5,
            losing_trade_patterns=[{"type": "consecutive_losses", "count": 10}],
            performance_by_market_condition={"bear_market_avg": -0.1},
        )
        config = {"entry_filter_params": {}, "exit_trigger_params": {}}
        suggestions = improver._generate_improvement_suggestions(report, config)
        assert len(suggestions) <= 5

    def test_no_suggestions_for_good_strategy(self):
        improver = StrategyImprover()
        report = WeaknessReport(
            strategy_name="test",
            max_drawdown=0.05,
            performance_by_market_condition={"bear_market_avg": -0.001},
        )
        config = {"entry_filter_params": {"volume": {}, "rsi_threshold": {}}, "exit_trigger_params": {}}
        suggestions = improver._generate_improvement_suggestions(report, config)
        assert not any("ドローダウン" in s for s in suggestions)

    def test_suggestions_respect_fundamental_only_constraints(self):
        improver = StrategyImprover()
        report = WeaknessReport(
            strategy_name="test",
            max_drawdown=0.5,
            performance_by_market_condition={"bear_market_avg": -0.05},
        )
        config = {"entry_filter_params": {}, "exit_trigger_params": {}}
        suggestions = improver._generate_improvement_suggestions(
            report,
            config,
            entry_filter_only=True,
            allowed_categories=["fundamental"],
        )
        assert all("下落相場" not in s for s in suggestions)


# ===== suggest_improvements =====


class TestSuggestImprovements:
    def test_high_drawdown_adds_atr(self):
        improver = StrategyImprover()
        report = WeaknessReport(strategy_name="test", max_drawdown=0.4)
        config = {"entry_filter_params": {}, "exit_trigger_params": {}}
        improvements = improver.suggest_improvements(report, config)
        atr_imps = [i for i in improvements if i.signal_name == "atr_support_break"]
        assert len(atr_imps) == 1
        assert atr_imps[0].target == "exit"

    def test_volume_filter_suggested(self):
        improver = StrategyImprover()
        report = WeaknessReport(strategy_name="test")
        config = {"entry_filter_params": {}, "exit_trigger_params": {}}
        improvements = improver.suggest_improvements(report, config)
        vol_imps = [i for i in improvements if i.signal_name == "volume"]
        assert len(vol_imps) == 1
        assert vol_imps[0].target == "entry"

    def test_no_volume_if_already_present(self):
        improver = StrategyImprover()
        report = WeaknessReport(strategy_name="test")
        config = {"entry_filter_params": {"volume": {"enabled": True}}, "exit_trigger_params": {}}
        improvements = improver.suggest_improvements(report, config)
        vol_imps = [i for i in improvements if i.signal_name == "volume"]
        assert len(vol_imps) == 0

    def test_bear_market_adds_index_signal(self):
        improver = StrategyImprover()
        report = WeaknessReport(
            strategy_name="test",
            performance_by_market_condition={"bear_market_avg": -0.05},
        )
        config = {"entry_filter_params": {}, "exit_trigger_params": {}}
        improvements = improver.suggest_improvements(report, config)
        idx_imps = [i for i in improvements if i.signal_name == "index_daily_change"]
        assert len(idx_imps) == 1

    def test_improvement_types(self):
        improver = StrategyImprover()
        report = WeaknessReport(strategy_name="test", max_drawdown=0.5)
        config = {"entry_filter_params": {}, "exit_trigger_params": {}}
        improvements = improver.suggest_improvements(report, config)
        for imp in improvements:
            assert isinstance(imp, Improvement)
            assert imp.improvement_type in ("add_signal", "remove_signal", "adjust_param")

    def test_entry_filter_only_blocks_exit_improvements(self):
        improver = StrategyImprover()
        report = WeaknessReport(strategy_name="test", max_drawdown=0.5)
        config = {"entry_filter_params": {}, "exit_trigger_params": {}}
        improvements = improver.suggest_improvements(
            report,
            config,
            entry_filter_only=True,
        )
        assert all(imp.target == "entry" for imp in improvements)
        assert all(imp.signal_name != "atr_support_break" for imp in improvements)

    def test_fundamental_only_constraints(self):
        improver = StrategyImprover()
        report = WeaknessReport(
            strategy_name="test",
            max_drawdown=0.5,
            performance_by_market_condition={"bear_market_avg": -0.05},
        )
        config = {"entry_filter_params": {}, "exit_trigger_params": {}}
        improvements = improver.suggest_improvements(
            report,
            config,
            entry_filter_only=True,
            allowed_categories=["fundamental"],
        )

        assert len(improvements) >= 1
        assert all(imp.target == "entry" for imp in improvements)
        assert all(imp.signal_name == "fundamental" for imp in improvements)


# ===== apply_improvements =====


class TestApplyImprovements:
    def test_add_signal_to_entry(self):
        improver = StrategyImprover()
        config = {"entry_filter_params": {}, "exit_trigger_params": {}}
        improvements = [
            Improvement(
                improvement_type="add_signal",
                target="entry",
                signal_name="volume",
                changes={"enabled": True, "threshold": 1.5},
                reason="test",
                expected_impact="test",
            )
        ]
        result = improver.apply_improvements(config, improvements)
        assert "volume" in result["entry_filter_params"]
        assert result["entry_filter_params"]["volume"]["threshold"] == 1.5

    def test_add_signal_to_exit(self):
        improver = StrategyImprover()
        config = {"entry_filter_params": {}, "exit_trigger_params": {}}
        improvements = [
            Improvement(
                improvement_type="add_signal",
                target="exit",
                signal_name="rsi_threshold",
                changes={"enabled": True, "period": 14},
                reason="test",
                expected_impact="test",
            )
        ]
        result = improver.apply_improvements(config, improvements)
        assert "rsi_threshold" in result["exit_trigger_params"]

    def test_remove_signal(self):
        improver = StrategyImprover()
        config = {
            "entry_filter_params": {"volume": {"enabled": True}},
            "exit_trigger_params": {},
        }
        improvements = [
            Improvement(
                improvement_type="remove_signal",
                target="entry",
                signal_name="volume",
                reason="test",
                expected_impact="test",
            )
        ]
        result = improver.apply_improvements(config, improvements)
        assert "volume" not in result["entry_filter_params"]

    def test_adjust_param(self):
        improver = StrategyImprover()
        config = {
            "entry_filter_params": {"volume": {"enabled": True, "threshold": 1.0}},
            "exit_trigger_params": {},
        }
        improvements = [
            Improvement(
                improvement_type="adjust_param",
                target="entry",
                signal_name="volume",
                changes={"threshold": 2.0},
                reason="test",
                expected_impact="test",
            )
        ]
        result = improver.apply_improvements(config, improvements)
        assert result["entry_filter_params"]["volume"]["threshold"] == 2.0
        assert result["entry_filter_params"]["volume"]["enabled"] is True

    def test_does_not_modify_original(self):
        improver = StrategyImprover()
        config = {"entry_filter_params": {"volume": {"enabled": True}}, "exit_trigger_params": {}}
        improvements = [
            Improvement(
                improvement_type="remove_signal",
                target="entry",
                signal_name="volume",
                reason="test",
                expected_impact="test",
            )
        ]
        result = improver.apply_improvements(config, improvements)
        assert "volume" not in result["entry_filter_params"]
        assert "volume" in config["entry_filter_params"]  # original unchanged

    def test_add_signal_creates_missing_key(self):
        improver = StrategyImprover()
        config = {}
        improvements = [
            Improvement(
                improvement_type="add_signal",
                target="entry",
                signal_name="volume",
                changes={"enabled": True},
                reason="test",
                expected_impact="test",
            )
        ]
        result = improver.apply_improvements(config, improvements)
        assert "entry_filter_params" in result
        assert "volume" in result["entry_filter_params"]

    def test_remove_nonexistent_signal_noop(self):
        improver = StrategyImprover()
        config = {"entry_filter_params": {}, "exit_trigger_params": {}}
        improvements = [
            Improvement(
                improvement_type="remove_signal",
                target="entry",
                signal_name="nonexistent",
                reason="test",
                expected_impact="test",
            )
        ]
        result = improver.apply_improvements(config, improvements)
        assert result == config

    def test_adjust_nonexistent_signal_noop(self):
        improver = StrategyImprover()
        config = {"entry_filter_params": {}, "exit_trigger_params": {}}
        improvements = [
            Improvement(
                improvement_type="adjust_param",
                target="entry",
                signal_name="nonexistent",
                changes={"threshold": 2.0},
                reason="test",
                expected_impact="test",
            )
        ]
        result = improver.apply_improvements(config, improvements)
        assert "nonexistent" not in result["entry_filter_params"]
