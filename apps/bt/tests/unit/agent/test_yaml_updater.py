"""yaml_updater.py のテスト"""

import os
from unittest.mock import MagicMock, patch

from ruamel.yaml import YAML

from src.domains.lab_agent.models import Improvement, StrategyCandidate
from src.domains.lab_agent.yaml_updater import YamlUpdater


def _make_candidate():
    return StrategyCandidate(
        strategy_id="test_strat",
        entry_filter_params={
            "volume": {"enabled": True, "direction": "surge", "threshold": 1.5},
        },
        exit_trigger_params={
            "rsi_threshold": {"enabled": True, "period": 14, "threshold": 70.0},
        },
        shared_config={"initial_cash": 5000000},
    )


class TestYamlUpdaterInit:
    def test_default_base_dir(self):
        updater = YamlUpdater()
        assert updater.base_dir == "config/strategies"

    def test_custom_base_dir(self):
        updater = YamlUpdater(base_dir="/custom/path")
        assert updater.base_dir == "/custom/path"

    def test_use_external_default(self):
        updater = YamlUpdater()
        assert updater.use_external is True

    def test_use_external_false(self):
        updater = YamlUpdater(use_external=False)
        assert updater.use_external is False


class TestResolveStrategyBasename:
    def test_returns_last_path_segment(self):
        updater = YamlUpdater()
        assert (
            updater._resolve_strategy_basename("production/range_break_v15")
            == "range_break_v15"
        )
        assert (
            updater._resolve_strategy_basename(r"production\range_break_v15")
            == "range_break_v15"
        )

    def test_empty_name_falls_back_to_default(self):
        updater = YamlUpdater()
        assert updater._resolve_strategy_basename("///") == "strategy"


class TestBuildYamlContent:
    def test_basic_structure(self):
        updater = YamlUpdater()
        candidate = _make_candidate()
        content = updater._build_yaml_content(candidate)
        assert "entry_filter_params" in content
        assert "exit_trigger_params" in content
        assert "shared_config" in content

    def test_empty_params(self):
        updater = YamlUpdater()
        candidate = StrategyCandidate(
            strategy_id="empty",
            entry_filter_params={},
            exit_trigger_params={},
        )
        content = updater._build_yaml_content(candidate)
        assert "entry_filter_params" not in content
        assert "exit_trigger_params" not in content

    def test_no_shared_config(self):
        updater = YamlUpdater()
        candidate = StrategyCandidate(
            strategy_id="no_shared",
            entry_filter_params={"volume": {"enabled": True}},
            exit_trigger_params={},
        )
        content = updater._build_yaml_content(candidate)
        assert "shared_config" not in content


class TestSaveCandidate:
    def test_save_to_path(self, tmp_path):
        updater = YamlUpdater(base_dir=str(tmp_path), use_external=False)
        candidate = _make_candidate()
        output = str(tmp_path / "output.yaml")
        result_path = updater.save_candidate(candidate, output_path=output)
        assert os.path.exists(result_path)

        yaml = YAML()
        with open(result_path) as f:
            loaded = yaml.load(f)
        assert "entry_filter_params" in loaded

    def test_auto_generate_path(self, tmp_path):
        updater = YamlUpdater(base_dir=str(tmp_path), use_external=False)
        candidate = _make_candidate()
        result_path = updater.save_candidate(candidate, category="production")
        assert os.path.exists(result_path)
        assert "production" in result_path

    def test_auto_generate_path_external_experimental(self, tmp_path):
        updater = YamlUpdater(base_dir=str(tmp_path), use_external=True)
        candidate = _make_candidate()

        with patch("src.shared.paths.get_strategies_dir", return_value=tmp_path):
            result_path = updater.save_candidate(candidate, category="experimental")

        assert os.path.exists(result_path)
        assert os.path.dirname(result_path) == str(tmp_path / "auto")


class TestFormatSignalParams:
    def test_numeric_rounding(self):
        updater = YamlUpdater()
        params = {"volume": {"threshold": 1.555555, "enabled": True}}
        formatted = updater._format_signal_params(params)
        assert "volume" in formatted

    def test_disabled_signal_excluded(self):
        updater = YamlUpdater()
        params = {"volume": {"enabled": False, "threshold": 1.0}}
        formatted = updater._format_signal_params(params)
        assert "volume" not in formatted

    def test_non_dict_signal_kept(self):
        updater = YamlUpdater()
        params = {"simple_signal": "value"}
        formatted = updater._format_signal_params(params)
        assert "simple_signal" in formatted


class TestBuildImprovedYaml:
    def test_header_contains_original_name(self):
        updater = YamlUpdater()
        config = {"entry_filter_params": {"volume": {"enabled": True}}}
        improvements = [
            Improvement(
                improvement_type="add_signal",
                target="entry",
                signal_name="volume",
                reason="test reason",
                expected_impact="test",
            )
        ]
        result = updater._build_improved_yaml(config, "original_strat", improvements)
        assert "original_strat" in result
        assert "Applied improvements:" in result
        assert "test reason" in result

    def test_yaml_body_present(self):
        updater = YamlUpdater()
        config = {"entry_filter_params": {"volume": {"enabled": True}}}
        improvements = []
        result = updater._build_improved_yaml(config, "test", improvements)
        assert "entry_filter_params" in result


class TestSaveEvolutionResult:
    def test_saves_two_files(self, tmp_path):
        updater = YamlUpdater(base_dir=str(tmp_path), use_external=False)
        candidate = _make_candidate()
        history = [{"generation": 1, "best_score": 0.5}]
        strategy_path, history_path = updater.save_evolution_result(
            candidate, history, "test_strat", output_dir=str(tmp_path)
        )
        assert os.path.exists(strategy_path)
        assert os.path.exists(history_path)
        assert "history" in history_path

        yaml = YAML()
        with open(history_path) as f:
            loaded = yaml.load(f)
        assert "evolution_history" in loaded
        assert loaded["strategy_id"] == "test_strat"

    def test_metadata_preserved(self, tmp_path):
        updater = YamlUpdater(base_dir=str(tmp_path), use_external=False)
        candidate = _make_candidate()
        candidate.metadata = {"method": "evolution", "generation": 10}
        history = []
        _, history_path = updater.save_evolution_result(
            candidate, history, "test", output_dir=str(tmp_path)
        )
        yaml = YAML()
        with open(history_path) as f:
            loaded = yaml.load(f)
        assert loaded["metadata"]["method"] == "evolution"

    def test_category_prefixed_name_does_not_create_subdir(self, tmp_path):
        updater = YamlUpdater(base_dir=str(tmp_path), use_external=False)
        candidate = _make_candidate()
        history = [{"generation": 1, "best_score": 0.5}]

        strategy_path, history_path = updater.save_evolution_result(
            candidate,
            history,
            "production/range_break_v15",
            output_dir=str(tmp_path),
        )

        assert os.path.dirname(strategy_path) == str(tmp_path)
        assert os.path.dirname(history_path) == str(tmp_path)
        assert os.path.basename(strategy_path).startswith("range_break_v15_")
        assert os.path.basename(history_path).startswith("range_break_v15_")

    def test_default_output_dir_non_external(self, tmp_path):
        updater = YamlUpdater(base_dir=str(tmp_path), use_external=False)
        candidate = _make_candidate()
        history = [{"generation": 1, "best_score": 0.5}]

        strategy_path, history_path = updater.save_evolution_result(
            candidate, history, "production/range_break_v15"
        )

        expected_dir = tmp_path / "experimental" / "evolved"
        assert os.path.dirname(strategy_path) == str(expected_dir)
        assert os.path.dirname(history_path) == str(expected_dir)

    def test_default_output_dir_external(self, tmp_path):
        updater = YamlUpdater(base_dir=str(tmp_path), use_external=True)
        candidate = _make_candidate()
        history = [{"generation": 1, "best_score": 0.5}]

        with patch("src.shared.paths.get_strategies_dir", return_value=tmp_path):
            strategy_path, history_path = updater.save_evolution_result(
                candidate, history, "production/range_break_v15"
            )

        expected_dir = tmp_path / "evolved"
        assert os.path.dirname(strategy_path) == str(expected_dir)
        assert os.path.dirname(history_path) == str(expected_dir)


class TestSaveOptunaResult:
    def test_saves_two_files(self, tmp_path):
        updater = YamlUpdater(base_dir=str(tmp_path), use_external=False)
        candidate = _make_candidate()
        study_history = [{"trial": 1, "value": 0.8}]
        strategy_path, history_path = updater.save_optuna_result(
            candidate, study_history, "test_strat", output_dir=str(tmp_path)
        )
        assert os.path.exists(strategy_path)
        assert os.path.exists(history_path)

        yaml = YAML()
        with open(history_path) as f:
            loaded = yaml.load(f)
        assert "optuna_history" in loaded

    def test_optuna_history_content(self, tmp_path):
        updater = YamlUpdater(base_dir=str(tmp_path), use_external=False)
        candidate = _make_candidate()
        study_history = [{"trial": 1, "value": 0.8}, {"trial": 2, "value": 0.9}]
        _, history_path = updater.save_optuna_result(
            candidate, study_history, "test", output_dir=str(tmp_path)
        )
        yaml = YAML()
        with open(history_path) as f:
            loaded = yaml.load(f)
        assert len(loaded["optuna_history"]) == 2

    def test_category_prefixed_name_does_not_create_subdir(self, tmp_path):
        updater = YamlUpdater(base_dir=str(tmp_path), use_external=False)
        candidate = _make_candidate()
        study_history = [{"trial": 1, "value": 0.8}]

        strategy_path, history_path = updater.save_optuna_result(
            candidate,
            study_history,
            "production/range_break_v15",
            output_dir=str(tmp_path),
        )

        assert os.path.dirname(strategy_path) == str(tmp_path)
        assert os.path.dirname(history_path) == str(tmp_path)
        assert os.path.basename(strategy_path).startswith("range_break_v15_")
        assert os.path.basename(history_path).startswith("range_break_v15_")

    def test_default_output_dir_non_external(self, tmp_path):
        updater = YamlUpdater(base_dir=str(tmp_path), use_external=False)
        candidate = _make_candidate()
        study_history = [{"trial": 1, "value": 0.8}]

        strategy_path, history_path = updater.save_optuna_result(
            candidate, study_history, "production/range_break_v15"
        )

        expected_dir = tmp_path / "experimental" / "optuna"
        assert os.path.dirname(strategy_path) == str(expected_dir)
        assert os.path.dirname(history_path) == str(expected_dir)

    def test_default_output_dir_external(self, tmp_path):
        updater = YamlUpdater(base_dir=str(tmp_path), use_external=True)
        candidate = _make_candidate()
        study_history = [{"trial": 1, "value": 0.8}]

        with patch("src.shared.paths.get_strategies_dir", return_value=tmp_path):
            strategy_path, history_path = updater.save_optuna_result(
                candidate, study_history, "production/range_break_v15"
            )

        expected_dir = tmp_path / "optuna"
        assert os.path.dirname(strategy_path) == str(expected_dir)
        assert os.path.dirname(history_path) == str(expected_dir)


class TestApplyImprovements:
    def test_apply_improvements_integration(self, tmp_path):
        updater = YamlUpdater(base_dir=str(tmp_path), use_external=False)
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
        original_config = {"entry_filter_params": {}, "exit_trigger_params": {}}
        output_path = str(tmp_path / "improved.yaml")

        with (
            patch("src.domains.strategy.runtime.loader.ConfigLoader") as mock_loader_cls,
            patch("src.domains.lab_agent.strategy_improver.StrategyImprover") as mock_improver_cls,
        ):
            mock_loader = MagicMock()
            mock_loader.load_strategy_config.return_value = original_config
            mock_loader_cls.return_value = mock_loader

            mock_improver = MagicMock()
            improved_config = {
                "entry_filter_params": {"volume": {"enabled": True, "threshold": 1.5}},
                "exit_trigger_params": {},
            }
            mock_improver.apply_improvements.return_value = improved_config
            mock_improver_cls.return_value = mock_improver

            result_path = updater.apply_improvements(
                "test_strategy", improvements, output_path=output_path
            )
        assert os.path.exists(result_path)
        content = open(result_path).read()
        assert "volume" in content

    def test_apply_improvements_default_path_non_external(self, tmp_path):
        updater = YamlUpdater(base_dir=str(tmp_path), use_external=False)
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
        original_config = {"entry_filter_params": {}, "exit_trigger_params": {}}

        with (
            patch("src.domains.strategy.runtime.loader.ConfigLoader") as mock_loader_cls,
            patch("src.domains.lab_agent.strategy_improver.StrategyImprover") as mock_improver_cls,
        ):
            mock_loader = MagicMock()
            mock_loader.load_strategy_config.return_value = original_config
            mock_loader_cls.return_value = mock_loader

            mock_improver = MagicMock()
            improved_config = {
                "entry_filter_params": {"volume": {"enabled": True, "threshold": 1.5}},
                "exit_trigger_params": {},
            }
            mock_improver.apply_improvements.return_value = improved_config
            mock_improver_cls.return_value = mock_improver

            result_path = updater.apply_improvements(
                "production/test_strategy", improvements, output_path=None
            )

        assert os.path.dirname(result_path) == str(tmp_path / "experimental")
        assert os.path.basename(result_path).startswith("test_strategy_improved_")

    def test_apply_improvements_default_path_external(self, tmp_path):
        updater = YamlUpdater(base_dir=str(tmp_path), use_external=True)
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
        original_config = {"entry_filter_params": {}, "exit_trigger_params": {}}

        with (
            patch("src.shared.paths.get_strategies_dir", return_value=tmp_path),
            patch("src.domains.strategy.runtime.loader.ConfigLoader") as mock_loader_cls,
            patch("src.domains.lab_agent.strategy_improver.StrategyImprover") as mock_improver_cls,
        ):
            mock_loader = MagicMock()
            mock_loader.load_strategy_config.return_value = original_config
            mock_loader_cls.return_value = mock_loader

            mock_improver = MagicMock()
            improved_config = {
                "entry_filter_params": {"volume": {"enabled": True, "threshold": 1.5}},
                "exit_trigger_params": {},
            }
            mock_improver.apply_improvements.return_value = improved_config
            mock_improver_cls.return_value = mock_improver

            result_path = updater.apply_improvements(
                "production/test_strategy", improvements, output_path=None
            )

        assert os.path.dirname(result_path) == str(tmp_path)
        assert os.path.basename(result_path).startswith("test_strategy_improved_")
