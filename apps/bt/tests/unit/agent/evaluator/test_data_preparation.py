"""agent/evaluator/data_preparation.py のテスト"""

from pathlib import Path
from typing import Any
from unittest.mock import patch

import pandas as pd

from src.domains.lab_agent.evaluator.data_preparation import (
    BatchPreparedData,
    _get_fallback_shared_config,
    convert_dataframes_to_dict,
    convert_dict_to_dataframes,
    load_default_shared_config,
)


class TestConvertDataframesToDict:
    def test_basic_conversion(self) -> None:
        idx = pd.date_range("2024-01-01", periods=3)
        df = pd.DataFrame({"Close": [100.0, 101.0, 102.0]}, index=idx)
        data = {"7203": {"daily": df}}
        result = convert_dataframes_to_dict(data)
        assert "7203" in result
        assert "daily" in result["7203"]
        assert result["7203"]["daily"]["columns"] == ["Close"]
        assert len(result["7203"]["daily"]["data"]) == 3

    def test_empty_data(self) -> None:
        result = convert_dataframes_to_dict({})
        assert result == {}

    def test_multiple_stocks(self) -> None:
        idx = pd.date_range("2024-01-01", periods=2)
        df1 = pd.DataFrame({"Close": [100.0, 101.0]}, index=idx)
        df2 = pd.DataFrame({"Close": [200.0, 201.0]}, index=idx)
        data = {"7203": {"daily": df1}, "9984": {"daily": df2}}
        result = convert_dataframes_to_dict(data)
        assert len(result) == 2


class TestConvertDictToDataframes:
    def test_roundtrip(self) -> None:
        idx = pd.date_range("2024-01-01", periods=3)
        df = pd.DataFrame({"Close": [100.0, 101.0, 102.0], "Volume": [1000, 2000, 3000]}, index=idx)
        data = {"7203": {"daily": df}}
        serialized = convert_dataframes_to_dict(data)
        restored = convert_dict_to_dataframes(serialized)
        assert "7203" in restored
        assert "daily" in restored["7203"]
        assert list(restored["7203"]["daily"].columns) == ["Close", "Volume"]
        assert len(restored["7203"]["daily"]) == 3

    def test_empty_data(self) -> None:
        result = convert_dict_to_dataframes({})
        assert result == {}


class TestGetFallbackSharedConfig:
    def test_returns_dict(self) -> None:
        config = _get_fallback_shared_config()
        assert isinstance(config, dict)
        assert "initial_cash" in config
        assert "fees" in config
        assert "dataset" in config
        assert "kelly_fraction" in config


class TestLoadDefaultSharedConfig:
    def test_file_not_exists(self, monkeypatch: Any) -> None:
        monkeypatch.setattr(Path, "exists", lambda self: False)
        config = load_default_shared_config()
        assert "initial_cash" in config  # fallback config

    def test_file_exists(self, tmp_path: Path) -> None:
        yaml_content = """
default:
  parameters:
    shared_config:
      initial_cash: 5000000
      fees: 0.001
"""
        yaml_file = tmp_path / "default.yaml"
        yaml_file.write_text(yaml_content)
        with patch(
            "src.domains.lab_agent.evaluator.data_preparation.Path",
            return_value=yaml_file,
        ):
            # Need to patch the Path constructor
            from src.domains.lab_agent.evaluator import data_preparation
            original = data_preparation.Path
            data_preparation.Path = lambda x: yaml_file  # type: ignore
            try:
                config = load_default_shared_config()
                assert config.get("initial_cash") == 5000000
            finally:
                data_preparation.Path = original


class TestBatchPreparedData:
    def test_creation(self) -> None:
        data = BatchPreparedData(stock_codes=["7203"], ohlcv_data=None, benchmark_data=None)
        assert data.stock_codes == ["7203"]
        assert data.ohlcv_data is None
        assert data.benchmark_data is None
