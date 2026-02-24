"""
Strategies Package

戦略パッケージ（YAML完全制御版）

全戦略でYamlConfigurableStrategyを直接使用。
戦略固有ロジックは完全にYAML制御。
"""

# Core
from .core.yaml_configurable_strategy import YamlConfigurableStrategy
from .core.factory import StrategyFactory

__all__ = ["YamlConfigurableStrategy", "StrategyFactory"]
