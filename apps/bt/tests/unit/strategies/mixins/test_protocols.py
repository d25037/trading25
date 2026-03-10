"""strategy core protocol definitions smoke tests."""

from src.domains.strategy.core.mixins import protocols


def test_strategy_protocol_import_exposes_execution_portfolio_surface() -> None:
    annotations = protocols.StrategyProtocol.__annotations__

    assert annotations["combined_portfolio"] is not None
    assert annotations["portfolio"] is not None
    assert annotations["execution_adapter"] is not None
