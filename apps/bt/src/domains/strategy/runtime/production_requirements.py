from __future__ import annotations

from typing import Any


def is_production_category(category: str | None) -> bool:
    return category == "production"


def has_explicit_dataset_in_strategy_config(config: dict[str, Any]) -> bool:
    shared_config = config.get("shared_config")
    if not isinstance(shared_config, dict):
        return False

    dataset = shared_config.get("dataset")
    return isinstance(dataset, str) and bool(dataset.strip())


def validate_production_strategy_dataset_requirement(
    *,
    category: str | None,
    config: dict[str, Any],
    strategy_name: str | None = None,
) -> None:
    if not is_production_category(category):
        return

    if has_explicit_dataset_in_strategy_config(config):
        return

    target = strategy_name or "production strategy"
    raise ValueError(
        f"{target} must declare shared_config.dataset explicitly in YAML. "
        "default.yaml inheritance does not satisfy production requirements."
    )
