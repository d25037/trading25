"""Tests for dataset loader utility normalization."""

from __future__ import annotations

import pytest

from src.infrastructure.data_access.loaders.utils import extract_dataset_name


def test_extract_dataset_name_accepts_legacy_path() -> None:
    assert extract_dataset_name("dataset/primeExTopix500.db") == "primeExTopix500"


def test_extract_dataset_name_rejects_path_traversal() -> None:
    with pytest.raises(ValueError, match="Invalid dataset name"):
        extract_dataset_name("../primeExTopix500.db")
