"""analysis テスト用の共通フィクスチャ"""

import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def returns_df():
    """ランダムリターンDataFrame (252日 x 3銘柄, seed=42)"""
    np.random.seed(42)
    idx = pd.date_range("2025-01-01", periods=252, freq="D")
    data = np.random.randn(252, 3) * 0.02
    return pd.DataFrame(data, index=idx, columns=[f"S{i}" for i in range(3)])
