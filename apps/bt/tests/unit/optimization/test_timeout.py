"""
Optimization timeout helper tests
"""

import sys
import time

import pytest

from src.optimization.engine import _run_with_timeout


@pytest.mark.skipif(sys.platform.startswith("win"), reason="signal-based timeout unsupported on Windows")
def test_run_with_timeout_raises_timeout_error():
    with pytest.raises(TimeoutError):
        _run_with_timeout(0.1, lambda: time.sleep(0.3))


@pytest.mark.skipif(sys.platform.startswith("win"), reason="signal-based timeout unsupported on Windows")
def test_run_with_timeout_returns_value():
    assert _run_with_timeout(1, lambda: "ok") == "ok"
