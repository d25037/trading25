"""candidate_processor.py のテスト (_safe_float)"""


from src.agent.evaluator.candidate_processor import _safe_float


class TestSafeFloat:
    def test_normal_value(self):
        assert _safe_float(1.5) == 1.5

    def test_zero(self):
        assert _safe_float(0.0) == 0.0

    def test_negative(self):
        assert _safe_float(-3.14) == -3.14

    def test_nan_returns_default(self):
        assert _safe_float(float("nan")) == 0.0

    def test_inf_returns_default(self):
        assert _safe_float(float("inf")) == 0.0

    def test_neg_inf_returns_default(self):
        assert _safe_float(float("-inf")) == 0.0

    def test_custom_default(self):
        assert _safe_float(float("nan"), default=-1.0) == -1.0

    def test_large_value(self):
        assert _safe_float(1e15) == 1e15
