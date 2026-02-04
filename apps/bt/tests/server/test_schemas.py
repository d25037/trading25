"""server/schemas/ のテスト"""


from src.server.schemas.backtest import (
    BacktestRequest,
    BacktestResultSummary,
    HtmlFileInfo,
    HtmlFileRenameRequest,
    JobStatus,
)
from src.server.schemas.common import SSEJobEvent
from src.server.schemas.optimize import (
    OptimizationRequest,
)


class TestJobStatus:
    def test_values(self):
        assert JobStatus.PENDING.value == "pending"
        assert JobStatus.RUNNING.value == "running"
        assert JobStatus.COMPLETED.value == "completed"
        assert JobStatus.FAILED.value == "failed"


class TestBacktestRequest:
    def test_basic(self):
        req = BacktestRequest(strategy_name="test")
        assert req.strategy_name == "test"
        assert req.strategy_config_override is None

    def test_with_override(self):
        req = BacktestRequest(
            strategy_name="test",
            strategy_config_override={"initial_cash": 5000000},
        )
        assert req.strategy_config_override["initial_cash"] == 5000000


class TestBacktestResultSummary:
    def test_basic(self):
        s = BacktestResultSummary(
            total_return=0.15,
            sharpe_ratio=1.8,
            calmar_ratio=0.9,
            max_drawdown=-0.05,
            win_rate=0.65,
            trade_count=100,
        )
        assert s.total_return == 0.15
        assert s.trade_count == 100

    def test_optional_html_path(self):
        s = BacktestResultSummary(
            total_return=0.1, sharpe_ratio=1.0, calmar_ratio=0.5,
            max_drawdown=-0.1, win_rate=0.5, trade_count=50,
            html_path="/path/to/result.html",
        )
        assert s.html_path == "/path/to/result.html"


class TestSSEJobEvent:
    def test_basic(self):
        event = SSEJobEvent(
            job_id="abc", status="running", progress=0.5, message="processing"
        )
        assert event.job_id == "abc"
        assert event.progress == 0.5

    def test_optional_fields(self):
        event = SSEJobEvent(job_id="abc", status="pending")
        assert event.progress is None
        assert event.message is None
        assert event.data is None


class TestOptimizationRequest:
    def test_basic(self):
        req = OptimizationRequest(strategy_name="test")
        assert req.strategy_name == "test"



class TestHtmlFileInfo:
    def test_basic(self):
        info = HtmlFileInfo(
            strategy_name="test_strat",
            filename="result.html",
            dataset_name="testds",
            created_at="2025-01-01T00:00:00",
            size_bytes=1024,
        )
        assert info.strategy_name == "test_strat"
        assert info.size_bytes == 1024


class TestHtmlFileRenameRequest:
    def test_basic(self):
        req = HtmlFileRenameRequest(new_filename="renamed.html")
        assert req.new_filename == "renamed.html"
