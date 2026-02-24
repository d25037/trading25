"""server/routes/backtest.py のテスト"""

import base64
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from src.entrypoints.http.schemas.backtest import BacktestResultSummary, JobStatus


@pytest.fixture
def mock_services():
    """backtest_service, job_manager を一括mock"""
    with (
        patch("src.entrypoints.http.routes.backtest.backtest_service") as mock_bt_svc,
        patch("src.entrypoints.http.routes.backtest.job_manager") as mock_jm,
    ):
        yield mock_bt_svc, mock_jm


def _make_job(
    job_id="test-1",
    status=JobStatus.COMPLETED,
    result=None,
    html_path=None,
    job_type="backtest",
    raw_result=None,
):
    job = MagicMock()
    job.job_id = job_id
    job.job_type = job_type
    job.status = status
    job.progress = 1.0 if status == JobStatus.COMPLETED else 0.0
    job.message = None
    job.created_at = datetime(2025, 1, 1)
    job.started_at = datetime(2025, 1, 1)
    job.completed_at = datetime(2025, 1, 1) if status == JobStatus.COMPLETED else None
    job.error = None
    job.result = result
    job.raw_result = raw_result
    job.html_path = html_path
    job.strategy_name = "test_strategy"
    job.dataset_name = "test_dataset"
    job.execution_time = 5.0
    return job


@pytest.fixture
def client():
    from src.entrypoints.http.app import create_app
    app = create_app()
    return TestClient(app)


class TestRunBacktest:
    def test_success(self, client, mock_services):
        mock_bt_svc, mock_jm = mock_services
        mock_bt_svc.submit_backtest = AsyncMock(return_value="job-1")
        mock_jm.get_job.return_value = _make_job("job-1", JobStatus.PENDING)
        resp = client.post(
            "/api/backtest/run",
            json={"strategy_name": "test"},
        )
        assert resp.status_code == 200
        assert resp.json()["job_id"] == "job-1"

    def test_not_found_job(self, client, mock_services):
        mock_bt_svc, mock_jm = mock_services
        mock_bt_svc.submit_backtest = AsyncMock(return_value="job-1")
        mock_jm.get_job.return_value = None
        resp = client.post(
            "/api/backtest/run",
            json={"strategy_name": "test"},
        )
        assert resp.status_code == 404

    def test_submit_error_returns_500(self, client, mock_services):
        mock_bt_svc, _ = mock_services
        mock_bt_svc.submit_backtest = AsyncMock(side_effect=RuntimeError("submit failed"))

        resp = client.post("/api/backtest/run", json={"strategy_name": "test"})
        assert resp.status_code == 500
        assert "submit failed" in str(resp.json())


class TestCancelJob:
    def test_success(self, client, mock_services):
        _, mock_jm = mock_services
        mock_jm.get_job.return_value = _make_job("job-1", JobStatus.RUNNING)
        mock_jm.cancel_job = AsyncMock(return_value=_make_job("job-1", JobStatus.CANCELLED))
        resp = client.post("/api/backtest/jobs/job-1/cancel")
        assert resp.status_code == 200

    def test_conflict_completed(self, client, mock_services):
        _, mock_jm = mock_services
        completed_job = _make_job("job-1", JobStatus.COMPLETED)
        mock_jm.get_job.return_value = completed_job
        mock_jm.cancel_job = AsyncMock(return_value=None)
        resp = client.post("/api/backtest/jobs/job-1/cancel")
        assert resp.status_code == 409


class TestGetResult:
    def test_success_no_html(self, client, mock_services):
        _, mock_jm = mock_services
        result = BacktestResultSummary(
            total_return=10.0,
            sharpe_ratio=1.5,
            calmar_ratio=2.0,
            max_drawdown=-5.0,
            win_rate=60.0,
            trade_count=100,
        )
        mock_jm.get_job.return_value = _make_job("job-1", JobStatus.COMPLETED, result=result)
        resp = client.get("/api/backtest/result/job-1")
        assert resp.status_code == 200
        data = resp.json()
        assert data["strategy_name"] == "test_strategy"

    def test_not_completed(self, client, mock_services):
        _, mock_jm = mock_services
        mock_jm.get_job.return_value = _make_job("job-1", JobStatus.RUNNING)
        resp = client.get("/api/backtest/result/job-1")
        assert resp.status_code == 400

    def test_not_found(self, client, mock_services):
        _, mock_jm = mock_services
        mock_jm.get_job.return_value = None
        resp = client.get("/api/backtest/result/missing")
        assert resp.status_code == 404

    def test_missing_result_returns_500(self, client, mock_services):
        _, mock_jm = mock_services
        mock_jm.get_job.return_value = _make_job("job-1", JobStatus.COMPLETED, result=None)
        resp = client.get("/api/backtest/result/job-1")
        assert resp.status_code == 500

    def test_success_uses_artifact_summary_even_when_result_missing(self, client, mock_services, tmp_path):
        _, mock_jm = mock_services
        html_path = tmp_path / "report.html"
        html_path.write_text("<html>ok</html>", encoding="utf-8")
        mock_jm.get_job.return_value = _make_job(
            "job-1",
            JobStatus.COMPLETED,
            result=None,
            html_path=str(html_path),
            raw_result={
                "total_return": 1.0,
                "sharpe_ratio": 1.0,
                "sortino_ratio": 1.0,
                "calmar_ratio": 1.0,
                "max_drawdown": -1.0,
                "win_rate": 50.0,
                "trade_count": 1,
            },
        )
        metrics = MagicMock(
            total_return=11.0,
            sharpe_ratio=1.9,
            sortino_ratio=2.2,
            calmar_ratio=2.4,
            max_drawdown=-4.0,
            win_rate=61.0,
            total_trades=17,
        )

        with patch("src.application.services.backtest_result_summary.extract_metrics_from_html", return_value=metrics):
            resp = client.get("/api/backtest/result/job-1")

        assert resp.status_code == 200
        data = resp.json()
        assert data["summary"]["total_return"] == 11.0
        assert data["summary"]["sortino_ratio"] == 2.2
        assert data["summary"]["trade_count"] == 17

    def test_include_html_returns_base64_content(self, client, mock_services, tmp_path):
        _, mock_jm = mock_services
        html_path = tmp_path / "report.html"
        html_path.write_bytes(b"<html>ok</html>")
        result = BacktestResultSummary(
            total_return=1.0,
            sharpe_ratio=1.0,
            calmar_ratio=1.0,
            max_drawdown=-0.1,
            win_rate=0.5,
            trade_count=1,
        )
        mock_jm.get_job.return_value = _make_job(
            "job-1",
            JobStatus.COMPLETED,
            result=result,
            html_path=str(html_path),
        )

        resp = client.get("/api/backtest/result/job-1?include_html=true")
        assert resp.status_code == 200
        assert resp.json()["html_content"] == base64.b64encode(b"<html>ok</html>").decode("utf-8")


class TestBacktestJobEndpoints:
    def test_get_job_status_success(self, client, mock_services):
        _, mock_jm = mock_services
        result = BacktestResultSummary(
            total_return=10.0,
            sharpe_ratio=1.5,
            sortino_ratio=1.8,
            calmar_ratio=2.0,
            max_drawdown=-5.0,
            win_rate=60.0,
            trade_count=100,
        )
        mock_jm.get_job.return_value = _make_job("job-1", JobStatus.COMPLETED, result=result)

        resp = client.get("/api/backtest/jobs/job-1")
        assert resp.status_code == 200
        assert resp.json()["job_id"] == "job-1"
        assert resp.json()["result"]["sortino_ratio"] == 1.8

    def test_get_job_status_prefers_artifact_summary(self, client, mock_services, tmp_path):
        _, mock_jm = mock_services
        html_path = tmp_path / "report.html"
        html_path.write_text("<html>ok</html>", encoding="utf-8")
        result = BacktestResultSummary(
            total_return=5.0,
            sharpe_ratio=1.1,
            sortino_ratio=1.3,
            calmar_ratio=1.4,
            max_drawdown=-3.0,
            win_rate=54.0,
            trade_count=9,
        )
        mock_jm.get_job.return_value = _make_job(
            "job-1",
            JobStatus.COMPLETED,
            result=result,
            html_path=str(html_path),
            raw_result=result.model_dump(),
        )
        metrics = MagicMock(
            total_return=15.0,
            sharpe_ratio=2.5,
            sortino_ratio=2.9,
            calmar_ratio=3.2,
            max_drawdown=-6.0,
            win_rate=66.0,
            total_trades=21,
        )

        with patch("src.application.services.backtest_result_summary.extract_metrics_from_html", return_value=metrics):
            resp = client.get("/api/backtest/jobs/job-1")

        assert resp.status_code == 200
        data = resp.json()
        assert data["result"]["total_return"] == 15.0
        assert data["result"]["sortino_ratio"] == 2.9
        assert data["result"]["trade_count"] == 21

    def test_list_jobs_success(self, client, mock_services):
        _, mock_jm = mock_services
        mock_jm.list_jobs.return_value = [
            _make_job("job-1", JobStatus.RUNNING),
            _make_job("job-2", JobStatus.COMPLETED),
        ]

        resp = client.get("/api/backtest/jobs?limit=2")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2
        assert data[0]["job_id"] == "job-1"


class TestSignalAttributionEndpoints:
    def _attribution_result(self):
        return {
            "baseline_metrics": {"total_return": 12.0, "sharpe_ratio": 1.2},
            "signals": [
                {
                    "signal_id": "entry.volume",
                    "scope": "entry",
                    "param_key": "volume",
                    "signal_name": "出来高",
                    "loo": {
                        "status": "ok",
                        "variant_metrics": {"total_return": 10.0, "sharpe_ratio": 1.0},
                        "delta_total_return": 2.0,
                        "delta_sharpe_ratio": 0.2,
                        "error": None,
                    },
                    "shapley": {
                        "status": "ok",
                        "total_return": 1.5,
                        "sharpe_ratio": 0.1,
                        "method": "exact",
                        "sample_size": 2,
                        "error": None,
                    },
                }
            ],
            "top_n_selection": {
                "top_n_requested": 5,
                "top_n_effective": 1,
                "selected_signal_ids": ["entry.volume"],
                "scores": [{"signal_id": "entry.volume", "score": 1.0}],
            },
            "timing": {
                "total_seconds": 1.0,
                "baseline_seconds": 0.2,
                "loo_seconds": 0.5,
                "shapley_seconds": 0.3,
            },
            "shapley": {
                "method": "exact",
                "sample_size": 2,
                "error": None,
                "evaluations": 2,
            },
        }

    def test_run_submit_success(self, client):
        with (
            patch("src.entrypoints.http.routes.backtest.backtest_attribution_service") as mock_attr_svc,
            patch("src.entrypoints.http.routes.backtest.job_manager") as mock_jm,
        ):
            mock_attr_svc.submit_attribution = AsyncMock(return_value="attr-1")
            mock_jm.get_job.return_value = _make_job(
                "attr-1",
                JobStatus.PENDING,
                job_type="backtest_attribution",
            )
            resp = client.post(
                "/api/backtest/attribution/run",
                json={"strategy_name": "test"},
            )
            assert resp.status_code == 200
            assert resp.json()["job_id"] == "attr-1"

    def test_run_submit_error_returns_500(self, client):
        with patch("src.entrypoints.http.routes.backtest.backtest_attribution_service") as mock_attr_svc:
            mock_attr_svc.submit_attribution = AsyncMock(side_effect=RuntimeError("attr submit failed"))
            resp = client.post("/api/backtest/attribution/run", json={"strategy_name": "test"})
            assert resp.status_code == 500
            assert "attr submit failed" in str(resp.json())

    def test_get_job_success_completed_with_result(self, client):
        with patch("src.entrypoints.http.routes.backtest.job_manager") as mock_jm:
            mock_jm.get_job.return_value = _make_job(
                "attr-1",
                JobStatus.COMPLETED,
                job_type="backtest_attribution",
                raw_result=self._attribution_result(),
            )
            resp = client.get("/api/backtest/attribution/jobs/attr-1")
            assert resp.status_code == 200
            data = resp.json()
            assert data["status"] == "completed"
            assert data["result_data"]["baseline_metrics"]["total_return"] == 12.0

    def test_get_job_attempts_parse_for_empty_raw_result(self, client):
        with (
            patch("src.entrypoints.http.routes.backtest.job_manager") as mock_jm,
            patch(
                "src.entrypoints.http.routes.backtest.SignalAttributionResult.model_validate",
                side_effect=ValueError("invalid result"),
            ) as mock_validate,
        ):
            mock_jm.get_job.return_value = _make_job(
                "attr-1",
                JobStatus.COMPLETED,
                job_type="backtest_attribution",
                raw_result={},
            )
            resp = client.get("/api/backtest/attribution/jobs/attr-1")
            assert resp.status_code == 200
            mock_validate.assert_called_once_with({})

    def test_get_job_type_mismatch(self, client):
        with patch("src.entrypoints.http.routes.backtest.job_manager") as mock_jm:
            mock_jm.get_job.return_value = _make_job(
                "attr-1",
                JobStatus.RUNNING,
                job_type="backtest",
            )
            resp = client.get("/api/backtest/attribution/jobs/attr-1")
            assert resp.status_code == 400

    def test_cancel_success(self, client):
        with patch("src.entrypoints.http.routes.backtest.job_manager") as mock_jm:
            mock_jm.get_job.return_value = _make_job(
                "attr-1",
                JobStatus.RUNNING,
                job_type="backtest_attribution",
            )
            mock_jm.cancel_job = AsyncMock(
                return_value=_make_job(
                    "attr-1",
                    JobStatus.CANCELLED,
                    job_type="backtest_attribution",
                )
            )
            resp = client.post("/api/backtest/attribution/jobs/attr-1/cancel")
            assert resp.status_code == 200
            assert resp.json()["status"] == "cancelled"

    def test_stream_nonexistent_job(self, client):
        with patch("src.entrypoints.http.routes.backtest.job_manager") as mock_jm:
            mock_jm.get_job.return_value = None
            resp = client.get("/api/backtest/attribution/jobs/missing/stream")
            assert resp.status_code == 404

    def test_result_success(self, client):
        with patch("src.entrypoints.http.routes.backtest.job_manager") as mock_jm:
            mock_jm.get_job.return_value = _make_job(
                "attr-1",
                JobStatus.COMPLETED,
                job_type="backtest_attribution",
                raw_result=self._attribution_result(),
            )
            resp = client.get("/api/backtest/attribution/result/attr-1")
            assert resp.status_code == 200
            data = resp.json()
            assert data["job_id"] == "attr-1"
            assert data["result"]["signals"][0]["signal_id"] == "entry.volume"

    def test_result_not_completed(self, client):
        with patch("src.entrypoints.http.routes.backtest.job_manager") as mock_jm:
            mock_jm.get_job.return_value = _make_job(
                "attr-1",
                JobStatus.RUNNING,
                job_type="backtest_attribution",
            )
            resp = client.get("/api/backtest/attribution/result/attr-1")
            assert resp.status_code == 400

    def test_result_missing_raw_result_returns_500(self, client):
        with patch("src.entrypoints.http.routes.backtest.job_manager") as mock_jm:
            mock_jm.get_job.return_value = _make_job(
                "attr-1",
                JobStatus.COMPLETED,
                job_type="backtest_attribution",
                raw_result=None,
            )
            resp = client.get("/api/backtest/attribution/result/attr-1")
            assert resp.status_code == 500

    def test_result_parse_error_returns_500(self, client):
        with (
            patch("src.entrypoints.http.routes.backtest.job_manager") as mock_jm,
            patch(
                "src.entrypoints.http.routes.backtest.SignalAttributionResult.model_validate",
                side_effect=ValueError("bad attribution"),
            ),
        ):
            mock_jm.get_job.return_value = _make_job(
                "attr-1",
                JobStatus.COMPLETED,
                job_type="backtest_attribution",
                raw_result={"unexpected": "shape"},
            )
            resp = client.get("/api/backtest/attribution/result/attr-1")
            assert resp.status_code == 500
            assert "bad attribution" in str(resp.json())

    def test_stream_success(self, client):
        async def _gen(_job_id):
            yield {"data": "ok"}

        with (
            patch("src.entrypoints.http.routes.backtest.job_manager") as mock_jm,
            patch("src.entrypoints.http.routes.backtest.sse_manager") as mock_sse,
        ):
            mock_jm.get_job.return_value = _make_job(
                "attr-1",
                JobStatus.RUNNING,
                job_type="backtest_attribution",
            )
            mock_sse.job_event_generator.return_value = _gen("attr-1")
            resp = client.get("/api/backtest/attribution/jobs/attr-1/stream")
            assert resp.status_code == 200


class TestAttributionArtifactEndpoints:
    def test_list_attribution_files(self, client, tmp_path):
        with (
            patch("src.entrypoints.http.routes.backtest.get_backtest_attribution_dir", return_value=tmp_path),
            patch("src.entrypoints.http.routes.backtest.list_attribution_files_in_dir") as mock_list,
        ):
            mock_list.return_value = (
                [
                    {
                        "strategy_name": "experimental/range_break_v18",
                        "filename": "attribution_20260112_123000_job-1.json",
                        "created_at": datetime(2026, 1, 12, 12, 30, 0),
                        "size_bytes": 2048,
                        "job_id": "job-1",
                    }
                ],
                1,
            )

            resp = client.get("/api/backtest/attribution-files?strategy=experimental%2Frange_break_v18&limit=10")
            assert resp.status_code == 200
            data = resp.json()
            assert data["total"] == 1
            assert data["files"][0]["strategy_name"] == "experimental/range_break_v18"
            assert data["files"][0]["job_id"] == "job-1"
            mock_list.assert_called_once_with(tmp_path, "experimental/range_break_v18", 10)

    def test_get_attribution_file_content(self, client, tmp_path):
        with (
            patch("src.entrypoints.http.routes.backtest.get_backtest_attribution_dir", return_value=tmp_path),
            patch("src.entrypoints.http.routes.backtest.read_attribution_file") as mock_read,
        ):
            mock_read.return_value = {
                "saved_at": "2026-01-12T12:30:00+00:00",
                "strategy": {"name": "experimental/range_break_v18"},
                "runtime": {"shapley_top_n": 5},
            }

            resp = client.get(
                "/api/backtest/attribution-files/content"
                "?strategy=experimental%2Frange_break_v18&filename=attribution_20260112_123000_job-1.json"
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["strategy_name"] == "experimental/range_break_v18"
            assert data["filename"] == "attribution_20260112_123000_job-1.json"
            assert data["artifact"]["runtime"]["shapley_top_n"] == 5
            mock_read.assert_called_once_with(
                tmp_path,
                "experimental/range_break_v18",
                "attribution_20260112_123000_job-1.json",
            )


class TestHtmlFileEndpoints:
    def test_list_html_files(self, client, tmp_path):
        with (
            patch("src.entrypoints.http.routes.backtest.get_backtest_results_dir", return_value=tmp_path),
            patch("src.entrypoints.http.routes.backtest.list_html_files_in_dir") as mock_list,
        ):
            mock_list.return_value = (
                [
                    {
                        "strategy_name": "s1",
                        "filename": "f1.html",
                        "dataset_name": "d1",
                        "created_at": datetime(2025, 1, 1),
                        "size_bytes": 123,
                    }
                ],
                1,
            )
            resp = client.get("/api/backtest/html-files?strategy=s1&limit=10")
            assert resp.status_code == 200
            data = resp.json()
            assert data["total"] == 1
            assert data["files"][0]["filename"] == "f1.html"

    def test_get_html_file_content_with_metrics(self, client, tmp_path):
        metrics = MagicMock(
            total_return=0.1,
            max_drawdown=-0.05,
            sharpe_ratio=1.2,
            sortino_ratio=1.3,
            calmar_ratio=2.1,
            win_rate=0.6,
            profit_factor=1.8,
            total_trades=10,
        )
        with (
            patch("src.entrypoints.http.routes.backtest.get_backtest_results_dir", return_value=tmp_path),
            patch("src.entrypoints.http.routes.backtest.read_html_file", return_value="<html>body</html>"),
            patch("src.domains.backtest.metrics_extractor.extract_metrics_from_html", return_value=metrics),
        ):
            resp = client.get("/api/backtest/html-files/s1/f1.html")
            assert resp.status_code == 200
            data = resp.json()
            assert data["strategy_name"] == "s1"
            assert data["filename"] == "f1.html"
            assert data["metrics"]["total_trades"] == 10

    def test_rename_html_file_endpoint(self, client, tmp_path):
        with (
            patch("src.entrypoints.http.routes.backtest.get_backtest_results_dir", return_value=tmp_path),
            patch("src.entrypoints.http.routes.backtest.rename_html_file") as mock_rename,
        ):
            resp = client.post(
                "/api/backtest/html-files/s1/f1.html/rename",
                json={"new_filename": "f2.html"},
            )
            assert resp.status_code == 200
            mock_rename.assert_called_once_with(tmp_path, "s1", "f1.html", "f2.html")
            assert resp.json()["new_filename"] == "f2.html"

    def test_delete_html_file_endpoint(self, client, tmp_path):
        with (
            patch("src.entrypoints.http.routes.backtest.get_backtest_results_dir", return_value=tmp_path),
            patch("src.entrypoints.http.routes.backtest.delete_html_file") as mock_delete,
        ):
            resp = client.delete("/api/backtest/html-files/s1/f1.html")
            assert resp.status_code == 200
            mock_delete.assert_called_once_with(tmp_path, "s1", "f1.html")
            assert resp.json()["success"] is True


class TestSSEEndpoints:
    def test_backtest_stream_success(self, client, mock_services):
        _, mock_jm = mock_services

        async def _gen(_job_id):
            yield {"data": "ok"}

        with patch("src.entrypoints.http.routes.backtest.sse_manager") as mock_sse:
            mock_jm.get_job.return_value = _make_job("job-1", JobStatus.RUNNING)
            mock_sse.job_event_generator.return_value = _gen("job-1")
            resp = client.get("/api/backtest/jobs/job-1/stream")
            assert resp.status_code == 200
