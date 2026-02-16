"""
Lab API Tests

Lab APIスキーマバリデーション + エンドポイントのテスト
"""

from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from src.server.app import app
from src.server.schemas.lab import (
    LabEvolveRequest,
    LabEvolveResult,
    LabGenerateRequest,
    LabGenerateResult,
    LabImproveRequest,
    LabImproveResult,
    LabJobResponse,
    LabOptimizeRequest,
    LabOptimizeResult,
    GenerateResultItem,
    EvolutionHistoryItem,
    OptimizeTrialItem,
    ImprovementItem,
)
from src.server.services.job_manager import JobManager, job_manager


@pytest.fixture
def client() -> TestClient:
    """FastAPIテストクライアント"""
    return TestClient(app)


class TestLabGenerateRequestSchema:
    """LabGenerateRequestのバリデーションテスト"""

    def test_default_values(self) -> None:
        """デフォルト値が設定される"""
        req = LabGenerateRequest()
        assert req.count == 100
        assert req.top == 10
        assert req.seed is None
        assert req.save is True
        assert req.direction == "longonly"
        assert req.timeframe == "daily"
        assert req.dataset == "primeExTopix500"
        assert req.entry_filter_only is False
        assert req.allowed_categories is None

    def test_custom_values(self) -> None:
        """カスタム値が設定できる"""
        req = LabGenerateRequest(
            count=500,
            top=20,
            seed=42,
            save=False,
            direction="both",
            timeframe="weekly",
            dataset="custom_dataset",
            entry_filter_only=True,
            allowed_categories=["fundamental", "volume"],
        )
        assert req.count == 500
        assert req.top == 20
        assert req.seed == 42
        assert req.save is False
        assert req.direction == "both"
        assert req.timeframe == "weekly"
        assert req.entry_filter_only is True
        assert req.allowed_categories == ["fundamental", "volume"]

    def test_count_too_small(self) -> None:
        """countが1未満でバリデーションエラー"""
        with pytest.raises(ValidationError):
            LabGenerateRequest(count=0)

    def test_count_too_large(self) -> None:
        """countが10000超でバリデーションエラー"""
        with pytest.raises(ValidationError):
            LabGenerateRequest(count=10001)

    def test_top_too_small(self) -> None:
        """topが1未満でバリデーションエラー"""
        with pytest.raises(ValidationError):
            LabGenerateRequest(top=0)

    def test_top_too_large(self) -> None:
        """topが100超でバリデーションエラー"""
        with pytest.raises(ValidationError):
            LabGenerateRequest(top=101)

    def test_invalid_direction(self) -> None:
        """不正なdirectionでバリデーションエラー"""
        with pytest.raises(ValidationError):
            LabGenerateRequest(direction="invalid")  # type: ignore[arg-type]

    def test_invalid_timeframe(self) -> None:
        """不正なtimeframeでバリデーションエラー"""
        with pytest.raises(ValidationError):
            LabGenerateRequest(timeframe="monthly")  # type: ignore[arg-type]

    def test_invalid_allowed_category(self) -> None:
        """不正なallowed_categoriesでバリデーションエラー"""
        with pytest.raises(ValidationError):
            LabGenerateRequest(allowed_categories=["invalid"])  # type: ignore[list-item]


class TestLabEvolveRequestSchema:
    """LabEvolveRequestのバリデーションテスト"""

    def test_valid_request(self) -> None:
        """有効なリクエストが作成できる"""
        req = LabEvolveRequest(strategy_name="reference/sma_cross")
        assert req.strategy_name == "reference/sma_cross"
        assert req.generations == 20
        assert req.population == 50
        assert req.save is True
        assert req.entry_filter_only is False
        assert req.target_scope == "both"
        assert req.allowed_categories is None

    def test_empty_strategy_name(self) -> None:
        """空の戦略名でバリデーションエラー"""
        with pytest.raises(ValidationError):
            LabEvolveRequest(strategy_name="")

    def test_generations_bounds(self) -> None:
        """世代数の範囲バリデーション"""
        with pytest.raises(ValidationError):
            LabEvolveRequest(strategy_name="test", generations=0)
        with pytest.raises(ValidationError):
            LabEvolveRequest(strategy_name="test", generations=101)

    def test_population_bounds(self) -> None:
        """個体数の範囲バリデーション"""
        with pytest.raises(ValidationError):
            LabEvolveRequest(strategy_name="test", population=9)
        with pytest.raises(ValidationError):
            LabEvolveRequest(strategy_name="test", population=501)

    def test_allowed_categories(self) -> None:
        """allowed_categories が設定できる"""
        req = LabEvolveRequest(
            strategy_name="test",
            entry_filter_only=True,
            allowed_categories=["fundamental"],
        )
        assert req.entry_filter_only is True
        assert req.target_scope == "entry_filter_only"
        assert req.allowed_categories == ["fundamental"]

    def test_target_scope_exit_only(self) -> None:
        req = LabEvolveRequest(
            strategy_name="test",
            target_scope="exit_trigger_only",
        )
        assert req.entry_filter_only is False
        assert req.target_scope == "exit_trigger_only"

    def test_invalid_allowed_category(self) -> None:
        """不正なallowed_categoriesでバリデーションエラー"""
        with pytest.raises(ValidationError):
            LabEvolveRequest(strategy_name="test", allowed_categories=["invalid"])  # type: ignore[list-item]

    def test_target_scope_conflict(self) -> None:
        with pytest.raises(ValidationError):
            LabEvolveRequest(
                strategy_name="test",
                target_scope="exit_trigger_only",
                entry_filter_only=True,
            )


class TestLabOptimizeRequestSchema:
    """LabOptimizeRequestのバリデーションテスト"""

    def test_valid_request(self) -> None:
        """有効なリクエストが作成できる"""
        req = LabOptimizeRequest(strategy_name="test")
        assert req.strategy_name == "test"
        assert req.trials == 100
        assert req.sampler == "tpe"
        assert req.save is True
        assert req.entry_filter_only is False
        assert req.target_scope == "both"
        assert req.allowed_categories is None
        assert req.scoring_weights is None

    def test_all_samplers(self) -> None:
        """全サンプラーが指定できる"""
        for sampler in ("tpe", "random", "cmaes"):
            req = LabOptimizeRequest(
                strategy_name="test",
                sampler=sampler,  # type: ignore[arg-type]
            )
            assert req.sampler == sampler

    def test_invalid_sampler(self) -> None:
        """不正なsamplerでバリデーションエラー"""
        with pytest.raises(ValidationError):
            LabOptimizeRequest(
                strategy_name="test",
                sampler="invalid",  # type: ignore[arg-type]
            )

    def test_trials_bounds(self) -> None:
        """試行回数の範囲バリデーション"""
        with pytest.raises(ValidationError):
            LabOptimizeRequest(strategy_name="test", trials=9)
        with pytest.raises(ValidationError):
            LabOptimizeRequest(strategy_name="test", trials=1001)

    def test_scoring_weights(self) -> None:
        """スコアリング重みが設定できる"""
        req = LabOptimizeRequest(
            strategy_name="test",
            scoring_weights={"sharpe_ratio": 0.6, "calmar_ratio": 0.4},
        )
        assert req.scoring_weights == {"sharpe_ratio": 0.6, "calmar_ratio": 0.4}

    def test_allowed_categories(self) -> None:
        """allowed_categories が設定できる"""
        req = LabOptimizeRequest(
            strategy_name="test",
            entry_filter_only=True,
            allowed_categories=["fundamental"],
        )
        assert req.entry_filter_only is True
        assert req.target_scope == "entry_filter_only"
        assert req.allowed_categories == ["fundamental"]

    def test_target_scope_exit_only(self) -> None:
        req = LabOptimizeRequest(
            strategy_name="test",
            target_scope="exit_trigger_only",
        )
        assert req.entry_filter_only is False
        assert req.target_scope == "exit_trigger_only"

    def test_invalid_allowed_category(self) -> None:
        """不正なallowed_categoriesでバリデーションエラー"""
        with pytest.raises(ValidationError):
            LabOptimizeRequest(strategy_name="test", allowed_categories=["invalid"])  # type: ignore[list-item]

    def test_target_scope_conflict(self) -> None:
        with pytest.raises(ValidationError):
            LabOptimizeRequest(
                strategy_name="test",
                target_scope="exit_trigger_only",
                entry_filter_only=True,
            )


class TestLabImproveRequestSchema:
    """LabImproveRequestのバリデーションテスト"""

    def test_valid_request(self) -> None:
        """有効なリクエストが作成できる"""
        req = LabImproveRequest(strategy_name="test")
        assert req.strategy_name == "test"
        assert req.auto_apply is True
        assert req.entry_filter_only is False
        assert req.allowed_categories is None

    def test_empty_strategy_name(self) -> None:
        """空の戦略名でバリデーションエラー"""
        with pytest.raises(ValidationError):
            LabImproveRequest(strategy_name="")

    def test_allowed_categories(self) -> None:
        """allowed_categories が設定できる"""
        req = LabImproveRequest(
            strategy_name="test",
            entry_filter_only=True,
            allowed_categories=["fundamental"],
        )
        assert req.entry_filter_only is True
        assert req.allowed_categories == ["fundamental"]


class TestLabResultModels:
    """Lab結果モデルのテスト"""

    def test_generate_result_item(self) -> None:
        """GenerateResultItemが作成できる"""
        item = GenerateResultItem(
            strategy_id="gen-001",
            score=1.5,
            sharpe_ratio=1.2,
            calmar_ratio=0.8,
            total_return=0.15,
        )
        assert item.strategy_id == "gen-001"
        assert item.score == 1.5

    def test_evolution_history_item(self) -> None:
        """EvolutionHistoryItemが作成できる"""
        item = EvolutionHistoryItem(
            generation=5,
            best_score=2.0,
            avg_score=1.5,
            worst_score=0.5,
        )
        assert item.generation == 5
        assert item.best_score == 2.0

    def test_optimize_trial_item(self) -> None:
        """OptimizeTrialItemが作成できる"""
        item = OptimizeTrialItem(
            trial=10,
            score=1.8,
            params={"entry_breakout_period": 100},
        )
        assert item.trial == 10
        assert item.params["entry_breakout_period"] == 100

    def test_improvement_item(self) -> None:
        """ImprovementItemが作成できる"""
        item = ImprovementItem(
            improvement_type="add_signal",
            target="entry",
            signal_name="rsi_threshold",
            changes={"threshold": 30},
            reason="RSIフィルタで過売り判定を追加",
            expected_impact="ドローダウン改善",
        )
        assert item.improvement_type == "add_signal"
        assert item.signal_name == "rsi_threshold"

    def test_generate_result(self) -> None:
        """LabGenerateResultが作成できる"""
        result = LabGenerateResult(
            results=[],
            total_generated=100,
            saved_strategy_path="/tmp/test.yaml",
        )
        assert result.lab_type == "generate"
        assert result.total_generated == 100

    def test_evolve_result(self) -> None:
        """LabEvolveResultが作成できる"""
        result = LabEvolveResult(
            best_strategy_id="evo-001",
            best_score=2.5,
            history=[],
        )
        assert result.lab_type == "evolve"
        assert result.best_strategy_id == "evo-001"

    def test_optimize_result(self) -> None:
        """LabOptimizeResultが作成できる"""
        result = LabOptimizeResult(
            best_score=3.0,
            best_params={"period": 100},
            total_trials=50,
            history=[],
        )
        assert result.lab_type == "optimize"
        assert result.best_score == 3.0

    def test_improve_result(self) -> None:
        """LabImproveResultが作成できる"""
        result = LabImproveResult(
            strategy_name="test",
            max_drawdown=-0.15,
            max_drawdown_duration_days=30,
            suggested_improvements=["RSIフィルタ追加"],
            improvements=[],
        )
        assert result.lab_type == "improve"
        assert result.strategy_name == "test"


class TestLabJobResponse:
    """LabJobResponseのテスト"""

    def test_response_with_generate_result(self) -> None:
        """generate結果付きレスポンスが構築できる"""
        from datetime import datetime

        result = LabGenerateResult(
            results=[],
            total_generated=100,
        )
        resp = LabJobResponse(
            job_id="test-123",
            status="pending",
            created_at=datetime.now(),
            lab_type="generate",
            strategy_name="generate",
            result_data=result,
        )
        assert resp.lab_type == "generate"
        assert resp.result_data is not None
        assert resp.result_data.lab_type == "generate"

    def test_response_without_result(self) -> None:
        """結果なしレスポンスが構築できる"""
        from datetime import datetime

        resp = LabJobResponse(
            job_id="test-456",
            status="running",
            created_at=datetime.now(),
            lab_type="evolve",
            strategy_name="test_strategy",
        )
        assert resp.result_data is None


class TestLabEndpoints:
    """Lab APIエンドポイントのテスト"""

    def test_get_nonexistent_job(self, client: TestClient) -> None:
        """存在しないジョブIDで404"""
        response = client.get("/api/lab/jobs/nonexistent-id")
        assert response.status_code == 404

    def test_get_non_lab_job(self, client: TestClient) -> None:
        """Labジョブでないジョブで400"""
        job_id = job_manager.create_job("test_strategy", job_type="backtest")
        response = client.get(f"/api/lab/jobs/{job_id}")
        assert response.status_code == 400
        assert "Labジョブではありません" in response.json()["message"]

    def test_get_lab_job(self, client: TestClient) -> None:
        """Labジョブが取得できる"""
        job_id = job_manager.create_job("test_strategy", job_type="lab_generate")
        response = client.get(f"/api/lab/jobs/{job_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["job_id"] == job_id
        assert data["lab_type"] == "generate"
        assert data["strategy_name"] == "test_strategy"
        assert data["result_data"] is None

    def test_list_lab_jobs_filters_non_lab_jobs(self, client: TestClient) -> None:
        """一覧APIはLabジョブのみ返す"""
        lab_job_id = job_manager.create_job("list_target", job_type="lab_generate")
        non_lab_job_id = job_manager.create_job("list_target", job_type="backtest")

        response = client.get("/api/lab/jobs?limit=50")
        assert response.status_code == 200
        data = response.json()
        listed_ids = {item["job_id"] for item in data}

        assert lab_job_id in listed_ids
        assert non_lab_job_id not in listed_ids

    def test_list_lab_jobs_respects_limit(self, client: TestClient) -> None:
        """一覧APIはlimit件数を守る"""
        older_lab_id = job_manager.create_job("limit_old", job_type="lab_generate")
        newer_lab_id = job_manager.create_job("limit_new", job_type="lab_optimize")

        response = client.get("/api/lab/jobs?limit=1")
        assert response.status_code == 200
        data = response.json()

        assert len(data) == 1
        assert data[0]["job_id"] == newer_lab_id
        assert data[0]["job_id"] != older_lab_id

    def test_get_lab_job_with_result(self, client: TestClient) -> None:
        """結果付きLabジョブが取得できる"""
        job_id = job_manager.create_job("test_strategy", job_type="lab_generate")
        job = job_manager.get_job(job_id)
        assert job is not None
        job.raw_result = {
            "lab_type": "generate",
            "results": [],
            "total_generated": 50,
            "saved_strategy_path": None,
        }
        response = client.get(f"/api/lab/jobs/{job_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["result_data"] is not None
        assert data["result_data"]["lab_type"] == "generate"
        assert data["result_data"]["total_generated"] == 50

    def test_cancel_nonexistent_job(self, client: TestClient) -> None:
        """存在しないジョブIDのキャンセルで404"""
        response = client.post("/api/lab/jobs/nonexistent-id/cancel")
        assert response.status_code == 404

    def test_cancel_lab_job(self, client: TestClient) -> None:
        """Labジョブがキャンセルできる"""
        job_id = job_manager.create_job("test_strategy", job_type="lab_evolve")
        response = client.post(f"/api/lab/jobs/{job_id}/cancel")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "cancelled"

    def test_stream_nonexistent_job(self, client: TestClient) -> None:
        """存在しないジョブIDのSSEストリームで404"""
        response = client.get("/api/lab/jobs/nonexistent-id/stream")
        assert response.status_code == 404

    def test_all_lab_job_types(self, client: TestClient) -> None:
        """全Labジョブタイプが取得できる"""
        type_map = {
            "lab_generate": "generate",
            "lab_evolve": "evolve",
            "lab_optimize": "optimize",
            "lab_improve": "improve",
        }
        for job_type, expected_lab_type in type_map.items():
            job_id = job_manager.create_job("test_strategy", job_type=job_type)
            response = client.get(f"/api/lab/jobs/{job_id}")
            assert response.status_code == 200
            data = response.json()
            assert data["lab_type"] == expected_lab_type

    def test_evolve_result_parsing(self, client: TestClient) -> None:
        """evolve結果がパースできる"""
        job_id = job_manager.create_job("test_strategy", job_type="lab_evolve")
        job = job_manager.get_job(job_id)
        assert job is not None
        job.raw_result = {
            "lab_type": "evolve",
            "best_strategy_id": "evo-001",
            "best_score": 2.5,
            "history": [
                {"generation": 0, "best_score": 1.0, "avg_score": 0.5, "worst_score": 0.1},
                {"generation": 1, "best_score": 2.5, "avg_score": 1.5, "worst_score": 0.8},
            ],
            "saved_strategy_path": "/tmp/evo.yaml",
            "saved_history_path": "/tmp/evo_history.yaml",
        }
        response = client.get(f"/api/lab/jobs/{job_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["result_data"]["lab_type"] == "evolve"
        assert data["result_data"]["best_score"] == 2.5
        assert len(data["result_data"]["history"]) == 2

    def test_optimize_result_parsing(self, client: TestClient) -> None:
        """optimize結果がパースできる"""
        job_id = job_manager.create_job("test_strategy", job_type="lab_optimize")
        job = job_manager.get_job(job_id)
        assert job is not None
        job.raw_result = {
            "lab_type": "optimize",
            "best_score": 3.0,
            "best_params": {"entry_breakout_period": 150},
            "total_trials": 100,
            "history": [
                {"trial": 0, "score": 1.0, "params": {}},
                {"trial": 1, "score": 3.0, "params": {"entry_breakout_period": 150}},
            ],
            "saved_strategy_path": "/tmp/opt.yaml",
            "saved_history_path": "/tmp/opt_history.yaml",
        }
        response = client.get(f"/api/lab/jobs/{job_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["result_data"]["lab_type"] == "optimize"
        assert data["result_data"]["best_score"] == 3.0
        assert data["result_data"]["total_trials"] == 100

    def test_improve_result_parsing(self, client: TestClient) -> None:
        """improve結果がパースできる"""
        job_id = job_manager.create_job("test_strategy", job_type="lab_improve")
        job = job_manager.get_job(job_id)
        assert job is not None
        job.raw_result = {
            "lab_type": "improve",
            "strategy_name": "test_strategy",
            "max_drawdown": -0.15,
            "max_drawdown_duration_days": 30,
            "suggested_improvements": ["RSIフィルタ追加"],
            "improvements": [
                {
                    "improvement_type": "add_signal",
                    "target": "entry",
                    "signal_name": "rsi_threshold",
                    "changes": {"threshold": 30},
                    "reason": "過売り判定",
                    "expected_impact": "DD改善",
                }
            ],
            "saved_strategy_path": "/tmp/improved.yaml",
        }
        response = client.get(f"/api/lab/jobs/{job_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["result_data"]["lab_type"] == "improve"
        assert len(data["result_data"]["improvements"]) == 1


class TestOptunaEndpointsRemoved:
    """旧Optunaエンドポイントが削除されていることの確認テスト"""

    def test_optuna_endpoint_removed(self, client: TestClient) -> None:
        """旧 /api/optimize/optuna エンドポイントが存在しない"""
        response = client.post(
            "/api/optimize/optuna",
            json={"strategy_name": "test", "sampler": "tpe", "n_trials": 50},
        )
        assert response.status_code in (404, 405)

    def test_optuna_history_endpoint_removed(self, client: TestClient) -> None:
        """旧 /api/optimize/optuna/history/{job_id} エンドポイントが存在しない"""
        response = client.get("/api/optimize/optuna/history/test-id")
        assert response.status_code in (404, 405)


class TestSSEEndpoints:
    """SSEストリームエンドポイントのテスト（既存テストの移行）"""

    def test_backtest_stream_nonexistent_job(self, client: TestClient) -> None:
        """存在しないジョブIDで404"""
        response = client.get("/api/backtest/jobs/nonexistent-id/stream")
        assert response.status_code == 404

    def test_optimize_stream_nonexistent_job(self, client: TestClient) -> None:
        """存在しないジョブIDで404"""
        response = client.get("/api/optimize/jobs/nonexistent-id/stream")
        assert response.status_code == 404

    def test_lab_stream_nonexistent_job(self, client: TestClient) -> None:
        """Lab SSEストリームで存在しないジョブIDで404"""
        response = client.get("/api/lab/jobs/nonexistent-id/stream")
        assert response.status_code == 404


class TestLabSubmitEndpoints:
    """Lab APIサブミットエンドポイントのテスト（mockベース）"""

    def test_submit_generate(self, client: TestClient) -> None:
        """generate サブミットが成功する"""
        job_id = job_manager.create_job("generate(n=50,top=5)", job_type="lab_generate")
        with patch(
            "src.server.routes.lab.lab_service.submit_generate",
            new_callable=AsyncMock,
            return_value=job_id,
        ) as mock_submit:
            response = client.post(
                "/api/lab/generate",
                json={
                    "count": 50,
                    "top": 5,
                    "entry_filter_only": True,
                    "allowed_categories": ["fundamental"],
                },
            )
            assert response.status_code == 200
            data = response.json()
            assert data["lab_type"] == "generate"
            mock_submit.assert_awaited_once_with(
                count=50,
                top=5,
                seed=None,
                save=True,
                direction="longonly",
                timeframe="daily",
                dataset="primeExTopix500",
                entry_filter_only=True,
                allowed_categories=["fundamental"],
            )

    def test_submit_evolve(self, client: TestClient) -> None:
        """evolve サブミットが成功する"""
        job_id = job_manager.create_job("test_strategy", job_type="lab_evolve")
        with patch(
            "src.server.routes.lab.lab_service.submit_evolve",
            new_callable=AsyncMock,
            return_value=job_id,
        ) as mock_submit:
            response = client.post(
                "/api/lab/evolve",
                json={
                    "strategy_name": "test_strategy",
                    "generations": 10,
                    "population": 30,
                    "structure_mode": "random_add",
                    "random_add_entry_signals": 2,
                    "random_add_exit_signals": 0,
                    "seed": 123,
                    "entry_filter_only": True,
                    "target_scope": "entry_filter_only",
                    "allowed_categories": ["fundamental"],
                },
            )
            assert response.status_code == 200
            data = response.json()
            assert data["lab_type"] == "evolve"
            assert data["strategy_name"] == "test_strategy"
            mock_submit.assert_awaited_once_with(
                strategy_name="test_strategy",
                generations=10,
                population=30,
                structure_mode="random_add",
                random_add_entry_signals=2,
                random_add_exit_signals=0,
                seed=123,
                save=True,
                entry_filter_only=True,
                target_scope="entry_filter_only",
                allowed_categories=["fundamental"],
            )

    def test_submit_optimize(self, client: TestClient) -> None:
        """optimize サブミットが成功する"""
        job_id = job_manager.create_job("test_strategy", job_type="lab_optimize")
        with patch(
            "src.server.routes.lab.lab_service.submit_optimize",
            new_callable=AsyncMock,
            return_value=job_id,
        ) as mock_submit:
            response = client.post(
                "/api/lab/optimize",
                json={
                    "strategy_name": "test_strategy",
                    "trials": 50,
                    "sampler": "cmaes",
                    "structure_mode": "random_add",
                    "random_add_entry_signals": 1,
                    "random_add_exit_signals": 1,
                    "seed": 7,
                    "entry_filter_only": True,
                    "target_scope": "entry_filter_only",
                    "allowed_categories": ["fundamental"],
                },
            )
            assert response.status_code == 200
            data = response.json()
            assert data["lab_type"] == "optimize"
            mock_submit.assert_awaited_once_with(
                strategy_name="test_strategy",
                trials=50,
                sampler="cmaes",
                structure_mode="random_add",
                random_add_entry_signals=1,
                random_add_exit_signals=1,
                seed=7,
                save=True,
                entry_filter_only=True,
                target_scope="entry_filter_only",
                allowed_categories=["fundamental"],
                scoring_weights=None,
            )

    def test_submit_improve(self, client: TestClient) -> None:
        """improve サブミットが成功する"""
        job_id = job_manager.create_job("test_strategy", job_type="lab_improve")
        with patch(
            "src.server.routes.lab.lab_service.submit_improve",
            new_callable=AsyncMock,
            return_value=job_id,
        ) as mock_submit:
            response = client.post(
                "/api/lab/improve",
                json={
                    "strategy_name": "test_strategy",
                    "auto_apply": False,
                    "entry_filter_only": True,
                    "allowed_categories": ["fundamental"],
                },
            )
            assert response.status_code == 200
            data = response.json()
            assert data["lab_type"] == "improve"
            mock_submit.assert_awaited_once_with(
                strategy_name="test_strategy",
                auto_apply=False,
                entry_filter_only=True,
                allowed_categories=["fundamental"],
            )

    def test_submit_generate_error(self, client: TestClient) -> None:
        """generate サブミットが例外で500を返す"""
        with patch(
            "src.server.routes.lab.lab_service.submit_generate",
            new_callable=AsyncMock,
            side_effect=RuntimeError("テストエラー"),
        ):
            response = client.post(
                "/api/lab/generate",
                json={},
            )
            assert response.status_code == 500
            assert "テストエラー" in response.json()["message"]

    def test_submit_evolve_error(self, client: TestClient) -> None:
        """evolve サブミットが例外で500を返す"""
        with patch(
            "src.server.routes.lab.lab_service.submit_evolve",
            new_callable=AsyncMock,
            side_effect=RuntimeError("evolveエラー"),
        ):
            response = client.post(
                "/api/lab/evolve",
                json={"strategy_name": "test"},
            )
            assert response.status_code == 500

    def test_submit_optimize_error(self, client: TestClient) -> None:
        """optimize サブミットが例外で500を返す"""
        with patch(
            "src.server.routes.lab.lab_service.submit_optimize",
            new_callable=AsyncMock,
            side_effect=RuntimeError("optimizeエラー"),
        ):
            response = client.post(
                "/api/lab/optimize",
                json={"strategy_name": "test"},
            )
            assert response.status_code == 500

    def test_submit_improve_error(self, client: TestClient) -> None:
        """improve サブミットが例外で500を返す"""
        with patch(
            "src.server.routes.lab.lab_service.submit_improve",
            new_callable=AsyncMock,
            side_effect=RuntimeError("improveエラー"),
        ):
            response = client.post(
                "/api/lab/improve",
                json={"strategy_name": "test"},
            )
            assert response.status_code == 500


class TestLabEdgeCases:
    """エッジケーステスト"""

    def test_cancel_completed_job(self, client: TestClient) -> None:
        """完了済みジョブのキャンセルで409"""
        job_id = job_manager.create_job("test", job_type="lab_generate")
        job = job_manager.get_job(job_id)
        assert job is not None
        from src.server.schemas.backtest import JobStatus

        job.status = JobStatus.COMPLETED
        job.completed_at = job.created_at

        response = client.post(f"/api/lab/jobs/{job_id}/cancel")
        assert response.status_code == 409

    def test_cancel_non_lab_job(self, client: TestClient) -> None:
        """Labジョブでないジョブのキャンセルで400"""
        job_id = job_manager.create_job("test", job_type="backtest")
        response = client.post(f"/api/lab/jobs/{job_id}/cancel")
        assert response.status_code == 400

    def test_stream_non_lab_job(self, client: TestClient) -> None:
        """Labジョブでないジョブのstreamで400"""
        job_id = job_manager.create_job("test", job_type="backtest")
        response = client.get(f"/api/lab/jobs/{job_id}/stream")
        assert response.status_code == 400

    def test_invalid_raw_result_graceful(self, client: TestClient) -> None:
        """不正なraw_resultでもクラッシュしない"""
        job_id = job_manager.create_job("test", job_type="lab_generate")
        job = job_manager.get_job(job_id)
        assert job is not None
        job.raw_result = {"lab_type": "generate", "invalid_field": True}

        response = client.get(f"/api/lab/jobs/{job_id}")
        assert response.status_code == 200
        data = response.json()
        # パースに失敗してもresult_dataはNoneになる
        assert data["result_data"] is None

    def test_generate_request_all_directions(self) -> None:
        """全direction値がバリデーションを通る"""
        for direction in ("longonly", "shortonly", "both"):
            req = LabGenerateRequest(direction=direction)  # type: ignore[arg-type]
            assert req.direction == direction

    def test_lab_job_response_serialization(self) -> None:
        """LabJobResponseのJSON直列化テスト"""
        from datetime import datetime

        resp = LabJobResponse(
            job_id="test-ser",
            status="pending",
            created_at=datetime(2025, 1, 1),
            lab_type="optimize",
            strategy_name="test",
        )
        data = resp.model_dump()
        assert data["lab_type"] == "optimize"
        assert data["result_data"] is None
        assert data["status"] == "pending"


class TestLabServiceUnit:
    """LabServiceの単体テスト"""

    def test_service_initialization(self) -> None:
        """サービスが初期化できる"""
        from src.server.services.lab_service import LabService

        service = LabService(max_workers=1)
        assert service._executor is not None
        assert service._manager is not None
        service._executor.shutdown(wait=False)

    def test_service_custom_manager(self) -> None:
        """カスタムManagerでサービスを初期化できる"""
        from src.server.services.lab_service import LabService

        manager = JobManager()
        service = LabService(manager=manager, max_workers=1)
        assert service._manager is manager
        service._executor.shutdown(wait=False)


@pytest.mark.asyncio
class TestLabServiceAsync:
    """LabServiceの非同期テスト"""

    async def test_submit_generate_creates_job(self) -> None:
        """submit_generateがジョブを作成する"""
        from src.server.services.lab_service import LabService

        manager = JobManager()
        service = LabService(manager=manager, max_workers=1)

        with patch.object(service, "_run_job", new_callable=AsyncMock):
            job_id = await service.submit_generate(count=10, top=3)
            job = manager.get_job(job_id)
            assert job is not None
            assert job.job_type == "lab_generate"
            assert "generate" in job.strategy_name
        service._executor.shutdown(wait=False)

    async def test_submit_evolve_creates_job(self) -> None:
        """submit_evolveがジョブを作成する"""
        from src.server.services.lab_service import LabService

        manager = JobManager()
        service = LabService(manager=manager, max_workers=1)

        with patch.object(service, "_run_job", new_callable=AsyncMock):
            job_id = await service.submit_evolve(strategy_name="test_strat")
            job = manager.get_job(job_id)
            assert job is not None
            assert job.job_type == "lab_evolve"
            assert job.strategy_name == "test_strat"
        service._executor.shutdown(wait=False)

    async def test_submit_optimize_creates_job(self) -> None:
        """submit_optimizeがジョブを作成する"""
        from src.server.services.lab_service import LabService

        manager = JobManager()
        service = LabService(manager=manager, max_workers=1)

        with patch.object(service, "_run_optimize", new_callable=AsyncMock):
            job_id = await service.submit_optimize(
                strategy_name="test_strat", trials=50, sampler="random"
            )
            job = manager.get_job(job_id)
            assert job is not None
            assert job.job_type == "lab_optimize"
        service._executor.shutdown(wait=False)

    async def test_submit_improve_creates_job(self) -> None:
        """submit_improveがジョブを作成する"""
        from src.server.services.lab_service import LabService

        manager = JobManager()
        service = LabService(manager=manager, max_workers=1)

        with patch.object(service, "_run_job", new_callable=AsyncMock):
            job_id = await service.submit_improve(strategy_name="test_strat", auto_apply=False)
            job = manager.get_job(job_id)
            assert job is not None
            assert job.job_type == "lab_improve"
        service._executor.shutdown(wait=False)

    async def _run_job_test(
        self,
        job_type: str,
        lab_type: str,
        sync_method: str,
        sync_args: tuple[object, ...],
        mock_result: dict[str, object] | None = None,
        side_effect: type[BaseException] | Exception | None = None,
    ) -> tuple["LabService", str]:  # type: ignore[name-defined]  # noqa: F821
        """_run_job テストの共通ヘルパー"""
        from src.server.services.lab_service import LabService

        manager = JobManager()
        service = LabService(manager=manager, max_workers=1)
        job_id = manager.create_job("test", job_type=job_type)

        patch_kwargs: dict[str, object] = {}
        if side_effect is not None:
            patch_kwargs["side_effect"] = side_effect
        else:
            patch_kwargs["return_value"] = mock_result

        with patch.object(service, sync_method, **patch_kwargs):  # type: ignore[arg-type]
            await service._run_job(
                job_id=job_id,
                lab_type=lab_type,
                start_message="開始...",
                complete_message="完了",
                cancel_message="キャンセル",
                fail_message="失敗",
                log_detail="test",
                sync_fn=getattr(service, sync_method),
                sync_args=sync_args,
            )

        return service, job_id

    async def test_run_generate_success(self) -> None:
        """_run_jobが成功時にCOMPLETEDになる（generate）"""
        mock_result = {
            "lab_type": "generate",
            "results": [],
            "total_generated": 10,
            "saved_strategy_path": None,
        }
        service, job_id = await self._run_job_test(
            job_type="lab_generate",
            lab_type="generate",
            sync_method="_execute_generate_sync",
            sync_args=(10, 3, None, False, "longonly", "daily", "test"),
            mock_result=mock_result,
        )
        job = service._manager.get_job(job_id)
        assert job is not None
        from src.server.schemas.backtest import JobStatus

        assert job.status == JobStatus.COMPLETED
        assert job.raw_result == mock_result
        service._executor.shutdown(wait=False)

    async def test_run_evolve_success(self) -> None:
        """_run_jobが成功時にCOMPLETEDになる（evolve）"""
        mock_result = {
            "lab_type": "evolve",
            "best_strategy_id": "evo-001",
            "best_score": 2.0,
            "history": [],
            "saved_strategy_path": None,
            "saved_history_path": None,
        }
        service, job_id = await self._run_job_test(
            job_type="lab_evolve",
            lab_type="evolve",
            sync_method="_execute_evolve_sync",
            sync_args=("test_strat", 10, 30, False),
            mock_result=mock_result,
        )
        job = service._manager.get_job(job_id)
        assert job is not None
        from src.server.schemas.backtest import JobStatus

        assert job.status == JobStatus.COMPLETED
        assert job.raw_result == mock_result
        service._executor.shutdown(wait=False)

    async def test_run_optimize_success(self) -> None:
        """_run_optimizeが成功時にCOMPLETEDになる"""
        from src.server.services.lab_service import LabService

        manager = JobManager()
        service = LabService(manager=manager, max_workers=1)

        job_id = manager.create_job("test_strat", job_type="lab_optimize")

        mock_result = {
            "lab_type": "optimize",
            "best_score": 3.0,
            "best_params": {},
            "total_trials": 50,
            "history": [],
            "saved_strategy_path": None,
            "saved_history_path": None,
        }
        with patch.object(service, "_execute_optimize_sync", return_value=mock_result):
            await service._run_optimize(
                job_id,
                "test_strat",
                50,
                "tpe",
                "params_only",
                1,
                1,
                None,
                False,
                False,
                [],
                None,
            )

        job = manager.get_job(job_id)
        assert job is not None
        from src.server.schemas.backtest import JobStatus

        assert job.status == JobStatus.COMPLETED
        service._executor.shutdown(wait=False)

    async def test_run_generate_success_without_job_record_for_raw_result(self) -> None:
        """_run_jobでget_jobがNoneでも完了できる"""
        from unittest.mock import MagicMock

        from src.server.services.lab_service import LabService

        manager = JobManager()
        service = LabService(manager=manager, max_workers=1)
        job_id = manager.create_job("test", job_type="lab_generate")
        sync_fn = MagicMock(
            return_value={
                "lab_type": "generate",
                "results": [],
                "total_generated": 1,
                "saved_strategy_path": None,
            }
        )

        with patch.object(manager, "get_job", return_value=None):
            await service._run_job(
                job_id=job_id,
                lab_type="generate",
                start_message="開始...",
                complete_message="完了",
                cancel_message="キャンセル",
                fail_message="失敗",
                log_detail="test",
                sync_fn=sync_fn,
                sync_args=(),
            )

        job = manager.get_job(job_id)
        assert job is not None
        from src.server.schemas.backtest import JobStatus

        assert job.status == JobStatus.COMPLETED
        assert job.raw_result is None
        service._executor.shutdown(wait=False)

    async def test_run_optimize_success_without_job_record_for_raw_result(self) -> None:
        """_run_optimizeでget_jobがNoneでも完了できる"""
        from src.server.services.lab_service import LabService

        manager = JobManager()
        service = LabService(manager=manager, max_workers=1)

        job_id = manager.create_job("test_strat", job_type="lab_optimize")
        mock_result = {
            "lab_type": "optimize",
            "best_score": 0.0,
            "best_params": {},
            "total_trials": 0,
            "history": [],
            "saved_strategy_path": None,
            "saved_history_path": None,
        }

        with (
            patch.object(service, "_execute_optimize_sync", return_value=mock_result),
            patch.object(manager, "get_job", return_value=None),
        ):
            await service._run_optimize(
                job_id,
                "test_strat",
                10,
                "tpe",
                "params_only",
                1,
                1,
                None,
                False,
                False,
                [],
                None,
            )

        job = manager.get_job(job_id)
        assert job is not None
        from src.server.schemas.backtest import JobStatus

        assert job.status == JobStatus.COMPLETED
        assert job.raw_result is None
        service._executor.shutdown(wait=False)

    async def test_run_improve_success(self) -> None:
        """_run_jobが成功時にCOMPLETEDになる（improve）"""
        mock_result = {
            "lab_type": "improve",
            "strategy_name": "test_strat",
            "max_drawdown": -0.1,
            "max_drawdown_duration_days": 10,
            "suggested_improvements": [],
            "improvements": [],
            "saved_strategy_path": None,
        }
        service, job_id = await self._run_job_test(
            job_type="lab_improve",
            lab_type="improve",
            sync_method="_execute_improve_sync",
            sync_args=("test_strat", False),
            mock_result=mock_result,
        )
        job = service._manager.get_job(job_id)
        assert job is not None
        from src.server.schemas.backtest import JobStatus

        assert job.status == JobStatus.COMPLETED
        service._executor.shutdown(wait=False)

    async def test_run_generate_failure(self) -> None:
        """_run_jobが例外時にFAILEDになる（generate）"""
        service, job_id = await self._run_job_test(
            job_type="lab_generate",
            lab_type="generate",
            sync_method="_execute_generate_sync",
            sync_args=(10, 3, None, False, "longonly", "daily", "test"),
            side_effect=RuntimeError("テストエラー"),
        )
        job = service._manager.get_job(job_id)
        assert job is not None
        from src.server.schemas.backtest import JobStatus

        assert job.status == JobStatus.FAILED
        assert job.error == "テストエラー"
        service._executor.shutdown(wait=False)

    async def test_run_evolve_failure(self) -> None:
        """_run_jobが例外時にFAILEDになる（evolve）"""
        service, job_id = await self._run_job_test(
            job_type="lab_evolve",
            lab_type="evolve",
            sync_method="_execute_evolve_sync",
            sync_args=("test", 10, 30, False),
            side_effect=ValueError("evolveエラー"),
        )
        job = service._manager.get_job(job_id)
        assert job is not None
        from src.server.schemas.backtest import JobStatus

        assert job.status == JobStatus.FAILED
        service._executor.shutdown(wait=False)

    async def test_run_optimize_failure(self) -> None:
        """_run_optimizeが例外時にFAILEDになる"""
        from src.server.services.lab_service import LabService

        manager = JobManager()
        service = LabService(manager=manager, max_workers=1)

        job_id = manager.create_job("test", job_type="lab_optimize")

        with patch.object(
            service, "_execute_optimize_sync", side_effect=RuntimeError("optエラー")
        ):
            await service._run_optimize(
                job_id,
                "test",
                50,
                "tpe",
                "params_only",
                1,
                1,
                None,
                False,
                False,
                [],
                None,
            )

        job = manager.get_job(job_id)
        assert job is not None
        from src.server.schemas.backtest import JobStatus

        assert job.status == JobStatus.FAILED
        service._executor.shutdown(wait=False)

    async def test_run_improve_failure(self) -> None:
        """_run_jobが例外時にFAILEDになる（improve）"""
        service, job_id = await self._run_job_test(
            job_type="lab_improve",
            lab_type="improve",
            sync_method="_execute_improve_sync",
            sync_args=("test", False),
            side_effect=RuntimeError("impエラー"),
        )
        job = service._manager.get_job(job_id)
        assert job is not None
        from src.server.schemas.backtest import JobStatus

        assert job.status == JobStatus.FAILED
        service._executor.shutdown(wait=False)

    async def test_run_generate_cancelled(self) -> None:
        """_run_jobがキャンセル時にCANCELLEDになる（generate）"""
        import asyncio

        service, job_id = await self._run_job_test(
            job_type="lab_generate",
            lab_type="generate",
            sync_method="_execute_generate_sync",
            sync_args=(10, 3, None, False, "longonly", "daily", "test"),
            side_effect=asyncio.CancelledError,
        )
        job = service._manager.get_job(job_id)
        assert job is not None
        from src.server.schemas.backtest import JobStatus

        assert job.status == JobStatus.CANCELLED
        service._executor.shutdown(wait=False)

    async def test_run_evolve_cancelled(self) -> None:
        """_run_jobがキャンセル時にCANCELLEDになる（evolve）"""
        import asyncio

        service, job_id = await self._run_job_test(
            job_type="lab_evolve",
            lab_type="evolve",
            sync_method="_execute_evolve_sync",
            sync_args=("test", 10, 30, False),
            side_effect=asyncio.CancelledError,
        )
        job = service._manager.get_job(job_id)
        assert job is not None
        from src.server.schemas.backtest import JobStatus

        assert job.status == JobStatus.CANCELLED
        service._executor.shutdown(wait=False)

    async def test_run_optimize_cancelled(self) -> None:
        """_run_optimizeがキャンセル時にCANCELLEDになる"""
        import asyncio

        from src.server.services.lab_service import LabService

        manager = JobManager()
        service = LabService(manager=manager, max_workers=1)

        job_id = manager.create_job("test", job_type="lab_optimize")

        with patch.object(
            service, "_execute_optimize_sync", side_effect=asyncio.CancelledError
        ):
            await service._run_optimize(
                job_id,
                "test",
                50,
                "tpe",
                "params_only",
                1,
                1,
                None,
                False,
                False,
                [],
                None,
            )

        job = manager.get_job(job_id)
        assert job is not None
        from src.server.schemas.backtest import JobStatus

        assert job.status == JobStatus.CANCELLED
        service._executor.shutdown(wait=False)

    async def test_run_improve_cancelled(self) -> None:
        """_run_jobがキャンセル時にCANCELLEDになる（improve）"""
        import asyncio

        service, job_id = await self._run_job_test(
            job_type="lab_improve",
            lab_type="improve",
            sync_method="_execute_improve_sync",
            sync_args=("test", False),
            side_effect=asyncio.CancelledError,
        )
        job = service._manager.get_job(job_id)
        assert job is not None
        from src.server.schemas.backtest import JobStatus

        assert job.status == JobStatus.CANCELLED
        service._executor.shutdown(wait=False)


class TestLabServiceSyncMethods:
    """LabServiceのsyncメソッドをmockで単体テスト"""

    def test_execute_generate_sync(self) -> None:
        """_execute_generate_syncが結果dictを返す"""
        from unittest.mock import MagicMock

        from src.server.services.lab_service import LabService

        service = LabService(max_workers=1)

        mock_candidate = MagicMock()
        mock_candidate.strategy_id = "gen-001"
        mock_candidate.entry_filter_params = {"breakout": {}}
        mock_candidate.exit_trigger_params = {"rsi": {}}

        mock_result = MagicMock()
        mock_result.candidate = mock_candidate
        mock_result.score = 1.5
        mock_result.sharpe_ratio = 1.2
        mock_result.calmar_ratio = 0.8
        mock_result.total_return = 0.15
        mock_result.max_drawdown = -0.05
        mock_result.win_rate = 0.6
        mock_result.trade_count = 50
        mock_result.success = True

        with (
            patch(
                "src.agent.strategy_generator.StrategyGenerator"
            ) as MockGen,
            patch(
                "src.agent.evaluator.StrategyEvaluator"
            ) as MockEval,
            patch(
                "src.agent.yaml_updater.YamlUpdater"
            ) as MockYaml,
        ):
            MockGen.return_value.generate.return_value = [mock_candidate]
            MockEval.return_value.evaluate_batch.return_value = [mock_result]
            MockYaml.return_value.save_candidate.return_value = "/tmp/saved.yaml"

            result = service._execute_generate_sync(
                count=10, top=3, seed=42, save=True,
                direction="longonly", timeframe="daily", dataset="test",
            )

        assert result["lab_type"] == "generate"
        assert result["total_generated"] == 10
        assert result["saved_strategy_path"] == "/tmp/saved.yaml"
        assert len(result["results"]) == 1
        service._executor.shutdown(wait=False)

    def test_execute_generate_sync_no_save(self) -> None:
        """_execute_generate_syncでsave=Falseの場合"""
        from unittest.mock import MagicMock

        from src.server.services.lab_service import LabService

        service = LabService(max_workers=1)

        mock_candidate = MagicMock()
        mock_candidate.strategy_id = "gen-002"
        mock_candidate.entry_filter_params = {}
        mock_candidate.exit_trigger_params = {}

        mock_result = MagicMock()
        mock_result.candidate = mock_candidate
        mock_result.score = 1.0
        mock_result.sharpe_ratio = 0.5
        mock_result.calmar_ratio = 0.3
        mock_result.total_return = 0.05
        mock_result.max_drawdown = -0.1
        mock_result.win_rate = 0.4
        mock_result.trade_count = 20
        mock_result.success = True

        with (
            patch("src.agent.strategy_generator.StrategyGenerator") as MockGen,
            patch("src.agent.evaluator.StrategyEvaluator") as MockEval,
        ):
            MockGen.return_value.generate.return_value = [mock_candidate]
            MockEval.return_value.evaluate_batch.return_value = [mock_result]

            result = service._execute_generate_sync(
                count=5, top=1, seed=None, save=False,
                direction="longonly", timeframe="daily", dataset="test",
            )

        assert result["saved_strategy_path"] is None
        service._executor.shutdown(wait=False)

    def test_execute_generate_sync_with_constraints(self) -> None:
        """_execute_generate_syncで制約がGeneratorConfigに渡る"""
        from src.server.services.lab_service import LabService

        service = LabService(max_workers=1)

        with (
            patch("src.agent.strategy_generator.StrategyGenerator") as MockGen,
            patch("src.agent.evaluator.StrategyEvaluator") as MockEval,
        ):
            MockGen.return_value.generate.return_value = []
            MockEval.return_value.evaluate_batch.return_value = []

            service._execute_generate_sync(
                count=5,
                top=1,
                seed=7,
                save=False,
                direction="longonly",
                timeframe="daily",
                dataset="test",
                entry_filter_only=True,
                allowed_categories=["fundamental"],
            )

            assert MockGen.call_count == 1
            call_args = MockGen.call_args.kwargs
            assert "config" in call_args
            config = call_args["config"]
            assert config.entry_filter_only is True
            assert config.allowed_categories == ["fundamental"]

        service._executor.shutdown(wait=False)

    def test_execute_evolve_sync(self) -> None:
        """_execute_evolve_syncが結果dictを返す"""
        from unittest.mock import MagicMock

        from src.server.services.lab_service import LabService

        service = LabService(max_workers=1)

        mock_candidate = MagicMock()
        mock_candidate.strategy_id = "evo-001"

        mock_history = [
            {"generation": 0, "best_score": 1.0, "avg_score": 0.5, "worst_score": 0.1},
            {"generation": 1, "best_score": 2.0, "avg_score": 1.0, "worst_score": 0.5},
        ]

        with (
            patch("src.agent.parameter_evolver.ParameterEvolver") as MockEvolver,
            patch("src.agent.yaml_updater.YamlUpdater") as MockYaml,
        ):
            MockEvolver.return_value.evolve.return_value = (mock_candidate, [])
            MockEvolver.return_value.get_evolution_history.return_value = mock_history
            MockYaml.return_value.save_evolution_result.return_value = ("/tmp/evo.yaml", "/tmp/hist.yaml")

            result = service._execute_evolve_sync(
                "test_strat",
                10,
                30,
                "params_only",
                1,
                1,
                None,
                True,
                True,
                ["fundamental"],
            )

        assert result["lab_type"] == "evolve"
        assert result["best_strategy_id"] == "evo-001"
        assert result["best_score"] == 2.0
        assert len(result["history"]) == 2
        assert result["saved_strategy_path"] == "/tmp/evo.yaml"
        config = MockEvolver.call_args.kwargs["config"]
        assert config.entry_filter_only is True
        assert config.target_scope == "entry_filter_only"
        assert config.allowed_categories == ["fundamental"]
        service._executor.shutdown(wait=False)

    def test_execute_evolve_sync_no_save(self) -> None:
        """_execute_evolve_syncでsave=Falseの場合"""
        from unittest.mock import MagicMock

        from src.server.services.lab_service import LabService

        service = LabService(max_workers=1)

        mock_candidate = MagicMock()
        mock_candidate.strategy_id = "evo-002"

        with patch("src.agent.parameter_evolver.ParameterEvolver") as MockEvolver:
            MockEvolver.return_value.evolve.return_value = (mock_candidate, [])
            MockEvolver.return_value.get_evolution_history.return_value = []

            result = service._execute_evolve_sync(
                "test_strat",
                5,
                20,
                "params_only",
                1,
                1,
                None,
                False,
            )

        assert result["saved_strategy_path"] is None
        assert result["saved_history_path"] is None
        service._executor.shutdown(wait=False)

    def test_execute_optimize_sync(self) -> None:
        """_execute_optimize_syncが結果dictを返す"""
        from unittest.mock import MagicMock

        from src.server.services.lab_service import LabService

        service = LabService(max_workers=1)

        mock_candidate = MagicMock()
        mock_study = MagicMock()
        mock_study.best_trial = MagicMock()
        mock_study.best_trial.params = {"period": 100}
        mock_study.best_value = 3.0
        mock_study.trials = [MagicMock(), MagicMock()]

        mock_history = [
            {"trial": 0, "score": 1.0, "params": {}},
            {"trial": 1, "score": 3.0, "params": {"period": 100}},
        ]

        with (
            patch("src.agent.optuna_optimizer.OptunaOptimizer") as MockOpt,
            patch("src.agent.yaml_updater.YamlUpdater") as MockYaml,
        ):
            MockOpt.return_value.optimize.return_value = (mock_candidate, mock_study)
            MockOpt.return_value.get_optimization_history.return_value = mock_history
            MockYaml.return_value.save_optuna_result.return_value = ("/tmp/opt.yaml", "/tmp/hist.yaml")

            result = service._execute_optimize_sync(
                "test_strat",
                50,
                "tpe",
                "params_only",
                1,
                1,
                None,
                True,
                True,
                ["fundamental"],
                None,
                None,
            )

        assert result["lab_type"] == "optimize"
        assert result["best_score"] == 3.0
        assert result["total_trials"] == 2
        assert result["saved_strategy_path"] == "/tmp/opt.yaml"
        config = MockOpt.call_args.kwargs["config"]
        assert config.entry_filter_only is True
        assert config.target_scope == "entry_filter_only"
        assert config.allowed_categories == ["fundamental"]
        service._executor.shutdown(wait=False)

    def test_execute_optimize_sync_without_best_trial_and_save(self) -> None:
        """_execute_optimize_syncでbest_trialなし/save=Falseの分岐を通す"""
        from unittest.mock import MagicMock

        from src.server.services.lab_service import LabService

        service = LabService(max_workers=1)

        mock_candidate = MagicMock()
        mock_study = MagicMock()
        mock_study.best_trial = None
        mock_study.best_value = 9.9
        mock_study.trials = []

        with patch("src.agent.optuna_optimizer.OptunaOptimizer") as MockOpt:
            MockOpt.return_value.optimize.return_value = (mock_candidate, mock_study)
            MockOpt.return_value.get_optimization_history.return_value = []

            result = service._execute_optimize_sync(
                "test_strat",
                10,
                "tpe",
                "params_only",
                1,
                1,
                None,
                False,
                False,
                [],
                None,
                None,
            )

        assert result["best_score"] == 0.0
        assert result["best_params"] == {}
        assert result["saved_strategy_path"] is None
        assert result["saved_history_path"] is None
        service._executor.shutdown(wait=False)

    def test_execute_improve_sync(self) -> None:
        """_execute_improve_syncが結果dictを返す"""
        from unittest.mock import MagicMock

        from src.server.services.lab_service import LabService

        service = LabService(max_workers=1)

        mock_report = MagicMock()
        mock_report.max_drawdown = -0.15
        mock_report.max_drawdown_duration_days = 30
        mock_report.suggested_improvements = ["RSI追加"]

        mock_improvement = MagicMock()
        mock_improvement.improvement_type = "add_signal"
        mock_improvement.target = "entry"
        mock_improvement.signal_name = "rsi_threshold"
        mock_improvement.changes = {"threshold": 30}
        mock_improvement.reason = "過売り"
        mock_improvement.expected_impact = "DD改善"

        with (
            patch("src.agent.strategy_improver.StrategyImprover") as MockImprover,
            patch("src.lib.strategy_runtime.loader.ConfigLoader") as MockLoader,
            patch("src.agent.yaml_updater.YamlUpdater") as MockYaml,
        ):
            MockImprover.return_value.analyze.return_value = mock_report
            MockLoader.return_value.load_strategy_config.return_value = {}
            MockImprover.return_value.suggest_improvements.return_value = [mock_improvement]
            MockYaml.return_value.apply_improvements.return_value = "/tmp/improved.yaml"

            result = service._execute_improve_sync("test_strat", True)

        assert result["lab_type"] == "improve"
        assert result["max_drawdown"] == -0.15
        assert len(result["improvements"]) == 1
        assert result["saved_strategy_path"] == "/tmp/improved.yaml"
        service._executor.shutdown(wait=False)

    def test_execute_improve_sync_no_apply(self) -> None:
        """_execute_improve_syncでauto_apply=Falseの場合"""
        from unittest.mock import MagicMock

        from src.server.services.lab_service import LabService

        service = LabService(max_workers=1)

        mock_report = MagicMock()
        mock_report.max_drawdown = -0.1
        mock_report.max_drawdown_duration_days = 10
        mock_report.suggested_improvements = []

        with (
            patch("src.agent.strategy_improver.StrategyImprover") as MockImprover,
            patch("src.lib.strategy_runtime.loader.ConfigLoader") as MockLoader,
        ):
            MockImprover.return_value.analyze.return_value = mock_report
            MockLoader.return_value.load_strategy_config.return_value = {}
            MockImprover.return_value.suggest_improvements.return_value = []

            result = service._execute_improve_sync("test_strat", False)

        assert result["saved_strategy_path"] is None
        assert result["improvements"] == []
        service._executor.shutdown(wait=False)
