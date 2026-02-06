"""
Server Route Tests
"""

from datetime import datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from src.server.app import app
from src.server.schemas.backtest import JobStatus
from src.server.services.job_manager import JobManager


@pytest.fixture
def client() -> TestClient:
    """FastAPIテストクライアント"""
    return TestClient(app)


class TestHealthEndpoint:
    """ヘルスチェックエンドポイントのテスト"""

    def test_health_check(self, client: TestClient) -> None:
        """ヘルスチェックが正常に動作する"""
        response = client.get("/api/health")
        assert response.status_code == 200

        data = response.json()
        assert data["status"] == "healthy"
        assert data["service"] == "trading25-bt"
        assert "version" in data


class TestStrategiesEndpoint:
    """戦略エンドポイントのテスト"""

    def test_list_strategies(self, client: TestClient) -> None:
        """戦略一覧を取得できる"""
        response = client.get("/api/strategies")
        assert response.status_code == 200

        data = response.json()
        assert "strategies" in data
        assert "total" in data
        assert isinstance(data["strategies"], list)

    def test_get_nonexistent_strategy(self, client: TestClient) -> None:
        """存在しない戦略を取得すると404"""
        response = client.get("/api/strategies/nonexistent_strategy_12345")
        assert response.status_code == 404


class TestBacktestEndpoint:
    """バックテストエンドポイントのテスト"""

    def test_list_jobs_empty(self, client: TestClient) -> None:
        """ジョブ一覧を取得できる（空の場合）"""
        response = client.get("/api/backtest/jobs")
        assert response.status_code == 200

        data = response.json()
        assert isinstance(data, list)

    def test_get_nonexistent_job(self, client: TestClient) -> None:
        """存在しないジョブを取得すると404"""
        response = client.get("/api/backtest/jobs/nonexistent-job-id")
        assert response.status_code == 404


class TestPathTraversalProtection:
    """パストラバーサル攻撃防止テスト

    Note: FastAPIのパスパラメータは '/' をセグメント区切りとして扱うため、
    '../etc' のようなパスはルーティング段階で404になる。
    バリデーション関数は '/','\\','..',null byte を含む値を拒否する。
    """

    def test_html_file_content_backslash_traversal(self, client: TestClient) -> None:
        """バックスラッシュによるパストラバーサルを拒否"""
        response = client.get("/api/backtest/html-files/strategy%5C..%5Cetc/test.html")
        assert response.status_code == 400

    def test_html_file_content_dotdot_strategy(self, client: TestClient) -> None:
        """strategyに '..' を含む場合を拒否"""
        response = client.get("/api/backtest/html-files/..etcpasswd/test.html")
        assert response.status_code == 400

    def test_html_file_content_dotdot_filename(self, client: TestClient) -> None:
        """filenameに '..' を含む場合を拒否"""
        response = client.get("/api/backtest/html-files/valid_strategy/..secret.html")
        assert response.status_code == 400

    def test_html_file_content_null_byte(self, client: TestClient) -> None:
        """null byteを含むパラメータを拒否"""
        response = client.get("/api/backtest/html-files/valid_strategy/test%00.html")
        assert response.status_code == 400

    def test_html_file_list_path_traversal(self, client: TestClient) -> None:
        """HTMLファイル一覧のstrategyフィルタでパストラバーサルを拒否"""
        response = client.get("/api/backtest/html-files?strategy=../../etc")
        assert response.status_code == 400

    def test_html_file_list_dotdot_traversal(self, client: TestClient) -> None:
        """HTMLファイル一覧のstrategyフィルタで '..' を拒否"""
        response = client.get("/api/backtest/html-files?strategy=..etc")
        assert response.status_code == 400

    def test_optimize_grid_config_backslash_traversal(self, client: TestClient) -> None:
        """Grid設定でバックスラッシュによるパストラバーサルを拒否"""
        response = client.get("/api/optimize/grid-configs/strategy%5C..%5Cetc")
        assert response.status_code == 400

    def test_optimize_html_file_null_byte(self, client: TestClient) -> None:
        """最適化HTMLファイルでnull byteを拒否"""
        response = client.get("/api/optimize/html-files/valid_strategy/test%00.html")
        assert response.status_code == 400

    def test_path_traversal_is_blocked_not_just_not_found(
        self, client: TestClient
    ) -> None:
        """パストラバーサル攻撃は400（不正リクエスト）で拒否、404ではない"""
        # バックスラッシュを含む戦略名
        response = client.get("/api/backtest/html-files/test%5C..%5Cetc/a.html")
        assert response.status_code == 400
        assert "不正な" in response.json()["message"]


class TestDefaultConfigEndpoint:
    """デフォルト設定エンドポイントのテスト"""

    def test_get_default_config(self, client: TestClient) -> None:
        """デフォルト設定を取得できる"""
        response = client.get("/api/config/default")
        assert response.status_code == 200

        data = response.json()
        assert "content" in data
        assert isinstance(data["content"], str)
        assert len(data["content"]) > 0

    def test_get_default_config_contains_yaml(self, client: TestClient) -> None:
        """デフォルト設定のcontentがYAML形式である"""
        response = client.get("/api/config/default")
        assert response.status_code == 200

        data = response.json()
        # default.yamlは"default:"キーを含むはず
        assert "default:" in data["content"]

    def test_update_default_config_invalid_yaml(self, client: TestClient) -> None:
        """無効なYAMLで更新すると400"""
        response = client.put(
            "/api/config/default",
            json={"content": "invalid: yaml: [broken"},
        )
        assert response.status_code == 400
        assert "YAML構文エラー" in response.json()["message"]

    def test_update_default_config_not_dict(self, client: TestClient) -> None:
        """YAMLがオブジェクトでない場合は400"""
        response = client.put(
            "/api/config/default",
            json={"content": "- item1\n- item2\n"},
        )
        assert response.status_code == 400
        assert "オブジェクト" in response.json()["message"]

    def test_update_default_config_missing_default_key(
        self, client: TestClient
    ) -> None:
        """'default'キーがない場合は400"""
        response = client.put(
            "/api/config/default",
            json={"content": "other_key:\n  value: 1\n"},
        )
        assert response.status_code == 400
        assert "'default'キー" in response.json()["message"]

    def test_update_default_config_default_not_dict(
        self, client: TestClient
    ) -> None:
        """'default'キーの値がオブジェクトでない場合は400"""
        response = client.put(
            "/api/config/default",
            json={"content": "default: just_a_string\n"},
        )
        assert response.status_code == 400
        assert "オブジェクト" in response.json()["message"]

    def test_update_and_get_roundtrip(self, client: TestClient) -> None:
        """更新→取得のラウンドトリップが正常に動作する"""
        # 1. 現在の設定を取得してバックアップ
        original = client.get("/api/config/default").json()["content"]

        try:
            # 2. 更新
            new_content = "default:\n  test_roundtrip: true\n"
            response = client.put(
                "/api/config/default",
                json={"content": new_content},
            )
            assert response.status_code == 200
            assert response.json()["success"] is True

            # 3. 取得して確認
            response = client.get("/api/config/default")
            assert response.status_code == 200
            assert "test_roundtrip" in response.json()["content"]
        finally:
            # 4. 元に戻す
            client.put(
                "/api/config/default",
                json={"content": original},
            )


class TestJobManagerCleanup:
    """JobManager cleanup_old_jobs テスト"""

    def test_cleanup_old_completed_jobs(self) -> None:
        """完了した古いジョブが削除される"""
        manager = JobManager()
        job_id = manager.create_job("test_strategy")

        # ジョブを完了状態にして古い時刻を設定
        job = manager.get_job(job_id)
        assert job is not None
        job.status = JobStatus.COMPLETED
        job.created_at = datetime.now() - timedelta(hours=25)

        deleted = manager.cleanup_old_jobs(max_age_hours=24)
        assert deleted == 1
        assert manager.get_job(job_id) is None

    def test_cleanup_preserves_recent_jobs(self) -> None:
        """最近のジョブは削除されない"""
        manager = JobManager()
        job_id = manager.create_job("test_strategy")

        job = manager.get_job(job_id)
        assert job is not None
        job.status = JobStatus.COMPLETED

        deleted = manager.cleanup_old_jobs(max_age_hours=24)
        assert deleted == 0
        assert manager.get_job(job_id) is not None

    def test_cleanup_preserves_running_jobs(self) -> None:
        """実行中のジョブは古くても削除されない"""
        manager = JobManager()
        job_id = manager.create_job("test_strategy")

        job = manager.get_job(job_id)
        assert job is not None
        job.status = JobStatus.RUNNING
        job.created_at = datetime.now() - timedelta(hours=25)

        deleted = manager.cleanup_old_jobs(max_age_hours=24)
        assert deleted == 0
        assert manager.get_job(job_id) is not None

    def test_cleanup_old_failed_jobs(self) -> None:
        """失敗した古いジョブも削除される"""
        manager = JobManager()
        job_id = manager.create_job("test_strategy")

        job = manager.get_job(job_id)
        assert job is not None
        job.status = JobStatus.FAILED
        job.created_at = datetime.now() - timedelta(hours=25)

        deleted = manager.cleanup_old_jobs(max_age_hours=24)
        assert deleted == 1
        assert manager.get_job(job_id) is None
