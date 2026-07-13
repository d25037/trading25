"""
Common API Schemas

ジョブステータスとジョブレスポンスの共通基底クラス
"""

from datetime import datetime

from pydantic import BaseModel, Field

from src.application.contracts import jobs as job_contracts
from src.domains.backtest.contracts import RunMetadata


class JobExecutionControl(BaseModel):
    """ジョブの durable execution control 状態"""

    lease_owner: str | None = Field(default=None, description="現在の execution lease owner")
    lease_expires_at: datetime | None = Field(
        default=None,
        description="現在の lease の失効時刻",
    )
    last_heartbeat_at: datetime | None = Field(
        default=None,
        description="最後の heartbeat 受信時刻",
    )
    cancel_requested: bool = Field(
        default=False,
        description="キャンセル要求が durable に記録されているか",
    )
    cancel_requested_at: datetime | None = Field(
        default=None,
        description="キャンセル要求受付時刻",
    )
    cancel_reason: str | None = Field(default=None, description="キャンセル理由")
    timeout_at: datetime | None = Field(default=None, description="実行タイムアウト時刻")


class BaseJobResponse(BaseModel):
    """ジョブレスポンス基底クラス"""

    job_id: str = Field(description="ジョブID")
    status: job_contracts.JobStatus = Field(description="ジョブステータス")
    progress: float | None = Field(default=None, description="進捗（0.0 - 1.0）")
    message: str | None = Field(default=None, description="ステータスメッセージ")
    created_at: datetime = Field(description="作成日時")
    started_at: datetime | None = Field(default=None, description="開始日時")
    completed_at: datetime | None = Field(default=None, description="完了日時")
    error: str | None = Field(default=None, description="エラーメッセージ")
    run_metadata: RunMetadata | None = Field(
        default=None,
        description="Engine-neutral run metadata",
    )
    execution_control: JobExecutionControl | None = Field(
        default=None,
        description="Durable execution control state",
    )

    model_config = {"use_enum_values": True}
