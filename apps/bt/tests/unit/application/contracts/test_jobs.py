from src.application.contracts.jobs import JobEvent, JobProgress, JobStatus


def test_job_status_values_are_stable() -> None:
    assert [status.value for status in JobStatus] == [
        "pending",
        "running",
        "completed",
        "failed",
        "cancelled",
    ]


def test_job_event_serialization_is_stable() -> None:
    event = JobEvent(
        job_id="job-1",
        status="running",
        progress=0.25,
        message="running",
        data={"stage": "load"},
    )
    assert event.model_dump(mode="json") == {
        "job_id": "job-1",
        "status": "running",
        "progress": 0.25,
        "message": "running",
        "data": {"stage": "load"},
    }


def test_job_progress_serialization_is_stable() -> None:
    progress = JobProgress(
        stage="copy",
        current=1,
        total=4,
        percentage=25.0,
        message="copying",
    )
    assert progress.model_dump(mode="json") == {
        "stage": "copy",
        "current": 1,
        "total": 4,
        "percentage": 25.0,
        "message": "copying",
    }
