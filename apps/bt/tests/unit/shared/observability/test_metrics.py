from src.shared.observability.metrics import MetricsRecorder


def test_request_error_rate_is_calculated() -> None:
    recorder = MetricsRecorder()

    recorder.record_request("GET", "/api/health", 200, 10.0)
    recorder.record_request("GET", "/api/fail", 500, 50.0)

    assert recorder.error_rate() == 0.5


def test_job_and_jquants_metrics_can_be_recorded() -> None:
    recorder = MetricsRecorder()

    recorder.record_job_duration("screening", "completed", 1200.0)
    recorder.record_jquants_fetch("/fins/summary")
    recorder.record_jquants_cache_state("/fins/summary", "hit")

    # no exception = recorder accepts phase5 metrics dimensions
    assert recorder.error_rate() == 0.0
