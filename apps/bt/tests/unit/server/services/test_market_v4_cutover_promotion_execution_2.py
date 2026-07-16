"""Market v4 cutover promotion execution tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.application.services.market_v4_cutover.contracts import (
    RetainedPromotionReportExpectation,
)
from src.infrastructure.db.market.managed_root import CutoverSafetyError
from tests.unit.server.services.market_v4_cutover_test_support import (
    FakeRuntime,
    FakeApi,
    _market_root,
    _retained_promotion_source,
    _TestAtomicExchange,
    _run_retained_promotion,
)


@pytest.mark.parametrize(
    "crash_boundary",
    ["committed_journaled", "cleanup_artifacts_deleted"],
)
def test_promotion_committed_recovery_completes_exact_pending_cleanup(
    tmp_path: Path,
    crash_boundary: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, _retained_root, config = _retained_promotion_source(data_root)
    service._workspace.atomic_exchange = _TestAtomicExchange()
    service._workspace.runtime = FakeRuntime(apis=[FakeApi()])

    def crash_after_commit(stage: str) -> None:
        if stage == crash_boundary:
            raise RuntimeError("simulated process crash after commit")

    service._workspace._promotion_boundary_hook = crash_after_commit
    with pytest.raises(CutoverSafetyError, match="cleanup incomplete"):
        _run_retained_promotion(service, config)

    staging = data_root / (
        "operations/market-v4-cutover/cleanup-staging/market-v4-active-20260716"
    )
    if crash_boundary == "committed_journaled":
        assert staging.is_dir()
    else:
        assert not staging.exists()

    service._workspace._promotion_boundary_hook = lambda _stage: None
    validator_expectations: list[RetainedPromotionReportExpectation | None] = []
    original_validator = service._promotion._promotion_reports._retained_promotion_report_contract_valid

    def record_strict_validation(
        report: object,
        *,
        expectation: RetainedPromotionReportExpectation | None = None,
    ) -> bool:
        validator_expectations.append(expectation)
        return original_validator(report, expectation=expectation)

    monkeypatch.setattr(
        service._promotion._promotion_reports,
        "_retained_promotion_report_contract_valid",
        record_strict_validation,
    )
    result = service._promotion._recover_retained_promotion(
        "market-v4-active-20260716",
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
    )

    assert result is not None
    assert validator_expectations
    assert all(expectation is not None for expectation in validator_expectations)
    assert not staging.exists()
    assert (
        data_root / "operations/market-v4-cutover/cleanup-results/"
        "market-v4-active-20260716.json"
    ).is_file()
