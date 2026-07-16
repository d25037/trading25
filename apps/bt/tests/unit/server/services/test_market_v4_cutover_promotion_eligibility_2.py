"""Market v4 cutover promotion eligibility tests."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from src.application.services.market_v4_cutover.contracts import (
    PromotionAppendResult,
    PromotionAppendStatus,
    PromotionState,
)
from src.application.services.market_v4_cutover.journal import PromotionJournal
from src.infrastructure.db.market.managed_root import CutoverSafetyError
from src.infrastructure.db.market import market_operation_lease
from tests.unit.server.services.market_v4_cutover_test_support import (
    _market_root,
    _retained_promotion_source,
)


@pytest.mark.parametrize(
    ("fault_state", "status"),
    [
        (PromotionState.RUNTIMES_DETACHED, PromotionAppendStatus.NOT_COMMITTED),
        (PromotionState.PREPARED, PromotionAppendStatus.NOT_COMMITTED),
        (PromotionState.RUNTIMES_DETACHED, PromotionAppendStatus.INDETERMINATE),
        (PromotionState.PREPARED, PromotionAppendStatus.INDETERMINATE),
    ],
)
def test_promotion_preparation_append_faults_stop_or_fence_at_exact_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault_state: PromotionState,
    status: PromotionAppendStatus,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    report_id = "market-v4-active-20260716"
    leaked_fds: tuple[int, int] | None = None
    with pytest.raises(CutoverSafetyError, match="indeterminate|not committed"):
        with service._retained_promotion_eligibility_scope(
            report_id=report_id,
            retained_report_id="market-v4-retained-20260715-r13",
            backup_id="market-v3-pre-v4-20260716",
            config=config,
        ) as eligibility:
            assert service._active_lease is not None
            assert service._retained_lease is not None
            journal = PromotionJournal(
                service._managed(),
                report_id,
                now=lambda: "2026-07-16T00:00:00Z",
            )
            original_append = journal.append

            def append(state: PromotionState, **kwargs: object):
                nonlocal leaked_fds
                if state is fault_state:
                    if status is PromotionAppendStatus.INDETERMINATE:
                        leaked_fds = (
                            service._active_lease.fd,
                            service._retained_lease.fd,
                        )
                    return PromotionAppendResult(
                        status,
                        None,
                        f"attempt-{state.value}-{status.value}",
                    )
                return original_append(state, **kwargs)  # type: ignore[arg-type]

            monkeypatch.setattr(journal, "append", append)
            service._prepare_retained_promotion_under_leases(
                eligibility,
                backup_id="market-v3-pre-v4-20260716",
                journal=journal,
            )

    if status is PromotionAppendStatus.INDETERMINATE:
        assert leaked_fds is not None
        try:
            for root in (data_root, retained_root):
                with pytest.raises(CutoverSafetyError, match="operation lease"):
                    market_operation_lease.MarketOperationLease.acquire_existing(
                        root,
                        exclusive=True,
                    )
        finally:
            for fd in leaked_fds:
                os.close(fd)
