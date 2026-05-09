from pathlib import Path


PRODUCT_FILES_THAT_MUST_NOT_CONTAIN_DIRECT_VALUATION_MATH = {
    "apps/bt/src/application/services/chart_service.py",
    "apps/bt/src/application/services/ranking_service.py",
}


def _repo_root() -> Path:
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "apps" / "bt").exists() and (parent / "apps" / "ts").exists():
            return parent
    raise AssertionError("Could not locate repository root")


def test_product_services_do_not_reintroduce_direct_per_pbr_math() -> None:
    banned_fragments = (
        "curr.close / actual_statement.earnings_per_share",
        "curr.close / forecast_statement.forward_eps",
        "curr.close / actual_statement.bps",
        "price * baseline_shares",
        "current_price /",
    )
    repo_root = _repo_root()
    for relative in PRODUCT_FILES_THAT_MUST_NOT_CONTAIN_DIRECT_VALUATION_MATH:
        text = (repo_root / relative).read_text()
        for fragment in banned_fragments:
            assert fragment not in text, (
                f"{relative} contains duplicated valuation math: {fragment}"
            )
