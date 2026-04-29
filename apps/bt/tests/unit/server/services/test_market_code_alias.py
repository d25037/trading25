"""
Market code alias utility tests.
"""

from src.shared.utils.market_code_alias import (
    canonicalize_market_list,
    expand_market_codes,
    format_market_scope_label,
    normalize_market_scope,
    parse_requested_market_codes,
    resolve_market_codes,
)


class TestParseRequestedMarketCodes:
    def test_returns_split_codes(self):
        result = parse_requested_market_codes("prime,standard")
        assert result == ["prime", "standard"]

    def test_returns_fallback_on_empty(self):
        result = parse_requested_market_codes("", fallback=["prime", "standard"])
        assert result == ["prime", "standard"]

    def test_returns_default_prime_when_empty_without_fallback(self):
        result = parse_requested_market_codes("")
        assert result == ["prime"]


class TestExpandMarketCodes:
    def test_expands_legacy_codes(self):
        result = expand_market_codes(["prime", "standard"])
        assert result == ["prime", "0111", "0101", "standard", "0112", "0102", "0106"]

    def test_expands_numeric_codes(self):
        result = expand_market_codes(["0111", "0112"])
        assert result == ["prime", "0111", "0101", "standard", "0112", "0102", "0106"]

    def test_deduplicates_and_keeps_unknown(self):
        result = expand_market_codes(["prime", "0111", "custom"])
        assert result == ["prime", "0111", "0101", "custom"]

    def test_can_exclude_legacy_codes(self):
        result = expand_market_codes(["prime", "0102"], include_legacy=False)
        assert result == ["prime", "0111", "standard", "0112"]


class TestResolveMarketCodes:
    def test_resolves_requested_and_query_codes(self):
        requested, query_codes = resolve_market_codes("prime,custom")
        assert requested == ["prime", "custom"]
        assert query_codes == ["prime", "0111", "0101", "custom"]

    def test_resolves_with_fallback(self):
        requested, query_codes = resolve_market_codes("", fallback=["standard"])
        assert requested == ["standard"]
        assert query_codes == ["standard", "0112", "0102", "0106"]


class TestNormalizeMarketScope:
    def test_normalizes_current_and_legacy_codes(self):
        assert normalize_market_scope("0111") == "prime"
        assert normalize_market_scope("0102") == "standard"
        assert normalize_market_scope("0107") == "growth"

    def test_normalizes_market_names(self):
        assert normalize_market_scope(None, market_name="東証一部") == "prime"
        assert normalize_market_scope(None, market_name="JASDAQ スタンダード") == "standard"
        assert normalize_market_scope(None, market_name="マザーズ") == "growth"

    def test_returns_default_for_unknown(self):
        assert normalize_market_scope("custom", default="custom") == "custom"


def test_canonicalize_market_list_and_label_use_shared_order():
    markets = canonicalize_market_list(["0107", "0111", "0102", "custom"])
    assert markets == ["prime", "standard", "growth", "custom"]
    assert format_market_scope_label(markets[:3]) == "All Markets"
