"""
Market code alias utility tests.
"""

from src.application.services.market_code_alias import (
    expand_market_codes,
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
        assert result == ["prime", "0111", "standard", "0112"]

    def test_expands_numeric_codes(self):
        result = expand_market_codes(["0111", "0112"])
        assert result == ["prime", "0111", "standard", "0112"]

    def test_deduplicates_and_keeps_unknown(self):
        result = expand_market_codes(["prime", "0111", "custom"])
        assert result == ["prime", "0111", "custom"]


class TestResolveMarketCodes:
    def test_resolves_requested_and_query_codes(self):
        requested, query_codes = resolve_market_codes("prime,custom")
        assert requested == ["prime", "custom"]
        assert query_codes == ["prime", "0111", "custom"]

    def test_resolves_with_fallback(self):
        requested, query_codes = resolve_market_codes("", fallback=["standard"])
        assert requested == ["standard"]
        assert query_codes == ["standard", "0112"]
