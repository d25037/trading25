"""
JQuantsAsyncClient Unit Tests

respx を使用した HTTP モックテスト。リトライ・ページネーション・タイムアウトを検証。
"""

import pytest
import httpx
import respx

from src.server.clients.jquants_client import JQuantsAsyncClient


@pytest.fixture
def client():
    """テスト用 JQuantsAsyncClient"""
    return JQuantsAsyncClient(api_key="test-api-key-12345678", plan="premium", timeout=5.0)


class TestJQuantsAsyncClient:
    def test_has_api_key(self, client):
        assert client.has_api_key is True

    def test_no_api_key(self):
        c = JQuantsAsyncClient(api_key="", plan="free")
        assert c.has_api_key is False

    def test_masked_key(self, client):
        assert client.masked_key == "test...5678"

    def test_masked_key_short(self):
        c = JQuantsAsyncClient(api_key="abc", plan="free")
        assert c.masked_key == "****"

    def test_masked_key_none(self):
        c = JQuantsAsyncClient(api_key="", plan="free")
        assert c.masked_key is None


class TestGet:
    @respx.mock
    @pytest.mark.asyncio
    async def test_get_success(self, client):
        """正常な GET リクエスト"""
        respx.get("https://api.jquants.com/v2/equities/master").mock(
            return_value=httpx.Response(200, json={"info": [{"Code": "72030"}]})
        )
        result = await client.get("/equities/master", {"code": "7203"})
        assert result["info"][0]["Code"] == "72030"
        await client.close()

    @respx.mock
    @pytest.mark.asyncio
    async def test_get_with_retry_on_500(self, client):
        """500 エラー時のリトライ"""
        route = respx.get("https://api.jquants.com/v2/equities/master")
        route.side_effect = [
            httpx.Response(500, json={"error": "Internal Server Error"}),
            httpx.Response(200, json={"info": []}),
        ]
        result = await client.get("/equities/master")
        assert result == {"info": []}
        assert route.call_count == 2
        await client.close()

    @respx.mock
    @pytest.mark.asyncio
    async def test_get_with_retry_on_429(self, client):
        """429 エラー時のリトライ"""
        route = respx.get("https://api.jquants.com/v2/equities/master")
        route.side_effect = [
            httpx.Response(429, json={"error": "Rate limit"}),
            httpx.Response(200, json={"info": []}),
        ]
        result = await client.get("/equities/master")
        assert result == {"info": []}
        assert route.call_count == 2
        await client.close()

    @respx.mock
    @pytest.mark.asyncio
    async def test_get_max_retries_exceeded(self, client):
        """最大リトライ回数超過"""
        respx.get("https://api.jquants.com/v2/equities/master").mock(
            return_value=httpx.Response(500, json={"error": "Internal Server Error"})
        )
        with pytest.raises(httpx.HTTPStatusError):
            await client.get("/equities/master")
        await client.close()

    @respx.mock
    @pytest.mark.asyncio
    async def test_get_non_retryable_error(self, client):
        """リトライ対象外のエラー（404）"""
        respx.get("https://api.jquants.com/v2/equities/master").mock(
            return_value=httpx.Response(404, json={"error": "Not Found"})
        )
        with pytest.raises(httpx.HTTPStatusError):
            await client.get("/equities/master")
        await client.close()


class TestGetPaginated:
    @respx.mock
    @pytest.mark.asyncio
    async def test_single_page(self, client):
        """単一ページのページネーション"""
        respx.get("https://api.jquants.com/v2/equities/master").mock(
            return_value=httpx.Response(200, json={"info": [{"Code": "7203"}]})
        )
        result = await client.get_paginated("/equities/master")
        assert len(result) == 1
        assert result[0]["Code"] == "7203"
        await client.close()

    @respx.mock
    @pytest.mark.asyncio
    async def test_multi_page(self, client):
        """複数ページのページネーション"""
        route = respx.get("https://api.jquants.com/v2/equities/master")
        route.side_effect = [
            httpx.Response(200, json={
                "info": [{"Code": "7203"}],
                "pagination_key": "page2",
            }),
            httpx.Response(200, json={
                "info": [{"Code": "6758"}],
            }),
        ]
        result = await client.get_paginated("/equities/master")
        assert len(result) == 2
        assert result[0]["Code"] == "7203"
        assert result[1]["Code"] == "6758"
        await client.close()

    @respx.mock
    @pytest.mark.asyncio
    async def test_max_pages_limit(self, client):
        """max_pages による制限"""
        route = respx.get("https://api.jquants.com/v2/equities/master")
        route.side_effect = [
            httpx.Response(200, json={
                "info": [{"Code": f"{i}"}],
                "pagination_key": f"page{i+1}",
            })
            for i in range(5)
        ]
        result = await client.get_paginated("/equities/master", max_pages=2)
        assert len(result) == 2
        await client.close()


class TestDataKeyExtraction:
    def test_known_endpoint(self, client):
        """既知のエンドポイントのデータキー"""
        body = {"daily_quotes": [{"Date": "2024-01-01"}]}
        key = client._extract_data_key("/equities/bars/daily", body)
        assert key == "daily_quotes"

    def test_unknown_endpoint_fallback(self, client):
        """未知のエンドポイントのフォールバック"""
        body = {"some_data": [{"id": 1}], "count": 5}
        key = client._extract_data_key("/unknown/path", body)
        assert key == "some_data"

    def test_no_list_field(self, client):
        """リスト型フィールドがない場合"""
        body = {"status": "ok", "count": 0}
        key = client._extract_data_key("/unknown/path", body)
        assert key is None
