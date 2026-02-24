"""
JQuantsAsyncClient Unit Tests

respx を使用した HTTP モックテスト。リトライ・ページネーション・タイムアウトを検証。
"""

import pytest
import httpx
import respx

from src.infrastructure.external_api.clients.jquants_client import JQuantsApiError, JQuantsAsyncClient


@pytest.fixture
def client():
    """テスト用 JQuantsAsyncClient"""
    return JQuantsAsyncClient(api_key="dummy_token_value_0000", plan="premium", timeout=5.0)


class TestJQuantsAsyncClient:
    def test_has_api_key(self, client):
        assert client.has_api_key is True

    def test_no_api_key(self):
        c = JQuantsAsyncClient(api_key="", plan="free")
        assert c.has_api_key is False

    def test_masked_key(self, client):
        assert client.masked_key == "dumm...0000"

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
            return_value=httpx.Response(200, json={"data": [{"Code": "72030"}]})
        )
        result = await client.get("/equities/master", {"code": "7203"})
        assert result["data"][0]["Code"] == "72030"
        await client.close()

    @respx.mock
    @pytest.mark.asyncio
    async def test_get_with_retry_on_500(self, client):
        """500 エラー時のリトライ"""
        route = respx.get("https://api.jquants.com/v2/equities/master")
        route.side_effect = [
            httpx.Response(500, json={"error": "Internal Server Error"}),
            httpx.Response(200, json={"data": []}),
        ]
        result = await client.get("/equities/master")
        assert result == {"data": []}
        assert route.call_count == 2
        await client.close()

    @respx.mock
    @pytest.mark.asyncio
    async def test_get_with_retry_on_429(self, client):
        """429 エラー時のリトライ"""
        route = respx.get("https://api.jquants.com/v2/equities/master")
        route.side_effect = [
            httpx.Response(429, json={"error": "Rate limit"}),
            httpx.Response(200, json={"data": []}),
        ]
        result = await client.get("/equities/master")
        assert result == {"data": []}
        assert route.call_count == 2
        await client.close()

    @respx.mock
    @pytest.mark.asyncio
    async def test_get_max_retries_exceeded(self, client):
        """最大リトライ回数超過で JQuantsApiError(502) が発生"""
        respx.get("https://api.jquants.com/v2/equities/master").mock(
            return_value=httpx.Response(500, json={"error": "Internal Server Error"})
        )
        with pytest.raises(JQuantsApiError) as exc_info:
            await client.get("/equities/master")
        assert exc_info.value.status_code == 502
        assert "500" in exc_info.value.message
        await client.close()

    @respx.mock
    @pytest.mark.asyncio
    async def test_get_non_retryable_error(self, client):
        """リトライ対象外のエラー（404）で JQuantsApiError(502) が発生"""
        respx.get("https://api.jquants.com/v2/equities/master").mock(
            return_value=httpx.Response(404, json={"error": "Not Found"})
        )
        with pytest.raises(JQuantsApiError) as exc_info:
            await client.get("/equities/master")
        assert exc_info.value.status_code == 502
        assert "404" in exc_info.value.message
        await client.close()


class TestGetPaginated:
    @respx.mock
    @pytest.mark.asyncio
    async def test_single_page(self, client):
        """単一ページのページネーション"""
        respx.get("https://api.jquants.com/v2/equities/master").mock(
            return_value=httpx.Response(200, json={"data": [{"Code": "7203"}]})
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
                "data": [{"Code": "7203"}],
                "pagination_key": "page2",
            }),
            httpx.Response(200, json={
                "data": [{"Code": "6758"}],
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
                "data": [{"Code": f"{i}"}],
                "pagination_key": f"page{i+1}",
            })
            for i in range(5)
        ]
        result = await client.get_paginated("/equities/master", max_pages=2)
        assert len(result) == 2
        await client.close()


class TestJQuantsApiError:
    @respx.mock
    @pytest.mark.asyncio
    async def test_403_raises_api_error_502(self, client):
        """403 が JQuantsApiError(502) を発生させること"""
        respx.get("https://api.jquants.com/v2/markets/margin-interest").mock(
            return_value=httpx.Response(403, json={"error": "Forbidden"})
        )
        with pytest.raises(JQuantsApiError) as exc_info:
            await client.get("/markets/margin-interest", {"code": "31030"})
        assert exc_info.value.status_code == 502
        assert "403" in exc_info.value.message
        await client.close()

    @respx.mock
    @pytest.mark.asyncio
    async def test_timeout_raises_api_error_504(self, client):
        """タイムアウトが JQuantsApiError(504) を発生させること"""
        respx.get("https://api.jquants.com/v2/equities/master").mock(
            side_effect=httpx.ReadTimeout("read timed out")
        )
        with pytest.raises(JQuantsApiError) as exc_info:
            await client.get("/equities/master")
        assert exc_info.value.status_code == 504
        assert "timeout" in exc_info.value.message.lower()
        await client.close()

    @respx.mock
    @pytest.mark.asyncio
    async def test_connection_error_raises_api_error_502(self, client):
        """接続エラーが JQuantsApiError(502) を発生させること"""
        respx.get("https://api.jquants.com/v2/equities/master").mock(
            side_effect=httpx.ConnectError("connection refused")
        )
        with pytest.raises(JQuantsApiError) as exc_info:
            await client.get("/equities/master")
        assert exc_info.value.status_code == 502
        assert "connection error" in exc_info.value.message.lower()
        await client.close()

    @respx.mock
    @pytest.mark.asyncio
    async def test_paginated_403_raises_api_error(self, client):
        """get_paginated でも 403 が JQuantsApiError(502) を発生させること"""
        respx.get("https://api.jquants.com/v2/equities/master").mock(
            return_value=httpx.Response(403, json={"error": "Forbidden"})
        )
        with pytest.raises(JQuantsApiError) as exc_info:
            await client.get_paginated("/equities/master")
        assert exc_info.value.status_code == 502
        await client.close()


class TestDataKeyExtraction:
    def test_known_endpoint(self, client):
        """既知のエンドポイントのデータキー"""
        body = {"data": [{"Date": "2024-01-01"}]}
        key = client._extract_data_key("/equities/bars/daily", body)
        assert key == "data"

    def test_known_endpoint_falls_back_when_mapped_key_missing(self, client):
        """既知エンドポイントでも data が無い場合は最初の配列キーへフォールバック"""
        body = {"indices": [{"Date": "2024-01-01"}]}
        key = client._extract_data_key("/indices/bars/daily", body)
        assert key == "indices"

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
