"""Unit tests for DatasetAPIClient."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.api.dataset_client import DatasetAPIClient


class TestDatasetAPIClient:
    """Tests for DatasetAPIClient."""

    def test_init(self) -> None:
        """Test initialization with dataset name."""
        client = DatasetAPIClient("sampleA")
        assert client.dataset_name == "sampleA"
        assert client.base_url == "http://localhost:3001"

    def test_dataset_path(self) -> None:
        """Test dataset path construction."""
        client = DatasetAPIClient("sampleA")
        assert client._dataset_path("/stocks") == "/api/dataset/sampleA/stocks"
        assert client._dataset_path("/topix") == "/api/dataset/sampleA/topix"

    @patch("httpx.Client")
    def test_get_stock_ohlcv_success(self, mock_client_class: MagicMock) -> None:
        """Test successful stock OHLCV data retrieval."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = [
            {"date": "2024-01-01", "open": 100, "high": 110, "low": 95, "close": 105, "volume": 1000},
            {"date": "2024-01-02", "open": 105, "high": 115, "low": 100, "close": 110, "volume": 1200},
        ]

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = DatasetAPIClient("sampleA")
        df = client.get_stock_ohlcv("7203", "2024-01-01", "2024-01-31")

        assert len(df) == 2
        assert list(df.columns) == ["Open", "High", "Low", "Close", "Volume"]
        assert df.index.name == "date"
        assert df.iloc[0]["Close"] == 105

    @patch("httpx.Client")
    def test_get_stock_ohlcv_empty(self, mock_client_class: MagicMock) -> None:
        """Test empty stock OHLCV response."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = []

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = DatasetAPIClient("sampleA")
        df = client.get_stock_ohlcv("9999")

        assert df.empty

    @patch("httpx.Client")
    def test_get_stock_list(self, mock_client_class: MagicMock) -> None:
        """Test stock list retrieval."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = [
            {"stockCode": "7203", "record_count": 500},
            {"stockCode": "9984", "record_count": 450},
        ]

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = DatasetAPIClient("sampleA")
        df = client.get_stock_list(min_records=100)

        assert len(df) == 2
        assert "stockCode" in df.columns

    @patch("httpx.Client")
    def test_get_topix(self, mock_client_class: MagicMock) -> None:
        """Test TOPIX data retrieval."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = [
            {"date": "2024-01-01", "open": 2500, "high": 2550, "low": 2480, "close": 2530},
            {"date": "2024-01-02", "open": 2530, "high": 2580, "low": 2510, "close": 2560},
        ]

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = DatasetAPIClient("sampleA")
        df = client.get_topix()

        assert len(df) == 2
        assert list(df.columns) == ["Open", "High", "Low", "Close"]
        assert "Volume" not in df.columns

    @patch("httpx.Client")
    def test_get_index(self, mock_client_class: MagicMock) -> None:
        """Test index data retrieval."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = [
            {"date": "2024-01-01", "open": 1000, "high": 1020, "low": 990, "close": 1010},
        ]

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = DatasetAPIClient("sampleA")
        df = client.get_index("I1001")

        assert len(df) == 1
        assert list(df.columns) == ["Open", "High", "Low", "Close"]

    @patch("httpx.Client")
    def test_get_index_list(self, mock_client_class: MagicMock) -> None:
        """Test index list retrieval."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = [
            {"indexCode": "I1001", "indexName": "Test Index", "record_count": 300},
        ]

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = DatasetAPIClient("sampleA")
        df = client.get_index_list()

        assert len(df) == 1
        assert "indexCode" in df.columns

    @patch("httpx.Client")
    def test_get_margin(self, mock_client_class: MagicMock) -> None:
        """Test margin data retrieval."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = [
            {"date": "2024-01-01", "longMarginVolume": 10000, "shortMarginVolume": 5000},
        ]

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = DatasetAPIClient("sampleA")
        df = client.get_margin("7203")

        assert len(df) == 1
        assert "longMarginVolume" in df.columns
        assert "shortMarginVolume" in df.columns

    @patch("httpx.Client")
    def test_get_statements(self, mock_client_class: MagicMock) -> None:
        """Test statements data retrieval."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = [
            {"disclosedDate": "2024-05-10", "earningsPerShare": 150.5, "profit": 1000000, "equity": 5000000},
        ]

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = DatasetAPIClient("sampleA")
        df = client.get_statements("7203")

        assert len(df) == 1
        assert df.index.name == "disclosedDate"

    @patch("httpx.Client")
    def test_get_sector_mapping(self, mock_client_class: MagicMock) -> None:
        """Test sector mapping retrieval."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = [
            {"sector_code": "33", "sector_name": "電気機器", "index_code": "I1001", "index_name": "電気機器指数"},
        ]

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = DatasetAPIClient("sampleA")
        df = client.get_sector_mapping()

        assert len(df) == 1
        assert "sector_code" in df.columns
        assert "index_code" in df.columns

    @patch("httpx.Client")
    def test_get_stock_sector_mapping(self, mock_client_class: MagicMock) -> None:
        """Test stock sector mapping retrieval."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = [
            {"code": "7203", "sector33Name": "輸送用機器"},
            {"code": "9984", "sector33Name": "情報・通信業"},
        ]

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = DatasetAPIClient("sampleA")
        result = client.get_stock_sector_mapping()

        assert result == {"7203": "輸送用機器", "9984": "情報・通信業"}

    @patch("httpx.Client")
    def test_get_stock_sector_mapping_empty(self, mock_client_class: MagicMock) -> None:
        """Test empty stock sector mapping."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = None

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = DatasetAPIClient("sampleA")
        result = client.get_stock_sector_mapping()
        assert result == {}

    @patch("httpx.Client")
    def test_get_sector_stocks(self, mock_client_class: MagicMock) -> None:
        """Test sector stocks retrieval."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = ["7203", "7267", "7269"]

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = DatasetAPIClient("sampleA")
        result = client.get_sector_stocks("輸送用機器")
        assert result == ["7203", "7267", "7269"]

    @patch("httpx.Client")
    def test_get_sector_stocks_empty(self, mock_client_class: MagicMock) -> None:
        """Test empty sector stocks."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = None

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = DatasetAPIClient("sampleA")
        result = client.get_sector_stocks("存在しない")
        assert result == []

    @patch("httpx.Client")
    def test_get_all_sectors(self, mock_client_class: MagicMock) -> None:
        """Test all sectors retrieval."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = [
            {"sector_code": "33", "sector_name": "電気機器", "stock_count": 50},
        ]

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = DatasetAPIClient("sampleA")
        df = client.get_all_sectors()
        assert len(df) == 1
        assert "sector_name" in df.columns

    @patch("httpx.Client")
    def test_get_all_sectors_empty(self, mock_client_class: MagicMock) -> None:
        """Test empty sectors response."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = None

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = DatasetAPIClient("sampleA")
        df = client.get_all_sectors()
        assert df.empty

    @patch("httpx.Client")
    def test_get_stock_list_with_limit(self, mock_client_class: MagicMock) -> None:
        """Test stock list with limit parameter."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = [{"stockCode": "7203"}]
        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = DatasetAPIClient("sampleA")
        result = client.get_stock_list(limit=10)
        assert len(result) == 1
        call_args = mock_httpx_client.request.call_args
        params = call_args.kwargs.get("params", {})
        assert params.get("limit") == 10

    @patch("httpx.Client")
    def test_get_stock_list_with_detail(self, mock_client_class: MagicMock) -> None:
        """Test stock list with detail parameter."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = [{"stockCode": "7203", "start_date": "2020-01-01"}]
        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = DatasetAPIClient("sampleA")
        result = client.get_stock_list(detail=True)
        assert len(result) == 1
        call_args = mock_httpx_client.request.call_args
        params = call_args.kwargs.get("params", {})
        assert params.get("detail") == "true"

    @patch("httpx.Client")
    def test_get_available_stocks(self, mock_client_class: MagicMock) -> None:
        """Test get_available_stocks delegates to get_stock_list with detail=True."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = [{"stockCode": "7203"}]
        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = DatasetAPIClient("sampleA")
        result = client.get_available_stocks()
        assert len(result) == 1

    @patch("httpx.Client")
    def test_get_symbol_list_success(self, mock_client_class: MagicMock) -> None:
        """Test successful symbol list retrieval."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = [{"code": "7203", "name": "トヨタ"}]
        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = DatasetAPIClient("sampleA")
        result = client.get_symbol_list(limit=100)
        assert len(result) == 1

    def test_get_symbol_list_limit_validation(self) -> None:
        """Test symbol list limit validation."""
        client = DatasetAPIClient("sampleA")

        with pytest.raises(ValueError) as exc_info:
            client.get_symbol_list(limit=0)
        assert "between 1 and 10000" in str(exc_info.value)

        with pytest.raises(ValueError) as exc_info:
            client.get_symbol_list(limit=10001)
        assert "between 1 and 10000" in str(exc_info.value)

    @patch("httpx.Client")
    def test_get_multiple_indices(self, mock_client_class: MagicMock) -> None:
        """Test multiple indices retrieval."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = [
            {"date": "2024-01-01", "open": 1000, "high": 1020, "low": 990, "close": 1010},
        ]

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = DatasetAPIClient("sampleA")
        result = client.get_multiple_indices(["I1001", "I1002"])

        # Each index should have its own DataFrame
        assert len(result) == 2
        assert "I1001" in result
        assert "I1002" in result

    @patch("httpx.Client")
    def test_get_multiple_margin(self, mock_client_class: MagicMock) -> None:
        """Test multiple margin data retrieval (deprecated, delegates to batch)."""
        import warnings

        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = {
            "7203": [
                {"date": "2024-01-01", "longMarginVolume": 10000, "shortMarginVolume": 5000},
            ],
            "9984": [
                {"date": "2024-01-01", "longMarginVolume": 20000, "shortMarginVolume": 8000},
            ],
        }

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = DatasetAPIClient("sampleA")
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            result = client.get_multiple_margin(["7203", "9984"])

        assert len(result) == 2
        assert "7203" in result
        assert "9984" in result

    @patch("httpx.Client")
    def test_context_manager(self, mock_client_class: MagicMock) -> None:
        """Test context manager usage."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = []

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        with DatasetAPIClient("sampleA") as client:
            _ = client.get_stock_list()

        mock_httpx_client.close.assert_called_once()

    @patch("httpx.Client")
    def test_get_stocks_ohlcv_batch_success(self, mock_client_class: MagicMock) -> None:
        """Test successful batch OHLCV data retrieval."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = {
            "7203": [
                {"date": "2024-01-01", "open": 100, "high": 110, "low": 95, "close": 105, "volume": 1000},
            ],
            "9984": [
                {"date": "2024-01-01", "open": 200, "high": 220, "low": 190, "close": 210, "volume": 2000},
            ],
        }

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = DatasetAPIClient("sampleA")
        result = client.get_stocks_ohlcv_batch(["7203", "9984"])

        assert len(result) == 2
        assert "7203" in result
        assert "9984" in result
        assert list(result["7203"].columns) == ["Open", "High", "Low", "Close", "Volume"]
        assert result["7203"].iloc[0]["Close"] == 105
        assert result["9984"].iloc[0]["Close"] == 210

    @patch("httpx.Client")
    def test_get_stocks_ohlcv_batch_empty(self, mock_client_class: MagicMock) -> None:
        """Test empty batch OHLCV response."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = {}

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = DatasetAPIClient("sampleA")
        result = client.get_stocks_ohlcv_batch([])

        assert result == {}

    @patch("httpx.Client")
    def test_get_stocks_ohlcv_batch_splits_large_requests(self, mock_client_class: MagicMock) -> None:
        """Test that batch requests are split when exceeding MAX_BATCH_SIZE."""
        mock_response = MagicMock()
        mock_response.is_success = True
        # Return empty dict for each batch call
        mock_response.json.return_value = {}

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = DatasetAPIClient("sampleA")
        # Create 150 stock codes (should be split into 2 batches: 100 + 50)
        stock_codes = [str(1000 + i) for i in range(150)]

        _ = client.get_stocks_ohlcv_batch(stock_codes)

        # Should have made 2 batch requests
        assert mock_httpx_client.request.call_count == 2

    @patch("httpx.Client")
    def test_get_stocks_ohlcv_batch_merges_results(self, mock_client_class: MagicMock) -> None:
        """Test that results from multiple batches are merged correctly."""
        call_count = [0]

        def mock_request(*args: object, **kwargs: object) -> MagicMock:
            call_count[0] += 1
            response = MagicMock()
            response.is_success = True
            if call_count[0] == 1:
                # First batch returns data for codes 1000-1099
                response.json.return_value = {
                    "1000": [{"date": "2024-01-01", "open": 100, "high": 110, "low": 95, "close": 105, "volume": 1000}],
                }
            else:
                # Second batch returns data for codes 1100-1149
                response.json.return_value = {
                    "1100": [{"date": "2024-01-01", "open": 200, "high": 220, "low": 190, "close": 210, "volume": 2000}],
                }
            return response

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.side_effect = mock_request
        mock_client_class.return_value = mock_httpx_client

        client = DatasetAPIClient("sampleA")
        stock_codes = [str(1000 + i) for i in range(150)]

        result = client.get_stocks_ohlcv_batch(stock_codes)

        # Should have both results merged
        assert "1000" in result
        assert "1100" in result
        assert result["1000"].iloc[0]["Close"] == 105
        assert result["1100"].iloc[0]["Close"] == 210

    @patch("httpx.Client")
    def test_get_stocks_ohlcv_batch_fallback_on_error(self, mock_client_class: MagicMock) -> None:
        """Test OHLCV batch falls back to individual requests on error."""
        call_count = [0]

        def mock_request(*args: object, **kwargs: object) -> MagicMock:
            call_count[0] += 1
            response = MagicMock()
            if call_count[0] == 1:
                raise ConnectionError("batch endpoint not available")
            response.is_success = True
            response.json.return_value = [
                {"date": "2024-01-01", "open": 100, "high": 110, "low": 95, "close": 105, "volume": 1000},
            ]
            return response

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.side_effect = mock_request
        mock_client_class.return_value = mock_httpx_client

        client = DatasetAPIClient("sampleA")
        result = client.get_stocks_ohlcv_batch(["7203", "9984"])

        assert len(result) == 2

    def test_max_batch_size_constant(self) -> None:
        """Test that MAX_BATCH_SIZE is set correctly."""
        client = DatasetAPIClient("sampleA")
        assert client.MAX_BATCH_SIZE == 100

    # ===== Margin Batch Tests =====

    @patch("httpx.Client")
    def test_get_margin_batch_success(self, mock_client_class: MagicMock) -> None:
        """Test successful batch margin data retrieval."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = {
            "7203": [
                {"date": "2024-01-01", "longMarginVolume": 10000, "shortMarginVolume": 5000},
                {"date": "2024-01-08", "longMarginVolume": 11000, "shortMarginVolume": 4500},
            ],
            "9984": [
                {"date": "2024-01-01", "longMarginVolume": 20000, "shortMarginVolume": 8000},
            ],
        }

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = DatasetAPIClient("sampleA")
        result = client.get_margin_batch(["7203", "9984"])

        assert len(result) == 2
        assert "7203" in result
        assert "9984" in result
        assert len(result["7203"]) == 2
        assert "longMarginVolume" in result["7203"].columns
        assert "shortMarginVolume" in result["7203"].columns

    @patch("httpx.Client")
    def test_get_margin_batch_empty(self, mock_client_class: MagicMock) -> None:
        """Test empty batch margin response."""
        client = DatasetAPIClient("sampleA")
        result = client.get_margin_batch([])
        assert result == {}

    @patch("httpx.Client")
    def test_get_margin_batch_splits_large_requests(self, mock_client_class: MagicMock) -> None:
        """Test that margin batch requests are split when exceeding MAX_BATCH_SIZE."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = {}

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = DatasetAPIClient("sampleA")
        stock_codes = [str(1000 + i) for i in range(150)]
        _ = client.get_margin_batch(stock_codes)

        assert mock_httpx_client.request.call_count == 2

    @patch("httpx.Client")
    def test_get_margin_batch_fallback_on_error(self, mock_client_class: MagicMock) -> None:
        """Test that margin batch falls back to individual requests on error."""
        call_count = [0]

        def mock_request(*args: object, **kwargs: object) -> MagicMock:
            call_count[0] += 1
            response = MagicMock()
            if call_count[0] == 1:
                # First call (batch) fails
                raise ConnectionError("batch endpoint not available")
            # Fallback individual calls succeed
            response.is_success = True
            response.json.return_value = [
                {"date": "2024-01-01", "longMarginVolume": 10000, "shortMarginVolume": 5000},
            ]
            return response

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.side_effect = mock_request
        mock_client_class.return_value = mock_httpx_client

        client = DatasetAPIClient("sampleA")
        result = client.get_margin_batch(["7203", "9984"])

        # Should have fallen back to individual requests
        assert len(result) == 2
        assert "7203" in result
        assert "9984" in result

    @patch("httpx.Client")
    def test_get_multiple_margin_deprecated(self, mock_client_class: MagicMock) -> None:
        """Test that get_multiple_margin issues a deprecation warning."""
        import warnings

        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = {
            "7203": [
                {"date": "2024-01-01", "longMarginVolume": 10000, "shortMarginVolume": 5000},
            ],
        }

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = DatasetAPIClient("sampleA")
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            _ = client.get_multiple_margin(["7203"])
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert "get_margin_batch" in str(w[0].message)

    # ===== Statements Batch Tests =====

    @patch("httpx.Client")
    def test_get_statements_batch_success(self, mock_client_class: MagicMock) -> None:
        """Test successful batch statements data retrieval."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = {
            "7203": [
                {"disclosedDate": "2024-05-10", "earningsPerShare": 150.5, "profit": 1000000, "equity": 5000000},
            ],
            "9984": [
                {"disclosedDate": "2024-05-15", "earningsPerShare": 200.0, "profit": 2000000, "equity": 8000000},
            ],
        }

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = DatasetAPIClient("sampleA")
        result = client.get_statements_batch(["7203", "9984"])

        assert len(result) == 2
        assert "7203" in result
        assert "9984" in result
        assert result["7203"].index.name == "disclosedDate"

    @patch("httpx.Client")
    def test_get_statements_batch_empty(self, mock_client_class: MagicMock) -> None:
        """Test empty batch statements response."""
        client = DatasetAPIClient("sampleA")
        result = client.get_statements_batch([])
        assert result == {}

    @patch("httpx.Client")
    def test_get_statements_batch_with_filters(self, mock_client_class: MagicMock) -> None:
        """Test batch statements with period_type and actual_only filters."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = {
            "7203": [
                {"disclosedDate": "2024-05-10", "earningsPerShare": 150.5, "profit": 1000000, "equity": 5000000},
            ],
        }

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = DatasetAPIClient("sampleA")
        result = client.get_statements_batch(
            ["7203"], period_type="FY", actual_only=True
        )

        assert len(result) == 1
        # Verify params were passed correctly
        call_args = mock_httpx_client.request.call_args
        params = call_args.kwargs.get("params", {})
        assert params.get("period_type") == "FY"
        assert params.get("actual_only") == "true"

    @patch("httpx.Client")
    def test_get_statements_batch_splits_large_requests(self, mock_client_class: MagicMock) -> None:
        """Test that statements batch requests are split when exceeding MAX_BATCH_SIZE."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = {}

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = DatasetAPIClient("sampleA")
        stock_codes = [str(1000 + i) for i in range(150)]
        _ = client.get_statements_batch(stock_codes)

        assert mock_httpx_client.request.call_count == 2

    @patch("httpx.Client")
    def test_get_statements_batch_fallback_on_error(self, mock_client_class: MagicMock) -> None:
        """Test that statements batch falls back to individual requests on error."""
        call_count = [0]

        def mock_request(*args: object, **kwargs: object) -> MagicMock:
            call_count[0] += 1
            response = MagicMock()
            if call_count[0] == 1:
                raise ConnectionError("batch endpoint not available")
            response.is_success = True
            response.json.return_value = [
                {"disclosedDate": "2024-05-10", "earningsPerShare": 150.5, "profit": 1000000, "equity": 5000000},
            ]
            return response

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.side_effect = mock_request
        mock_client_class.return_value = mock_httpx_client

        client = DatasetAPIClient("sampleA")
        result = client.get_statements_batch(["7203", "9984"])

        assert len(result) == 2
        assert "7203" in result
        assert "9984" in result
