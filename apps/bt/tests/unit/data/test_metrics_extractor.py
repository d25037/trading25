"""
Metrics Extractor Unit Tests
"""

from pathlib import Path

import pytest

from src.data.metrics_extractor import (
    BacktestMetrics,
    extract_metrics_from_html,
)


def _write_html(tmp_path: Path, name: str, content: str) -> Path:
    """テスト用HTMLファイルを作成して返す"""
    html_file = tmp_path / name
    html_file.write_text(content, encoding="utf-8")
    return html_file


class TestExtractMetricsFromHtml:
    """extract_metrics_from_html関数のテスト"""

    def test_extract_metrics_from_json_preferred(self, tmp_path: Path) -> None:
        """JSONが存在する場合はJSONを優先する"""
        html_file = _write_html(
            tmp_path,
            "metrics.html",
            "<html><table><tr><th>Total Return [%]</th><td>10.0</td></tr></table></html>",
        )
        json_path = html_file.with_suffix(".metrics.json")
        json_path.write_text(
            """{\"total_return\": 30.0, \"sharpe_ratio\": 1.2, \"total_trades\": 5}""",
            encoding="utf-8",
        )

        metrics = extract_metrics_from_html(html_file)

        assert metrics.total_return == 30.0
        assert metrics.sharpe_ratio == 1.2
        assert metrics.total_trades == 5

    def test_extract_missing_json_metrics_from_html(self, tmp_path: Path) -> None:
        """JSONに欠損がある場合はHTMLから不足分を補完する"""
        html_file = _write_html(
            tmp_path,
            "metrics.html",
            """
            <html><body><table>
                <tr><th>Total Return [%]</th><td>25.5</td></tr>
                <tr><th>Sortino Ratio</th><td>2.1</td></tr>
                <tr><th>Total Trades</th><td>100</td></tr>
            </table></body></html>
            """,
        )
        json_path = html_file.with_suffix(".metrics.json")
        json_path.write_text(
            """{"total_return": 30.0, "sharpe_ratio": 1.2}""",
            encoding="utf-8",
        )

        metrics = extract_metrics_from_html(html_file)

        assert metrics.total_return == 30.0
        assert metrics.sharpe_ratio == 1.2
        assert metrics.sortino_ratio == 2.1
        assert metrics.total_trades == 100

    def test_extract_metrics_from_valid_html(self, tmp_path: Path) -> None:
        """正常なHTMLからメトリクスを抽出できる"""
        html_file = _write_html(
            tmp_path,
            "test.html",
            """
            <html><body><table>
                <tr><th>Total Return [%]</th><td>25.5</td></tr>
                <tr><th>Max Drawdown [%]</th><td>-10.2</td></tr>
                <tr><th>Sharpe Ratio</th><td>1.5</td></tr>
                <tr><th>Sortino Ratio</th><td>2.1</td></tr>
                <tr><th>Calmar Ratio</th><td>2.5</td></tr>
                <tr><th>Win Rate [%]</th><td>55.0</td></tr>
                <tr><th>Profit Factor</th><td>1.8</td></tr>
                <tr><th>Total Trades</th><td>100</td></tr>
            </table></body></html>
            """,
        )

        metrics = extract_metrics_from_html(html_file)

        assert metrics.total_return == 25.5
        assert metrics.max_drawdown == -10.2
        assert metrics.sharpe_ratio == 1.5
        assert metrics.sortino_ratio == 2.1
        assert metrics.calmar_ratio == 2.5
        assert metrics.win_rate == 55.0
        assert metrics.profit_factor == 1.8
        assert metrics.total_trades == 100

    def test_extract_metrics_from_empty_file(self, tmp_path: Path) -> None:
        """空のファイルからは空のメトリクスを返す"""
        html_file = _write_html(tmp_path, "empty.html", "")
        metrics = extract_metrics_from_html(html_file)

        assert metrics.total_return is None
        assert metrics.sharpe_ratio is None

    def test_extract_metrics_from_nonexistent_file(self, tmp_path: Path) -> None:
        """存在しないファイルからは空のメトリクスを返す"""
        metrics = extract_metrics_from_html(tmp_path / "nonexistent.html")
        assert metrics.total_return is None

    def test_extract_metrics_with_unicode_escapes(self, tmp_path: Path) -> None:
        """Unicodeエスケープを含むHTMLからメトリクスを抽出できる"""
        html_file = _write_html(
            tmp_path,
            "unicode.html",
            "\\u003Chtml\\u003E\\u003Ctable\\u003E"
            "\\u003Ctr\\u003E\\u003Cth\\u003ETotal Return [%]\\u003C/th\\u003E"
            "\\u003Ctd\\u003E30.5\\u003C/td\\u003E\\u003C/tr\\u003E"
            "\\u003C/table\\u003E\\u003C/html\\u003E",
        )

        metrics = extract_metrics_from_html(html_file)
        assert metrics.total_return == 30.5

    def test_extract_metrics_with_invalid_values(self, tmp_path: Path) -> None:
        """不正な値を含むHTMLからはNoneを返す"""
        html_file = _write_html(
            tmp_path,
            "invalid.html",
            """
            <html><table>
                <tr><th>Total Return [%]</th><td>invalid</td></tr>
                <tr><th>Sharpe Ratio</th><td>N/A</td></tr>
            </table></html>
            """,
        )

        metrics = extract_metrics_from_html(html_file)
        assert metrics.total_return is None
        assert metrics.sharpe_ratio is None


class TestExtractKellyMetrics:
    """Kelly配分率抽出のテスト"""

    @pytest.mark.parametrize(
        ("percentage", "expected"),
        [
            ("34.5%", 0.345),
            ("0.0%", 0.0),
            ("100.0%", 1.0),
        ],
        ids=["normal", "zero", "full"],
    )
    def test_extract_kelly_allocation(
        self, tmp_path: Path, percentage: str, expected: float
    ) -> None:
        """Kelly配分率を正しく抽出できる"""
        html_file = _write_html(
            tmp_path,
            "kelly.html",
            f"<table><tr><td>最適配分率</td><td>{percentage}</td></tr></table>",
        )

        metrics = extract_metrics_from_html(html_file)
        assert metrics.optimal_allocation == pytest.approx(expected)

    def test_extract_kelly_allocation_invalid(self, tmp_path: Path) -> None:
        """不正な配分率値からはNoneを返す"""
        html_file = _write_html(
            tmp_path,
            "kelly_invalid.html",
            "<table><tr><td>最適配分率</td><td>N/A</td></tr></table>",
        )

        metrics = extract_metrics_from_html(html_file)
        assert metrics.optimal_allocation is None

    def test_extract_kelly_allocation_missing(self, tmp_path: Path) -> None:
        """Kelly情報がないHTMLからはNoneを返す"""
        html_file = _write_html(
            tmp_path,
            "no_kelly.html",
            "<table><tr><th>Total Return [%]</th><td>25.5</td></tr></table>",
        )

        metrics = extract_metrics_from_html(html_file)
        assert metrics.optimal_allocation is None

    def test_extract_kelly_with_unicode_escapes(self, tmp_path: Path) -> None:
        """UnicodeエスケープされたKelly配分率を抽出できる"""
        html_file = _write_html(
            tmp_path,
            "kelly_unicode.html",
            "\\u003Ctd\\u003E最適配分率\\u003C/td\\u003E"
            "\\u003Ctd\\u003E50.0%\\u003C/td\\u003E",
        )

        metrics = extract_metrics_from_html(html_file)
        assert metrics.optimal_allocation == pytest.approx(0.5)


class TestBacktestMetrics:
    """BacktestMetrics dataclassのテスト"""

    def test_default_values(self) -> None:
        metrics = BacktestMetrics()
        assert metrics.total_return is None
        assert metrics.max_drawdown is None
        assert metrics.total_trades is None

    def test_with_values(self) -> None:
        metrics = BacktestMetrics(total_return=25.5, sharpe_ratio=1.5)
        assert metrics.total_return == 25.5
        assert metrics.sharpe_ratio == 1.5
