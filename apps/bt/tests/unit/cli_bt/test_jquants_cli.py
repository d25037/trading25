"""Tests for bt jquants CLI commands."""

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import patch

import httpx
import pytest
import typer
from typer.testing import CliRunner

from src.entrypoints.cli import jquants as jquants_module

runner = CliRunner()


def test_jquants_help_lists_auth_and_fetch() -> None:
    result = runner.invoke(jquants_module.jquants_app, ["--help"])
    assert result.exit_code == 0
    assert "auth" in result.stdout
    assert "fetch" in result.stdout


def test_jquants_fetch_daily_quotes_writes_json(tmp_path: Path) -> None:
    payload = {"data": [{"Date": "2026-03-03", "Code": "7203"}]}

    with (
        patch("src.entrypoints.cli.jquants._request_json", return_value=payload),
        patch("src.entrypoints.cli.jquants._today_label", return_value="2026-03-03"),
    ):
        result = runner.invoke(
            jquants_module.jquants_app,
            [
                "fetch",
                "daily-quotes",
                "7203",
                "--json",
                "--output",
                str(tmp_path),
            ],
        )

    assert result.exit_code == 0
    output_path = tmp_path / "7203_daily_2026-03-03.json"
    assert output_path.exists()
    written = json.loads(output_path.read_text(encoding="utf-8"))
    assert written["data"][0]["Code"] == "7203"


def test_jquants_refresh_tokens_alias_calls_status() -> None:
    with patch("src.entrypoints.cli.jquants.auth_status") as mock_status:
        result = runner.invoke(jquants_module.jquants_app, ["auth", "refresh-tokens"])

    assert result.exit_code == 0
    mock_status.assert_called_once_with(bt_url=jquants_module.DEFAULT_BT_API_URL)


def test_find_env_file_uses_repo_root_env(monkeypatch, tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("JQUANTS_API_KEY=test\n", encoding="utf-8")
    monkeypatch.setattr(jquants_module, "_find_repo_root", lambda _start: tmp_path)

    assert jquants_module._find_env_file() == env_file


def test_auth_clear_removes_key_from_env(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("FOO=1\nJQUANTS_API_KEY=abc\nBAR=2\n", encoding="utf-8")

    with patch("src.entrypoints.cli.jquants._find_env_file", return_value=env_file):
        result = runner.invoke(jquants_module.jquants_app, ["auth", "clear"])

    assert result.exit_code == 0
    content = env_file.read_text(encoding="utf-8")
    assert "JQUANTS_API_KEY=" not in content
    assert "FOO=1" in content
    assert "BAR=2" in content


def test_auth_clear_fails_when_env_missing() -> None:
    with patch("src.entrypoints.cli.jquants._find_env_file", return_value=None):
        result = runner.invoke(jquants_module.jquants_app, ["auth", "clear"])

    assert result.exit_code == 1
    assert "No .env file found" in result.stdout


def test_auth_clear_reports_when_key_not_found(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("FOO=1\nBAR=2\n", encoding="utf-8")

    with patch("src.entrypoints.cli.jquants._find_env_file", return_value=env_file):
        result = runner.invoke(jquants_module.jquants_app, ["auth", "clear"])

    assert result.exit_code == 0
    assert "JQUANTS_API_KEY not found" in result.stdout


def test_auth_status_renders_local_and_api_status(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("JQUANTS_API_KEY=abc\n", encoding="utf-8")
    with (
        patch("src.entrypoints.cli.jquants._find_env_file", return_value=env_file),
        patch("src.entrypoints.cli.jquants._has_local_api_key", return_value=True),
        patch(
            "src.entrypoints.cli.jquants._request_json",
            return_value={"authenticated": True, "hasApiKey": True},
        ),
    ):
        result = runner.invoke(jquants_module.jquants_app, ["auth", "status"])

    assert result.exit_code == 0
    assert "JQuants Auth Status" in result.stdout
    assert "api authenticated" in result.stdout


def test_request_json_raises_exit_on_http_status_error() -> None:
    request = httpx.Request("GET", "http://localhost:3002/api/jquants/auth/status")
    response = httpx.Response(502, request=request, json={"message": "upstream error"})

    class DummyClient:
        def __enter__(self):  # noqa: ANN204
            return self

        def __exit__(self, *_args):  # noqa: ANN204
            return False

        def get(self, _path: str, params=None):  # noqa: ANN001, ANN201
            return response

    with patch("src.entrypoints.cli.jquants.httpx.Client", return_value=DummyClient()):
        with pytest.raises(typer.Exit):
            jquants_module._request_json("http://localhost:3002", "/api/jquants/auth/status")


def test_request_json_raises_exit_on_connection_error() -> None:
    request = httpx.Request("GET", "http://localhost:3002/api/jquants/auth/status")

    class DummyClient:
        def __enter__(self):  # noqa: ANN204
            return self

        def __exit__(self, *_args):  # noqa: ANN204
            return False

        def get(self, _path: str, params=None):  # noqa: ANN001, ANN201
            raise httpx.ConnectError("connection failed", request=request)

    with patch("src.entrypoints.cli.jquants.httpx.Client", return_value=DummyClient()):
        with pytest.raises(typer.Exit):
            jquants_module._request_json("http://localhost:3002", "/api/jquants/auth/status")


def test_write_csv_handles_empty_rows(tmp_path: Path) -> None:
    path = jquants_module._write_csv([], tmp_path, "empty.csv")
    assert path.exists()
    assert path.read_text(encoding="utf-8") == ""


def test_has_local_api_key_reads_env_file_and_falls_back_to_process_env(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("# comment\nJQUANTS_API_KEY='abc'\n", encoding="utf-8")
    assert jquants_module._has_local_api_key(env_file) is True

    env_file.write_text("JQUANTS_API_KEY=\n", encoding="utf-8")
    assert jquants_module._has_local_api_key(env_file) is False

    with patch.dict(os.environ, {"JQUANTS_API_KEY": "from-process"}, clear=False):
        assert jquants_module._has_local_api_key(None) is True


def test_fetch_listed_info_writes_csv(tmp_path: Path) -> None:
    payload = {"info": [{"code": "7203", "companyName": "Toyota"}]}
    with (
        patch("src.entrypoints.cli.jquants._request_json", return_value=payload),
        patch("src.entrypoints.cli.jquants._today_label", return_value="2026-03-03"),
    ):
        result = runner.invoke(
            jquants_module.jquants_app,
            ["fetch", "listed-info", "--csv", "--output", str(tmp_path)],
        )

    assert result.exit_code == 0
    output_path = tmp_path / "listed_info_2026-03-03.csv"
    assert output_path.exists()
    assert "companyName" in output_path.read_text(encoding="utf-8")


def test_fetch_margin_writes_json(tmp_path: Path) -> None:
    payload = {"marginInterest": [{"date": "2026-03-03", "code": "7203"}]}
    with (
        patch("src.entrypoints.cli.jquants._request_json", return_value=payload),
        patch("src.entrypoints.cli.jquants._today_label", return_value="2026-03-03"),
    ):
        result = runner.invoke(
            jquants_module.jquants_app,
            ["fetch", "margin", "7203", "--json", "--output", str(tmp_path)],
        )

    assert result.exit_code == 0
    output_path = tmp_path / "7203_margin_2026-03-03.json"
    assert output_path.exists()


def test_fetch_indices_writes_json_and_handles_non_list_payload(tmp_path: Path) -> None:
    payload = {"indices": {"not": "a list"}}
    with (
        patch("src.entrypoints.cli.jquants._request_json", return_value=payload),
        patch("src.entrypoints.cli.jquants._today_label", return_value="2026-03-03"),
    ):
        result = runner.invoke(
            jquants_module.jquants_app,
            ["fetch", "indices", "--json", "--output", str(tmp_path)],
        )

    assert result.exit_code == 0
    output_path = tmp_path / "indices_2026-03-03.json"
    assert output_path.exists()


def test_fetch_topix_writes_csv(tmp_path: Path) -> None:
    payload = {"topix": [{"Date": "2026-03-03", "Close": 1000}]}
    with (
        patch("src.entrypoints.cli.jquants._request_json", return_value=payload),
        patch("src.entrypoints.cli.jquants._today_label", return_value="2026-03-03"),
    ):
        result = runner.invoke(
            jquants_module.jquants_app,
            ["fetch", "topix", "--csv", "--output", str(tmp_path)],
        )

    assert result.exit_code == 0
    output_path = tmp_path / "topix_2026-03-03.csv"
    assert output_path.exists()


def test_fetch_test_data_writes_csv(tmp_path: Path) -> None:
    payload = {"data": [{"Date": "2026-03-03", "Code": "7203", "Close": 1000}]}
    with patch("src.entrypoints.cli.jquants._request_json", return_value=payload):
        result = runner.invoke(
            jquants_module.jquants_app,
            ["fetch", "test-data", "--days", "10", "--output", str(tmp_path)],
        )

    assert result.exit_code == 0
    output_path = tmp_path / "toyota_7203_daily.csv"
    assert output_path.exists()


def test_request_json_success_and_non_object_payload() -> None:
    request = httpx.Request("GET", "http://localhost:3002/api/jquants/auth/status")

    class SuccessClient:
        def __enter__(self):  # noqa: ANN204
            return self

        def __exit__(self, *_args):  # noqa: ANN204
            return False

        def get(self, _path: str, params=None):  # noqa: ANN001, ANN201
            return httpx.Response(200, request=request, json={"ok": True})

    with patch("src.entrypoints.cli.jquants.httpx.Client", return_value=SuccessClient()):
        assert jquants_module._request_json("http://localhost:3002", "/api/jquants/auth/status") == {"ok": True}

    class NonObjectClient:
        def __enter__(self):  # noqa: ANN204
            return self

        def __exit__(self, *_args):  # noqa: ANN204
            return False

        def get(self, _path: str, params=None):  # noqa: ANN001, ANN201
            return httpx.Response(200, request=request, json=[1, 2, 3])

    with patch("src.entrypoints.cli.jquants.httpx.Client", return_value=NonObjectClient()):
        with pytest.raises(RuntimeError):
            jquants_module._request_json("http://localhost:3002", "/api/jquants/auth/status")


def test_find_repo_root_and_today_label(tmp_path: Path) -> None:
    nested = tmp_path / "a" / "b"
    nested.mkdir(parents=True)
    (tmp_path / ".git").mkdir()
    assert jquants_module._find_repo_root(nested) == tmp_path
    assert len(jquants_module._today_label()) == 10


def test_find_env_file_returns_none_for_missing_repo_or_env(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(jquants_module, "_find_repo_root", lambda _start: None)
    assert jquants_module._find_env_file() is None

    monkeypatch.setattr(jquants_module, "_find_repo_root", lambda _start: tmp_path)
    assert jquants_module._find_env_file() is None


def test_fetch_daily_quotes_csv_and_non_list_payload(tmp_path: Path) -> None:
    with (
        patch("src.entrypoints.cli.jquants._request_json", return_value={"data": {"x": 1}}),
        patch("src.entrypoints.cli.jquants._today_label", return_value="2026-03-03"),
    ):
        result = runner.invoke(
            jquants_module.jquants_app,
            ["fetch", "daily-quotes", "7203", "--csv", "--output", str(tmp_path)],
        )

    assert result.exit_code == 0
    output_path = tmp_path / "7203_daily_2026-03-03.csv"
    assert output_path.exists()
    assert output_path.read_text(encoding="utf-8") == ""


def test_fetch_daily_quotes_without_output_file(tmp_path: Path) -> None:
    with patch("src.entrypoints.cli.jquants._request_json", return_value={"data": []}):
        result = runner.invoke(
            jquants_module.jquants_app,
            ["fetch", "daily-quotes", "7203", "--output", str(tmp_path)],
        )
    assert result.exit_code == 0
    assert list(tmp_path.glob("*.csv")) == []
    assert list(tmp_path.glob("*.json")) == []


def test_fetch_listed_info_json_and_non_list_payload(tmp_path: Path) -> None:
    with (
        patch("src.entrypoints.cli.jquants._request_json", return_value={"info": {"x": 1}}),
        patch("src.entrypoints.cli.jquants._today_label", return_value="2026-03-03"),
    ):
        result = runner.invoke(
            jquants_module.jquants_app,
            ["fetch", "listed-info", "--json", "--output", str(tmp_path)],
        )

    assert result.exit_code == 0
    assert (tmp_path / "listed_info_2026-03-03.json").exists()


def test_fetch_margin_csv_and_non_list_payload(tmp_path: Path) -> None:
    with (
        patch("src.entrypoints.cli.jquants._request_json", return_value={"marginInterest": {"x": 1}}),
        patch("src.entrypoints.cli.jquants._today_label", return_value="2026-03-03"),
    ):
        result = runner.invoke(
            jquants_module.jquants_app,
            ["fetch", "margin", "7203", "--csv", "--output", str(tmp_path)],
        )

    assert result.exit_code == 0
    assert (tmp_path / "7203_margin_2026-03-03.csv").exists()


def test_fetch_indices_csv_with_list_payload(tmp_path: Path) -> None:
    payload = {"indices": [{"Date": "2026-03-03", "Code": "0040"}]}
    with (
        patch("src.entrypoints.cli.jquants._request_json", return_value=payload),
        patch("src.entrypoints.cli.jquants._today_label", return_value="2026-03-03"),
    ):
        result = runner.invoke(
            jquants_module.jquants_app,
            ["fetch", "indices", "--csv", "--output", str(tmp_path)],
        )

    assert result.exit_code == 0
    assert (tmp_path / "indices_2026-03-03.csv").exists()


def test_fetch_topix_json_and_non_list_payload(tmp_path: Path) -> None:
    with (
        patch("src.entrypoints.cli.jquants._request_json", return_value={"topix": {"x": 1}}),
        patch("src.entrypoints.cli.jquants._today_label", return_value="2026-03-03"),
    ):
        result = runner.invoke(
            jquants_module.jquants_app,
            ["fetch", "topix", "--json", "--output", str(tmp_path)],
        )

    assert result.exit_code == 0
    assert (tmp_path / "topix_2026-03-03.json").exists()


def test_fetch_test_data_handles_non_list_payload(tmp_path: Path) -> None:
    with patch("src.entrypoints.cli.jquants._request_json", return_value={"data": {"x": 1}}):
        result = runner.invoke(
            jquants_module.jquants_app,
            ["fetch", "test-data", "--output", str(tmp_path)],
        )

    assert result.exit_code == 0
    output_path = tmp_path / "toyota_7203_daily.csv"
    assert output_path.exists()
    assert output_path.read_text(encoding="utf-8") == ""
