"""
JQuants CLI Commands

Thin debug-oriented commands for calling FastAPI JQuants proxy endpoints.
"""

from __future__ import annotations

import csv
from datetime import date, timedelta
import json
import os
from pathlib import Path
from typing import Any

import httpx
import typer
from rich.console import Console
from rich.table import Table

console = Console()

DEFAULT_BT_API_URL = os.getenv("BT_API_URL", "http://localhost:3002")
DEFAULT_OUTPUT_DIR = Path("data")

jquants_app = typer.Typer(
    name="jquants",
    help="JQuants proxy debug commands",
    rich_markup_mode="rich",
)

auth_app = typer.Typer(
    name="auth",
    help="JQuants auth status/cache commands",
    rich_markup_mode="rich",
)

fetch_app = typer.Typer(
    name="fetch",
    help="Fetch data through /api/jquants/*",
    rich_markup_mode="rich",
)

jquants_app.add_typer(auth_app, name="auth")
jquants_app.add_typer(fetch_app, name="fetch")


def _request_json(
    bt_url: str,
    path: str,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    try:
        with httpx.Client(base_url=bt_url, timeout=60.0) as client:
            response = client.get(path, params=params)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise RuntimeError("API response is not an object")
            return payload
    except httpx.HTTPStatusError as exc:
        detail = exc.response.text
        try:
            body = exc.response.json()
            if isinstance(body, dict):
                message = body.get("message")
                error = body.get("error")
                if isinstance(message, str) and message:
                    detail = message
                elif isinstance(error, str) and error:
                    detail = error
        except Exception:
            pass
        console.print(f"[red]HTTP {exc.response.status_code} {path}: {detail}[/red]")
        raise typer.Exit(code=1) from None
    except httpx.HTTPError as exc:
        console.print(f"[red]Failed to reach bt API at {bt_url}: {exc}[/red]")
        raise typer.Exit(code=1) from None


def _ensure_output_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def _write_json(data: Any, output_dir: Path, filename: str) -> Path:
    target = _ensure_output_dir(output_dir) / filename
    target.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return target


def _write_csv(rows: list[dict[str, Any]], output_dir: Path, filename: str) -> Path:
    target = _ensure_output_dir(output_dir) / filename
    if not rows:
        target.write_text("", encoding="utf-8")
        return target

    # Stable field order based on first row
    fieldnames = list(rows[0].keys())
    with target.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return target


def _today_label() -> str:
    return date.today().isoformat()


def _print_fetch_summary(title: str, count: int, output_path: Path | None = None) -> None:
    table = Table(title=title, show_header=False)
    table.add_column("key", style="cyan")
    table.add_column("value", style="white")
    table.add_row("records", str(count))
    if output_path is not None:
        table.add_row("output", str(output_path))
    console.print(table)


def _find_repo_root(start: Path) -> Path | None:
    for current in (start, *start.parents):
        if (current / ".git").exists():
            return current
    return None


def _find_env_file() -> Path | None:
    # Monorepo SoT: always target repository root .env
    repo_root = _find_repo_root(Path(__file__).resolve())
    if repo_root is None:
        return None
    env_path = repo_root / ".env"
    if env_path.exists():
        return env_path
    return None


def _has_local_api_key(env_file: Path | None) -> bool:
    if env_file is not None and env_file.exists():
        try:
            for raw in env_file.read_text(encoding="utf-8").splitlines():
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("JQUANTS_API_KEY="):
                    _, value = line.split("=", 1)
                    return len(value.strip().strip('"').strip("'")) > 0
        except Exception:
            pass
    return len(os.getenv("JQUANTS_API_KEY", "").strip()) > 0


def _clear_local_api_key(env_file: Path) -> bool:
    lines = env_file.read_text(encoding="utf-8").splitlines()
    kept: list[str] = []
    removed = False
    for line in lines:
        if line.strip().startswith("JQUANTS_API_KEY="):
            removed = True
            continue
        kept.append(line)
    env_file.write_text("\n".join(kept) + ("\n" if kept else ""), encoding="utf-8")
    return removed


@auth_app.command("status")
def auth_status(
    bt_url: str = typer.Option(DEFAULT_BT_API_URL, "--bt-url", help="bt FastAPI URL"),
) -> None:
    """Show local key status and /api/jquants/auth/status."""
    env_file = _find_env_file()
    local_key = _has_local_api_key(env_file)
    api_status = _request_json(bt_url, "/api/jquants/auth/status")

    table = Table(title="JQuants Auth Status", show_header=False)
    table.add_column("key", style="cyan")
    table.add_column("value", style="white")
    table.add_row("env file", str(env_file) if env_file else "(not found)")
    table.add_row("local JQUANTS_API_KEY", "yes" if local_key else "no")
    table.add_row("api authenticated", "yes" if bool(api_status.get("authenticated")) else "no")
    table.add_row("api hasApiKey", "yes" if bool(api_status.get("hasApiKey")) else "no")
    console.print(table)


@auth_app.command("clear")
def auth_clear() -> None:
    """Clear JQUANTS_API_KEY from nearest .env."""
    env_file = _find_env_file()
    if env_file is None:
        console.print("[red]No .env file found.[/red]")
        raise typer.Exit(code=1)

    removed = _clear_local_api_key(env_file)
    if removed:
        console.print(f"[green]Cleared JQUANTS_API_KEY from {env_file}[/green]")
    else:
        console.print(f"[yellow]JQUANTS_API_KEY not found in {env_file}[/yellow]")


@auth_app.command("refresh-tokens")
def auth_refresh_tokens(
    bt_url: str = typer.Option(DEFAULT_BT_API_URL, "--bt-url", help="bt FastAPI URL"),
) -> None:
    """Alias of auth status."""
    auth_status(bt_url=bt_url)


@fetch_app.command("daily-quotes")
def fetch_daily_quotes(
    code: str = typer.Argument(..., help="Stock code"),
    date_value: str | None = typer.Option(None, "--date", help="Date YYYY-MM-DD"),
    date_from: str | None = typer.Option(None, "--from", help="From YYYY-MM-DD"),
    date_to: str | None = typer.Option(None, "--to", help="To YYYY-MM-DD"),
    csv_output: bool = typer.Option(False, "--csv", help="Write CSV"),
    json_output: bool = typer.Option(False, "--json", help="Write JSON"),
    output_dir: Path = typer.Option(DEFAULT_OUTPUT_DIR, "--output", help="Output directory"),
    bt_url: str = typer.Option(DEFAULT_BT_API_URL, "--bt-url", help="bt FastAPI URL"),
) -> None:
    """Fetch /api/jquants/daily-quotes."""
    payload = _request_json(
        bt_url,
        "/api/jquants/daily-quotes",
        params={"code": code, "date": date_value, "from": date_from, "to": date_to},
    )
    rows = payload.get("data", [])
    if not isinstance(rows, list):
        rows = []

    written: Path | None = None
    if csv_output:
        safe_rows = [row for row in rows if isinstance(row, dict)]
        written = _write_csv(safe_rows, output_dir, f"{code}_daily_{_today_label()}.csv")
    elif json_output:
        written = _write_json(payload, output_dir, f"{code}_daily_{_today_label()}.json")

    _print_fetch_summary("Daily Quotes", len(rows), written)


@fetch_app.command("listed-info")
def fetch_listed_info(
    code: str | None = typer.Argument(None, help="Optional stock code"),
    date_value: str | None = typer.Option(None, "--date", help="Date YYYY-MM-DD"),
    csv_output: bool = typer.Option(False, "--csv", help="Write CSV"),
    json_output: bool = typer.Option(False, "--json", help="Write JSON"),
    output_dir: Path = typer.Option(DEFAULT_OUTPUT_DIR, "--output", help="Output directory"),
    bt_url: str = typer.Option(DEFAULT_BT_API_URL, "--bt-url", help="bt FastAPI URL"),
) -> None:
    """Fetch /api/jquants/listed-info."""
    payload = _request_json(
        bt_url,
        "/api/jquants/listed-info",
        params={"code": code, "date": date_value},
    )
    rows = payload.get("info", [])
    if not isinstance(rows, list):
        rows = []

    written: Path | None = None
    if csv_output:
        safe_rows = [row for row in rows if isinstance(row, dict)]
        written = _write_csv(safe_rows, output_dir, f"listed_info_{_today_label()}.csv")
    elif json_output:
        written = _write_json(payload, output_dir, f"listed_info_{_today_label()}.json")

    _print_fetch_summary("Listed Info", len(rows), written)


@fetch_app.command("margin")
def fetch_margin_interest(
    code: str = typer.Argument(..., help="Stock code"),
    date_value: str | None = typer.Option(None, "--date", help="Date YYYY-MM-DD"),
    date_from: str | None = typer.Option(None, "--from", help="From YYYY-MM-DD"),
    date_to: str | None = typer.Option(None, "--to", help="To YYYY-MM-DD"),
    csv_output: bool = typer.Option(False, "--csv", help="Write CSV"),
    json_output: bool = typer.Option(False, "--json", help="Write JSON"),
    output_dir: Path = typer.Option(DEFAULT_OUTPUT_DIR, "--output", help="Output directory"),
    bt_url: str = typer.Option(DEFAULT_BT_API_URL, "--bt-url", help="bt FastAPI URL"),
) -> None:
    """Fetch /api/jquants/stocks/{code}/margin-interest."""
    payload = _request_json(
        bt_url,
        f"/api/jquants/stocks/{code}/margin-interest",
        params={"date": date_value, "from": date_from, "to": date_to},
    )
    rows = payload.get("marginInterest", [])
    if not isinstance(rows, list):
        rows = []

    written: Path | None = None
    if csv_output:
        safe_rows = [row for row in rows if isinstance(row, dict)]
        written = _write_csv(safe_rows, output_dir, f"{code}_margin_{_today_label()}.csv")
    elif json_output:
        written = _write_json(payload, output_dir, f"{code}_margin_{_today_label()}.json")

    _print_fetch_summary("Margin Interest", len(rows), written)


@fetch_app.command("indices")
def fetch_indices(
    code: str | None = typer.Option(None, "--code", help="Index code"),
    date_value: str | None = typer.Option(None, "--date", help="Date YYYY-MM-DD"),
    date_from: str | None = typer.Option(None, "--from", help="From YYYY-MM-DD"),
    date_to: str | None = typer.Option(None, "--to", help="To YYYY-MM-DD"),
    csv_output: bool = typer.Option(False, "--csv", help="Write CSV"),
    json_output: bool = typer.Option(False, "--json", help="Write JSON"),
    output_dir: Path = typer.Option(DEFAULT_OUTPUT_DIR, "--output", help="Output directory"),
    bt_url: str = typer.Option(DEFAULT_BT_API_URL, "--bt-url", help="bt FastAPI URL"),
) -> None:
    """Fetch /api/jquants/indices."""
    payload = _request_json(
        bt_url,
        "/api/jquants/indices",
        params={"code": code, "date": date_value, "from": date_from, "to": date_to},
    )
    rows = payload.get("indices", [])
    if not isinstance(rows, list):
        rows = []

    written: Path | None = None
    if csv_output:
        safe_rows = [row for row in rows if isinstance(row, dict)]
        written = _write_csv(safe_rows, output_dir, f"indices_{_today_label()}.csv")
    elif json_output:
        written = _write_json(payload, output_dir, f"indices_{_today_label()}.json")

    _print_fetch_summary("Indices", len(rows), written)


@fetch_app.command("topix")
def fetch_topix(
    date_value: str | None = typer.Option(None, "--date", help="Date YYYY-MM-DD"),
    date_from: str | None = typer.Option(None, "--from", help="From YYYY-MM-DD"),
    date_to: str | None = typer.Option(None, "--to", help="To YYYY-MM-DD"),
    csv_output: bool = typer.Option(False, "--csv", help="Write CSV"),
    json_output: bool = typer.Option(False, "--json", help="Write JSON"),
    output_dir: Path = typer.Option(DEFAULT_OUTPUT_DIR, "--output", help="Output directory"),
    bt_url: str = typer.Option(DEFAULT_BT_API_URL, "--bt-url", help="bt FastAPI URL"),
) -> None:
    """Fetch /api/jquants/topix."""
    payload = _request_json(
        bt_url,
        "/api/jquants/topix",
        params={"date": date_value, "from": date_from, "to": date_to},
    )
    rows = payload.get("topix", [])
    if not isinstance(rows, list):
        rows = []

    written: Path | None = None
    if csv_output:
        safe_rows = [row for row in rows if isinstance(row, dict)]
        written = _write_csv(safe_rows, output_dir, f"topix_{_today_label()}.csv")
    elif json_output:
        written = _write_json(payload, output_dir, f"topix_{_today_label()}.json")

    _print_fetch_summary("TOPIX", len(rows), written)


@fetch_app.command("test-data")
def fetch_test_data(
    days: int = typer.Option(365, "--days", min=1, help="Number of days"),
    output_dir: Path = typer.Option(DEFAULT_OUTPUT_DIR, "--output", help="Output directory"),
    bt_url: str = typer.Option(DEFAULT_BT_API_URL, "--bt-url", help="bt FastAPI URL"),
) -> None:
    """Fetch Toyota(7203) daily quotes for test fixtures."""
    end_date = date.today()
    start_date = end_date - timedelta(days=days)
    payload = _request_json(
        bt_url,
        "/api/jquants/daily-quotes",
        params={
            "code": "7203",
            "from": start_date.isoformat(),
            "to": end_date.isoformat(),
        },
    )
    rows = payload.get("data", [])
    if not isinstance(rows, list):
        rows = []

    safe_rows = [row for row in rows if isinstance(row, dict)]
    written = _write_csv(safe_rows, output_dir, "toyota_7203_daily.csv")
    _print_fetch_summary("Toyota Test Data", len(rows), written)
