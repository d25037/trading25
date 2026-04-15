---
name: bt-cli-commands
description: bt Typer CLI 実装を扱うスキル。コマンド追加、オプション変更、ヘルプ文更新、CLI テスト修正時に使用する。
---

# bt-cli-commands

## When to use

- `bt` / `bt lab` のサブコマンド、オプション、help 文、CLI テストを変更するとき。
- `bt intraday-sync` のような local DuckDB / J-Quants maintenance command を追加・変更するとき。

## Source of Truth

- `apps/bt/src/entrypoints/cli/__init__.py`
- `apps/bt/src/entrypoints/cli/lab.py`
- `apps/bt/tests/unit/cli`
- `apps/bt/tests/unit/cli_bt`
- `references/bt-cli-commands.md`

## Workflow

1. 既存サブコマンド体系（`bt`, `bt lab`）への追加か変更かを先に決める。
2. help 文、option 名、実行エントリを Typer 定義と一致させる。
3. 生成済みコマンド一覧と CLI テストを同時更新する。
4. Portfolio 操作は web UI 前提のままにし、廃止済み CLI を戻さない。
5. market data maintenance command は FastAPI 経由か direct data-plane access かを明示し、SoT を混ぜない。

## Guardrails

- API 依存コマンドは FastAPI `:3002` 前提のヘルプを保つ。
- 新規オプション追加時はテストケースも同時更新する。
- `bt` 直下と `bt lab` 配下の責務を混在させない。
- `bt intraday-sync` は local DuckDB + J-Quants client を直接使う maintenance command で、daily sync job API とは責務を分ける。

## Verification

- `python3 scripts/skills/refresh_skill_references.py --check`
- `uv run --project apps/bt pytest tests/unit/cli tests/unit/cli_bt`
- `uv run --project apps/bt ruff check src/entrypoints/cli`
- `uv run --project apps/bt pyright src/entrypoints/cli`
