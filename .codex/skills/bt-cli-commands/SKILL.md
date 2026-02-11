---
name: bt-cli-commands
description: bt と portfolio の Typer CLI 実装を扱うスキル。コマンド追加、オプション変更、ヘルプ文更新、CLI テスト修正時に使用する。
---

# bt-cli-commands

## Source of Truth

- `apps/bt/src/cli_bt/__init__.py`
- `apps/bt/src/cli_bt/lab.py`
- `apps/bt/src/cli_portfolio/__init__.py`
- Generated reference: `references/bt-cli-commands.md`

## Rules

- 既存サブコマンド体系（`bt`, `bt lab`, `portfolio`）を維持する。
- API 依存コマンドは FastAPI `:3002` 前提のヘルプを保つ。
- 新規オプション追加時はテストケースも同時更新する。
