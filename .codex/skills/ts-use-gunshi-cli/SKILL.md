---
name: ts-use-gunshi-cli
description: ts/cli 実装で Gunshi を標準採用するためのスキル。新規コマンド追加、コマンド階層変更、ヘルプ整備、CLI UX 改修時に使用する。
---

# ts-use-gunshi-cli

ts 側 CLI 実装では `gunshi` を優先し、`yargs`/`commander`/`cac` などの別ライブラリを新規導入しない。

## Source of Truth

- Entry point: `apps/ts/packages/cli/src/index.ts`
- Command groups: `apps/ts/packages/cli/src/commands/**`
- Shared constants/errors: `apps/ts/packages/cli/src/utils/**`

## Command Topology (Current)

- `db`
- `dataset`
- `analysis` (`analyze` alias)
- `jquants`
- `portfolio`
- `watchlist`
- `backtest` (`bt` alias)

## Implementation Pattern

1. ルートは `define` で宣言し、グループごとに分離する。
2. グループは `cli(args, command, { subCommands })` で実行する。
3. help 表示のためサブコマンドは direct import または lazy import を目的に応じて選ぶ。
4. エラーは `CLIError` ベースで統一する。

## References

- `apps/ts/package.json`
- `apps/ts/packages/cli/src/**`
- `apps/ts/node_modules/@gunshi/docs/**.md`
