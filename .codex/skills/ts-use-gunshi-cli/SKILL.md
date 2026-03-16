---
name: ts-use-gunshi-cli
description: 廃止済み ts CLI 参照を整理し、headless 運用を bt CLI に寄せるためのスキル。旧 ts/cli ドキュメントや参照の移行時に使用する。
---

# ts-use-gunshi-cli

## When to use

- 旧 ts CLI package 参照を cleanup するとき。
- headless 操作を `apps/bt` の `bt` CLI に寄せる doc や skill を更新するとき。

## Source of Truth

- `docs/ts-cli-scope.md`
- `apps/ts/AGENTS.md`
- `apps/ts/README.md`
- `apps/bt/src/entrypoints/cli`

## Workflow

1. 要求された変更が本当に ts 側 CLI なのか、bt CLI へ寄せるべきものかを先に判定する。
2. 旧 ts CLI 参照は `docs/ts-cli-scope.md` の方針に合わせて `bt` コマンドへ置き換える。
3. removed ts CLI package を再導入せず、必要な headless 操作は bt 側の CLI へ誘導する。

## Guardrails

- removed ts CLI package を復活させない。
- `gunshi` や他の CLI 依存を ts workspace に新規追加しない。
- 日常操作は web UI、headless 操作は bt CLI という導線を維持する。

## Verification

- `bun run quality:deps:audit`
- `rg -n "packages/cli" apps/ts/AGENTS.md apps/ts/README.md docs/ts-cli-scope.md .codex/skills`
