---
id: bt-031
title: bt/ts CLI責務境界の明確化と統一運用
status: done
priority: high
labels: [architecture, cli, governance, integration]
project: bt
created: 2026-02-12
updated: 2026-02-12
depends_on: []
blocks: []
parent: null
---

# bt-031 bt/ts CLI責務境界の明確化と統一運用

## 目的
FastAPI を唯一バックエンドとする現行構成に合わせ、CLI責務を `apps/ts/packages/cli`（ユーザー向け）と `apps/bt`（実行基盤・運用向け）で明確化し、重複実装や運用上の混乱を防ぐ。

## 受け入れ条件
- bt/ts それぞれの CLI コマンドを「公開UI」「内部運用」「非推奨」に分類した一覧があること
- `apps/bt` 側 CLI のサポート方針（維持/縮小/段階的非推奨）が文書化されていること
- `apps/ts` 側を一次CLIとする運用ルール（README/AGENTS/Runbook）が一致していること
- ヘルプ/エラーメッセージ内の起動案内が現行構成（`uv run bt server --port 3002`）に統一されていること
- 互換性に影響する変更がある場合、移行手順と期限（deprecation window）が記載されていること

## 実施内容
- `apps/ts/packages/cli/src/` の実行時エラーメッセージを統一し、`bun dev:api` / `bun run dev:api` 案内を
  `uv run bt server --port 3002` に置換。
- `apps/ts/README.md` / `apps/ts/AGENTS.md` の主導線から `dev:api` を外し、
  `dev:api` は archived notice 用の互換スクリプトである旨を追記。
- `apps/ts/packages/cli/AGENTS.md` のエラーハンドリング例を現行起動案内に更新。
- `apps/bt/README.md` と `apps/bt/docs/commands.md` に責務境界を追記し、
  一次CLIは `apps/ts/packages/cli`、`apps/bt` CLIは実行基盤・運用向けであることを明文化。
- 追加対応（breaking）として、`apps/bt` の `portfolio` CLIエントリポイントと実装・専用テストを削除。
- スキル参照生成スクリプト（`scripts/skills/refresh_skill_references.py`）から
  `cli_portfolio` 依存を除去し、`bt-cli-commands` 参照を更新。

## 結果
- 受け入れ条件のうち、以下を満たした:
  - CLI/ドキュメントの起動案内を `uv run bt server --port 3002` に統一
  - `apps/ts` を一次CLI運用とする文書整合
  - `apps/bt` CLI の運用向け位置付けを文書化
  - breaking 方針として `portfolio` CLI を撤去し、ポートフォリオ操作を `apps/ts/packages/cli` に一本化
- 検証結果:
  - `rg -n "bun run dev:api|bun dev:api" apps/ts/packages/cli/src -S` で一致 0 件
  - `bun run --filter @trading25/cli typecheck` 成功
  - `bun run --filter @trading25/cli test` 成功（43 pass）
  - `bun run --filter @trading25/cli build` 成功
  - `python3 scripts/skills/refresh_skill_references.py --check` 成功
  - `uv run --project apps/bt pytest apps/bt/tests/unit/cli_bt apps/bt/tests/unit/data/test_portfolio_loaders.py` 成功（20 pass）
  - `uv run --project apps/bt bt --help` 成功、`uv run --project apps/bt portfolio --help` は未定義として失敗（期待どおり）
  - `uv run --project apps/bt pytest apps/bt/tests/unit/scripts/test_refresh_skill_references.py` 成功（7 pass）
  - `uv run --project apps/bt pytest ... --cov=refresh_skill_references --cov-branch` で
    `scripts/skills/refresh_skill_references.py` のカバレッジ 85%（line+branch）
- OpenAPI/contract 変更はなく、`bt:sync` と `contracts/` 更新は不要。

## 補足
- 現状、`cli_market` 系は `bt-024` で `apps/ts/cli` に一本化済み。
- 本Issueは「bt CLI全廃」ではなく、責務境界の明確化と運用統一を対象とする。
