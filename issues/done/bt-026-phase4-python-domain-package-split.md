---
id: bt-026
title: "Phase 4: Python ドメインパッケージ分離"
status: done
priority: medium
labels: [architecture, refactor]
project: bt
created: 2026-02-09
updated: 2026-02-10
closed: 2026-02-10
depends_on: []
blocks: []
parent: null
---

# bt-026 Phase 4: Python ドメインパッケージ分離

## 目的
`apps/bt/src` の DB/指標/バックテスト責務を明確な境界へ再配置し、`server` と CLI を thin adapter 化する。

## 受け入れ条件
- `apps/bt/src/lib/market_db`, `dataset_io`, `indicators`, `backtest_core`, `strategy_runtime` の境界が作成される
- `apps/bt/src/server` と `apps/bt/src/cli_*` が新境界を経由して依存する
- API/CLI の既存挙動に回帰がない（既存テスト + 追加回帰テストで確認）
- lint/typecheck/test が通る

## 実施内容
- Phase 4C: `src/server/db` と dataset I/O の再配置
- 指標計算・backtest 実行・strategy runtime の責務分割
- import 依存とモジュール境界の整理
- 段階移行中の互換 import 方針を定義

## 進捗
- [x] Step1: `apps/bt/src/lib/market_db`, `apps/bt/src/lib/dataset_io` を作成
- [x] Step1: `src/server/db/*` と `dataset_writer` を新境界へ実体移管
- [x] Step1: `src/server` 側 import を `src.lib.*` へ切替
- [x] Step1: `src/server/db/*` に互換 re-export を配置（段階移行）
- [x] Step2: `apps/bt/src/lib/indicators` 分離（`src/utils/indicators.py` は互換 facade 化）
- [x] Step2: `apps/bt/src/lib/backtest_core` 分離（`runner`/`marimo_executor`/`walkforward` 境界を追加）
- [x] Step2: `apps/bt/src/lib/strategy_runtime` 分離（`loader`/`parameter_extractor`/`validator` 等の境界を追加）
- [x] Step2: `src/server` と `src/cli_*` の `backtest` / `strategy_config` / `utils.indicators` 参照を `src.lib.*` へ切替
- [x] Step3: `src/server/db/*` 互換 re-export を削除し、テスト import を `src/lib/*` へ統一

## 結果
- 2026-02-09: Phase 4C Step1（DB + dataset I/O 分離）を完了
- `src/server/db` の実装本体を `src/lib/market_db` へ移管し、`DatasetWriter` を `src/lib/dataset_io` へ移管
- `src/server/routes` / `src/server/services` / `src/server/app.py` の参照先を新境界へ切替
- 2026-02-09: Phase 4C Step2（indicators / backtest_core / strategy_runtime 境界追加）を完了
- `src/server` / `src/cli_*` は `src.lib.backtest_core.*` / `src.lib.strategy_runtime.*` / `src.lib.indicators` を経由する構成へ移行
- 2026-02-09: Step2 追補として `ConfigLoader` / `BacktestRunner` / `MarimoExecutor` の実装本体を `src/lib/*` へ移管し、`src/strategy_config` / `src/backtest` は互換 facade へ移行
- 2026-02-10: `src/server/db/*.py` 互換 facade を削除し、`tests/unit/server/**` の参照先を `src/lib/*` に切替
- 検証結果
  - `uv run ruff check src tests`: passed
  - `uv run pyright src`: 0 errors（既存 warning 1）
  - `uv run pytest tests/security/test_security_validation.py tests/unit/backtest/test_backtest_runner.py tests/unit/backtest/test_marimo_executor.py tests/unit/backtest/test_walkforward.py tests/unit/backtest/test_manifest.py tests/unit/optimization/test_notebook_generator.py tests/unit/agent/test_yaml_updater.py tests/unit/server/routes/test_strategies.py tests/unit/lib/test_phase4c_import_boundaries.py`: 81 passed
  - `uv run pytest tests/server/routes/test_lab.py -k "execute_improve_sync"`: 2 passed
- 2026-02-10: Phase 4 完了に伴い本 Issue をクローズ。実装状態は `docs/unified-roadmap.md` に反映済み。

## 補足
- 参照: `docs/archive/unified-roadmap-2026-02-10.md` Phase 4（再ベースライン）
