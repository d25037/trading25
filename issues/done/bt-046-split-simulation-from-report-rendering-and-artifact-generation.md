---
id: bt-046
title: "Simulation と report rendering / artifact generation を分離"
status: done
priority: medium
labels: [artifacts, reports, marimo, execution, bt]
project: bt
created: 2026-03-08
updated: 2026-03-10
depends_on: [bt-039, bt-041]
blocks: []
parent: bt-037
---

# bt-046 Simulation と report rendering / artifact generation を分離

## 目的
- `BacktestRunner -> MarimoExecutor` に近い現行構造を解き、simulation を report export から分離する。
- HTML を presentation artifact に限定し、canonical result を先に確定させる。

## 受け入れ条件
- [x] simulation 完了時点で canonical result と core artifacts が確定する。
- [x] HTML / notebook render は後段 renderer として動作する。
- [x] result summary は HTML 依存なしで再解決できる。
- [x] report 生成失敗時も simulation result 自体は保持される。

## 実施内容
- [x] `BacktestRunner` から simulation と report rendering を分離する。
- [x] artifact writer / renderer の役割を分ける。
- [x] `result.html`, `metrics.json`, `manifest.json` の責務を再定義する。
- [x] 回帰テストと smoke フローを更新する。

## 結果
- 2026-03-10: `apps/bt/src/domains/backtest/core/runner.py` を `simulation -> core artifact write -> report render` の三段に再構成し、`metrics.json` / `manifest.json` / `*.simulation.pkl` を simulation 完了時点で確定させるようにした。`BacktestResult` は `metrics_path` / `manifest_path` / `simulation_payload_path` / `render_error` を保持し、HTML は optional にした。
- 2026-03-10: `apps/bt/src/domains/backtest/core/marimo_executor.py` に report path planning を追加し、render は事前確定した artifact path を使う renderer に寄せた。`apps/bt/notebooks/templates/strategy_analysis.py` は precomputed simulation payload を読むように変更し、notebook 側の `metrics.json` 書き込み責務を除去した。
- 2026-03-10: `apps/bt/src/application/services/backtest_result_summary.py` / `run_registry.py` / `backtest_worker.py` / `backtest_service.py` を更新し、summary 解決は `metrics.json -> canonical summary -> legacy fallback` を優先するようにした。HTML は presentation artifact のみとして扱う。
- 2026-03-10: `apps/bt/src/application/services/run_contracts.py` / `job_manager.py` を更新し、`_metrics_path` / `_manifest_path` から artifact index を構築できるようにした。HTML が無い completed run でも `METRICS_JSON` / `MANIFEST_JSON` を registry から再解決できる。
- 2026-03-10: 回帰として `uv run --project /Users/shinjiroaso/.codex/worktrees/bf77/trading25/apps/bt pytest tests/unit/backtest/test_backtest_runner.py tests/unit/backtest/test_manifest.py tests/unit/server/services/test_backtest_result_summary.py tests/unit/server/test_run_registry.py tests/unit/server/test_backtest_worker.py tests/unit/backtest/test_marimo_executor.py`、`uv run --project /Users/shinjiroaso/.codex/worktrees/bf77/trading25/apps/bt pytest tests/unit/server/services/test_backtest_service.py tests/unit/server/test_run_contracts.py tests/server/test_job_manager.py tests/unit/server/routes/test_backtest.py`、`uv run --project /Users/shinjiroaso/.codex/worktrees/bf77/trading25/apps/bt ruff check ...`、`uv run --project /Users/shinjiroaso/.codex/worktrees/bf77/trading25/apps/bt pyright ...` を実行し、対象差分のテスト・lint・型チェック通過を確認した。

## 補足
- 参照: `docs/backtest-greenfield-rebuild.md` Section 2.2, 7, 9.2
