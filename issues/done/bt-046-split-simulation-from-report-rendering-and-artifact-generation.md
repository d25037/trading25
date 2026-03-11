---
id: bt-046
title: "Simulation と report rendering / artifact generation を分離"
status: done
priority: medium
labels: [artifacts, reports, marimo, execution, bt]
project: bt
created: 2026-03-08
updated: 2026-03-11
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
- 2026-03-11: `apps/bt/src/domains/backtest/core/artifacts.py` を追加し、manifest 書き込みを `BacktestArtifactWriter` へ分離した。`BacktestRunner` は render 前に artifact path を解決し、manifest を runner 本体の責務から外した。
- 2026-03-11: `apps/bt/src/domains/backtest/core/runner.py` は `expected_html_path` / `metrics_path` / `manifest_path` を持つ `BacktestResult` を返すようにし、HTML 未生成でも `*.metrics.json` が残っていれば job を継続できるようにした。render 警告は `render_error` として保持する。
- 2026-03-11: `apps/bt/src/application/services/backtest_result_summary.py` / `backtest_service.py` / `backtest_worker.py` を更新し、HTML 本体がなくても sibling `*.metrics.json` から summary を復元できるようにした。worker は HTML path を `None` のまま durable 保存できる。
- 2026-03-11: `apps/bt/src/application/services/run_contracts.py` は raw_result 内の `_metrics_path` / `_manifest_path` も artifact index に取り込むようになり、`html_path` が無い完了ジョブでも core artifacts を列挙できるようにした。
- 2026-03-11: 回帰として `test_backtest_runner.py` / `test_backtest_result_summary.py` / `test_backtest_service.py` / `test_backtest_worker.py` / `test_run_contracts.py` / `test_job_manager.py` を実行し、HTML なし metrics-only 完了ケースを含めて 125 件 pass を確認した。
- 2026-03-11: `apps/bt/src/domains/backtest/core/simulation.py` を追加し、`BacktestRunner` は `simulation -> metrics/manifest write -> report render` の順で実行するようにした。`metrics.json` と初回 `manifest.json` は render 前に確定し、progress も simulation/render の二段表示になった。
- 2026-03-11: `apps/bt/notebooks/templates/strategy_analysis.py` の metrics 書き出しは shared helper を使うようにし、runner が先に生成した canonical `metrics.json` を notebook export が上書きしないようにした。
- 2026-03-11: `apps/bt/src/application/services/run_registry.py` は `_expected_html_path` / metrics artifact から backtest summary を再解決できるようにし、`job.html_path is None` かつ `job.result is None` のケースでも `/api/backtest/jobs/{id}` と `/api/backtest/result/{id}` が metrics-only artifact から復元できるようにした。
- 2026-03-11: 追加回帰として `test_run_registry.py` と `routes/test_backtest.py` を含む 171 件の pytest、ruff、pyright を通した。
- 2026-03-11: `apps/bt/src/domains/backtest/core/artifacts.py` の manifest は artifact contract を持つようにし、`metrics.json` を `canonical_summary`、`manifest.json` を `artifact_catalog`、`result.html` を `presentation_only`、`*.report.json` を `renderer_input` として責務を明示した。`test_manifest.py` / `test_backtest_runner.py` / `test_backtest_command.py` の 31 件で runner/manifest/CLI の回帰を確認した。
- 2026-03-11: `apps/bt/src/entrypoints/http/routes/html_file_utils.py` は `*.html` の rename/delete を sibling の `*.metrics.json` / `*.manifest.json` / `*.report.json` まで bundle として扱うようにし、保存済み run の artifact orphan を防ぐようにした。`test_html_file_utils.py` の 28 件を通した。
- 2026-03-11: `apps/bt/src/domains/backtest/contracts.py` に `ArtifactKind.REPORT_JSON` を追加し、`run_contracts.py` は `*.report.json` を artifact index に含めるようにした。OpenAPI / generated TS types は `bun run --filter @trading25/contracts bt:sync` と `bt:check` で同期済みで、`test_run_contracts.py` を含む 73 件の pytest、ruff、pyright を通した。
- 2026-03-11: `apps/bt/src/application/services/backtest_result_summary.py` / `run_registry.py` は `metrics_path` を直接扱うようにし、summary re-resolution が HTML sibling 推論に依存しない artifact-first read path になった。`run_registry` は metrics artifact 単独でも `html_path` を合成せず、`expected_html_path` がある場合だけ互換用に引き継ぐ。
- 2026-03-11: `apps/bt/notebooks/templates/strategy_analysis.py` は `report_data_path` 必須の presentation renderer へ変更し、`StrategyFactory.execute_strategy_with_config()` fallback と notebook 内 metrics export を除去した。
- 2026-03-11: `apps/bt/src/domains/backtest/core/runner.py` は simulation 完了直後に pending render status の artifact checkpoint を発行できるようにし、`backtest_worker.py` はそれを durable `raw_result` として保存するようにした。render が最終的に失敗しても simulation artifacts は保持される。`test_backtest_result_summary.py` / `test_run_registry.py` / `test_backtest_worker.py` / `test_backtest_runner.py` / `test_backtest_service.py` / `test_backtest_command.py` / `test_run_contracts.py` / `test_html_file_utils.py` の 128 件の pytest、ruff、pyright を通した。
- 上記により、simulation の canonical artifacts 確定、renderer の後段化、artifact-first summary 解決、render failure 時の durable 保持まで受け入れ条件を満たしたため、本 issue を done とする。

## 補足
- 参照: `docs/backtest-greenfield-rebuild.md` Section 2.2, 7, 9.2
