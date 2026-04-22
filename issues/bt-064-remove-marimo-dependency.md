---
id: bt-064
title: "marimo 依存を repo から撤去する"
status: open
priority: medium
labels: [bt, dependencies, reports, research, cleanup]
project: bt
created: 2026-04-22
updated: 2026-04-22
depends_on: []
blocks: []
parent: null
---

# bt-064 marimo 依存を repo から撤去する

## 目的
- 通常の bt runtime / CI / Dependabot から marimo を外し、notebook runtime の upstream 変更で core CI が止まる状態をなくす。
- backtest result の SoT を既存の成果物セット（`result.html` + `*.metrics.json` + manifest / payload）に保ち、report rendering を notebook runtime から切り離す。
- research workflow は runner-first + bundle-first を維持し、optional viewer がなくても再現・検証できる状態にする。

## 開始時点の状況
- `apps/bt/pyproject.toml` の通常 dependency に `marimo>=0.23.0` がある。
- `apps/bt/src/domains/backtest/core/marimo_executor.py` が backtest report HTML の export を担っている。
- `BacktestRunner` / Nautilus adapter / optimization notebook generator が `MarimoExecutor` を経由して HTML を生成している。
- `apps/bt/notebooks/templates/*.py` と `apps/bt/notebooks/playground/*_playground.py` は marimo app として残っている。
- `scripts/check-research-guardrails.py` は notebook の viewer-only 形を検査しているが、marimo runtime 自体は必要としない。
- 一部 unit test は marimo import / marimo executor の subprocess 実行契約を前提にしている。

## 方針
- marimo を「core report renderer」から外す。代替 renderer は Python 標準の HTML writer か、既存 payload を読む軽量テンプレート renderer とする。
- notebook viewer は repo の必須導線から外す。残す場合も optional artifact / archived example 扱いにし、通常 dependency と CI から切る。
- 移行中も `BacktestResult.html_path` と `/api/backtest/result/{id}` の成果物契約は維持する。

## 実施計画
- [x] Phase 1: backtest report renderer の抽象を作る
  - `ReportRenderer` 相当の interface を追加する。
  - 現行 `MarimoExecutor.plan_report_paths(...)` の path planning を marimo 非依存の helper に移す。
  - `BacktestRunner` は `report_payload.json` から HTML を生成する renderer を呼ぶ。
- [x] Phase 2: marimo 非依存 HTML renderer を実装する
  - [x] `apps/bt/notebooks/templates/strategy_analysis.py` の表示責務を、payload-driven な HTML renderer に移す。
  - [x] `result.html` は metrics / manifest / report payload を読む静的 HTML とし、失敗時も manifest の `report_status` / `render_error` を維持する。
  - [x] `tests/unit/backtest/test_marimo_executor.py` は削除し、新 renderer の入出力契約テストへ置き換える。
- [x] Phase 3: optimization report を旧 notebook generator 名から外す
  - [x] `apps/bt/src/domains/optimization/optimization_report_renderer.py` の実装を Marimo 実行から静的 HTML writer へ置き換える。
  - [x] `apps/bt/notebooks/templates/optimization_analysis.py` 依存を削除する。
  - [x] 公開名を `optimization_report_renderer.py` へ整理し、旧 `notebook_generator` 名を撤去する。
- [x] Phase 4: playground notebook を optional / archive へ移す
  - [x] `apps/bt/notebooks/playground/*_playground.py` を削除し、research 再現導線を docs / runner bundle へ一本化する。
  - [x] `scripts/check-research-guardrails.py` は notebook 検査ではなく docs / runner / bundle surface 検査へ寄せる。
  - [x] docs の `marimo edit` 再現コマンドを runner / bundle / docs の導線へ置き換える。
- [x] Phase 5: dependency と CI から marimo を削除する
  - [x] `apps/bt/pyproject.toml` から `marimo` と mypy override を削除する。
  - [x] `apps/bt/uv.lock` から marimo tree を落とす。
  - [x] active CI / prepush dependency から `marimo check --strict` が不要な状態にする。
  - [x] Dependabot の marimo 更新 PR が出ない状態にする。

## 受け入れ条件
- [x] `uv run --project apps/bt pytest` が marimo 未インストール環境で通る。
- [x] `uv run --project apps/bt pyright apps/bt/src` が marimo 未インストール環境で通る。
- [ ] `bt backtest <strategy>` が `result.html` / metrics / manifest / simulation payload / report payload を生成する。
- [ ] `/api/backtest/jobs/{id}` と `/api/backtest/result/{id}` が既存成果物から summary を再解決できる。
- [x] research runner / bundle / canonical note の再現導線が notebook runtime に依存しない。
- [x] `apps/bt/pyproject.toml` と `apps/bt/uv.lock` に marimo が残っていない。

## 進捗
- 2026-04-22: `apps/bt/src/domains/backtest/core/report_renderer.py` を追加し、artifact path planning と payload-driven static HTML rendering を marimo 非依存に分離した。
- 2026-04-22: `BacktestRunner` は `MarimoExecutor` ではなく `StaticHtmlReportRenderer` を使って `result.html` を生成するように変更した。`metrics.json` / manifest / simulation payload / report payload の bt-046 契約は維持した。
- 2026-04-22: Nautilus verification の artifact path planning も `BacktestReportPathPlanner` へ切り替えた。report は従来通り `not_requested` のまま。
- 2026-04-22: `BacktestArtifactWriter` の report renderer metadata を `static_html` に更新し、manifest の versions から marimo version を外した。
- 2026-04-22: `tests/unit/backtest/test_backtest_runner.py` を static renderer 前提へ更新し、`tests/unit/backtest/test_report_renderer.py` を追加した。
- 2026-04-22: `bt backtest` CLI の HTML 生成導線を `MarimoExecutor.execute_notebook` から `BacktestRunner` + static renderer へ切り替えた。
- 2026-04-22: optimization report generator を Marimo template 実行から静的 HTML writer へ切り替え、関連 unit test を static HTML 前提へ更新した。
- 2026-04-22: 参照が無くなった `apps/bt/src/domains/backtest/core/marimo_executor.py` と旧 executor 専用テストを削除した。
- 2026-04-22: `apps/bt/pyproject.toml` / `apps/bt/uv.lock` / Dependabot grouping から marimo を削除した。旧 template import test も削除し、pytest 側が marimo runtime を import しないようにした。
- 2026-04-22: `apps/bt/src/domains/optimization/notebook_generator.py` を `optimization_report_renderer.py` へ改名し、`generate_optimization_notebook` / `_generate_visualization_notebook` の旧公開名を削除した。
- 2026-04-22: `apps/bt/notebooks/templates/*.py` と `apps/bt/notebooks/playground/*_playground.py` を active repo surface から削除した。
- 2026-04-22: research README / baseline note / skills / AGENTS / docs を runner bundle + static HTML report 前提へ更新した。
- 2026-04-22: `scripts/check-research-guardrails.py` を legacy playground file と docs 内 notebook runtime reference の検出へ切り替えた。
- 2026-04-22: `uv run pytest` の全体実行で既存の matplotlib fake module 注入テストが1件落ちたため、`tests/unit/strategies/utils/test_optimization.py` を修正し、再実行で `5380 passed, 1 skipped` を確認した。

## 注意点
- 一括削除は blast radius が大きい。まず renderer 分離で `BacktestRunner` の report rendering を差し替え、その後 notebook / docs / tests を削る。
- 既存の HTML 形状を完全再現する必要はないが、API と成果物契約は維持する。
- marimo notebook は SoT ではない。残す場合も optional viewer であり、CI の必須 gate にしない。
