---
id: bt-063
title: "research surface を runner bundle docs で正規化する"
status: in-progress
priority: medium
labels: [bt, research, docs, workflow]
project: bt
created: 2026-04-18
updated: 2026-04-19
depends_on: [bt-061, bt-062]
blocks: []
parent: bt-060
---

# bt-063 research surface を runner bundle docs で正規化する

## 目的
- notebook / docs / publication の入口を runner-first + bundle-first に揃え、published research の surface を一貫させる。
- research の最低限成果物を `runner + bundle + summary + docs reference` に固定する。

## 受け入れ条件
- [ ] playground / published notebook / docs の参照関係が runner-first に統一されている。
- [x] published research の最小要件が docs か skill に明文化されている。
- [x] latest bundle resolution / summary / docs reference の surface が family 横断で再利用可能になっている。

## 実施内容
- [ ] notebook / docs / research catalog の surface を棚卸しする。
- [x] published research の最小成果物を定義し、skill / docs / templates に反映する。
- [x] bundle summary と docs reference の生成 helper を統一する。

## 結果
- `apps/bt/src/shared/research_notebook_viewer.py` の `build_bundle_viewer_controls(...)` に `docs_readme_path` を追加し、shared notebook surface から `runner_path` / canonical note / bundle artifact set (`manifest.json`, `results.duckdb`, `summary.md`, optional `summary.json`) を共通表示できるようにした。
- docs-backed playground notebook 12 本を更新し、canonical note README を viewer controls から辿れるようにした。
- `scripts/check-research-guardrails.py` に `docs_readme_path` の prefix / filename / file existence validation を追加し、viewer-only notebook surface の drift を防ぐ guardrail を拡張した。
- `apps/bt/docs/README.md`, `apps/bt/docs/experiments/README.md`, `.codex/skills/bt-research-workflow/SKILL.md` を更新し、published research の最小 surface を `runner script + latest bundle + canonical note + baseline note` として明文化した。
- docs-backed canonical note のうち runner 記載が欠けていた README に `Runner` bullet を追加し、notebook / domain / tests と並ぶ runner-first 入口を揃えた。
- `apps/bt/src/domains/analytics/research_bundle.py` に canonical note README の path helper を追加し、`apps/bt/src/application/services/research_catalog_service.py` と `/api/analytics/research` schema から `docsReadmePath` を返せるようにした。
- `apps/bt/tests/unit/domains/analytics/test_research_bundle.py` と `apps/bt/tests/unit/server/routes/test_analytics_research.py` を更新し、catalog/detail の docs reference と canonical note helper を回帰テスト化した。
- canonical note README の Reproduction を、専用 runner がある family について `uv run --project apps/bt python apps/bt/scripts/research/run_*.py ...` へ揃えた。
- 検証:
  - `UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt ruff check apps/bt/src/shared/research_notebook_viewer.py apps/bt/tests/unit/shared/test_research_notebook_viewer.py apps/bt/tests/unit/scripts/test_check_research_guardrails.py`
  - `UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt pyright apps/bt/src/shared/research_notebook_viewer.py`
  - `UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt pytest apps/bt/tests/unit/shared/test_research_notebook_viewer.py apps/bt/tests/unit/scripts/test_check_research_guardrails.py`
  - `python3 scripts/check-research-guardrails.py`
  - `UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt marimo check --strict ...` (changed docs-backed playground notebooks)
  - `UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt ruff check apps/bt/src/domains/analytics/research_bundle.py apps/bt/src/application/services/research_catalog_service.py apps/bt/src/entrypoints/http/routes/analytics_research.py apps/bt/src/entrypoints/http/schemas/research.py apps/bt/tests/unit/domains/analytics/test_research_bundle.py apps/bt/tests/unit/server/routes/test_analytics_research.py`
  - `UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt pyright apps/bt/src/domains/analytics/research_bundle.py apps/bt/src/application/services/research_catalog_service.py apps/bt/src/entrypoints/http/routes/analytics_research.py apps/bt/src/entrypoints/http/schemas/research.py`
  - `UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt pytest apps/bt/tests/unit/domains/analytics/test_research_bundle.py apps/bt/tests/unit/server/routes/test_analytics_research.py apps/bt/tests/unit/shared/test_research_notebook_viewer.py apps/bt/tests/unit/scripts/test_check_research_guardrails.py`
  - `bun run --filter @trading25/contracts bt:sync`

## 補足
- 親 issue: `bt-060`
- 依存: `bt-061`, `bt-062`
- 残件: `apps/bt/docs/experiments/market-behavior/topix100-sma-ratio-lightgbm/README.md` は専用 runner が未整備のため、inline Python reproduction のまま。次 slice では dedicated runner / bundle entry を追加するか、既存 runner と LightGBM step の二段導線として canonical に整理する。
