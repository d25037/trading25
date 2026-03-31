---
id: bt-059
title: "runner-first 研究フローへ移行し notebook を viewer に下げる"
status: in-progress
priority: high
labels: [bt, research, workflow, reproducibility, notebook, skills]
project: bt
created: 2026-03-31
updated: 2026-03-31
depends_on: []
blocks: []
parent: null
---

# bt-059 runner-first 研究フローへ移行し notebook を viewer に下げる

## 目的
- `notebook で実験して解析、本番へ反映` という現行フローを、`domain -> reproducible runner -> artifact bundle -> experiment note -> optional notebook viewer` へ移行する。
- notebook を研究の主導線ではなく閲覧 UI に下げ、再現性と過去データの閲覧性を両立する。
- assistant 主体で実装・実行しても、後から user が同じ成果物を検証・再利用できる状態にする。

## 背景
- 現在の research は domain logic を `src/domains/analytics` に置いている一方、実運用では notebook を介した確認が暗黙の前提になっている。
- `marimo check` や unit test が通っていても、notebook runtime integration の破綻を見逃すことがある。
- 一方で `docs/experiments` はすでに canonical note と baseline note の運用があり、研究成果の記録基盤として使える。
- 足りないのは、domain 実行と experiment note の間にある `再現可能な実行導線` と `成果物 bundle` である。

## 受け入れ条件
- [x] 研究実行の主導線が notebook ではなく `runner script` に移っている。
- [x] 各研究 run が artifact bundle として保存され、`manifest + tables + summary` を後から再読できる。
- [x] `docs/experiments` の baseline note が run bundle と結びつく。
- [x] notebook は bundle を読む viewer-first 構成に移行し、新規計算は明示操作でのみ走る。
- [x] workflow 変更に合わせて関連 skill が更新されている。

## 実施内容
- [x] `apps/bt/scripts/research/` を新設し、research runner の置き場を作る。
- [x] `src/domains/analytics` もしくは隣接 util に artifact bundle writer を追加する。
- [x] bundle format を定義する。
  - `manifest.json`
  - `results.duckdb`
  - `summary.md`
  - 任意で `figures/`, `exports/`
- [x] manifest に最低限以下を保存する。
  - `experiment_id`
  - `run_id`
  - `created_at`
  - `git_commit`
  - `git_dirty`
  - `module`
  - `function`
  - `params`
  - `db_path`
  - `db_fingerprint`
  - `analysis_start_date`
  - `analysis_end_date`
  - `output_tables`
- [x] pilot として少なくとも 1 本の既存研究を runner-first 化する。
  - 候補: `topix100_price_vs_sma_rank_future_close`
- [x] `docs/experiments` の `README.md` と対象実験 note に、bundle/run_id ベースの再現手順を追記する。
- [x] notebook を viewer-first に改修する。
  - 初期入力は `run_id` または `bundle_path`
  - 再計算は明示操作のみ
  - bundle があれば notebook なしでも研究結果が追える
- [x] skill を更新する。
  - `.codex/skills/bt-marimo-playground/SKILL.md` を notebook-first ではなく viewer-first 運用へ更新
  - 必要なら `runner-first research` 用の新 skill を追加

## 結果
- 2026-03-31: `apps/bt/src/domains/analytics/research_bundle.py` を追加し、`manifest.json + results.duckdb + summary.md` を保存・再読する research bundle 基盤を追加した。
- 2026-03-31: `apps/bt/scripts/research/run_topix100_price_vs_sma_rank_future_close.py` を追加し、`topix100_price_vs_sma_rank_future_close` を pilot の runner-first 実験として bundle 出力できるようにした。
- 2026-03-31: `apps/bt/notebooks/playground/topix100_price_vs_sma_rank_future_close_playground.py` を viewer-first へ切り替え、latest bundle の読込を既定にし、fresh recompute は `Mode = Run Fresh Analysis` の明示操作時だけ走るようにした。
- 2026-03-31: `apps/bt/docs/experiments/README.md` と pilot note を runner-first 前提へ更新し、`.codex/skills/bt-marimo-playground/SKILL.md` も viewer-first workflow に更新した。
- 2026-03-31: `apps/bt/scripts/research/run_topix100_price_vs_sma_q10_bounce.py` と `run_topix100_price_vs_sma_q10_bounce_regime_conditioning.py` を追加し、`Q10 bounce` と `regime conditioning` も bundle 出力できるようにした。
- 2026-03-31: `apps/bt/notebooks/playground/topix100_price_vs_sma_q10_bounce_playground.py` と `topix100_price_vs_sma50_q10_bounce_regime_conditioning_playground.py` を viewer-first へ切り替え、latest bundle 読込を既定にした。

## 補足
- 現行の experiment note 基盤: `apps/bt/docs/experiments/README.md`
- 代表研究の note 例: `apps/bt/docs/experiments/market-behavior/topix100-sma-ratio-lightgbm/README.md`
- notebook は廃止ではなく、`optional viewer` に役割変更する。
- local skill の更新は必要。少なくとも `bt-marimo-playground` は現行の前提を変える必要がある。
- 次の展開対象は `topix100_price_vs_sma_q10_bounce` と `topix100_price_vs_sma50_q10_bounce_regime_conditioning` の runner-first 化。
