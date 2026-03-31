---
id: bt-059
title: "runner-first 研究フローへ移行し notebook を viewer に下げる"
status: open
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
- [ ] 研究実行の主導線が notebook ではなく `runner script` に移っている。
- [ ] 各研究 run が artifact bundle として保存され、`manifest + tables + summary` を後から再読できる。
- [ ] `docs/experiments` の baseline note が run bundle と結びつく。
- [ ] notebook は bundle を読む viewer-first 構成に移行し、新規計算は明示操作でのみ走る。
- [ ] workflow 変更に合わせて関連 skill が更新されている。

## 実施内容
- [ ] `apps/bt/scripts/research/` を新設し、research runner の置き場を作る。
- [ ] `src/domains/analytics` もしくは隣接 util に artifact bundle writer を追加する。
- [ ] bundle format を定義する。
  - `manifest.json`
  - `results.duckdb`
  - `summary.md`
  - 任意で `figures/`, `exports/`
- [ ] manifest に最低限以下を保存する。
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
- [ ] pilot として少なくとも 1 本の既存研究を runner-first 化する。
  - 候補: `topix100_price_vs_sma_rank_future_close`
- [ ] `docs/experiments` の `README.md` と対象実験 note に、bundle/run_id ベースの再現手順を追記する。
- [ ] notebook を viewer-first に改修する。
  - 初期入力は `run_id` または `bundle_path`
  - 再計算は明示操作のみ
  - bundle があれば notebook なしでも研究結果が追える
- [ ] skill を更新する。
  - `.codex/skills/bt-marimo-playground/SKILL.md` を notebook-first ではなく viewer-first 運用へ更新
  - 必要なら `runner-first research` 用の新 skill を追加

## 結果
- 未着手。

## 補足
- 現行の experiment note 基盤: `apps/bt/docs/experiments/README.md`
- 代表研究の note 例: `apps/bt/docs/experiments/market-behavior/topix100-sma-ratio-lightgbm/README.md`
- notebook は廃止ではなく、`optional viewer` に役割変更する。
- local skill の更新は必要。少なくとも `bt-marimo-playground` は現行の前提を変える必要がある。
