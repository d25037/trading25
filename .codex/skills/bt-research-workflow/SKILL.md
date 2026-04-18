---
name: bt-research-workflow
description: apps/bt の research runner / bundle / optional notebook viewer workflow を扱うスキル。研究定義を src/domains に実装し、vectorbt fast path・Nautilus verification・viewer-only notebook で運用するときに使用する。
---

# bt-research-workflow

## When to use

- runner-first research script、bundle、optional notebook viewer を追加・改修するとき。
- analytics research の定義を `src/domains` に残し、再現可能な実行を runner に寄せたいとき。
- notebook を SoT にせず、viewer-only surface として扱いたいとき。

## Source of Truth

- `apps/bt/scripts/research`
- `apps/bt/scripts/research/common.py`
- `apps/bt/src/domains`
- `apps/bt/src/shared/utils/pit_guard.py`
- `apps/bt/src/shared/research_notebook_viewer.py`
- `apps/bt/notebooks/playground`
- `apps/bt/notebooks/templates`
- `apps/bt/tests/unit`

## Workflow

1. 変更したい計算ロジックを `apps/bt/src/domains` の既存ドメインに実装する。
2. 追加・変更したロジックの unit test を `apps/bt/tests/unit` に実装する。
3. `apps/bt/scripts/research` に runner script を追加・更新し、`manifest.json + results.duckdb + summary.md` の bundle を保存できるようにする。
4. snapshot / universe / fundamentals / ranking join は必ず `as_of_date` 基準で切り、`slice_frame_as_of` / `latest_rows_per_group_as_of` / `filter_records_as_of` を優先利用する。
5. research 内の高速 backtest は `vectorbt` adapter を使い、追加の custom execution engine を増やさない。上位候補の authoritative check が必要な場合だけ `Nautilus` verification を使う。
6. `apps/bt/notebooks/playground` の notebook は latest bundle を既定で読む viewer-only にし、fresh recompute は notebook に持ち込まず runner script へ寄せる。
7. 再利用価値が高い場合、notebook を template へ昇格する。

## Guardrails

- notebook 内にビジネス計算ロジックを実装しない。計算は `src/domains` から import する。
- notebook を唯一の再現導線にしない。再現可能な run は runner script と bundle を SoT にする。
- future leak / point-in-time contamination は P0 として扱う。`latest per group` は必ず as-of filtering の後に取る。
- 新しい research pipeline では PIT stability test を追加し、discovery / validation / walk-forward を跨いだ future-derived bucket や summary を使わない。
- execution semantics の会計は `vectorbt` fast path に寄せ、`Nautilus` は verification 用に限定する。
- notebook は UI と可視化に限定し、bundle viewer として動かす。
- playground notebook は `src.shared.research_notebook_viewer` を使い、`runner_path` は実在する `apps/bt/scripts/research/run_*.py` を指す。
- notebook ファイル名は `<topic>_playground.py` 形式を使う。
- marimo notebook は標準ヘッダと一意な公開変数名を維持する。

## Verification

- `uv run pytest <affected tests>`
- `uv run --project apps/bt python apps/bt/scripts/research/<runner>.py --help`
- `uv run --project apps/bt marimo check --strict <changed-notebook.py>`
- `python3 scripts/check-research-guardrails.py`
- `python3 scripts/skills/audit_skills.py --strict-legacy`
