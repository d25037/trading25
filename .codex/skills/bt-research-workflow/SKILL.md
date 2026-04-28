---
name: bt-research-workflow
description: apps/bt の research runner / bundle workflow を扱うスキル。研究定義を src/domains に実装し、vectorbt fast path・Nautilus verification・canonical docs で運用するときに使用する。
---

# bt-research-workflow

## When to use

- runner-first research script、bundle、canonical experiment docs を追加・改修するとき。
- analytics research の定義を `src/domains` に残し、再現可能な実行を runner に寄せたいとき。
- notebook runtime に依存せず、runner / bundle / docs を SoT として扱いたいとき。

## Source of Truth

- `apps/bt/scripts/research`
- `apps/bt/scripts/research/common.py`
- `apps/bt/src/domains`
- `apps/bt/src/shared/utils/pit_guard.py`
- `apps/bt/tests/unit`
- `apps/bt/docs/experiments`

## Workflow

1. 変更したい計算ロジックを `apps/bt/src/domains` の既存ドメインに実装する。
2. 追加・変更したロジックの unit test を `apps/bt/tests/unit` に実装する。
3. `apps/bt/scripts/research` に runner script を追加・更新し、`manifest.json + results.duckdb + summary.md` の bundle を保存できるようにする。published surface は `summary.json` または canonical README の `## Published Readout` を持つ publication-ready な研究ブリーフにする。
4. snapshot / universe / fundamentals / ranking join は必ず `as_of_date` 基準で切り、`slice_frame_as_of` / `latest_rows_per_group_as_of` / `filter_records_as_of` を優先利用する。
5. research 内の高速 backtest は `vectorbt` adapter を使い、追加の custom execution engine を増やさない。上位候補の authoritative check が必要な場合だけ `Nautilus` verification を使う。
6. 長く残す研究は `apps/bt/docs/experiments/*/*/README.md` の canonical note にし、runner と bundle 出力から辿れるようにする。README の先頭付近に `## Published Readout` を置き、Codex closeout で説明した判断・数値・解釈をチャットだけでなく source md に保存する。
7. 結果確認は runner が出力する `summary.md` / `summary.json` / `results.duckdb` を使う。

## Guardrails

- notebook runtime を repo の必須導線に戻さない。再現可能な run は runner script と bundle を SoT にする。
- future leak / point-in-time contamination は P0 として扱う。`latest per group` は必ず as-of filtering の後に取る。
- 新しい research pipeline では PIT stability test を追加し、discovery / validation / walk-forward を跨いだ future-derived bucket や summary を使わない。
- execution semantics の会計は `vectorbt` fast path に寄せ、`Nautilus` は verification 用に限定する。
- experiment README には削除済み notebook path や notebook runtime command を戻さない。
- Research closeout は source md への publication を完了条件にする。`## Published Readout` には `Decision` / `Main Findings` / `Interpretation` / `Production Implication` / `Caveats` / `Source Artifacts` を必ず含める。
- `scripts/check-research-guardrails.py` で runner / bundle / docs surface の退行を検出する。

## Verification

- `uv run pytest <affected tests>`
- `uv run --project apps/bt python apps/bt/scripts/research/<runner>.py --help`
- `python3 scripts/check-research-guardrails.py`
- `python3 scripts/skills/audit_skills.py --strict-legacy`
