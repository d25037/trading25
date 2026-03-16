---
name: bt-marimo-playground
description: apps/bt で marimo 実験 notebook を追加・改修するときに使うスキル。計算ロジックを src/domains に実装し、notebook は UI 検証専用として構築・運用する。
---

# bt-marimo-playground

## When to use

- marimo playground notebook を追加・改修するとき。
- notebook で UI 検証をしたいが、計算ロジックは domain に残したいとき。

## Source of Truth

- `apps/bt/notebooks/playground`
- `apps/bt/notebooks/templates`
- `apps/bt/src/domains`
- `apps/bt/tests/unit`

## Workflow

1. 変更したい計算ロジックを `apps/bt/src/domains` の既存ドメインに実装する。
2. 追加・変更したロジックの unit test を `apps/bt/tests/unit` に実装する。
3. `apps/bt/notebooks/playground` に marimo notebook を作り、UI から domain 関数を呼んで挙動を確認する。
4. 再利用価値が高い場合、notebook を template へ昇格する。

## Guardrails

- notebook 内にビジネス計算ロジックを実装しない。計算は `src/domains` から import する。
- notebook は UI と可視化に限定する。
- notebook ファイル名は `<topic>_playground.py` 形式を使う。
- marimo notebook は標準ヘッダと一意な公開変数名を維持する。

## Verification

- `uv run pytest <affected tests>`
- `uv run --project apps/bt marimo check --strict <changed-notebook.py>`
- `python3 scripts/skills/audit_skills.py --strict-legacy`
