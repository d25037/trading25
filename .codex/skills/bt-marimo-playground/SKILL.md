---
name: bt-marimo-playground
description: apps/bt で marimo 実験 notebook を追加・改修するときに使うスキル。計算ロジックを src/domains に実装し、notebook は UI 検証専用として構築・運用する。
---

# bt-marimo-playground

## Scope

- `apps/bt/notebooks/playground/**`
- `apps/bt/notebooks/templates/**`
- `apps/bt/src/domains/**`
- `apps/bt/tests/unit/**`

## Fixed Workflow

1. 変更したい計算ロジックを `apps/bt/src/domains` の既存ドメインに実装する。
2. 追加・変更したロジックの unit test を `apps/bt/tests/unit` に実装する。
3. `apps/bt/notebooks/playground` に marimo notebook を作り、UI から domain 関数を呼んで挙動を確認する。
4. 再利用価値が高い場合、 notebook自体を`apps/bt/notebooks/templates` に昇格したり、ロジックをts frontendに昇格する。

## Guardrails

- notebook 内にビジネス計算ロジックを実装しない。計算は `src/domains` から import する。
- notebook は UI と可視化に限定する（入力 controls、図表、サマリー表示）。
- 再現性のため、サンプル入力は seed 固定または deterministic な入力を使う。
- notebook ファイル名は `<topic>_playground.py` 形式を使う。
- template への昇格時は設定参照パス（`template_notebook`）と関連テストを同時更新する。
- marimo notebook は marimo 構文に適合させる。
  - `import marimo` / `__generated_with` / `app = marimo.App(...)` の標準ヘッダを維持する。
  - cell をまたぐ公開変数名は一意にし、cell 内だけで使う一時変数は `_` 接頭辞にする。
  - cell 内で早期 `return` を使わない。分岐結果は `_view` / `_chart` などの変数に束ねて最後の式で表示する。
  - notebook 固有の UI 状態と domain 計算結果を同じ名前で再定義しない。

## Validation

- `uv run pytest <affected tests>`
- `uv run --project apps/bt marimo check --strict <changed-notebook.py>`
- notebook 構造を変更した場合は `uv run --project apps/bt marimo edit <changed-notebook.py> --headless -p <unused-port>` か `marimo run` で起動確認する
- `python scripts/skills/audit_skills.py --strict-legacy`
- `rg -n "notebooks/templates/marimo" apps/bt AGENTS.md`
