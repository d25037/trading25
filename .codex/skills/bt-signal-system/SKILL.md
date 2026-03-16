---
name: bt-signal-system
description: bt の統一シグナルシステムを扱うスキル。`entry_filter_params` / `exit_trigger_params`、signal registry、SignalProcessor の変更時に使用する。
---

# bt-signal-system

## When to use

- `entry_filter_params` / `exit_trigger_params`、signal registry、SignalProcessor を変更するとき。
- YAML と signal parameter model の整合を見直すとき。

## Source of Truth

- `apps/bt/src/shared/models/signals`
- `apps/bt/src/domains/strategy/signals`
- `apps/bt/src/domains/strategy/runtime`
- `apps/bt/src/entrypoints/http/routes/signal_reference.py`
- `apps/bt/config/strategies`

## Workflow

1. parameter model、YAML、registry、processor の順で変更範囲を確認する。
2. Entry は AND、Exit は OR の不変条件を維持する。
3. 新規 signal は registry と signal reference への反映漏れがないか確認する。
4. fundamentals 系 signal は share-adjusted baseline と forecast revision の扱いを崩さない。

## Guardrails

- `forward_eps_growth` と `peg_ratio` は FY 実績 EPS 固定 + 必要時のみ FEPS 修正反映を維持する。
- signal 実装を route や YAML loader に分散させない。
- runtime validation と backend strict validation の二重 SoT を作らない。

## Verification

- `uv run --project apps/bt pytest tests/unit/strategies/signals tests/unit/strategies/test_signal_processor.py tests/unit/server/routes/test_signal_reference.py`
- `uv run --project apps/bt pytest tests/unit/models/test_signals_base.py tests/unit/models/test_signals_params.py`
- `uv run --project apps/bt ruff check src/shared/models/signals src/domains/strategy/signals src/domains/strategy/runtime`
