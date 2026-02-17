---
name: bt-signal-system
description: bt の統一シグナルシステムを扱うスキル。`entry_filter_params` / `exit_trigger_params`、SignalProcessor、シグナル実装変更時に使用する。
---

# bt-signal-system

## Core Principle

- Entry は AND で絞り込み。
- Exit は OR で追加発火。

## Scope

- `apps/bt/src/models/signals.py`
- `apps/bt/src/strategies/signals/**`
- `apps/bt/src/strategies/signals/processor.py`

## Review Checklist

1. パラメータモデルと YAML の整合性。
2. エントリー/エグジット結合ロジックの不変条件。
3. 追加シグナルの registry 反映漏れ。
4. `forward_eps_growth` / `peg_ratio` は FY実績EPS固定 + 四半期 FEPS 修正反映（必要時のみ追加取得）を維持。
