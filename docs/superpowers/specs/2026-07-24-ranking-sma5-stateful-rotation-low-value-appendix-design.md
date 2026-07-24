# SMA5 Stateful Rotation Low-Value Appendix Design

## Purpose

既存のone-hop stateful rotation研究に対するappendixとして、indices側は強いが
Valueが悪い群でもX2 / X3 / X4 rotationが有効かを確認する。

計算、source event、target episode、paired counterfactual、cost、decision ruleは
`ranking-sma5-score-ring-stateful-rotation-evidence`と同一にし、ring maskだけを
変更する。

## Appendix Rings

`long_hybrid_leadership_score`は全ringで`>= 0.7`に固定する。

| Ring | Long Hybrid | Value |
| --- | ---: | ---: |
| `low_value_core` | `>= 0.7` | `<= 0.2` |
| `low_value_near1` | `>= 0.7` | `<= 0.3` |
| `low_value_near2` | `>= 0.7` | `<= 0.4` |

Sourceとtargetは同じappendix ringに属することを必須とする。Targetは同日に
X2 / X3 / X4のすべてに非該当する銘柄とする。

## Frozen Stateful Comparison

- Source: `E0_no_sma5_filter` / 60-session baseline position内のfirst trigger
- Trigger precedence: X4 → X3 → X2
- Target exit: 次trigger、ring離脱、60-session、terminalの最初
- Counterfactual: targetと同じ終了日までsourceを継続保有
- Paired delta: target累積return − 同期間source累積return
- Cost: rotation時に一度だけ`0 / 10 / 20 bps`
- Aggregation: target sleevesをsource event内で等ウェイト化

## Outputs

既存stateful研究と同じ6表をappendix専用bundleへ保存する。

- `stateful_rotation_summary_df`
- `stateful_rotation_annual_df`
- `stateful_rotation_exit_reason_df`
- `stateful_rotation_decision_df`
- `stateful_rotation_event_df`
- `coverage_diagnostics_df`

日本語Published Readoutでは、通常のhigh-Value score ringとの違いをX2 / X3 /
X4別に短く比較する。

## Decision Rule

Appendix Core 10bps median > 0、positive event rate > 50%、positive yearが
過半数、Near1 / Near2 10bps median >= 0、Core 20bps median >= 0をすべて
満たすtriggerだけを`stateful_rotation_candidate`とする。

これはappendixのexploratory結果であり、production promotionやrecursive
portfolio実装は行わない。

## Verification

- 3つの非対称ring境界（Long Hybrid `0.7`、Value `0.2/0.3/0.4`）
- Sourceとtargetが同じappendix ringであること
- 既存stateful episode semanticsとの一致
- Canonical bundle値のread-only再計算

