---
id: bt-040
title: "CompiledStrategyIR と availability model を導入"
status: open
priority: high
labels: [strategy, signals, no-lookahead, execution, bt]
project: bt
created: 2026-03-08
updated: 2026-03-08
depends_on: [bt-039]
blocks: [bt-041, bt-044]
parent: bt-037
---

# bt-040 CompiledStrategyIR と availability model を導入

## 目的
- YAML を authoring format に留め、実行時 SoT を `CompiledStrategyIR` へ移す。
- `current_session_round_trip_oracle` のような例外フラグ増殖を止め、availability model で no-lookahead を統一管理する。

## 受け入れ条件
- [ ] strategy validation 後に `CompiledStrategyIR` を生成できる。
- [ ] signal / feature に `observation_time`, `available_at`, `decision_cutoff`, `execution_session` を持てる。
- [ ] same-day oracle と prior-day signal を同じルールで評価できる。
- [ ] 既存 signal system の主要ケースに回帰テストがある。

## 実施内容
- [ ] strategy spec compiler を追加する。
- [ ] availability model を signal processor 周辺へ導入する。
- [ ] round-trip 系 execution semantics を ad-hoc shift から段階的に移行する。
- [ ] docs と signal reference を更新する。

## 結果
- 未着手

## 補足
- 参照: `docs/backtest-greenfield-rebuild.md` Section 5.1, 5.2

