---
id: bt-040
title: "CompiledStrategyIR と availability model を導入"
status: done
priority: high
labels: [strategy, signals, no-lookahead, execution, bt]
project: bt
created: 2026-03-08
updated: 2026-03-10
depends_on: [bt-039]
blocks: [bt-041, bt-044]
parent: bt-037
---

# bt-040 CompiledStrategyIR と availability model を導入

## 目的
- YAML を authoring format に留め、実行時 SoT を `CompiledStrategyIR` へ移す。
- `current_session_round_trip_oracle` のような例外フラグ増殖を止め、availability model で no-lookahead を統一管理する。

## 受け入れ条件
- [x] strategy validation 後に `CompiledStrategyIR` を生成できる。
- [x] signal / feature に `observation_time`, `available_at`, `decision_cutoff`, `execution_session` を持てる。
- [x] same-day oracle と prior-day signal を同じルールで評価できる。
- [x] 既存 signal system の主要ケースに回帰テストがある。

## 実施内容
- [x] strategy spec compiler を追加する。
- [x] availability model を signal processor 周辺へ導入する。
- [x] round-trip 系 execution semantics を ad-hoc shift から段階的に移行する。
- [x] docs と signal reference を更新する。

## 結果
- `CompiledStrategyIR` / availability model の shadow compile を追加した。
- `/api/strategies/{name}/validate` で compiled IR を返せるようにした。
- `run_spec.compiled_strategy_requirements` は compiled IR 由来で埋めるようにした。
- `YamlConfigurableStrategy` / `SignalProcessor` / `BacktestExecutorMixin` は compiled availability / execution semantics を優先参照するようにした。
- `/api/signals/reference` と web `SignalReferencePanel` で execution semantics ごとの availability profile を表示できるようにした。
- screening strategy の `standard/oracle/unsupported` 判定も compiled availability 起点に寄せた。
- screening evaluator / `YamlConfigurableStrategy` は `SignalProcessor` へ compiled strategy を直接渡すようにし、主要 path の boolean fallback を減らした。
- `SignalProcessor` の public API から `current_session_round_trip_oracle` 引数を外し、timing 判定は compiled availability を SoT にした。
- screening runtime の内部 dataclass から派生 boolean を外し、`compiled_strategy.execution_semantics` / availability だけで oracle 判定できるようにした。
- `StrategyFactory` / `BacktestRunner` / `BacktestExecutorMixin` の round-trip 判定は compiler helper を共有し、validation と execution path の重複を減らした。
- signal 起因の same-day oracle は screening oracle として扱いつつ、backtest の round-trip execution とは分離した。

## 補足
- 参照: `docs/backtest-greenfield-rebuild.md` Section 5.1, 5.2
