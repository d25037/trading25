---
id: bt-041
title: "VectorbtAdapter を抽出し domain から vbt.Portfolio を除去"
status: done
priority: high
labels: [vectorbt, adapter, refactor, domain, bt]
project: bt
created: 2026-03-08
updated: 2026-03-10
depends_on: [bt-039, bt-040]
blocks: [bt-045, bt-046]
parent: bt-037
---

# bt-041 VectorbtAdapter を抽出し domain から vbt.Portfolio を除去

## 目的
- `vectorbt` を SoT ではなく backend adapter の 1 つへ格下げする。
- domain / protocol / strategy state に漏れている `vbt.Portfolio` 依存を引き剥がす。

## 受け入れ条件
- [x] strategy protocol / runtime state / analytics の公開境界から `vbt.Portfolio` が消える。
- [x] 現行 backtest path は `VectorbtAdapter` 経由で動作する。
- [x] canonical result writer により `vectorbt` 結果を正規化できる。
- [x] 回帰テストで既存 backtest/optimization の主要指標が維持される。

## 実施内容
- [x] `apps/bt/src/domains/strategy/core/mixins/protocols.py` の portfolio 型境界を置き換える。
- [x] `backtest_executor_mixin.py` / Kelly analyzer の返り値と runtime state を `ExecutionPortfolioProtocol` へ寄せ、実行時は `VectorbtPortfolioAdapter` で包む。
- [x] attribution / optimization / lab / CLI 側の consumer を `ExecutionPortfolioProtocol` 前提へ段階移行する。
- [x] indicator/signal 計算の `vectorbt` 依存を棚卸しし、必要な純粋計算を抽出する。
- [x] `VectorbtAdapter` と正規化レイヤーを追加する。

## 結果
- 2026-03-10: `apps/bt/src/domains/backtest/vectorbt_adapter.py` を追加し、`ExecutionPortfolioProtocol` / `ExecutionTradeLedgerProtocol` / `VectorbtPortfolioAdapter` / `canonical_metrics_from_portfolio` を導入した。
- 2026-03-10: `YamlConfigurableStrategy` と strategy mixin (`protocols.py` / `backtest_executor_mixin.py` / `portfolio_analyzer_mixin_kelly.py`) の公開型境界から `vbt.Portfolio` を外し、現行 VectorBT 実装は adapter 経由で返す形へ移行した。
- 2026-03-10: `BacktestRunner` の walk-forward metric 収集を正規化 helper 経由へ切り替えた。
- 2026-03-10: `canonical_metrics_from_portfolio` を test double / partial protocol 実装でも動くように強化し、attribution / optimization / lab / CLI の scalar metric 取得を helper 経由へ寄せた。`apps/bt/src` 直下の raw `portfolio.(total_return|sharpe_ratio|calmar_ratio|max_drawdown|sortino_ratio)` 呼び出しは adapter 内部のみに集約された。
- 2026-03-10: 回帰確認として `tests/unit/backtest/test_signal_attribution.py`、`tests/unit/backtest/test_backtest_runner.py`、`tests/unit/optimization/test_scoring.py`、`tests/unit/optimization/test_engine_metrics.py`、`tests/unit/agent/evaluator/test_candidate_processor.py`、`tests/unit/agent/test_optuna_optimizer.py`、`tests/unit/agent/test_strategy_improver.py`、`tests/unit/strategies/utils/test_optimization.py`、`tests/unit/strategies/mixins/test_portfolio_analyzer_mixin_kelly.py` を実行し、231件 pass を確認した。あわせて対象差分に対して `ruff check` / `pyright` を通した。
- 2026-03-10: signal / indicator 側の棚卸しを完了し、`apps/bt/src` の direct `vectorbt` import / `.vbt` 呼び出しは adapter 内部のみに集約された。残存していた `domains/strategy/signals/beta.py` の `vectorbt` 依存は除去し、`method="vectorbt"` は互換 alias として numba 実装へフォールバックさせた。
- 2026-03-10: βシグナル回帰として `tests/unit/strategies/signals/test_beta.py`、`tests/unit/filters/test_beta_filters.py`、`tests/integration/test_signal_processor_beta.py` を実行し、43件 pass を確認した。追加差分に対して `ruff check` / `pyright` も通した。

## 補足
- 参照: `docs/backtest-greenfield-rebuild.md` Section 5.3, 5.5
