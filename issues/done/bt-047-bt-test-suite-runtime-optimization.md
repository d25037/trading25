---
id: bt-047
title: "bt フルスイートテストの実行時間最適化"
status: done
priority: medium
labels: [bt, testing, performance, ci]
project: bt
created: 2026-03-20
updated: 2026-03-24
depends_on: []
blocks: []
parent: null
---

# bt-047 bt フルスイートテストの実行時間最適化

## 目的
- `apps/bt` のフルスイートテストがローカルで約 6 分かかる状況に対して、通常開発時の待ち時間を削減する。
- フルスイートの信頼性は維持しつつ、ローカル実行と CI 実行の役割を整理する。

## 背景
- `apps/bt/tests` は unit/server/strategy/agent/data/backtest など広い範囲を含み、テストファイル数も多い。
- `apps/bt/pyproject.toml` の pytest 設定では `slow` marker は定義されているが、現状は slow テストの明示的な運用が弱い。
- `scripts/test-packages.sh` と `scripts/test-apps.sh` はどちらも素の `pytest` 実行を前提としており、ローカル高速化オプションがない。
- `apps/bt/tests/conftest.py` には `create_app()` / `TestClient` / `httpx.AsyncClient` / DuckDB / vectorbt 系 fixture があり、一部テストは unit でも初期化コストが高い可能性がある。

## 受け入れ条件
- [x] 遅いテスト群を特定できるように、ローカルで durations 計測または同等の可視化手段が提供される。
- [x] `slow` marker の付与基準が決まり、通常開発時に slow を除外した実行手段が用意される。
- [x] ローカル向けに任意の並列実行オプション（例: `pytest-xdist`）を導入するか、導入しない理由を文書化する。
- [x] FastAPI app 初期化や DuckDB 初期化が不要な unit テストを service/domain 直呼びへ寄せる整理方針が定義される。

## 実施内容
- [x] `apps/bt/tests` のうち遅いファイル・重い fixture 利用箇所を棚卸しする。
- [x] `scripts/test-packages.sh` / `scripts/test-apps.sh` にローカル診断用 durations 出力や高速モードの導入可否を検討する。
- [x] `pytest-xdist` 導入時の並列安全性（tmp_path, env, XDG path, shared state）を確認する。
- [x] `apps/bt/tests/conftest.py` と `tests/unit/server/**` を中心に、HTTP 起動不要のテストを軽量化する候補を抽出する。
- [x] README または docs に「通常フルスイート」「高速ローカル実行」「slow 含む完全確認」の使い分けを記載する。

## 結果
- 2026-03-24: `[scripts/bt-pytest.sh](/Users/shinjiroaso/.codex/worktrees/3e53/trading25/scripts/bt-pytest.sh)` を追加し、`BT_PYTEST_FAST=1` による `slow` 除外、`BT_PYTEST_DURATIONS` による durations 可視化、`BT_PYTEST_MARKEXPR` による任意の `-m` 条件指定を可能にした。`[scripts/test-packages.sh](/Users/shinjiroaso/.codex/worktrees/3e53/trading25/scripts/test-packages.sh)` と `[scripts/test-apps.sh](/Users/shinjiroaso/.codex/worktrees/3e53/trading25/scripts/test-apps.sh)` から共通利用する形に整理した。
- 2026-03-24: `[apps/bt/docs/test-runtime.md](/Users/shinjiroaso/.codex/worktrees/3e53/trading25/apps/bt/docs/test-runtime.md)` を追加し、baseline hotspot、`slow` marker policy、full/fast 実行コマンド、`pytest-xdist` をまだ導入しない理由、今後の lightweight refactor targets を明文化した。
- 2026-03-24: J-Quants retry/backoff と worker timeout 系の人工待機を短縮し、runtime-heavy な検証だけを `slow` 管理へ寄せた。対象は `[test_jquants_client.py](/Users/shinjiroaso/.codex/worktrees/3e53/trading25/apps/bt/tests/unit/server/clients/test_jquants_client.py)`、`[test_backtest_worker.py](/Users/shinjiroaso/.codex/worktrees/3e53/trading25/apps/bt/tests/unit/server/test_backtest_worker.py)`、`[test_lab_worker.py](/Users/shinjiroaso/.codex/worktrees/3e53/trading25/apps/bt/tests/unit/server/test_lab_worker.py)`、`[test_optimization_worker.py](/Users/shinjiroaso/.codex/worktrees/3e53/trading25/apps/bt/tests/unit/server/test_optimization_worker.py)`、`[test_time_series_store.py](/Users/shinjiroaso/.codex/worktrees/3e53/trading25/apps/bt/tests/unit/server/db/test_time_series_store.py)`、`[test_backtest_executor_mixin_paths.py](/Users/shinjiroaso/.codex/worktrees/3e53/trading25/apps/bt/tests/unit/strategies/mixins/test_backtest_executor_mixin_paths.py)`、`[test_indicators.py](/Users/shinjiroaso/.codex/worktrees/3e53/trading25/apps/bt/tests/unit/utils/test_indicators.py)`。
- 2026-03-24: smoke baseline helper が `TRADING25_*` env を漏らして `tests/paths` / `tests/security` を壊していた cross-test state leakage を修正した。対象は `[test_collect_production_smoke_baseline.py](/Users/shinjiroaso/.codex/worktrees/3e53/trading25/apps/bt/tests/unit/test_collect_production_smoke_baseline.py)`、`[test_resolver.py](/Users/shinjiroaso/.codex/worktrees/3e53/trading25/apps/bt/tests/paths/test_resolver.py)`、`[test_security_validation.py](/Users/shinjiroaso/.codex/worktrees/3e53/trading25/apps/bt/tests/security/test_security_validation.py)`。
- 2026-03-24: 不変な app/schema/market fixture を module 共有に寄せ、server unit の起動コストを削減した。対象は `[test_analytics_complex.py](/Users/shinjiroaso/.codex/worktrees/3e53/trading25/apps/bt/tests/unit/server/routes/test_analytics_complex.py)`、`[test_openapi.py](/Users/shinjiroaso/.codex/worktrees/3e53/trading25/apps/bt/tests/unit/server/test_openapi.py)`、`[test_routes_portfolio_performance.py](/Users/shinjiroaso/.codex/worktrees/3e53/trading25/apps/bt/tests/unit/server/test_routes_portfolio_performance.py)`。
- 2026-03-24: 主要な spot check と回帰確認を実施した。`tests/unit/server/clients/test_jquants_client.py` は `23 passed in 0.31s`、runtime-heavy subset は `65 passed, 5 deselected in 3.02s`、`analytics_complex + openapi + portfolio_performance` は `63 passed in 6.02s` を確認した。

## 補足
- 現時点では約 6 分は異常ではないが、開発ループ短縮の観点では改善余地がある。
- CI はすでに `package-unit-tests` と `app-integration-tests` に分割されているため、まずはローカル体験の改善を優先する。
- 追加の hotspot は残っているが、この issue では「高速ローカル実行の入口」「`slow` 運用」「xdist 保留理由」「軽量化方針の明文化」を到達点とし、その範囲は満たした。
- 継続的な runtime 削減は `bt-048` で管理する。
