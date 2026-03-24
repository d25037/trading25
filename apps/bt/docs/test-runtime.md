# bt Test Runtime Guide

`apps/bt` のテストは `full` を正本に保ちつつ、ローカルの通常開発では `fast` モードを使い分ける。

## Runtime Snapshot

2026-03-24 の着手時 baseline:

```bash
cd apps/bt
./.venv/bin/pytest tests/unit tests/api tests/integration tests/paths tests/security tests/server -q --durations=40
```

- 収集: 4,521 tests
- 実行時間: 約 116 秒
- 主な hotspot:
  - J-Quants retry/backoff 系
  - worker timeout 系
  - vectorbt round-trip 実行検証
  - DuckDB publish/inspect 直列化検証

retry/backoff と timeout の人工待機はテスト側で短縮し、残る runtime-heavy な検証だけを `slow` marker で管理する。

変更後の spot check:

- `tests/unit/server/clients/test_jquants_client.py`: 23 passed in 0.31s
- `tests/unit/server/db/test_time_series_store.py` などの fast mode subset: 65 passed, 5 deselected in 3.02s

## Slow Marker Policy

`slow` は次の条件のいずれかを満たすテストに付与する。

- `vectorbt` の実ポートフォリオ生成や trade-book 検証を伴い、通常の inner loop では毎回不要
- DuckDB concurrency / serialization のような重い統合検証
- 外部ライブラリ互換性の数値比較で、正確性確認の価値は高いが毎回は不要

逆に、retry や timeout のように wall-clock 待機をモックで潰せるテストは `slow` に逃がさず、まず待ち時間を除去する。

## Local Commands

通常の full 実行:

```bash
./scripts/test-packages.sh
./scripts/test-apps.sh
```

`slow` を除外した高速ローカル実行:

```bash
BT_PYTEST_FAST=1 ./scripts/test-packages.sh
BT_PYTEST_FAST=1 ./scripts/test-apps.sh
```

durations を見ながら実行:

```bash
BT_PYTEST_DURATIONS=25 ./scripts/test-packages.sh
BT_PYTEST_DURATIONS=25 ./scripts/test-apps.sh
```

高速ローカル実行と durations を併用:

```bash
BT_PYTEST_FAST=1 BT_PYTEST_DURATIONS=25 ./scripts/test-packages.sh
BT_PYTEST_FAST=1 BT_PYTEST_DURATIONS=25 ./scripts/test-apps.sh
```

`BT_PYTEST_MARKEXPR` を指定すると、`BT_PYTEST_FAST=1` より優先して pytest の `-m` 条件をそのまま渡す。

## Why Not xdist Yet

`pytest-xdist` の導入は保留にする。

理由:

- 現在の suite は environment / XDG path / resolver state を跨いで変更するテストがあり、並列化前に隔離の棚卸しが必要
- 2026-03-24 の full 実測でも `tests/paths/test_resolver.py` と `tests/security/test_security_validation.py` に cross-test state leakage が見えており、まず順次実行前提の shared-state を解消すべき

並列化を再検討する条件:

- path resolver / config loader 系テストが毎回独立に通る
- `tmp_path`, env vars, `~/.local/share/trading25` 相当の解決先を worker ごとに分離できる
- shared singleton / global cache の初期化が process-safe と確認できる

## Lightweight Refactor Targets

今後の軽量化候補:

- `tests/unit/server/**` で route 単位の I/O が不要なものを service/domain 直呼びへ寄せる
- `tests/conftest.py` の `create_app()` / `TestClient` / DuckDB 初期化 fixture を pure unit から外す
- retry / timeout / polling 系で fake clock または injected sleeper を使い、実時間依存をなくす
