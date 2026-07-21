# Market Sync Simplification and Fractional AdjVo Design

## Goal

`market.duckdb` の個人運用を、明示的な破壊的再構築である `initial` と、毎日の更新・欠損補完を担う `incremental` の2経路へ限定する。同時に issue #507 を修正し、J-Quants が返す小数の provider-adjusted daily volume (`AdjVo`) を DuckDB、Parquet、Dataset、分析、API の全経路で保持する。

## Public sync contract

- `POST /api/db/sync` の `mode` は `initial` と `incremental` のみ受け付ける。既定値は `incremental` とする。
- `initial` は `resetBeforeSync: true` を必須とする。`incremental` は `resetBeforeSync: false` のみ受け付ける。
- `initial` は既存 DB の schema/version を開く前に、固定された Market root 配下の `market.duckdb`、WAL、market Parquet のみを削除し、現行 schema v5 を作り直して full sync する。
- `incremental` は現行の DuckDB inspection anchor、missing stock dates、fundamentals、indices、options、margin の更新・欠損補完を実行する。旧 `repair` の fundamentals 回復能力は既にこの経路に含まれる。
- Web UI は `incremental` を初期選択にし、`initial` 選択時は常に `RESET` の文字入力確認を要求する。
- `enforceBulkForStockData` は sync mode と独立した効率制御として維持する。

## Removed architecture

次を active runtime、CLI、Web UI、OpenAPI contract、テスト、runbook、repository guidance から削除する。

- `auto` sync mode
- `repair` sync mode と Warning Recovery UI
- reset eligibility と「compatible Market v5 のみ live reset 可」という制約
- `bt market-cutover` CLI
- cutover、rehearsal、backup、journal、promotion、retained runtime、quarantine、atomic directory activation の専用実装
- cutover 専用 production smoke strategy と runbook

通常運用でも必要な writer lease、in-process lock、固定 root、path confinement、symlink/special-file rejection、通常 compaction の atomic file exchange、maintenance finalizer は残す。`initial` が削除する対象は Market Data Plane に限定し、`portfolio.db` と `datasets/` は変更しない。

未知の sync mode を `initial` にフォールバックしてはならない。契約検証または strategy resolution で明示的に拒否する。

## Issue #507: numeric semantics

- raw daily `Vo` は非負整数のまま保持する。
- minute volume は非負整数のまま保持する。
- daily provider-adjusted `AdjVo` は finite、非負の `float` として受理する。
- `stock_data_raw.adjusted_volume` と consumer `stock_data.volume` のみ `DOUBLE` にする。
- physical Market schema version は 5、Dataset manifest payload schema version は 4 のままとする。既存の旧 column type を持つ DB は migration せず、RESET initial で再構築する。
- provider window validation は raw volume の整数性だけを要求し、adjusted volume を float のまま保持する。provider-adjusted consistency は整数丸めではなく provider precision を考慮した float 比較で検証する。
- drift detection、Dataset copy、Parquet export、event-time ranking、daily market/dataset/watchlist API から adjusted daily volume の整数 cast を除去する。
- daily API volume は number/float、minute volume は integer として OpenAPI に公開する。

## Data flow

`initial` は typed confirmation 済みの request から writer lease を取得し、旧 Market handles を要求せずに安全な reset/open を行い、その新しい `MarketDb` と `MarketTimeSeriesStore` で `InitialSyncStrategy` を実行する。

`incremental` は現行 Market v5 handles を取得し、schema と adjustment mode を検証して `IncrementalSyncStrategy` を実行する。DB が missing、malformed、pre-v5、または column contract 不一致なら destructive `initial` を案内し、migration や compatibility read は行わない。

日足 REST row は `Vo` を整数、`AdjVo` を float として `stock_data_raw` に取り込み、同一 per-code transaction で `AdjVo` を consumer `stock_data.volume` に publish する。Dataset snapshot と analytics/API はこの値を変更せず伝播する。

## Error handling

- `initial` で `resetBeforeSync` が true でなければ 422。
- `incremental` で `resetBeforeSync` が true なら 422。
- active job がある場合は従来どおり 409。
- reset target が固定 root 外、symlink、special file、または lease 競合なら reset を開始せず失敗する。
- reset/open が失敗した場合は元エラーを保持し、未作成の finalizer/session による二次例外で覆い隠さない。
- provider daily row の `AdjVo` が negative、NaN、infinite、または非数値なら拒否する。
- unknown strategy は例外とし、destructive initial へ fallback しない。

## Verification

TDD で以下を実証する。

- `AdjVo=87308.9` が row conversion、provider window、Market DuckDB、Parquet、Dataset v4、event-time ranking、daily APIs を変更なく通過する。
- negative/non-finite `AdjVo` を拒否し、fractional raw `Vo` を拒否する。
- raw daily `Vo` と minute volume の物理型・API contract は integer のまま。
- `initial + resetBeforeSync=true` は missing/malformed/incompatible root でも旧 DB を開かず reset/rebuild を開始できる。
- `initial` without reset、`incremental` with reset、`auto`、`repair`、unknown mode を拒否する。
- frontend は2モードだけを表示し、initial は常に RESET confirmation、incremental は通常起動する。
- cutover CLI/runtime/package/runbook と active guidance の参照が残っていない。
- backend tests、ruff、pyright、OpenAPI sync、frontend tests、typecheck が通る。

## Scope boundaries

Dataset snapshot format自体、portfolio DB、intraday sync mode、stock refresh endpoint、Market schema version、provider-adjusted price semantics、normal compaction/finalization は変更しない。過去の `docs/superpowers/` 設計・計画は履歴として残せるが、active guidance から参照しない。
