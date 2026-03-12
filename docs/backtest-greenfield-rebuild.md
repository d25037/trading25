# Backtest Greenfield Rebuild Notes

作成日: 2026-03-08

## 1. 結論

現行のバックテストシステムは「かなり良くなっているが、最良の形ではない」と考える。

- 良い点ははっきりしている。
  - FastAPI を backend の SoT に寄せたこと
  - DuckDB + Parquet を market data の SoT にしたこと
  - artifact-first で結果を再解決する方向に寄せたこと
  - signal system / job API / web integration が一応つながっていること
- ただし、greenfield で導入した `market.duckdb + parquet` は market plane にしか入っておらず、dataset plane はまだ legacy `dataset.db` に強く依存している。
- 今後 `Nautilus Trader` を導入する可能性があるなら、設計の中心は「vectorbt を置き換えること」ではなく「multi-engine を成立させる抽象境界を先に作ること」になる。
- ただし、実行エンジンの境界はまだ曖昧で、責務分離も完全ではない。
  - `apps/bt/src/domains/backtest/core/runner.py`
  - `apps/bt/src/domains/backtest/core/marimo_executor.py`
  - `apps/bt/src/domains/strategy/core/mixins/backtest_executor_mixin.py`
  - `apps/bt/src/application/services/job_manager.py`
  - `apps/ts/packages/web/src/hooks/useBacktest.ts`

要するに、現行は「使える v2」であって、「今の知見でゼロから作る最終形」ではない。

## 2. 現行システムの評価

### 2.1 維持したいもの

- FastAPI を唯一の backend とする方針。
  - API 契約、ジョブ投入、結果取得の入口が一本化される。
- DuckDB + Parquet を research data plane の SoT とする方針。
  - 時系列・分析の負荷に対して妥当。
- `result.html + metrics + manifest` の artifact-first 発想。
  - job memory を唯一の SoT にしないのは正しい。
- web が typed client 経由で backend を使う構成。
  - `apps/ts/packages/web/src/lib/backtest-client.ts`
- strategy/signal/fundamentals を `src/domains/*` に寄せる方向。
  - 実装上の改善余地は大きいが、方向自体は正しい。
- market time-series を `market.duckdb + parquet` に寄せた判断。
  - SoT と同期・検証の軸がかなり明快になった。

### 2.2 現行が「最良ではない」理由

- 実行の本体がまだ notebook export に強く引っ張られている。
  - `BacktestRunner` は `MarimoExecutor` を直接呼び、HTML 出力が実行パスの中心に近い。
  - 理想では HTML は presentation artifact であり、simulation engine の本体ではない。
- 戦略オブジェクトが抱える責務が重い。
  - `backtest_executor_mixin.py` はデータロード、signal 生成、execution policy、portfolio 作成、Kelly 最適化を同時に抱えている。
  - 1 ファイルで 979 行ある時点で、関心の分離が足りない。
- execution semantics が signal processing 側に漏れている。
  - 今回の `current_session_round_trip_oracle` のような仕様は、本来は「どの情報がいつ利用可能か」という availability model で扱うべきで、個別 signal の 1 日ラグとして散らすべきではない。
- worker 分離が不完全。
  - backtest / optimize / lab / screening は API プロセス内の `asyncio.create_task + ThreadPoolExecutor` で動く。
  - これはローカル用途では実用的だが、job plane と API plane がまだ同居している。
- 実行パスが一枚岩ではない。
  - backtest は `BacktestRunner -> MarimoExecutor` 寄りの流れだが、optimize / lab は strategy runtime をより直接叩いている。
  - つまり「同じ engine を別 UI から使う」のではなく、「近いが少し違う実行系」が並んでいる。
- data plane が二重化している。
  - market plane は `market.duckdb + parquet` に進んだが、dataset plane はまだ `dataset.db` 読み書きが残っている。
  - `apps/bt/src/infrastructure/db/market/dataset_db.py`
  - `apps/bt/src/infrastructure/db/dataset_io/dataset_writer.py`
  - `apps/bt/src/infrastructure/data_access/clients.py`
  - `apps/bt/src/infrastructure/data_access/loaders/index_loaders.py`
  - この状態では「dataset snapshot も research snapshot である」という整理がシステム上まだ徹底できていない。
- vectorbt 依存が execution に閉じていない。
  - portfolio 実行だけでなく strategy protocol、portfolio analytics、indicator/signal 計算にも `vectorbt` が染み出している。
  - 将来 `Nautilus Trader` を併存させるなら、このままでは adapter 境界を切れない。
- job モデルは便利だが、実験管理モデルとしては薄い。
  - `JobInfo` は status 管理には十分でも、dataset version / strategy version / execution model version / parent run の追跡が弱い。
- cancel は durable execution control というより status control に近い。
  - run を本当に止める仕組みより、「止めたことにする」管理の比重がまだ高い。
- backend は SSE を持っているのに、web の backtest / optimize は polling が主導線。
  - SSE と polling の二重設計が残っている。
  - lab は SSE を使うが、backtest 系は `refetchInterval` 中心。

## 3. ゼロから作り直すならどうするか

前提は次の通り。

- 主用途は日本株の research / screening / backtest / optimize / lab
- 日足中心だが、execution policy は厳密に扱いたい
- local-first で開発可能であること
- 再現性と no-lookahead を最優先すること

私なら、次の 6 つを中核に置く。

1. `Research Snapshot Plane`
2. `Dataset Snapshot Plane`
3. `Feature Availability Plane`
4. `Strategy Spec + Compiler`
5. `Execution Engine Abstraction`
6. `Experiment Registry + Artifact Catalog`

## 4. 目標アーキテクチャ

```text
ts/web
  -> FastAPI API (control plane only)
      -> Job Registry / Run Registry
      -> Worker Lease / Cancel / Stream
      -> Artifact Catalog

Worker Runtime
  -> Snapshot Resolver
  -> Feature Builder
  -> Strategy Compiler
  -> Engine Adapter
      -> VectorbtAdapter
      -> NautilusAdapter
  -> Evaluator / Attribution / Optimization
  -> Artifact Writer

Storage
  -> market-timeseries/market.duckdb + parquet
  -> datasets/{snapshot}/dataset.duckdb + parquet + manifest
  -> SQLite or Postgres: jobs / runs / strategy versions / artifact index
  -> Filesystem object store: html / json / parquet / manifests
```

### 4.1 Control Plane

FastAPI は orchestration だけを持つ。

- run を作る
- run status を返す
- cancel する
- SSE で stream する
- artifact を列挙する
- strategy spec / dataset snapshot を解決する

ここでは simulation しない。API プロセスは重い計算を持たない。

### 4.2 Research Data Plane

DuckDB + Parquet を維持する。ただし market と dataset を分断しない。

- market plane は mutable canonical store とする
  - `market.duckdb + parquet`
  - sync, inspection, validation, latest pointer の SoT
- dataset plane は immutable snapshot store とする
  - `datasets/{snapshot}/dataset.duckdb + parquet + manifest.json`
  - create/resume の成果物 SoT
- market data は immutable snapshot view と mutable latest pointer を分ける
- feature table も snapshot 単位で materialize できるようにする
- run は必ず snapshot ID を pin する
- screening / backtest / optimize / lab が同じ snapshot resolver を使う

resolver policy は次で固定する。

- market plane は `market:latest` を canonical ID とし、`MARKET_TIMESERIES_DIR/market.duckdb` を解決する
- dataset plane は dataset snapshot 名を canonical ID とし、`datasets/{snapshot}/dataset.duckdb` を優先する
- dataset snapshot で DuckDB が欠ける移行期間は `datasets/{snapshot}/dataset.db` compatibility artifact を読む
- legacy flat `datasets/{snapshot}.db` は最後の fallback とし、new snapshot bundle があればそちらを優先する
- public API は `GET /api/snapshots/resolve` で market latest / dataset snapshot を同一契約で返す

重要なのは「同じ strategy でも、同じ snapshot を pin しなければ同じ結果を保証しない」ことをシステムとして明示すること。

### 4.3 Dataset Snapshot Plane

これは今回の追加前提で最重要。

現状の dataset は immutable snapshot という概念自体は正しいが、物理形式が legacy `dataset.db` に寄っている。  
greenfield の一体設計では、dataset も market と同じく DuckDB + Parquet へ寄せる。

理想の snapshot 構成:

```text
datasets/{dataset_name}/
  manifest.v1.json
  dataset.duckdb
  parquet/
    stocks.parquet
    stock_data.parquet
    topix_data.parquet
    indices_data.parquet
    margin_data.parquet
    statements.parquet
```

この形にすると:

- market plane と dataset plane の query model を揃えられる
- `DatasetDb` 専用の SQLite 制約から解放できる
- backtest / optimize / lab / charts が同じ columnar snapshot contract を共有できる
- 将来 `Nautilus` 用に bar/event snapshot を追加しても同じ registry に載せられる

移行期間は `dataset.db` を compatibility artifact として残してもよいが、SoT は `dataset.duckdb + parquet + manifest` にする。

### 4.4 Control DB

この repo の前提なら最初は SQLite でよい。ただし interface は Postgres 差し替え前提で切る。

- 単一ユーザー・単一マシンなら SQLite で十分
- 複数 worker / 複数ユーザー / remote execution が必要になったら Postgres に移行

大事なのは DB 製品名ではなく、control plane と research plane を明確に分離すること。

## 5. 実行モデルの作り方

### 5.1 Strategy は YAML ではなく IR を SoT にする

YAML は authoring format としては残してよいが、実行時 SoT にはしない。

理想の流れ:

1. YAML/JSON/UI 編集
2. strict validation
3. `StrategySpec` に正規化
4. `CompiledStrategyIR` を生成
5. engine は IR だけを受け取って実行

これで次が可能になる。

- 静的 validation
- dependency analysis
- feature prefetch 計画
- strategy fingerprint 生成
- optimize/lab/screening/backtest の共通実行

### 5.2 Availability model を first-class にする

これは現行の最大改善点。

今の実装では、`current_session_round_trip_oracle` の正しさを保つために、非 oracle entry signal を 1 日ラグさせる必要が出た。これは応急処置としては正しいが、理想形ではない。

ゼロから作るなら、各 feature と signal に対して次を持たせる。

- `observation_time`
- `available_at`
- `decision_cutoff`
- `execution_session`

例:

- 前日終値ベースの RSI は「当日寄り前には利用可能」
- 当日寄り付きギャップ oracle は「当日寄り直後に利用可能」
- fundamentals は「開示日時以後にのみ利用可能」

こうしておけば、engine が no-lookahead を強制できる。

現行 `bt-040` の途中実装では、この availability model を `CompiledStrategyIR.signals[].availability` として生成し、`SignalProcessor` の same-session oracle ラグ判定は compiled availability を優先するようにした。あわせて `/api/signals/reference` でも signal ごとの availability profile を返し、authoring UI から `standard` / `next_session_round_trip` / `current_session_round_trip_oracle` の差を確認できるようにしている。

- signal processor 側で個別に `shift(1)` する必要がない
- same-day oracle と prior-day signal を同じ run 内で安全に共存できる
- screening / backtest / lab / optimize が同じ時間意味論を共有できる

### 5.3 Execution Engine は抽象化する

`Nautilus Trader` 導入可能性を前提にするなら、ここは「専用 kernel を即自作する」より先に「engine-neutral contract を SoT にする」べきである。

authoritative な SoT:

- `RunSpec`
- `CompiledStrategyIR`
- `EngineInputSnapshot`
- `CanonicalExecutionResult`

engine 実装:

- `VectorbtAdapter`
  - 高速 research
  - screening に近い評価
  - optimize/lab の一次探索
- `NautilusAdapter`
  - 高忠実度 backtest
  - execution semantics / fill model / event-driven verification
  - paper/live parity を意識した最終検証

この設計なら `vectorbt` は重要な backend のまま残せる。  
ただし SoT ではなくなる。

### 5.4 VectorBT と Nautilus の役割分担

私は次の分担を推奨する。

- `vectorbt`
  - bulk な候補探索
  - 高速な parameter sweep
  - signal system の研究
  - notebook sandbox
- `Nautilus`
  - 現実装は日足 `Open/Close` ベースの verification run のみに限定する
  - `RunSpec.engine_family=nautilus` を worker runtime で解釈し、single backtest run を canonical result に正規化する
  - 未対応の execution semantics / timeframe / data shape は fail-fast し、`vectorbt` へ自動 fallback しない
  - `metrics.json` / `manifest.json` / `engine.json` / `diagnostics.json` を core artifact とし、`result.html` は生成しない
  - real runtime smoke は optional dependency を入れた separate CI workflow で観測し、default CI には混ぜない
  - intraday / event-driven execution model、paper/live parity は次段階の拡張範囲として残す

つまり、全 run を `Nautilus` に寄せるのではなく、`fast path` と `verification path` を分ける。

### 5.5 engine 抽象化のために先に外すべき依存

現状は以下が `vectorbt` に直接結びついている。

- strategy protocol の portfolio 型
- strategy オブジェクトの状態
- backtest executor の portfolio 作成
- Kelly 計算の trade access
- indicator/signal 計算の一部

したがって先に必要なのは:

1. `vbt.Portfolio` を public/domain interface から外す
2. indicator/signal 計算を `vectorbt` 非依存へ寄せる
3. canonical result schema を先に固定する
4. その上で `VectorbtAdapter` と `NautilusAdapter` を並立させる

## 6. 実験管理の作り方

run は単なる job ではなく experiment record として扱う。

最低でも次を保存する。

- `run_id`
- `run_type` (`screening`, `backtest`, `optimize`, `lab`, `attribution`)
- `strategy_fingerprint`
- `strategy_source_ref`
- `dataset_snapshot_id`
- `feature_snapshot_id`
- `execution_policy_version`
- `engine_family` (`vectorbt`, `nautilus`, `custom`)
- `engine_version`
- `code_version`
- `parent_run_id`
- `parameters`
- `artifacts`
- `metrics`

これでできるようになること:

- 同一 run の厳密再現
- optimize trial の lineage 追跡
- lab での派生戦略の provenance 管理
- `best_params` だけでなく「どのデータ・どの engine で best だったか」を説明可能

## 7. 成果物の扱い

presentation artifact と canonical artifact を分ける。

### 7.1 canonical artifact

- `manifest.json`
- `metrics.json`
- `positions.parquet`
- `trades.parquet`
- `signals.parquet`
- `equity_curve.parquet`
- `diagnostics.json`
- `engine.json`

### 7.2 presentation artifact

- `result.html`
- chart images
- downloadable summaries

`result.html` は見るためのものに限定する。  
canonical な結果再解決は HTML パースに寄せない。
`vectorbt` path は presentation artifact として `result.html` を持てるが、`Nautilus` verification path は core artifact のみを authoritative output とし、`html_path=None` を許容する。

### 7.3 registry reader の優先順位

移行期間の read path は次の順で解決する。

1. `artifact_index`
2. `canonical_result`
3. legacy job columns (`result_json`, `raw_result_json`, `html_path`)

つまり route は個別に `html_path` や `raw_result` を読むのではなく、artifact-first な registry reader を経由して summary / attribution result を復元する。  
legacy columns は compatibility path であり SoT ではない。

## 8. Optimize / Lab の作り方

現行では backtest, optimize, lab が job manager を共有しつつ、それぞれ少しずつ違う流れを持っている。ゼロからなら、これらは全部「compiled strategy + snapshot + engine adapter + evaluator」を共有する。

### 8.1 Optimize

- parameter search space を IR 化する
- trial は compiled strategy diff として表現する
- evaluator は backtest と完全共通にする
- best/worst だけでなく Pareto frontier を保存する
- pruning, retry, seed, cache key を run registry で管理する
- 一次探索は `VectorbtAdapter`、再検証は `NautilusAdapter` を選べるようにする

### 8.2 Lab

- generate / evolve / improve は strategy authoring workflow として分離する
- 評価自体は backtest engine を使う
- 生成戦略は YAML 直書きではなく strategy registry に publish する
- human approval を経て production へ promote する
- lab の候補比較は fast path と verification path を切り替えられるようにする

## 9. クライアント設計

### 9.1 web

web は次の導線だけ持てばよい。

- run create
- run stream
- run result
- artifact browser
- strategy editor
- optimization explorer

job 状態は SSE を主導線にする。polling は fallback にする。

現行のように backend が SSE を持っているのに、backtest / optimize 側 UI が polling 主体なのは中途半端なので、ゼロからなら最初から揃える。

### 9.2 notebook

notebook は「engine の実行面」ではなく「分析・可視化面」に限定する。

- notebook は canonical artifact を読む
- notebook から engine 本体を直接起動しない
- report template は renderer layer に置く

これは marimo をやめるという意味ではない。  
marimo を job execution の中心から外すという意味である。

## 10. Dataset と Engine を一体でどう移行するか

ここは separate concern ではない。  
dataset plane と engine abstraction は一緒に進める必要がある。

理由:

- `Nautilus` を入れるなら snapshot contract を明確にする必要がある
- dataset が SQLite のままだと engine 間で同じ入力 snapshot を保証しづらい
- canonical result の比較には canonical input snapshot も必要

私なら移行順をこう切る。

### Phase A: Canonical contracts を先に固定

- `RunSpec`
- `CompiledStrategyIR`
- `DatasetSnapshotManifest`
- `CanonicalExecutionResult`

### Phase B: dataset plane を DuckDB + Parquet へ移す

- `dataset.db` の SoT をやめる
- `dataset.duckdb + parquet + manifest` を新 SoT にする
- `DatasetDb` は compatibility reader として残す
- loaders/client は snapshot resolver 経由に統一する
- HTTP request schema でも `source='market'` と dataset snapshot 名を同じ contract で扱う

### Phase C: vectorbt を adapter 化する

- 現行 runtime を `VectorbtAdapter` の中へ押し込む
- domain interface から `vbt.Portfolio` を消す
- canonical result writer を導入する

### Phase D: Nautilus を verification engine として追加する

- 同じ `RunSpec` と snapshot から worker runtime 上で Nautilus verification run を起動する
- backtest family の single run を engine-aware に dispatch できるようにする
- result を canonical schema と artifact registry に正規化する
- 初期スコープは日足 `Open/Close` verification とし、HTML 生成や比較 orchestration は含めない

### Phase E: optimize/lab を二段化する

- trial 全件は vectorbt
- 上位候補のみ Nautilus
- `bt-044` で導入した core artifact / canonical result を使って fast path と verification path を比較保存する
- 差分が大きい候補は warning / invalidation / priority review の対象にする

## 11. ゼロからでも変えない判断

全部を捨てる必要はない。今の知見で残すべきものもある。

- FastAPI 一本化
- DuckDB + Parquet
- contracts/OpenAPI 駆動
- artifact-first
- domain logic を `src/domains/*` に集める方向
- local-first

逆に、最初から捨てるものは次の通り。

- notebook export を authoritative execution path に置くこと
- API プロセス内 ThreadPool を worker の代わりに使い続けること
- YAML を実行時 SoT にすること
- signal 側で ad-hoc に lookahead 調整すること
- dataset snapshot の SoT を SQLite のまま固定すること
- presentation artifact と canonical artifact を混ぜること

## 12. 今のコードベースで先にやるべき改善

全面作り直しをしなくても、次の順で寄せるのが一番効く。

1. `BacktestRunner` から marimo 実行を外し、simulation result と report rendering を分離する。
2. `current_session_round_trip_oracle` のような例外処理を一般化し、availability model を導入する。
3. dataset snapshot を `dataset.db` から `dataset.duckdb + parquet + manifest` へ移し、snapshot resolver を一本化する。
4. `backtest_executor_mixin.py` を分割し、`data loading`, `signal compilation`, `execution policy`, `portfolio assembly` を別モジュールに分ける。
5. domain interface から `vbt.Portfolio` を外し、`VectorbtAdapter` の境界を作る。
6. backtest / optimize / lab が同じ compiled strategy execution pipeline を通るように揃える。
7. API 内 background task を worker process に移し、job lease と cancel を durable にする。
8. `NautilusAdapter` を verification engine として追加する。
9. backtest / optimize / lab / attribution の run metadata を experiment registry として統一する。
10. web の backtest / optimize を SSE 主導に寄せる。
11. result summary の canonical source を `metrics.json + manifest.json` に固定する。

## 13. 最終的な考え

現行システムは、方向としてはかなり正しい。  
特に「FastAPI SoT」「DuckDB SoT」「artifact-first」「typed contract」は捨てるべきではない。

ただし、今の知見でゼロから作るなら、設計の中心は notebook でも YAML でも job manager でも `vectorbt` でもない。  
中心に置くべきなのは次の 3 つである。

- `availability-aware compiled strategy`
- `engine-neutral execution contract`
- `experiment registry with canonical artifacts`

その上で:

- market plane は `market.duckdb + parquet`
- dataset plane は `dataset.duckdb + parquet + manifest`
- engine は `vectorbt + Nautilus` の併存

この 3 つを先に固定すれば、web, optimize, lab, attribution, notebooks はその上に自然に載る。  
逆にここが曖昧なままだと、仕様追加のたびに signal 遅延や例外フラグが増えていく。
