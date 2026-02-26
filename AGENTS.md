## Role
あなたは apps/bt/ と apps/ts/ の結合を統合管理するオーケストレーターです。
subagentsを用いてそれぞれのプロジェクトを横断的に把握します。

## データフロー・ポート割り当て

```
JQUANTS API ──→ FastAPI (:3002) ──→ SQLite (market.db / portfolio.db / datasets)
                     ↓
                  ts/web (:5173)
                  ts/cli
```

| サービス | ポート | 技術 |
|---|---|---|
| bt/server | 3002 | FastAPI + uvicorn |
| ts/web | 5173 | Vite + React 19 |

- **FastAPI** が唯一のバックエンド
- **bt** は SQLite に直接アクセス（`contracts/` スキーマ準拠、SQLAlchemy Core 使用）
  - **market.db**: 読み書き（SQLAlchemy Core）
  - **portfolio.db**: CRUD（SQLAlchemy Core）
  - **dataset.db**: 読み書き（SQLAlchemy Core）
- `market.db` の `incremental sync` は `topix_data` / `stock_data` だけでなく `indices_data` も更新する。`index_master` はローカル catalog を SoT として補完し、`indices_data` は code 指定同期（catalog + 既存DBコード）を基本に、日付指定同期で新規コードを補完する（`indices-only` は指数再同期専用モード）。不足 `index_master` はプレースホルダ補完し、FK 制約付きの既存DBでも継続可能にする
- `market.db` の `statements` upsert は `(code, disclosed_date)` 衝突時に非NULL優先マージ（`coalesce(excluded, existing)`）とし、同日別ドキュメント取り込み時の forecast 欠損上書きを防止する
- Backtest 実行パスは `BT_DATA_ACCESS_MODE=direct` で DatasetDb/MarketDb を直接参照し、FastAPI 内部HTTPを経由しない
- DatasetDb の `statements` 読み取りは legacy snapshot（配当/配当性向の forecast 列欠落）でも `NULL` 補完で継続し、必須列 `code` / `disclosed_date` 欠落時はエラーにする
- Dataset API `GET /api/dataset/{name}/info` の SoT は `snapshot` + `stats` + `validation`（`details.dataCoverage` / `details.fkIntegrity` / `details.stockCountValidation` 含む）とし、web 側は legacy `snapshot` 形式を正規化して後方互換を維持する
- Backtest result summary の SoT は成果物セット（`result.html` + `*.metrics.json`）。`/api/backtest/jobs/{id}` と `/api/backtest/result/{id}` は成果物から再解決し、必要時のみ job memory/raw_result をフォールバックとして使う
- Screening API は非同期ジョブ方式（`POST /api/analytics/screening/jobs` / `GET /api/analytics/screening/jobs/{id}` / `POST /api/analytics/screening/jobs/{id}/cancel` / `GET /api/analytics/screening/result/{id}`）を SoT とする。旧 `GET /api/analytics/screening` は 410
- Screening 実行時のデータ SoT は `market.db`（`stock_data` / `topix_data` / `indices_data` / `stocks`）とし、dataset へのフォールバックを禁止する
- Strategy 設定検証の SoT は backend strict validation（`/api/strategies/{name}/validate` と保存時検証）で、frontend のローカル検証は補助扱い（deprecated）
- Strategy YAML更新の SoT は `/api/strategies/{name}` で、`production` / `experimental` を更新可能（`production` は既存ファイルの編集のみ許可）。`rename` / `delete` は引き続き `experimental` 限定
- Strategy `rename` / `delete` の権限判定はトップレベルカテゴリ基準で行い、`experimental/**`（例: `experimental/optuna/foo`）は許可する
- 市場コードフィルタは legacy (`prime/standard/growth`) と current (`0111/0112/0113`) を同義として扱う
- **ts/web** は `/api` パスを FastAPI (:3002) にプロキシ
- **Hono サーバー** (:3001) は廃止済み（`apps/ts/packages/api` は削除済み）

## OpenAPI契約

bt が FastAPI の OpenAPI スキーマを公開し、ts/shared が型を自動生成する。
```bash
bun run --filter @trading25/shared bt:sync   # bt の OpenAPI → TS型生成
```
スキーマ変更時は必ず `bt:sync` を実行し、`contracts/` 配下も更新すること。
- `apps/ts` workspace は `@redocly/openapi-core` を `1.34.5` に固定し、`bt:sync`（openapi-typescript）を安定実行する

## contracts/ ガバナンス

`contracts/` に bt/ts 間の安定インターフェースを定義。詳細は [`contracts/README.md`](contracts/README.md) 参照。
- **バージョニング**: additive (minor) / breaking (major) → 新版ファイル作成
- **命名規則**: `{domain}-{purpose}-v{N}.schema.json`
- **現行追加契約**: `fundamentals-metrics-v2.schema.json`（fundamentals API 指標拡張、`bookToMarket` を削除）
- **アーカイブ**: `hono-openapi-baseline.json`（Phase 3 移行 baseline、参照用に保持）

## エラーレスポンス

統一エラーレスポンスフォーマット:
```json
{"status":"error","error":"Not Found","message":"...","details?":[...],"timestamp":"...","correlationId":"..."}
```
- FastAPI: 例外ハンドラが `HTTPException(detail=...)` を自動変換
- `RequestLoggerMiddleware` が `JQuantsApiError`(502/504) / `SQLAlchemyError`(500) / 汎用例外(500) をキャッチし統一フォーマットで返却
- correlation ID: `x-correlation-id` ヘッダで伝播（なければ自動生成）
- 内部HTTP呼び出し（`src/api/client.py`）も `x-correlation-id` を伝播
- ErrorResponse スキーマは OpenAPI で全エンドポイントに 400/404/500 として公開

## J-Quants Proxy キャッシュ/観測

- `JQuantsProxyService` は in-memory TTL + singleflight を使用
  - `/markets/margin-interest`: 5分
  - `/fins/summary`（`/statements` / `/statements/raw` で共有）: 15分
- 実外部呼び出しは `event="jquants_fetch"`、キャッシュ状態は `event="jquants_proxy_cache"` で構造化ログ出力

## ミドルウェア構成（FastAPI）

登録順（LIFO: 下から上に実行）:
1. **RequestLoggerMiddleware** — リクエストロギング（最外側）
2. **CorrelationIdMiddleware** — correlation ID 管理
3. **CORSMiddleware** — CORS（最内側）

- OpenAPI 設定は `openapi_config.py` に集中管理
- ドキュメント UI: `/doc`（Swagger UI）、`/docs` `/redoc` は無効

## 共有XDGパス

両プロジェクトが `~/.local/share/trading25/` を共有:
- `market.db` / `datasets/` / `portfolio.db` — FastAPI が管理
- `strategies/experimental/` / `strategies/production/` / `strategies/legacy/` / `backtest/results/` / `backtest/attribution/` — bt が管理

## bt (Python / uv)
VectorBT基盤の高速バックテスト・Marimo Notebook実行システム。
FastAPI サーバー（:3002）とtyper CLI を提供。

```bash
uv sync                          # 環境セットアップ
uv run bt server --port 3002     # APIサーバー起動
uv run bt backtest <strategy>    # バックテスト実行
uv run bt lab generate --entry-filter-only --allowed-category fundamental
uv run bt lab evolve <strategy> --entry-filter-only --allowed-category fundamental
uv run bt lab optimize <strategy> --entry-filter-only --allowed-category fundamental
uv run bt lab improve <strategy> --entry-filter-only --allowed-category fundamental
uv run pytest tests/             # テスト
uv run ruff check src/           # リント
uv run pyright src/              # 型チェック
```

- Lab `evolve/optimize` の API/Web は `target_scope`（`entry_filter_only` / `exit_trigger_only` / `both`）を受け付ける（`entry_filter_only` は互換フラグとして維持）
- Lab `evolve/optimize` の frontend `allowed categories` は `all` / `fundamental only` を提供
- Lab frontend は `Run` / `History` タブを持ち、`/api/lab/jobs` で実行履歴を一覧し、選択したジョブの進捗・結果を再表示できる
- Lab `optimize`（Optuna）は開始時に OHLCV/benchmark を1回プリフェッチして trial 間で再利用し、`pruning=true` 時は第1段階バックテストの暫定スコアで早期枝刈りを行う
- Optimization HTML（`notebooks/templates/marimo/optimization_analysis.py`）は、各パラメータ組み合わせの `Trades`（closed trades件数）と Best detail の `Trade Count` を表示する
- `/api/optimize/jobs/{id}` は `best_score` / `total_combinations` に加えて `best_params` / `worst_score` / `worst_params` を返し、最適化ジョブ結果カードで best/worst 条件を比較表示できる
- `forward_eps_growth` / `peg_ratio` は FY実績EPSを分母に固定し、`period_type=FY` でも必要時のみ追加取得した四半期 FEPS 修正を forecast 側へ反映する
- Fundamental signal は `forecast_eps_above_recent_fy_actuals`（最新予想EPS > 直近FY X回の実績EPS最大値）をサポートし、`lookback_fy_count` で比較年数を指定できる。forecast revision 読み込み（screening/lab/optimization/backtest）を有効化する
- Fundamental signal system は `cfo_margin` / `simple_fcf_margin`（売上高比マージン判定）をサポートし、`OperatingCashFlow` / `InvestingCashFlow` / `Sales` をデータ要件とする
- Fundamental signal は `cfo_to_net_profit_ratio`（営業CF/純利益）をサポートし、`consecutive_periods` 判定は比率値同値時でも開示更新（OperatingCashFlow/Profit）を起点に連続判定する
- Fundamentals は EPS に加えて `dividend_fy` / `forecast_dividend_fy` と `payout_ratio` / `forecast_payout_ratio`（実績/予想）を SoT とし、Charts の Fundamentals panel と Backtest Signal system（`forward_dividend_growth` / `dividend_per_share_growth` / `payout_ratio` / `forward_payout_ratio`）で同一指標を使う。配当性向は API 返却時に percent 単位へ正規化し、decimal スケール値（例: 0.283）を 28.3% として扱う
- fundamentals 最新値の forecast EPS は同一期末内で `DiscDate` が新しい開示を優先し、旧開示値の逆転表示を防ぐ
- Charts Fundamentals panel は `forecastEpsAboveRecentFyActuals`（最新予想EPS > 直近FY X回の実績EPS最大値）を latest metrics で返し、`forecastEpsLookbackFyCount` に応じた true/false を表示する（旧 `forecastEpsAboveAllHistoricalActuals` は互換フィールド）
- `/api/analytics/fundamental-ranking` は `market.db`（`statements`/`stocks`/`stock_data`）を SoT とし、`metricKey` と `rankings.ratioHigh` / `rankings.ratioLow` を返す。現在の `metricKey` は `eps_forecast_to_actual`（最新の予想EPS / 最新の実績EPS）で、予想EPSは `revised(四半期) > adjusted FY forecast > raw FY forecast`、実績EPSは最新 FY EPS（share補正）を採用する。`forecastAboveRecentFyActuals=true` と `forecastLookbackFyCount` で「最新予想EPS > 直近FY X回の実績EPS最大値」条件を追加フィルタできる（旧 `forecastAboveAllActuals` も互換）。将来の比率指標追加は `metricKey` で識別する
- Strategy group 再振り分けは `/api/strategies/{strategy_name}/move`（`target_category`: `production` / `experimental` / `legacy`）を SoT とし、web の `Backtest > Strategies` から実行する

主要技術: Python 3.12, vectorbt, pydantic, FastAPI, pandas, ruff, pyright, pytest

## ts (TypeScript / bun)
日本株式の解析を行うTypeScriptモノレポ。ランタイムは **bun** を使用。

| パッケージ | 役割 |
|---|---|
| `packages/web/` | React 19 + Vite フロントエンド |
| `packages/shared/` | 共有ライブラリ（OpenAPI 生成型, JQuants, TA/FA指標） |
| `packages/cli/` | Gunshi CLI（db/dataset/jquants/backtest/analysis の運用・自動化） |
| `packages/api-clients/` | FastAPI クライアント（backtest/JQuants） |

```bash
bun run workspace:dev            # web 起動（FastAPI :3002 にプロキシ）
bun run workspace:dev:sync       # bt:sync + web:dev（sync失敗時はwarningで継続）
bun run workspace:test           # テスト
bun run quality:typecheck        # 型チェック
bun run quality:lint && bun run quality:check:fix  # リント（Biome）
bun run --filter @trading25/web e2e:smoke  # web E2E smoke（Playwright）
bun run cli:run backtest attribution run <strategy> --wait
```
`main` ブランチでは `workspace:dev` を既定とし、`workspace:dev:sync` は OpenAPI 契約更新の確認が必要な場合のみ使う。

- Backtest UI は `Attribution` サブタブ内に `Run` / `History` を持ち、進捗取得は 2 秒ポーリング
- Backtest `Strategies` 画面の YAML Editor は `production` / `experimental` の編集を許可し、`Rename` / `Delete` は `experimental` のみ許可
- Backtest `Strategies > Optimize` は `Open Editor` ポップアップで Monaco + Signal Reference を表示し、`Current` / `Saved` / `State` 要約を維持する。保存ブロックは YAML 構文エラー時のみとする
- Backtest Runner の `Optimization` セクションは Grid 概要（params/combinations）に加えて `parameter_ranges` の具体値一覧を表示し、Optimization 完了カードでは Best/Worst Params と各 score を表示する
- `analysis screening`（web/cli）は production 戦略を動的選択し、非同期ジョブ（2秒ポーリング）で実行する。`sortBy` 既定は `matchedDate`、`order` 既定は `desc`。`backtestMetric` は廃止
- Analysis 画面は `Screening / Daily Ranking / Fundamental Ranking` の3タブ構成。Fundamental Ranking は `Forecast High / Forecast Low / Actual High / Actual Low` の4サブタブで最新EPSランキングを表示する
- Charts の sidebar 設定はカテゴリ別 Dialog（Chart Settings / Panel Layout / Fundamental Metrics / FY History Metrics / Overlay / Sub-Chart / Signal Overlay）で編集する。Fundamental 系パネル（Fundamentals / FY History / Margin Pressure / Factor Regression）は `fundamentalsPanelOrder` で表示順を保持・編集し、Fundamentals パネル内部の指標は `fundamentalsMetricOrder` / `fundamentalsMetricVisibility`、FY History パネル内部の指標は `fundamentalsHistoryMetricOrder` / `fundamentalsHistoryMetricVisibility` で順序・表示ON/OFFを保持する。Fundamentals パネル高さは表示中指標数に応じて動的に変化する
- Portfolio / Watchlist の銘柄追加入力はチャート検索と同等の銘柄サーチ（コード/銘柄名）を使う。追加送信 payload は `companyName` 必須（候補選択時は候補名、未選択時はコードをフォールバック）。Watchlist 追加の送信は 4 桁コードのみ許可する
- Fundamentals summary の予想EPS表示は `revisedForecastEps > adjustedForecastEps > forecastEps` の優先順位を SoT とする

主要技術: TypeScript, Bun, React 19, Vite, Tailwind CSS v4, Biome, OpenAPI generated types

## Issue管理

プロジェクトルートの `issues/`（オープン）、`issues/done/`（クローズ済み）で管理。
フォーマット: `{id}-{slug}.md`（例: `bt-016-test-coverage-70.md`）

## Skills ガバナンス

- プロジェクト正本のスキルは `/.codex/skills` に配置する
- `apps/ts/.claude/skills` と `apps/bt/.claude/skills` は legacy 参照用（read-only）
- 参照生成: `scripts/skills/refresh_skill_references.py`
- 監査: `scripts/skills/audit_skills.py --strict-legacy`

## CI

`.github/workflows/ci.yml` により全ブランチ push / PR で自動実行。
- **skills**: audit（stale検知 / frontmatter検証 / legacy変更検知）
- **ts**: lint → 型生成 → build → typecheck → test + coverage
- **web e2e**: Playwright Chromium smoke（bt server :3002 を起動して実行）
- **bt**: lint → typecheck → test + coverage（ゲート70%）

## ロードマップ

現行インデックスは [`docs/unified-roadmap.md`](docs/unified-roadmap.md) を参照。  
Phase 1-4 の大規模リファクタリングは完了し、実行タスクは `issues/`（open）/`issues/done/`（closed）で管理する。  
旧統合ロードマップ本文は `docs/archive/unified-roadmap-2026-02-10.md` に archive 済み。
