# trading25 Monorepo

FastAPI バックエンド（`apps/bt`）と TypeScript クライアント（`apps/ts`）を統合したモノレポです。

## Current Architecture

```
JQUANTS API ──→ FastAPI (:3002) ──→ SQLite (market.db / portfolio.db / datasets)
                     ↓
                  ts/web (:5173, /api proxy)
                  ts/cli
```

- バックエンドは `apps/bt` の FastAPI に一本化済み
- financial-analysis のロジック SoT は `apps/bt`（ts 側は API consumer / proxy）
- 旧 `apps/ts/packages/api`（Hono 互換 API レイヤー）は削除済み
- Backtest 実行パスは `apps/bt` 内で dataset/market DB を直接参照し、内部HTTP self-call を回避

## Repository Layout

- `apps/bt` - Python 3.12 + FastAPI + vectorbt + typer CLI
- `apps/ts` - Bun workspace（web / cli / shared / api-clients）
- `contracts` - bt/ts 間の安定インターフェース（JSON Schema, OpenAPI baseline）
- `docs` - ロードマップ、設計判断、監査レポート
  - `docs/bt-src-layering-guide.md` - `apps/bt/src` の 5層配置ガイド
- `issues` - ローカル Issue 管理（`issues/` と `issues/done/`）
- `scripts` - ルート統合スクリプト

## Quick Start

### 1) FastAPI 起動（apps/bt）
```bash
cd apps/bt
uv sync
uv run bt server --port 3002
```

### 2) Web 起動（apps/ts）
```bash
cd <repo-root>
cp .env.example .env
cd apps/ts
bun install
bun run workspace:dev
```

`.env` の SoT はリポジトリルート（`<repo-root>/.env`）です。

`bun run workspace:dev:sync` を使うと、起動前に `bt:sync`（OpenAPI 取得と型生成）を実行します。`bt:sync` 失敗時は warning を出して `web:dev` を継続します。
`main` ブランチでは `workspace:dev` を既定にし、`workspace:dev:sync` は契約更新確認が必要な時だけ使う運用を推奨します。

### 3) Signal Attribution（LOO + Shapley top-N）
- Web: Backtest ページの `Attribution` サブタブで `Run` から実行し、`History` で保存済み JSON を閲覧
- CLI:
```bash
cd apps/ts
bun run cli:run backtest attribution run <strategy> --wait
bun run cli:run backtest attribution status <job-id>
bun run cli:run backtest attribution results <job-id>
bun run cli:run backtest attribution cancel <job-id>
```
- 保存先（XDG）: `~/.local/share/trading25/backtest/attribution/<strategy>/`
- 補足: `portfolio/watchlist` 操作は CLI から web UI（Portfolio タブ）へ移行済み

### 4) Lab（fundamental 制約付き生成/進化/最適化/改善）
```bash
cd apps/bt
uv run bt lab generate --count 50 --top 5 --entry-filter-only --allowed-category fundamental
uv run bt lab evolve experimental/base_strategy_01 --entry-filter-only --allowed-category fundamental
uv run bt lab optimize experimental/base_strategy_01 --entry-filter-only --allowed-category fundamental
uv run bt lab improve experimental/base_strategy_01 --entry-filter-only --allowed-category fundamental --no-apply
uv run bt lab evolve experimental/base_strategy_01 --structure-mode params_only
uv run bt lab optimize experimental/base_strategy_01 --trials 50 --structure-mode random_add --random-add-entry-signals 1 --random-add-exit-signals 1 --seed 42
```
- API では `/api/lab/generate` `/api/lab/evolve` `/api/lab/optimize` `/api/lab/improve` に
  `entry_filter_only` / `allowed_categories` を指定可能
- `/api/lab/evolve` と `/api/lab/optimize` は `target_scope`（`entry_filter_only` / `exit_trigger_only` / `both`）を指定可能
  - `entry_filter_only` は後方互換フラグとして維持（`target_scope=entry_filter_only` と同義）
  - `allowed_categories` は `all` または `fundamental` 運用を推奨
- `evolve` / `optimize` は `--structure-mode` で探索方式を切り替え可能
  - `params_only`: 既存シグナルのパラメータのみ探索
  - `random_add`: ランダムなシグナル追加 + パラメータ探索（追加数は `--random-add-entry-signals` / `--random-add-exit-signals`）
- API では `/api/lab/evolve` と `/api/lab/optimize` に `structure_mode` / `random_add_*` / `seed` も指定可能
- Web の Backtest > Lab ページでも `evolve` / `optimize` に同じ `structure_mode` 設定を反映済み

### 5) Analysis（Screening / Daily Ranking / Fundamental Ranking）
Analysis は `Screening / Daily Ranking / Fundamental Ranking` の3タブ構成です。

- Web: Analysis > Screening で production 戦略を動的選択（未選択=全production）
- CLI:
```bash
cd apps/ts
bun run cli:run analysis screening --strategies production/forward_eps_driven --sort-by matchedDate
```
- Screening API:
```bash
POST /api/analytics/screening/jobs
GET /api/analytics/screening/jobs/{job_id}
POST /api/analytics/screening/jobs/{job_id}/cancel
GET /api/analytics/screening/result/{job_id}
```
- リクエストは `markets`, `strategies`, `recentDays`, `date`, `sortBy`, `order`, `limit`
- 既定ソートは `matchedDate desc`
- CLI は完了待機が既定（`--no-wait` で job_id を返して終了）
- 旧 `rangeBreakFast/Slow`, `minBreakPercentage`, `minVolumeRatio` は廃止（後方互換なし）
- 旧 `GET /api/analytics/screening` は 410（移行メッセージ返却）
- Fundamental Ranking API:
```bash
GET /api/analytics/fundamental-ranking?markets=prime&limit=20
```
- `fundamental-ranking` は `metricKey` と `rankings.ratioHigh` / `rankings.ratioLow` を返す。現在の `metricKey` は `eps_forecast_to_actual`（最新の予想EPS / 最新の実績EPS）で、予想EPSは `revised > adjusted FY forecast > raw FY forecast` 優先、実績EPSは最新 FY EPS（share補正）を採用する。将来の比率指標追加は `metricKey` で識別する

## Monorepo Commands (root)

```bash
./scripts/lint.sh         # dep-direction + apps/ts lint + apps/bt ruff
./scripts/typecheck.sh    # apps/ts typecheck + apps/bt pyright
./scripts/test-packages.sh # package unit tests (ts packages + bt unit)
./scripts/test-apps.sh    # app integration tests (ts apps + bt api/integration)
./scripts/test.sh         # test-packages + test-apps
```

## OpenAPI Contract Sync

FastAPI スキーマ更新後は `apps/ts` で次を実行:

```bash
bun run --filter @trading25/shared bt:sync
```

## CI

`.github/workflows/ci.yml` で以下を実行:

1. Codex skills audit（`scripts/skills/audit_skills.py --strict-legacy`）
2. Lint
3. Typecheck
4. Package unit tests
5. App integration tests
6. Secret scan（gitleaks）
7. Dependency vulnerability audit（Bun/Python）
8. Web E2E smoke tests（Playwright Chromium + bt server）

## Governance

- Security policy: [`SECURITY.md`](SECURITY.md)
- License: [`LICENSE`](LICENSE)
- Code ownership: [`.github/CODEOWNERS`](.github/CODEOWNERS)
- Security CI triage runbook: [`docs/security/ci-security-triage-runbook.md`](docs/security/ci-security-triage-runbook.md)
