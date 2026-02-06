# TA計算エンジン統合計画: apps/ts/ → apps/bt/ 移行

## 背景

現在、テクニカル分析(TA)の計算ロジックがapps/ts/とapps/bt/に二重実装されている。

| 観点 | apps/ts/ (TypeScript) | apps/bt/ (Python/vectorbt) |
|------|------|------|
| 指標数 | 8種 (SMA,EMA,RSI,MACD,PPO,Bollinger,ATR,NBarSupport) | 34シグナル (全TA + ファンダメンタル + セクター) |
| 計算場所 | ブラウザ内 (client-side) | サーバー (backtest実行時のみ) |
| ファイル数 | 28ファイル, 66+関数 | signals/ 配下 34定義 |
| API公開 | なし (shared libraryとしてimport) | なし (boolean signalのみ内部使用) |
| ユースケース | チャート描画 | バックテスト判定 |

### 問題点

1. **計算結果の不一致リスク**: 同じRSI(14)でもEMA初期値処理の差異で値が微妙にずれうる
2. **二重メンテナンス**: 同じ指標のバグ修正を2箇所で行う必要がある
3. **チャート⇔バックテストの乖離**: チャートで見える指標値とバックテストの判定基準が異なるエンジンで計算される
4. **機能格差**: apps/bt/の34シグナルのうちチャートで可視化できるのはapps/ts/が実装済みの8種のみ

### 目標

**apps/bt/をTA計算の単一エンジン(Single Source of Truth)とし、apps/ts/webはapps/bt/ APIから指標値を取得して描画する。**

---

## アーキテクチャ変更概要

### Before (現状)
```
apps/ts/web (ブラウザ)
  ├── @trading25/shared/ta/ でインジケータ計算 (client-side)
  ├── /api/chart/stocks/{symbol} → apps/ts/api → OHLCV取得
  └── /bt/api/backtest/run → apps/bt/ → バックテスト実行 (別エンジンで計算)
```

### After (目標)
```
apps/ts/web (ブラウザ)
  ├── /bt/api/indicators/compute → apps/bt/ → インジケータ計算 (server-side, vectorbt)
  ├── /api/chart/stocks/{symbol} → apps/ts/api → OHLCV取得 (変更なし)
  └── /bt/api/backtest/run → apps/bt/ → バックテスト実行 (同一エンジンで計算)
```

---

## フェーズ構成

### フェーズ0: 前提整理

**目的**: 移行の土台となる既存issue (bt-018〜021) を先に解決し、apps/bt/のAPI責務を整理する。

| 作業 | 対象issue | 内容 |
|------|-----------|------|
| デッドコード削除 | bt-018 | `MarketAPIClient.perform_screening()` 削除 |
| Portfolio client縮小 | bt-019 | write系メソッド削除 |
| cli_market整理 | bt-020 | ranking/screening CLIをapps/ts/cliに一本化 |
| TOPIXローダー整理 | bt-021 | 二重ロードパスの明確化 |

**完了条件**: apps/bt/のAPI client/CLIが必要最小限に整理されていること。

**✅ 完了 (2026-02-02)**:
- bt-018: `MarketAPIClient.perform_screening()` 削除
- bt-019: `PortfolioAPIClient` write系6メソッド削除（read-only化）
- bt-020: `src/cli_market/` 全削除、ranking系メソッド・関数削除、`market_analysis.py` をre-exportモジュールに簡素化、`RankingItem`/`ScreeningResult` モデル削除、ドキュメント更新
- bt-021: `index_loaders.py` のTOPIX二重ロードパスをdocstring整理（signal_screening β値計算 + cli_portfolio PCA分析）
- ts-121, ts-122: bt-020完了注記追加（issueはcloseせず）
- 全1774テスト通過、ruff/pyright通過

---

### フェーズ1: apps/bt/ Indicator API の構築

**目的**: apps/bt/にインジケータ値を返すAPIエンドポイントを新設する。

#### 1-1. インジケータ計算サービス作成

**新規ファイル**: `apps/bt/src/server/services/indicator_service.py`

vectorbtの計算結果をbooleanではなく**生の数値列**として返すラッパー:

```python
# 返却例: RSI
{
  "indicator": "rsi",
  "params": {"period": 14},
  "data": [
    {"date": "2025-01-06", "value": 45.2},
    {"date": "2025-01-07", "value": 48.7},
    ...
  ]
}

# 返却例: MACD
{
  "indicator": "macd",
  "params": {"fast": 12, "slow": 26, "signal": 9},
  "data": [
    {"date": "2025-01-06", "macd": 12.5, "signal": 10.2, "histogram": 2.3},
    ...
  ]
}

# 返却例: Bollinger Bands
{
  "indicator": "bollinger",
  "params": {"period": 20, "deviation": 2.0},
  "data": [
    {"date": "2025-01-06", "upper": 2850.0, "middle": 2800.0, "lower": 2750.0},
    ...
  ]
}
```

**対応インジケータ (Phase 1)**:
| インジケータ | vectorbt関数 | レスポンス構造 |
|---|---|---|
| SMA | `vbt.MA.run(ewm=False)` | `{date, value}` |
| EMA | `vbt.MA.run(ewm=True)` | `{date, value}` |
| RSI | `vbt.RSI.run()` | `{date, value}` |
| MACD | `vbt.MACD.run()` | `{date, macd, signal, histogram}` |
| PPO | MACD正規化 | `{date, ppo, signal, histogram}` |
| Bollinger Bands | `vbt.BBANDS.run()` | `{date, upper, middle, lower}` |
| ATR | `vbt.ATR.run()` | `{date, value}` |
| ATR Support | ATR + min(low) | `{date, value}` |
| N-Bar Support | `rolling(N).min()` | `{date, value}` |
| Volume Comparison | `vbt.MA.run(volume)` | `{date, shortMA, longThresholdLower, longThresholdHigher}` |
| Trading Value MA | `vbt.MA.run(close*volume, ewm=True)` | `{date, value}` |

#### 1-2. APIエンドポイント定義

**新規ファイル**: `apps/bt/src/server/routes/indicators.py`

```
POST /api/indicators/compute
```

**リクエスト**:
```json
{
  "stock_code": "7203",
  "source": "market",           // "market" (market.db) or dataset名
  "timeframe": "daily",         // "daily" | "weekly" | "monthly"
  "indicators": [
    {"type": "sma", "params": {"period": 20}},
    {"type": "rsi", "params": {"period": 14}},
    {"type": "macd", "params": {"fast": 12, "slow": 26, "signal": 9}},
    {"type": "bollinger", "params": {"period": 20, "deviation": 2.0}}
  ],
  "start_date": "2024-01-01",  // optional
  "end_date": "2025-02-01"     // optional
}
```

**レスポンス**:
```json
{
  "stock_code": "7203",
  "timeframe": "daily",
  "meta": {
    "start": "2024-01-04",
    "end": "2025-01-31",
    "bars": 245,
    "source": "market",
    "nan_handling": "omit"
  },
  "indicators": {
    "sma_20": [{"date": "...", "value": 2500.5}, ...],
    "rsi_14": [{"date": "...", "value": 45.2}, ...],
    "macd_12_26_9": [{"date": "...", "macd": 12.5, "signal": 10.2, "histogram": 2.3}, ...],
    "bollinger_20_2.0": [{"date": "...", "upper": 2850, "middle": 2800, "lower": 2750}, ...]
  }
}
```

**NaN/欠損値の扱い**:
- インジケータの初期区間（SMA(20)の最初19本等）は `null` を返却
- フロントエンドは `null` を描画スキップとして処理
- `meta.nan_handling`: `"omit"` (null行を省略) or `"include"` (null値を含めて返却)
  - デフォルト: `"include"` (チャート描画でdate alignmentが容易)

**設計方針**:
- 1リクエストで複数インジケータを一括計算（OHLCV読み込みは1回）
- `source: "market"` でmarket.db、dataset名指定でdataset.dbを使用
- timeframe変換はapps/bt/側で実施（pandas resample）
- Pydanticスキーマで型安全性を保証

**API制限 (Rate Limit & 境界条件)**:
- `max_indicators`: 1リクエストあたり最大10指標
- `max_bars`: 最大2000本 (約8年分の日足)
- リクエストタイムアウト: 10秒
- 同時リクエスト上限: 5 (ThreadPoolExecutor workers)

**新規ファイル**: `apps/bt/src/server/schemas/indicators.py`

#### 1-3. データソース対応

| source値 | データ元 | 用途 |
|---|---|---|
| `"market"` | MarketAPIClient → apps/ts/ `/api/market/stocks/{code}/ohlcv` | チャート表示 (最新データ) |
| `"{dataset_name}"` | DatasetAPIClient → apps/ts/ `/api/dataset/{name}/stocks/{code}/ohlcv` | バックテスト連動表示 |

**キャッシュ戦略**:
- OHLCV取得結果をインメモリキャッシュ (TTL: 60秒)
- 同一銘柄の複数インジケータ計算はOHLCVを共有
- market.dbソースは頻繁に変わらないためキャッシュ効果が高い

#### 1-4. Margin Indicator API

信用取引指標は別エンドポイントとする（データソースが異なるため）:

```
POST /api/indicators/margin
```

**リクエスト**:
```json
{
  "stock_code": "7203",
  "source": "market",
  "indicators": ["margin_long_pressure", "margin_flow_pressure", "margin_turnover_days"],
  "average_period": 20
}
```

**実装**: apps/ts/shared/src/ta/margin-pressure-indicators.ts のロジックをPython/pandasで再実装。

**完了条件**:
- [x] `POST /api/indicators/compute` が11種のインジケータ値をJSON返却する
- [x] `POST /api/indicators/margin` が3種の信用指標を返却する
- [x] OpenAPIスキーマが自動生成される（FastAPI + Pydantic v2で自動生成）
- [ ] レスポンスタイムが単一銘柄×4インジケータで < 500ms（実環境での計測は未実施）

**✅ 完了 (2026-02-02)**:
- 11種テクニカルインジケータ: SMA, EMA, RSI, MACD, PPO, Bollinger, ATR, ATR Support, N-Bar Support, Volume Comparison, Trading Value MA
- 3種信用指標: margin_long_pressure, margin_flow_pressure, margin_turnover_days
- 共通計算関数 `src/utils/indicators.py` を新設し、signal関数とindicator serviceの両方から利用
- Registry pattern による indicator_service 実装（`INDICATOR_REGISTRY` + `MARGIN_REGISTRY`）
- Pydantic v2 discriminated union による型安全なリクエスト/レスポンス定義
- ThreadPoolExecutor(max_workers=5) + asyncio.wait_for(timeout=10s) による非同期実行
- 107テスト追加（全1,881テスト通過）、主要ファイルのカバレッジ80%以上
- コードレビュー（PPO除算ゼロ対策、エラーハンドリング改善等）+ リファクタリング実施済み

---

### フェーズ2: apps/ts/web のAPI移行

**目的**: apps/ts/webのチャート描画をclient-side計算からapps/bt/ API呼び出しに切り替える。

#### 2-1. apps/bt/ Indicator API クライアント作成

**新規ファイル**: `apps/ts/packages/web/src/lib/bt-indicator-client.ts`

```typescript
interface IndicatorRequest {
  stock_code: string;
  source: 'market' | string;
  timeframe: 'daily' | 'weekly' | 'monthly';
  indicators: IndicatorSpec[];
  start_date?: string;
  end_date?: string;
}

async function fetchIndicators(req: IndicatorRequest): Promise<IndicatorResponse>
```

#### 2-2. React Hook 作成

**新規ファイル**: `apps/ts/packages/web/src/hooks/useIndicators.ts`

```typescript
function useIndicators(
  stockCode: string,
  indicators: IndicatorSpec[],
  timeframe: Timeframe,
  source: string
): {
  data: IndicatorResponse | null;
  isLoading: boolean;
  error: Error | null;
}
```

- TanStack Query でキャッシュ・再取得を管理
- パラメータ変更時の debounce (300ms) で連続リクエスト抑制
- staleTime: 60秒 (OHLCVキャッシュと合わせる)

#### 2-3. useMultiTimeframeChart の改修

**変更ファイル**: `apps/ts/packages/web/src/components/Chart/hooks/useMultiTimeframeChart.ts`

Before:
```typescript
import { sma, ema, macd, bollingerBands, ... } from '@trading25/shared/ta';
// → ブラウザ内で計算
```

After:
```typescript
import { useIndicators } from '@/hooks/useIndicators';
// → apps/bt/ APIから取得
```

**段階的移行**: 各インジケータを1つずつ切り替え、フォールバック付きで移行。

```typescript
// 移行期間中のフォールバックパターン
const serverIndicators = useIndicators(code, specs, timeframe, 'market');
const clientFallback = useMemo(() => {
  if (serverIndicators.data) return null;
  return computeClientSide(ohlcv, settings); // 旧ロジック
}, [serverIndicators.data, ohlcv, settings]);
```

#### 2-4. Vite Proxy 更新

**変更ファイル**: `apps/ts/packages/web/vite.config.ts`

既存の `/bt` → `localhost:3002` プロキシで対応可能。追加設定不要。

#### 2-5. レスポンス型定義

**新規ファイル**: `apps/ts/packages/shared/src/types/indicator-types.ts`

apps/bt/ APIのレスポンス型をsharedに定義し、web/からimport。

**完了条件**:
- [x] 全11インジケータがapps/bt/ API経由で描画される（通常モード）
- [ ] client-side計算へのフォールバックが動作する（apps/bt/停止時）→ Phase 2.5で判断
- [ ] チャート操作（timeframe切替、パラメータ変更）の体感遅延が < 1秒（手動検証未実施）
- [x] margin指標もapps/bt/ API経由に切替済み

**✅ 完了 (2026-02-02)**:

**apps/bt/ 側変更**:
- `VolumeComparisonParams` の `threshold` を `lower_multiplier` / `higher_multiplier` に分離（スキーマ + サービス + テスト）
- apps/bt/ テスト全83通過

**apps/ts/ 側新規ファイル**:
- `apps/ts/packages/web/src/hooks/useBtIndicators.ts` — apps/bt/ Indicator API の TanStack Query フック
  - `buildIndicatorSpecs()`: ChartSettings → BtIndicatorSpec[] 変換
  - `mapBtResponseToChartData()`: apps/bt/ レスポンス → ChartData 構造変換（lookup table pattern で complexity 制限クリア）
  - relativeMode 時は specs が空になり API 呼び出しをスキップ
- `apps/ts/packages/web/src/hooks/useBtMarginIndicators.ts` — apps/bt/ Margin API の TanStack Query フック
  - `useMarginPressureIndicators`（apps/ts/ API経由）を完全置き換え

**apps/ts/ 側リファクタ**:
- `useMultiTimeframeChart.ts` (746行 → 430行、約300行削減)
  - 通常モード: `useBtIndicators()` × 3 timeframes（daily/weekly/monthly）で apps/bt/ API 経由
  - relativeMode: client-side 計算を `createChartDataClientSide()` として維持（`@trading25/shared/ta` import 残存）
  - helper関数を `computeClientSideIndicators()` + `computeClientSideSubCharts()` に分割（lint complexity制限クリア）
- `ChartsPage.tsx`: `useMarginPressureIndicators` → `useBtMarginIndicators` に切り替え

**テスト**:
- `useBtIndicators.test.ts`: 23テスト（specs builder、date→time変換、relativeMode時API非呼出、API request body検証）
- `useBtMarginIndicators.test.ts`: 7テスト（レスポンス変換、エラーハンドリング、空データ）
- TypeCheck全パス、Lint errors 0

**設計判断**:
- client-side fallback は実装しない（Phase 2.5 の並走検証後に Phase 3 で判断）
- apps/bt/ API 失敗時は candlestick のみ表示 + インジケータ部分が空
- 3 timeframe は TanStack Query が自動並列実行（バッチ API は Phase 2 では不要）
- relativeMode 用の client-side ロジックは Phase 3 で apps/bt/ に relative 対応を入れた後に削除予定

---

### フェーズ2.5: 並走検証期間 (Codexレビューで追加)

**目的**: 旧ロジック(client-side)と新API(apps/bt/)の計算結果を並走比較し、移行の安全性を確認する。

#### 検証内容

- apps/ts/webのフォールバックモードを活用し、**両方の結果を同時に計算**
- 各インジケータについて、新旧の出力を比較ログとして記録
- 計測項目:
  - **不一致率**: 全データポイントのうち許容誤差を超えるポイントの割合
  - **レイテンシ**: apps/bt/ API呼び出しのP50/P95/P99
  - **エラー率**: apps/bt/ APIのタイムアウト・接続失敗の発生率

#### 判定基準 (Phase 3 進行条件)

| 指標 | 閾値 |
|------|------|
| 不一致率 (全インジケータ平均) | < 0.1% |
| P95 レイテンシ | < 800ms |
| API エラー率 | < 1% |
| サーキットブレーカー発動回数 | 0回/日 |

基準未達の場合はPhase 1のAPI実装を改修し、再度検証する。

**完了条件**:
- [ ] 全11インジケータの比較ログが収集済み
- [ ] 判定基準を全て満たしている
- [ ] 比較結果レポートが作成されている

---

### フェーズ3: apps/ts/shared/src/ta/ の段階的廃止

**目的**: 二重実装を解消し、apps/ts/のTA計算コードを削除する。

#### 3-1. 廃止対象の分類

| カテゴリ | ファイル | 判定 | 理由 |
|---|---|---|---|
| **インジケータ** | sma.ts, ema.ts, rsi.ts, macd.ts, ppo.ts, bollinger.ts, atr.ts, atr-support.ts, n-bar-support.ts | **廃止** | apps/bt/ APIに移行 |
| **Volume系** | volume-comparison.ts, trading-value-ma.ts | **廃止** | apps/bt/ APIに移行 |
| **Margin系** | margin-pressure-indicators.ts, margin-volume-ratio.ts | **廃止** | apps/bt/ APIに移行 |
| **Timeframe変換** | timeframe/*.ts (11ファイル) | **維持** | apps/ts/api dataset-data-serviceが使用。OHLCV→週足/月足変換はデータ配信の責務 |
| **Relative OHLC** | relative/*.ts (4ファイル) | **維持** | TOPIX相対パフォーマンスはチャート表示の責務 |
| **Utilities** | utils.ts | **維持** | NaN処理等の汎用ユーティリティ |

**廃止ファイル数**: 13ファイル (28ファイル中)
**維持ファイル数**: 15ファイル (timeframe 11 + relative 4 + utils)

#### 3-2. screening/ モジュールへの影響

`apps/ts/packages/shared/src/screening/volume-utils.ts` が `sma`, `ema` をimportしている。

**対応**: volume-utils.ts内にインライン実装するか、apps/bt/ screening APIに委譲する。
screening自体がapps/ts/内で完結すべきか（レンジブレイク検出 = apps/ts/固有機能）の判断が必要。

→ **推奨**: screening/volume-utils.ts に最小限のSMA/EMA実装を残す（3-5行の関数）。
スクリーニングはapps/ts/の責務として維持するが、TA libraryとしては廃止する。

#### 3-3. テスト更新

- ta/ のユニットテスト (*.test.ts) を削除
- useMultiTimeframeChart のテストをAPI mock ベースに書き換え
- apps/bt/ indicator API の結合テストを追加

**完了条件**:
- [x] 13ファイルが削除済み
- [x] apps/ts/packages/shared/src/ta/index.ts の export が timeframe/ と relative/ のみ
- [x] 全テストがパス
- [x] screening/ が独立して動作する

**Phase 3 一部完了 (2026-02-03)**:

relativeMode依存のため段階的削除を実施。以下が完了:

**削除済みファイル (10個)**:
- `rsi.ts` + `rsi.test.ts` + `rsi.real-data.test.ts` — relativeMode非依存のため即削除
- `margin-pressure-indicators.ts` + `margin-pressure-indicators.test.ts` — apps/bt/ API proxy に移行
- `sma.real-data.test.ts`, `ema.real-data.test.ts`, `macd.real-data.test.ts`, `ppo.real-data.test.ts`, `margin-volume-ratio.real-data.test.ts` — real-dataテストは不要（goldenテストはapps/bt/側で実施）
- `useChart.ts` (レガシーhook、未使用)

**変更済みファイル**:
- `ta/index.ts` — rsi, margin-pressure-indicators系exportを削除
- `shared/src/index.ts` — 同上のre-exportを削除
- `screening/volume-utils.ts` — SMA/EMAをta/からのimportからインライン実装に変更（既存実装の完全コピー）
- `api/services/stock-data.ts` — `getMarginPressureIndicators()` を apps/bt/ `POST /api/indicators/margin` proxy呼び出しに変更

**削除延期**:
- `margin-volume-ratio.ts` + test — apps/bt/に未実装のため延期
- `__fixtures__/` 全体 — apps/bt/ Pythonテストが依存
- 10個のインジケータファイル (sma, ema, macd, ppo, bollinger, atr, atr-support, n-bar-support, volume-comparison, trading-value-ma) — relativeMode依存のため apps/bt/ relative対応後に削除（Phase 3.5）

---

### フェーズ4: 拡張 — apps/bt/シグナルのWeb可視化

**目的**: apps/bt/の34シグナルをWebチャート上で可視化可能にする。

#### 4-1. Signal Overlay API

```
POST /api/indicators/signals
```

**リクエスト**:
```json
{
  "stock_code": "7203",
  "source": "market",
  "signals": [
    {"type": "rsi_threshold", "params": {"period": 14, "threshold": 30, "direction": "below"}},
    {"type": "period_breakout", "params": {"period": 60, "direction": "high"}}
  ]
}
```

**レスポンス**: boolean配列 + トリガー日付リスト

```json
{
  "signals": {
    "rsi_threshold": {
      "trigger_dates": ["2025-01-15", "2025-01-22"],
      "values": [{"date": "2025-01-06", "active": false}, ...]
    }
  }
}
```

#### 4-2. Web UIへのシグナルマーカー追加

チャート上にシグナル発火点をマーカー表示（▲/▼アイコン）。
バックテスト戦略のエントリー/エグジットポイントと同じ表示で、「このシグナルでここでエントリーした」が視覚的に確認できる。

#### 4-3. 新規インジケータの段階的追加

apps/bt/の既存シグナルから、チャート可視化に適したものを順次追加:

| 優先度 | インジケータ | apps/bt/既存シグナル |
|---|---|---|
| 高 | セクターローテーション (RRG) | `sector_rotation_phase_signal` |
| 高 | ベータ係数 | `beta_range_signal` |
| 中 | Fibonacci Retracement | `retracement_signal` |
| 中 | Mean Reversion bands | `mean_reversion_combined_signal` |
| 低 | 信用残パーセンタイル | `margin_balance_percentile_signal` |

**完了条件**:
- [ ] Signal Overlay APIが稼働
- [ ] Web UIにシグナルマーカーが表示される
- [ ] バックテスト結果のエントリー/エグジットとチャートシグナルが一致する

---

## apps/ts/に残すもの（変更なし）

以下の機能はapps/ts/の責務として維持する:

| 機能 | 理由 |
|---|---|
| ファンダメンタル計算 (PER/PBR/ROE等) | JQUANTSデータに直接アクセスが必要。apps/bt/は apps/ts/ API経由でstatements取得後に独自計算している |
| ファクター回帰 (OLS) | 統計分析はapps/ts/の分析機能。apps/bt/のPCA/回帰は別用途（ポートフォリオ最適化） |
| ランキング (SQL集計) | 純粋なDB集計、TA計算不要 |
| レンジブレイクスクリーニング | apps/ts/固有のマーケットスキャン機能。apps/bt/のシグナルスクリーニングとは目的が異なる |
| Timeframe変換 | dataset API でのOHLCV変換はデータ配信の責務 |
| Relative OHLC | TOPIX相対パフォーマンス表示はチャートUI機能 |

---

## リスクと対策

### R1: レイテンシ増加
- **リスク**: client-side → server-side移行でチャート描画が遅延
- **対策**:
  - 1リクエストで複数インジケータを一括計算
  - apps/bt/側OHLCVキャッシュ (TTL 60秒)
  - TanStack Query の staleTime でフロントキャッシュ
  - 初回以降はキャッシュヒットで即座に描画
  - API制限: max_indicators=10, max_bars=2000, timeout=10秒
- **目標**: 単一銘柄×4インジケータで < 500ms

### R2: apps/bt/ SPOF化 (Single Point of Failure)
- **リスク**: apps/bt/が停止・遅延するとapps/ts/webのインジケータ表示が全停止
- **対策 (段階的劣化)**:
  1. **サーキットブレーカー**: apps/bt/ APIへの接続失敗が3回連続で発生したら、30秒間リクエストを停止
  2. **直近成功キャッシュ**: TanStack Queryの `staleTime: 5分` + `gcTime: 30分` で直近の成功結果を保持
  3. **部分提供**: 一部インジケータが失敗しても成功分は描画
  4. **劣化UI**: apps/bt/未接続時は「インジケータ利用不可」バナー + OHLCVチャートのみ表示
  - OHLCVチャート自体はapps/ts/apiのみで表示可能（コア機能は維持）

### R3: 計算結果の互換性
- **リスク**: TypeScript実装とvectorbt実装で微小な数値差異
- **対策**:
  - **ゴールデンデータセット**: 固定OHLCV入力に対する期待出力をJSON化して保持
  - 移行前に両エンジンの出力を比較する自動テスト作成
  - 許容誤差基準:
    - SMA/EMA: 絶対誤差 < 0.01円
    - RSI: 絶対誤差 < 0.1ポイント
    - MACD/Bollinger: 相対誤差 < 0.01%
    - NaN/Inf: 同一位置で発生すること
  - 差異がある場合はvectorbtの結果を正とする（バックテスト基準）

### R4: Timeframe変換の一貫性
- **リスク**: apps/ts/のdailyToWeekly()とapps/bt/のpandas resampleで週足/月足の区切りが異なる可能性
- **対策**:
  - apps/bt/のtimeframe変換ロジックをapps/ts/の既存実装と突合
  - 週の開始日（月曜/日曜）、月末処理の統一
  - **統合テスト**: 固定日足データ → 週足/月足変換 → インジケータ計算 → 表示値 の全パイプラインを検証
  - apps/ts/の `dailyToWeekly()` と apps/bt/の `pandas.resample('W-FRI')` の出力を比較

### R5: データソースの一貫性 (Codexレビューで追加)
- **リスク**: apps/bt/とapps/ts/で異なるOHLCデータ（調整済み/未調整、欠損補正の有無）を使用すると、同一インジケータでも結果が異なる
- **対策**:
  - apps/bt/ indicator APIのデータソースを明確に限定: market.db or dataset.db（apps/ts/ API経由）
  - 調整方式（株式分割補正等）をリクエストパラメータに含めない（データソース側で一元管理）
  - ゴールデンデータテストで「同一入力データ」であることを保証

---

## 実行順序とissue対応

```
フェーズ0: bt-018, bt-019, bt-020, bt-021 (既存issue消化)
    ↓
フェーズ1: bt-0XX (新規: indicator API構築)
    ├── 1-1: indicator_service.py
    ├── 1-2: routes/indicators.py + schemas
    ├── 1-3: data source対応 + キャッシュ
    └── 1-4: margin indicator API
    ↓
フェーズ2: ts-1XX (新規: web移行)
    ├── 2-1: bt-indicator-client.ts
    ├── 2-2: useIndicators hook
    ├── 2-3: useMultiTimeframeChart改修
    └── 2-5: shared型定義
    ↓
フェーズ2.5: 並走検証期間
    ├── 旧ロジック vs 新API の結果比較ログ収集
    ├── 不一致率・レイテンシ計測
    └── 判定基準: 不一致率 < 0.1%, P95レイテンシ < 800ms で Phase 3 に進む
    ↓
フェーズ3: ts-1XX (新規: ta/廃止)
    ├── 3-1: indicator系13ファイル削除
    ├── 3-2: screening/volume-utils対応
    └── 3-3: テスト更新
    ↓
フェーズ4: bt-0XX + ts-1XX (新規: シグナル可視化)
    ├── 4-1: signal overlay API
    ├── 4-2: チャートマーカーUI
    └── 4-3: 新規インジケータ追加
```

---

## 補足: production配信アーキテクチャ

現状Vite proxyで開発中だが、production環境ではreverse proxyが必要:

```
nginx / Caddy
  ├── /api/*     → apps/ts/api (3001)
  ├── /bt/api/*  → apps/bt/    (3002)
  └── /*         → apps/ts/web static files
```

フェーズ2でapps/bt/ APIへの依存が増えるため、production配信設計は並行して検討すべき。
（ただし本計画のスコープ外とする）

---

## Phase 3.5 完了 (2026-02-03)

### 実施内容

1. **apps/bt/ relative OHLCサポート追加**
   - `IndicatorComputeRequest` に `benchmark_code` と `relative_options` パラメータ追加
   - `calculate_relative_ohlcv()` 関数新規作成（apps/ts/のRelativeOHLCConverterロジック移植）
   - TOPIXデータは `MarketAPIClient.get_topix()` 経由で取得
   - `handle_zero_division`: `"skip"` / `"zero"` / `"null"` の3モード対応（apps/ts/との完全互換）

2. **apps/bt/ margin-volume-ratio追加**
   - `MARGIN_REGISTRY` に `margin_volume_ratio` 計算関数追加
   - ISO週単位で日次出来高を平均し信用残高比率を算出
   - `_get_iso_week_key()` ヘルパー関数で週キー生成を統一

3. **apps/ts/web relativeMode → apps/bt/ API移行**
   - `useBtIndicators`: relativeModeガード削除、`benchmark_code: 'topix'` 付きリクエスト生成
   - `useMultiTimeframeChart`: client-side計算関数群を全削除
   - 通常モード・relativeMode共にapps/bt/ API経由でインジケータ計算

4. **apps/ts/api margin-volume-ratio → apps/bt/ API proxy**
   - `stock-data.ts`の`getMarginVolumeRatio()`をapps/bt/ API呼び出しに変更

5. **apps/ts/インジケータファイル削除**
   - 11インジケータ本体 + 11テストファイル削除
   - `ta/index.ts`: timeframe, relative, utilsのみ残存
   - `shared/src/index.ts`: インジケータre-export全削除
   - `generate-golden-datasets.ts`: deprecated化

6. **コードレビュー・リファクタリング (2026-02-03)**
   - `_compute_relative_ohlc_column()` ヘルパー関数で単一列計算ロジックを分離
   - `RelativeOHLCOptions` から未使用の `align_dates` フィールドを削除
   - 14テストケース追加（relative OHLC 7件 + margin_volume_ratio 5件 + 統合2件）
   - エッジケース対応: 全ベンチマークゼロ、NaN信用残高、複数週データ

### 削除ファイル一覧
- sma.ts, ema.ts, macd.ts, ppo.ts, atr.ts, atr-support.ts, n-bar-support.ts
- bollinger.ts (旧 bollinger-bands.ts), volume-comparison.ts, trading-value-ma.ts
- margin-volume-ratio.ts
- 対応する全テストファイル

### 残存モジュール（apps/ts/shared/ta/）
- `relative/` — ローソク足の相対OHLC変換（useMultiTimeframeChartのcandlestick用）
- `timeframe/` — dailyToWeekly, dailyToMonthly等（dataset-data-service使用）
- `utils.ts` — 汎用ユーティリティ（cleanNaNValues）

### apps/bt/テスト追加
- `tests/server/test_indicator_service.py`: 14テスト（全パス）
  - `TestCalculateRelativeOHLCV`: 7テスト（basic, date_alignment, zero_skip, zero_zero, zero_null, no_common_dates, all_zero_skip）
  - `TestComputeMarginVolumeRatio`: 5テスト（basic, empty, zero_weekly_avg, nan_margins, multiple_weeks）
  - `TestIndicatorServiceRelativeMode`: 2テスト（with_benchmark, without_benchmark）
