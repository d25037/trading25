# Stop-Limit Daily Classification

## Published Readout

### Decision
- PIT-safe rerun completed. 旧 baseline の market grouping headline は撤回し、`20260608_pit_safe_stock_master_daily` の結果で置き換える。

### Why This Research Was Run
- JPX 標準制限値幅テーブルを daily OHLC に当てた stop-limit event 分類を、signal-date の市場区分で再集計する。
- 旧 readout は latest `stocks` snapshot grouping と書かれていたため、fallback removal の high-value queue で PIT-safe grouping に直して rerun した。

### Data Scope / PIT Assumptions
- Run ID: `20260608_pit_safe_stock_master_daily`
- Analysis range: `2016-05-18 -> 2026-06-05`
- Historical run market schema: `3`（retired; 数値の provenance としてのみ保持）
- Current rerun requirement: Market schema v4 / `local_projection_v2_event_time`
- Universe source: `stock_master_daily`
- As-of policy: event date の `stock_master_daily` で market grouping する。latest `stocks` fallback は使わない。
- Signal-date market mapping missing events: `0`

### Main Findings
#### 結論: primary event は 24,611 件、outside-standard-band は 5,066 件

| Metric | Value |
| --- | ---:|
| Primary classified events | `24,611` |
| Directional events (`stop_high` / `stop_low`) | `24,574` |
| Outside-standard-band rows | `5,066` |
| Signal-date market unmapped events | `0` |

#### 結論: event volume は stop_high intraday_range が中心

| Market | Side | Intraday | Events | Close@Limit |
| --- | --- | --- | ---:| ---:|
| グロース | stop_high | intraday_range | `3,321` | `2,121` |
| スタンダード | stop_high | intraday_range | `3,087` | `1,811` |
| JASDAQ スタンダード | stop_high | intraday_range | `2,937` | `1,651` |
| マザーズ | stop_high | intraday_range | `2,196` | `1,358` |
| 東証一部 | stop_high | intraday_range | `1,633` | `988` |
| 東証二部 | stop_high | intraday_range | `1,611` | `864` |

#### 結論: buy-only 候補は stop_low intraday_range の反発確認 branch に寄る

| Candidate | Direction | Samples | Mean 5d trade return | Profit ratio |
| --- | --- | ---:| ---:| ---:|
| プライム single-price stop_high | long | `322` | `+5.36%` | `56.21%` |
| プライム off-limit stop_low reversal | long | `186` | `+4.66%` | `69.35%` |
| スタンダード off-limit stop_low reversal | long | `315` | `+4.20%` | `60.00%` |
| グロース single-price stop_low | short | `244` | `+3.25%` | `65.57%` |

### Interpretation
- `stop_high` event volume は small/mid market に厚いが、そのまま long-only signal へ移すには弱い branch も多い。
- `stop_low × intraday_range × off_limit_close` の reversal branch は、buy-only follow-up の親として残す価値がある。特にプライム / スタンダードでは 5d trade return がプラス。
- outside-standard-band は 5,066 件あり、daily OHLC だけでは broadened limit / special quote を厳密分類できない。primary event から分離したまま扱う。

### Production Implication
- buy-only の実務候補はこの classification 単体ではなく、`stop-limit-buy-only-next-close-followthrough` の portfolio lens と合わせて判断する。
- stop-limit event parser は reusable source logic として残す。ただし strategy / Ranking evidence に使う場合は、signal-date market grouping と outside-standard-band 分離を必須にする。

### Caveats
- classification は daily OHLC 由来で、板・特別気配・拡大制限値幅の公式状態までは復元しない。
- 旧 baseline の数値は下の既存セクションに残るが、この `Published Readout` より優先しない。

### Source Artifacts
- Experiment: `market-behavior/stop-limit-daily-classification`
- Runner: `apps/bt/scripts/research/run_stop_limit_daily_classification.py`
- Domain logic: `apps/bt/src/domains/analytics/stop_limit_daily_classification.py`
- Bundle: `~/.local/share/trading25/research/market-behavior/stop-limit-daily-classification/20260608_pit_safe_stock_master_daily/`
- Bundle tables: `event_df`, `summary_df`, `continuation_summary_df`, `candidate_trade_summary_df`, `outside_standard_band_df`
- `summary.json` / legacy digest fields are intentionally not used as publication evidence.

JPX の標準制限値幅テーブルを前日終値に当て、日足 OHLC だけで再現できる `stop_high / stop_low` event を市場別・日中値動き別に分解する実験です。

## Purpose

- `stock_data` 日足 DB だけで、ストップ高 / ストップ安を再現可能な形で分類する。
- signal-date `stock_master_daily` を使って `プライム / スタンダード / グロース / unmapped` に分ける。
- `OHLC がすべて同一` な one-price day と、日中に値幅を持った `intraday_range` を分ける。
- `close も limit に張り付いたか` と、その後の continuation / reversal を切り分ける。
- 実務的な branch として、buy-only で残りそうな枝を follow-up 研究へ渡す。

## JPX Daily Price-Limit Definition

- 基準価格:
  - 前営業日の終値
- 標準制限値幅:
  - JPX 公表の標準テーブルを使用
- stop 高 / 安の判定:
  - `prev_close ± standard_limit_width` を upper / lower として exact hit を分類
- one-price / intraday の判定:
  - `open = high = low = close` なら `single_price`
  - それ以外は `intraday_range`
- 標準帯の外へ出た row:
  - `outside_standard_band` として別集計
  - broadened limit や special quote を daily OHLC だけで厳密に復元できないため、primary event に混ぜない

## Scope

- Data source:
  - `stock_data` daily OHLC only
- Market grouping:
  - signal-date `stock_master_daily` only
- Forward windows:
  - `event close -> next open`
  - `event close -> next close`
  - `event close -> close +3 sessions`
  - `event close -> close +5 sessions`

## Source Of Truth

- Runner:
  - `apps/bt/scripts/research/run_stop_limit_daily_classification.py`
  - `apps/bt/scripts/research/run_stop_limit_buy_only_next_close_followthrough.py`
- Domain logic:
  - `apps/bt/src/domains/analytics/stop_limit_daily_classification.py`
  - `apps/bt/src/domains/analytics/stop_limit_buy_only_next_close_followthrough.py`
  - `apps/bt/src/domains/analytics/jpx_daily_price_limits.py`
- Tests:
  - `apps/bt/tests/unit/domains/analytics/test_jpx_daily_price_limits.py`
  - `apps/bt/tests/unit/domains/analytics/test_stop_limit_daily_classification.py`
  - `apps/bt/tests/unit/domains/analytics/test_stop_limit_buy_only_next_close_followthrough.py`

## Latest Baseline

- [baseline-2026-04-20.md](./baseline-2026-04-20.md)

## Current Read

- 標準制限値幅テーブルで exact hit として拾えた primary event は `24,506` 件、`outside_standard_band` は `5,135` 件でした。daily OHLC だけで読む限り、標準帯の外はかなり無視できない量です。
- 件数は `stop_high` が圧倒的に多く、特に `スタンダード / グロース` の `intraday_range stop_high` が厚いです。一方で実務的に long-only で触りやすい枝は `stop_low × intraday_range` に寄ります。
- `single_price stop_low` は next close までの continuation が非常に強いですが、空売り主体の read になりやすいので、実務枝としては別扱いが必要でした。
- `stop_low × intraday_range` の buy-only follow-up では、`翌日引けが発生日終値より上で引けたら買う` という枝が trade-level では強く、`stop_low + plus` の 5 日後成績は `スタンダード +4.99% / グロース +6.48% / プライム +5.19%` でした。
- ただし同日等ウェイトの portfolio lens を掛けると `スタンダード -2.06% / グロース +0.06% / プライム +1.01%` まで落ちます。スタンダード/グロースの edge は少数の crash-rebound 日に偏っており、`毎日その条件を全部買う` 戦略にはそのまま移りません。
- その多銘柄日を当てると、`2024-08-06` や `2025-04-08` のように event 前日に TOPIX が急落し、entry 日に大きく反発した局面が中心でした。したがってこの研究の後半は、個別 stop-low edge というより `市場クラッシュ翌日の反発確認` をどこまで純化できるか、という問題設定に近づいています。

## Reproduction

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt python \
  apps/bt/scripts/research/run_stop_limit_daily_classification.py \
  --output-root /tmp/trading25-research
```

buy-only follow-up:

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt python \
  apps/bt/scripts/research/run_stop_limit_buy_only_next_close_followthrough.py \
  --output-root /tmp/trading25-research
```

bundle は
`/tmp/trading25-research/market-behavior/stop-limit-daily-classification/<run_id>/`
および
`/tmp/trading25-research/market-behavior/stop-limit-buy-only-next-close-followthrough/<run_id>/`
に保存されます。

## Next Questions

- `outside_standard_band` を broadened limit / special quote 仮説でどう再分類するか。
- `stop_low × intraday_range × next_close=plus` の edge を、`TOPIX event day` の急落度や breadth でどこまで説明できるか。
- `4+ names same day` のような crowding proxy を明示条件に入れると、buy-only branch は portfolio lens でも残るか。
