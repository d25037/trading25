# Accumulation Flow Followthrough

## Question

CMF / Chaikin Oscillator / OBV で「価格上昇前の買い集め proxy」を作り、翌営業日寄りからその後の終値までの return に再現性があるかを調べる。

## Runner

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_accumulation_flow_followthrough.py
```

主な option:

- `--start-date` / `--end-date`: signal date の分析範囲。
- `--horizons`: `5,10,20,60` のような forward close horizon。
- `--chaikin-fast-period` / `--chaikin-slow-period`: Chaikin oscillator の ADL EMA 期間。
- `--cmf-threshold` / `--chaikin-oscillator-threshold` / `--obv-score-threshold`: 各 flow proxy の成立閾値。
- `--min-votes`: CMF / Chaikin / OBV のうち必要な成立数。default は `2`。
- `--max-close-to-sma` / `--max-close-to-high`: まだ伸び切っていない価格状態の制約。
- `--lower-wick-threshold`: 下ヒゲ吸収 branch の閾値。
- `--concentration-caps`: entry date ごとの上位N銘柄 cap。default は `10,25,50`。

## Indicator Definitions

- `CMF`: `sum(money_flow_multiplier * volume) / sum(volume)`。
- `Chaikin oscillator`: `EMA(ADL, fast) - EMA(ADL, slow)`。CMF と同じ money flow multiplier を使うが、rolling 比率ではなく ADL の短期加速を測る。
- `OBV flow score`: `OBV` の lookback 差分 / 同期間の出来高合計。

## Output

Bundle は `manifest.json` / `results.duckdb` / `summary.md` / `summary.json` を出力する。

主要 table:

- `universe_summary_df`: universe 別の銘柄数、stock-days、branch 別 event count。
- `event_df`: signal date 単位のイベント明細。filter branch は重複行として保持する。
- `event_summary_df`: universe / branch / horizon の trade-level 集計。
- `yearly_summary_df`: 年別の trade-level 集計。
- `entry_cohort_df`: 同一 entry date の signal を等ウェイト化した日次 cohort。
- `cohort_portfolio_summary_df`: entry cohort の portfolio lens 集計。
- `yearly_cohort_summary_df`: entry cohort の年別 portfolio lens 集計。
- `capped_entry_cohort_df`: signal strength 順に entry date ごとの銘柄数を cap した cohort。
- `capped_cohort_portfolio_summary_df`: concentration cap ごとの portfolio lens 集計。
- `oos_portfolio_summary_df`: `2016-2020` discovery / `2021-2023` validation / `2024-forward` OOS の固定期間別 portfolio lens。

Concentration cap の順位付けは、同日までに観測できる `accumulation_vote_count`、`CMF`、`OBV flow score`、`Chaikin oscillator`、`lower_wick_ratio` の順に使う。同一条件の最後は `code` で安定ソートする。

## Interpretation

この研究の CMF / Chaikin / OBV は機関投資家の実際の注文主体を直接観測するものではなく、日足 OHLCV から見た accumulation proxy として扱う。採用判断では `event_summary_df` の trade-level 平均だけでなく、同日に signal が集中したときの影響を見る `cohort_portfolio_summary_df`、TOPIX 同期間 return を差し引く `mean_excess_return`、および concentration cap / OOS の robustness を優先して確認する。

PIT 面では、signal date の特徴量は同日以前の OHLCV だけで計算し、future return は評価列として後から付与する。universe membership は現行 stock master の `market_code` / `scale_category` による proxy で、過去時点の市場区分移動はこの runner では補正しない。
