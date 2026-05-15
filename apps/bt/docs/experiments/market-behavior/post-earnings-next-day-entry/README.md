# Post-Earnings Next-Day Entry

## Published Readout

### Decision

`post_earnings_next_day_entry` は、「決算発表後、翌営業日の寄りで買うとどうなるか」を、寄らずストップ高・ストップ安を約定不能として分離して見る research として追加する。

今回の初回 run では、発表後に positive event を確認してから買っても、Prime 全体では単純な post-earnings drift は強くない。Prime positive event の20-session TOPIX excess median は FY `-0.88%`、non-FY `-0.02%`。ただし、ユーザーが問題にしていた `20d/60d runup + ADV/FF >= 1%` の positive event に限ると、FY の `ADV/FF 1.0-2.0%` は median `+2.39%`、non-FY の `ADV/FF 1.0-2.0%` は median `+0.71%` で、hold-through より「発表後に絞る」方が読みやすい。

一方、`ADV/FF >= 2.0%` は強気材料ではない。FY positive runup bucket でも median `+0.52%`、severe loss `21.43%`、non-FY は median `-0.50%`。高参加・高注目ほど右尾と左尾が同居する。

この readout は bundle `/private/tmp/trading25-research/market-behavior/post-earnings-next-day-entry/20260515_post_earnings_next_day_initial` に基づく。入力 DB は `/Users/shinjiroaso/.local/share/trading25/market-timeseries/market.duckdb`、対象は `2016-04-01` から `2026-05-14`。

### Main Findings

#### 結論

Prime の attempted events は `81,530` events / `2,048` codes。`limit_up_no_fill` は全体 `0.22%`、`limit_down_no_fill` は `0.05%` と低頻度。ただし、positive event や high ADV/FF runup では no-fill rate が局所的に上がるため、executable return に混ぜない。

| scope | attempted | executable rate | limit up no-fill | limit down no-fill | gap extreme executable |
|---|---:|---:|---:|---:|---:|
| Prime all | `81,530` | `99.72%` | `0.22%` | `0.05%` | `7.76%` |

#### Event Strength

発表後に positive event を確認してから買っても、Prime 全体では20営業日 median が強くない。negative event は FY で特に弱い。

| FY | event | attempted | executable | limit up no-fill | limit down no-fill | 1d median | 5d median | 20d median | severe loss |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| true | positive | `13,200` | `13,154` | `0.34%` | `0.01%` | `+0.04%` | `-0.32%` | `-0.88%` | `9.69%` |
| true | negative | `7,120` | `7,092` | `0.11%` | `0.28%` | `-0.31%` | `-0.84%` | `-1.86%` | `11.15%` |
| false | positive | `11,375` | `11,324` | `0.43%` | `0.02%` | `+0.00%` | `-0.35%` | `-0.02%` | `8.87%` |
| false | negative | `8,865` | `8,844` | `0.11%` | `0.12%` | `-0.14%` | `-0.44%` | `-0.53%` | `8.72%` |

#### Positive x Runup x ADV/FF

ユーザーの実務 bucket に近い `positive event x 20d/60d runup x ADV/FF >= 1%` では、`1.0-2.0%` が比較的良い。`>=2.0%` はサンプルが小さく、中央値と左尾のバランスが悪い。

| FY | ADV/FF threshold | attempted | executable | limit up no-fill | 1d median | 5d median | 20d median | severe loss |
|---:|---|---:|---:|---:|---:|---:|---:|---:|
| true | `1.0-2.0%` | `153` | `149` | `2.61%` | `+0.80%` | `+0.66%` | `+2.39%` | `11.41%` |
| true | `>=2.0%` | `84` | `84` | `0.00%` | `+0.37%` | `+1.00%` | `+0.52%` | `21.43%` |
| false | `1.0-2.0%` | `226` | `223` | `0.88%` | `+0.00%` | `-0.05%` | `+0.71%` | `13.45%` |
| false | `>=2.0%` | `179` | `173` | `3.35%` | `-0.26%` | `-0.01%` | `-0.50%` | `11.56%` |

#### Exact Runup Buckets

細かく見ると、FY `runup x runup x ADV/FF 1.0-2.0%` は median `+3.34%`、severe loss `7.89%` と最も読みやすい。一方、`strong_runup` は sample が小さく、分布が荒い。

| FY | 20d bucket | 60d bucket | ADV/FF | attempted | executable | 20d median | severe loss |
|---:|---|---|---|---:|---:|---:|---:|
| true | `runup` | `runup` | `1.0-2.0` | `76` | `76` | `+3.34%` | `7.89%` |
| true | `runup` | `strong_runup` | `1.0-2.0` | `51` | `48` | `+1.28%` | `18.75%` |
| true | `runup` | `runup` | `ge2.0` | `27` | `27` | `-0.41%` | `18.52%` |
| false | `runup` | `runup` | `1.0-2.0` | `100` | `100` | `+0.61%` | `10.00%` |
| false | `runup` | `runup` | `ge2.0` | `58` | `56` | `+2.06%` | `1.79%` |
| false | `runup` | `strong_runup` | `ge2.0` | `79` | `77` | `-1.69%` | `12.99%` |

#### Limit No-Fill

寄らずストップ高は positive event に多いが、件数は全体では小さい。実務上は「positive を見てから買う」ほどこの no-fill bias が出るため、executable return から必ず除外する。

| FY | event | execution label | events | avg pre20 | avg pre60 | median ADV/FF |
|---:|---|---|---:|---:|---:|---:|
| false | positive | `limit_up_no_fill` | `49` | `+4.50%` | `+23.83%` | `0.51%` |
| true | positive | `limit_up_no_fill` | `45` | `+2.27%` | `+12.46%` | `0.41%` |
| true | negative | `limit_down_no_fill` | `20` | `-4.31%` | `-2.10%` | `0.74%` |
| false | negative | `limit_down_no_fill` | `11` | `-1.96%` | `+4.75%` | `1.01%` |

### Interpretation

hold-through の問題は、発表前には positive / negative を選べないことだった。post-entry はこの問題を解消するが、発表直後の gap を払って入るため、Prime positive 全体では median がほぼ残らない。

実務 bucket に近い `runup x runup x ADV/FF 1.0-2.0%` は、FY でも post-entry の方が見込みがある。これは「本決算またぎは厳しいが、良い FY を見て翌日に買う」なら検討余地がある、という整理になる。ただし sample は `76` events と小さく、production に落とすには walk-forward / year split / portfolio lens が必要。

`ADV/FF >= 2.0%` は引き続き危険。post-entry でも右尾はあるが、FY positive runup で severe loss `21.43%`、non-FY positive runup で limit-up no-fill `3.35%` と、混雑・注目度の副作用が残る。

### Production Implication

この Phase 1 だけで production strategy に post-earnings entry を追加しない。

次に見る価値がある候補は以下。

| 用途 | 候補 |
|---|---|
| Candidate | Prime positive FY の `20d runup x 60d runup x ADV/FF 1.0-2.0%` |
| Avoid / sizing | `ADV/FF >= 2.0%`、`strong_runup`、`limit_up_no_fill` 近傍 |
| Execution accounting | `limit_up_no_fill` は executable return から除外し、attempted view では no-fill rate として別掲 |
| Next validation | 年度別 split、market regime split、same-day equal-weight portfolio、entry 翌日寄りの gap cost 分解 |

### Caveats

- `event_strength` は発表後 outcome。post-entry では使えるが、hold-through の事前条件には使えない。
- Entry は `entry_date` の `Open`、exit は horizon 日目の `Close`。TOPIX excess は TOPIX の `entry_date close -> exit close` を使う。TOPIX open が無いため、1d excess は stock open-to-close に近い。
- `limit_up_no_fill` / `limit_down_no_fill` は daily OHLC による近似。標準 JPX 制限値幅と one-price day で分類しており、特別気配・拡大制限値幅・板状況までは分からない。
- no-fill event は executable return から除外している。attempted view では no-fill rate として別掲する。
- `gap_extreme_executable` は no-fill ではないが、entry gap が大きい日として別 label にしている。
- `Med ADV60` は Daily Ranking と同じ trailing median `close * volume` を使うが、liquidity bucket は `ADV60 / free-float` の単純分類。

### Source Artifacts

- module: `apps/bt/src/domains/analytics/post_earnings_next_day_entry.py`
- runner: `apps/bt/scripts/research/run_post_earnings_next_day_entry.py`
- bundle experiment id: `market-behavior/post-earnings-next-day-entry`
- latest result bundle: `/private/tmp/trading25-research/market-behavior/post-earnings-next-day-entry/20260515_post_earnings_next_day_initial`
- result tables: `event_feature_df`, `coverage_diagnostics_df`, `execution_diagnostics_df`, `post_entry_expectancy_df`, `attempted_entry_outcome_df`, `limit_no_fill_df`

## Runner

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_post_earnings_next_day_entry.py \
  --start-date 2016-04-01 \
  --end-date 2026-05-14 \
  --pre-windows 20,60 \
  --horizons 1,5,20 \
  --liquidity-window 60 \
  --output-root /private/tmp/trading25-research \
  --run-id 20260515_post_earnings_next_day_initial
```

## Artifact Tables

- `event_feature_df`: event-level disclosure outcome, next-session execution label, no-fill status, pre features, and post-entry returns.
- `coverage_diagnostics_df`: market-level event, executable, stop no-fill, and liquidity coverage.
- `execution_diagnostics_df`: market / FY / event strength / execution label counts.
- `post_entry_expectancy_df`: executable-only forward returns by market / FY / event strength / pre return buckets / ADV60-to-free-float bucket.
- `attempted_entry_outcome_df`: attempted-event view with executable and no-fill rates.
- `limit_no_fill_df`: one-price stop-limit events separated from executable returns.
