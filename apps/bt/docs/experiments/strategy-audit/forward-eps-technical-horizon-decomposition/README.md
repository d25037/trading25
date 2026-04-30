# Forward EPS Technical Horizon Decomposition

## Published Readout

### Decision

`production/forward_eps_driven` の v3 再精査として、entry 前日時点の `RSI 10/20/60`、`run-up 10/20/60`、`risk-adjusted-return 10/20/60` を追加し、technical overheat の時間軸を分解した。

結論は、ファンダメンタル追加条件よりも technical overheat の「完成度」を見る方が有益。ただし、単純な hard exclude ではなく、Prime 限定の size haircut / risk cap 候補として扱う。特に 60d run-up は full-history の severe loss を最も濃くし、10d short-climax は holdout で左尾をよく分けた。一方で Standard は overheat 側に右尾も多く、Prime と同じ pruning rule を掛けると平均リターンを壊しやすい。

### Why This Research Was Run

`forward_eps_driven` の v3 readout では、低PBR・低forward PER・forward EPS margin の追加より、急騰後に入る trade の左尾管理が重要だった。既存分析は `RSI10`、`run-up 20/60`、strategy 設定由来の `RAR60` に寄っていたため、10/20/60 horizon を揃えて、短期クライマックスなのか、60d trend maturity なのかを切り分けた。

### Data Scope / PIT Assumptions

- Strategy: `production/forward_eps_driven`
- Universe preset: `primeExTopix500`
- Data source: `market.duckdb` v3
- Period: `2016-05-02` -> `2026-04-30`
- Holdout: recent 6 calendar months, `2025-10-30` -> `2026-04-30`
- Entry enrichment: entry 日の前営業日までの price series だけで `RSI` / `run-up` / `risk-adjusted-return` を計算
- Thresholds: `train_pre_holdout` の market-scope 別 Q80 を calibration threshold とし、full / holdout の candidate rule に適用

### Main Findings

#### 結論: Prime の bad tail は 60d run-up と 10d climax の両方で見える

| scope | feature | high bucket avg | high bucket severe | low bucket avg | low bucket severe | read |
|---|---:|---:|---:|---:|---:|---|
| Prime full | `runup_60d_pct` | `+6.59%` | `44.93%` | `+9.85%` | `14.71%` | 60d 上昇完成後の entry が severe loss を最も濃くする |
| Prime full | `runup_20d_pct` | `-2.44%` | `43.48%` | `+12.23%` | `26.47%` | 20d 急騰は平均も悪化 |
| Prime full | `runup_10d_pct` | `+0.99%` | `42.03%` | `+18.09%` | `23.53%` | 10d 短期過熱も左尾を濃縮 |
| Prime full | `risk_adjusted_return_60d` | `+4.88%` | `40.58%` | `+12.07%` | `22.06%` | RAR は 60d 側が最も tail diagnostic になる |
| Prime full | `rsi_10` | `-0.44%` | `36.23%` | `+17.29%` | `22.06%` | RSI は短期 climax の補助軸として有効 |

#### 結論: Prime の right tail はむしろ過熱が低い

| cohort | trades | avg trade | median `runup_10d` | median `runup_20d` | median `runup_60d` | median `RAR10` | median `RSI10` |
|---|---:|---:|---:|---:|---:|---:|---:|
| Prime all | 344 | `+5.88%` | `+6.78%` | `+12.12%` | `+30.23%` | `4.06` | `66.67` |
| Prime severe loss | 85 | `-15.44%` | `+11.34%` | `+19.69%` | `+41.38%` | `4.29` | `66.94` |
| Prime right tail p90 | 35 | `+86.03%` | `+0.93%` | `+7.26%` | `+27.90%` | `0.81` | `53.00` |

この差は重要。右尾は「entry 前に強く上がり切った銘柄」ではなく、むしろ過熱が低い状態から forward EPS 条件に乗った銘柄だった。したがって、technical overheat は alpha source ではなく bad-tail control として読む。

#### 結論: Prime candidate は hard exclude より haircut が自然

| rule | selected trades | selected avg | selected severe | kept avg | kept severe | kept worst | right-tail retention | 0.5x haircut avg | 0.5x haircut severe |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Prime full `legacy_20_60_runup_rar60_q80_overlap_ge2` | 62 | `+5.28%` | `45.16%` | `+6.01%` | `20.21%` | `-24.31%` | `85.71%` | `+5.40%` | `19.19%` |
| Prime full `overheat_same_horizon_60d_q80_overlap_ge2` | 71 | `+4.59%` | `39.44%` | `+6.22%` | `20.88%` | `-24.31%` | `82.86%` | `+5.41%` | `19.19%` |
| Prime full `short_climax_10d_q80_overlap_ge2` | 68 | `-0.12%` | `38.24%` | `+7.36%` | `21.38%` | `-33.89%` | `88.57%` | `+5.89%` | `18.60%` |
| Prime full `overheat_runup_rar_cross_horizon_q80_overlap_ge3` | 70 | `+0.10%` | `38.57%` | `+7.36%` | `21.17%` | `-24.31%` | `85.71%` | `+5.87%` | `19.19%` |

`short_climax_10d` は平均改善が大きいが worst を取り切らない。`legacy_20_60_runup_rar60` と cross-horizon run-up/RAR は worst を `-24.31%` まで改善するが、平均改善は小さい。Production では hard exclude より、該当 trade の 0.5x sizing / max exposure cap から検証するのが妥当。

#### 結論: Holdout では 10d short-climax が最も鋭いが、worst は 20/60/RAR 側が捕まえる

| rule | selected trades | selected avg | selected severe | kept avg | kept severe | kept worst | right-tail retention | 0.5x haircut avg | 0.5x haircut severe | 0.5x haircut worst |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Prime holdout `short_climax_10d_q80_overlap_ge2` | 3 | `-9.77%` | `66.67%` | `+10.03%` | `11.11%` | `-32.05%` | `100.00%` | `+7.90%` | `9.52%` | `-32.05%` |
| Prime holdout `legacy_20_60_runup_rar60_q80_overlap_ge2` | 7 | `+0.82%` | `28.57%` | `+10.39%` | `14.29%` | `-15.20%` | `100.00%` | `+7.06%` | `14.29%` | `-16.03%` |
| Prime holdout `overheat_same_horizon_60d_q80_overlap_ge2` | 6 | `+2.69%` | `16.67%` | `+9.00%` | `20.00%` | `-15.20%` | `100.00%` | `+6.82%` | `19.05%` | `-16.03%` |

Holdout の trade 数は 21 と小さいが、10d short-climax は右尾を全て残しながら severe loss を濃縮した。一方、worst trade の改善は 20/60/RAR overlap の方が明確だった。したがって、短期 climax と 60d maturity は代替ではなく、別々の risk lens として扱う。

#### 結論: Standard には同じ overheat pruning を掛けない

| scope | rule | selected trades | selected avg | selected severe | kept avg | kept severe | read |
|---|---|---:|---:|---:|---:|---:|---|
| Standard full | `legacy_20_60_runup_rar60_q80_overlap_ge2` | 13 | `+26.93%` | `30.77%` | `+3.93%` | `12.73%` | 左尾も濃いが右尾も多く、除外すると平均を壊す |
| Standard full | `overheat_same_horizon_10d_q80_overlap_ge2` | 16 | `+24.03%` | `12.50%` | `+3.50%` | `17.31%` | 10d 過熱はむしろ winners を含む |
| Standard full | `overheat_all_technical_q80_overlap_ge4` | 16 | `+22.01%` | `12.50%` | `+4.12%` | `17.31%` | Standard は別研究・別 rule が必要 |

### Interpretation

新しい事実は、`forward_eps_driven` の technical risk が単一の「過熱」ではなく、少なくとも二種類あること。

1. `runup_60d_pct` / `RAR60` が示す trend maturity: 長く上がり切った後に forward EPS 条件で入ると、full-history の severe loss が濃くなる。
2. `RSI10` / `runup_10d` / `RAR10` が示す short climax: 直近 holdout では少数の悪い trade を強く分ける。

Prime の right tail は entry 前の overheat がむしろ低いため、過熱 filter は alpha selection ではない。右尾を残しつつ左尾を小さくする sizing/risk-control lens として扱うべき。

### Production Implication

現時点で `production/forward_eps_driven` に hard entry filter は追加しない。次の実装候補は、Prime 限定で以下の position sizing / risk cap を portfolio-level backtest すること。

1. `short_climax_10d_q80_overlap_ge2` は 0.5x sizing 候補。Holdout で severe loss を強く濃縮したが、worst trade は取り切らない。
2. `legacy_20_60_runup_rar60_q80_overlap_ge2` または `overheat_runup_rar_cross_horizon_q80_overlap_ge3` は max-loss / worst-tail cap 候補。Holdout worst の改善が最も分かりやすい。
3. Standard には Prime の pruning rule を流用しない。Standard は right-tail も overheat 側に乗るため、市場別の別検証が必要。

### Caveats

- この readout は trade-level proxy であり、portfolio CAGR / max drawdown / capital sharing を直接示さない。
- Holdout は 21 trades と小標本。
- Threshold は train-window Q80 で calibration したが、実 production 化には rolling / walk-forward threshold と portfolio-level sizing verification が必要。
- `risk_adjusted_return_*` は今回 `sharpe` ratio type で統一した。

### Source Artifacts

- Bundle: `~/.local/share/trading25/research/strategy-audit/forward-eps-technical-horizon-decomposition/20260430_forward_eps_technical_horizon_prime_ex_topix500_rerun2`
- Results DB: `~/.local/share/trading25/research/strategy-audit/forward-eps-technical-horizon-decomposition/20260430_forward_eps_technical_horizon_prime_ex_topix500_rerun2/results.duckdb`
- Summary: `~/.local/share/trading25/research/strategy-audit/forward-eps-technical-horizon-decomposition/20260430_forward_eps_technical_horizon_prime_ex_topix500_rerun2/summary.md`
- Runner: `uv run --project apps/bt python apps/bt/scripts/research/run_forward_eps_technical_horizon_decomposition.py --dataset primeExTopix500 --holdout-months 6 --run-id 20260430_forward_eps_technical_horizon_prime_ex_topix500_rerun2`

## Runner

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_forward_eps_technical_horizon_decomposition.py \
  --dataset primeExTopix500 \
  --holdout-months 6 \
  --run-id 20260430_forward_eps_technical_horizon_prime_ex_topix500_rerun2
```

## Artifact Tables

- `dataset_summary_df`: market.duckdb v3 universe summary.
- `scenario_summary_df`: `forward_eps_driven` の window 別 trade-level summary。
- `market_scope_summary_df`: entry 日 PIT market scope ごとの summary。
- `threshold_summary_df`: train-window Q80 thresholds by market scope and feature.
- `horizon_bucket_summary_df`: `RSI` / `run-up` / `risk-adjusted-return` の horizon bucket summary。
- `horizon_contrast_summary_df`: low bucket vs high bucket contrast。
- `horizon_tail_profile_df`: all / severe loss / right tail の horizon feature median profile。
- `horizon_candidate_summary_df`: overheat overlap / haircut candidate summary。
- `enriched_trade_df`: entry 時点 10/20/60 technical features 付き trade ledger。
