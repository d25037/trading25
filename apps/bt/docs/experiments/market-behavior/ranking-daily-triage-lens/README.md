# Ranking Daily Triage Lens

Daily Ranking を自動売買候補ではなく、裁量トレード用の少数銘柄 shortlist に圧縮する research。

## Published Readout

### Decision

Daily Ranking Research Base を使い、`inspect` / `watch` / `ignore` / `kill` の triage bucket を作った。目的は「買い rule」を作ることではなく、毎日 5-15 銘柄だけ見る前提で、候補密度と左尾を定量評価することである。

`20260617_ranking_daily_triage_lens_prime_2024_v3` では、Top-K shortlist は母集団を 99% 以上圧縮し、20D/60D の median TOPIX excess をプラスにできた。一方、future winner capture は非常に低く、この rule を「未来の上位銘柄発見器」とは呼ばない。現時点の production implication は、Daily Ranking を裁量判断へ渡す `attention filter` として扱い、Top-K 銘柄を機械的に買う strategy へ昇格しないこと。

### Main Findings

#### 結論: shortlist は候補密度を上げるが、future winner を直接当てる力はまだ弱い

Prime / 2024-01-01 以降 / `stock_master_daily_exact_date` / horizons 20D, 60D。`Inspect Top K` は `triage_bucket != kill` を日付ごとに `triage_score` 順で切ったもの。

| horizon | Top K | candidates | selected | attention reduction | median TOPIX excess | positive | strong gain | severe loss | future winner capture |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 20D | 5 | 938,402 | 2,885 | 99.69% | +0.68% | 54.11% | 13.45% | 6.24% | 0.00% |
| 20D | 10 | 938,402 | 5,770 | 99.39% | +0.67% | 54.14% | 12.58% | 5.29% | 0.24% |
| 20D | 15 | 938,402 | 8,655 | 99.08% | +0.62% | 53.76% | 11.70% | 4.70% | 0.34% |
| 60D | 5 | 870,560 | 2,685 | 99.69% | +1.63% | 54.12% | 34.41% | 19.07% | 0.15% |
| 60D | 10 | 870,560 | 5,370 | 99.38% | +2.14% | 55.40% | 32.94% | 17.71% | 0.47% |
| 60D | 15 | 870,560 | 8,055 | 99.07% | +1.80% | 54.76% | 31.22% | 17.19% | 0.76% |

`future winner capture` は、日付ごとの forward return 上位 Top K と shortlist の overlap 平均。値が低いため、この lens は「その日の大勝ち銘柄を当てる」ものではなく、「見なくてよい大半を落として、median と severe loss を少し良くする」ものと読む。

#### 結論: neutral rerating inspect は効くが、多数候補を全部見る根拠にはしない

| horizon | liquidity regime | bucket | obs | median TOPIX excess | positive | strong gain | severe loss |
| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: |
| 20D | neutral_rerating | inspect | 14,202 | +1.17% | 58.34% | 11.03% | 2.34% |
| 20D | neutral_rerating | watch | 41,125 | +0.03% | 50.21% | 8.68% | 4.91% |
| 20D | neutral_rerating | ignore | 123,314 | -0.56% | 46.49% | 8.38% | 7.12% |
| 20D | neutral_rerating | kill | 98,599 | -1.46% | 41.96% | 9.26% | 12.09% |
| 60D | neutral_rerating | inspect | 13,923 | +2.74% | 59.37% | 29.96% | 12.83% |
| 60D | neutral_rerating | watch | 39,535 | -0.44% | 48.65% | 21.92% | 18.60% |
| 60D | neutral_rerating | ignore | 118,251 | -1.91% | 43.68% | 18.32% | 23.11% |
| 60D | neutral_rerating | kill | 92,962 | -4.43% | 37.79% | 17.18% | 32.87% |

neutral rerating は `inspect` に絞ると 20D/60D とも良い。ただし `watch` / `ignore` は薄く、大量の neutral deep value を全部見る根拠にはならない。裁量 workflow では neutral は raw material であり、`low10 PBR + low10 Fwd PER`、sector strong、ATR20 accel、long hybrid leadership のような追加理由があるものだけを inspect へ上げるのが自然。

#### 結論: crowded rerating は 20D inspect なら候補だが、60D では左尾が残る

| horizon | liquidity regime | bucket | obs | median TOPIX excess | positive | strong gain | severe loss |
| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: |
| 20D | crowded_rerating | inspect | 1,376 | +0.57% | 52.54% | 19.62% | 9.67% |
| 20D | crowded_rerating | watch | 3,993 | -0.94% | 44.80% | 12.97% | 9.09% |
| 20D | crowded_rerating | ignore | 10,474 | -0.57% | 47.65% | 17.01% | 14.16% |
| 20D | crowded_rerating | kill | 26,425 | -0.97% | 46.68% | 21.40% | 22.16% |
| 60D | crowded_rerating | inspect | 1,355 | -1.66% | 44.13% | 27.97% | 28.63% |
| 60D | crowded_rerating | watch | 3,829 | -1.88% | 45.39% | 24.39% | 27.00% |
| 60D | crowded_rerating | ignore | 9,948 | -0.15% | 49.59% | 30.89% | 28.30% |
| 60D | crowded_rerating | kill | 24,517 | -3.40% | 43.65% | 28.74% | 36.89% |

crowded rerating は、ユーザーが裁量で拾っている実感に近く、20D では inspect が watch/ignore より良い。ただし 60D では inspect でも median がマイナスで、severe loss が 28.63% 残る。したがって crowded は「右尾候補として見る」ことは妥当だが、hold horizon と exit / sizing discipline を別に必要とする。

#### 結論: kill は平均的には機能するが、右尾漏れが大きい

| horizon | candidates | killed | strong gain | killed strong gain | kill leakage | killed median |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 20D | 938,402 | 338,760 | 83,470 | 34,832 | 41.73% | -1.57% |
| 60D | 870,560 | 314,040 | 163,482 | 56,650 | 34.65% | -4.21% |

`kill` bucket の median は明確に悪いので候補圧縮には役立つ。ただし strong gain の 34-42% が kill に漏れており、kill は「絶対に上がらない」ではない。裁量上は、kill を通常は見ないが、event / news / special situation で例外確認する設計が必要。

### Interpretation

この研究は、Daily Ranking を strategy signal としてではなく、人間の注意力を配分する triage system として評価した。結果はかなり実務的で、`inspect` は母集団を大きく圧縮し、neutral rerating では median / positive rate / severe loss を改善した。一方で、future winner capture は低く、短期の大勝ち銘柄を機械的に拾う機能はまだ弱い。

ユーザーの現状 workflow、つまり neutral deep value の大量候補を無視しつつ crowded rerating からも拾う運用は、この初回結果と矛盾しない。neutral は広すぎるので inspect 理由が必要。crowded は 20D では見る価値があるが、60D では左尾が残るため、裁量判断・horizon 管理・position sizing を前提にする。

### Production Implication

- Daily Ranking にすぐ triage UI を入れるなら、まず `Inspect / Watch / Ignore / Kill` を research-only overlay として扱う。
- `neutral_rerating inspect` は裁量 shortlist の主候補。大量の neutral deep value を全部表示・確認する必要はない。
- `crowded_rerating inspect` は 20D の right-tail candidate / discretionary inspect 枠。60D hold 前提の green にはしない。
- `kill` は候補圧縮には有用だが、right-tail leakage が大きいため、完全除外 rule ではなく default hidden / exception review が妥当。
- 次の研究では manual review label (`would_trade`, `watch_only`, `reject`, `missed_but_should_have_seen`) を後付けできる table を作り、ユーザーの裁量判断との calibration を測る。

### Caveats

- 初回 triage rule は deterministic heuristic。user manual labels はまだ入っていない。
- outcome は close-to-close TOPIX excess。entry timing、exit、position sizing、transaction cost は未評価。
- `future_winner_capture` は日付ごとの forward return 上位 Top K との overlap で、値が低い。これは shortlist が「未来の上位銘柄予測器」ではないことを示す。
- PSR / Fwd PSR percentile は現行 `daily_ranking_research_ranked` に常時露出していないため、今回の live run では optional fallback として扱った。PSR は kill 補助に留まり、主 score ではない。
- Prime-only evidence であり、Standard/Growth へ外挿しない。

### Source Artifacts

| Artifact | Path |
| --- | --- |
| runner | `apps/bt/scripts/research/run_ranking_daily_triage_lens.py` |
| domain module | `apps/bt/src/domains/analytics/ranking_daily_triage_lens.py` |
| tests | `apps/bt/tests/unit/domains/analytics/test_ranking_daily_triage_lens.py` |
| bundle | `/private/tmp/trading25-research/market-behavior/ranking-daily-triage-lens/20260617_ranking_daily_triage_lens_prime_2024_v3` |
| result tables | `coverage_diagnostics_df`, `daily_triage_candidates_df`, `attention_efficiency_df`, `kill_leakage_df`, `crowded_vs_neutral_triage_df`, `observation_sample_df` |

## Reproduction

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_ranking_daily_triage_lens.py \
  --start-date 2024-01-01 \
  --horizons 20,60 \
  --markets prime \
  --top-ks 5,10,15 \
  --output-root /private/tmp/trading25-research \
  --run-id 20260617_ranking_daily_triage_lens_prime_2024_v3
```
