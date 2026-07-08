# Ranking SMA5 Position State Evidence

Daily Ranking Research Base を使い、既存の SMA5 count / below-streak / ATR20 正規化乖離を「ある銘柄を保有している状態」に変換して検証する。従来の 20D / 60D fixed forward excess ではなく、entry signal の翌 close-to-close 区間から保有し、exit signal が出た close では次の close-to-close 区間を持たない、という position-state excess を集計する。

## Published Readout

### Decision

Run: `20260708_sma5_position_state_prime_2018_rotation_v1`

Initial cash-exit run: `20260708_sma5_position_state_prime_2018_multiscaffold_v1`

対象は Prime、`analysis_start_date=2018-01-01`、primary outcome は `held_state=true` の日だけを等ウェイト化した翌営業日 close-to-close TOPIX excess return。entry は既存 long scaffold の成立日を基準にし、`avoid_atr20_above_ge1` は `sma5_atr20_deviation >= +1.0` の追いかけ買いを除外する。exit は `count_0_1`、`below_sma5_streak_ge3`、`atr20_below_le_neg1`、および3条件の combined を比較した。

Follow-up では `combined_count_streak_atr` が cash exit ではなく他銘柄への持ち替え trigger になる実務を反映し、combined exit event の同日・同一 scaffold・同一 entry rule 内で、combined exit に該当しない候補 basket へ乗り換えた場合の翌営業日 excess を `rotation_evidence_df` として追加した。

結論:

- この position-state research は、従来の fixed 20D excess より実運用に近い outcome として有用。特に「entry を遅らせる」「保有を続ける」「exit watch を起動する」を同じ日次 return 会計で比較できる。
- entry 側では `avoid_atr20_above_ge1` が一貫して改善する。`Neutral + Deep Value + Long Hybrid + ATR20 Accel` では entry signal の翌日 median excess が `+0.092%` から `+0.153%` に改善し、win rate も `52.95%` から `54.81%` へ上がる。
- exit 側では `ATR20 <= -1.0` が最も保有型の成績に合う。Primary scaffold の `avoid_atr20_above_ge1 + atr20_below_le_neg1` は date-level IR `1.296`、cumulative excess `+360.69%`、trade median `+0.417%`。
- `count_0_1` と `below_sma5_streak_ge3` は早く切るため、損失 trade の一部は削るが、保有日数と右尾も削りやすい。combined は tail control と回転率の面では扱いやすいが、期待値は `ATR20 <= -1.0` 単独に劣る。
- ただし combined を cash exit ではなく rotation trigger として見ると、primary scaffold では明確に有用。`Neutral + Deep Value + Long Hybrid + ATR20 Accel` の `avoid_atr20_above_ge1` では、`count_0_1` exit 後の healthy same-scaffold basket への持ち替えが source 持ち続け比 median `+0.283%`、`atr20_below_le_neg1` exit 後は `+0.496%`。
- Sector Strong まで絞ると、position-state の優位はむしろ弱くなる。`Neutral + Deep Value + Sector Strong + ATR20 Accel` は `avoid_atr20_above_ge1 + atr20_below_le_neg1` でも date-level IR `0.686` に留まり、同じ SMA5 exit 技術の効果は primary scaffold より薄い。

### Main Findings

#### 結論: entry は `+1 ATR` 追いかけ買いを避ける方が良い

`sma5_atr20_deviation >= +1.0` の entry を除外すると、全 scaffold で翌日 excess の median / win rate が改善した。これは既存 ATR readout の「`+1 ATR` は entry ban ではなく chase caution」という解釈を、position-state 側では entry delay として使えることを示す。

| scaffold | entry rule | entries | codes | dates | median next-session excess | win rate | median ATR20 deviation |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Deep Value + Long Hybrid + ATR20 Accel | no filter | 3,606 | 190 | 841 | -0.008% | 49.78% | 0.107 |
| Deep Value + Long Hybrid + ATR20 Accel | avoid `>= +1 ATR` | 3,115 | 184 | 795 | +0.015% | 50.63% | -0.022 |
| Neutral + Deep Value + Long Hybrid + ATR20 Accel | no filter | 1,862 | 114 | 514 | +0.092% | 52.95% | 0.311 |
| Neutral + Deep Value + Long Hybrid + ATR20 Accel | avoid `>= +1 ATR` | 1,498 | 107 | 467 | +0.153% | 54.81% | 0.133 |
| Neutral + Deep Value + Sector Strong + ATR20 Accel | no filter | 2,634 | 208 | 694 | +0.092% | 52.81% | 0.383 |
| Neutral + Deep Value + Sector Strong + ATR20 Accel | avoid `>= +1 ATR` | 2,056 | 201 | 632 | +0.149% | 53.99% | 0.179 |

#### 結論: primary scaffold では ATR20 exit が最も hold-friendly

`Neutral + Deep Value + Long Hybrid + ATR20 Accel` では、`ATR20 <= -1.0` まで保有を続ける variant が最も強い。combined exit は trade median と win rate は残るが、保有期間が短くなり、累積 excess と date-level IR は落ちる。

| entry rule | exit rule | dates | position days | median daily excess | cumulative excess | date-level IR | median trade excess | median holding days |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| avoid `>= +1 ATR` | `atr20_below_le_neg1` | 1,283 | 8,843 | +0.034% | +360.69% | 1.296 | +0.417% | 20 |
| avoid `>= +1 ATR` | `combined_count_streak_atr` | 781 | 4,184 | +0.058% | +105.68% | 1.067 | +0.167% | 5 |
| avoid `>= +1 ATR` | `count_0_1` | 954 | 5,595 | +0.019% | +89.98% | 0.814 | +0.091% | 8 |
| avoid `>= +1 ATR` | `below_sma5_streak_ge3` | 868 | 4,739 | +0.021% | +52.36% | 0.596 | +0.097% | 7 |
| no filter | `atr20_below_le_neg1` | 1,332 | 9,668 | +0.041% | +301.76% | 1.141 | +0.205% | 21 |
| no filter | `combined_count_streak_atr` | 822 | 4,719 | +0.051% | +94.69% | 0.910 | +0.093% | 5 |

#### 結論: broad Deep Value では daily は強いが trade median は弱い

`Deep Value + Long Hybrid + ATR20 Accel` まで広げると、`ATR20 <= -1.0` exit の日次成績は最も高いが、trade median はマイナスになる。大きい勝ち trade が平均・累積を押し上げる一方、小さい負け trade が多い。production では broad scaffold をそのまま保有ルール化せず、neutral primary scaffold を優先する。

| scaffold | entry rule | exit rule | position days | median daily excess | cumulative excess | date-level IR | median trade excess | win trade rate |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Deep Value + Long Hybrid + ATR20 Accel | avoid `>= +1 ATR` | `atr20_below_le_neg1` | 16,352 | +0.070% | +614.74% | 1.445 | -0.371% | 46.74% |
| Deep Value + Long Hybrid + ATR20 Accel | avoid `>= +1 ATR` | `count_0_1` | 9,613 | +0.041% | +149.54% | 0.861 | -0.172% | 46.71% |
| Deep Value + Long Hybrid + ATR20 Accel | avoid `>= +1 ATR` | `combined_count_streak_atr` | 7,037 | +0.005% | +76.78% | 0.659 | -0.121% | 47.24% |

#### 結論: Sector Strong 追加は position-state では上乗せにならない

`Neutral + Deep Value + Sector Strong + ATR20 Accel` は entry の翌日 quality は良いが、保有状態にした後の edge は primary scaffold より薄い。sector strong は fixed-horizon の candidate confidence には使えるが、この SMA5 position-state では hard refinement としては弱い。

| entry rule | exit rule | position days | median daily excess | cumulative excess | date-level IR | median trade excess |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| avoid `>= +1 ATR` | `atr20_below_le_neg1` | 13,060 | +0.036% | +104.81% | 0.686 | -0.101% |
| avoid `>= +1 ATR` | `combined_count_streak_atr` | 6,082 | +0.025% | +37.49% | 0.477 | -0.036% |
| no filter | `below_sma5_streak_ge3` | 7,946 | -0.011% | -0.33% | 0.115 | -0.206% |

#### 結論: combined exit は cash exit より rotation trigger として読む方が実務に近い

combined exit event で source を翌日も持ち続けた場合と、同じ日の同一 scaffold 内で combined exit に該当しない候補 basket へ乗り換えた場合を比較した。Primary scaffold では、乗り換え先がある event は `58〜67%` 程度に限られるが、利用可能な日は rotation の median delta が正で、特に `count_0_1` と `atr20_below_le_neg1` の source 悪化を補いやすい。

| scaffold | entry rule | exit reason | target available | target count median | source next median | rotation basket median | rotation - source median | rotation outperform |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Neutral + Deep Value + Long Hybrid + ATR20 Accel | avoid `>= +1 ATR` | `atr20_below_le_neg1` | 40.98% | 3.5 | -0.050% | +0.246% | +0.496% | 64.00% |
| Neutral + Deep Value + Long Hybrid + ATR20 Accel | avoid `>= +1 ATR` | `below_sma5_streak_ge3` | 58.48% | 3.0 | +0.227% | +0.497% | +0.074% | 53.85% |
| Neutral + Deep Value + Long Hybrid + ATR20 Accel | avoid `>= +1 ATR` | `count_0_1` | 67.07% | 3.0 | -0.108% | +0.210% | +0.283% | 60.00% |
| Deep Value + Long Hybrid + ATR20 Accel | avoid `>= +1 ATR` | `count_0_1` | 71.50% | 4.0 | -0.333% | -0.245% | +0.090% | 53.62% |
| Neutral + Deep Value + Sector Strong + ATR20 Accel | avoid `>= +1 ATR` | `atr20_below_le_neg1` | 45.22% | 3.0 | +0.342% | +0.902% | -0.086% | 47.97% |
| Neutral + Deep Value + Sector Strong + ATR20 Accel | avoid `>= +1 ATR` | `count_0_1` | 59.62% | 3.0 | -0.336% | +0.066% | +0.022% | 51.61% |

`valid_same_scaffold_basket` と `healthy_same_scaffold_basket` は、primary `avoid_atr20_above_ge1` では同じ候補になることが多い。これは entry rule 側で `+1 ATR` を除外し、rotation 側でも combined exit に該当する弱い候補を除外しているため。

### Interpretation

今回の主な価値は、SMA5 系の診断を「銘柄日に付けるラベル」から「実際に持つ/持たない状態」へ変換した点にある。これにより、`20D excess` のような固定 horizon では見えなかった、exit の早さと右尾放棄の trade-off が見える。

`ATR20 <= -1.0` は、従来 readout では stop-review 候補だったが、position-state では「この水準までは保有を継続する」ルールとしても強い。特に primary scaffold では trade median がプラスで、日次 equal-weight の IR も最も高い。逆に `count_0_1` や `below_sma5_streak_ge3` は弱化検知としては速いが、即 exit にすると rebound と winner を捨てやすい。

`combined_count_streak_atr` は保有期間を短くし、trade の severe loss をほぼ消す。ただし cash exit として評価すると期待値も削る。実務に近い rotation lens では、primary scaffold の同日 healthy candidate basket へ移すことで source 持ち続けより改善する日が多い。したがって combined は「現金化」より「他銘柄へ資金を逃がす」trigger として読む方が自然。

### Production Implication

- Daily Ranking の long 候補では、`sma5_atr20_deviation >= +1.0` を hard ban ではなく entry delay / chase caution として使う。
- Primary long scaffold は `Neutral + Deep Value + Long Hybrid Leadership + ATR20 Accel` を優先する。ここで `avoid_atr20_above_ge1 + atr20_below_le_neg1` が最も hold-friendly。
- `sma5_above_count_5d = 0/1` と `close < SMA5` 3連続は、即時全売却よりも `exit_watch` / position shrink / same-scaffold rotation trigger として扱う。rotation 候補が同日に存在する場合は、cash exit より同一 primary scaffold 内での持ち替えを優先して検討する。
- `ATR20 <= -1.0` は最終 stop-review の第一候補だが、これも intraday stop ではなく日足 close 確定後の翌 session exposure 調整として扱う。
- Sector Strong をさらに重ねても position-state edge は増えないため、この研究単独では hard refinement にしない。

### Caveats

- outcome は翌営業日 close-to-close TOPIX excess の日次 equal-weight aggregation。execution cost、税、slippage、capacity、寄り/引け約定差は未反映。
- signal は当日 close を含むため、entry/exit は次の close-to-close interval から反映した。pre-open screening 可能性は別研究が必要。
- `trade_excess_return_pct` は日次 excess の複利近似であり、実際の portfolio NAV ではない。
- rotation は同日 candidate basket への機械的な近似で、実際の保有銘柄数、資金制約、既存保有との重複、約定優先順位は未反映。
- publication run は Prime 2018年以降。Standard/Growth やより古い history には外挿しない。
- broader `Deep Value + Long Hybrid + ATR20 Accel` は daily aggregate が強い一方、trade median がマイナスになりやすい。large winner dependency があるため、position sizing / portfolio lens なしに mechanical rule 化しない。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_ranking_sma5_position_state_evidence.py`
- Module: `apps/bt/src/domains/analytics/ranking_sma5_position_state_evidence.py`
- Test: `apps/bt/tests/unit/domains/analytics/test_ranking_sma5_position_state_evidence.py`
- Bundle: `~/.local/share/trading25/research/market-behavior/ranking-sma5-position-state-evidence/20260708_sma5_position_state_prime_2018_rotation_v1/`
- Cash-exit bundle: `~/.local/share/trading25/research/market-behavior/ranking-sma5-position-state-evidence/20260708_sma5_position_state_prime_2018_multiscaffold_v1/`
- Primary-only bundle: `~/.local/share/trading25/research/market-behavior/ranking-sma5-position-state-evidence/20260708_sma5_position_state_prime_2018_v1/`
- Results tables: `entry_rule_evidence_df`, `position_state_daily_evidence_df`, `position_state_trade_evidence_df`, `exit_reason_evidence_df`, `rotation_evidence_df`, `coverage_diagnostics_df`, `observation_sample_df`
