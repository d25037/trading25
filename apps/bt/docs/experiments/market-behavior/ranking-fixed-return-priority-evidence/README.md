# Ranking Fixed Return Priority Evidence

## Published Readout

### Decision

**fixed 20D/60D を Ranking 優先度から外すとも、現状のまま有効と認定するとも結論できない。正式判定は `insufficient_evidence` である。**

`strict_value_long_only` では fixed 20D、fixed 60D、等ウェイト合成のすべてが20D forward TOPIX-excess returnを正方向に順位付けした。しかし、独立再現先の `value_extension_long_only` は1日あたりの候補が少なく、連続順位を比較できた日が9日だけだった。そこで60Dと合成は逆方向になった。事前に固定した「2 family・50 paired dates・median focus 5銘柄」の採用gateを満たさないため、production Rankingの優先度カラム、badge、API、UIは変更しない。

この結果は「fixed 20D/60Dに価値がない」という棄却ではない。強い候補群では有望だが、置換・維持を決めるだけの独立再現性がまだない、という結論である。raw 20D/60Dは当面 informational fieldとして維持できるが、本bundle単独では優先度ロジックの根拠にしない。

### Research Question

全銘柄の一般的momentumではなく、fixed returnを使わずに抽出した「returnが期待されるlong候補」の内側で、fixed 20D/60Dが次の20D excess returnを順位付けできるかを検証した。

候補群はfixed return、neutral/crowded/stress、`momentum_20_60_top20`、`ex_overheat`、current sector strengthを使う前に確定した。

| Independent family | Fixed-free definition |
| --- | --- |
| `strict_value_long_only` | Deep Value + 120D/252D/504D Long Hybrid Leadership + raw ATR20 acceleration |
| `value_extension_long_only` | equal-weight value score `>=0.8` + 同じLong Hybrid + raw ATR20 acceleration、Deep Valueを除外 |

### Data Scope / PIT

signal dateの`stock_master_daily` exact-date membershipでPrime相当だけを解決した。市場再編前は`0101`、再編後は`0111`で、Standard/Growthは含めない。Market v4のPIT valuation、adjusted price、liquidityを使い、signal close後からforward outcomeを測るafter-close研究である。primaryは20D close-to-close TOPIX-excess return、5D/60Dは補助診断。2024年以降は仮説起点でありholdoutではない。

### Main Result

#### 連続順位

各signal dateのPrime全銘柄内で20D/60D return percentileを作り、候補family内の上位20%と下位20%を比較した。

| Family | Priority | Eligible dates | Mean 20D top-bottom lift | 95% moving-block CI | Median IC | IC-positive dates | Gate reading |
| --- | --- | ---: | ---: | --- | ---: | ---: | --- |
| `strict_value_long_only` | fixed 20D | 108 | `+1.999 pp` | `[+0.107, +4.261]` | `+0.093` | `59.3%` | 効果は正、focus数不足 |
| `strict_value_long_only` | fixed 60D | 108 | `+3.115 pp` | `[+0.117, +6.143]` | `+0.213` | `72.2%` | 効果は正、focus数不足 |
| `strict_value_long_only` | equal composite | 108 | `+3.497 pp` | `[+0.900, +6.262]` | `+0.202` | `72.2%` | 効果は正、focus数不足 |
| `value_extension_long_only` | fixed 20D | 9 | `+1.609 pp` | inferentially insufficient | `+0.321` | `55.6%` | 50日未満 |
| `value_extension_long_only` | fixed 60D | 9 | `-2.254 pp` | inferentially insufficient | `+0.014` | `55.6%` | 逆方向・50日未満 |
| `value_extension_long_only` | equal composite | 9 | `-0.144 pp` | inferentially insufficient | `+0.070` | `55.6%` | 逆方向・50日未満 |

`strict_value_long_only`の候補数中央値は1日2銘柄、`value_extension_long_only`は1日1銘柄だった。top/bottom各側の中央値は2銘柄で、事前gateの5銘柄に届かない。第二familyの9日では20-session blockが系列全体を覆うためbootstrap分布が退化する。その数値を有意なCIとは読まず、`insufficient_sample`を優先する。

期間別にも、2017–2021でcontinuous 20D比較を成立させたfamilyはなかった。`strict_value_long_only`は2022–2023と2024+で全variantが正だったが、`value_extension_long_only`は60Dと合成が両期間で負だった。

| Family | Priority | 2017–2021 | 2022–2023 | 2024+ |
| --- | --- | ---: | ---: | ---: |
| `strict_value_long_only` | fixed 20D | no eligible dates | `+0.995 pp` (48日) | `+2.801 pp` (60日) |
| `strict_value_long_only` | fixed 60D | no eligible dates | `+2.787 pp` (48日) | `+3.377 pp` (60日) |
| `strict_value_long_only` | equal composite | no eligible dates | `+2.955 pp` (48日) | `+3.930 pp` (60日) |
| `value_extension_long_only` | fixed 20D | no eligible dates | `+1.561 pp` (8日) | `+1.993 pp` (1日) |
| `value_extension_long_only` | fixed 60D | no eligible dates | `-1.519 pp` (8日) | `-8.134 pp` (1日) |
| `value_extension_long_only` | equal composite | no eligible dates | `-0.041 pp` (8日) | `-0.965 pp` (1日) |

年次行も`segment_stability`に保存している。2024+はholdoutではなく、1日のrecent値は独立検証とは読めない。

#### `20D<0`の先行仮説は方向として再現した

20D primary outcomeの単純な感度集計では、signal時点の20D returnが非負の群が最も強かった。

| Family | Signal時点20D状態 | Observations | Mean forward 20D TOPIX excess |
| --- | --- | ---: | ---: |
| `strict_value_long_only` | `20D >= 0` | 2,502 | `+3.370%` |
| `strict_value_long_only` | `-10% < 20D < 0` | 717 | `+1.335%` |
| `strict_value_long_only` | `20D <= -10%` | 361 | `-0.634%` |
| `value_extension_long_only` | `20D >= 0` | 687 | `+1.922%` |
| `value_extension_long_only` | `-10% < 20D < 0` | 313 | `+0.247%` |
| `value_extension_long_only` | `20D <= -10%` | 134 | `+0.321%` |

これはsame-date priority gateではなく感度集計だが、「20D<0はlong候補のexcess returnを悪くしやすい」という先行結果と整合する。deep pullbackはfamilyで符号が分かれ、単一の反転ルールにはできない。

#### `++` badgeは有望だが採用できない

`strict_value_long_only`では、20D/60Dがともに正の`++`は`+-`より`+2.843 pp`（28 paired dates、CI `[+0.366,+5.320]`）、`-+`より`+2.153 pp`（35 dates、CI `[+0.470,+3.875]`）だった。

一方、`value_extension_long_only`では`++ - -+`が10日だけで、`++ - +-`は最小cell数を満たす比較が作れなかった。2つの必要contrastを2 familyで再現するbadge gateは不通過である。

#### Top-Kは合成が最も良いが、運用gateを満たさない

| Priority | K | Eligible dates | Mean lift vs eligible basket | Severe-loss差 | Sector HHI差 |
| --- | ---: | ---: | ---: | ---: | ---: |
| fixed 20D | 5 | 145 | `+0.263 pp` | `+0.45 pp` | `+0.039` |
| fixed 20D | 10 | 40 | `+0.062 pp` | `-0.02 pp` | `-0.073` |
| fixed 60D | 5 | 145 | `+0.460 pp` | `+0.18 pp` | `+0.047` |
| fixed 60D | 10 | 40 | `+0.374 pp` | `+0.23 pp` | `-0.057` |
| equal composite | 5 | 145 | `+0.769 pp` | `+0.04 pp` | `+0.058` |
| equal composite | 10 | 40 | `+0.146 pp` | `+0.23 pp` | `-0.072` |

点推定は全行正だが、K=5ではsector concentrationが悪化し、leave-one-family-outでは方向が安定しない。Top-K単独でcontinuous family replication不足を上書きしない。

### Interpretation

- fixed 20D/60Dは、少なくともstrict Deep Value long群では無情報ではない。
- その中では20D単独より60D、さらに等ウェイト合成の点推定が強かった。
- ただし第二familyでは60Dと合成が逆方向で、比較可能日も9日しかない。
- よって「fixed 20D/60Dの方が明らかに優秀」とも「不要」とも言えない。
- 現時点のRanking優先度変更は見送る。これは`reject`ではなく`insufficient_evidence`である。

N225-excess感度ではsame-date top-bottom比較からbenchmark returnが相殺されるため、TOPIXと同じpriority liftになる。sector equal-weight、bank除外、liquidity z帯、`>=0`境界、date fixed-effect回帰も感度表に保存したが、primary gateには使っていない。

### Production Implication

- production Rankingの優先度カラムを追加・削除・置換しない。
- `++` badgeを追加しない。
- raw fixed 20D/60Dはinformational表示として維持可能だが、優先度有効性を確定したとは扱わない。
- 次の有効なresearchは、fixed-free候補の独立familyを広げつつ期待returnを保ち、第二familyで50 paired datesと各側5銘柄を確保すること。今回のgateや結果を変更せず別experimentとして行う。

### Caveats

- observation-level forward returnであり、portfolio performance、turnover、cost、capacity、executionを示さない。
- 2024+はholdoutではない。
- long scaffoldが強く狭いため、全観測は4,769でもsame-date top/bottom比較は大きく減る。
- `value_extension_long_only`の小標本CIは推論に使えない。
- incomplete forward windowsは除外した。latest signal dateはfamily/horizonごとに異なる。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_ranking_fixed_return_priority_evidence.py`
- Module: `apps/bt/src/domains/analytics/ranking_fixed_return_priority_evidence.py`
- Tests: `apps/bt/tests/unit/domains/analytics/test_ranking_fixed_return_priority_evidence.py`
- Durable bundle: `/Users/mirage/.local/share/trading25/research/market-behavior/ranking-fixed-return-priority-evidence/20260718_prime_pit_fixed_return_priority_v5/`
- Manifest: `/Users/mirage/.local/share/trading25/research/market-behavior/ranking-fixed-return-priority-evidence/20260718_prime_pit_fixed_return_priority_v5/manifest.json`
- Results: `/Users/mirage/.local/share/trading25/research/market-behavior/ranking-fixed-return-priority-evidence/20260718_prime_pit_fixed_return_priority_v5/results.duckdb`
- Summary: `/Users/mirage/.local/share/trading25/research/market-behavior/ranking-fixed-return-priority-evidence/20260718_prime_pit_fixed_return_priority_v5/summary.md`
- Bundle tables: `coverage_attrition`, `scaffold_registry`, `continuous_priority_lift`, `fixed_2x2_daily`, `fixed_incremental_contrast`, `topk_priority_lift`, `segment_stability`, `bootstrap_effect_ci`, `regression_sensitivity`, `decision_gate`, `observation_sample`
- Provenance: manifest git commit `59b96a6b0e5def3c336ac25354adcd94d4a2b309`; `git_dirty=true`はユーザー所有の`.gitignore`だけがrun時に未commitだったためである。

Reproduce:

```bash
uv run --directory apps/bt python \
  scripts/research/run_ranking_fixed_return_priority_evidence.py \
  --db-path ~/.local/share/trading25/market-timeseries/market.duckdb \
  --start-date 2017-01-01 \
  --bootstrap-resamples 2000 \
  --bootstrap-seed 31 \
  --run-id 20260718_prime_pit_fixed_return_priority_v5
```
