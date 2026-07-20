# Ranking Fixed Return Priority Evidence

## Published Readout

### Decision

**fixed 20D/60D を Ranking 優先度から外すとも、現状のまま有効と認定するとも結論できない。正式判定は `insufficient_evidence` である。**

`strict_value_long_only` では fixed 20D、fixed 60D、等ウェイト合成のすべてが20D forward TOPIX-excess returnを正方向に順位付けした。しかし、独立再現先の `value_extension_long_only` は1日あたりの候補が少なく、連続順位を比較できた日が9日だけだった。そこで60Dと合成は逆方向になった。事前に固定した「2 family・50 paired dates・median focus 5銘柄」の採用gateを満たさないため、production Rankingの優先度カラム、badge、API、UIは変更しない。

この結果は「fixed 20D/60Dに価値がない」という棄却ではない。強い候補群では有望だが、置換・維持を決めるだけの独立再現性がまだない、という結論である。raw 20D/60Dは当面 informational fieldとして維持できるが、本bundle単独では優先度ロジックの根拠にしない。

Published run `20260719_prime_price_pit_fixed_return_priority_v11` は clean source commit `85140932d6b785b41069db3ef7afe1d066cb0d5e` (`git_dirty=false`) から再実行した。authoritative completion date / stock return / TOPIX excessの透過、N225 endpoint alignment、event-time adjustment frontier、Top-K outcome coverageを明示監査したうえで判定は変わらなかった。

| publication metric | value |
| --- | ---: |
| `observation_count` | `4785` |
| `strict_value_observation_count` | `3640` |
| `value_extension_observation_count` | `1145` |

### Research Question

全銘柄の一般的momentumではなく、fixed returnを使わずに抽出した「returnが期待されるlong候補」の内側で、fixed 20D/60Dが次の20D excess returnを順位付けできるかを検証した。

候補群はfixed return、neutral/crowded/stress、`momentum_20_60_top20`、`ex_overheat`、current sector strengthを使う前に確定した。

| Independent family | Fixed-free definition |
| --- | --- |
| `strict_value_long_only` | Deep Value + 120D/252D/504D Long Hybrid Leadership + raw ATR20 acceleration |
| `value_extension_long_only` | equal-weight value score `>=0.8` + 同じLong Hybrid + raw ATR20 acceleration、Deep Valueを除外 |

### Data Scope / PIT

signal dateの`stock_master_daily` exact-date membershipでPrime相当だけを解決した。市場再編前は`0101`、再編後は`0111`で、Standard/Growthは含めない。Market v4のPIT valuation、liquidityを使い、signal close後からforward outcomeを測るafter-close研究である。primaryは20D close-to-close TOPIX-excess return、5D/60Dは補助診断。2024年以降は仮説起点でありholdoutではない。

価格の物理 SoT は `stock_data_raw` であり、`stock_data` fallback は行わない。signal feature は exact signal-date `basis_id` を complete lookback 全体へ適用し、forward outcome は exact completion-date `basis_id` を signal/completion 両 endpoint へ適用した。旧 v1–v10 bundle は immutable archive として保持し、以下のv11 clean rerunをpublication SoTとする。

### Main Findings

#### 結論

v11 でも正式判定は `insufficient_evidence` であり、frozen gate、cohort、parameter、fixed-free membership ordering は変更していない。`strict_value_long_only` の3 priority は正方向だが、median focus 2銘柄で必要な5銘柄に届かず、`value_extension_long_only` は独立再現が不足する。

| Decision key | v11 verdict | Primary reason |
| --- | --- | --- |
| `fixed20_priority` | 不通過 | `insufficient_sample` |
| `fixed60_priority` | 不通過 | `insufficient_sample` |
| `fixed_equal_priority` | 不通過 | `insufficient_sample` |
| `plusplus_badge` | 不通過 | `insufficient_sample` |
| `final_recommendation` | `insufficient_evidence` | 2 family replication gateを満たさない |

#### 連続順位

各signal dateのPrime全銘柄内で20D/60D return percentileを作り、候補family内の上位20%と下位20%を比較した。

| Family | Priority | Eligible dates | Mean 20D top-bottom lift | 95% moving-block CI | Median IC | IC-positive dates | Gate reading |
| --- | --- | ---: | ---: | --- | ---: | ---: | --- |
| `strict_value_long_only` | fixed 20D | 113 | `+2.065 pp` | `[+0.189, +4.217]` | `+0.130` | `61.1%` | 効果は正、focus数不足 |
| `strict_value_long_only` | fixed 60D | 113 | `+3.641 pp` | `[+0.486, +6.786]` | `+0.227` | `74.3%` | 効果は正、focus数不足 |
| `strict_value_long_only` | equal composite | 113 | `+4.050 pp` | `[+1.158, +6.831]` | `+0.224` | `73.5%` | 効果は正、focus数不足 |
| `value_extension_long_only` | fixed 20D | 9 | `+1.609 pp` | inferentially insufficient | `+0.321` | `55.6%` | 50日未満 |
| `value_extension_long_only` | fixed 60D | 9 | `-2.254 pp` | inferentially insufficient | `+0.014` | `55.6%` | 逆方向・50日未満 |
| `value_extension_long_only` | equal composite | 9 | `-0.144 pp` | inferentially insufficient | `+0.070` | `55.6%` | 逆方向・50日未満 |

`strict_value_long_only`の候補数中央値は1日2銘柄、`value_extension_long_only`は1日1銘柄だった。top/bottom各側の中央値は2銘柄で、事前gateの5銘柄に届かない。第二familyの9日では20-session blockが系列全体を覆うためbootstrap分布が退化する。その数値を有意なCIとは読まず、`insufficient_sample`を優先する。

期間別にも、2017–2021でcontinuous 20D比較を成立させたfamilyはなかった。`strict_value_long_only`は2022–2023と2024+で全variantが正だったが、`value_extension_long_only`は60Dと合成が両期間で負だった。

| Family | Priority | 2017–2021 | 2022–2023 | 2024+ |
| --- | --- | ---: | ---: | ---: |
| `strict_value_long_only` | fixed 20D | no eligible dates | `+1.231 pp` (53日) | `+2.801 pp` (60日) |
| `strict_value_long_only` | fixed 60D | no eligible dates | `+3.940 pp` (53日) | `+3.377 pp` (60日) |
| `strict_value_long_only` | equal composite | no eligible dates | `+4.186 pp` (53日) | `+3.930 pp` (60日) |
| `value_extension_long_only` | fixed 20D | no eligible dates | `+1.561 pp` (8日) | `+1.993 pp` (1日) |
| `value_extension_long_only` | fixed 60D | no eligible dates | `-1.519 pp` (8日) | `-8.134 pp` (1日) |
| `value_extension_long_only` | equal composite | no eligible dates | `-0.041 pp` (8日) | `-0.965 pp` (1日) |

年次行も`segment_stability`に保存している。2024+はholdoutではなく、1日のrecent値は独立検証とは読めない。

#### `20D<0`の先行仮説は方向として再現した

20D primary outcomeの単純な感度集計では、signal時点の20D returnが非負の群が最も強かった。

| Family | Signal時点20D状態 | Observations | Mean forward 20D TOPIX excess |
| --- | --- | ---: | ---: |
| `strict_value_long_only` | `20D >= 0` | 2,521 | `+3.322%` |
| `strict_value_long_only` | `-10% < 20D < 0` | 731 | `+1.118%` |
| `strict_value_long_only` | `20D <= -10%` | 366 | `-0.762%` |
| `value_extension_long_only` | `20D >= 0` | 687 | `+1.931%` |
| `value_extension_long_only` | `-10% < 20D < 0` | 317 | `+0.236%` |
| `value_extension_long_only` | `20D <= -10%` | 134 | `+0.321%` |

これはsame-date priority gateではなく感度集計だが、「20D<0はlong候補のexcess returnを悪くしやすい」という先行結果と整合する。deep pullbackはfamilyで符号が分かれ、単一の反転ルールにはできない。

#### `++` badgeは有望だが採用できない

`strict_value_long_only`では、20D/60Dがともに正の`++`は`+-`より`+2.843 pp`（28 paired dates、CI `[+0.366,+5.320]`）、`-+`より`+2.103 pp`（36 dates、CI `[+0.506,+3.723]`）だった。

一方、`value_extension_long_only`では`++ - -+`が10日だけで、`++ - +-`は最小cell数を満たす比較が作れなかった。2つの必要contrastを2 familyで再現するbadge gateは不通過である。

#### Top-Kは合成が最も良いが、運用gateを満たさない

| Priority | K | Eligible dates | Mean lift vs eligible basket | Severe-loss差 | Sector HHI差 |
| --- | ---: | ---: | ---: | ---: | ---: |
| fixed 20D | 5 | 145 | `+0.354 pp` | `+0.16 pp` | `+0.039` |
| fixed 20D | 10 | 40 | `+0.062 pp` | `-0.02 pp` | `-0.073` |
| fixed 60D | 5 | 145 | `+0.551 pp` | `-0.11 pp` | `+0.046` |
| fixed 60D | 10 | 40 | `+0.374 pp` | `+0.23 pp` | `-0.057` |
| equal composite | 5 | 145 | `+0.857 pp` | `-0.25 pp` | `+0.057` |
| equal composite | 10 | 40 | `+0.146 pp` | `+0.23 pp` | `-0.072` |

点推定は全行正だが、K=5ではsector concentrationが悪化し、leave-one-family-outでは方向が安定しない。Top-K単独でcontinuous family replication不足を上書きしない。

### Interpretation

- fixed 20D/60Dは、少なくともstrict Deep Value long群では無情報ではない。
- その中では20D単独より60D、さらに等ウェイト合成の点推定が強かった。
- ただし第二familyでは60Dと合成が逆方向で、比較可能日も9日しかない。
- よって「fixed 20D/60Dの方が明らかに優秀」とも「不要」とも言えない。
- 現時点のRanking優先度変更は見送る。これは`reject`ではなく`insufficient_evidence`である。

v11の`topk_priority_lift`は4,263 complete rowsと3 `incomplete_outcomes` audit rowsを保持した。未完了windowを成績0として混入せず、complete rowsだけで集計しても上記のgate判断は変わらない。`observation_sample`は全4,785 rowsに5D/20D/60Dのcompletion date、stock return、TOPIX excess、completion-date aligned N225 excessを保持する。

N225-excess感度ではsame-date top-bottom比較からbenchmark returnが相殺されるため、TOPIXと同じpriority liftになる。sector equal-weight、bank除外、liquidity z帯、`>=0`境界、date fixed-effect回帰も感度表に保存したが、primary gateには使っていない。

### Production Implication

- production Rankingの優先度カラムを追加・削除・置換しない。
- `++` badgeを追加しない。
- raw fixed 20D/60Dはinformational表示として維持可能だが、優先度有効性を確定したとは扱わない。
- 次の有効なresearchは、fixed-free候補の独立familyを広げつつ期待returnを保ち、第二familyで50 paired datesと各側5銘柄を確保すること。今回のgateや結果を変更せず別experimentとして行う。

### Caveats

- observation-level forward returnであり、portfolio performance、turnover、cost、capacity、executionを示さない。
- 2024+はholdoutではない。
- long scaffoldが強く狭いため、全観測は4,762でもsame-date top/bottom比較は大きく減る。
- `value_extension_long_only`の小標本CIは推論に使えない。
- incomplete forward windowsは効果集計から除外し、Top-Kでは3行を`outcome_status=incomplete_outcomes`、効果metric `NULL`の監査行として保持した。latest signal dateはfamily/horizonごとに異なる。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_ranking_fixed_return_priority_evidence.py`
- Module: `apps/bt/src/domains/analytics/ranking_fixed_return_priority_evidence.py`
- Tests: `apps/bt/tests/unit/domains/analytics/test_ranking_fixed_return_priority_evidence.py`
- Durable bundle: `~/.local/share/trading25/research/market-behavior/ranking-fixed-return-priority-evidence/20260719_prime_price_pit_fixed_return_priority_v11/`
- Manifest: `~/.local/share/trading25/research/market-behavior/ranking-fixed-return-priority-evidence/20260719_prime_price_pit_fixed_return_priority_v11/manifest.json`
- Results: `~/.local/share/trading25/research/market-behavior/ranking-fixed-return-priority-evidence/20260719_prime_price_pit_fixed_return_priority_v11/results.duckdb`
- Summary: `~/.local/share/trading25/research/market-behavior/ranking-fixed-return-priority-evidence/20260719_prime_price_pit_fixed_return_priority_v11/summary.md`
- Bundle tables: `coverage_attrition`, `scaffold_registry`, `continuous_priority_lift`, `fixed_2x2_daily`, `fixed_incremental_contrast`, `topk_priority_lift`, `segment_stability`, `bootstrap_effect_ci`, `regression_sensitivity`, `decision_gate`, `observation_sample`
- Price-PIT audit: canonical raw `9,748,001`、signal features `4,511,414`、outcome requests `13,534,242`、completed outcomes `13,375,258`、signal basis / segments `3,582 / 5,542`、completion basis / segments `3,583 / 4,742`。全count/hashはmanifestとsummaryで一致し、projection SHA-256は`7bd911d7964d924cd21b46cdbf13b349b41b7230ede70304bcc442df80b4235f`、`no_stock_data_fallback=true`、verification statusは`verified`である。
- Provenance: source commit `85140932d6b785b41069db3ef7afe1d066cb0d5e`; `git_dirty=false`。artifact SHA-256はmanifest `2b7e8434b7f39c230b891f5b583741017dff77c24ed2a09cf11461846f1b7386`、results `e0efe3fb055d91caecdb3924c201950d01b7ed03e2e56d0157bc3eecc1e64b1e`、summary `f2b9cc3c1b2fcafb01834dd810ddcbe24d40dda624d7e438edb8fb9a75cf590d`。
- Superseded immutable archives: v1–v10 bundleは削除・上書きせず保持する。

Reproduce:

```bash
uv run --project apps/bt python \
  apps/bt/scripts/research/run_ranking_fixed_return_priority_evidence.py \
  --run-id 20260719_prime_price_pit_fixed_return_priority_v11
```
