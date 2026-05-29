# Ranking Sector Strength Evidence

Daily Ranking の `crowded_rerating` / `neutral_rerating` green/blue を、PIT 33セクター強弱で分解する研究。

## Published Readout

### Decision

Prime では 33セクター強弱よりも、まず value confirmation の有無で Daily Ranking green/blue を分けるべき。`neutral_rerating blue` を broad に平均すると弱く見えるが、これは `no_value_confirmation` が大量に混ざるため。`neutral_rerating blue` のうち `low_pbr20_low_fwd_per20` は 20D TOPIX excess / sector excess の両方で強く、弱セクターでも残る。

Product rule はまだ変更しないが、Sector overlay を考える前に `value_confirmation_tier` を UI / research の主要説明軸として扱う。Sector は `no_value_confirmation` を救済しない。一方で strong value の `neutral blue` は `sector_weak_consistent` でも 20D sector excess median がプラスなので、弱セクターだけで落とすのは早い。

Sector を使うなら、`value_condition x sector` の交差で使う。broad な `neutral blue + sector_weak` だけを見て downrank すると、strong value の良い subset まで落とす。

### Main Findings

#### 結論

Primary v2 run `20260529_ranking_sector_strength_prime_value_v2` は `2016-04-01` から `2026-05-14`、Prime、horizon `5/10/20/60`。観測数は `536,579`、code `1,719`、date `944`、33セクター coverage は `100%`。`color_sector_interaction_df` と `sector_excess_interaction_df` は `value_confirmation_tier` / `value_condition` を必須軸にした。

#### 結論: neutral blue は value confirmation で別物になる

`color_sector_interaction_df`、`neutral_rerating blue`、20D TOPIX excess。

| Value condition | Sector | Obs | Median | Win rate | Severe loss | Read |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `low_pbr20_low_fwd_per20` | `sector_strong + mixed` | 1,092 | +2.409% | 69.05% | 0.92% | 最良。sector strong だが伸び切りではない |
| `low_pbr20_low_fwd_per20` | `sector_strong_consistent` | 10,501 | +1.716% | 61.63% | 3.23% | 強い |
| `low_pbr20_low_fwd_per20` | `sector_weak_consistent` | 1,879 | +1.127% | 60.09% | 1.60% | 弱セクターでも残る |
| `low_pbr20_only` | `sector_strong_consistent` | 11,267 | +1.183% | 56.72% | 5.12% | medium value は sector tailwind が効く |
| `low_pbr20_only` | `sector_weak_consistent` | 3,363 | -0.574% | 45.79% | 6.24% | medium value は弱セクターで落ちる |
| `low_per20_fwdper_per_lte_1_0` | `sector_strong_consistent` | 1,310 | +0.595% | 54.12% | 4.66% | やや良い |
| `no_value_confirmation` | `sector_strong + mixed` | 6,185 | -0.442% | 47.36% | 6.31% | sector strong でも弱い |
| `no_value_confirmation` | `sector_weak_consistent` | 30,995 | -1.295% | 41.96% | 9.90% | 最も避けたい |

#### 結論: strong value の neutral blue は sector excess でも残る

`sector_excess_interaction_df`、`neutral_rerating blue`、20D。`SectorEx` は個別銘柄 - 同33セクター平均。

| Value condition | Sector | Obs | SectorEx median | SectorEx win | Read |
| --- | --- | ---: | ---: | ---: | --- |
| `low_pbr20_low_fwd_per20` | `sector_weak_consistent` | 1,879 | +0.928% | 58.75% | 弱セクター内でも個別優位が残る |
| `low_pbr20_low_fwd_per20` | `sector_strong + mixed` | 1,092 | +0.767% | 56.23% | 強い |
| `low_pbr20_low_fwd_per20` | `sector_neutral + mixed` | 19,067 | +0.204% | 52.01% | まだ残る |
| `low_pbr20_only` | `sector_weak_consistent` | 3,363 | +0.018% | 50.22% | sector excess はほぼ中立 |
| `no_value_confirmation` | `sector_weak_consistent` | 30,995 | -0.174% | 48.77% | 個別優位なし |
| `no_value_confirmation` | `sector_strong_consistent` | 42,406 | -0.816% | 43.94% | 強セクター内でも劣後 |

#### 結論: green は引き続き強い

Green は全て `strong_value_confirmation`。20D では `neutral_rerating green` の `low_per20_fwdper_per_lte_0_8` が `sector_strong_consistent` で median `+3.112%`、`sector_neutral` で `+1.249%`。`crowded_rerating green` は `low_pbr20_low_fwd_per20` で median `+0.543%` から `+0.610%`、`low_per20_fwdper_per_lte_0_8` で `+2.938%`。green は強いが、crowded green は tail caution を残す。

### Interpretation

今回の主眼は、セクター強弱が Ranking green/blue の説明変数なのか、個別シグナルの上乗せなのかを分けることだった。v2 では、まず value confirmation が第一軸で、sector は第二軸だと分かった。

`neutral_rerating blue` は broad には弱いが、`low_pbr20_low_fwd_per20` だけは強い。これは `neutral blue` の中で「実質的に strong value blue」と呼ぶべき subset。弱セクターでも TOPIX excess / sector excess が残るため、sector weak を理由にこの subset を落とすのは避ける。

一方で `no_value_confirmation` は sector strong でも弱く、sector は救済条件にならない。`low_pbr20_only` や `low_per20_fwdper_per_lte_1_0` は medium value で、sector strong ならやや良いが、弱セクターでは鈍る。

### Production Implication

Daily Ranking UI では、`neutral_rerating` の broad blue を value tier で分解する。`neutral_rerating` は green 以外を一括 blue にせず、strong value blue / medium light blue / no-value neutral に分ける。

Sector overlay はこの後の補助軸とし、まず `value_confirmation_tier` を表示色へ反映する。

| Candidate classification | 条件 | 意味 |
| --- | --- | --- |
| `Strong value blue` | `neutral_rerating blue` + `low_pbr20_low_fwd_per20` | blue 内の本命。弱セクターでも落としすぎない |
| `Medium value blue` | `low_pbr20_only` / `low_per20_fwdper_per_lte_1_0` | sector strong なら補強、sector weak なら caution |
| `No-value neutral` | `no_value_confirmation` | sector strong でも弱い。blue ではなく gray/neutral |
| `Sector tailwind` | `sector_strong_consistent` | medium value の補助。strong value の必須条件ではない |
| `Sector headwind` | `sector_weak_consistent` | no-value / medium value の caution。strong value には弱めに適用 |

Ranking 表示では `neutral blue` を value tier で分ける。Sector はその後の badge/overlay が妥当。

### Caveats

- Primary universe は Prime。
- 33セクター分類は `stock_master_daily.date = target date` の exact-date PIT join のみを primary とする。
- セクターbasketの forward return は同日同セクター構成銘柄の等ウェイト平均で、cost / capacity / turnover は含まない。
- Ranking の green/blue 定義は既存 `ranking-color-evidence` の UI evidence layer であり、portfolio rule ではない。

### Source Artifacts

- Domain: `apps/bt/src/domains/analytics/ranking_sector_strength_evidence.py`
- Runner: `apps/bt/scripts/research/run_ranking_sector_strength_evidence.py`
- Tests: `apps/bt/tests/unit/domains/analytics/test_ranking_sector_strength_evidence.py`
- Bundle v1 broad: `/private/tmp/trading25-research/market-behavior/ranking-sector-strength-evidence/20260529_ranking_sector_strength_prime_v1/`
- Bundle v2 value-confirmed: `/private/tmp/trading25-research/market-behavior/ranking-sector-strength-evidence/20260529_ranking_sector_strength_prime_value_v2/`

## Design

- `ranking_color_evidence` の fast daily panel を再利用し、`daily_valuation` と既存 liquidity/value color semantics を SoT にする。
- `ranking_sector_daily_state` は `market_scope x date x sector_33_name` で1回だけ作る。
- `color_sector_interaction_df` / `sector_excess_interaction_df` は `value_confirmation_tier` と非重複 `value_condition` を必ず含む。
- `sector_strength_score` は `sector_20d_topix_excess_pct`、`sector_60d_topix_excess_pct`、`sector_breadth_20d_pct` の within-date rank 平均。
- `sector_strength_bucket` は score 上位20%を `sector_strong`、下位20%を `sector_weak`、それ以外を `sector_neutral` とする。
- `color_sector_interaction_df` は TOPIX excess outcome、`sector_excess_interaction_df` は同33セクター平均との差分 outcome を返す。

## Run

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_ranking_sector_strength_evidence.py \
  --start-date 2016-04-01 \
  --end-date 2026-05-14 \
  --horizons 5,10,20,60 \
  --markets prime \
  --run-id 20260529_ranking_sector_strength_prime_value_v2
```
