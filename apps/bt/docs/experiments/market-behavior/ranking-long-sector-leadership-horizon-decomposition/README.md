# Ranking Long Sector Leadership Horizon Decomposition

## Published Readout

### Decision

Long側の `Momentum Value + Balanced Sector Strength: Strong` は、`Balanced Sector Strength` だけで評価するのではなく、`Long Sector Leadership` を別軸として選択・比較できるようにする。長期 sector leadership は ex Banks では改善を示し、all sectors でも 20D は既存 baseline と概ね同等、60D は改善する。ただし short側の `Balanced Sector Strength: Weak` は `Balanced Sector Strength` が機能しているため置換しない。

したがって完全置換ではなく、Daily Ranking / research runner で sector strength family を `Balanced Sector Strength` と `Long Sector Leadership` から選択できる二層化を次の実装候補にする。次の検証は、sector strength の式をさらに足すより、sector cap / sector-balanced portfolio lens で「銀行が leader であることを認めつつ、銀行以外でも return source が残るか」を見る。

### Main Findings

#### 結論1: 2022-2026 の ex Banks では長期 leadership がBalanced Sector Strength を上回る

`Undervalued + 20/60D Momentum` に対して、`Balanced Sector Strength: Strong` と PIT 長期 sector leadership を比較した。数値は年次 median を observation count で加重した期間集計。

| period | horizon | sector scope | overlay | obs | median raw | median TOPIX excess | win rate | bank share | future top5 share |
| --- | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2022-2026 | 20D | ex Banks | Balanced Sector Strength: Strong | 1,412 | 1.24% | 0.02% | 49.9% | 0.0% | 18.9% |
| 2022-2026 | 20D | ex Banks | Long Hybrid Leadership | 1,257 | 2.65% | 1.62% | 57.6% | 0.0% | 27.8% |
| 2022-2026 | 60D | ex Banks | Balanced Sector Strength: Strong | 1,385 | 5.77% | 1.30% | 52.3% | 0.0% | 18.0% |
| 2022-2026 | 60D | ex Banks | Long Hybrid Leadership | 1,245 | 8.46% | 4.26% | 60.3% | 0.0% | 27.1% |

#### 結論2: all sectors では長期 leadership も銀行集中を解けない

2022-2026 の all sectors では `Long Hybrid Leadership` の成績は強いが、bank share は約75%で、`Balanced Sector Strength: Strong` と同じ問題を残す。

| period | horizon | overlay | obs | median raw | median TOPIX excess | win rate | bank share | future top5 share |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2022-2026 | 20D | Balanced Sector Strength: Strong | 5,630 | 3.76% | 2.53% | 64.5% | 74.9% | 4.7% |
| 2022-2026 | 20D | Long Hybrid Leadership | 5,053 | 4.42% | 2.40% | 64.4% | 75.1% | 6.9% |
| 2022-2026 | 60D | Balanced Sector Strength: Strong | 5,524 | 4.85% | 1.75% | 54.5% | 74.9% | 4.5% |
| 2022-2026 | 60D | Long Hybrid Leadership | 5,000 | 8.21% | 3.30% | 58.0% | 75.1% | 6.7% |

この結果は、銀行込みで `Long Hybrid Leadership` が既存 baseline より劣るという意味ではない。20D TOPIX excess は `Balanced Sector Strength` がわずかに上だが、raw return と60Dでは `Long Hybrid Leadership` が上回る。銀行集中は欠陥というより、銀行が実際に leader sector だった局面を正しく拾っている。ただし score を一本化すると short側の `Balanced Sector Strength: Weak` の有効性まで壊すため、置換ではなく選択可能化が妥当。

#### 結論3: 2016-2021 は 60D では改善するが、20D は不安定

2016-2021 の ex Banks では 60D の長期 leadership が改善する。一方、20D は `Long Hybrid Leadership` が悪く、2020 のコロナ局面で大きく崩れる。短期 timing signal としてはまだ弱い。

| period | horizon | sector scope | overlay | obs | median raw | median TOPIX excess | win rate | bank share | future top5 share |
| --- | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2016-2021 | 20D | ex Banks | Balanced Sector Strength: Strong | 1,004 | 1.91% | -0.36% | 48.8% | 0.0% | 25.4% |
| 2016-2021 | 20D | ex Banks | Long Hybrid Leadership | 274 | -0.26% | -0.84% | 44.9% | 0.0% | 57.3% |
| 2016-2021 | 60D | ex Banks | Balanced Sector Strength: Strong | 1,004 | 4.37% | 0.78% | 52.2% | 0.0% | 25.4% |
| 2016-2021 | 60D | ex Banks | Long Hybrid Leadership | 274 | 1.99% | 4.27% | 51.5% | 0.0% | 57.3% |
| 2016-2021 | 60D | ex Banks | Balanced Strong + Long Hybrid Leadership | 89 | 17.38% | 18.76% | 73.0% | 0.0% | 75.3% |

#### 結論4: 年次では 2022/2023/2025 の ex Banks 改善が主な根拠、2024/2026 はまだ銀行依存

| year | horizon | sector scope | Balanced Sector Strength: Strong excess | Long Hybrid Leadership excess | Long Hybrid obs | Long Hybrid bank share |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| 2022 | 20D | ex Banks | -0.08% | 1.09% | 248 | 0.0% |
| 2023 | 20D | ex Banks | 1.19% | 2.04% | 777 | 0.0% |
| 2024 | 20D | ex Banks | -1.06% | -1.72% | 35 | 0.0% |
| 2025 | 20D | ex Banks | -0.80% | 1.49% | 171 | 0.0% |
| 2026 | 20D | ex Banks | -4.06% | -0.26% | 26 | 0.0% |
| 2022 | 60D | ex Banks | 4.81% | 6.62% | 236 | 0.0% |
| 2023 | 60D | ex Banks | 1.25% | 2.85% | 777 | 0.0% |
| 2024 | 60D | ex Banks | -5.23% | -0.96% | 35 | 0.0% |
| 2025 | 60D | ex Banks | 2.26% | 9.74% | 171 | 0.0% |
| 2026 | 60D | ex Banks | -12.46% | -3.84% | 26 | 0.0% |

#### 結論5: 原因分解では「Balanced-only が悪い」と「Long-only が良い」の両方が効いている

2026-07-07 までの local `market.duckdb` で `balanced_long_switch_attribution_df` を追加し、`Balanced Sector Strength: Strong` から `Long Hybrid Leadership: Strong` へ切り替えたときの差分を、共通採用・Long Hybrid で落とす側・Long Hybrid で拾う側に分けた。ここで `Long not strong` は `long_hybrid_leadership_score < 0.8`、`Balanced not strong` は `sector_strength_bucket != sector_strong` を指す。

ex Banks では、`Balanced Strong` だが `Long not strong` の dropped 側は 20D/60D とも median TOPIX excess が負で、`Balanced not strong` だが `Long Strong` の added 側は 20D/60D とも正だった。したがって、原因は片方だけではない。ただし「Long Hybrid が拾う added 側が良い」効果の方が 60D では大きく、`Balanced Strong` を必須にするとこの return source を捨てる。

| period | sector scope | switch group | obs (20D / 60D) | date baskets (20D / 60D) | 20D obs median excess | 20D date median excess | 60D obs median excess | 60D date median excess |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2022-2026 | ex Banks | Both strong | 700 / 695 | 295 / 292 | +0.85% | +0.82% | +4.32% | +5.59% |
| 2022-2026 | ex Banks | Balanced strong, Long not strong | 743 / 710 | 416 / 403 | -0.91% | -0.59% | -1.87% | -4.05% |
| 2022-2026 | ex Banks | Balanced not strong, Long strong | 558 / 549 | 300 / 298 | +2.19% | +2.52% | +4.36% | +7.65% |
| 2022-2026 | all sectors | Both strong | 3,630 / 3,592 | 479 / 460 | +2.26% | +1.51% | +2.10% | +4.00% |
| 2022-2026 | all sectors | Balanced strong, Long not strong | 2,034 / 1,949 | 499 / 486 | +2.40% | +0.44% | +0.92% | -0.13% |
| 2022-2026 | all sectors | Balanced not strong, Long strong | 1,427 / 1,406 | 481 / 470 | +2.47% | +2.35% | +6.59% | +7.05% |

`all sectors` の both strong は観測不足で落ちたのではなく、前表では mismatch 2群を強調するため省略していた。実際には both strong も十分厚いが、bank share が 20D/60D とも約81%あるため、銀行込みの headline だけで判断すると差が鈍る。`all sectors` では dropped 側も observation median はプラスに見えるが、date-level basket では 20D が弱く、60D はマイナスになるため、採用判断は ex Banks と date-level basket を必ず併用する。

全期間で見ると、balanced が long hybrid に負ける理由は概ね同じ。ex Banks の `Balanced strong / Long not strong` は 20D date median `-0.50%`、60D `-0.40%` と弱く、`Balanced not strong / Long strong` は 20D `+1.08%`、60D `+5.78%` と強い。つまり全期間では「balanced-only を落とす」効果と「long-only を拾う」効果の両方が残る。

ただし 2016-2021 だけでは短期20Dの理由は変わる。ex Banks の `Balanced not strong / Long strong` は 20D date median `-0.95%` で、`Balanced strong / Long not strong` の `-0.42%` より悪い。一方 60D では `Balanced not strong / Long strong` が `+1.54%`、`Balanced strong / Long not strong` が `+1.39%` で小幅に上回る。したがって pre-2022 は「20Dでもlong-onlyが良い」という話ではなく、long hybrid の優位は主に 60D holding 側に寄る。2022-2026 では20D/60Dともに long-only added 側がはっきり強くなり、ここが近年の結論を支えている。

#### 結論6: Long Strong なら Balanced Neutral/Weak を即除外しないが、`<0.2` は exception review に留める

2026-07-07 時点の latest sector state では、`非鉄金属` は `long_hybrid_leadership_score=0.961` だが `sector_strength_score=0.069` で、`Long Strong / Balanced Weak` の極端な乖離になっている。この形が買い許容できるかを見るため、`Long Strong` 内で `sector_strength_score` を band 分解した。

2022-2026 の ex Banks では、`0.2..0.4` は 20D/60D とも非常に強く、`0.4..0.8` や `>=0.8` よりむしろ良い。これは「長期 leadership は強いが balanced current score がまだ追いついていない」押し目・回復初動として買い許容できる。ただし `0.2..0.4` の実体は主に `鉄鋼` / `鉱業` で、sector generalization はまだ限定的。

| period | sector scope | balanced score band | obs | date baskets | 20D date median excess | 60D date median excess | severe loss 20D / 60D |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| 2022-2026 | ex Banks | `<0.2` | 12 / 12 | 10 / 10 | +6.53% | +12.25% | 0.0% / 0.0% |
| 2022-2026 | ex Banks | `0.2..0.4` | 58 / 58 | 46 / 46 | +5.49% | +12.16% | 0.0% / 3.4% |
| 2022-2026 | ex Banks | `0.4..0.6` | 124 / 123 | 85 / 84 | +1.00% | +8.83% | 2.4% / 3.3% |
| 2022-2026 | ex Banks | `0.6..0.8` | 364 / 356 | 193 / 192 | +1.61% | +5.04% | 5.2% / 11.8% |
| 2022-2026 | ex Banks | `>=0.8` | 700 / 695 | 295 / 292 | +0.82% | +5.59% | 8.6% / 24.5% |

`<0.2` は数値だけなら最も強いが、2022-2026 ex Banks で 12 observations / 10 dates / 2 sectors しかない。業種内訳も `鉄鋼` 11 obs と `鉱業` 1 obs で、`非鉄金属` の exact historical analog ではない。したがって `sector_strength_score < 0.2` は hard reject ではないが、production では `small-size exception / review required` とする。買い許容の実務線は `0.2..0.4` 以上、`<0.2` は Long Hybrid が極端に高い、個別銘柄の value / momentum scaffold が維持される、かつ sector exposure を小さくする場合だけ許容する。

全期間（local DB では 2016-06-13 から 2026-07-07）に広げても、方向は変わらない。ex Banks の `0.2..0.4` は 76 obs / 64 dates / 8 sectors まで増え、20D date median `+3.20%`、60D date median `+10.85%` を維持する。`<0.2` も 21 obs / 19 dates / 3 sectors で 20D `+3.69%`、60D `+11.54%` だが、依然として薄い。したがって all-period でも `0.2..0.4` は買い許容、`<0.2` は exception review という線引きは維持する。ただし 2016-2021 単独では `0.2..0.4` の20D date median は `+0.94%` まで落ち、date-level IR は弱い一方、60D は `+4.18%` を保つため、古い局面まで含めると短期 entry signal というより 60D holding candidate として読む。

### Interpretation

長期 sector leadership は、短期の `Balanced Sector Strength` とは違う情報を持っている。特に ex Banks では、2022-2026 の 20D/60D ともに `Balanced Sector Strength: Strong` より改善したため、「銀行以外から return を得る」方向の candidate として価値がある。

ただし、all sectors の `Long Hybrid Leadership` は 2025/2026 でほぼ銀行業になる。これは「銀行業が長期 winner として認識され続ける」こと自体を捕まえているだけで、銀行 beta を超える score とは言えない。銀行業を抜かずに採用すると、`Balanced Sector Strength: Strong` と同じ concentration 問題を再生産する。

2016-2021 では 60D の改善が見える一方、20D は不安定で、2018/2020 のような局面では長期 winner 追随が短期で逆風になる。したがって daily Ranking の long-side confidence overlay に直結するより、holding horizon / rebalance horizon を分ける必要がある。

switch attribution の読みは、`Balanced Strong` を long 側の必須 gate にしないことを支持する。`Balanced Strong & Long not strong` は ex Banks で避けたい低品質領域になり、`Balanced not strong & Long strong` は 20D/60D の追加候補になっているため、long 側では Long Hybrid を主判定、Balanced は補助診断として扱う方が自然。

Balanced score band の読みは、`Long Strong` が成立している限り、balanced が neutral から weak に落ちたことだけでは買いを止めない。特に `0.2..0.4` は許容可能な pullback / early recovery bucket として扱える。一方、`<0.2` は過去成績が良くても薄く、latest `非鉄金属` のような extreme divergence は entry permission ではなく exception review として扱う。

### Production Implication

現時点で production Ranking の `Balanced Sector Strength` を完全置換しない。候補として残すのは、次の3つ。

1. `Long Sector Leadership` は long-side research candidate として継続する。
2. `Balanced Sector Strength` family を `Balanced Sector Strength` / `Long Sector Leadership` から選択可能にする。long側は後者を優先候補、short側は前者を維持する。
3. 採用前に sector cap / sector-balanced portfolio lens を必須にする。とくに銀行業を除外するのではなく、銀行業の自然な強さを認めつつ、1セクター集中で headline return が決まらない構造にする。

short側はこの readout の対象外。既存の `Overvalued + Momentum + Balanced Sector Strength: Weak` は、`Balanced Sector Strength` で十分に機能しているという前回結論を維持する。

### Caveats

- 2026 は anchor end date が 2026-05-14 の partial year。
- forward return は daily close-to-close の forward outcome で、portfolio construction、turnover、cost、capacity は未反映。
- `future top5 share` は、過去 discussion で確認した 2016-2026 の事後 sector winner 診断であり、signal には使っていない。
- 長期 leadership は 120/252/504D の過去 sector return rank だけで作り、future sector ranking は使っていない。
- `Long Hybrid Leadership` は sector-level bucket なので、sector内の銘柄選別や position cap は未検証。
- ATR20 Accel の補足診断では、pre-2022 TSE1 (`0101`) を `prime` に正規化しない market SoT bug が見つかった。`recent_return_threshold_forward_response._market_master_cte` を shared `MARKET_CODES_BY_SCOPE` に寄せて修正した後、2018-2021 の `Undervalued + ATR20 Accel + Long Hybrid Leadership` は 20D TOPIX excess -0.17%、60D +2.17% だった。ATR20 Accel は 20D の補助候補だが、60D では Momentum 20/60 の方が強い。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_ranking_long_sector_leadership_horizon_decomposition.py`
- Domain: `apps/bt/src/domains/analytics/ranking_long_sector_leadership_horizon_decomposition.py`
- Test: `apps/bt/tests/unit/domains/analytics/test_ranking_long_sector_leadership_horizon_decomposition.py`
- Bundle: `/tmp/trading25-research/market-behavior/ranking-long-sector-leadership-horizon-decomposition/20260604_ranking_long_sector_leadership_prime_v1/`
- Supplemental switch-attribution bundle: `/tmp/trading25-research/market-behavior/ranking-long-sector-leadership-horizon-decomposition/20260709_balanced_long_switch_attribution/`
- Balanced tolerance bundles: `/tmp/trading25-research/market-behavior/ranking-long-sector-leadership-horizon-decomposition/20260709_long_hybrid_balanced_tolerance/`, `/tmp/trading25-research/market-behavior/ranking-long-sector-leadership-horizon-decomposition/20260709_long_hybrid_balanced_tolerance_min50/`, `/tmp/trading25-research/market-behavior/ranking-long-sector-leadership-horizon-decomposition/20260709_long_hybrid_balanced_tolerance_min1/`
- Tables: `annual_overlay_summary_df`, `bank_concentration_df`, `sector_contribution_df`, `leadership_horizon_df`, `balanced_vs_long_matrix_df`, `balanced_long_switch_attribution_df`, `long_hybrid_balanced_tolerance_df`, `future_top5_diagnostic_df`, `overlay_comparison_df`

## Method

Base universe is Prime/TSE1 SoT resolved by `stock_master_daily_exact_date`. The base long condition is:

- `Undervalued`: same Daily Ranking terminology, implemented as PBR percentile <= 20% and forward PER percentile <= 20%.
- `20/60D Momentum`: both 20D and 60D recent return percentiles >= 80%.

Sector overlays are compared as:

- no sector overlay
- `Balanced Sector Strength: Strong`
- `Long Index Leadership`: past 120/252/504D sector index TOPIX-excess rank
- `Long Constituent/Breadth Leadership`: past 120/252/504D constituent TOPIX-excess rank and breadth rank
- `Long Hybrid Leadership`: average of index and constituent/breadth leadership
- crosses of `Balanced Sector Strength` and `Long Hybrid Leadership`

All long leadership inputs are anchor-date PIT past-return inputs only.
