# Ranking Good Forward Valuation Chain

## Published Readout

### Decision

Daily Ranking の `Neutral Good` / `Crowded Good` に限定し、`PER > Fwd PER > Fwd P/OP` という絶対値 chain が追加の優秀銘柄抽出条件になるかを検証した。

結論として、この chain は Good 全体や `Neutral Good` の hard filter にはしない。`Crowded Good` では改善するが、中央値は小さく左尾も重いため、使うなら補助 badge / tie-breaker に留める。

### Main Findings

#### 結論: Good 全体では chain 条件がむしろ弱い

`20260531_ranking_good_forward_valuation_chain_prime_v2` は、Ranking API の Good 判定に合わせた `rerating_good_valuation_chain_df` を追加し、Prime-only の 5D/10D/20D/60D close-to-close TOPIX excess return を比較した。chain は `PER > Fwd PER > Fwd P/OP` かつ3指標すべて正で定義した。

20D では、Good 全体で chain 条件は observation `16,394`、median `+0.690%`、win rate `54.52%`。Good 全体の `+0.971%` / `56.41%`、chain 以外の `+1.122%` / `57.27%` を下回る。

| Good scope | Chain condition | Observation | Mean | Median | Win rate | Severe loss | Read |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| all Good | all | 52,645 | +1.755% | +0.971% | 56.41% | 4.42% | base |
| all Good | `PER > Fwd PER > Fwd P/OP` | 16,394 | +1.414% | +0.690% | 54.52% | 3.98% | base より弱い |
| all Good | without chain | 36,251 | +1.909% | +1.122% | 57.27% | 4.62% | chain より強い |

#### 結論: Neutral Good では chain なしの方が強い

`Neutral Good` は chain 条件で悪化する。20D では chain が median `+0.762%` / win rate `55.52%`、without chain が `+1.348%` / `59.41%`。60D でも chain は median `+1.591%` / severe loss `14.17%`、without chain は `+3.004%` / `11.75%`。

| Horizon | Chain condition | Observation | Mean | Median | Win rate | Severe loss |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 20D | all | 40,804 | +1.767% | +1.152% | 58.22% | 2.84% |
| 20D | `PER > Fwd PER > Fwd P/OP` | 12,499 | +1.289% | +0.762% | 55.52% | 2.74% |
| 20D | without chain | 28,305 | +1.978% | +1.348% | 59.41% | 2.89% |
| 60D | all | 39,814 | +4.371% | +2.593% | 58.89% | 12.49% |
| 60D | `PER > Fwd PER > Fwd P/OP` | 12,125 | +3.076% | +1.591% | 55.46% | 14.17% |
| 60D | without chain | 27,689 | +4.938% | +3.004% | 60.39% | 11.75% |

#### 結論: Crowded Good では改善するが、tail は重い

`Crowded Good` では chain 条件が改善する。20D では chain が median `+0.272%` / severe loss `7.96%`、without chain が `-0.089%` / `10.76%`。60D でも chain が median `+0.109%` / severe loss `23.65%`、without chain が `-0.200%` / `26.46%`。ただし absolute level と left tail はまだ強い採用条件と呼ぶには弱い。

| Horizon | Chain condition | Observation | Mean | Median | Win rate | Severe loss |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 20D | all | 11,841 | +1.712% | +0.047% | 50.19% | 9.84% |
| 20D | `PER > Fwd PER > Fwd P/OP` | 3,895 | +1.815% | +0.272% | 51.30% | 7.96% |
| 20D | without chain | 7,946 | +1.662% | -0.089% | 49.65% | 10.76% |
| 60D | all | 11,517 | +4.213% | -0.113% | 49.74% | 25.54% |
| 60D | `PER > Fwd PER > Fwd P/OP` | 3,772 | +4.709% | +0.109% | 50.37% | 23.65% |
| 60D | without chain | 7,745 | +3.972% | -0.200% | 49.44% | 26.46% |

### Interpretation

`PER > Fwd PER > Fwd P/OP` は直感的には「利益予想も営業利益評価も改善している」ように見えるが、Ranking Good の中では universal な上位抽出条件ではなかった。特に `Neutral Good` では、既存 Good 条件そのものが十分に効いており、chain を足すとむしろ期待値と勝率を落とす。

一方、`Crowded Good` はもともと混雑と左尾riskを含むため、chain が valuation quality の補助確認として働く。とはいえ 20D median は `+0.272%`、60D severe loss は `23.65%` で、単独で採用強度を上げるほどではない。

### Production Implication

`PER > Fwd PER > Fwd P/OP` は `Neutral Good` / Good 全体の追加 hard filter にしない。`Crowded Good` に限って、補助 badge、tie-breaker、または詳細診断として表示する候補に留める。

Ranking の主ルールは既存 `ranking-color-evidence` の value confirmation を維持する。今回の chain は `Fwd P/OP` の読みを深掘りする補助研究であり、`流動性Z` の green/blue 定義を置き換えない。

### Caveats

- Prime-only の UI evidence layer であり、portfolio rule ではない。
- outcome は 5D/10D/20D/60D close-to-close TOPIX excess return。primary read は 20D。
- local `market.duckdb` の coverage は Prime 1,734,958 observations / 1,920 codes / 1,015 dates。
- `rerating_good_valuation_chain_df` は `ranking-color-evidence` runner の追加テーブルとして出力している。

### Source Artifacts

| Artifact | Path |
| --- | --- |
| runner | `apps/bt/scripts/research/run_ranking_color_evidence.py` |
| domain module | `apps/bt/src/domains/analytics/ranking_color_evidence.py` |
| bundle | `~/.local/share/trading25/research/market-behavior/ranking-color-evidence/20260531_ranking_good_forward_valuation_chain_prime_v2` |
| result table | `rerating_good_valuation_chain_df` |
| command | `uv run --project apps/bt python apps/bt/scripts/research/run_ranking_color_evidence.py --horizons 5,10,20,60 --markets prime --run-id 20260531_ranking_good_forward_valuation_chain_prime_v2 --notes "Neutral/Crowded Good restricted PER > FwdPER > Fwd P/OP comparison"` |
