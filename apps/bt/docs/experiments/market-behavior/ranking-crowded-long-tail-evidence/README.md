# Ranking Crowded Long Tail Evidence

## Published Readout

### Decision

Daily Ranking の long 側で `Crowded Rerating + Long Hybrid Leadership >= 0.8` を固定し、left tail がどの valuation low10 overlap、ATR/overheat、sector bucket で発生するかを検証する。

この実験の主目的は PSR の有用性検証ではなく、Crowded long scaffold の中で left tail を削る条件と悪化させる条件を総合的に特定することである。PBR / PER / Fwd PER / PSR / Fwd PSR / Fwd P/OP の low10 overlap、ATR/overheat、sector bucket、horizon path を同一 panel 上で比較し、採用優先・caution overlay・sample constrained な分岐を分ける。

### Main Findings

#### 結論

`Crowded Rerating + Long Hybrid Leadership >= 0.8` は平均リターンだけを見ると有効だが、分布は右に歪んでおり、左尾が重い。したがってこの scaffold はそのまま green にするのではなく、valuation confirmation と overheat caution で tail を管理する必要がある。

Prime / 2023-01-01〜2026-03-31 / close-to-close TOPIX excess の scaffold は 6,337 obs / 200 codes / 732 dates。基準 `all_crowded_long_hybrid` は 60D 平均 +4.11%、中央値 -2.43%、p10 -24.84%、CVaR5 -38.68%、severe loss rate 33.0% で、平均は良いが左尾が重い。

left-tail pruning としてもっとも安定しているのは `low10 PBR`、さらに `low10 PBR AND low10 Fwd PER`。この組み合わせは 20D/60D の平均、中央値、p10、CVaR5、severe loss rate を同時に改善する。PSR/Fwd PSR はこの総合分解の一軸として検証したが、主たる tail pruning 条件にはならない。

| bucket | horizon | obs | mean | median | p10 | CVaR5 | severe loss | win rate |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| all crowded long hybrid | 20D | 6,331 | +1.80% | -0.90% | -13.51% | -24.08% | 16.4% | 46.5% |
| low10 PBR | 20D | 1,153 | +3.39% | +1.59% | -10.37% | -17.41% | 10.8% | 55.2% |
| low10 PBR AND low10 Fwd PER | 20D | 667 | +4.88% | +3.52% | -8.88% | -14.88% | 7.3% | 59.4% |
| low10 PSR | 20D | 1,015 | +1.22% | -1.33% | -13.34% | -21.61% | 15.3% | 45.2% |
| low10 Fwd PSR | 20D | 926 | +1.07% | -1.36% | -13.30% | -21.47% | 14.9% | 43.6% |
| all crowded long hybrid | 60D | 6,259 | +4.11% | -2.43% | -24.84% | -38.68% | 33.0% | 44.8% |
| low10 PBR | 60D | 1,144 | +9.09% | +2.13% | -19.64% | -27.47% | 28.8% | 52.8% |
| low10 PBR AND low10 Fwd PER | 60D | 667 | +12.59% | +7.75% | -16.63% | -26.04% | 22.6% | 62.1% |
| low10 PSR | 60D | 994 | +2.30% | -2.63% | -23.20% | -32.75% | 31.0% | 42.2% |
| low10 Fwd PSR | 60D | 908 | +1.38% | -2.59% | -23.83% | -32.10% | 32.3% | 41.5% |

Valuation overlap の読みは「低倍率なら何でも良い」ではない。`low10 PBR AND low10 PSR AND low10 Fwd PSR` は 60D 平均 +0.89%、中央値 -6.26%、severe loss 46.1% まで悪化し、売上倍率の低さを重ねても left tail は削れない。Crowded long の tail management では、売上倍率よりも資本価値と利益予想の確認が重要。

ATR/overheat 分解では、`atr20_to_atr60_overheat` と `recent20_overheat_ge30` が左尾悪化の警戒条件。一方 `low10 PBR AND low10 Fwd PER + ATR20 acceleration ex-overheat` は 20D 平均 +14.80%、中央値 +11.11%、severe loss 0.0%（77 obs）、60D 平均 +30.29%、中央値 +38.58%、severe loss 7.8%（77 obs）で、Crowded long の中でも最も質の良い continuation 条件。

Sector bucket は `Long Hybrid >= 0.8` 固定により sector weak が 57 obs しかなく、弱セクター比較としては採用しない。主に neutral/strong を見ると、`low10 PBR AND low10 Fwd PER` は neutral/strong の両方で tail を改善する一方、売上倍率だけの低さは sector strong でも tail を削れない。この研究から sector weak rule は作らず、sector は別 scaffold で再検証する。

### Interpretation

Crowded long は winner/continuation を拾うため平均はプラスになりやすいが、未調整のままだと “遅れて入った高ボラ continuation” の左尾が残る。研究の焦点は、平均をさらに上げることではなく、右尾を残しながら severe loss と CVaR を削る条件を見つけること。

PBR と Fwd PER の組み合わせは、資本価値と利益予想の両方で安さを確認するため、Crowded 状態でも downside が削れる。PSR/Fwd PSR は収益性・資本効率を見ないため、低倍率が必ずしも再評価余地を意味せず、今回の総合 left-tail map では補助軸に留まる。

ATR は一律の除外条件ではなく、confirmation と caution に分けて読む。ATR20 acceleration ex-overheat は value-confirmed crowded long を強めるが、recent20 overheat や ATR20/60 overheat は left-tail caution として別表示する価値がある。

### Production Implication

- Crowded long の採用優先は `low10 PBR`、high conviction は `low10 PBR AND low10 Fwd PER`。
- `low10 PBR AND low10 Fwd PER AND ATR20 acceleration ex-overheat` は Crowded long continuation の strongest candidate として、次は portfolio lens / sector cap / transaction cost 付きで確認する。
- `recent20 overheat_ge30` と `atr20_to_atr60_overheat` は Crowded long の left-tail caution overlay 候補。
- `low10 PSR` / `low10 Fwd PSR` は、この総合 left-tail 分解では採用条件ではなく valuation diagnostic / tie-breaker に留める。
- `sector weak` はこの scaffold では sample constrained。Long Hybrid strong 固定の内側で sector weak rule を作らない。

### Caveats

- デフォルト分析開始日は `2023-01-01`。full-history ではなく、PSR / Fwd PSR SoT 化後の近年検証を高速に回すための初期設定。
- outcome は close-to-close TOPIX excess return。
- Crowded long は borrow/cost ではなく long candidate selection の left-tail diagnosis として読む。
- `Long Hybrid Leadership >= 0.8` 固定後は sector weak がほぼ残らないため、sector weak の結論は short-side 研究と混ぜない。
- tail metrics は銘柄日次 observation ベースで、portfolio construction / sector cap / turnover cost は未反映。

### Source Artifacts

| Artifact | Path |
| --- | --- |
| runner | `apps/bt/scripts/research/run_ranking_crowded_long_tail_evidence.py` |
| domain module | `apps/bt/src/domains/analytics/ranking_crowded_long_tail_evidence.py` |
| reusable base | `apps/bt/src/domains/analytics/daily_ranking_research_base.py` |
| result bundle | `/private/tmp/trading25-research/market-behavior/ranking-crowded-long-tail-evidence/20260613_ranking_crowded_long_tail_prime_2023_2026_v1` |
