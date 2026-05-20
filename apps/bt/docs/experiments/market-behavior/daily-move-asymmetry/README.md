# Daily Move Asymmetry

TOPIX と Prime 全銘柄の日足 close-to-close move を、上昇・下落の符号と volatility-normalized magnitude で揃えて比較し、粘着性・瞬発力・反発力の非対称性を観察する研究。

## Published Readout

### Decision

初回 runner を追加し、TOPIX と Prime 株の daily move asymmetry を research surface として採用する。production rule にはまだ昇格しない。

TOPIX 単体では、同程度の下落後は上昇後より反発方向に寄りやすく、特に 20営業日では large down 後の mean / median が large up 後を上回った。一方 Prime 個別株は raw return では下落後の反発が見えるが、TOPIX excess / beta-adjusted では中央値がマイナスに残り、下落後の raw rebound の相当部分は market rebound / beta exposure と読むべきだった。

### Why This Research Was Run

「株価は長期的にインフレで上がる」という drift ではなく、日足レベルで同じ程度の上昇と下落を揃えたときに、翌日以降の粘着性・瞬発力・反発力が非対称かを確認する。対象は TOPIX と Prime 全銘柄で、個別株では市場全体の反発・下落と銘柄固有の反応を分ける。

### Data Scope / PIT Assumptions

入力は active `market.duckdb` の `topix_data` / `stock_data` / `stock_master_daily`。Prime membership は同日 `stock_master_daily.market_code` を優先し、未整備 DB では `stocks` latest fallback を明示する。event bucket は当日までの close-to-close return と rolling volatility だけで作り、forward return は outcome 測定にのみ使う。

### Main Findings

#### 結論

| Lens | Horizon | Pair | Down mean | Up mean | Down median | Up median | Down rebound / Up reversal | Severe loss | 読み |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | --- | --- |
| TOPIX raw | 1d | medium | `+0.19%` | `-0.02%` | `+0.20%` | `-0.00%` | `58.63% / 50.14%` | `0.00% / 0.00%` | 中程度下落は翌日反発寄り |
| TOPIX raw | 20d | large | `+1.32%` | `+0.70%` | `+2.02%` | `+1.24%` | `65.95% / 38.87%` | `9.48% / 9.19%` | 大きめ下落後の 20d rebound が強い |
| Prime raw | 20d | large | `+1.11%` | `+0.81%` | `+0.71%` | `+0.31%` | `53.94% / 47.79%` | `21.19% / 22.36%` | raw では下落後 rebound が見える |
| Prime raw | 20d | extreme | `+1.87%` | `+1.16%` | `+1.19%` | `+0.35%` | `56.22% / 47.69%` | `20.89% / 23.45%` | 極端下落後 raw rebound はさらに強い |
| Prime TOPIX excess | 20d | large | `+0.05%` | `-0.03%` | `-0.52%` | `-0.67%` | `46.51% / 54.32%` | `23.04% / 23.90%` | excess では中央値が弱い |
| Prime TOPIX excess | 20d | extreme | `-0.00%` | `-0.16%` | `-0.57%` | `-1.01%` | `46.49% / 55.94%` | `24.36% / 27.31%` | raw rebound は市場反発でかなり説明される |
| Prime beta-adjusted | 20d | large | `+0.13%` | `+0.03%` | `-0.39%` | `-0.57%` | `47.28% / 53.94%` | `22.04% / 22.90%` | beta 控除後も平均は少し残るが中央値は弱い |
| Prime beta-adjusted | 20d | extreme | `+0.13%` | `-0.05%` | `-0.38%` | `-0.87%` | `47.50% / 55.40%` | `22.98% / 26.04%` | 銘柄固有 alpha と呼ぶには弱い |

#### Sign Persistence

| Scope | Current sign | Obs | Same sign next day | Opposite next day | Next mean |
| --- | --- | ---: | ---: | ---: | ---: |
| TOPIX | up | `1,314` | `53.50%` | `46.50%` | `+0.05%` |
| TOPIX | down | `1,127` | `45.79%` | `54.21%` | `+0.05%` |
| Prime stocks | up | `2,314,495` | `47.88%` | `48.62%` | `+0.04%` |
| Prime stocks | down | `2,256,519` | `46.67%` | `49.89%` | `+0.06%` |

TOPIX は上昇日の翌日に上昇継続しやすく、下落日の翌日は反発しやすい。Prime 個別株は翌日単位では上下とも反対方向がやや多く、符号だけで粘着性を読むより、TOPIX excess / beta-adjusted と magnitude bucket を合わせる必要がある。

### Interpretation

TOPIX は「下落の後に下落が粘る」よりも、日足 close-to-close では反発側の非対称性が強い。20営業日の large pair では下落後 mean `+1.32%` / median `+2.02%` に対して、上昇後は mean `+0.70%` / median `+1.24%` だった。

Prime 個別株の raw return でも似た見かけはあるが、TOPIX excess にすると large down 20d の median は `-0.52%`、extreme down 20d は `-0.57%` で、hit rate も 50% を割る。これは「急落銘柄を買えば個別 alpha」というより、「市場反発に乗る raw rebound と、なお弱い銘柄固有 distribution が同居している」と読むのが妥当。

上昇後については、Prime の extreme up は 20d TOPIX excess median `-1.01%`、severe loss `27.31%` で、下落後より悪い左尾を持つ。急騰後の追随は、少なくとも単独では caution diagnostic として扱う。

### Production Implication

現時点では signal ではなく market behavior diagnostic。TOPIX の下落後反発は market exposure / hedge timing の補助にはなりうるが、Prime 個別株では raw return をそのまま entry rule にしない。個別株側で使うなら、TOPIX excess / beta-adjusted residual、左尾、market breadth を併用した risk state / sizing / warning の候補に留める。

### Caveats

日足 OHLCV では intraday path は見えないため、瞬発力は close-to-close の到達速度と forward distribution で近似する。beta は初期実装では全期間 static beta であり、rolling beta / regime beta ではない。large / extreme bucket は銘柄・時期によってサンプル偏りがあるため、平均だけでなく中央値と左尾を優先して読む。

### Source Artifacts

- Domain: `apps/bt/src/domains/analytics/daily_move_asymmetry.py`
- Runner: `apps/bt/scripts/research/run_daily_move_asymmetry.py`
- Bundle: `/tmp/trading25-research/market-behavior/daily-move-asymmetry/20260520_daily_move_asymmetry_prime_topix_v2/`
- Results DB: `/tmp/trading25-research/market-behavior/daily-move-asymmetry/20260520_daily_move_asymmetry_prime_topix_v2/results.duckdb`
- Summary: `/tmp/trading25-research/market-behavior/daily-move-asymmetry/20260520_daily_move_asymmetry_prime_topix_v2/summary.md`

## Run

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_daily_move_asymmetry.py \
  --output-root /tmp/trading25-research
```

Focused smoke:

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_daily_move_asymmetry.py \
  --horizons 1,5 \
  --rolling-vol-window 40 \
  --min-observations 20 \
  --output-root /tmp/trading25-research \
  --run-id daily_move_asymmetry_smoke
```
