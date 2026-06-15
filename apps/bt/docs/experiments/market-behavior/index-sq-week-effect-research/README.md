# Index SQ Week Effect Research

毎月第2金曜の SQ（Special Quotation）を含む月曜〜金曜について、TOPIX と日経平均（`N225_UNDERPX`）の指数レベル値動きがその他の週と異なるかを検証する研究。

## Published Readout

### Decision

SQ週は「指数の週次リターン方向が別物」とまでは言いにくい。特に TOPIX の週次リターンと絶対週次リターンは、その他週との差が小さく統計的にも弱い。一方、日経平均（`N225_UNDERPX`）は SQ週の週内日次ボラティリティと最大日次変動が明確に高く、OP市場側では front contract の出来高、ATM近傍IV、SQ週中のIV低下がかなり違う。

したがって production 上は、SQ週を「方向シグナル」ではなく、日経平均主導の短期ボラティリティ/OPイベント risk overlay として扱う。TOPIX beta や個別銘柄選別を SQ週だけで反転させる根拠にはまだしない。

### Why This Research Was Run

「毎月第2週金曜はSQで、当該週では機関投資家による異質な値動きが出やすい」という一般的な見方が、local `market.duckdb` の TOPIX / 日経平均 / 日経225OP データで本当に観測されるかを確認するため。

### Data Scope / PIT Assumptions

入力は active `market.duckdb` の `topix_data`、`indices_data` の `N225_UNDERPX`、および `options_225_data`。SQ週は「各月第2金曜を含む月曜〜金曜」として calendar fixed に定義し、未来の値動きから bucket を作らない。日経平均は J-Quants OP の `UnderPx` 由来 synthetic index であり、日中OHLCの高安情報ではないため、N225 の `mean_intraday_range_pct` と `mean_open_to_close_return_pct` は解釈対象から外す。

### Main Findings

#### 結論

| Lens | Metric | SQ n | Other n | SQ mean | Other mean | Diff | Effect | p | 読み |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| N225 | `daily_return_std_pct` | `119` | `394` | `1.27` | `1.04` | `+0.24pt` | `0.41` | `0.0288` | SQ週は週内の日次ボラが高い |
| N225 | `max_abs_daily_return_pct` | `119` | `397` | `2.10` | `1.72` | `+0.37pt` | `0.39` | `0.0188` | 週内最大ショックも大きい |
| TOPIX | `daily_return_std_pct` | `121` | `401` | `1.09` | `0.92` | `+0.17pt` | `0.34` | `0.0629` | 同方向だがN225より弱い |
| TOPIX | `mean_intraday_range_pct` | `121` | `404` | `1.14` | `1.00` | `+0.14pt` | `0.31` | `0.0898` | TOPIX日中レンジもやや広い |
| N225 | `week_return_pct` | `119` | `397` | `0.63` | `0.13` | `+0.50pt` | `0.22` | `0.0502` | 方向はプラス寄りだが効果は小さい |
| TOPIX | `week_return_pct` | `121` | `404` | `0.27` | `0.12` | `+0.15pt` | `0.07` | `0.5161` | TOPIX方向差はほぼない |
| TOPIX | `abs_week_return_pct` | `121` | `404` | `1.64` | `1.55` | `+0.09pt` | `0.06` | `0.5694` | 週全体の絶対値動きは大差なし |

#### OP Deep Dive

| Metric | SQ n | Other n | SQ mean | Other mean | Diff | Relative | Effect | p | 読み |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `front_volume_sum` | `119` | `397` | `344,744` | `293,018` | `+51,726` | `+17.65%` | `0.42` | `0.0007` | front OP 出来高はSQ週に明確に増える |
| `atm_implied_volatility_mean` | `119` | `397` | `22.02` | `19.08` | `+2.93pt` | `+15.37%` | `0.44` | `0.0034` | SQ週はATM近傍IV水準が高い |
| `atm_implied_volatility_change` | `119` | `397` | `-5.16` | `-0.55` | `-4.61pt` | `-838.51%` | `-1.20` | `0.0004` | SQ週中にIVが大きく剥落する |
| `front_open_interest_change_pct` | `119` | `397` | `-3.93%` | `+6.26%` | `-10.18pt` | `-162.75%` | `-1.78` | `0.1454` | 建玉はSQ週で落ちやすいが分散が大きい |
| `front_open_interest_mean` | `119` | `397` | `328,091` | `315,663` | `+12,428` | `+3.94%` | `0.07` | `0.4539` | 平均建玉水準差は小さい |

#### Days To SQ

front contract の daily aggregate では、SQまで `4` calendar days の ATM IV change が `+5.16pt` と大きく、SQ直前 `1-3` days ではIV水準は高いまま、変化は概ね鈍化または低下に寄る。出来高も `1-4` days to SQ で `72k-81k` 台と、`10-28` days の `61k-67k` 台より高い。これは「SQ週のOP市場は明確に別物」という読みを補強する。

| Days to SQ | Front volume mean | Front OI mean | ATM IV mean | ATM IV change mean |
| ---: | ---: | ---: | ---: | ---: |
| `1` | `74,310` | `344,552` | `22.15` | `+0.15` |
| `2` | `72,022` | `345,192` | `21.67` | `-0.39` |
| `3` | `74,561` | `347,311` | `22.37` | `-1.34` |
| `4` | `81,301` | `360,584` | `25.05` | `+5.16` |
| `10` | `61,733` | `343,399` | `19.56` | `-0.51` |
| `14` | `67,353` | `331,113` | `19.08` | `-0.05` |
| `21` | `61,491` | `310,573` | `18.24` | `-0.04` |
| `28` | `63,796` | `281,189` | `19.20` | `-2.01` |

### Interpretation

指数側の結果は「SQ週は必ず大きく上がる/下がる」というより、「日経平均の週内ボラティリティが上がりやすい」と読むのが妥当。TOPIX も同方向のレンジ拡大はあるが、効果量と p 値は N225 より弱く、TOPIX broad market 全体の方向性を変えるほどではない。

OP側は指数側よりはるかに明確で、front contract の出来高増加、ATM IV 水準上昇、SQ週中のIV低下が観測される。したがって、一般に言われる「SQ週の異質性」は、TOPIXの週次方向ではなく、日経平均OPの満期接近に伴う activity / vol surface の変化として強く出ている。

### Production Implication

Ranking / Screening / Backtest へ直接の long/short direction rule として入れない。使うなら、SQ週 flag を market diagnostic として持ち、日経平均連動・先物/OP影響を受けやすい basket の short-horizon risk sizing、volatility warning、entry timing caution に限定する。TOPIX broad exposure の hard filter にはしない。

### Caveats

`N225_UNDERPX` は OP `UnderPx` 由来の synthetic daily series で、日中OHLCは true OHLC ではない。N225 の intraday range は使わず、close-to-close / week level のみを見る。Welch t は normal approximation であり、fat-tail と serial correlation を完全には扱っていない。OP deep dive は front SQ contract の aggregate で、strike/put-call別の建玉偏りや dealer gamma proxy までは分解していない。2026-06-11 時点の active local DB に依存する。

### Source Artifacts

- Domain: `apps/bt/src/domains/analytics/index_sq_week_effect_research.py`
- Runner: `apps/bt/scripts/research/run_index_sq_week_effect_research.py`
- Bundle: `~/.local/share/trading25/research/market-behavior/index-sq-week-effect-research/20260611_index_sq_week_effect_topix_n225_options/`
- Results DB: `~/.local/share/trading25/research/market-behavior/index-sq-week-effect-research/20260611_index_sq_week_effect_topix_n225_options/results.duckdb`
- Summary: `~/.local/share/trading25/research/market-behavior/index-sq-week-effect-research/20260611_index_sq_week_effect_topix_n225_options/summary.md`

## Run

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_index_sq_week_effect_research.py \
  --run-id 20260611_index_sq_week_effect_topix_n225_options
```

Focused test:

```bash
uv run --project apps/bt pytest apps/bt/tests/unit/domains/analytics/test_index_sq_week_effect_research.py
```
