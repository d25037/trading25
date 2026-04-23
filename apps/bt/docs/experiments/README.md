# Experiments

`apps/bt` の runner / domain / bundle 実験の索引です。

## Conventions

- 実験ロジックの SoT は `apps/bt/src/domains/analytics/` に置く。
- 再現可能な実行導線の SoT は `apps/bt/scripts/research/` に置く。
- 長く残す知見は `apps/bt/docs/experiments/` に集約する。
- 各実験は `README.md` を canonical note、`baseline-YYYY-MM-DD.md` を時点固定の結果メモとして残す。
- runner-first 実験は `~/.local/share/trading25/research/<experiment>/<run_id>/` に `manifest.json + results.duckdb + summary.md` を保存し、structured summary を出す場合は `summary.json` も含める。
- published research の最小 surface は `runner script + bundle output + canonical note README + baseline note` とする。
- notebook runtime は repo の必須導線から外す。結果確認は runner が出力する `summary.md` / `summary.json` / `results.duckdb` を使う。
- 画像を固定資産として残す場合のみ `figures/` に保存する。

## Index

- [market-behavior/topix-gap-intraday-distribution/](./market-behavior/topix-gap-intraday-distribution/README.md)
  - TOPIX の寄り付き gap を条件に、個別銘柄群の当日 intraday と簡易 rotation ルールを観察する実験。
- [market-behavior/topix-close-stock-overnight/](./market-behavior/topix-close-stock-overnight/README.md)
  - TOPIX の当日引け変動を条件に、個別銘柄群の `close -> next open` を観察する実験。
- [market-behavior/nt-ratio-change-stock-overnight/](./market-behavior/nt-ratio-change-stock-overnight/README.md)
  - NT 倍率の前日比を条件に、個別銘柄群の `close -> next open` を観察する実験。
- [market-behavior/nt-ratio-change-topix-close-stock-overnight/](./market-behavior/nt-ratio-change-topix-close-stock-overnight/README.md)
  - NT 倍率前日比と TOPIX 引け変動の joint regime ごとに、個別銘柄群の `close -> next open` を観察する実験。
- [market-behavior/topix100-vi-change-regime-conditioning/](./market-behavior/topix100-vi-change-regime-conditioning/README.md)
  - 日経VI 前日比 regime ごとに、TOPIX100 の price/volume split がその後どう振る舞うかを観察する実験。
- [market-behavior/topix100-price-vs-sma-rank-future-close/](./market-behavior/topix100-price-vs-sma-rank-future-close/README.md)
  - TOPIX100 の `price / SMA20|50|100` 単独特徴を decile と price/volume split で比較し、continuation か mean-reversion かを観察する実験。
- [market-behavior/topix100-price-vs-sma-q10-bounce/](./market-behavior/topix100-price-vs-sma-q10-bounce/README.md)
  - `price / SMA` family の `Q10` 側だけを切り出し、`Q10 Low vs ...` の bounce 仮説を feature / horizon ごとに比較する実験。
- [market-behavior/topix100-price-vs-sma-q10-bounce-regime-conditioning/](./market-behavior/topix100-price-vs-sma-q10-bounce-regime-conditioning/README.md)
  - `SMA50 Q10 Low` bounce を same-day `TOPIX close` / `NT ratio` regime で条件付けし、どの market state で反発が強いかを見る実験。
- [market-behavior/topix100-sma50-raw-vs-atr-q10-bounce/](./market-behavior/topix100-sma50-raw-vs-atr-q10-bounce/README.md)
  - `SMA50` の plain gap と `ATR14` 正規化 gap を同じ `Q10 / middle x volume` frame で比較し、見た目の違いを volatility scale で説明できるかを見る実験。
- [market-behavior/topix100-sma-ratio-lightgbm/](./market-behavior/topix100-sma-ratio-lightgbm/README.md)
  - TOPIX100 の 6 本の SMA ratio 特徴に対して、hand-crafted composite baseline と LightGBM ranker を walk-forward OOS で比較する実験。
- [market-behavior/topix-downside-return-standard-deviation-shock-confirmation-committee-overlay/](./market-behavior/topix-downside-return-standard-deviation-shock-confirmation-committee-overlay/README.md)
  - TOPIX を long-only で持つ前提で、downside return standard deviation と trend / breadth confirmation を使う fixed overlay を committee 化し、walk-forward と pure OOS で評価する実験。
- [market-behavior/stock-intraday-overnight-share/](./market-behavior/stock-intraday-overnight-share/README.md)
  - 個別銘柄の値幅を `open -> close` と `close -> next open` に分解し、銘柄群ごとの intraday / overnight 構成比を観察する実験。
- [market-behavior/annual-first-open-last-close-fundamental-panel/](./market-behavior/annual-first-open-last-close-fundamental-panel/README.md)
  - 各銘柄を大発会 `Open` で買って大納会 `Close` で売る年次保有 return を、買付時点 FY ファンダメンタルと株式数補正付きで観察する実験。
- [market-behavior/annual-fundamental-confounder-analysis/](./market-behavior/annual-fundamental-confounder-analysis/README.md)
  - 年次保有ファンダメンタル panel を土台に、低PBR・小型・低forward PER・低ADVなどの交絡と独立効果を統計的に分解する実験。
- [market-behavior/stop-limit-daily-classification/](./market-behavior/stop-limit-daily-classification/README.md)
  - JPX 制限値幅の標準テーブルを前日終値ベースで当て、ストップ高 / ストップ安の exact hit を market / intraday state / close-at-limit で分類する実験。
- [market-behavior/stop-limit-buy-only-next-close-followthrough/](./market-behavior/stop-limit-buy-only-next-close-followthrough/README.md)
  - `stop_low × intraday_range` の翌日引け確認後に買う buy-only 枝を、trade-level と同日等ウェイト portfolio lens の両方で読む実験。
- [market-behavior/speculative-volume-surge-follow-on/](./market-behavior/speculative-volume-surge-follow-on/README.md)
  - `+10% close × 10x volume` の speculative surge episode をまとめ、初動の伸びと後日 breakout / secondary surge の関係を見る実験。
- [market-behavior/speculative-volume-surge-pullback-edge/](./market-behavior/speculative-volume-surge-pullback-edge/README.md)
  - 初回 surge 後の pullback close が `surge 前日 close` 比でどこにいると、その後の upside/downside 非対称性が良いかを見る実験。
- [market-behavior/speculative-volume-surge-prime-pullback-profile/](./market-behavior/speculative-volume-surge-prime-pullback-profile/README.md)
  - `プライム` surge を `1 episode = 1 deepest-pullback label` に落とし、浅い continuation と深い押しの close return / asymmetry を比較する実験。
- [market-behavior/speculative-volume-surge-prime-pullback-tradeable/](./market-behavior/speculative-volume-surge-prime-pullback-tradeable/README.md)
  - `プライム` surge の first-pullback entry を `0-10% / 10-20%` で執行し、`20営業日 hold or peak reclaim` の tradeable return と deepest-family alignment を見る実験。
