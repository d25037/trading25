---
id: bt-056
title: "TOPIX100 ranking と universe signal に LightGBM scorer を接続する"
status: open
priority: high
labels: [bt, analytics, lightgbm, ranking, signals, runtime]
project: bt
created: 2026-03-31
updated: 2026-03-31
depends_on: []
blocks: []
parent: bt-055
---

# bt-056 TOPIX100 ranking と universe signal に LightGBM scorer を接続する

## 目的
- `TOPIX100 SMA ratio` 研究で OOS 優位だった `LightGBM` を、research-only helper から `ranking` と `signal` の product surface に昇格させる。
- 現在の離散 bucket ベースの `price / SMA` surface を、offline 学習済みの連続 score で置き換えられるか検証する。

## 背景
- 既存の LightGBM helper は `TOPIX100 SMA ratio` の event panel に対して walk-forward OOS 評価まで持っている。
- `TOPIX100 ranking` は snapshot SQL で price/volume 比率を並べるだけで、学習ベースの score は持っていない。
- `universe_rank_bucket` は cross-sectional bucket rule を返す heuristic signal で、研究上有効だった非線形順位付けをまだ使っていない。

## 受け入れ条件
- [ ] research の LightGBM scorer を再利用または抽出して、runtime から読める internal scorer を定義する。
- [ ] `ranking_service` から LightGBM score の snapshot ranking を返せる。
- [ ] `universe_rank_bucket` と並列で使える signal surface、または後継 signal を定義できる。
- [ ] 学習導線は offline/research に限定し、API runtime では「学習済み scorer の推論のみ」を前提にする。
- [ ] fallback と model-unavailable 時の挙動を明文化する。

## 実施内容
- [ ] `topix100_sma_ratio_rank_future_close_lightgbm.py` の runtime 再利用可能部分を抽出する。
- [ ] score の保存形式と読み出し boundary を決める。
- [ ] `ranking_service` に experimental な LightGBM metric を追加できるよう設計する。
- [ ] signal system 側で `bucket rule` と `model score threshold/rank` のどちらを SoT にするか整理する。
- [ ] OOS baseline と runtime snapshot surface の整合確認を行う。

## 結果
- 未着手。

## 補足
- 有望根拠: `apps/bt/src/domains/analytics/topix100_sma_ratio_rank_future_close_lightgbm.py#L203` 以降で日次 query group と lambdarank が成立している。
- product surface: `apps/bt/src/application/services/ranking_service.py#L45`, `apps/bt/src/domains/strategy/signals/universe_rank_bucket.py#L59`
- 主リスクは model artifact 管理と offline/online 境界。
