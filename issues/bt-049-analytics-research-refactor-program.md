---
id: bt-049
title: "analytics research module 群のリファクタリング program"
status: open
priority: high
labels: [bt, analytics, refactor, testing]
project: bt
created: 2026-03-27
updated: 2026-03-27
depends_on: []
blocks: [bt-050, bt-051, bt-052, bt-053, bt-054]
parent: null
---

# bt-049 analytics research module 群のリファクタリング program

## 目的
- `apps/bt/src/domains/analytics` に追加された research module 群の重複を解消し、feature/universe/regime 差分を spec で表現できる構造へ寄せる。
- unit test 側の fixture 重複を削減し、今後の research 追加時に test cost が線形に増えない状態を作る。
- notebook/playground は薄い UI 層に留め、domain 側の責務境界を明確にする。

## 背景
- `topix100_sma_ratio_rank_future_close.py` は 3500 行超、`topix100_sma_ratio_regime_conditioning.py` は 1300 行超となっており、query / feature build / bucketing / significance / selection が 1 ファイルに集中している。
- `topix100_price_vs_sma20_rank_future_close.py` と `topix100_price_vs_sma20_regime_conditioning.py` は既存 research の派生だが、helper の再実装と wrapper 的 import が混在している。
- analytics test では DuckDB fixture builder が複数ファイルに重複しており、schema 修正や data pattern 修正の追従コストが高い。

## 受け入れ条件
- [ ] child issue が作成済みで、依存順が明示されている。
- [ ] rank research / regime conditioning / fixture builder / naming cleanup / hedge decomposition の workstream が個別 issue として管理される。
- [ ] program 完了時に、analytics research module 群の重複箇所と残課題が整理される。

## 実施内容
- [ ] tracking issue として child issue の完了状態を管理する。
- [ ] 実装中に設計変更が出た場合は child issue と依存関係を更新する。
- [ ] core 抽出後に notebook / test / wrapper の責務を再点検する。
- [ ] 完了時に analytics module 群の構造整理結果を issue に記録する。

## 結果
（完了後に記載）

## Child Issue 状態

### 未着手
- [ ] `bt-050` TOPIX rank research core を shared module へ抽出
- [ ] `bt-051` regime conditioning core を shared module へ抽出
- [ ] `bt-052` analytics unit test の market DB fixture を共通化
- [ ] `bt-053` decile / bucket naming と research wrapper interface を整理
- [ ] `bt-054` oversized analytics module を責務別に分割

## 補足
- 主対象: `apps/bt/src/domains/analytics/*rank_future_close.py`、`*regime_conditioning.py`、`hedge_1357_nt_ratio_topix.py`、`apps/bt/tests/unit/domains/analytics/*`
