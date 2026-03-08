---
id: bt-044
title: "NautilusAdapter を verification engine として追加"
status: open
priority: medium
labels: [nautilus, engine, verification, backtest, bt]
project: bt
created: 2026-03-08
updated: 2026-03-08
depends_on: [bt-040, bt-042, bt-043]
blocks: [bt-045]
parent: bt-037
---

# bt-044 NautilusAdapter を verification engine として追加

## 目的
- `Nautilus Trader` を fast path の代替ではなく verification engine として組み込む。
- 同じ `RunSpec` / snapshot / strategy IR から Nautilus 実行を起動し、canonical result に正規化する。

## 受け入れ条件
- [ ] `NautilusAdapter` が最小スコープの backtest run を実行できる。
- [ ] 結果が `CanonicalExecutionResult` へ正規化される。
- [ ] worker runtime 上で Nautilus 実行が成立する。
- [ ] engine metadata と diagnostics が artifact に残る。

## 実施内容
- [ ] Nautilus 用の input mapping と bar/event snapshot contract を定義する。
- [ ] `RunSpec -> Nautilus run` 変換層を実装する。
- [ ] canonical normalization / comparison テストを追加する。
- [ ] 導入範囲を日足検証から始め、拡張方針を docs 化する。

## 結果
- 未着手

## 補足
- 参照: `docs/backtest-greenfield-rebuild.md` Section 5.3, 5.4, 10

