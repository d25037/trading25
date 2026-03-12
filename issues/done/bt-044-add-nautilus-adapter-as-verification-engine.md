---
id: bt-044
title: "NautilusAdapter を verification engine として追加"
status: done
priority: medium
labels: [nautilus, engine, verification, backtest, bt]
project: bt
created: 2026-03-08
updated: 2026-03-12
depends_on: [bt-040, bt-042, bt-043]
blocks: [bt-045]
parent: bt-037
---

# bt-044 NautilusAdapter を verification engine として追加

## 目的
- `Nautilus Trader` を fast path の代替ではなく verification engine として組み込む。
- 同じ `RunSpec` / snapshot / strategy IR から Nautilus 実行を起動し、canonical result に正規化する。

## 受け入れ条件
- [x] `NautilusAdapter` が最小スコープの backtest run を実行できる。
- [x] 結果が `CanonicalExecutionResult` へ正規化される。
- [x] worker runtime 上で Nautilus 実行が成立する。
- [x] engine metadata と diagnostics が artifact に残る。

## 実施内容
- [x] Nautilus 用の input mapping と bar/event snapshot contract を定義する。
- [x] `RunSpec -> Nautilus run` 変換層を実装する。
- [x] canonical normalization / comparison テストを追加する。
- [x] 導入範囲を日足検証から始め、拡張方針を docs 化する。

## 結果
- `BacktestRequest.engine_family` を追加し、submit service と worker の SoT を `RunSpec.engine_family` に統一した。
- worker は claim 後に persisted `job.run_spec` を優先して engine dispatch し、`vectorbt` は既存 runner、`nautilus` は `NautilusVerificationRunner` を使うようにした。
- `NautilusVerificationRunner` を追加し、日足 `Open/Close` verification 限定で `RunSpec` / snapshot / compiled strategy から Nautilus 実行と canonical normalization を行えるようにした。
- Nautilus path は `metrics.json` / `manifest.json` / `engine.json` / `diagnostics.json` を core artifact とし、`html_path=None` でも結果再解決できるようにした。
- `ArtifactKind` に `engine_json` / `diagnostics_json` を追加し、artifact index と OpenAPI / TS generated types を更新した。
- `apps/bt/pyproject.toml` に optional な `nautilus` dependency group を追加し、未導入環境では missing dependency error で fail-fast するようにした。
- real `nautilus_trader` runtime を使う smoke test と separate CI workflow を追加し、default CI から切り離したまま runtime compatibility を観測できるようにした。
- `bt-045` に残る範囲は comparison、verification queue、ranking 反映、UI/API 表示差分である。

## 補足
- 参照: `docs/backtest-greenfield-rebuild.md` Section 5.3, 5.4, 10
