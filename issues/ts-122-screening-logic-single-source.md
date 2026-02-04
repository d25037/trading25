---
id: ts-122
title: スクリーニングロジックの一本化検討
status: open
priority: medium
labels: [design, analytics, api-integration]
project: ts
created: 2026-02-02
updated: 2026-02-02
depends_on: []
blocks: []
parent: null
---

# ts-122 スクリーニングロジックの一本化検討

## 目的
apps/ts/ と apps/bt/ に分散するスクリーニングロジックの責務を明確にし、重複を排除する。

## 受け入れ条件
- apps/ts/ のスクリーニング（レンジブレイク検出）と apps/bt/ のスクリーニング（シグナルベース）の違いが明文化されていること
- 重複するロジックがあれば一方に統合されていること
- 将来的なスクリーニング機能追加の方針が決まっていること

## 実施内容
- 現状の二重実装:
  - **apps/ts/**: `/api/analytics/screening` — レンジブレイク検出（support/resistance breakout）
  - **apps/bt/**: `signal_screening.py` — シグナルベーススクリーニング（apps/bt/のシグナル定義を使用）
- 分析観点:
  1. 両者のスクリーニングロジックは本当に異なるか？（入力データ・アルゴリズム・出力形式の比較）
  2. apps/bt/ のシグナルスクリーニングはバックテスト用シグナルの再利用であり、apps/ts/ のレンジブレイクとは目的が異なる可能性が高い
  3. 統合するなら apps/ts/ API にシグナルベーススクリーニングを追加し、apps/bt/ は API を呼ぶだけにする方法がある
  4. ただし apps/bt/ のシグナル定義は Python 固有のため、apps/ts/ に移植するコストが高い
- 推奨: 現状維持（目的が異なる）だが、ドキュメントで差異を明確化

## 結果

### bt-020 完了注記 (2026-02-02)
apps/bt/ の cli_market/screening.py は削除されたが、`signal_screening.py` は維持されている。
- apps/bt/ のシグナルベーススクリーニング（`src/data/signal_screening.py`）はバックテスト用シグナルの検証ツールとして引き続き使用
- `market_analysis.py` はre-exportモジュールに簡素化され、signal_screeningの主要シンボルをre-exportするのみ
- apps/ts/ のレンジブレイクスクリーニング（`/api/analytics/screening`）とは目的・実装が異なるため、現状維持が妥当

## 補足
- apps/ts/ のスクリーニング: 市場全体のテクニカルパターン検出（Web UI向け）
- apps/bt/ のスクリーニング: バックテスト戦略のシグナル適用結果確認（開発・検証向け）
- 両者は目的・ユーザー・出力形式が異なるため、統合は必須ではない
- bt-020 (cli_market削除) と関連: apps/bt/のCLIスクリーニングが削除されても signal_screening.py 自体は残る
