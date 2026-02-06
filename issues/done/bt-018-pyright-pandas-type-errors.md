---
id: bt-018
title: Pyright pandas型エラーの解消
status: done
priority: low
labels: [type-safety, tech-debt]
project: bt
created: 2026-02-03
updated: 2026-02-06
depends_on: []
blocks: []
parent: null
---

# bt-018 Pyright pandas型エラーの解消

## 目的
`src/server/services/indicator_service.py` に存在する21件のPyright型エラーを解消し、型安全性を向上させる。

## 背景
現在の型エラーは主にpandasの型定義の問題に起因する:
- `int()` への `Scalar` 型の引数
- `pd.Series[Any]` の型不整合
- Series の `__bool__` メソッドの型問題

これらはランタイムには影響しないが、型チェックの信頼性を損なう。

## 受け入れ条件
- [ ] `uv run pyright src/server/services/indicator_service.py` でエラーが0件
- [ ] 既存テストがすべてパス
- [ ] ランタイム動作に変更なし

## 実施内容
- `indicator_service.py`: pandas Scalar/Series[Any] を `float()` 経由で安全に変換、pandas-stubs の既知制限箇所に `# type: ignore[arg-type]` を付与、`.agg()` の dict 引数に `# type: ignore[arg-type]` を付与
- `fundamentals_service.py`: `normalize_period_type()` の戻り値 `str | None` に対する None ガード追加

## 結果
- `uv run pyright src/` で 0 errors 達成（warning 1件は既知の `__all__` 警告のみ）

## 補足
- 優先度: low（ランタイムには影響しないため）
- 対象関数: `compute_margin_long_pressure`, `compute_margin_flow_pressure`, `compute_margin_turnover_days`, `compute_margin_volume_ratio`
- 修正アプローチ候補:
  1. 明示的な型キャスト（`int(float(val))`）
  2. `# type: ignore` コメント追加
  3. pandas-stubs の更新
