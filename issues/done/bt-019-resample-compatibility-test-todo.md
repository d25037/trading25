---
id: bt-019
title: 互換性テストのTODOコメント解消
status: closed
priority: low
labels: [test, cleanup]
project: bt
created: 2026-02-03
updated: 2026-02-03
depends_on: []
blocks: []
parent: null
---

# bt-019 互換性テストのTODOコメント解消

## 目的
`tests/server/test_resample_compatibility.py` のTODOコメントを解消し、テストの完全性を確保する。

## 背景
週足のインデックス調整（月曜日への変換）が実装完了したため、以下のTODOコメントを有効化またはクリーンアップする必要がある:

```python
# tests/server/test_resample_compatibility.py:241-245
# TODO: apps/bt/実装を週開始日に修正後、このテストを有効化
# assert actual_date == exp["date"], \
#     f"Week {i+1} 日付不一致: {actual_date} != {exp['date']}"
# 現状はpandas週末アンカーの日付を許容
pass
```

## 受け入れ条件
- [ ] TODOコメントを削除
- [ ] 週足日付検証のアサーションを有効化
- [ ] テストがパスすることを確認

## 実施内容
（着手後に記載）

## 結果
TODOコメント削除、週足日付検証アサーションを有効化。テスト全4件通過を確認。(2026-02-06)

## 補足
- 関連コミット: Timeframe Resample機能の移植（2026-02-03）
- 修正済みの実装: `IndicatorService.resample_timeframe()` で週開始日に調整済み
