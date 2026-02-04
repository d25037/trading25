---
id: bt-017
title: シグナルレジストリのparam_key重複検証
status: open
priority: low
labels: [signal, registry, validation]
project: bt
created: 2026-02-01
updated: 2026-02-01
depends_on: []
blocks: []
parent: null
---

# bt-017 シグナルレジストリのparam_key重複検証

## 目的

`SIGNAL_REGISTRY` に登録されるシグナル定義の `param_key` が一意であることを保証する仕組みを追加し、将来的な登録ミスを防止する。

## 背景

`eps_growth` → `forward_eps_growth` リネーム＋新 `eps_growth` 追加（実績ベース）の際、param_key の衝突リスクが指摘された。現状は重複がないが、シグナル数が増加するにつれてヒューマンエラーの可能性が高まる。

## 受け入れ条件

- `SIGNAL_REGISTRY` のモジュールロード時に `param_key` の重複を検出し `ValueError` を送出する
- 重複検出のユニットテストが存在する
- 既存テスト全通過

## 実施案

`src/strategies/signals/registry.py` の末尾にバリデーション関数を追加：

```python
def _validate_registry() -> None:
    seen: set[str] = set()
    for sig in SIGNAL_REGISTRY:
        if sig.param_key in seen:
            raise ValueError(f"Duplicate param_key in SIGNAL_REGISTRY: {sig.param_key}")
        seen.add(sig.param_key)

_validate_registry()
```

## 補足

- 影響範囲は小さく、モジュールロード時の一回限りのチェック
- パフォーマンスへの影響は無視できる（現在30弱のシグナル定義）
