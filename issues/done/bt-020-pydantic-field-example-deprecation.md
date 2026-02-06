---
id: bt-020
title: "Pydantic V2 deprecation: Field(example=...) を json_schema_extra に移行"
status: closed
priority: low
labels: [refactor, pydantic]
project: bt
created: 2026-02-03
updated: 2026-02-03
depends_on: []
blocks: []
parent: null
---

# bt-020 Pydantic V2 deprecation: Field(example=...) を json_schema_extra に移行

## 目的
Pydantic V2のdeprecation warningを解消し、V3.0への移行準備を行う。

## 受け入れ条件
- `Field(example=...)` を使用している全スキーマが `json_schema_extra` 形式に移行済み
- テスト実行時にPydantic deprecation warningが発生しない

## 概要
Pydantic V2のdeprecation warningが発生している。`Field(example=...)` は V3.0 で削除予定のため、`json_schema_extra` に移行する必要がある。

## 対象ファイル
- `src/server/schemas/fundamentals.py`
- その他 `Field(example=...)` を使用しているスキーマファイル

## 警告メッセージ
```
PydanticDeprecatedSince20: Using extra keyword arguments on `Field` is deprecated and will be removed.
Use `json_schema_extra` instead. (Extra keys: 'example').
Deprecated in Pydantic V2.0 to be removed in V3.0.
```

## 対応方法
```python
# Before
symbol: str = Field(..., description="Stock code", example="7203")

# After
symbol: str = Field(..., description="Stock code", json_schema_extra={"example": "7203"})
```

## 補足
- 優先度: 低（V3.0リリースまでに対応）
- Pydantic V2 Migration Guide: https://errors.pydantic.dev/2.11/migration/
