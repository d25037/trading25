---
id: ts-119
title: Lab結果データのランタイムバリデーション追加
status: done
priority: low
labels: [web, lab, type-safety]
project: ts
created: 2026-02-02
updated: 2026-02-02
closed: 2026-02-02
depends_on: []
blocks: []
parent: null
---

# ts-119 Lab結果データのランタイムバリデーション追加

## 目的

LabPanel で `result_data` の discriminated union (`LabResultData`) をランタイムでバリデーションし、バックエンドが予期しないデータ形状を返した場合のクラッシュを防止する。

現在は TypeScript のコンパイル時型チェックのみに依存しており、`lab_type` フィールドと実際のデータ構造が不一致の場合に結果コンポーネント内でランタイムエラーが発生する可能性がある。

## 受け入れ条件

- `LabResultData` に対応する Zod スキーマを定義
- `LabPanel.tsx` で `result_data` を描画前にバリデーション
- バリデーション失敗時にユーザーフレンドリーなエラーメッセージを表示（クラッシュしない）
- 既存テストが壊れない

## 補足

- Code Review Phase 2 の Issue #5 (Confidence 77) から派生
- bt-openapi sync が完了し自動生成型に置換された後に実施するのが効率的

## 結果

- Zod v4 `discriminatedUnion` で `LabResultData` スキーマを定義（`lab-result-schemas.ts`）
- `validateLabResultData()` 関数で safeParse ベースのバリデーション実装
- 双方向型チェック（`Exact<A,B>`）でスキーマと TypeScript 型のドリフトをコンパイル時検出
- テスト12件（正常4variant + 異常7パターン + エラーパス情報）、カバレッジ100%
