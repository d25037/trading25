---
id: ts-141
title: "TS 依存宣言 drift を防ぐ audit guardrail 追加"
status: done
priority: medium
labels: [tooling, ci, dependencies]
project: ts
created: 2026-03-16
updated: 2026-03-23
depends_on: [ts-139]
blocks: []
parent: ts-138
---

# ts-141 TS 依存宣言 drift を防ぐ audit guardrail 追加

## 目的
- `apps/ts` の dependency declaration drift を検出する軽量 audit を導入し、未使用依存や pin ずれを早期に止める

## 受け入れ条件
- root command から dependency audit を実行できる
- allowlist 付きで unused dependency と missing declaration と override drift を検出できる
- script の unit test があり、local quality flow か CI のいずれかに組み込まれている

## 実施内容
- [x] audit script と pure helper を `apps/ts/scripts/` に追加する
- [x] allowlist と override drift ルールを明文化する
- [x] root task / script から呼び出せるようにする
- [x] script test を追加し、workspace test で実行する

## 結果
- `apps/ts/scripts/dependency-audit.ts` と `dependency-audit-lib.ts` を追加し、unused dependency / missing declaration / override drift を検出できるようにした。
- allowlist と override drift の扱いを audit config に明文化し、`monaco-editor` や tooling dependency の扱いを整理した。
- root command から `bun run quality:deps:audit` で呼べるようにし、`quality:typecheck` フローへ組み込んだ。
- script test を追加し、`root:test` と `workspace:test` で常時実行されるようにした。
- 実行結果:
  - `bun run quality:deps:audit` ✅
  - `bun run root:test` ✅
  - `bun run workspace:test` ✅
  - `bun run quality:typecheck` ✅

## 補足
- security audit とは別物で、目的は manifest と実 usage の整合確認
- root override-only pin は runtime dependency と区別して扱う
