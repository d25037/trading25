---
id: ts-129
title: Coverage backlog reorganization
status: open
priority: medium
labels: [test, coverage, maintenance]
project: ts
created: 2026-02-13
updated: 2026-02-13
depends_on: [ts-111, ts-113, ts-114, ts-014c]
blocks: []
parent: null
---

# ts-129 Coverage backlog reorganization

## 目的
- 既存の coverage 関連Issue（open/done）を棚卸しし、重複や依存関係の曖昧さを解消する。
- 今後の実行順序が迷わない状態に整理する。

## 対象（open）
- `ts-111` shared 80/80
- `ts-113` cli 70/70
- `ts-114` web 45/70
- `ts-014c` cli 90/90（stretch）

## 整理方針
- CLI は段階目標として管理:
  - `ts-113` (70/70) → `ts-014c` (90/90)
- shared/web は独立タスクとして並行可能:
  - `ts-111`, `ts-114`
- 上記4件を本Issueの子タスクとして統合管理する。

## 受け入れ条件
- coverage関連の open Issue に parent/depends_on/blocks が反映されている。
- CLI の段階目標（70/70 → 90/90）の依存関係が明文化されている。
- 既存 done Issue は履歴として維持し、open の重複管理がない状態になっている。

## 実施内容
- [x] coverage関連 open Issue の棚卸し
- [x] `ts-111/113/114/014c` の frontmatter を統一（labels/updated/parent）
- [x] CLI 目標の依存関係（`ts-113` blocks `ts-014c`、`ts-014c` depends_on `ts-113`）を設定
- [ ] done履歴の `status: closed` を `done/wontfix` へ正規化（必要時）
- [ ] 実測カバレッジを採取し、優先順位（shared/web/cli）を再評価

## 結果
- 進行中

## 補足
- done 履歴（coverage系）:
  - `ts-001a` `ts-001b` `ts-001c` `ts-001d` `ts-001e`
  - `ts-014a` `ts-014b` `ts-014d`
  - `ts-112` `ts-117`
  - `bt-015` `bt-016`
