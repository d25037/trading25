# trading25 ロードマップ（現行インデックス）

最終更新: 2026-04-22

## 方針

- Phase 1-4 の大規模リファクタリングは完了。
- 長期の実行タスク管理は GitHub Issues に移行済み。
- repo 内 `issues/` は local issue 時代の archive とし、active queue には使わない。
- 旧統合ロードマップ本文は履歴として archive 化。

## 履歴（archive）

- `docs/archive/unified-roadmap-2026-02-10.md`

## Phase 状態（2026-02-10 時点）

| Phase | 状態 | 備考 |
|---|---|---|
| 1 | 完了（1A運用課題あり） | 回帰監視タスクを Issue 化 |
| 2 | 実質完了 | 延期項目は Issue 化 |
| 3 | 完了 | FastAPI 一本化完了 |
| 4 | 完了 | packages分離・責務再配置完了 |
| 5 | 未着手 | 実装タスクを Issue 化 |

## Active Issues（roadmap 由来）

### Phase 1A
- `bt-027` [#341](https://github.com/d25037/trading25/issues/341) Phase 1A: TA 回帰監視基盤の実装

### Phase 2 延期項目
- `ts-125` [#359](https://github.com/d25037/trading25/issues/359) Dataset snapshot manifest 出力の実装
- `bt-028` [#342](https://github.com/d25037/trading25/issues/342) Dataset snapshot reader とスキーマ検証の実装

### Phase 5
- `bt-029` [#343](https://github.com/d25037/trading25/issues/343) Phase 5A: Signal Overlay API 実装
- `ts-126` [#360](https://github.com/d25037/trading25/issues/360) Phase 5B: Web シグナルマーカー表示
- `bt-030` [#344](https://github.com/d25037/trading25/issues/344) Phase 5C: 新規インジケータ追加

## クローズ済み（Phase 4 仕上げ）

- `issues/done/ts-124-phase4-ts-package-separation.md`
- `issues/done/bt-026-phase4-python-domain-package-split.md`

## Local Issue Archive

- GitHub 移行前に close 済みだった local issue は `issues/done/` に残す。
- 2026-04-22 に GitHub Issues へ移行した元 open issue は `issues/archive/migrated-to-github/` に残す。
- 旧 local issue ID（例: `bt-027`）は GitHub Issue タイトルに保持する。

## Note

`packages/contracts` 作成タスクは、`contracts/` 直接管理方針の継続により現時点では採用しない（新規 Issue は作成しない）。
