# trading25 ロードマップ（現行インデックス）

最終更新: 2026-02-10

## 方針

- Phase 1-4 の大規模リファクタリングは完了。
- 長期の実行タスク管理は `issues/`（open）/`issues/done/`（closed）に移行。
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
- `bt-027` `issues/bt-027-phase1a-ta-regression-monitoring.md`

### Phase 2 延期項目
- `ts-125` `issues/ts-125-dataset-snapshot-manifest-output.md`
- `bt-028` `issues/bt-028-dataset-snapshot-reader-schema-validation.md`

### Phase 5
- `bt-029` `issues/bt-029-phase5-signal-overlay-api.md`
- `ts-126` `issues/ts-126-phase5-web-signal-markers.md`
- `bt-030` `issues/bt-030-phase5-new-indicators.md`

## クローズ済み（Phase 4 仕上げ）

- `issues/done/ts-124-phase4-ts-package-separation.md`
- `issues/done/bt-026-phase4-python-domain-package-split.md`

## Note

`packages/contracts` 作成タスクは、`contracts/` 直接管理方針の継続により現時点では採用しない（新規 Issue は作成しない）。
