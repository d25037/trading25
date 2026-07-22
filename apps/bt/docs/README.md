# Documentation

`apps/bt` のドキュメント入口です。

## Primary References

- [AGENTS.md](../AGENTS.md): 運用ルール・責務・開発ガイド
- [strategies.md](./strategies.md): 戦略YAML・CLI・signal metadata・optimizationの現行ガイド
- [experiments/](./experiments/README.md): runner / bundle / notebook / baseline 実験の索引
- [vectorbt/](./vectorbt/README.md): VectorBT 関連リファレンス
- Market DB sync は既定の `incremental` と破壊的な `initial` の2モードのみを使います。`initial` は `resetBeforeSync=true` を必須とし、既存 `market.duckdb` / `parquet/` を削除して再構築します。

## Notes

- 現行バックテスト基盤は VectorBT 前提です。
- API は FastAPI (`:3002`) を使用します。
- research ロジックの SoT は `src/domains/analytics/`、再現可能な実行導線の SoT は `scripts/research/`、notebook は bundle viewer surface として扱います。
- 長く残す research note は `docs/experiments/<experiment>/README.md` を canonical note にし、`baseline-YYYY-MM-DD.md` を時点固定メモに使います。
