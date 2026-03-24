# Documentation

`apps/bt` のドキュメント入口です。

## Primary References

- [AGENTS.md](../AGENTS.md): 運用ルール・責務・開発ガイド
- [CLAUDE.md](../CLAUDE.md): プロジェクト補足ドキュメント（AGENTS.md へのシンボリックリンク）
- [experiments/](./experiments/README.md): notebook / domain 実験の索引と baseline
- [vectorbt/](./vectorbt/README.md): VectorBT 関連リファレンス

## Notes

- 現行バックテスト基盤は VectorBT 前提です。
- API は FastAPI (`:3002`) を使用します。
- 実験ノートは `docs/experiments/` に集約し、実験コード本体は `notebooks/playground/` を SoT とします。
