---
name: ts-vercel-react-best-practices
description: React パフォーマンス最適化のための Vercel ガイドラインを適用するスキル。ts/web の React コンポーネント実装、レビュー、リファクタリング、描画最適化時に使用する。
---

# ts-vercel-react-best-practices

このスキルは Vercel の React 最適化ルールセットを参照する。対象は `apps/ts/packages/web`（Vite + React）で、フレームワーク固有ルールは適用可否を判断して使う。

## References

- Rule catalog: `rules/*.md`
- Detailed handbook: `AGENTS.md`
- Web package context: `apps/ts/packages/web/AGENTS.md`

## Usage

1. 対象コンポーネントのボトルネック分類（waterfall, bundle, rerender など）。
2. 該当カテゴリの `rules/*.md` を開く。
3. Vite/React 構成で適用可能なルールのみ採用する（フレームワーク専用 API は無理に使わない）。
4. 修正後に型・テスト・描画挙動を検証する。
