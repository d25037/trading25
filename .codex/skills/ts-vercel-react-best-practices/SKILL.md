---
name: ts-vercel-react-best-practices
description: Use when ts/web の React component実装、performance review、rerender・bundle・waterfall最適化、またはReact refactoringを行うとき。
---

# ts-vercel-react-best-practices

このスキルは Vercel の React 最適化ルールセットを参照する。対象は `apps/ts/packages/web`（Vite + React）で、フレームワーク固有ルールは適用可否を判断して使う。

## References

- Upstream package: `build-web-apps@0.1.2`
- Upstream catalog: `skills/react-best-practices`
- Catalog version: `1.0.0`
- The vendored `AGENTS.md` and 64-rule `rules/*.md` inventory are content-pinned by `scripts/skills/audit_skills.py` after trailing-whitespace normalization; refresh both together from that installed package.
- Rule catalog: `rules/*.md`
- Detailed handbook: `AGENTS.md`
- Web package context: `apps/ts/packages/web/AGENTS.md`

## Usage

1. 対象コンポーネントのボトルネック分類（waterfall, bundle, rerender など）。
2. 該当カテゴリの `rules/*.md` を開く。
3. Vite/React 構成で適用可能なルールのみ採用する（フレームワーク専用 API は無理に使わない）。
4. 修正後に型・テスト・描画挙動を検証する。
