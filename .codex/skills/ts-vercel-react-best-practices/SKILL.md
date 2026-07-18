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
- Provenance manifest: `react-catalog-provenance.json`
- The source verifier normalizes every source and vendored file by decoding UTF-8, converting CRLF/CR to LF, stripping trailing spaces/tabs per line, removing trailing blank lines, and appending exactly one LF.
- The aggregate rules digest processes files by sorted basename and hashes `UTF-8 basename + NUL + normalized UTF-8 content + NUL` for each file.
- Rule catalog: `rules/*.md`
- Detailed handbook: `AGENTS.md`
- Web package context: `apps/ts/packages/web/AGENTS.md`

## Usage

1. 対象コンポーネントのボトルネック分類（waterfall, bundle, rerender など）。
2. 該当カテゴリの `rules/*.md` を開く。
3. Vite/React 構成で適用可能なルールのみ採用する（フレームワーク専用 API は無理に使わない）。
4. 修正後に型・テスト・描画挙動を検証する。

## Catalog maintenance

Installed sourceとの照合を既定にする。installed pluginを利用できないCIでは、strict skill auditが同じmanifestを使うoffline local-inventory検証を行う。

```bash
python3 scripts/skills/verify_react_catalog.py
python3 scripts/skills/verify_react_catalog.py --offline
python3 scripts/skills/verify_react_catalog.py --refresh
```

自動探索できない場合だけ、`--source /absolute/path/to/build-web-apps/0.1.2/skills/react-best-practices` を明示する。refresh後はinstalled-source verificationとstrict skill auditの両方を実行する。
