# Security Best Practices Report (2026-02-24)

## Executive Summary

- `ts-131` 〜 `ts-134` の最終セキュリティチェックを再実施。
- Python/JavaScript 依存脆弱性は解消済み（`pip-audit` / `bun audit` ともにゼロ）。
- `window.open` の opener 保護不足は 3 箇所すべて修正済み。
- `gitleaks` は Docker で実行可能になり、現在の検出は **ローカル未追跡の `<repo-root>/.env` 1件のみ**。

## Scope / Method

1. local issue 進捗の整合確認（`ts-131`〜`ts-134`）
2. frontend opener 対策の実装確認
3. Python 依存脆弱性監査（`uv run --with pip-audit pip-audit`）
4. JS 依存脆弱性監査（`bun audit`）
5. secret scan（`gitleaks` Docker実行）
6. 追跡状態/権限確認（`git ls-files --error-unmatch`, `git check-ignore`, `ls -l`）

## Current Findings

### [Low] SBP-LOCAL-ENV-001: ローカル `.env` に実キーが存在（未追跡）

- **Location**
  - `<repo-root>/.env:2`
- **Evidence**
  - `gitleaks detect --source="/repo" --no-git` で `JQUANTS_API_KEY` を検出（1件）
  - `git ls-files --error-unmatch .env` は untracked（追跡なし）
  - `git check-ignore -v .env` は `.gitignore` により ignore
- **Impact**
  - Git への漏えいリスクは低いが、ローカル端末共有・バックアップ流出時の漏えい面は残る。
- **Fix**
  - 権限を `600` に設定済み（`-rw-------`）。
  - 必要に応じてキーのローテーションを実施。
- **Mitigation**
  - `.env` は追跡対象外であり、CI の checkout 環境には通常存在しない。

## Resolved Findings

### [Resolved] SBP-OPS-001: Docker daemon 停止による gitleaks 未実施

- **Resolution Evidence**
  - `docker version` で daemon 接続成功
  - `docker run ... gitleaks ...` 実行完了

### [Resolved] SBP-PY-001: Python依存脆弱性

- **Location**
  - `<repo-root>/apps/bt/uv.lock:373` (`fonttools`)
  - `<repo-root>/apps/bt/uv.lock:1240` (`pillow`)
  - `<repo-root>/apps/bt/uv.lock:2087` (`urllib3`)
- **Resolution Evidence**
  - `uv run --with pip-audit pip-audit` → `No known vulnerabilities found`
  - lock 更新:
    - `fonttools 4.61.1`
    - `pillow 12.1.1`
    - `urllib3 2.6.3`

### [Resolved] SBP-WEB-001: `window.open` opener 保護不足

- **Location**
  - `<repo-root>/apps/ts/packages/web/src/components/Backtest/HtmlFileBrowser.tsx:216`
  - `<repo-root>/apps/ts/packages/web/src/components/Backtest/OptimizationHtmlFileBrowser.tsx:170`
  - `<repo-root>/apps/ts/packages/web/src/components/Backtest/ResultHtmlViewer.tsx:34`
- **Resolution Evidence**
  - 3箇所すべて `window.open(url, '_blank', 'noopener,noreferrer')` に統一。

### [Resolved] SBP-TS-001: JS依存脆弱性（`bun audit`）

- **Location**
  - `<repo-root>/apps/ts/packages/web/package.json:52`
  - `<repo-root>/apps/ts/packages/web/package.json:53`
  - `<repo-root>/apps/ts/packages/web/package.json:58`
  - `<repo-root>/apps/ts/package.json:51`
- **Resolution Evidence**
  - `bun audit` → `No vulnerabilities found`
  - `vitest` / `@vitest/coverage-v8` / `@vitest/ui` を `4.0.18` に更新
  - `@redocly/openapi-core` を `2.19.1` override で固定

### [Resolved] SBP-SEC-SCAN-001: gitleaks 誤検知（テストダミー値）

- **Location (examples)**
  - `<repo-root>/apps/bt/tests/conftest.py:160`
  - `<repo-root>/apps/bt/tests/unit/server/clients/test_jquants_client.py:17`
  - `<repo-root>/apps/ts/packages/cli/src/utils/cli-token-manager.test.ts:150`
  - `<repo-root>/docs/security/secret-key-runbook.md:7`
- **Resolution Evidence**
  - テスト用トークンを低エントロピーのダミー値へ統一
  - runbook 文言を調整
  - 再スキャンで上記由来の検出は消失

## No-Finding Areas (Checked)

- 追跡ファイルとしての秘密情報（`.env`, `*.pem`, `*.p12`, credentials）
- `shell=True` 利用
- CORS ワイルドカード設定

## Verification Log (Post-fix)

- `cd apps/bt && uv run --with pip-audit pip-audit` ✅
- `cd apps/ts && bun audit` ✅
- `cd apps/ts && bun run --filter @trading25/shared bt:generate-types` ✅
- `cd apps/ts && bun run --filter @trading25/web test` ✅ (106 files / 836 tests)
- `cd apps/bt && uv run pytest ...` ✅ (117 tests)
- `cd apps/ts && bun run --filter @trading25/cli test` ✅ (89 tests)
- `docker run ... gitleaks ...` ⚠️ local `.env` のみ検出

## Recommended Next Action

1. `<repo-root>/.env` の API キーを継続利用するなら、定期ローテーション運用（runbook準拠）を実施。
