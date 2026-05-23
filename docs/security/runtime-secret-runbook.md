# Runtime Secret Handling Runbook

## Purpose

Prevent accidental commit, logging, or local persistence of runtime secrets such as `JQUANTS_API_KEY`.

## Scope

- J-Quants API credentials
- Repo-external non-secret config in `~/.config/trading25/config.env`
- 1Password CLI / `op run --env-file ~/.config/trading25/secrets.env` based local execution

## Baseline Rules

1. Do not commit runtime secret files.
2. Do not create repo root `.env` files for trading25 runtime config.
3. Keep non-secret values such as `JQUANTS_PLAN` in `~/.config/trading25/config.env`.
4. Keep secret references such as `JQUANTS_API_KEY=op://...` in `~/.config/trading25/secrets.env`.
5. If a temporary local export is required, place it outside the repository and delete it after use.

## Local Verification

Run from repository root:

```bash
git ls-files | rg '(^|/)\.env(\.|$)|trading25\.key'
```

Expected result: no output.

Check local runtime files:

```bash
ls -l ~/.config/trading25/config.env ~/.config/trading25/secrets.env
```

## Rotation Procedure

Use when leakage is suspected or periodic rotation is required.

1. Rotate the secret in 1Password or the configured secret manager.
2. Revoke the old J-Quants API key at the provider if leakage is suspected.
3. Confirm `~/.config/trading25/secrets.env` points to the rotated `op://...` reference.
4. Start `bt server` through `scripts/dev-bt-server.sh` and verify `/api/jquants/auth/status` reports `hasApiKey=true`.
5. Securely dispose of any temporary local exports if they were created for recovery.

## Incident Notes

- If a secret was committed, rotate it before history cleanup.
- If a secret appeared in logs, treat those logs as sensitive artifacts until retention or deletion is confirmed.
