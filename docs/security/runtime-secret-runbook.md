# Runtime Secret Handling Runbook

## Purpose

Prevent accidental commit, logging, or local persistence of runtime secrets such as `JQUANTS_API_KEY`.

## Scope

- J-Quants API credentials
- Repo-external runtime config referenced by `TRADING25_ENV_FILE`
- 1Password Environments / `op run --env-file` based local execution

## Baseline Rules

1. Do not commit runtime secret files.
2. Do not create repo root `.env` files for trading25 runtime config.
3. Prefer 1Password Environments for `JQUANTS_API_KEY`.
4. Keep non-secret values such as `JQUANTS_PLAN` in the same runtime config only when it simplifies local launch.
5. If a temporary local export is required, place it outside the repository and delete it after use.

## Local Verification

Run from repository root:

```bash
git ls-files | rg '(^|/)\.env(\.|$)|trading25\.key'
```

Expected result: no output.

Check the active runtime source:

```bash
printf '%s\n' "${TRADING25_ENV_FILE:-"(TRADING25_ENV_FILE not set)"}"
```

## Rotation Procedure

Use when leakage is suspected or periodic rotation is required.

1. Rotate the secret in 1Password or the configured secret manager.
2. Revoke the old J-Quants API key at the provider if leakage is suspected.
3. Confirm the repo-external runtime config or `op run --env-file` path resolves the new value.
4. Start `bt server` and verify `/api/jquants/auth/status` reports `hasApiKey=true`.
5. Securely dispose of any temporary local exports if they were created for recovery.

## Incident Notes

- If a secret was committed, rotate it before history cleanup.
- If a secret appeared in logs, treat those logs as sensitive artifacts until retention or deletion is confirmed.
