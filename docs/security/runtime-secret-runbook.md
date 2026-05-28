# Runtime Secret Handling Runbook

## Purpose

Prevent accidental commit, logging, or local persistence of runtime secrets such as `JQUANTS_API_KEY`.

## Scope

- J-Quants API credentials
- Repo-external non-secret config in `~/.config/trading25/config.env`
- 1Password CLI / `op run --env-file ~/.config/trading25/secrets.env` based local execution
- Optional 1Password Service Account token stored in macOS Keychain for SSH/headless execution

## Baseline Rules

1. Do not commit runtime secret files.
2. Do not create repo root `.env` files for trading25 runtime config.
3. Keep non-secret values such as `JQUANTS_PLAN` in `~/.config/trading25/config.env`.
4. Keep secret references such as `JQUANTS_API_KEY=op://...` in `~/.config/trading25/secrets.env`.
5. Do not store `OP_SERVICE_ACCOUNT_TOKEN` in repo files or runtime env files.
6. If a temporary local export is required, place it outside the repository and delete it after use.

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

For SSH/headless starts where desktop app authentication is impractical, store a narrowly scoped 1Password Service Account token in macOS Keychain and let `scripts/dev-bt-server.sh` read it into the process environment only for `op run`. The J-Quants item must live in a vault the Service Account can access, and `~/.config/trading25/secrets.env` must point to that vault.

```bash
security add-generic-password \
  -a "$USER" \
  -s trading25-op-service-account-token \
  -w '<OP_SERVICE_ACCOUNT_TOKEN>' \
  -U

TRADING25_OP_SERVICE_ACCOUNT_TOKEN_KEYCHAIN_SERVICE=trading25-op-service-account-token \
scripts/dev-bt-server.sh
```

The Keychain item can also use a fixed account name instead of the macOS user:

```bash
TRADING25_OP_SERVICE_ACCOUNT_TOKEN_KEYCHAIN_ACCOUNT=trading25 \
TRADING25_OP_SERVICE_ACCOUNT_TOKEN_KEYCHAIN_SERVICE=trading25-op-service-account-token \
scripts/dev-bt-server.sh
```

The 1Password SSH agent socket (`SSH_AUTH_SOCK` / `IdentityAgent`) is only for SSH and Git private-key signing. It can make SSH login smoother, but it does not authenticate `op run` for secret-reference resolution.

## Rotation Procedure

Use when leakage is suspected or periodic rotation is required.

1. Rotate the secret in 1Password or the configured secret manager.
2. Revoke the old J-Quants API key at the provider if leakage is suspected.
3. Confirm `~/.config/trading25/secrets.env` points to the rotated `op://...` reference.
4. If a Service Account token is used, rotate it and update the macOS Keychain item.
5. Start `bt server` through `scripts/dev-bt-server.sh` and verify `/api/jquants/auth/status` reports `hasApiKey=true`.
6. Securely dispose of any temporary local exports if they were created for recovery.

## Incident Notes

- If a secret was committed, rotate it before history cleanup.
- If a secret appeared in logs, treat those logs as sensitive artifacts until retention or deletion is confirmed.
