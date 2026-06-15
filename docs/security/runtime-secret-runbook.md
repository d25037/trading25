# Runtime Secret Handling Runbook

## Purpose

Prevent accidental commit, logging, or local persistence of runtime secrets such as `JQUANTS_API_KEY`.

## Scope

- J-Quants API credentials
- Repo-external non-secret config in `~/.config/trading25/config.env`
- macOS Keychain based local and SSH/headless execution
- Temporary `JQUANTS_API_KEY` environment override for break-glass recovery

## Baseline Rules

1. Do not commit runtime secret files.
2. Do not create repo root `.env` files for trading25 runtime config.
3. Keep non-secret values such as `JQUANTS_PLAN` and Keychain service/account names in `~/.config/trading25/config.env`.
4. Store the J-Quants API key itself in each Mac's Keychain.
5. Do not store `JQUANTS_API_KEY` in repo files, `~/.config/trading25/config.env`, shell startup files, or persistent env files.
6. If a temporary local export is required, keep it shell-local and unset it after use.

## Local Verification

Run from repository root:

```bash
git ls-files | rg '(^|/)\.env(\.|$)|trading25\.key'
```

Expected result: no output.

Check local runtime files:

```bash
ls -l ~/.config/trading25/config.env
```

The default wrapper path is `JQUANTS_API_KEY` environment override first, then macOS Keychain. Register the J-Quants API key on every Mac that runs `bt server`, including SSH targets:

```bash
security add-generic-password \
  -a trading25 \
  -s trading25-jquants-api-key \
  -w '<JQUANTS_API_KEY>' \
  -U
```

Keep the Keychain lookup metadata outside the repository:

```bash
# ~/.config/trading25/config.env
JQUANTS_PLAN=standard
TRADING25_JQUANTS_API_KEY_KEYCHAIN_SERVICE=trading25-jquants-api-key # gitleaks:allow
TRADING25_JQUANTS_API_KEY_KEYCHAIN_ACCOUNT=trading25
```

Verify the wrapper without printing the API key:

```bash
TRADING25_DRY_RUN=1 scripts/dev-bt-server.sh
```

Expected output includes `J-Quants auth mode: keychain`. If Keychain access is temporarily broken, use a shell-local break-glass override:

```bash
read -s JQUANTS_API_KEY
export JQUANTS_API_KEY
scripts/dev-bt-server.sh
unset JQUANTS_API_KEY
```

Do not add this override to `.zshrc`, `config.env`, repo `.env`, or other persistent files.

## Rotation Procedure

Use when leakage is suspected or periodic rotation is required.

1. Revoke or rotate the J-Quants API key at the provider.
2. Update the Keychain item on every Mac that runs `bt server`.
3. Confirm `~/.config/trading25/config.env` points to the intended Keychain service/account.
4. Start `bt server` through `scripts/dev-bt-server.sh` and verify `/api/jquants/auth/status` reports `hasApiKey=true`.
5. Securely dispose of any temporary local exports if they were created for recovery.

## Incident Notes

- If a secret was committed, rotate it before history cleanup.
- If a secret appeared in logs, treat those logs as sensitive artifacts until retention or deletion is confirmed.
