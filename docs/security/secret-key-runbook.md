# Secret Key Handling Runbook (`.trading25.key`)

## Purpose
Prevent accidental commit and misuse of the local encryption key file used by `SecureEnvManager`.

## Scope
- Managed file name: `.trading25.key`
- Related component: `apps/ts/packages/shared/src/utils/secure-env-manager.ts`

## Baseline Rules
1. Do not commit `.trading25.key` to Git.
2. Keep file permission as `0600`.
3. Store the key outside tracked source directories when possible.

## Tracking Check (Before PR / Release)
Run from repository root:

```bash
git ls-files | rg 'trading25\.key'
```

Expected result: no output.

If any file is listed:
1. Remove from index:
```bash
git rm --cached <path-to-key-file>
```
2. Confirm local ignore rules:
```bash
rg -n 'trading25\.key' .gitignore
```
3. Re-check:
```bash
git ls-files | rg 'trading25\.key'
```

## Permission Check
```bash
ls -l <path-to-.trading25.key>
```

Expected mode: owner read/write only (`-rw-------`, `600`).

If permission is too broad:
```bash
chmod 600 <path-to-.trading25.key>
```

## Rotation Procedure
Use when leakage is suspected or periodic rotation is required.

1. Backup encrypted token values (`.env`) in a secure local location.
2. Delete old key file.
3. Generate a new key via application flow that initializes `SecureEnvManager`.
4. Re-encrypt token(s) with the new key.
5. Verify decryption works in local runtime.
6. Securely dispose of old backups if no longer required.

## Incident Notes
- Treat leaked key as credential compromise.
- Rotate key and encrypted token payloads together.
- Record incident date, affected environment, and completion timestamp in internal ops notes.
