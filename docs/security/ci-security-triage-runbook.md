# CI Security Triage Runbook

## Purpose
Define triage steps when CI security automation fails (`secret-scan`, `dependency-vulnerability-audit`).

## Scope
- Secret scan: `secret-scan` job (`gitleaks`)
- Dependency vulnerability audit: `dependency-vulnerability-audit` job (`bun audit`, `pip-audit`)

## 1) Secret Scan Failure (`secret-scan`)

### Immediate steps
1. Open failed workflow log and identify the reported file and line.
2. Check whether the value is a real secret or test/example text.
3. If it is a real secret:
   - Revoke/rotate immediately.
   - Remove the secret from repository contents and history if needed.
   - Open an incident issue and document impact.
4. If it is a false positive:
   - Replace literal with safe fixture text if possible.
   - If replacement is not possible, add an explicit inline allow comment with rationale.

### Verification
Run locally before re-push:

```bash
docker run --rm -v "$(pwd):/repo" ghcr.io/gitleaks/gitleaks:v8.25.1 \
  detect --source="/repo" --no-git --redact --verbose
```

## 2) Bun Vulnerability Audit Failure (`bun audit`)

### Immediate steps
1. Reproduce locally:

```bash
cd apps/ts
bun install --frozen-lockfile
bun audit --audit-level=moderate
```

2. Identify vulnerable package and the dependency path.
3. Prefer direct package upgrade first, then transitive resolution.
4. If no safe upgrade exists:
   - Pin/override to patched transitive version, or
   - Temporarily document risk acceptance with expiration date.

## 3) Python Vulnerability Audit Failure (`pip-audit`)

### Immediate steps
1. Reproduce locally:

```bash
cd apps/bt
uv run --locked --with pip-audit pip-audit
```

2. Identify affected package and advisory ID.
3. Upgrade dependency in `pyproject.toml` and refresh `uv.lock`.
4. Re-run audit and project test suites.

## 4) PR and Issue Handling
1. Link failing job URL in the tracking issue.
2. Classify severity:
   - High/Critical: hotfix priority
   - Medium: next patch cycle
   - Low: scheduled backlog
3. Document:
   - root cause
   - chosen remediation
   - rollback plan (if any)
   - completion date
