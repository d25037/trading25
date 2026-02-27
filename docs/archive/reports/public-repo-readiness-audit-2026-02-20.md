# Public Repository Readiness Audit (2026-02-20)

## Scope
- Repository-wide static review for public化 readiness.
- Focus areas: secret exposure, security posture, governance artifacts, and operational safety for open-source publication.

## Method
- Pattern-based secret scan (`rg`) on tracked files.
- Manual review of core operational/security-related files.
- Check for OSS governance files commonly expected in public repos.

## Findings

### ✅ Positive findings
1. **No obvious leaked credentials in tracked files**
   - High-risk token/private-key patterns were not detected by repository-wide `rg` scan.
2. **Environment files are ignored by default**
   - `.env` and `.env.*` are ignored in root `.gitignore`.
3. **Runtime databases/ephemeral artifacts are mostly excluded**
   - DB, cache, logs, and test artifacts are broadly ignored.
4. **CORS is currently restricted to local development origins**
   - FastAPI CORS origins are localhost/127.0.0.1 only.

### ⚠️ Risks / gaps before making repo public

#### High priority
1. **Encryption key file may be accidentally committed**
   - `SecureEnvManager` defaults key path to `./.trading25.key` (current working directory).
   - Root `.gitignore` does **not** currently ignore `.trading25.key`.
   - Risk: local encryption key can be committed if generated from repo root.

2. **No published security disclosure policy**
   - `SECURITY.md` is missing at repo root.
   - Public users/researchers have no documented vuln reporting path or SLA.

#### Medium priority
3. **No explicit OSS license file**
   - `LICENSE` is missing.
   - Public repository without clear license is legally ambiguous for contributors/users.

4. **No CODEOWNERS for review boundaries**
   - `.github/CODEOWNERS` is missing.
   - Ownership/mandatory review routing is undefined for public contributions.

5. **No dependency/security automation metadata detected**
   - Dependabot config and dedicated secret scanning workflow/config were not found.
   - CI currently emphasizes lint/type/test quality, but not supply-chain/security scanning.

#### Low priority / operational
6. **No `.env` template for safer onboarding**
   - `.env` is ignored (good), but no `.env.example` / `.env.template` was found.
   - New external contributors may create ad-hoc env files inconsistently.

## Recommended action plan

### Phase A (must-do before public)
1. Add `.trading25.key` to `.gitignore` (and optionally `**/.trading25.key`).
2. Add `SECURITY.md` with:
   - reporting channel,
   - supported versions,
   - disclosure policy/timeline.
3. Add `LICENSE` (e.g., MIT/Apache-2.0/proprietary choice explicitly).

### Phase B (strongly recommended)
4. Add `.github/CODEOWNERS` for `apps/bt`, `apps/ts`, `contracts`, `docs`.
5. Add dependency/security automation:
   - `.github/dependabot.yml`,
   - secret scan in CI (e.g., gitleaks/detect-secrets),
   - vulnerability audit step (pip/bun ecosystem).

### Phase C (nice-to-have)
6. Add `.env.example` (no real secrets) documenting required vars:
   - `JQUANTS_API_KEY`, `JQUANTS_PLAN`, `API_BASE_URL`, `BT_API_URL`.
7. Add a short `docs/security/public-repo-hardening.md` runbook for maintainers.

## Notes
- This audit is static/repository-centric; runtime infrastructure hardening (cloud IAM, WAF, secret manager, network policies) must be reviewed separately before production public service exposure.
