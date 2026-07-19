# Task 4 report — Harden secret scanning and workflow reproducibility

## Status

Fixed and committed on `codex/repo-governance-modernization`.

- Commit: `3d4861b8` (`ci: harden workflow security and reproducibility`)
- Outcome: docs-only changes can no longer bypass privacy or secret scanning;
  gitleaks scans the relevant Git commit range; both workflows are read-only,
  credential-safe, and reproducibly pinned.
- Scope: the five Task 4 implementation/test files only. Existing Task 1–3
  commits were preserved.

## Security boundary and patch strategy

The attacker-controlled input is repository content introduced by a pull
request or push, including documentation. Before this task, `docs_only` skipped
both `repo-guardrails` (which owns the privacy check) and `secret-scan`, and the
secret scan used `--no-git`, so secrets present in commit history but absent
from the checked-out working tree were outside the scan boundary.

The enforced invariant is:

1. every CI change, including docs-only changes, must complete privacy and
   secret scanning;
2. gitleaks must inspect the Git commits introduced by the event;
3. workflow automation has read-only repository permission, does not retain
   checkout credentials, and does not execute mutable action/tool versions;
4. only the known synthetic `sma_atr_bands_2_3_1.0` assertion may be excluded,
   and only for the `generic-api-key` rule at its exact test path and line;
5. Nautilus remains an unconditional `main` push smoke, while pull requests run
   it only for its workflow, dependency lock/config, bt runtime, smoke test, or
   smoke script.

The narrow repository-native patch removes the two docs-only job conditions,
makes both jobs unconditionally required by `ci-gate`, computes a PR/push Git
range with a one-commit fallback, and invokes current gitleaks using its real
`git <repo>` interface. Product/research/contract/dependency path selection is
otherwise unchanged.

## TDD evidence

### Initial RED

Workflow tests were added before workflow/config edits for top-level
permissions, checkout credentials, immutable action pins, centralized tool
versions, docs-only security requirements, Git-aware gitleaks, and Nautilus
path scope. The docs-only fixture was changed to model privacy/secret scan as
always successful while leaving product-only jobs skipped.

```bash
uv run --directory apps/bt pytest \
  tests/unit/scripts/test_ci_workflow.py \
  tests/unit/scripts/test_ci_changed_scope.py -q
```

Result: `13 failed, 41 passed`. Each failure corresponded to an audited gap:
missing permissions/env, persisted credentials, mutable action references,
unpinned tools, docs-only job conditions/gate behavior, `--no-git`, or an
unscoped Nautilus pull request trigger.

### Integration RED 1 — current gitleaks CLI

After the first workflow GREEN, the checksum-verified official gitleaks 8.30.1
binary rejected the workflow-shaped invocation:

```text
Error: unknown flag: --source
Usage: gitleaks git [flags] [repo]
```

A regression assertion requiring `git "/repo"` and forbidding `--source` was
added first. It failed against the workflow, then passed after switching to the
current positional repository argument.

### Integration RED 2 — allowlist parser compatibility

The next real scan failed closed while loading configuration:

```text
[allowlist] is deprecated, it cannot be used alongside [[allowlists]]
```

A TOML regression test first failed because the legacy top-level `allowlist`
still existed. The existing four UI/storage-key regexes were then migrated
without broadening from `[allowlist]` to a second `[[allowlists]]` entry.

### Integration RED 3 — event fallback range

An exploratory `--all` scan proved the parser worked but found 32 historical
fixture findings across 1,557 old commits. Since normal PR/push behavior uses
the introduced commit range, using `--all` only for dispatch/zero-before would
make that fallback permanently unusable. A test first rejected `--all` and
required `-1 ${{ github.sha }}`; the fallback was then changed to scan the
current head commit while preserving full PR and push ranges.

### Integration RED 4 — exact synthetic key

A working-tree scan identified the only current finding at
`apps/bt/tests/unit/server/services/test_indicator_service.py:296`: the exact
assertion for synthetic value `sma_atr_bands_2_3_1.0`.

The initial registry-file hypothesis was therefore rejected. The allowlist
test was changed first and failed on the old path/regex. The config was then
narrowed to all of the following simultaneously:

- rule: `generic-api-key`;
- condition: `AND`;
- regex target: complete line;
- path: exactly `test_indicator_service.py`;
- line: exactly the synthetic-key assertion.

### GREEN

```bash
uv run --directory apps/bt pytest \
  tests/unit/scripts/test_ci_workflow.py \
  tests/unit/scripts/test_ci_changed_scope.py -q
```

Final result: `55 passed, 1 existing warning`.

## Version and action provenance

Official release/tag sources were queried rather than guessing hashes.

- Bun: official `bun-v1.3.14` release, fixed as `BUN_VERSION=1.3.14`.
- uv: official latest `0.11.29` release at verification time, fixed as
  `UV_VERSION=0.11.29` instead of `latest`.
- gitleaks: official latest `v8.30.1` release at verification time, fixed as
  `GITLEAKS_VERSION=8.30.1`.
- `actions/checkout@v7.0.0`:
  `9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0`.
- `actions/setup-python@v6.2.0`:
  `a309ff8b426b58ec0e2a45f0f869d46889d02405`.
- `astral-sh/setup-uv@v8.3.2`:
  `11f9893b081a58869d3b5fccaea48c9e9e46f990`.
- `actions/download-artifact@v8.0.0`:
  `70fc10c6e5e1ce46ad2ea6f2b72d43f7d47b13c3`.
- `actions/upload-artifact@v7.0.0`:
  `bbbca2ddaa5d8feaa63e36b76fdaad77386f024f`.
- `actions/cache@v6.0.0`:
  `2c8a9bd7457de244a408f35966fab2fb45fda9c8`.

All external `uses:` lines in both workflows are now 40-character SHAs with a
readable exact-version comment.

## Ordered security verification

### Applicability and buildability

```bash
uv run --directory apps/bt pytest \
  tests/unit/scripts/test_ci_workflow.py \
  tests/unit/scripts/test_ci_changed_scope.py -q
uv run --directory apps/bt ruff check \
  tests/unit/scripts/test_ci_workflow.py \
  tests/unit/scripts/test_ci_changed_scope.py
git diff --check
```

Post-commit results: `55 passed`; Ruff reported `All checks passed!`; whitespace
check exited 0.

The checksum-verified official actionlint 1.7.12 binary parsed both workflows
successfully. It emitted one pre-existing warning for the folded `web-e2e`
`${{ ... }}` condition at `ci.yml:495`; no Task 4 line produced a warning or
error, and actionlint exited 0.

### Security closure

The official gitleaks archive and its official checksum list were downloaded;
`shasum -a 256 -c` returned `OK`, and the binary reported `8.30.1`.

Exact committed change range:

```bash
gitleaks git . --config=.gitleaks.toml \
  --log-opts='HEAD^..HEAD' --redact --verbose
```

Result: `1 commits scanned`, approximately 12.20 KB, `no leaks found`.

Current checkout including all files:

```bash
gitleaks dir . --config=.gitleaks.toml --redact --verbose
```

Result: approximately 60.15 MB scanned, `no leaks found`.

Fallback syntax was separately exercised with `--log-opts='-1 HEAD'`; it
scanned one commit and found no leaks. PR and push forms are asserted to retain
their `base..head` / `before..head` ranges.

### Change-aware bypass review

- The gitleaks job has no job-level `if`, cannot use `--no-git` or `--source`,
  and is always required by the final gate.
- The privacy-owning `repo-guardrails` job likewise has no job-level `if` and
  is always required.
- A missing/malformed scope output still fails before required-job evaluation;
  the Task 1 fail-closed scope behavior is unchanged.
- Every checkout step is enumerated by the regression test and must set parsed
  boolean `persist-credentials: false`.
- Every external action line is enumerated and must match a full SHA plus
  version comment; a new mutable action fails the test.
- The synthetic allowlist does not match another path, another rule, a partial
  line, or another `sma_atr_bands` value because all criteria are required.

### Preserved behavior

```bash
python3 scripts/check-privacy-leaks.py
```

Result: `[privacy-leak-check] OK`.

The existing non-product/docs-only gate acceptance test passes when both
security jobs succeed, while new negative tests reject either job being
skipped. Main pushes in `nautilus-smoke.yml` retain exactly
`branches: [main]`; only `pull_request` has a path filter, and manual dispatch
remains available.

## Changed files

- `.github/workflows/ci.yml`
- `.github/workflows/nautilus-smoke.yml`
- `.gitleaks.toml`
- `apps/bt/tests/unit/scripts/test_ci_workflow.py`
- `apps/bt/tests/unit/scripts/test_ci_changed_scope.py`

The requested report is intentionally outside the implementation commit, as in
the prior SDD task handoffs.

## Self-review

- The commit contains exactly the five Task 4 files; the concurrently modified
  Task 3 report and the untracked plan/spec files were not staged.
- Task 1 path classification remains intact: docs still classify as docs-only
  with `security_ci=false`; mandatory security jobs are enforced at the
  workflow/final-gate boundary instead of pretending dependency audit should
  run for docs.
- Task 2 TypeScript build/coverage steps remain in place; only their runtime and
  action references changed to fixed versions/SHAs.
- Task 3 package-removal changes are untouched.
- No broad path, commit, stopword, or generic-regex suppression was added.
- Failure remains closed at change classification, privacy, gitleaks process,
  and final required-job evaluation.

## Concerns

- Full `--all` history scanning currently reports 32 old synthetic/example
  findings. CI intentionally scans the commits introduced by each event, not
  the entire legacy history, so current and future event ranges remain usable
  and fail closed. A separate history-cleanup/baseline project would be needed
  before whole-history scanning could become a required gate.
- actionlint's existing folded `web-e2e` condition warning remains outside Task
  4 scope. The repository's own workflow regression test still pins that
  existing condition, and Task 4 did not alter its behavior.

## Review fix — complete Nautilus scope and content-address tool execution

### Findings

The first Task 4 commit left two review gaps:

1. The Nautilus pull-request path filter covered the dedicated smoke script,
   bt sources, lock/config, and smoke test, but omitted the shared runtime
   launcher `scripts/bt-run.sh` and shared pytest bootstrap
   `apps/bt/tests/conftest.py`.
2. Action references were immutable, but tool acquisition was not fully
   content-addressed. The Bun install step piped the mutable installer response
   into Bash, and gitleaks used only a container tag.

### Upstream provenance audit

No hash or digest was inferred.

- The official `oven-sh/setup-bun` latest release was `v2.2.0`, commit
  `0c5077e51419868618aeaa5fe8019c62421857d6`. Its pinned implementation was
  inspected: it downloads the release zip with `downloadTool`, extracts it,
  and checks the executable version, but does not validate the release
  artifact checksum. It was therefore not used for this content-addressed
  boundary.
- The official Bun 1.3.14 `SHASUMS256.txt` records
  `951ee2aee855f08595aeec6225226a298d3fea83a3dcd6465c09cbccdf7e848f`
  for `bun-linux-x64.zip`. The artifact was independently downloaded and
  `shasum -a 256 -c` returned `OK`.
- The official GHCR `gitleaks/gitleaks:v8.30.1` response returned OCI index
  digest
  `sha256:c00b6bd0aeb3071cbcb79009cb16a60dd9e0a7c60e2be9ab65d25e6bc8abbb7f`.
  The manifest body was downloaded and independently hashed to the same
  digest. The image reference now includes both the readable version tag and
  this verified index digest.

### Review RED

Before implementation, workflow tests were extended to require:

- both omitted Nautilus paths;
- the exact official Bun artifact URL, hard-coded archive SHA-256 verification,
  and absence of the mutable installer pipeline in every Bun job;
- the exact gitleaks OCI digest in the Docker image reference; and
- both content digests in the centralized workflow environment.

```bash
uv run --directory apps/bt pytest \
  tests/unit/scripts/test_ci_workflow.py \
  tests/unit/scripts/test_ci_changed_scope.py -q
```

Result before workflow edits: `4 failed, 53 passed`. The failures were exactly
the missing centralized digests, mutable Bun installer, mutable gitleaks tag,
and incomplete Nautilus path list.

### Review GREEN

- Added `scripts/bt-run.sh` and `apps/bt/tests/conftest.py` to the Nautilus PR
  paths without adding path filters to `push` or `workflow_dispatch`.
- Replaced all seven Bun installer pipelines with a download from the exact Bun
  1.3.14 release URL, `sha256sum -c` against the repository-pinned digest,
  extraction, and installation under `RUNNER_TEMP`.
- Changed the gitleaks image to
  `v8.30.1@sha256:c00b6bd0aeb3071cbcb79009cb16a60dd9e0a7c60e2be9ab65d25e6bc8abbb7f`
  through separately pinned version/digest environment values.

Focused workflow/taxonomy tests after the fix: `57 passed, 1 existing warning`.

### Review verification and self-review

The following checks were rerun after the review changes:

```bash
uv run --directory apps/bt pytest \
  tests/unit/scripts/test_ci_workflow.py \
  tests/unit/scripts/test_ci_changed_scope.py -q
uv run --directory apps/bt ruff check \
  tests/unit/scripts/test_ci_workflow.py \
  tests/unit/scripts/test_ci_changed_scope.py
python3 scripts/check-privacy-leaks.py
gitleaks git . --config=.gitleaks.toml \
  --log-opts='HEAD^..HEAD' --redact --verbose
gitleaks dir . --config=.gitleaks.toml --redact --verbose
actionlint .github/workflows/ci.yml .github/workflows/nautilus-smoke.yml
git diff --check
```

Results before the review-fix commit:

- pytest: `57 passed`;
- Ruff: `All checks passed!`;
- privacy: `[privacy-leak-check] OK`;
- gitleaks commit range: one commit scanned, `no leaks found`;
- gitleaks checkout: approximately 60.20 MB scanned, `no leaks found`;
- whitespace: passed;
- actionlint: no new warning or error. It repeated only the pre-existing folded
  `web-e2e` condition warning documented above.

Self-review confirmed that every `Install Bun` step is enumerated by the test,
every one verifies the same official artifact digest before extraction, and no
`bun.com/install` or `bash -s` remains. The gitleaks test rejects a tag-only
reference. The two added Nautilus inputs directly affect the real runtime/smoke
boundary while main pushes and manual dispatch retain their prior behavior.

The local host did not provide Docker, so the digest-pinned container reference
could not be pulled and executed end to end here. The OCI index digest was
instead verified independently from the authenticated registry response and
manifest body, while the checksum-verified gitleaks 8.30.1 release binary ran
both the real commit-range and checkout scans.
