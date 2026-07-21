---
name: trading25-dependabot-maintenance
description: "Use when maintaining the weekly trading25 Dependabot PR batch or resolving blockers that prevent the batch from merging."
---

# Trading25 Dependabot Maintenance

## Overview

Run the whole weekly maintenance loop from the repository root. Treat the user's request to merge Dependabot PRs as explicit merge approval for the open batch, but still verify required checks and conflicts before each merge.

## When to use

Use this skill for the recurring Dependabot batch targeting `main`, including CI blocker remediation and final local synchronization.

## Source of Truth

GitHub PR state and required GitHub Actions checks are authoritative. Record the initial local/remote boundary before maintenance. Pre-existing dirty files and local commits ahead of `origin/main` belong to the user and are outside the batch.

## Workflow

1. Inspect and record the initial boundary before changing branches or files:
   - `git status --short --branch`
   - `git worktree list`
   - `gh auth status`
   - `git rev-parse main` and `git rev-parse origin/main`
   - `git log --oneline origin/main..main`
   - Keep the initial branch, `main`/`origin/main` OIDs, dirty paths, and ahead commit hashes in the maintenance notes.

2. Refresh the remote boundary without publishing pre-existing local work:
   - Run `git fetch origin --prune`, then record the refreshed `origin/main` OID as the batch base.
   - Do not push commits that were already in `origin/main..main` at the initial boundary. Dependabot batch approval is not approval to publish them.
   - If the current worktree is dirty or `main` was already ahead, preserve it in place and use a temporary worktree rooted at the refreshed `origin/main` for batch-owned fixes.

3. Enumerate the batch:
   - `gh pr list --author app/dependabot --state open --json number,title,headRefName,baseRefName,isDraft,mergeStateStatus,reviewDecision,updatedAt,url`
   - Work only on open Dependabot PRs targeting `main`.

4. Rebase every open Dependabot PR onto the current remote base:
   - Prefer `gh pr update-branch <PR> --rebase`.
   - If GitHub cannot rebase due to a lockfile conflict, use a temporary worktree for that one PR, resolve the conflict, push the PR branch, then return to `main`.

5. Check merge gates:
   - Use `gh pr checks <PR> --required` for the required gate.
   - Use `gh pr view <PR> --json mergeStateStatus,reviewDecision,statusCheckRollup,mergeable` when the status is ambiguous.
   - Pending checks should be polled. Do not merge while required checks are pending or failing.
   - `REVIEW_REQUIRED` is expected for Dependabot PRs in this repo; branch protection may require `--admin` after required checks are green.

6. Fix or close blockers:
   - If multiple PRs fail with the same repo-wide error, repair it in the batch worktree based on the recorded batch base.
   - Before pushing a repair, inspect `git log --oneline <batch-base>..HEAD` and verify that every commit was created for the current batch. Push only that batch-owned range to `main`; never include the recorded pre-existing ahead commits.
   - After a batch repair reaches `main`, fetch again, update the batch base, and rebase the PRs again.
   - Check these recurring blockers early when CI points there: `scripts/test-packages.sh`, `scripts/ci/test_targets.py`, `scripts/ci/test_taxonomy.py`, `scripts/coverage-gate.sh`, `scripts/skills/audit_skills.py --strict-legacy`, stale generated `.codex/skills/*/references/openapi-paths.md`, and stale web smoke routes.
   - Treat `js-yaml` major-version PRs skeptically. Prior `js-yaml 5.x` PRs broke `@redocly/openapi-core 1.34.5` during OpenAPI type generation; if the same failure appears, close the PR rather than forcing the override.

7. Merge clean PRs:
   - Use squash merge by default: `gh pr merge <PR> --squash --delete-branch`.
   - If branch protection requires admin merge and required checks are green, use `--admin`.
   - If delete-branch fails because a branch is held by a local worktree, treat it as local cleanup after confirming the PR merged.

8. Finish without rewriting the initial local state:
   - Remove only temporary worktrees created by this batch.
   - If the original `main` was clean and not ahead, fast-forward it with `git pull --ff-only origin main`.
   - If it had pre-existing dirty files or ahead commits, do not pull, reset, rebase, stash, commit, or push them. Confirm the recorded dirty paths and ahead commits remain intact, and report local/remote divergence explicitly.
   - Confirm `gh pr list --author app/dependabot --state open` is empty, except for any deliberately closed or documented non-mergeable PRs.
   - Report merged PR numbers, closed/skipped PRs with reasons, final remote `main` commit, preserved initial local state, and validation/check status.

## Guardrails

- Preserve unrelated worktrees and dirty files.
- Preserve every dirty path and ahead commit recorded at the initial boundary; never stage, commit, stash, rebase, reset, or push them as part of the batch.
- Push only commits created for the current Dependabot batch and verified against the recorded batch base.
- Never merge while required checks are pending or failing.
- Close an incompatible dependency update only after its failure is confirmed as dependency-specific.

## Verification

- Confirm every merged PR reached `MERGED` and every intentional rejection reached `CLOSED`.
- Confirm no unhandled Dependabot PR targeting `main` remains open.
- Confirm local `main` equals `origin/main` only when the initial local state was clean and not ahead. Otherwise confirm the recorded dirty/ahead state is preserved and report the remaining divergence.
```bash
gh pr list --author app/dependabot --state open --base main
git status --short --branch
python3 scripts/skills/audit_skills.py --strict-legacy
```

## Skill Maintenance

When this weekly flow changes, update this skill in place and validate it with:

```bash
python ~/.codex/skills/.system/skill-creator/scripts/quick_validate.py .codex/skills/trading25-dependabot-maintenance
```
