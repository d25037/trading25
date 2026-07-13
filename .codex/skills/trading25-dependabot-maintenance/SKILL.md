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

GitHub PR state and required GitHub Actions checks are authoritative. Local `main` must be compared with `origin/main` before PR branches are rebased.

## Workflow

1. Inspect local state before changing branches:
   - `git status --short --branch`
   - `git worktree list`
   - `gh auth status`

2. Return to `main`, fetch, and publish local main first:
   - Refuse to discard unrelated dirty files. Ask only if local changes block checkout.
   - Run `git fetch origin --prune`, `git switch main`, then inspect `git log --oneline origin/main..main`.
   - If `main` is ahead, run `git push origin main` before rebasing PRs. This repo often has local main commits that must become the Dependabot base first.

3. Enumerate the batch:
   - `gh pr list --author app/dependabot --state open --json number,title,headRefName,baseRefName,isDraft,mergeStateStatus,reviewDecision,updatedAt,url`
   - Work only on open Dependabot PRs targeting `main`.

4. Rebase every open Dependabot PR onto the newly pushed base:
   - Prefer `gh pr update-branch <PR> --rebase`.
   - If GitHub cannot rebase due to a lockfile conflict, use a temporary worktree for that one PR, resolve the conflict, push the PR branch, then return to `main`.

5. Check merge gates:
   - Use `gh pr checks <PR> --required` for the required gate.
   - Use `gh pr view <PR> --json mergeStateStatus,reviewDecision,statusCheckRollup,mergeable` when the status is ambiguous.
   - Pending checks should be polled. Do not merge while required checks are pending or failing.
   - `REVIEW_REQUIRED` is expected for Dependabot PRs in this repo; branch protection may require `--admin` after required checks are green.

6. Fix or close blockers:
   - If multiple PRs fail with the same repo-wide error, repair `main` first, push it, then rebase the PRs again.
   - Check these recurring blockers early when CI points there: `scripts/test-packages.sh`, `scripts/ci/test_targets.py`, `scripts/ci/test_taxonomy.py`, `scripts/coverage-gate.sh`, `scripts/skills/audit_skills.py --strict-legacy`, stale generated `.codex/skills/*/references/openapi-paths.md`, and stale web smoke routes.
   - Treat `js-yaml` major-version PRs skeptically. Prior `js-yaml 5.x` PRs broke `@redocly/openapi-core 1.34.5` during OpenAPI type generation; if the same failure appears, close the PR rather than forcing the override.

7. Merge clean PRs:
   - Use squash merge by default: `gh pr merge <PR> --squash --delete-branch`.
   - If branch protection requires admin merge and required checks are green, use `--admin`.
   - If delete-branch fails because a branch is held by a local worktree, treat it as local cleanup after confirming the PR merged.

8. Finish with local synchronization:
   - Run `git switch main`.
   - Run `git pull --ff-only origin main`.
   - Confirm `gh pr list --author app/dependabot --state open` is empty, except for any deliberately closed or documented non-mergeable PRs.
   - Report merged PR numbers, closed/skipped PRs with reasons, final `main` commit, and validation/check status.

## Guardrails

- Preserve unrelated worktrees and dirty files.
- Never merge while required checks are pending or failing.
- Close an incompatible dependency update only after its failure is confirmed as dependency-specific.

## Verification

- Confirm every merged PR reached `MERGED` and every intentional rejection reached `CLOSED`.
- Confirm no unhandled Dependabot PR targeting `main` remains open.
- Confirm local `main` equals `origin/main` and the worktree is clean.

## Skill Maintenance

When this weekly flow changes, update this skill in place and validate it with:

```bash
python ~/.codex/skills/.system/skill-creator/scripts/quick_validate.py .codex/skills/trading25-dependabot-maintenance
```
