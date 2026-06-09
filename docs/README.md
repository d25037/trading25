# Documentation Index

Status as of 2026-06-09.

This directory is a documentation surface, not an active task queue. Open and
active task tracking lives in GitHub Issues. Long-form specs, runbooks, audits,
and research notes can live here when a GitHub Issue or PR links back to them.

## Status Classes

| Status | Meaning |
| --- | --- |
| `living` | Current source of truth or current operating guidance. Keep updated when behavior changes. |
| `runbook` | Operational guidance used during setup, triage, or incident response. Keep current. |
| `audit` | Point-in-time evidence or measurement. Do not silently treat as current unless it says `latest`. |
| `design-note` | Design context or proposal. Validate against current code and `AGENTS.md` before implementation. |
| `historical` | Kept for context. Not a current task list or implementation contract. |
| `archived` | Superseded material intentionally moved under `docs/archive/`. |
| `plan-note` | Superpowers implementation plan/spec. It is not active unless a GitHub Issue or user request explicitly resumes it. |

## Current SoT And Runbooks

| Doc | Status | Use |
| --- | --- | --- |
| [`architecture-sot-matrix.md`](architecture-sot-matrix.md) | `living` | Cross-surface SoT matrix for Screening, Symbol Workbench, Backtest, and signal semantics. |
| [`bt-src-layering-guide.md`](bt-src-layering-guide.md) | `living` | `apps/bt/src` layer placement rules. |
| [`research-pit-invalidation-register.md`](research-pit-invalidation-register.md) | `living` | Cross-experiment PIT invalidation and rerun queue. Experiment READMEs remain the individual Published Readout SoT. |
| [`ts-cli-scope.md`](ts-cli-scope.md) | `living` | TypeScript CLI deprecation and current workspace scope. |
| [`unified-roadmap.md`](unified-roadmap.md) | `living` | Roadmap index and archive pointer. Verify linked GitHub Issue state before treating an item as open. |
| [`phase5-reliability-observability-runbook.md`](phase5-reliability-observability-runbook.md) | `runbook` | Reliability and observability triage guidance. |
| [`security/ci-security-triage-runbook.md`](security/ci-security-triage-runbook.md) | `runbook` | Security CI triage. |
| [`security/runtime-secret-runbook.md`](security/runtime-secret-runbook.md) | `runbook` | Runtime secret handling and local J-Quants key setup. |

## Audits And Snapshots

| Doc | Status | Use |
| --- | --- | --- |
| [`maintainability-snapshot-latest.md`](maintainability-snapshot-latest.md) | `audit` | Latest checked maintainability snapshot. Regenerate before using as a current baseline. |
| [`maintainability-snapshot-2026-05-27.md`](maintainability-snapshot-2026-05-27.md) | `audit` | Fixed 2026-05-27 maintainability baseline. |
| [`valuation-sot-audit.md`](valuation-sot-audit.md) | `audit` | Valuation SoT inventory and follow-up targets. Validate against current code before changing valuation behavior. |
| [`phase6-release-gate-report.md`](phase6-release-gate-report.md) | `audit` | Phase 6 release-gate implementation and benchmark record. |
| [`phase6-production-smoke-report.md`](phase6-production-smoke-report.md) | `audit` | Production-scale smoke baseline. |
| [`streak-point-in-time-audit-2026-04-10.md`](streak-point-in-time-audit-2026-04-10.md) | `audit` | TOPIX100 streak PIT audit. Current downstream use is governed by `research-pit-invalidation-register.md`. |
| [`topix100-streak-swing-followup-2026-04-13.md`](topix100-streak-swing-followup-2026-04-13.md) | `audit` | Follow-up readout for TOPIX100 streak swing runs. Check invalidation status before reuse. |

## Design Notes

| Doc | Status | Use |
| --- | --- | --- |
| [`market-duckdb-sot-v3-plan.md`](market-duckdb-sot-v3-plan.md) | `design-note` | Market DB v3 migration design record. Current runtime contract is `AGENTS.md` plus implementation. |
| [`frontend-mobile-galaxy-s26-ultra-design.md`](frontend-mobile-galaxy-s26-ultra-design.md) | `design-note` | Mobile frontend target and design direction. |
| [`greenfield-architecture-blueprint.md`](greenfield-architecture-blueprint.md) | `historical` | Earlier greenfield architecture blueprint. Use for background only. |
| [`greenfield-implementation-checklist.md`](greenfield-implementation-checklist.md) | `historical` | Earlier greenfield implementation checklist. Not an active queue. |
| [`greenfield-project-charter.md`](greenfield-project-charter.md) | `historical` | Earlier 90-day greenfield charter. Not an active scope contract. |
| [`backtest-greenfield-rebuild.md`](backtest-greenfield-rebuild.md) | `historical` | Backtest redesign notes and context. Validate against current backtest implementation. |

## Other Documentation Areas

| Area | Status | Rule |
| --- | --- | --- |
| [`archive/`](archive/) | `archived` | Superseded roadmaps, migration notes, and reports. Do not use as current guidance without revalidation. |
| [`superpowers/`](superpowers/) | `plan-note` | Implementation plans/specs. See [`superpowers/README.md`](superpowers/README.md). |
| [`../apps/bt/docs/experiments/`](../apps/bt/docs/experiments/) | `living research` | Research publication surface. `README.md` `## Published Readout` is the publication SoT. |
| [`../issues/`](../issues/) | `archive` | Local issue archive only. New active work belongs in GitHub Issues. |

## Maintenance Rules

- Add a row here when adding a new top-level `docs/*.md` file.
- If a design note is implemented and no longer current guidance, either move it
  to `docs/archive/` or mark it `historical` here.
- Do not use repo Markdown as an active queue. Link docs from GitHub Issues or PRs.
- For research conclusions, prefer the experiment README `## Published Readout`;
  use `research-pit-invalidation-register.md` to check whether the result is
  valid, invalidated, or waiting for a PIT-safe rerun.
