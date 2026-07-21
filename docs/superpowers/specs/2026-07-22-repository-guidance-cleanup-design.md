# Repository Guidance Cleanup Design

**Date:** 2026-07-22  
**Status:** Approved design  
**Scope:** `AGENTS.md`, repository/app READMEs, repository-local skills, living docs, and directly exposed P1/P2 implementation drift

## Objective

Make repository guidance safe to execute against the Market v5 codebase without turning historical documentation cleanup into an unbounded rewrite. Root `AGENTS.md` remains the cross-project contract; child `AGENTS.md` files provide local deltas; READMEs remain navigational and executable; skills describe current task workflows and invariants; living docs must not promote rejected Market v3/v4 evidence.

## Severity Policy

- Fix P0-P2 findings in the current work.
- Create GitHub Issues for P3 and lower findings instead of expanding the current diff.
- Do not delete or bulk-move historical plans, specs, audits, or experiment bundles in this cleanup.
- Historical Market v3/v4 material may remain only when it is explicitly labeled non-current and rerun-required.

## Current Findings In Scope

### P1: invalid research guidance

Add prominent historical/pre-v5 invalidation notices to the two experiment READMEs that currently recommend production or ranking reuse from Market v3 evidence:

- `pre-disclosure-flow-volatility`
- `annual-first-open-last-close-fundamental-panel`

The notices must prohibit production reuse until a Market v5/provider-adjusted rerun is published.

### P2: documentation and skill contradictions

Correct the following current-contract violations:

- Root README `repair` guidance must be non-price-only; price/options gaps use `incremental` sync.
- Root README workspace and Issue ownership must match the actual TS packages and GitHub Issues SoT.
- `contracts/README.md` must describe strict source OpenAPI export with no server/stale fallback.
- Root `AGENTS.md` must not reference deleted `src/api/client.py` and must describe middleware order consistently with runtime behavior.
- `apps/bt/README.md` and child AGENTS command examples must be executable from their documented working directories and use repository-shipped strategies unless an XDG prerequisite is explicit.
- `apps/bt/docs/strategies.md` must be replaced with a concise current guide based on strategy YAML, backend validation, the signal registry/metadata, and the current `bt` CLI.
- The Market v5 cutover runbook must be indexed from the documentation entry points.
- Market v4 ranking readouts that remain on the living research surface must be marked invalid/rerun-required, and the experiment index must not claim an obsolete published version.
- Financial-analysis skills must use current field names and keep calculation logic in `domains`; application services remain orchestration/I/O.
- Market sync skill auto-mode guidance must include DuckDB inspection when `last_sync_date` is absent.
- Dependabot maintenance must never push pre-existing local commits and must allow unrelated dirty state when isolated from in-scope work.
- Portfolio/watchlist guidance must use the backend's current alphanumeric four-character code contract.

### P2: implementation drift exposed by guidance audit

The watchlist UI currently rejects valid alphanumeric codes such as `130A` even though the backend accepts `^\d[0-9A-Z]\d[0-9A-Z]$`. Fix this behavior with a focused failing test followed by the minimal implementation change. Do not broaden this cleanup into unrelated portfolio work.

## Document Responsibilities

### Root `AGENTS.md`

Own cross-project architecture, hard invariants, SoT boundaries, and commands that apply across bt and ts. Correct factual contradictions, but do not split or comprehensively rewrite it in this change.

### Child `AGENTS.md`

Own directory-specific rules, local commands, and implementation patterns. Avoid restating the full root architecture where a short reference is sufficient.

### READMEs

Provide repository layout, quick start, common commands, and links to canonical runbooks. Commands must state their working directory when ambiguity is possible. Detailed behavioral contracts belong in AGENTS, skills, or living docs.

### Skills

Contain current trigger conditions, workflow, validation commands, and non-negotiable invariants. Remove obsolete field names and unsafe operational instructions. Generated reference files remain generated and are validated rather than manually reorganized.

### Living docs and research READMEs

Living docs may provide current operational or research guidance. Point-in-time plans and audits remain traceability artifacts. Any pre-v5 research result on a living surface must carry a visible invalidation/rerun-required state.

## P3 Follow-up Issues

Create grouped GitHub Issues for:

- Superpowers plan/spec index drift and historical classification.
- Closed items still listed as active in the unified roadmap.
- Old strategy/Kelly document links and remaining historical path cleanup.
- Semantic-layer freshness metadata and automated freshness validation.
- Dependabot skill size and extraction of incident history.
- Offline pin/digest validation for external design-guideline sources.

Issue creation must not change P3 documentation in the current branch.

## Verification

Run focused checks only:

- Markdown local-link validation across root, apps, docs, and skills.
- `python scripts/skills/audit_skills.py --strict-legacy`
- generated skill reference freshness checks.
- `python scripts/check-research-guardrails.py`
- privacy leak check.
- focused watchlist UI tests, then the relevant web type/lint checks.
- targeted searches proving removed field names, invalid commands, and stale recovery instructions no longer appear on living guidance surfaces.
- `git diff --check` and a final review of only the in-scope files.

Do not restart the full repository test suite for an isolated documentation or focused UI failure. Diagnose and rerun the affected command only.

## Non-Goals

- No bulk archive migration.
- No deletion of historical Market v3/v4 evidence.
- No complete rewrite of all 100+ experiment READMEs.
- No generalized portfolio refactor.
- No full AGENTS hierarchy redesign.
- No P3 cleanup implementation in this change.
