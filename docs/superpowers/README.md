# Superpowers Plans And Specs

Status as of 2026-06-09.

This directory stores implementation plans and design specs produced during
agentic development. These files are not the active work queue. A plan becomes
active only when a GitHub Issue, PR, or explicit user request resumes it.

## Status Rules

| Status | Meaning |
| --- | --- |
| `plan-note` | Implementation plan kept for traceability. Check code and GitHub Issues before executing. |
| `spec-note` | Design spec kept for review context. Do not implement unless explicitly resumed. |
| `recent-plan` | Recently written plan that may be resumed soon, but still not an active queue by itself. |

## Plans

| Plan | Status | Scope |
| --- | --- | --- |
| [`plans/2026-06-09-daily-ranking-table-filters.md`](plans/2026-06-09-daily-ranking-table-filters.md) | `recent-plan` | Daily Ranking URL-backed table filters. |
| [`plans/2026-05-27-maintainability-refactor-targets.md`](plans/2026-05-27-maintainability-refactor-targets.md) | `plan-note` | Maintainability measurement baseline and refactor target framing. |
| [`plans/2026-05-27-maintainability-phase3.md`](plans/2026-05-27-maintainability-phase3.md) | `plan-note` | Maintainability phase 3. |
| [`plans/2026-05-27-maintainability-phase4.md`](plans/2026-05-27-maintainability-phase4.md) | `plan-note` | Maintainability phase 4. |
| [`plans/2026-05-27-maintainability-phase5.md`](plans/2026-05-27-maintainability-phase5.md) | `plan-note` | Maintainability phase 5. |
| [`plans/2026-05-27-maintainability-phase6.md`](plans/2026-05-27-maintainability-phase6.md) | `plan-note` | Maintainability phase 6. |
| [`plans/2026-05-27-maintainability-phase7.md`](plans/2026-05-27-maintainability-phase7.md) | `plan-note` | Maintainability phase 7. |
| [`plans/2026-05-27-maintainability-phase8.md`](plans/2026-05-27-maintainability-phase8.md) | `plan-note` | Maintainability phase 8. |
| [`plans/2026-05-27-maintainability-phase9.md`](plans/2026-05-27-maintainability-phase9.md) | `plan-note` | Maintainability phase 9. |
| [`plans/2026-05-27-maintainability-phase10.md`](plans/2026-05-27-maintainability-phase10.md) | `plan-note` | Maintainability phase 10. |
| [`plans/2026-05-27-maintainability-phase11.md`](plans/2026-05-27-maintainability-phase11.md) | `plan-note` | Maintainability phase 11. |
| [`plans/2026-05-26-issues-411-414-contract-cleanup.md`](plans/2026-05-26-issues-411-414-contract-cleanup.md) | `plan-note` | Contract cleanup for GitHub issues 411-414. |
| [`plans/2026-05-22-ranking-valuation-coloring.md`](plans/2026-05-22-ranking-valuation-coloring.md) | `plan-note` | Ranking valuation coloring implementation. |
| [`plans/2026-05-17-fundamental-analysis-forecast-operating-profit.md`](plans/2026-05-17-fundamental-analysis-forecast-operating-profit.md) | `plan-note` | Forecast operating profit display. |
| [`plans/2026-05-16-adjusted-fundamentals-sot.md`](plans/2026-05-16-adjusted-fundamentals-sot.md) | `plan-note` | Adjusted fundamentals SoT migration. |
| [`plans/2026-05-15-post-earnings-next-day-entry.md`](plans/2026-05-15-post-earnings-next-day-entry.md) | `plan-note` | Post-earnings next-day entry research. |
| [`plans/2026-05-11-ranking-daily-table-sort.md`](plans/2026-05-11-ranking-daily-table-sort.md) | `plan-note` | Daily Ranking table sorting. |
| [`plans/2026-05-09-indices-valuation-sot.md`](plans/2026-05-09-indices-valuation-sot.md) | `plan-note` | Indices valuation SoT implementation. |

## Specs

| Spec | Status | Scope |
| --- | --- | --- |
| [`specs/2026-05-22-ranking-valuation-coloring-design.md`](specs/2026-05-22-ranking-valuation-coloring-design.md) | `spec-note` | Ranking Individual Stocks evidence-based coloring design. |
| [`specs/2026-04-20-standard-negative-eps-speculative-winner-feature-combos-design.md`](specs/2026-04-20-standard-negative-eps-speculative-winner-feature-combos-design.md) | `spec-note` | Standard negative EPS speculative winner feature-combo research design. |

## Maintenance Rules

- Add every new `plans/*.md` or `specs/*.md` file to this index.
- Do not infer that unchecked boxes mean active work. Check GitHub Issues, PRs,
  current code, and the user's current request before executing an old plan.
- When a plan is fully superseded, either move it under `docs/archive/` or keep
  it listed here with a note explaining the replacement.
