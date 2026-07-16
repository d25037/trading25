# Complete Maintenance Completion Evidence

Date: 2026-07-16 JST

Roadmap: `docs/superpowers/plans/2026-07-16-complete-maintenance-execution.md`

Implementation begins after `a71b9731`; this document is the completion
evidence commit for the resulting range.

## Verdict

The approved repository-maintenance roadmap is implemented and verified. The retained
Market v4 payload is active without another sync, Market DuckDB growth is
bounded by semantic delta writes and verified maintenance, the remaining
application/HTTP boundary debt and cutover monolith are removed, and the final
PIT/legacy audit findings are closed.

## Requirement-to-evidence matrix

| Requirement | Result | Current evidence |
|---|---|---|
| Preserve the user-owned local Codex configuration | PASS | Every final status check reports only untracked `.codex/config.toml`; it was not read, edited, staged, or committed. |
| Market v4 is the only active time-series contract | PASS | Active `/api/db/stats` and `/api/db/validate` return schema version 4/current and `timeSeriesSource=duckdb-parquet`; the active adjustment mode is `local_projection_v2_event_time`. v3 auto-migration, dual-read, and compatibility aliases remain absent. |
| Promote the validated retained payload without another sync/J-Quants call | PASS | Promotion report `market-v4-active-20260716-r3/report.json` has `status=passed`, `activationMode=retained_atomic_exchange`, `noSync=true`, `noJQuants=true`, joined server/worker verdicts, committed journal, and complete semantic smoke. |
| Preserve rollback evidence | PASS | Immutable backup `market-v3-pre-v4-20260716-r3`, quarantined former active payload, consumed retained-source marker, checksums, and exact identities remain present. |
| Prevent repeated syncs from growing DuckDB indefinitely | PASS | Tasks 6-14 implement semantic delta publication, differential derived tables, one cross-process writer lease, differential adjusted/technical materialization, fixed 512 MiB/10% soft and 1 GiB hard triggers, verified atomic compaction, and durable recovery. Synthetic three-cycle acceptance proved zero mutations on cycles two/three, stable Parquet identities, bounded DB/free-block growth, and forced hard-cap compaction. |
| Produce current operational maintenance evidence without another sync | PASS | `uv run bt market-maintain` completed in 6.18 s with `compacted=false`, `trigger=none`, `validation=passed`. Active DB inode, size (6,987,198,464 bytes), mtime, and absent WAL were identical before/after. `maintenance.v1.json` is valid with SHA-256 `68f7ccddf0ea7ef87abc09502daf887bb8f5ee322f13e2471fe917c3b22d2f07`. |
| Dataset snapshots are event-time Market v4 bundles only | PASS | Dataset resolution/creation supports only `dataset.duckdb + parquet/ + manifest.v2.json` with payload schema 3 and copies/validates the complete cutoff-bound basis graph. Legacy dataset DB/schema paths are rejection fixtures only. |
| Fundamentals and valuation are PIT-safe | PASS | Raw `statements` remains provenance; `statement_metrics_adjusted` and `daily_valuation` are basis-bound SoTs. Active validation reports 7,916 retained/ready bases, zero invalid/overlapping/under-covered bases, and zero missing/extra/stale/wrong-basis rows. |
| Remove confirmed future-leaking research surfaces | PASS | Named invalid research implementations, publications, fixtures, and consumers were deleted and fail-closed guards pass. The living PIT register now requires Market v4/event-time lineage. |
| Historical sector signals use PIT membership | PASS | `stock_master_daily` supplies date-indexed membership with 4/5-digit normalization, same-day four-digit precedence, and NULL tombstones. Rotation/volatility compute each sector on its own continuous history and select the boolean result by evaluation-date membership; unknown/tombstone dates are false. Independent review is CLEAN. |
| Remove deprecated optimization query/runtime compatibility | PASS | `grid_config_path` and standalone grid-YAML runtime/docs are removed. Strategy YAML `optimization` is the sole runtime SoT; only the explicit one-shot `bt migrate-optimization-specs` migration remains. |
| Consolidate DTO ownership | PASS | Application services no longer import HTTP schema modules. Canonical job, analytics, Screening, Ranking, portfolio, factor-regression, signal, backtest, and maintenance contracts live in application/shared contracts; generated OpenAPI/TypeScript contracts are clean. |
| Make maintainability evidence deterministic and Python-3.12-correct | PASS | The snapshot fails fast below Python 3.12, parses tracked Python with 3.12 grammar and filenames, rejects malformed input, omits wall-clock metadata, and is enforced by CI/pre-push. Current JSON/Markdown artifacts pass `--check`. |
| Split the cutover monolith without compatibility forwarding | PASS | The old module is absent. The explicit-composition package has a 190-line facade, 10 collaborators, no mixins/grafts/dynamic forwarding/root re-exports, production modules <=693 lines, methods <=180 lines, and split tests <=832 lines. |
| Remove stale platform surfaces | PASS | Hono `apps/ts/packages/api`, SQLite Market time-series mirrors, old cutover root imports, deprecated Dataset resume, optimization grid-config routes, and undocumented writable Market opens are absent or covered only by explicit rejection/history tests. |

## Active Market evidence

Fresh canonical HTTP reads after maintenance:

- `GET /api/db/stats`: HTTP 200, initialized, schema v4/current,
  `timeSeriesSource=duckdb-parquet`.
- `GET /api/db/validate`: HTTP 200, overall `healthy`; domains are
  `coreDaily=healthy`, `derivatives=info`, `intraday=healthy`,
  `sourceQuality=info`.
- DuckDB: 6,987,198,464 bytes; 26,654 total blocks, 26,572 used, 82 free;
  21,495,808 free bytes; WAL 0; temp bytes 0; stale artifacts 0.
- Stock daily: 9,748,001 rows, 2,441 dates, 2016-07-15 through 2026-07-15,
  zero missing dates.
- Adjusted metrics: `ready`; 7,916/7,916 retained/ready bases; all missing,
  extra, stale, wrong-basis, orphan, overlap, and under-coverage counters are
  zero.
- Maintenance: `evidenceStatus=valid`, `outcome=passed`,
  `operation=market_maintain`, `compacted=false`, `trigger=none`,
  `validation=passed`.

The remaining validation information is operational data quality, not a
maintenance-code failure: options coverage is one allowed TOPIX date behind,
with one missing UnderPx date and five historical conflicting UnderPx dates.
No sync was run to hide or alter those diagnostics.

## Final gates

| Gate | Result |
|---|---|
| Backend complete pytest | PASS: 7,125 passed / 2 skipped (analytics 676; remaining suite 6,449 / 2 skipped) |
| Signal remediation | 807 passed; independent re-review CLEAN |
| Architecture, CI taxonomy, skill/refresh/privacy unit guards | 281 passed |
| Root lint | PASS: dependency direction 86 allowed / 0 violations / 0 stale; Biome 474 files; Ruff clean |
| Root typecheck | PASS: all TypeScript packages; Pyright 0 errors/warnings |
| Maintainability snapshot | PASS (`--check`) |
| Skill audit | PASS (`--strict-legacy`) |
| Skill generated references | PASS (`--check`) |
| Privacy repository scan | PASS |
| Research guardrails | PASS |
| TypeScript workspace tests | 1,782 passed |
| TypeScript dependency audit | PASS, 6 manifests |
| Playwright smoke | 4 passed with canonical FastAPI; both temporary web/backend servers joined cleanly |
| OpenAPI normalized contract check | PASS |
| OpenAPI `bt:sync`/generated types | PASS; snapshot/types unchanged |
| Active stats/validate | HTTP 200 / HTTP 200; validation `healthy` |
| Operational no-op maintenance | PASS; verified sidecar, unchanged DB identity/size/mtime, WAL absent |
| `git diff --check a71b9731..HEAD` | PASS |

## Forbidden-reference classification

Fresh searches and fail-closed tests report no unexpected runtime matches for:

- application imports from `entrypoints.http.schemas`;
- the deleted `market_v4_cutover.py` monolith or package-root compatibility
  imports;
- `grid_config_path`, `/api/optimize/grid-configs`,
  `/api/dataset/resume`, or production `timeoutMinutes`;
- `load_market_stock_sector_mapping`, stitched
  `build_stock_sector_close`, or `stocks_latest` in Signal sector evaluation;
- the deleted Hono package or port 3001 runtime.

Remaining `v3`, `legacy`, and `deprecated` text is intentional and classified:

- versioned historical contracts and archived provenance;
- explicit incompatibility/rejection tests and error messages;
- the immutable v3 backup/quarantine and cutover recovery runbook;
- the required 410 Screening migration response;
- the explicit one-shot optimization-spec migration command.

## Accepted implementation commits

The range contains 73 bounded commits. Key accepted heads are:

- retained no-sync promotion and recovery: `5da8f96c..70ba596c`;
- differential writers and ownership: `52cebc48..7d24bbb3`;
- verified compaction: `df48a082`, `05dbe65a`, `b59e578d`;
- common maintenance finalizer: `5b82e8e2..9acada6e`;
- bounded-growth acceptance: `0ed85c69`, `a6c29d6b`;
- maintainability: `059fb3e6`, `834ecac2`;
- DTO boundary: `437a4a8f`, `d93d5ef9`, `f4331dce`;
- cutover split: `2f414853`, `255f01a2`, `49575d78`;
- final PIT/legacy remediation: `5110959f`, `e71fc57b`;
- final gate drift closure: `e0ea491a`.

## Completion condition

The roadmap may be marked complete after the final tracked-worktree/diff check
and independent review of this evidence are clean.

## Post-roadmap origin/main integration

After the roadmap received a CLEAN final review, local `main` merged
`origin/main` at `c509f367` with merge commit `69c275a6`. The merge base was
`8ff1c0aa`; local `main` remained the architectural base and now contains all
origin commits.

The origin-only delta was limited to the Shikiho Chrome extension, its
Symbol Workbench UI, and related documentation. It did not modify `apps/bt`,
OpenAPI, generated contracts, API clients, or Market/Dataset data-plane code.
The two jointly modified Symbol Workbench files merged without textual
conflicts. The merged behavior preserves local Fundamentals PIT recovery and
uses:

- the canonical Shikiho snapshot for TTL, chart overlays, and provisional
  market-data semantics;
- a progressive candidate only for panel/header display;
- metadata-only acquisition traces and diagnostics;
- exact authenticated user tabs without navigation/reload/close, otherwise an
  extension-owned one-shot tab with bounded acquisition and cleanup.

Post-merge verification:

- TypeScript workspace: 1,992 passed, including 269 extension tests and 1,431
  web tests;
- root lint/typecheck: dependency direction, Biome (496 files), Ruff, all
  TypeScript packages, dependency audit, and Pyright passed;
- production builds: contracts, API clients, utils, web, and Shikiho extension
  passed;
- Playwright smoke: 4 passed with a temporary canonical FastAPI server, which
  shut down cleanly;
- OpenAPI contract check, strict skill audit, generated skill references, and
  privacy scan passed;
- extension manifest permissions remain exactly `storage` and `alarms`, with
  no host permissions; content scripts use `document_start`;
- maintainability artifacts were regenerated and pass `--check`;
- `origin/main` is an ancestor of local `main`; no origin-only commit remains.

Interactive Chrome acceptance requires the user's authenticated Shikiho tab
and is therefore not represented as an automated gate. The extension's tab
ownership, one-shot cleanup, canonical/candidate separation, progress trace,
earnings-date extraction, and permission invariants are covered by the merged
unit and integration suites.
