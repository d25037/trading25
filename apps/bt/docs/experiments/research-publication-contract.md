# Research Publication Contract

Research is publication-ready only when the reusable conclusion is written back
to the source artifact, not only summarized in a Codex chat closeout.

`Published Readout` prose must be written in Japanese. Keep runner names, table
names, metric keys, code paths, and other exact identifiers in their original
English form when that preserves traceability. `Main Findings` should be
written as short conclusion subheadings followed by a compact evidence table.
Use prose only for a single non-tabular caveat; do not pack multiple horizons,
buckets, or metrics into one sentence.

## Canonical Surfaces

- `README.md`: durable current readout for the experiment.
- `baseline-YYYY-MM-DD.md`: evidence for a specific run or baseline.
- `summary.json`: structured UI digest when a bundle publishes one.
- `results.duckdb`: raw reproducibility artifact.

`summary.json` is publication-ready only when it contains explicit
`readoutSections`. Legacy digest fields such as `purpose`, `method`,
`resultHeadline`, `resultBullets`, and `considerations` are not a UI source of
truth and must not be promoted into the Research detail page.

## README Contract

Place this section near the top of long-lived experiment READMEs:

```md
## Published Readout

### Decision
- Adopt, reject, or keep the finding as context.

### Why This Research Was Run
- One or two bullets explaining the question.

### Data Scope / PIT Assumptions
- Universe, period, as-of rules, and known data limitations.

### Main Findings
#### Practical takeaway as a conclusion

| Scope | Metric | Value |
| --- | --- | --- |
| baseline | mean return | `+1.29%` |
| baseline | severe loss rate | `10.62%` |

### Interpretation
- What the results mean, and what they do not prove.

### Production Implication
- How this changes strategy, ranking, screening, or future research.

### Caveats
- Future-leak checks, missingness, capacity, cost, and sample-bias caveats.

### Source Artifacts
- Runner, bundle path, `results.duckdb`, `summary.json`, and commit.
```

The Research UI treats README files without a complete `Published Readout` as
`needs-publication-summary`. That is intentional: weak source markdown should
not be silently promoted into a polished conclusion.
