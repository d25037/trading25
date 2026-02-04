# Contracts

This directory defines stable interfaces between `apps/ts` and `apps/bt`.

## Policy
- Treat contract changes as versioned changes (additive vs breaking).
- If a breaking change is required, create a new versioned contract file.
- Keep contracts implementation-agnostic; no internal details.

## Files
- `dataset-schema.json`: Minimal dataset schema expected by `apps/bt`.
