# Market v4 Cutover Runbook

## Retained rehearsal

Use a retained rehearsal only after correcting downstream smoke or application
code when the isolated Market v4 data plane itself is unchanged. Any change to
sync, ingest, schema, Parquet publication, PIT materialization, or other data
plane behavior requires a full `market-cutover rehearse` instead.

```bash
uv run bt market-cutover rehearse-retained market-v4-retained-20260715-r12 \
  --source-rehearsal-id market-v4-rehearsal-20260715-r10 \
  --symbol 7203 \
  --strategy production/cutover_smoke \
  --dataset-preset primeMarket
```

The source is identified only by its rehearsal report ID. The command accepts
no source path, force option, or compatibility mode. It reuses the service-owned
retained root, runs the current semantic smoke, and writes a new rehearsal
report bound to the current code identity.

The retained command makes zero J-Quants requests. It does not require or load
J-Quants secrets or plan configuration. Reports created before the retained
rehearsal contract are rejected, including reports without the required
rehearsal mode and successful server/worker process-join evidence. Run a full
`market-cutover rehearse` to establish fresh provenance when the source report
is ineligible.
