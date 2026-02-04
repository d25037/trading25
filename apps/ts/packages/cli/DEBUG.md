# CLI Debug Guide

Simple debugging using industry-standard libraries.

## Quick Start

```bash
# Enable debug output
DEBUG=trading25:* bun run cli dataset-v2 create test.db --preset testing

# Enable specific namespaces only
DEBUG=trading25:cli,trading25:dataset bun run cli dataset-v2 create test.db --preset testing

# CLI flags (easier)
bun run cli dataset-v2 create test.db --preset testing --debug    # Basic debug
bun run cli dataset-v2 create test.db --preset testing --verbose # More verbose
bun run cli dataset-v2 create test.db --preset testing --trace   # Maximum verbosity
```

## Debug Namespaces

- `trading25:cli` - CLI-specific debug output
- `trading25:dataset` - Dataset operations debug output  
- `trading25:api` - API call debugging
- `trading25:perf` - Performance timing information
- `trading25:db` - Database operation debugging

## Environment Variables

```bash
DEBUG=trading25:*           # Enable all debugging
DEBUG=trading25:api         # API calls only
DEBUG=trading25:perf        # Performance timing only
```

## Performance Monitoring

Performance timing is automatic when debug is enabled:

```bash
DEBUG=trading25:perf bun run cli dataset-v2 create test.db --preset testing
# Output: trading25:perf Dataset creation: 1234.5ms
```

## Memory Usage

```bash
bun run cli dataset-v2 create test.db --preset testing --debug
# Shows initial and final memory usage automatically
```

## What Was Simplified

**Before (1,000+ lines of over-engineering):**
- Custom debug configuration singleton
- Complex performance monitoring classes
- Over-engineered API tracking systems
- Reinvented correlation IDs and structured logging

**After (200 lines using industry standards):**
- Standard `debug` package (30M+ weekly downloads)  
- Built-in Node.js `performance` API
- Simple utility functions
- Familiar environment variable patterns

**Benefits:**
- 85% less code to maintain
- Better performance (optimized libraries)
- Industry-standard debugging patterns
- Community support and bug fixes
- Easier for new developers to understand