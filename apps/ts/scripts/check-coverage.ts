type CoverageThreshold = {
  lines: number;
  functions: number;
};

type CoverageSummary = {
  lines: number;
  functions: number;
};

const thresholds: Record<string, CoverageThreshold> = {
  shared: { lines: 0.8, functions: 0.8 },
  cli: { lines: 0.7, functions: 0.7 },
  web: { lines: 0.45, functions: 0.7 },
};

const coverageFiles: Record<string, string> = {
  shared: 'packages/shared/coverage/lcov.info',
  cli: 'packages/cli/coverage/lcov.info',
  web: 'packages/web/coverage/lcov.info',
};

function parseLcov(content: string): CoverageSummary {
  let linesFound = 0;
  let linesHit = 0;
  let functionsFound = 0;
  let functionsHit = 0;

  const lines = content.split('\n');
  for (const line of lines) {
    if (line.startsWith('LF:')) {
      linesFound += Number(line.slice(3));
    } else if (line.startsWith('LH:')) {
      linesHit += Number(line.slice(3));
    } else if (line.startsWith('FNF:')) {
      functionsFound += Number(line.slice(4));
    } else if (line.startsWith('FNH:')) {
      functionsHit += Number(line.slice(4));
    }
  }

  const lineCoverage = linesFound === 0 ? 1 : linesHit / linesFound;
  const functionCoverage = functionsFound === 0 ? 1 : functionsHit / functionsFound;

  return {
    lines: lineCoverage,
    functions: functionCoverage,
  };
}

function formatPercent(value: number): string {
  return `${(value * 100).toFixed(2)}%`;
}

const failures: string[] = [];

for (const [pkg, filePath] of Object.entries(coverageFiles)) {
  const content = await Bun.file(filePath).text().catch(() => null);
  if (!content) {
    failures.push(`${pkg}: coverage file not found at ${filePath}`);
    continue;
  }

  const summary = parseLcov(content);
  const threshold = thresholds[pkg];
  if (!threshold) {
    failures.push(`${pkg}: missing coverage threshold config`);
    continue;
  }

  if (summary.lines < threshold.lines) {
    failures.push(
      `${pkg}: line coverage ${formatPercent(summary.lines)} < ${formatPercent(threshold.lines)}`
    );
  }
  if (summary.functions < threshold.functions) {
    failures.push(
      `${pkg}: function coverage ${formatPercent(summary.functions)} < ${formatPercent(threshold.functions)}`
    );
  }
}

if (failures.length > 0) {
  for (const failure of failures) {
    console.error(`Coverage gate failed: ${failure}`);
  }
  process.exit(1);
} else {
  console.log('Coverage gate passed for all packages.');
}
