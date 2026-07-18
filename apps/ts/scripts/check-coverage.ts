type CoverageThreshold = {
  lines: number;
  functions: number;
};

type CoverageSummary = {
  lines: number;
  functions: number;
};

const thresholds: Record<string, CoverageThreshold> = {
  contracts: { lines: 0.6, functions: 0.6 },
  utils: { lines: 0.8, functions: 0.8 },
  'api-clients': { lines: 0.8, functions: 0.8 },
  shikiho: { lines: 0.7, functions: 0.7 },
  web: { lines: 0.45, functions: 0.7 },
};

const coverageFiles: Record<string, string> = {
  contracts: 'packages/contracts/coverage/lcov.info',
  utils: 'packages/utils/coverage/lcov.info',
  'api-clients': 'packages/api-clients/coverage/lcov.info',
  shikiho: 'extensions/shikiho/coverage/lcov.info',
  web: 'packages/web/coverage/lcov.info',
};

function parseLcov(content: string): CoverageSummary {
  if (content.trim().length === 0) {
    throw new Error('coverage file is empty');
  }

  const recordParts = content.split('end_of_record');
  const trailingContent = recordParts.pop();
  if (trailingContent?.trim()) {
    throw new Error('record is missing end_of_record');
  }
  const records = recordParts.map((record) => record.trim()).filter(Boolean);
  if (records.length === 0) {
    throw new Error('coverage file contains no records');
  }

  let linesFound = 0;
  let linesHit = 0;
  let functionsFound = 0;
  let functionsHit = 0;

  const readTotal = (record: string, tag: 'LF' | 'LH' | 'FNF' | 'FNH'): number => {
    const prefix = `${tag}:`;
    const matches = record.split('\n').filter((line) => line.startsWith(prefix));
    if (matches.length !== 1) {
      throw new Error(`record must contain exactly one ${tag} total`);
    }
    const rawValue = matches[0]?.slice(prefix.length) ?? '';
    if (!/^(0|[1-9]\d*)$/.test(rawValue)) {
      throw new Error(`${tag} total must be a non-negative integer`);
    }
    const value = Number(rawValue);
    if (!Number.isSafeInteger(value)) {
      throw new Error(`${tag} total must be a finite safe integer`);
    }
    return value;
  };

  const addTotal = (current: number, value: number, tag: string): number => {
    const next = current + value;
    if (!Number.isSafeInteger(next)) {
      throw new Error(`${tag} aggregate exceeds the safe integer range`);
    }
    return next;
  };

  for (const [index, record] of records.entries()) {
    const sources = record.split('\n').filter((line) => line.startsWith('SF:'));
    if (sources.length !== 1 || sources[0] === 'SF:') {
      throw new Error(`record ${index + 1} must contain exactly one non-empty SF field`);
    }

    const recordLinesFound = readTotal(record, 'LF');
    const recordLinesHit = readTotal(record, 'LH');
    const recordFunctionsFound = readTotal(record, 'FNF');
    const recordFunctionsHit = readTotal(record, 'FNH');
    if (recordLinesHit > recordLinesFound) {
      throw new Error(`record ${index + 1} has LH greater than LF`);
    }
    if (recordFunctionsHit > recordFunctionsFound) {
      throw new Error(`record ${index + 1} has FNH greater than FNF`);
    }

    linesFound = addTotal(linesFound, recordLinesFound, 'LF');
    linesHit = addTotal(linesHit, recordLinesHit, 'LH');
    functionsFound = addTotal(functionsFound, recordFunctionsFound, 'FNF');
    functionsHit = addTotal(functionsHit, recordFunctionsHit, 'FNH');
  }

  if (linesFound === 0 || functionsFound === 0) {
    throw new Error('coverage file contains no measurable lines or functions');
  }

  const lineCoverage = linesHit / linesFound;
  const functionCoverage = functionsHit / functionsFound;

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
  if (content === null) {
    failures.push(`${pkg}: coverage file not found at ${filePath}`);
    continue;
  }

  let summary: CoverageSummary;
  try {
    summary = parseLcov(content);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    failures.push(`${pkg}: invalid LCOV data at ${filePath}: ${message}`);
    continue;
  }
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
