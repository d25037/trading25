/**
 * Dataset - Sample Command
 * Stock sampling via API
 */

import { writeFile } from 'node:fs/promises';
import chalk from 'chalk';
import { define } from 'gunshi';
import ora from 'ora';
import { ApiClient, type DatasetSampleResponse } from '../../utils/api-client.js';
import { CLI_NAME } from '../../utils/constants.js';
import { displayFooter, displayHeader, displayKeyValue, displaySection } from '../../utils/display-helpers.js';
import { CLIValidationError, DATASET_TIPS, handleCommandError } from '../../utils/error-handling.js';
import { logDebug } from '../../utils/format-helpers.js';

/**
 * Display sample result information
 */
function displaySampleResult(response: DatasetSampleResponse): void {
  displayHeader('Stock Sampling Results');

  displaySection('📊', 'Sample Statistics');
  displayKeyValue('Sampled', `${chalk.yellow(response.codes.length.toString())} stocks`);
  displayKeyValue('Total Available', chalk.yellow(response.metadata.totalAvailable.toString()));
  displayKeyValue('Stratification', response.metadata.stratificationUsed ? chalk.green('Yes') : chalk.gray('No'));

  if (response.metadata.marketDistribution) {
    console.log(chalk.white('\n  🏢 Market Distribution:'));
    for (const [market, count] of Object.entries(response.metadata.marketDistribution)) {
      console.log(chalk.white(`    ${market}: ${chalk.yellow(count.toString())}`));
    }
  }

  if (response.metadata.sectorDistribution) {
    console.log(chalk.white('\n  🏭 Sector Distribution (Top 5):'));
    const topSectors = Object.entries(response.metadata.sectorDistribution)
      .sort(([, a], [, b]) => b - a)
      .slice(0, 5);

    for (const [sector, count] of topSectors) {
      console.log(chalk.white(`    ${sector}: ${chalk.yellow(count.toString())}`));
    }

    if (Object.keys(response.metadata.sectorDistribution).length > 5) {
      console.log(
        chalk.gray(`    ... and ${Object.keys(response.metadata.sectorDistribution).length - 5} more sectors`)
      );
    }
  }

  console.log(chalk.white('\n📋 Sampled Stock Codes:'));
  const codes = response.codes;
  for (let i = 0; i < codes.length; i += 10) {
    const row = codes.slice(i, i + 10);
    console.log(chalk.cyan(`  ${row.join(', ')}`));
  }

  displayFooter();
}

/**
 * Save result to file
 */
async function saveResultToFile(response: DatasetSampleResponse, outputPath: string): Promise<void> {
  const outputData = {
    generatedAt: new Date().toISOString(),
    stockCodes: response.codes,
    metadata: response.metadata,
  };

  await writeFile(outputPath, JSON.stringify(outputData, null, 2));
  console.log(chalk.green(`\n💾 Results saved to: ${outputPath}`));
}

/**
 * Sample command definition
 */
export const sampleCommand = define({
  name: 'sample',
  description: 'Sample random stocks from a dataset snapshot',
  args: {
    name: {
      type: 'positional',
      description: 'Dataset filename (within XDG datasets directory)',
    },
    size: {
      type: 'string',
      short: 's',
      description: 'Sample size',
      default: '300',
    },
    byMarket: {
      type: 'boolean',
      description: 'Stratify sampling by market',
    },
    bySector: {
      type: 'boolean',
      description: 'Stratify sampling by sector',
    },
    seed: {
      type: 'string',
      description: 'Random seed for reproducibility',
    },
    output: {
      type: 'string',
      description: 'Output file for results (JSON)',
    },
    debug: {
      type: 'boolean',
      description: 'Enable detailed output for debugging',
    },
  },
  examples: `
# Sample 300 stocks (default)
${CLI_NAME} dataset sample prime.db

# Sample 100 stocks with market stratification
${CLI_NAME} dataset sample prime.db -s 100 --by-market

# Sample with reproducible seed
${CLI_NAME} dataset sample prime.db --seed 42

# Save results to file
${CLI_NAME} dataset sample prime.db --output sample.json
  `.trim(),
  run: async (ctx) => {
    const { name: datasetName, size, byMarket, bySector, seed, output, debug } = ctx.values;
    const isDebug = debug ?? false;
    const spinner = ora('Sampling stocks...').start();

    if (!datasetName) {
      spinner.fail(chalk.red('Dataset name is required'));
      throw new CLIValidationError('Dataset name is required');
    }

    const sampleSize = Number.parseInt(size ?? '300', 10);
    const seedNum = seed ? Number.parseInt(seed, 10) : undefined;

    logDebug(isDebug, `Dataset name: ${datasetName}`);
    logDebug(isDebug, `Size: ${sampleSize}`);
    logDebug(isDebug, `Seed: ${seedNum ?? 'random'}`);
    logDebug(isDebug, `By market: ${byMarket ?? false}`);
    logDebug(isDebug, `By sector: ${bySector ?? false}`);

    try {
      const apiClient = new ApiClient();
      const datasetClient = apiClient.dataset;

      spinner.text = `Sampling ${sampleSize} stocks...`;
      const response = await datasetClient.sampleDataset(datasetName, {
        size: sampleSize,
        byMarket,
        bySector,
        seed: seedNum,
      });

      spinner.stop();

      logDebug(isDebug, 'Response received');
      logDebug(isDebug, `Sampled: ${response.codes.length} stocks`);

      displaySampleResult(response);

      if (output) {
        await saveResultToFile(response, output);
      }
    } catch (error) {
      handleCommandError(error, spinner, {
        failMessage: 'Sampling failed',
        debug: isDebug,
        tips: DATASET_TIPS.sample,
      });
    }
  },
});
