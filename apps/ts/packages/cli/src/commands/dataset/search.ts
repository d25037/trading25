/**
 * Dataset - Search Command
 * Stock search via API
 */

import chalk from 'chalk';
import { define } from 'gunshi';
import ora from 'ora';
import { ApiClient, type DatasetSearchResponse, type DatasetSearchResultItem } from '../../utils/api-client.js';
import { CLI_NAME } from '../../utils/constants.js';
import { displayFooter, displayHeader, displayNoResults } from '../../utils/display-helpers.js';
import { CLIValidationError, DATASET_TIPS, handleCommandError } from '../../utils/error-handling.js';
import { logDebug } from '../../utils/format-helpers.js';

/**
 * Get icon for match type
 */
function getMatchIcon(matchType: DatasetSearchResultItem['matchType']): string {
  switch (matchType) {
    case 'code':
      return '🔢';
    case 'name':
      return '📝';
    case 'english_name':
      return '🌐';
    default:
      return '🔍';
  }
}

const SEARCH_NO_RESULTS_TIPS = [
  'Using a partial company name',
  'Searching by stock code (4 characters)',
  'Removing the --exact flag',
];

/**
 * Display search results
 */
function displaySearchResults(response: DatasetSearchResponse, searchTerm: string): void {
  displayHeader(`Search Results for "${searchTerm}"`);

  if (response.results.length === 0) {
    displayNoResults('No stocks found matching your criteria.', SEARCH_NO_RESULTS_TIPS);
    return;
  }

  console.log(
    chalk.white(
      `\n📊 Found ${chalk.yellow(response.totalFound.toString())} result${response.totalFound === 1 ? '' : 's'}:\n`
    )
  );

  for (const result of response.results) {
    const matchIcon = getMatchIcon(result.matchType);

    console.log(`${matchIcon} ${chalk.cyan(result.code)} - ${chalk.white(result.companyName)}`);
    if (result.companyNameEnglish && result.companyNameEnglish !== result.companyName) {
      console.log(chalk.gray(`   English: ${result.companyNameEnglish}`));
    }
    console.log(chalk.gray(`   Market: ${result.marketName} | Sector: ${result.sectorName}`));
    console.log(
      chalk.gray(
        `   Match: ${result.matchType === 'code' ? 'Stock Code' : result.matchType === 'name' ? 'Company Name' : 'English Name'}`
      )
    );
    console.log('');
  }

  displayFooter();
}

/**
 * Search command definition
 */
export const searchCommand = define({
  name: 'search',
  description: 'Search for stocks within a dataset snapshot',
  args: {
    name: {
      type: 'positional',
      description: 'Dataset filename (within XDG datasets directory)',
    },
    term: {
      type: 'positional',
      description: 'Search term (company name or stock code)',
    },
    limit: {
      type: 'string',
      description: 'Limit number of results',
      default: '20',
    },
    exact: {
      type: 'boolean',
      description: 'Exact match only',
    },
    debug: {
      type: 'boolean',
      description: 'Enable detailed output for debugging',
    },
  },
  examples: `
# Search by company name
${CLI_NAME} dataset search prime.db toyota

# Search by stock code
${CLI_NAME} dataset search prime.db 7203

# Limit results
${CLI_NAME} dataset search prime.db bank --limit 10

# Exact match only
${CLI_NAME} dataset search prime.db "Toyota Motor" --exact
  `.trim(),
  run: async (ctx) => {
    const { name: datasetName, term: searchTerm, limit, exact, debug } = ctx.values;
    const isDebug = debug ?? false;
    const spinner = ora('Searching stocks...').start();

    if (!datasetName || !searchTerm) {
      spinner.fail(chalk.red('Dataset name and search term are required'));
      throw new CLIValidationError(
        `Dataset name and search term are required\nUsage: ${CLI_NAME} dataset search <name> <term>`
      );
    }

    const limitNum = Number.parseInt(limit ?? '20', 10);

    logDebug(isDebug, `Dataset name: ${datasetName}`);
    logDebug(isDebug, `Search term: ${searchTerm}`);
    logDebug(isDebug, `Limit: ${limitNum}`);
    logDebug(isDebug, `Exact match: ${exact ?? false}`);

    try {
      const apiClient = new ApiClient();
      const datasetClient = apiClient.dataset;

      spinner.text = `Searching for "${searchTerm}"...`;
      const response = await datasetClient.searchDataset(datasetName, searchTerm, {
        limit: limitNum,
        exact,
      });

      spinner.stop();

      logDebug(isDebug, 'Response received');
      logDebug(isDebug, `Found: ${response.totalFound} results`);

      displaySearchResults(response, searchTerm);
    } catch (error) {
      handleCommandError(error, spinner, {
        failMessage: 'Search failed',
        debug: isDebug,
        tips: DATASET_TIPS.search,
      });
    }
  },
});
