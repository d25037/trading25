/**
 * JQuants Auth - Status Command
 * Check authentication status
 */

import { getProjectEnvPath } from '@trading25/shared/utils/find-project-root';
import chalk from 'chalk';
import { define } from 'gunshi';
import ora from 'ora';
import { ApiClient } from '../../../utils/api-client.js';
import { CLITokenManager } from '../../../utils/cli-token-manager.js';
import { CLI_NAME } from '../../../utils/constants.js';
import { handleApiError } from '../index.js';
/**
 * Status command definition
 */
export const statusCommand = define({
  name: 'status',
  description: 'Check authentication status',
  args: {},
  examples: `
# Check authentication status
${CLI_NAME} jquants auth status
  `.trim(),
  run: async () => {
    // Use repository root .env as source of truth
    const envPath = getProjectEnvPath();
    const tokenManager = new CLITokenManager(envPath);
    const spinner = ora('Checking authentication status...').start();

    try {
      // Display stored tokens from repository root .env
      await tokenManager.displayStatus();

      // Check authentication status via API
      const apiClient = new ApiClient();
      const authStatus = await apiClient.auth.getAuthStatus();

      spinner.stop();
      console.log(chalk.cyan('\n🔐 Authentication Status'));
      console.log(chalk.white('━'.repeat(50)));
      console.log(chalk.yellow('API Authenticated:'), authStatus.authenticated ? chalk.green('Yes') : chalk.red('No'));
      console.log(chalk.yellow('API Key Configured:'), authStatus.hasApiKey ? chalk.green('Yes') : chalk.red('No'));

      if (!authStatus.authenticated) {
        console.log(chalk.yellow('\n💡 Set JQUANTS_API_KEY in your .env file and retry.'));
      }
    } catch (error) {
      spinner.fail();
      handleApiError(error, 'Failed to check authentication status');
    }
  },
});
