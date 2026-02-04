/**
 * JQuants Auth - Status Command
 * Display JQuants API v2 authentication status
 */

import { getProjectEnvPath } from '@trading25/shared/utils/find-project-root';
import chalk from 'chalk';
import { define } from 'gunshi';
import { CLITokenManager } from '../../../utils/cli-token-manager.js';

import { CLI_NAME } from '../../../utils/constants.js';
export async function statusAction(): Promise<void> {
  // Use the project root discovery utility to find .env file
  const envPath = getProjectEnvPath();
  const tokenManager = new CLITokenManager(envPath);

  // Display current API key status
  await tokenManager.displayStatus();

  const hasApiKey = await tokenManager.hasValidTokens();
  if (!hasApiKey) {
    console.log(chalk.yellow('\nTo configure JQuants API v2:'));
    console.log(chalk.gray('  Set JQUANTS_API_KEY in your .env file'));
  }
}

/**
 * Status command definition (replaces refresh-tokens for v2)
 */
export const refreshTokensCommand = define({
  name: 'status',
  description: 'Display JQuants API v2 authentication status',
  args: {},
  examples: `
# Check API key status
${CLI_NAME} jquants auth status
  `.trim(),
  run: async () => {
    await statusAction();
  },
});
