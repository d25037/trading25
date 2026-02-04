/**
 * JQuants Auth - Clear Command
 * Clear tokens from .env file
 */

import { getProjectEnvPath } from '@trading25/shared/utils/find-project-root';
import { define } from 'gunshi';
import { CLITokenManager } from '../../../utils/cli-token-manager.js';

import { CLI_NAME } from '../../../utils/constants.js';
/**
 * Clear command definition
 */
export const clearCommand = define({
  name: 'clear',
  description: 'Clear tokens from .env file',
  args: {},
  examples: `
# Clear all stored tokens
${CLI_NAME} jquants auth clear
  `.trim(),
  run: async () => {
    // Use the project root discovery utility to find .env file
    const envPath = getProjectEnvPath();
    const tokenManager = new CLITokenManager(envPath);
    await tokenManager.clearTokens();
  },
});
