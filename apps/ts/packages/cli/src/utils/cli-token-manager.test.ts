import { beforeEach, describe, expect, it, mock } from 'bun:test';
import { FileTokenStorage, TokenManager } from '@trading25/shared';
import { CLITokenManager, createCliTokenManager } from './cli-token-manager';

// Mock dependencies
mock.module('@trading25/shared', () => ({
  FileTokenStorage: mock(),
  TokenManager: mock(),
}));

mock.module('chalk', () => ({
  default: {
    red: mock((text: string) => text),
    green: mock((text: string) => text),
    yellow: mock((text: string) => text),
    blue: mock((text: string) => text),
    cyan: mock((text: string) => text),
    white: mock((text: string) => text),
    gray: mock((text: string) => text),
  },
}));

// Mock console
const mockConsole = {
  log: mock(),
  error: mock(),
};
global.console = mockConsole as unknown as Console;

describe('CLI Token Manager', () => {
  let mockFileTokenStorage: {
    saveTokens: ReturnType<typeof mock>;
    getTokens: ReturnType<typeof mock>;
    clearTokens: ReturnType<typeof mock>;
    hasValidTokens: ReturnType<typeof mock>;
    isAvailable: ReturnType<typeof mock>;
  };
  let mockTokenManager: {
    saveTokens: ReturnType<typeof mock>;
    getTokens: ReturnType<typeof mock>;
    clearTokens: ReturnType<typeof mock>;
    hasValidTokens: ReturnType<typeof mock>;
    isStorageAvailable: ReturnType<typeof mock>;
  };

  beforeEach(() => {
    mockFileTokenStorage = {
      saveTokens: mock(),
      getTokens: mock(),
      clearTokens: mock(),
      hasValidTokens: mock(),
      isAvailable: mock().mockReturnValue(true),
    };

    mockTokenManager = {
      saveTokens: mock(),
      getTokens: mock(),
      clearTokens: mock(),
      hasValidTokens: mock(),
      isStorageAvailable: mock().mockReturnValue(true),
    };

    (FileTokenStorage as unknown as ReturnType<typeof mock>).mockImplementation(() => mockFileTokenStorage);
    (TokenManager as unknown as ReturnType<typeof mock>).mockImplementation(() => mockTokenManager);
  });

  describe('createCliTokenManager', () => {
    it('should create TokenManager with FileTokenStorage', () => {
      createCliTokenManager();

      expect(FileTokenStorage).toHaveBeenCalledWith({
        envPath: undefined,
        logger: expect.any(Function),
      });
      expect(TokenManager).toHaveBeenCalledWith(mockFileTokenStorage);
    });

    it('should create TokenManager with custom env path', () => {
      const customPath = '/custom/.env';
      createCliTokenManager(customPath);

      expect(FileTokenStorage).toHaveBeenCalledWith({
        envPath: customPath,
        logger: expect.any(Function),
      });
    });

    it('should configure logger for error messages', () => {
      createCliTokenManager();
      const mockedFileTokenStorage = FileTokenStorage as unknown as ReturnType<typeof mock> & {
        mock: { calls: unknown[][] };
      };
      const configObject = mockedFileTokenStorage.mock.calls[0]?.[0] as {
        logger: (msg: string, level?: string) => void;
      };

      configObject.logger('Test error', 'error');
      expect(mockConsole.error).toHaveBeenCalledWith('❌ Test error');
    });

    it('should configure logger for warning messages', () => {
      createCliTokenManager();
      const mockedFileTokenStorage = FileTokenStorage as unknown as ReturnType<typeof mock> & {
        mock: { calls: unknown[][] };
      };
      const configObject = mockedFileTokenStorage.mock.calls[0]?.[0] as {
        logger: (msg: string, level?: string) => void;
      };

      configObject.logger('Test warning', 'warn');
      expect(mockConsole.log).toHaveBeenCalledWith('Warning: Test warning');
    });

    it('should configure logger for info messages', () => {
      createCliTokenManager();
      const mockedFileTokenStorage = FileTokenStorage as unknown as ReturnType<typeof mock> & {
        mock: { calls: unknown[][] };
      };
      const configObject = mockedFileTokenStorage.mock.calls[0]?.[0] as {
        logger: (msg: string, level?: string) => void;
      };

      configObject.logger('Test info', 'info');
      expect(mockConsole.log).toHaveBeenCalledWith('✅ Test info');
    });

    it('should configure logger for default messages', () => {
      createCliTokenManager();
      const mockedFileTokenStorage = FileTokenStorage as unknown as ReturnType<typeof mock> & {
        mock: { calls: unknown[][] };
      };
      const configObject = mockedFileTokenStorage.mock.calls[0]?.[0] as {
        logger: (msg: string, level?: string) => void;
      };

      configObject.logger('Test message');
      expect(mockConsole.log).toHaveBeenCalledWith('Test message');
    });
  });

  describe('CLITokenManager', () => {
    let cliTokenManager: CLITokenManager;

    beforeEach(() => {
      cliTokenManager = new CLITokenManager();
    });

    describe('saveApiKey', () => {
      it('should save API key via TokenManager', async () => {
        const apiKey = 'dummy_token_value_1234';

        await cliTokenManager.saveApiKey(apiKey);

        expect(mockTokenManager.saveTokens).toHaveBeenCalledWith({ apiKey });
      });
    });

    describe('getApiKey', () => {
      it('should get API key from TokenManager', async () => {
        const mockTokenData = {
          apiKey: 'dummy_token_value_1234',
        };
        mockTokenManager.getTokens.mockResolvedValue(mockTokenData);

        const result = await cliTokenManager.getApiKey();

        expect(mockTokenManager.getTokens).toHaveBeenCalled();
        expect(result).toBe('dummy_token_value_1234');
      });

      it('should handle missing API key', async () => {
        mockTokenManager.getTokens.mockResolvedValue({});

        const result = await cliTokenManager.getApiKey();

        expect(result).toBeUndefined();
      });
    });

    describe('clearTokens', () => {
      it('should clear tokens via TokenManager', async () => {
        await cliTokenManager.clearTokens();

        expect(mockTokenManager.clearTokens).toHaveBeenCalled();
      });
    });

    describe('hasValidTokens', () => {
      it('should check if API key is valid', async () => {
        mockTokenManager.hasValidTokens.mockResolvedValue(true);

        const result = await cliTokenManager.hasValidTokens();

        expect(mockTokenManager.hasValidTokens).toHaveBeenCalled();
        expect(result).toBe(true);
      });
    });

    describe('displayStatus', () => {
      it('should display status with API key present', async () => {
        mockTokenManager.getTokens.mockResolvedValue({
          apiKey: 'dummy_token_value_9999',
        });

        await cliTokenManager.displayStatus();

        expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('JQuants API v2 Status'));
        expect(mockConsole.log).toHaveBeenCalledWith('Has API Key:', 'Yes');
        expect(mockConsole.log).toHaveBeenCalledWith('API Key:', 'dumm...9999');
      });

      it('should display status with no API key', async () => {
        mockTokenManager.getTokens.mockResolvedValue({});

        await cliTokenManager.displayStatus();

        expect(mockConsole.log).toHaveBeenCalledWith('Has API Key:', 'No');
      });
    });
  });
});
