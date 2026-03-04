import { mock } from 'bun:test';

export const mockConsole = {
  log: mock(),
  error: mock(),
  warn: mock(),
  info: mock(),
};

export const mockWebSocket = mock(() => ({
  on: mock(),
  send: mock(),
  close: mock(),
  readyState: 1, // WebSocket.OPEN
}));

export const mockProcessEnv = {
  MAIL_ADDRESS: 'test@example.com',
  PASSWORD: 'test_password',
  REFRESH_TOKEN: 'test_refresh_token',
  ID_TOKEN: 'test_id_token',
};
