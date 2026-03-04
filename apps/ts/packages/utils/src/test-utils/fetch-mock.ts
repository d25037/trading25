export function createMockResponse<T>(data: T, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    statusText: status === 200 ? 'OK' : 'Error',
    headers: { 'Content-Type': 'application/json' },
  });
}

export function createMockErrorResponse(message: string, status: number): Response {
  return new Response(JSON.stringify({ message }), {
    status,
    statusText: message,
    headers: { 'Content-Type': 'application/json' },
  });
}

export function createNetworkError(): TypeError {
  return new TypeError('Failed to fetch');
}

export function createTimeoutAbortError(): Error {
  const error = new Error('The operation was aborted');
  error.name = 'AbortError';
  return error;
}
