type MockEventSourceListener = (event: { data: string }) => void;
type MockEventSourcePayload = Record<string, unknown> | string;

function serializePayload(data: MockEventSourcePayload): string {
  return typeof data === 'string' ? data : JSON.stringify(data);
}

export class MockEventSource {
  static instances: MockEventSource[] = [];

  readonly url: string;
  onopen: (() => void) | null = null;
  onmessage: MockEventSourceListener | null = null;
  onerror: (() => void) | null = null;
  readyState = 0;
  closed = false;
  listeners: Record<string, MockEventSourceListener[]> = {};

  constructor(url: string) {
    this.url = url;
    MockEventSource.instances.push(this);
  }

  static reset(): void {
    MockEventSource.instances = [];
  }

  close(): void {
    this.closed = true;
    this.readyState = 2;
  }

  addEventListener(type: string, listener: MockEventSourceListener): void {
    if (!this.listeners[type]) {
      this.listeners[type] = [];
    }
    this.listeners[type].push(listener);
  }

  removeEventListener(type: string, listener: MockEventSourceListener): void {
    const entries = this.listeners[type];
    if (!entries) {
      return;
    }
    this.listeners[type] = entries.filter((entry) => entry !== listener);
  }

  simulateOpen(): void {
    this.readyState = 1;
    this.onopen?.();
  }

  simulateMessage(data: MockEventSourcePayload): void {
    this.onmessage?.({ data: serializePayload(data) });
  }

  simulateNamedMessage(type: string, data: MockEventSourcePayload): void {
    this.simulateRawNamedMessage(type, serializePayload(data));
  }

  simulateRawNamedMessage(type: string, rawData: string): void {
    for (const listener of this.listeners[type] ?? []) {
      listener({ data: rawData });
    }
  }

  simulateError(): void {
    this.onerror?.();
  }
}
