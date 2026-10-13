import type { BrowserContext, Request } from '@playwright/test';
import type { ScenarioState } from './schemas';

interface ConsoleRecord {
  type: string;
  text: string;
  url: string;
}

interface RequestIdentity {
  id: number;
  ordinal: number;
}

interface HttpRecord extends RequestIdentity {
  method: string;
  url: string;
  status: number;
  headers: { name: string; value: string }[];
}

interface NetworkRecord extends RequestIdentity {
  method: string;
  url: string;
  errorText: string;
}

type ExpectedNetwork = ScenarioState['expectedNetworkErrors'][number];

export class Monitor {
  console: ConsoleRecord[] = [];
  http: HttpRecord[] = [];
  network: NetworkRecord[] = [];
  errors: string[] = [];
  private identities = new WeakMap<Request, RequestIdentity>();
  private ordinals = new Map<string, number>();
  private completed = new Set<number>();
  private nextId = 0;

  private identity(request: Request): RequestIdentity {
    const existing = this.identities.get(request);
    if (existing) return existing;
    const key = `${request.method()} ${new URL(request.url()).pathname}`;
    const ordinal = (this.ordinals.get(key) || 0) + 1;
    this.ordinals.set(key, ordinal);
    const identity = { id: ++this.nextId, ordinal };
    this.identities.set(request, identity);
    return identity;
  }

  async attach(context: BrowserContext, origin: string) {
    await context.route('**/*', async (route) => {
      const url = new URL(route.request().url());
      if (url.origin === origin || ['data:', 'blob:'].includes(url.protocol)) return route.continue();
      this.errors.push(`Blocked external network: ${route.request().method()} ${url}`);
      await route.abort('blockedbyclient');
    });
    await context.routeWebSocket('**/*', (socket) => {
      this.errors.push(`Blocked unexpected WebSocket: ${socket.url()}`);
      socket.close({ code: 1008, reason: 'Offline review disallows WebSocket dependencies' });
    });
    context.on('request', (request) => this.identity(request));
    context.on('requestfinished', (request) => this.completed.add(this.identity(request).id));
    context.on('console', (message) =>
      this.console.push({
        type: message.type(),
        text: message.text(),
        url: message.location().url,
      }),
    );
    context.on('weberror', (error) => this.errors.push(`Browser error: ${error.error().message}`));
    context.on('requestfailed', (request) =>
      this.network.push({
        ...this.identity(request),
        method: request.method(),
        url: request.url(),
        errorText: request.failure()?.errorText || 'unknown',
      }),
    );
    context.on('response', (response) =>
      this.http.push({
        ...this.identity(response.request()),
        method: response.request().method(),
        url: response.url(),
        status: response.status(),
        headers: Object.entries(response.headers()).map(([name, value]) => ({ name, value })),
      }),
    );
  }

  validate(state?: ScenarioState) {
    const expected = state?.expectedHttpErrors || [];
    for (const response of this.http.filter((item) => item.status >= 400)) {
      if (
        !expected.some(
          (item) =>
            item.method === response.method &&
            item.status === response.status &&
            item.path === new URL(response.url).pathname,
        )
      ) {
        this.errors.push(`Unexpected HTTP ${response.status}: ${response.method} ${response.url}`);
      }
    }
    for (const item of expected) {
      const count = this.http.filter(
        (response) =>
          response.method === item.method &&
          response.status === item.status &&
          new URL(response.url).pathname === item.path,
      ).length;
      if (count !== item.count)
        this.errors.push(`Expected ${item.count} HTTP ${item.status} ${item.path}; saw ${count}`);
    }
    this.validateNetwork(state);
    this.validateConsole(expected);
    return this.errors;
  }

  private matches(actual: NetworkRecord, item: ExpectedNetwork) {
    return (
      actual.method === item.method &&
      new URL(actual.url).pathname === item.path &&
      actual.errorText === item.errorText &&
      actual.ordinal === item.requestOrdinal
    );
  }

  private responseMatches(response: HttpRecord, item: ExpectedNetwork) {
    const header = (name: string) => response.headers.find((value) => value.name === name.toLowerCase())?.value;
    return (
      response.status === item.responseStatus &&
      header('content-type')?.split(';')[0] === item.responseContentType &&
      header(item.responseHeader.name) === item.responseHeader.value
    );
  }

  private validateLifecycle(actual: NetworkRecord, item: ExpectedNetwork, state: ScenarioState) {
    const response = this.http.find((value) => value.id === actual.id);
    const failed = state.checkpoints.indexOf(item.failedCheckpoint);
    const recovered = state.checkpoints.indexOf(item.recoveredCheckpoint);
    const retry = this.http.find(
      (value) =>
        value.method === item.method &&
        new URL(value.url).pathname === item.path &&
        value.ordinal === item.requestOrdinal + 1,
    );
    if (
      !response ||
      !this.responseMatches(response, item) ||
      failed < 0 ||
      recovered <= failed ||
      !retry ||
      !this.responseMatches(retry, item) ||
      !this.completed.has(retry.id)
    ) {
      this.errors.push(`Expected canceled-stream lifecycle was not proved: ${item.method} ${item.path}`);
    }
  }

  private validateNetwork(state?: ScenarioState) {
    const expected = state?.expectedNetworkErrors || [];
    for (const actual of this.network) {
      const item = expected.find((value) => this.matches(actual, value));
      if (!item || !state) this.errors.push(`Network error: ${actual.method} ${actual.url} ${actual.errorText}`);
      else this.validateLifecycle(actual, item, state);
    }
    for (const item of expected) {
      const count = this.network.filter((actual) => this.matches(actual, item)).length;
      if (count !== item.count) this.errors.push(`Expected ${item.count} ${item.errorText} ${item.path}; saw ${count}`);
    }
  }

  private validateConsole(expected: ScenarioState['expectedHttpErrors']) {
    for (const message of this.console.filter((item) => item.type === 'error')) {
      const knownHttp = this.http.find(
        (response) =>
          response.url === message.url &&
          response.status >= 400 &&
          expected.some(
            (item) =>
              item.method === response.method &&
              item.status === response.status &&
              item.path === new URL(response.url).pathname,
          ),
      );
      const resourceError = /^Failed to load resource: the server responded with a status of \d+/.test(message.text);
      if (!knownHttp || !resourceError) this.errors.push(`Console error: ${message.text}`);
    }
  }

  evidence() {
    return {
      console: this.console,
      http: this.http,
      network: this.network,
      errors: this.errors,
      completedRequestIds: [...this.completed],
    };
  }
}
