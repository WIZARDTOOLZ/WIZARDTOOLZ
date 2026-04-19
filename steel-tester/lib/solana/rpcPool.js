import { Connection } from '@solana/web3.js';

const DEFAULT_PUBLIC_RPC_URL = 'https://api.mainnet-beta.solana.com';
const SUBSCRIPTION_METHOD_PREFIXES = ['on', 'removeOn'];
const SUBSCRIPTION_METHODS = new Set([
  'onAccountChange',
  'onLogs',
  'onProgramAccountChange',
  'onRootChange',
  'onSignature',
  'onSlotChange',
  'onSlotUpdate',
  'removeAccountChangeListener',
  'removeOnLogsListener',
  'removeProgramAccountChangeListener',
  'removeRootChangeListener',
  'removeSignatureListener',
  'removeSlotChangeListener',
  'removeSlotUpdateListener',
]);

function parsePositiveInt(value, fallback) {
  const parsed = Number.parseInt(String(value ?? '').trim(), 10);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

export function parseRpcUrlList(...sources) {
  const urls = sources
    .flatMap((value) => {
      if (Array.isArray(value)) {
        return value;
      }
      if (typeof value === 'string') {
        return value.split(',');
      }
      return [];
    })
    .map((value) => String(value || '').trim())
    .filter(Boolean);

  const deduped = [];
  for (const url of urls) {
    if (!deduped.includes(url)) {
      deduped.push(url);
    }
  }
  return deduped.length > 0 ? deduped : [DEFAULT_PUBLIC_RPC_URL];
}

function shouldTreatAsSubscription(methodName) {
  if (SUBSCRIPTION_METHODS.has(methodName)) {
    return true;
  }

  return SUBSCRIPTION_METHOD_PREFIXES.some((prefix) => methodName.startsWith(prefix));
}

function parseRetryAfterMs(value) {
  if (value && typeof value.get === 'function') {
    value = value.get('retry-after');
  }
  const parsedSeconds = Number.parseInt(String(value || '').trim(), 10);
  if (Number.isInteger(parsedSeconds) && parsedSeconds > 0) {
    return parsedSeconds * 1000;
  }
  return null;
}

function isRetryableStatus(status) {
  return [403, 408, 409, 425, 429, 500, 502, 503, 504].includes(Number(status));
}

function isRetryableRpcError(error) {
  const message = String(error?.message || error || '').toLowerCase();
  if (!message) {
    return false;
  }

  return (
    message.includes('429')
    || message.includes('403')
    || message.includes('rate limit')
    || message.includes('too many requests')
    || message.includes('fetch failed')
    || message.includes('etimedout')
    || message.includes('timeout')
    || message.includes('socket hang up')
    || message.includes('econnreset')
    || message.includes('econnrefused')
    || message.includes('temporarily unavailable')
    || message.includes('service unavailable')
    || message.includes('bad gateway')
    || message.includes('gateway timeout')
  );
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function createManagedSolanaRpcPool({
  urls,
  commitment = 'confirmed',
  label = 'solana-rpc',
  cooldownMs = 30_000,
  timeoutMs = 12_000,
  maxRetries = null,
} = {}) {
  const endpointUrls = parseRpcUrlList(urls);
  const states = endpointUrls.map((url) => ({
    url,
    connection: new Connection(url, commitment),
    cooldownUntil: 0,
    failCount: 0,
    lastError: null,
    lastUsedAt: null,
    lastRecoveredAt: null,
  }));

  let activeIndex = 0;
  const wrappedMethods = new Map();

  function getActiveState() {
    return states[activeIndex] ?? states[0];
  }

  function getReadyIndices(excluded = new Set()) {
    const now = Date.now();
    return states
      .map((state, index) => ({ state, index }))
      .filter(({ index }) => !excluded.has(index))
      .filter(({ state }) => state.cooldownUntil <= now)
      .map(({ index }) => index);
  }

  function chooseNextIndex(excluded = new Set()) {
    const ready = getReadyIndices(excluded);
    if (ready.length > 0) {
      if (ready.includes(activeIndex) && !excluded.has(activeIndex)) {
        return activeIndex;
      }
      return ready[0];
    }

    const candidates = states
      .map((state, index) => ({ state, index }))
      .filter(({ index }) => !excluded.has(index))
      .sort((left, right) => left.state.cooldownUntil - right.state.cooldownUntil);
    return candidates[0]?.index ?? activeIndex;
  }

  function markFailure(index, error, retryAfterMs = null) {
    const state = states[index];
    if (!state) {
      return;
    }

    const retryMs = retryAfterMs ?? cooldownMs;
    state.cooldownUntil = Date.now() + retryMs;
    state.failCount += 1;
    state.lastError = String(error?.message || error);
  }

  function markSuccess(index) {
    const state = states[index];
    if (!state) {
      return;
    }

    state.cooldownUntil = 0;
    state.failCount = 0;
    state.lastError = null;
    state.lastRecoveredAt = new Date().toISOString();
  }

  function rotate(reason = 'manual') {
    const nextIndex = chooseNextIndex(new Set([activeIndex]));
    const previousIndex = activeIndex;
    activeIndex = nextIndex;
    if (previousIndex !== nextIndex) {
      console.warn(`[${label}] Switched RPC from ${states[previousIndex]?.url} to ${states[nextIndex]?.url} (${reason}).`);
    }
    return states[nextIndex]?.url ?? states[0]?.url;
  }

  async function withFailover(methodName, args = []) {
    const attemptLimit = Math.max(
      1,
      Math.min(states.length, Number.isInteger(maxRetries) && maxRetries > 0 ? maxRetries : states.length),
    );
    const attempted = new Set();
    let lastError = null;

    for (let attempt = 0; attempt < attemptLimit; attempt += 1) {
      const index = chooseNextIndex(attempted);
      const state = states[index];
      attempted.add(index);
      activeIndex = index;
      state.lastUsedAt = new Date().toISOString();

      try {
        const result = await state.connection[methodName](...args);
        markSuccess(index);
        return result;
      } catch (error) {
        lastError = error;
        if (!isRetryableRpcError(error) || attempt === attemptLimit - 1) {
          throw error;
        }

        markFailure(index, error);
        rotate(`${methodName} retry`);
        await delay(50);
      }
    }

    throw lastError ?? new Error(`${methodName} failed on every configured RPC endpoint.`);
  }

  async function rpcRequest(method, params = []) {
    const attemptLimit = Math.max(
      1,
      Math.min(states.length, Number.isInteger(maxRetries) && maxRetries > 0 ? maxRetries : states.length),
    );
    const attempted = new Set();
    let lastError = null;

    for (let attempt = 0; attempt < attemptLimit; attempt += 1) {
      const index = chooseNextIndex(attempted);
      const state = states[index];
      attempted.add(index);
      activeIndex = index;
      state.lastUsedAt = new Date().toISOString();

      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), timeoutMs);
      try {
        const response = await fetch(state.url, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            jsonrpc: '2.0',
            id: Date.now(),
            method,
            params,
          }),
          signal: controller.signal,
        });
        clearTimeout(timeout);

        if (!response.ok) {
          const retryAfterMs = parseRetryAfterMs(response.headers) ?? cooldownMs;
          const error = new Error(`RPC ${method} failed on ${state.url} with status ${response.status}.`);
          if (!isRetryableStatus(response.status) || attempt === attemptLimit - 1) {
            throw error;
          }

          markFailure(index, error, retryAfterMs);
          rotate(`${method} ${response.status}`);
          continue;
        }

        const payload = await response.json();
        if (payload?.error) {
          const error = new Error(payload.error.message || `${method} failed.`);
          if (!isRetryableRpcError(error) || attempt === attemptLimit - 1) {
            throw error;
          }

          markFailure(index, error);
          rotate(`${method} rpc_error`);
          continue;
        }

        markSuccess(index);
        return payload.result;
      } catch (error) {
        clearTimeout(timeout);
        lastError = error;
        if (!isRetryableRpcError(error) || attempt === attemptLimit - 1) {
          throw error;
        }

        markFailure(index, error);
        rotate(`${method} transport_error`);
      }
    }

    throw lastError ?? new Error(`RPC request ${method} failed on every configured endpoint.`);
  }

  function getRpcPoolStatus() {
    return {
      activeIndex,
      activeUrl: getActiveState()?.url ?? null,
      endpoints: states.map((state, index) => ({
        index,
        url: state.url,
        active: index === activeIndex,
        coolingDown: state.cooldownUntil > Date.now(),
        cooldownUntil: state.cooldownUntil > 0 ? new Date(state.cooldownUntil).toISOString() : null,
        failCount: state.failCount,
        lastError: state.lastError,
        lastUsedAt: state.lastUsedAt,
        lastRecoveredAt: state.lastRecoveredAt,
      })),
    };
  }

  const connectionProxy = new Proxy(states[0].connection, {
    get(_target, prop) {
      if (prop === 'rpcEndpoint') {
        return getActiveState()?.url ?? states[0]?.url;
      }
      if (prop === 'getRpcPoolStatus') {
        return getRpcPoolStatus;
      }
      if (prop === 'rotateRpc') {
        return rotate;
      }
      if (prop === '__rpcPool') {
        return {
          getRpcPoolStatus,
          rotateRpc: rotate,
          rpcRequest,
          getCurrentUrl: () => getActiveState()?.url ?? states[0]?.url,
        };
      }

      const currentConnection = getActiveState()?.connection ?? states[0].connection;
      const value = currentConnection[prop];
      if (typeof prop !== 'string' || typeof value !== 'function') {
        return value;
      }

      if (shouldTreatAsSubscription(prop)) {
        return value.bind(currentConnection);
      }

      if (!wrappedMethods.has(prop)) {
        wrappedMethods.set(prop, (...args) => withFailover(prop, args));
      }
      return wrappedMethods.get(prop);
    },
  });

  return {
    connection: connectionProxy,
    rpcRequest,
    rotateRpc: rotate,
    getRpcPoolStatus,
    urls: endpointUrls,
  };
}
