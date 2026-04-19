import 'dotenv/config';
import Steel from 'steel-sdk';
import { chromium } from 'playwright';
import fs from 'node:fs/promises';
import path from 'node:path';

const ROOT_DIR = path.resolve('.');
const OUTPUT_DIR = path.join(ROOT_DIR, 'outputs');
const REACTION_ENDPOINT_FRAGMENT = '/hype/reactions/v2/dexPair/';
const REACTION_STATE_ENDPOINT_FRAGMENT = '/hype/reactions/dexPair/';
const DEFAULT_HYPE_API_BASE_URL = process.env.DEX_REACTION_API_BASE_URL || 'https://io.dexscreener.com';
const REACTION_KEYS = {
  rocket: 'rocket',
  fire: 'fire',
  poop: 'poop',
  flag: 'triangular_flag_on_post',
};

const BUTTONS = {
  rocket: {
    label: 'Rocket',
    emoji: '🚀',
    locatorFn: (page) =>
      page.locator('xpath=(//button[contains(@class,"chakra-button")][.//polygon[contains(@points,"3.77,71.73")]])[1]'),
  },
  fire: {
    label: 'Fire',
    emoji: '🔥',
    locatorFn: (page) =>
      page.locator('xpath=(//button[contains(@class,"chakra-button")][.//path[contains(@d,"M35.56,40.73")]])[1]'),
  },
  poop: {
    label: 'Poop',
    emoji: '💩',
    locatorFn: (page) =>
      page.locator('xpath=(//button[contains(@class,"chakra-button")][.//path[contains(@d,"M118.89,75.13")]])[1]'),
  },
  flag: {
    label: 'Flag',
    emoji: '🚩',
    locatorFn: (page) =>
      page.locator('xpath=(//button[contains(@class,"chakra-button")][.//path[contains(@d,"M8.04,3.32")]])[1]'),
  },
};

function parsePairIdentity(targetUrl) {
  const url = new URL(targetUrl);
  const parts = url.pathname.split('/').filter(Boolean);

  if (parts.length < 2) {
    throw new Error(`Unsupported Dex URL path: ${url.pathname}`);
  }

  return {
    chainId: parts[0].toLowerCase(),
    pairId: parts[1],
  };
}

function parseReactionCount(text) {
  const match = String(text || '').match(/(\d[\d,]*)\s*$/);
  if (!match) {
    return null;
  }

  return Number.parseInt(match[1].replace(/,/g, ''), 10);
}

function getReactionMatcherSource(buttonKey) {
  if (buttonKey === 'rocket') {
    return `
      Array.from(button.querySelectorAll('polygon')).some((node) =>
        (node.getAttribute('points') || '').includes('3.77,71.73')
      )
    `;
  }

  if (buttonKey === 'fire') {
    return `
      Array.from(button.querySelectorAll('path')).some((node) =>
        (node.getAttribute('d') || '').includes('M35.56,40.73')
      )
    `;
  }

  if (buttonKey === 'poop') {
    return `
      Array.from(button.querySelectorAll('path')).some((node) =>
        (node.getAttribute('d') || '').includes('M118.89,75.13')
      )
    `;
  }

  return `
    Array.from(button.querySelectorAll('path')).some((node) =>
      (node.getAttribute('d') || '').includes('M8.04,3.32')
    )
  `;
}

function getConfig() {
  const missing = [];

  const apiKey = process.env.STEEL_API_KEY;
  if (!apiKey) missing.push('STEEL_API_KEY');

  const targetUrl = process.env.TARGET_URL;
  if (!targetUrl) missing.push('TARGET_URL');

  const buttonKey = (process.env.BUTTON || '').toLowerCase();
  if (!buttonKey) missing.push('BUTTON');
  else if (!BUTTONS[buttonKey]) missing.push('BUTTON(valid: rocket|fire|poop|flag)');

  const sessionCount = Number.parseInt(process.env.SESSION_COUNT || '1', 10);
  if (!Number.isInteger(sessionCount) || sessionCount < 1) {
    missing.push('SESSION_COUNT(positive integer)');
  }

  if (missing.length) {
    throw new Error(`Missing or invalid environment values: ${missing.join(', ')}`);
  }

  return {
    apiKey,
    targetUrl,
    pairIdentity: parsePairIdentity(targetUrl),
    buttonKey,
    button: BUTTONS[buttonKey],
    reactionKey: REACTION_KEYS[buttonKey],
    hypeApiBaseUrl: process.env.DEX_REACTION_API_BASE_URL || DEFAULT_HYPE_API_BASE_URL,
    sessionCount,
    navigationTimeoutMs: Number.parseInt(process.env.NAVIGATION_TIMEOUT_MS || '30000', 10),
    buttonWaitMs: Number.parseInt(process.env.BUTTON_WAIT_MS || '10000', 10),
    reactionWaitMs: Number.parseInt(process.env.REACTION_WAIT_MS || '45000', 10),
    dexReadyWaitMs: Number.parseInt(process.env.DEX_READY_WAIT_MS || '60000', 10),
    postClickSettleMs: Number.parseInt(process.env.POST_CLICK_SETTLE_MS || '250', 10),
    sessionLaunchDelayMs: Number.parseInt(process.env.SESSION_LAUNCH_DELAY || '1000', 10),
    saveSuccessScreenshot: process.env.SAVE_SUCCESS_SCREENSHOT === '1',
    turnstileWaitMs: Number.parseInt(process.env.TURNSTILE_WAIT_MS || '45000', 10),
    verifyPollMs: Number.parseInt(process.env.REACTION_VERIFY_POLL_MS || '1250', 10),
    uiFallbackWaitMs: Number.parseInt(process.env.UI_FALLBACK_WAIT_MS || '45000', 10),
  };
}

function log(message, data = null) {
  const prefix = `[${new Date().toISOString()}]`;
  if (data) {
    console.log(`${prefix} ${message} ${JSON.stringify(data)}`);
    return;
  }
  console.log(`${prefix} ${message}`);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function elapsedMs(startTime) {
  return Date.now() - startTime;
}

function formatElapsed(ms) {
  if (!Number.isFinite(ms)) {
    return 'n/a';
  }
  return `${(ms / 1000).toFixed(2)}s`;
}

async function ensureOutputDir() {
  await fs.mkdir(OUTPUT_DIR, { recursive: true });
}

async function detectTurnstile(page) {
  return page.evaluate(() => {
    const iframe = document.querySelector('iframe[src*="challenges.cloudflare.com"]');
    const widget = document.querySelector('.cf-turnstile, [class*="cf-turnstile"], [data-sitekey]');
    return Boolean(iframe || widget);
  });
}

async function waitForTurnstile(page, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await detectTurnstile(page)) {
      return true;
    }
    await sleep(500);
  }
  return false;
}

async function waitForDexPageReady(page, timeoutMs) {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    const title = await page.title().catch(() => '');
    const body = await page.locator('body').innerText({ timeout: 2000 }).catch(() => '');
    const lowerBody = body.toLowerCase();
    const buttonCount = await page.locator('button.chakra-button').count().catch(() => 0);
    const stillVerifying =
      title === 'Just a moment...' ||
      lowerBody.includes('performing security verification') ||
      lowerBody.includes('turnstile solving...');

    if (!stillVerifying && buttonCount > 0) {
      return { title, buttonCount };
    }

    await sleep(1000);
  }

  const finalTitle = await page.title().catch(() => '');
  const finalBody = await page.locator('body').innerText({ timeout: 2000 }).catch(() => '');
  throw new Error(
    `Dex page did not become ready in time. Title="${finalTitle}" BodyHint="${finalBody.slice(0, 160).replace(/\s+/g, ' ')}"`,
  );
}

async function collectBlockedSignal(page) {
  const patterns = [
    'too many requests',
    'rate limit',
    'temporarily blocked',
    'request blocked',
    'try again later',
    'access denied',
    'challenge required',
  ];

  try {
    const body = (await page.locator('body').innerText({ timeout: 5000 })).toLowerCase();
    return patterns.find((pattern) => body.includes(pattern)) ?? null;
  } catch {
    return null;
  }
}

async function markReactionButton(page, buttonKey) {
  const match = await page.evaluate((target) => {
    const buttons = Array.from(document.querySelectorAll('button.chakra-button'));
    buttons.forEach((button) => button.removeAttribute('data-reaction-target'));

    const isMatch = (button) => {
      if (target === 'rocket') {
        return Array.from(button.querySelectorAll('polygon')).some((node) =>
          (node.getAttribute('points') || '').includes('3.77,71.73'));
      }

      if (target === 'fire') {
        return Array.from(button.querySelectorAll('path')).some((node) =>
          (node.getAttribute('d') || '').includes('M35.56,40.73'));
      }

      if (target === 'poop') {
        return Array.from(button.querySelectorAll('path')).some((node) =>
          (node.getAttribute('d') || '').includes('M118.89,75.13'));
      }

      if (target === 'flag') {
        return Array.from(button.querySelectorAll('path')).some((node) =>
          (node.getAttribute('d') || '').includes('M8.04,3.32'));
      }

      return false;
    };

    const foundIndex = buttons.findIndex((button) => {
      if (!isMatch(button)) {
        return false;
      }

      const rect = button.getBoundingClientRect();
      const style = window.getComputedStyle(button);
      return style.display !== 'none' &&
        style.visibility !== 'hidden' &&
        style.opacity !== '0' &&
        rect.width > 0 &&
        rect.height > 0;
    });
    if (foundIndex < 0) {
      return null;
    }

    const button = buttons[foundIndex];
    button.setAttribute('data-reaction-target', target);
    const rect = button.getBoundingClientRect();
    return {
      index: foundIndex,
      text: (button.textContent || '').trim(),
      width: rect.width,
      height: rect.height,
    };
  }, buttonKey);

  if (!match) {
    throw new Error(`Could not find visible ${buttonKey} reaction button on the Dex page.`);
  }

  return match;
}

async function waitForReactionRowReady(page, buttonKey, timeoutMs) {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    const state = await page.evaluate((target) => {
      const buttons = Array.from(document.querySelectorAll('button.chakra-button'));

      const isMatch = (button) => {
        if (target === 'rocket') {
          return Array.from(button.querySelectorAll('polygon')).some((node) =>
            (node.getAttribute('points') || '').includes('3.77,71.73'));
        }

        if (target === 'fire') {
          return Array.from(button.querySelectorAll('path')).some((node) =>
            (node.getAttribute('d') || '').includes('M35.56,40.73'));
        }

        if (target === 'poop') {
          return Array.from(button.querySelectorAll('path')).some((node) =>
            (node.getAttribute('d') || '').includes('M118.89,75.13'));
        }

        if (target === 'flag') {
          return Array.from(button.querySelectorAll('path')).some((node) =>
            (node.getAttribute('d') || '').includes('M8.04,3.32'));
        }

        return false;
      };

      const button = buttons.find((candidate) => isMatch(candidate));
      if (!button) {
        return { ready: false, reason: 'missing' };
      }

      const title = document.title || '';
      const text = (button.textContent || '').trim();
      const ready = !title.startsWith('Loading ') && /^\d+$/.test(text);
      return { ready, title, text };
    }, buttonKey);

    if (state.ready) {
      return state;
    }

    await sleep(1000);
  }

  const finalState = await page.evaluate((target) => {
    const buttons = Array.from(document.querySelectorAll('button.chakra-button'));
    const button = buttons.find((candidate) => {
      if (target === 'rocket') {
        return Array.from(candidate.querySelectorAll('polygon')).some((node) =>
          (node.getAttribute('points') || '').includes('3.77,71.73'));
      }
      if (target === 'fire') {
        return Array.from(candidate.querySelectorAll('path')).some((node) =>
          (node.getAttribute('d') || '').includes('M35.56,40.73'));
      }
      if (target === 'poop') {
        return Array.from(candidate.querySelectorAll('path')).some((node) =>
          (node.getAttribute('d') || '').includes('M118.89,75.13'));
      }
      if (target === 'flag') {
        return Array.from(candidate.querySelectorAll('path')).some((node) =>
          (node.getAttribute('d') || '').includes('M8.04,3.32'));
      }
      return false;
    });
    return {
      title: document.title || '',
      text: button ? (button.textContent || '').trim() : '',
    };
  }, buttonKey);

  throw new Error(
    `Reaction row did not become ready in time. Title="${finalState.title}" ButtonText="${finalState.text}"`,
  );
}

async function clickMarkedReactionButton(page, buttonKey) {
  const geometry = await page.evaluate((target) => {
    const button = document.querySelector(`button[data-reaction-target="${target}"]`);
    if (!button) {
      return null;
    }

    button.scrollIntoView({ block: 'center', inline: 'center', behavior: 'instant' });
    const rect = button.getBoundingClientRect();
    return {
      x: rect.left + (rect.width / 2),
      y: rect.top + (rect.height / 2),
      width: rect.width,
      height: rect.height,
    };
  }, buttonKey);

  if (!geometry || !Number.isFinite(geometry.x) || !Number.isFinite(geometry.y)) {
    throw new Error(`Could not resolve click geometry for ${buttonKey} reaction button.`);
  }

  await page.mouse.move(geometry.x, geometry.y, { steps: 10 });
  await sleep(120);
  await page.mouse.down();
  await sleep(60);
  await page.mouse.up();

  return geometry;
}

async function getReactionDomState(page, buttonKey) {
  return page.evaluate(
    ({ target }) => {
      const buttons = Array.from(document.querySelectorAll('button.chakra-button'));
      const button = buttons.find((candidate) => {
        if (target === 'rocket') {
          return Array.from(candidate.querySelectorAll('polygon')).some((node) =>
            (node.getAttribute('points') || '').includes('3.77,71.73'));
        }
        if (target === 'fire') {
          return Array.from(candidate.querySelectorAll('path')).some((node) =>
            (node.getAttribute('d') || '').includes('M35.56,40.73'));
        }
        if (target === 'poop') {
          return Array.from(candidate.querySelectorAll('path')).some((node) =>
            (node.getAttribute('d') || '').includes('M118.89,75.13'));
        }
        return Array.from(candidate.querySelectorAll('path')).some((node) =>
          (node.getAttribute('d') || '').includes('M8.04,3.32'));
      });

      if (!button) {
        return { found: false };
      }

      const text = (button.textContent || '').trim();
      const countMatch = text.match(/(\d[\d,]*)\s*$/);
      const reacted =
        button.getAttribute('aria-pressed') === 'true' ||
        button.getAttribute('data-state') === 'on' ||
        button.getAttribute('data-active') === 'true';

      return {
        found: true,
        text,
        count: countMatch ? Number.parseInt(countMatch[1].replace(/,/g, ''), 10) : null,
        disabled: button.disabled,
        ariaDisabled: button.getAttribute('aria-disabled'),
        ariaPressed: button.getAttribute('aria-pressed'),
        reacted,
      };
    },
    { target: buttonKey },
  );
}

async function fetchReactionState(page, cfg) {
  return page.evaluate(async ({ baseUrl, endpointPath, chainId, pairId }) => {
    const url = new URL(baseUrl);
    url.pathname = `${endpointPath}${chainId}:${pairId}`;

    try {
      const response = await fetch(url.toString(), {
        method: 'GET',
        mode: 'cors',
        credentials: 'include',
      });
      const text = await response.text();
      let payload = null;

      try {
        payload = JSON.parse(text);
      } catch {}

      return {
        ok: response.ok,
        status: response.status,
        url: response.url,
        payload,
        text,
      };
    } catch (error) {
      return {
        ok: false,
        status: null,
        url: url.toString(),
        payload: null,
        text: null,
        error: error?.message || String(error),
      };
    }
  }, {
    baseUrl: cfg.hypeApiBaseUrl,
    endpointPath: REACTION_STATE_ENDPOINT_FRAGMENT,
    chainId: cfg.pairIdentity.chainId,
    pairId: cfg.pairIdentity.pairId,
  });
}

async function getReactionSiteKey(page) {
  return page.evaluate(() => {
    return window.__DS_ENV?.DS_TURNSTILE_REACTIONS_SITE_KEY ?? null;
  });
}

async function obtainTurnstileToken(page, siteKey, timeoutMs) {
  if (!siteKey) {
    throw new Error('Dex Turnstile reactions site key was not available on the page.');
  }

  return page.evaluate(
    async ({ siteKeyValue, timeout }) => {
      const loadTurnstileApi = async () => {
        if (window.turnstile) {
          return;
        }

        await new Promise((resolve, reject) => {
          const existing = document.querySelector('script[src*="challenges.cloudflare.com/turnstile"]');
          const done = () => resolve();
          const fail = () => reject(new Error('Failed to load Turnstile API.'));

          if (existing) {
            existing.addEventListener('load', done, { once: true });
            existing.addEventListener('error', fail, { once: true });
            return;
          }

          const callbackName = `__wizardtoolzTurnstileOnLoad_${Date.now()}`;
          window[callbackName] = () => {
            delete window[callbackName];
            resolve();
          };

          const script = document.createElement('script');
          script.src = `https://challenges.cloudflare.com/turnstile/v0/api.js?onload=${callbackName}&render=explicit`;
          script.async = true;
          script.addEventListener('error', () => {
            delete window[callbackName];
            fail();
          }, { once: true });
          document.head.appendChild(script);
        });
      };

      await loadTurnstileApi();

      const existingHost = document.getElementById('__wizardtoolz-turnstile-host');
      if (existingHost) {
        existingHost.remove();
      }

      const host = document.createElement('div');
      host.id = '__wizardtoolz-turnstile-host';
      host.style.position = 'fixed';
      host.style.bottom = '16px';
      host.style.right = '16px';
      host.style.zIndex = '2147483647';
      host.style.background = 'transparent';
      document.body.appendChild(host);

      return await new Promise((resolve, reject) => {
        let widgetId = null;
        let finished = false;
        const cleanup = () => {
          if (widgetId !== null && window.turnstile) {
            try {
              window.turnstile.remove(widgetId);
            } catch {}
          }
          host.remove();
        };
        const settle = (fn, value) => {
          if (finished) {
            return;
          }
          finished = true;
          clearTimeout(timer);
          cleanup();
          fn(value);
        };
        const timer = setTimeout(() => {
          settle(reject, new Error('Timed out waiting for Turnstile token.'));
        }, timeout);

        try {
          widgetId = window.turnstile.render(host, {
            sitekey: siteKeyValue,
            theme: 'auto',
            fixedSize: true,
            callback: (token) => settle(resolve, { token, widgetId }),
            'error-callback': (code) => settle(reject, new Error(`Turnstile error: ${code}`)),
            'expired-callback': () => settle(reject, new Error('Turnstile token expired before submit.')),
            'timeout-callback': () => settle(reject, new Error('Turnstile challenge timed out.')),
          });
        } catch (error) {
          settle(reject, error);
        }
      });
    },
    { siteKeyValue: siteKey, timeout: timeoutMs },
  );
}

async function submitReactionTransport(page, cfg, token, transport) {
  return page.evaluate(
    async ({ baseUrl, endpointPath, chainId, pairId, reactionKey, captchaValue, methodName }) => {
      const url = new URL(baseUrl);
      url.pathname = `${endpointPath}${chainId}:${pairId}`;
      url.searchParams.set('captchaValue', captchaValue);

      const body = JSON.stringify({ emoji: reactionKey });

      try {
        if (methodName === 'json-fetch') {
          const response = await fetch(url.toString(), {
            method: 'POST',
            mode: 'cors',
            credentials: 'include',
            headers: {
              accept: '*/*',
              'content-type': 'application/json',
            },
            body,
          });
          const text = await response.text();
          return { transport: methodName, ok: response.ok, status: response.status, text };
        }

        if (methodName === 'text-fetch') {
          const response = await fetch(url.toString(), {
            method: 'POST',
            mode: 'cors',
            credentials: 'include',
            headers: {
              accept: '*/*',
              'content-type': 'text/plain;charset=UTF-8',
            },
            body,
          });
          const text = await response.text();
          return { transport: methodName, ok: response.ok, status: response.status, text };
        }

        if (methodName === 'beacon') {
          const sent = navigator.sendBeacon(
            url.toString(),
            new Blob([body], { type: 'text/plain;charset=UTF-8' }),
          );
          return { transport: methodName, ok: sent, status: sent ? 202 : null, text: null };
        }

        throw new Error(`Unknown transport: ${methodName}`);
      } catch (error) {
        return {
          transport: methodName,
          ok: false,
          status: null,
          text: null,
          error: error?.message || String(error),
        };
      }
    },
    {
      baseUrl: cfg.hypeApiBaseUrl,
      endpointPath: REACTION_ENDPOINT_FRAGMENT,
      chainId: cfg.pairIdentity.chainId,
      pairId: cfg.pairIdentity.pairId,
      reactionKey: cfg.reactionKey,
      captchaValue: token,
      methodName: transport,
    },
  );
}

async function waitForVerifiedReaction(page, cfg, initialDomState, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  let lastApiState = null;
  let lastDomState = initialDomState;

  while (Date.now() < deadline) {
    lastApiState = await fetchReactionState(page, cfg);
    if (lastApiState?.payload?.userReaction?.reaction === cfg.reactionKey) {
      return {
        verificationSource: 'api',
        apiState: lastApiState,
        domState: await getReactionDomState(page, cfg.buttonKey),
      };
    }

    lastDomState = await getReactionDomState(page, cfg.buttonKey);
    if (
      lastDomState?.found &&
      typeof initialDomState?.count === 'number' &&
      typeof lastDomState.count === 'number' &&
      lastDomState.count > initialDomState.count
    ) {
      return {
        verificationSource: 'dom-count',
        apiState: lastApiState,
        domState: lastDomState,
      };
    }

    if (lastDomState?.reacted) {
      return {
        verificationSource: 'dom-reacted',
        apiState: lastApiState,
        domState: lastDomState,
      };
    }

    await sleep(cfg.verifyPollMs);
  }

  throw new Error(
    `Reaction could not be verified. DOM=${JSON.stringify(lastDomState)} API=${JSON.stringify(lastApiState)}`,
  );
}

async function waitForReactionConfirmation(page, reactionKey, timeoutMs) {
  const response = await page.waitForResponse(async (candidate) => {
    if (!candidate.url().includes(REACTION_ENDPOINT_FRAGMENT)) {
      return false;
    }

    if (candidate.request().method() !== 'POST') {
      return false;
    }

    const postData = candidate.request().postData() || '';
    if (!postData.includes(`"emoji":"${reactionKey}"`)) {
      return false;
    }

    return true;
  }, { timeout: timeoutMs });

  const payload = await response.json().catch(() => null);
  const confirmedReaction = payload?.userReaction?.reaction || null;
  const totals = payload?.reactions || null;

  if (confirmedReaction !== reactionKey) {
    throw new Error(`Reaction response did not confirm ${reactionKey}.`);
  }

  return {
    responseUrl: response.url(),
    confirmedReaction,
    totals,
  };
}

async function runSingleSession(index, cfg, steel) {
  const sessionStart = Date.now();
  const result = {
    index,
    sessionId: null,
    clicked: false,
    turnstileDetected: false,
    turnstileTokenLength: null,
    reactionConfirmed: false,
    confirmedReaction: null,
    reactionTotals: null,
    reactionResponseUrl: null,
    reactionVerificationSource: null,
    submissionTransport: null,
    submissionAttempts: [],
    blockedSignal: null,
    screenshotPath: null,
    finalUrl: null,
    title: null,
    error: null,
    timingsMs: {
      total: null,
      navigation: null,
      pageReady: null,
      reactionRowReady: null,
      preClick: null,
      postClickToConfirm: null,
      screenshot: null,
    },
  };

  let session;
  let browser;

  try {
    log(`Session ${index + 1}/${cfg.sessionCount}: creating Steel session`);
    session = await steel.sessions.create({
      solveCaptcha: true,
      useProxy: true,
      sessionTimeout: 300000,
    });
    result.sessionId = session.id;

    browser = await chromium.connectOverCDP(
      `wss://connect.steel.dev?apiKey=${encodeURIComponent(cfg.apiKey)}&sessionId=${encodeURIComponent(session.id)}`,
    );

    const context = browser.contexts()[0] ?? await browser.newContext();
    const page = context.pages()[0] ?? await context.newPage();
    page.setDefaultTimeout(cfg.navigationTimeoutMs);
    page.setDefaultNavigationTimeout(cfg.navigationTimeoutMs);

    log(`Session ${index + 1}: navigating`, { target: cfg.targetUrl });
    const navigationStart = Date.now();
    await page.goto(cfg.targetUrl, {
      waitUntil: 'domcontentloaded',
      timeout: cfg.navigationTimeoutMs,
    });
    result.timingsMs.navigation = elapsedMs(navigationStart);

    const pageReadyStart = Date.now();
    const dexReady = await waitForDexPageReady(page, cfg.dexReadyWaitMs);
    result.timingsMs.pageReady = elapsedMs(pageReadyStart);
    log(`Session ${index + 1}: Dex page ready`, dexReady);

    const rowReadyStart = Date.now();
    const reactionRowState = await waitForReactionRowReady(page, cfg.buttonKey, cfg.dexReadyWaitMs);
    result.timingsMs.reactionRowReady = elapsedMs(rowReadyStart);
    log(`Session ${index + 1}: reaction row ready`, reactionRowState);

    const initialDomState = await getReactionDomState(page, cfg.buttonKey);
    const siteKey = await getReactionSiteKey(page);
    log(`Session ${index + 1}: reaction baseline`, {
      dom: initialDomState,
      siteKeyPresent: Boolean(siteKey),
      pairIdentity: cfg.pairIdentity,
    });

    let rescuedByBeacon = false;
    let rescueTokenLength = null;
    page.on('requestfailed', async (request) => {
      try {
        if (!request.url().includes(REACTION_ENDPOINT_FRAGMENT) || rescuedByBeacon) {
          return;
        }

        const token = new URL(request.url()).searchParams.get('captchaValue');
        if (!token) {
          return;
        }

        rescuedByBeacon = true;
        rescueTokenLength = token.length;
        log(`Session ${index + 1}: reaction POST failed, attempting beacon rescue`, {
          error: request.failure()?.errorText ?? 'unknown',
          tokenLength: rescueTokenLength,
        });

        const submission = await submitReactionTransport(page, cfg, token, 'beacon');
        result.submissionAttempts.push(submission);
        result.submissionTransport = 'beacon-rescue';
        log(`Session ${index + 1}: beacon rescue attempted`, submission);
      } catch (error) {
        log(`Session ${index + 1}: beacon rescue failed`, { error: error.message });
      }
    });

    const buttonMatch = await markReactionButton(page, cfg.buttonKey);
    const button = page.locator(`button[data-reaction-target="${cfg.buttonKey}"]`);
    await button.waitFor({ state: 'attached', timeout: cfg.buttonWaitMs });
    const preClickStart = Date.now();
    const confirmationPromise = waitForReactionConfirmation(page, cfg.reactionKey, cfg.uiFallbackWaitMs)
      .then((value) => ({ source: 'response', value }));
    const verificationPromise = waitForVerifiedReaction(page, cfg, initialDomState, cfg.uiFallbackWaitMs)
      .then((value) => ({ source: 'verification', value }));
    const clickGeometry = await clickMarkedReactionButton(page, cfg.buttonKey);
    result.clicked = true;
    result.timingsMs.preClick = elapsedMs(preClickStart);
    log(`Session ${index + 1}: UI click ${cfg.button.emoji} ${cfg.button.label}`, {
      ...buttonMatch,
      clickGeometry,
    });

    await sleep(cfg.postClickSettleMs);
    const postClickStart = Date.now();
    const raceOutcome = await Promise.any([
      confirmationPromise,
      verificationPromise,
    ]);
    result.timingsMs.postClickToConfirm = elapsedMs(postClickStart);

    if (raceOutcome.source === 'response') {
      const confirmation = raceOutcome.value;
      result.reactionConfirmed = true;
      result.confirmedReaction = confirmation.confirmedReaction;
      result.reactionTotals = confirmation.totals;
      result.reactionResponseUrl = confirmation.responseUrl;
      result.reactionVerificationSource = rescuedByBeacon ? 'ui-response-after-rescue' : 'ui-response';
      if (rescueTokenLength) {
        result.turnstileDetected = true;
        result.turnstileTokenLength = rescueTokenLength;
      }
      log(`Session ${index + 1}: reaction confirmed`, {
        reaction: confirmation.confirmedReaction,
        totals: confirmation.totals,
        rescuedByBeacon,
      });
    } else if (raceOutcome.source === 'verification') {
      const verified = raceOutcome.value;
      result.reactionConfirmed = true;
      result.confirmedReaction =
        verified.apiState?.payload?.userReaction?.reaction ?? cfg.reactionKey;
      result.reactionTotals = verified.apiState?.payload?.reactions ?? null;
      result.reactionResponseUrl = verified.apiState?.url ?? null;
      result.reactionVerificationSource = verified.verificationSource;
      result.submissionTransport = rescuedByBeacon ? 'beacon-rescue' : result.submissionTransport;
      if (rescueTokenLength) {
        result.turnstileDetected = true;
        result.turnstileTokenLength = rescueTokenLength;
      }
      log(`Session ${index + 1}: reaction verified after delayed state sync`, {
        source: verified.verificationSource,
        reaction: result.confirmedReaction,
        totals: result.reactionTotals,
        rescuedByBeacon,
      });
    }

    result.blockedSignal = await collectBlockedSignal(page);
    result.finalUrl = page.url();
    result.title = await page.title();

    if (cfg.saveSuccessScreenshot) {
      const screenshotStart = Date.now();
      const screenshotName = `run-${Date.now()}-session-${index + 1}.png`;
      result.screenshotPath = path.join(OUTPUT_DIR, screenshotName);
      await page.screenshot({
        path: result.screenshotPath,
        fullPage: true,
      });
      result.timingsMs.screenshot = elapsedMs(screenshotStart);
      log(`Session ${index + 1}: screenshot saved`, { path: result.screenshotPath });
    }
  } catch (error) {
    result.error = error.message;
    log(`Session ${index + 1}: failed`, { error: error.message });
  } finally {
    try {
      await browser?.close();
    } catch {}

    if (session) {
      try {
        await steel.sessions.release(session.id);
        log(`Session ${index + 1}: released Steel session`);
      } catch (error) {
        log(`Session ${index + 1}: release failed`, { error: error.message });
      }
    }

    result.timingsMs.total = elapsedMs(sessionStart);
  }

  return result;
}

function printSummary(cfg, results) {
  const challenged = results.filter((result) => result.turnstileDetected).length;
  const failed = results.filter((result) => result.error).length;
  const blocked = results.filter((result) => result.blockedSignal).length;

  console.log('');
  console.log('='.repeat(72));
  console.log('SAFE STEEL RUNNER SUMMARY');
  console.log('='.repeat(72));
  console.log(`Target:   ${cfg.targetUrl}`);
  console.log(`Button:   ${cfg.button.emoji} ${cfg.button.label}`);
  console.log(`Sessions: ${cfg.sessionCount}`);
  console.log(`Errors:   ${failed}`);
  console.log(`Challenges detected: ${challenged}`);
  console.log(`Blocked signals:     ${blocked}`);
  console.log('');

  for (const result of results) {
    console.log(`Session ${result.index + 1}:`);
    console.log(`  Session ID:   ${result.sessionId ?? 'N/A'}`);
    console.log(`  Clicked:      ${result.clicked ? 'yes' : 'no'}`);
    console.log(`  Turnstile:    ${result.turnstileDetected ? 'detected' : 'not detected'}`);
    console.log(`  Token length: ${result.turnstileTokenLength ?? 'N/A'}`);
    console.log(`  Confirmed:    ${result.reactionConfirmed ? 'yes' : 'no'}`);
    console.log(`  Reaction:     ${result.confirmedReaction ?? 'N/A'}`);
    console.log(`  Verify via:   ${result.reactionVerificationSource ?? 'N/A'}`);
    console.log(`  Transport:    ${result.submissionTransport ?? 'N/A'}`);
    console.log(`  Blocked hint: ${result.blockedSignal ?? 'none'}`);
    console.log(`  Title:        ${result.title ?? 'N/A'}`);
    console.log(`  URL:          ${result.finalUrl ?? 'N/A'}`);
    console.log(`  API URL:      ${result.reactionResponseUrl ?? 'N/A'}`);
    console.log(`  Totals:       ${result.reactionTotals ? JSON.stringify(result.reactionTotals) : 'N/A'}`);
    console.log(`  Shot path:    ${result.screenshotPath ?? 'N/A'}`);
    console.log(`  Timing total: ${formatElapsed(result.timingsMs?.total)}`);
    console.log(`  Nav:          ${formatElapsed(result.timingsMs?.navigation)}`);
    console.log(`  Page ready:   ${formatElapsed(result.timingsMs?.pageReady)}`);
    console.log(`  Row ready:    ${formatElapsed(result.timingsMs?.reactionRowReady)}`);
    console.log(`  Pre-click:    ${formatElapsed(result.timingsMs?.preClick)}`);
    console.log(`  Confirm:      ${formatElapsed(result.timingsMs?.postClickToConfirm)}`);
    if (result.timingsMs?.screenshot !== null) {
      console.log(`  Shot save:    ${formatElapsed(result.timingsMs?.screenshot)}`);
    }
    if (result.submissionAttempts?.length) {
      console.log(`  Attempts:     ${JSON.stringify(result.submissionAttempts)}`);
    }
    if (result.error) {
      console.log(`  Error:        ${result.error}`);
    }
    console.log('');
  }
}

async function main() {
  const cfg = getConfig();
  await ensureOutputDir();

  const steel = new Steel({ apiKey: cfg.apiKey });
  const results = [];

  log('Starting safe Steel runner', {
    target: cfg.targetUrl,
    button: cfg.buttonKey,
    sessions: cfg.sessionCount,
    solveCaptcha: true,
    useProxy: true,
  });

  for (let index = 0; index < cfg.sessionCount; index += 1) {
    if (index > 0) {
      await sleep(cfg.sessionLaunchDelayMs);
    }
    results.push(await runSingleSession(index, cfg, steel));
  }

  printSummary(cfg, results);
  process.exitCode = results.some((result) => result.error) ? 1 : 0;
}

main().catch((error) => {
  console.error(`Fatal: ${error.message}`);
  process.exit(1);
});
