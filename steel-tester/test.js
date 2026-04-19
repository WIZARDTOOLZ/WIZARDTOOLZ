/**
 * test.js — Steel + Playwright Turnstile Challenge Test Runner
 *
 * Configure everything in .env, run `node test.js`, done.
 *
 * Prerequisites:
 *   npm install steel-sdk playwright dotenv
 *
 * .env:
 *   STEEL_API_KEY=your_steel_api_key
 *   TARGET_URL=https://your-site.com/page
 *   BUTTON=rocket        # rocket | fire | poop | flag
 *   SESSION_COUNT=3      # how many sessions to run
 */

import 'dotenv/config';
import Steel from 'steel-sdk';
import { chromium } from 'playwright';

// ─── BUTTON DEFINITIONS ───────────────────────────────────────────────────────
//
// Each button is identified by something unique *inside* its SVG so we never
// rely on fragile nth-child positioning. The selector walks up: find the SVG
// landmark, then grab its closest ancestor <button>.
//
// rocket  → SVG starts with a <polygon> (no other button has one)
// fire    → SVG contains a radialGradient with id="emoji-u1f525-g1"
// poop    → SVG contains a path filled with the exact brown rgb(136, 87, 66)
// flag    → SVG contains a path filled with the exact grey rgb(176, 176, 176)
//
// We use page.locator() with a filter so Playwright finds the button whose
// inner SVG matches a unique attribute or child element.

const BUTTONS = {
  rocket: {
    label: 'Rocket 🚀',
    // The rocket SVG is the only one that starts with a <polygon>
    locatorFn: (page) =>
      page.locator('button.chakra-button').filter({
        has: page.locator('svg polygon'),
      }).first(),
  },
  fire: {
    label: 'Fire 🔥',
    // The fire SVG is the only one with this specific gradient id
    locatorFn: (page) =>
      page.locator('button.chakra-button').filter({
        has: page.locator('svg #emoji-u1f525-g1'),
      }).first(),
  },
  poop: {
    label: 'Poop 💩',
    // The poop SVG is the only one with this exact brown fill color
    // We match via an xpath attribute check on the path element
    locatorFn: (page) =>
      page.locator('button.chakra-button').filter({
        has: page.locator('xpath=.//path[contains(@style,"136, 87, 66")]'),
      }).first(),
  },
  flag: {
    label: 'Flag 🚩',
    // The flag SVG is the only one with this exact grey fill color
    locatorFn: (page) =>
      page.locator('button.chakra-button').filter({
        has: page.locator('xpath=.//path[contains(@style,"176, 176, 176")]'),
      }).first(),
  },
};

// ─── ENV CONFIG ───────────────────────────────────────────────────────────────

function getConfig() {
  const missing = [];

  const apiKey = process.env.STEEL_API_KEY;
  if (!apiKey) missing.push('STEEL_API_KEY');

  const targetUrl = process.env.TARGET_URL;
  if (!targetUrl) missing.push('TARGET_URL');

  const buttonKey = (process.env.BUTTON || '').toLowerCase();
  if (!buttonKey) missing.push('BUTTON');
  else if (!BUTTONS[buttonKey]) {
    console.error(`\nERROR: BUTTON="${buttonKey}" is not valid.`);
    console.error(`       Choose one of: ${Object.keys(BUTTONS).join(' | ')}\n`);
    process.exit(1);
  }

  if (missing.length > 0) {
    console.error(`\nERROR: Missing required .env variables: ${missing.join(', ')}`);
    console.error('       See the .env section at the top of test.js\n');
    process.exit(1);
  }

  const sessionCount = parseInt(process.env.SESSION_COUNT || '3', 10);
  if (isNaN(sessionCount) || sessionCount < 1) {
    console.error('\nERROR: SESSION_COUNT must be a positive integer\n');
    process.exit(1);
  }

  return {
    apiKey,
    targetUrl,
    buttonKey,
    button: BUTTONS[buttonKey],
    sessionCount,

    // Timeouts — override in .env if needed
    buttonWaitMs:        parseInt(process.env.BUTTON_WAIT_MS        || '10000', 10),
    turnstileWaitMs:     parseInt(process.env.TURNSTILE_WAIT_MS     || '15000', 10),
    captchaSolveMs:      parseInt(process.env.CAPTCHA_SOLVE_MS      || '90000', 10),
    captchaPollMs:       parseInt(process.env.CAPTCHA_POLL_MS       || '3000',  10),
    postSolveSettleMs:   parseInt(process.env.POST_SOLVE_SETTLE_MS  || '3000',  10),
    sessionLaunchDelay:  parseInt(process.env.SESSION_LAUNCH_DELAY  || '1500',  10),
  };
}

// ─── HELPERS ──────────────────────────────────────────────────────────────────

function log(sessionIndex, level, msg, data = null) {
  const ts  = new Date().toISOString();
  const tag = `[${ts}] [Session ${sessionIndex + 1}] [${level.toUpperCase()}]`;
  data ? console.log(`${tag} ${msg}`, JSON.stringify(data, null, 2))
       : console.log(`${tag} ${msg}`);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ─── TURNSTILE DETECTION ──────────────────────────────────────────────────────

async function detectTurnstile(page) {
  return page.evaluate(() => {
    const iframe  = document.querySelector('iframe[src*="challenges.cloudflare.com"]');
    const widget  = document.querySelector('.cf-turnstile, [class*="cf-turnstile"], [data-sitekey]');
    return !!(iframe || widget);
  });
}

// ─── CAPTCHA SOLVE POLLING ────────────────────────────────────────────────────

async function waitForCaptchaSolve(steel, sessionId, sessionIndex, cfg) {
  const deadline = Date.now() + cfg.captchaSolveMs;

  while (Date.now() < deadline) {
    await sleep(cfg.captchaPollMs);

    let status;
    try {
      status = await steel.sessions.getSessionCaptchaStatus(sessionId);
    } catch (err) {
      log(sessionIndex, 'warn', 'Captcha poll error — retrying', { error: err.message });
      continue;
    }

    log(sessionIndex, 'debug', 'Captcha status', status);

    const pages = status?.pages ?? [];
    if (pages.length === 0) {
      log(sessionIndex, 'info', 'No pending tasks — likely auto-resolved before first poll');
      return { solved: true, status: 'auto_resolved' };
    }

    for (const p of pages) {
      for (const task of p?.captchaTasks ?? []) {
        if (task.status === 'solved')                        return { solved: true,  status: 'solved',      detail: task };
        if (['failed', 'error'].includes(task.status))      return { solved: false, status: task.status,   detail: task };
        log(sessionIndex, 'info', `Captcha in progress — ${task.status}`);
      }
    }
  }

  return { solved: false, status: 'timeout' };
}

// ─── SINGLE SESSION ───────────────────────────────────────────────────────────

async function runSession(sessionIndex, cfg, steel) {
  const result = {
    sessionIndex,
    sessionId:         null,
    buttonClicked:     false,
    turnstileDetected: false,
    captchaResult:     null,
    pageTitle:         null,
    finalUrl:          null,
    error:             null,
    durationMs:        null,
  };

  const t0      = Date.now();
  let session   = null;
  let browser   = null;

  try {
    // 1. Create Steel session
    log(sessionIndex, 'info', 'Creating Steel session…');
    session = await steel.sessions.create({
      solveCaptcha:   true,
      useProxy:       true,
      sessionTimeout: 300_000,
    });
    result.sessionId = session.id;
    log(sessionIndex, 'info', `Session ready: ${session.id}`);

    // 2. Connect Playwright
    browser = await chromium.connectOverCDP(
      `wss://connect.steel.dev?apiKey=${cfg.apiKey}&sessionId=${session.id}`
    );
    const page = browser.contexts()[0].pages()[0] ?? await browser.contexts()[0].newPage();

    // 3. Navigate
    log(sessionIndex, 'info', `→ ${cfg.targetUrl}`);
    await page.goto(cfg.targetUrl, { waitUntil: 'domcontentloaded', timeout: 30_000 });
    result.pageTitle = await page.title();
    result.finalUrl  = page.url();
    log(sessionIndex, 'info', `Loaded: "${result.pageTitle}"`);

    // 4. Click the configured button
    log(sessionIndex, 'info', `Waiting for ${cfg.button.label} button…`);
    const btn = cfg.button.locatorFn(page);
    await btn.waitFor({ state: 'visible', timeout: cfg.buttonWaitMs });
    await btn.click();
    result.buttonClicked = true;
    log(sessionIndex, 'info', `✓ Clicked ${cfg.button.label}`);

    // 5. Watch for Turnstile
    log(sessionIndex, 'info', 'Watching for Cloudflare Turnstile…');
    const deadline = Date.now() + cfg.turnstileWaitMs;
    while (Date.now() < deadline) {
      if (await detectTurnstile(page)) { result.turnstileDetected = true; break; }
      await sleep(500);
    }

    if (result.turnstileDetected) {
      log(sessionIndex, 'info', '⚠️  Turnstile detected — waiting for Steel to solve…');
      const cr = await waitForCaptchaSolve(steel, session.id, sessionIndex, cfg);
      result.captchaResult = cr;
      log(sessionIndex, cr.solved ? 'info' : 'warn',
        cr.solved ? `✅ Solved (${cr.status})` : `❌ Not solved — ${cr.status}`);
      await sleep(cfg.postSolveSettleMs);
    } else {
      log(sessionIndex, 'info', '⚪ No Turnstile — session was not challenged');
    }

    result.finalUrl  = page.url();
    result.pageTitle = await page.title();
    log(sessionIndex, 'info', `Done: "${result.pageTitle}" @ ${result.finalUrl}`);

  } catch (err) {
    log(sessionIndex, 'error', 'Session failed', { error: err.message });
    result.error = err.message;
  } finally {
    try { await browser?.close(); }   catch (_) {}
    if (session) {
      try {
        await steel.sessions.release(session.id);
        log(sessionIndex, 'info', 'Session released');
      } catch (err) {
        log(sessionIndex, 'warn', 'Release failed', { error: err.message });
      }
    }
    result.durationMs = Date.now() - t0;
  }

  return result;
}

// ─── SUMMARY ─────────────────────────────────────────────────────────────────

function printSummary(results, cfg) {
  const total      = results.length;
  const errored    = results.filter((r) => r.error).length;
  const challenged = results.filter((r) => r.turnstileDetected).length;
  const solved     = results.filter((r) => r.captchaResult?.solved).length;
  const clean      = results.filter((r) => !r.turnstileDetected && !r.error).length;
  const pct        = (n, d) => d === 0 ? '—' : `${Math.round((n / d) * 100)}%`;

  console.log('\n' + '═'.repeat(68));
  console.log('  SUMMARY');
  console.log('═'.repeat(68));
  console.log(`  Button:          ${cfg.button.label}`);
  console.log(`  Target:          ${cfg.targetUrl}`);
  console.log(`  Sessions run:    ${total}`);
  console.log(`  Errors:          ${errored}`);
  console.log(`  Got Turnstile:   ${challenged}  (${pct(challenged, total)})`);
  console.log(`  Captcha solved:  ${solved}  (${pct(solved, challenged)} of challenged)`);
  console.log(`  Not challenged:  ${clean}  (${pct(clean, total)})`);

  console.log('\n  Per-session:');
  for (const r of results) {
    const icon = r.error
      ? '💥 ERROR'
      : r.turnstileDetected
        ? r.captchaResult?.solved ? '✅ CHALLENGED → SOLVED' : '❌ CHALLENGED → UNSOLVED'
        : '⚪ NOT CHALLENGED';

    console.log(`\n  [${r.sessionIndex + 1}] ${icon}  (${r.durationMs}ms)`);
    console.log(`      ID:      ${r.sessionId ?? 'N/A'}`);
    console.log(`      Button:  ${r.buttonClicked ? 'clicked ✓' : 'not found ✗'}`);
    console.log(`      URL:     ${r.finalUrl ?? 'N/A'}`);
    if (r.captchaResult?.status) console.log(`      Captcha: ${r.captchaResult.status}`);
    if (r.error)                 console.log(`      Error:   ${r.error}`);
  }

  console.log('\n  ─── Rate-Limit Diagnosis ───');
  if (challenged === 0) {
    console.log('  ⚪ No sessions challenged. Turnstile not firing for this pattern.');
    console.log('     Try more sessions or a different button.');
  } else if (challenged === total) {
    console.log('  🟡 All sessions challenged. Either universal protection or');
    console.log('     your site challenges all automated traffic regardless of rate.');
  } else {
    const half     = Math.floor(total / 2);
    const earlyHit = results.slice(0, half).filter((r) => r.turnstileDetected).length;
    const lateHit  = results.slice(half).filter((r) => r.turnstileDetected).length;
    console.log(`  🔴 Partial challenges: ${pct(challenged, total)} of sessions hit Turnstile.`);
    console.log(`     Early half: ${earlyHit}/${half} challenged`);
    console.log(`     Late half:  ${lateHit}/${total - half} challenged`);
    if (lateHit > earlyHit) console.log('     → Escalating pattern — classic rate-limit behaviour.');
  }

  console.log('\n' + '═'.repeat(68) + '\n');
}

// ─── MAIN ─────────────────────────────────────────────────────────────────────

async function main() {
  const cfg   = getConfig();
  const steel = new Steel({ steelAPIKey: cfg.apiKey });

  console.log('\n' + '═'.repeat(68));
  console.log('  Steel + Playwright — Turnstile Test Runner');
  console.log('═'.repeat(68));
  console.log(`  Button:   ${cfg.button.label}`);
  console.log(`  Target:   ${cfg.targetUrl}`);
  console.log(`  Sessions: ${cfg.sessionCount}`);
  console.log();

  const results = [];
  for (let i = 0; i < cfg.sessionCount; i++) {
    if (i > 0) await sleep(cfg.sessionLaunchDelay);
    results.push(await runSession(i, cfg, steel));
  }

  printSummary(results, cfg);
}

main().catch((err) => {
  console.error('Fatal:', err);
  process.exit(1);
});
