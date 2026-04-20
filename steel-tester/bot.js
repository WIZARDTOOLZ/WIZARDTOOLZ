import 'dotenv/config';
import { Bot, InlineKeyboard, InputFile, InputMediaBuilder } from 'grammy';
import { generateKeyPairSync } from 'node:crypto';
import fs from 'node:fs/promises';
import path from 'node:path';
import { spawn } from 'node:child_process';
import sharp from 'sharp';
import {
  Connection,
  Keypair,
  PublicKey,
  SystemProgram,
  Transaction,
} from '@solana/web3.js';
import { TOKEN_2022_PROGRAM_ID, TOKEN_PROGRAM_ID } from '@solana/spl-token';
import {
  BURN_AGENT_ALERT_IMAGE_PATH,
  BURN_AGENT_MENU_IMAGE_PATH,
  BRAND_NAME,
  BRAND_TAGLINE,
  DATA_DIR,
  FOMO_MENU_IMAGE_PATH,
  HOLDER_BOOSTER_MENU_IMAGE_PATH,
  MAGIC_SELL_MENU_IMAGE_PATH,
  MENU_EMOJI_IMAGE_PATH,
  MENU_HOME_IMAGE_PATH,
  MENU_LOGO_IMAGE_PATH,
  REACTION_MENU_IMAGE_PATH,
  RESIZER_PRESETS,
  SALES_BROADCAST_IMAGE_PATH,
  SNIPER_MENU_IMAGE_PATH,
  STORE_PATH,
  SUPPORT_USERNAME,
  TG_DIVIDER,
  VOLUME_MENU_IMAGE_PATH,
} from './lib/brand.js';
import { createBotConfig } from './lib/config.js';
import { createManagedSolanaRpcPool } from './lib/solana/rpcPool.js';
import {
  registerTelegramCommands,
  startPaymentPollingLoop,
  startPollingTransport,
  startWebhookTransport,
} from './lib/runtime/telegramTransport.js';
import { buildBuySellText, buildHelpText, buildHomeText } from './lib/ui/liveCopy.js';
const LAMPORTS_PER_SOL = 1_000_000_000;
const BURN_AGENT_WITHDRAW_RESERVE_LAMPORTS = Math.floor(0.01 * LAMPORTS_PER_SOL);
const APPLE_BOOSTER_FEE_RESERVE_LAMPORTS = Math.floor(0.01 * LAMPORTS_PER_SOL);
const FOMO_DEFAULT_WALLET_COUNT = 6;
const FOMO_MIN_WALLET_COUNT = 3;
const FOMO_MAX_WALLET_COUNT = 25;
const FOMO_WORKER_GAS_RESERVE_LAMPORTS = Math.floor(0.003 * LAMPORTS_PER_SOL);
const SNIPER_PRESET_PERCENTS = [25, 50, 75, 100];
const SNIPER_MIN_PERCENT = 1;
const SNIPER_MAX_PERCENT = 100;
const SNIPER_DEFAULT_WALLET_COUNT = 1;
const SNIPER_MIN_WALLET_COUNT = 1;
const SNIPER_MAX_WALLET_COUNT = 20;
const SNIPER_GAS_RESERVE_LAMPORTS = Math.floor(0.01 * LAMPORTS_PER_SOL);
const SNIPER_MAGIC_SETUP_FEE_LAMPORTS = Math.floor(0.1 * LAMPORTS_PER_SOL);
const MAGIC_BUNDLE_DEFAULT_WALLET_COUNT = 5;
const MAGIC_BUNDLE_MIN_WALLET_COUNT = 1;
const MAGIC_BUNDLE_MAX_WALLET_COUNT = 20;
const LAUNCH_BUY_DEFAULT_WALLET_COUNT = 5;
const LAUNCH_BUY_MIN_WALLET_COUNT = 1;
const LAUNCH_BUY_MAX_WALLET_COUNT = 20;
const LAUNCH_BUY_MAGIC_SETUP_FEE_LAMPORTS = Math.floor(0.5 * LAMPORTS_PER_SOL);
const LAUNCH_BUY_NORMAL_SETUP_FEE_LAMPORTS = Math.floor(0.35 * LAMPORTS_PER_SOL);
const LAUNCH_BUY_DEFAULT_JITO_TIP_LAMPORTS = Math.floor(0.01 * LAMPORTS_PER_SOL);
const LAUNCH_BUY_LAUNCH_OVERHEAD_LAMPORTS = Math.floor(0.012 * LAMPORTS_PER_SOL);
const LAUNCH_BUY_BUYER_RESERVE_LAMPORTS = Math.floor(0.006 * LAMPORTS_PER_SOL);
const LAUNCH_BUY_ASSETS_DIR = path.join(DATA_DIR, 'launch-assets');
const MAGIC_SELL_DEFAULT_SELLER_WALLET_COUNT = 12;
const MAGIC_SELL_MIN_BUY_LAMPORTS = Math.floor(0.1 * LAMPORTS_PER_SOL);
const MAGIC_SELL_SELL_PERCENT = 25;
const MAGIC_SELL_WORKER_GAS_RESERVE_LAMPORTS = Math.floor(0.0025 * LAMPORTS_PER_SOL);
const PAYMENT_STATES = {
  NONE: 'none',
  PENDING: 'pending',
  PAID: 'paid',
  EXPIRED: 'expired',
};

const BUTTONS = {
  rocket: { key: 'rocket', label: 'Rocket', emoji: '\u{1F680}' },
  fire: { key: 'fire', label: 'Fire', emoji: '\u{1F525}' },
  poop: { key: 'poop', label: 'Poop', emoji: '\u{1F4A9}' },
  flag: { key: 'flag', label: 'Flag', emoji: '\u{1F6A9}' },
};
const BUNDLE_PRICING = {
  25: { amount: 25, usdPrice: 0.72, pricePerApple: 0.0288, role: 'entry / overpriced' },
  50: { amount: 50, usdPrice: 1.35, pricePerApple: 0.0270, role: 'still meh' },
  100: { amount: 100, usdPrice: 2.49, pricePerApple: 0.0249, role: 'baseline fair' },
  250: { amount: 250, usdPrice: 5.99, pricePerApple: 0.0240, role: 'better deal' },
  500: { amount: 500, usdPrice: 11.49, pricePerApple: 0.0230, role: 'best value' },
  1000: { amount: 1000, usdPrice: 21.99, pricePerApple: 0.0220, role: 'max value' },
};

const ORGANIC_VOLUME_PACKAGES = [
  { key: '3k', label: '3K', priceSol: '0.5', rebateSol: '0.08', treasuryCutSol: '0.05', emoji: '\u{1F9EA}' },
  { key: '5k', label: '5K', priceSol: '0.95', rebateSol: '0.15', treasuryCutSol: '0.20', emoji: '\u{1F331}' },
  { key: '10k', label: '10K', priceSol: '1.8', rebateSol: '0.3', treasuryCutSol: '0.30', emoji: '\u{1F33F}' },
  { key: '20k', label: '20K', priceSol: '3.4', rebateSol: '0.7', treasuryCutSol: '0.40', emoji: '\u{1F4AA}' },
  { key: '30k', label: '30K', priceSol: '5.1', rebateSol: '1.0', treasuryCutSol: '0.60', emoji: '\u{1F4AA}' },
  { key: '50k', label: '50K', priceSol: '8.5', rebateSol: '1.7', treasuryCutSol: '1.00', emoji: '\u{1F680}' },
  { key: '75k', label: '75K', priceSol: '12.5', rebateSol: '2.5', treasuryCutSol: '1.25', emoji: '\u{1F680}' },
  { key: '100k', label: '100K', priceSol: '17.0', rebateSol: '3.4', treasuryCutSol: '2.00', emoji: '\u{1F3C6}' },
  { key: '200k', label: '200K', priceSol: '34.0', rebateSol: '6.9', treasuryCutSol: '4.00', emoji: '\u{1F48E}' },
  { key: '500k', label: '500K', priceSol: '85.0', rebateSol: '17.2', treasuryCutSol: '10.00', emoji: '\u{1F3C5}' },
];
const BUNDLED_VOLUME_PACKAGES = [
  { key: '20k', label: '20K', priceSol: '3.4', rebateSol: '0.7', treasuryCutSol: '0.40', emoji: '\u{1F4BC}' },
  { key: '30k', label: '30K', priceSol: '5.0', rebateSol: '1.0', treasuryCutSol: '0.60', emoji: '\u{1F680}' },
  { key: '50k', label: '50K', priceSol: '8.2', rebateSol: '1.7', treasuryCutSol: '1.00', emoji: '\u{1F680}' },
  { key: '100k', label: '100K', priceSol: '16.2', rebateSol: '3.4', treasuryCutSol: '2.00', emoji: '\u{1F3C6}' },
  { key: '200k', label: '200K', priceSol: '31.5', rebateSol: '6.9', treasuryCutSol: '4.00', emoji: '\u{1F48E}' },
  { key: '500k', label: '500K', priceSol: '74.0', rebateSol: '17.2', treasuryCutSol: '10.00', emoji: '\u{1F3C5}' },
];
const BASE58_ALPHABET = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';
const BASE58_LOWERCASE = new Set(BASE58_ALPHABET.toLowerCase().split(''));
const VANITY_WALLET_SERVICE_FEE_LAMPORTS = parseSolToLamports('0.01');
const VANITY_WALLET_TREASURY_SHARE_LAMPORTS = Math.floor(VANITY_WALLET_SERVICE_FEE_LAMPORTS / 2);
const VANITY_WALLET_BURN_SHARE_LAMPORTS = VANITY_WALLET_SERVICE_FEE_LAMPORTS - VANITY_WALLET_TREASURY_SHARE_LAMPORTS;
const VANITY_WALLET_MAX_PATTERN_LENGTH = 4;
const VANITY_WALLET_BATCH_SIZE = 1500;
const STAKING_MIN_CLAIM_LAMPORTS = parseSolToLamports('0.01');
const STAKING_UNSTAKE_COOLDOWN_DAYS = 7;
const STAKING_EARLY_WEIGHT_DAYS = 7;
const STAKING_REWARDS_VAULT_FEE_RESERVE_LAMPORTS = parseSolToLamports('0.001');
const vanityWalletJobs = new Map();

let paymentPollInFlight = false;
let solUsdRateCache = null;
let solUsdRateCachedAt = 0;
const SOL_PRICE_CACHE_MS = 60_000;

function parseCsv(value, fallback = []) {
  if (!value) return fallback;
  return value
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean);
}

function parsePositiveInts(value, fallback) {
  const parsed = parseCsv(value)
    .map((item) => Number.parseInt(item, 10))
    .filter((item) => Number.isInteger(item) && item > 0);
  return parsed.length > 0 ? parsed : fallback;
}

function parsePositiveInt(value, fallback) {
  const parsed = Number.parseInt(value || '', 10);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function parseOptionalInt(value, fallback = null) {
  if (value === undefined || value === null || String(value).trim() === '') {
    return fallback;
  }

  const parsed = Number.parseInt(String(value).trim(), 10);
  return Number.isInteger(parsed) ? parsed : fallback;
}

function parseSolToLamports(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return 0;
  }

  return Math.round(parsed * LAMPORTS_PER_SOL);
}

function parseSolAmountToLamports(value, label = 'SOL amount') {
  const raw = String(value || '').trim();
  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${label} must be a positive SOL amount.`);
  }

  return Math.round(parsed * LAMPORTS_PER_SOL);
}

function getBundlePricing(amount) {
  return BUNDLE_PRICING[Number(amount)] ?? null;
}

function getOrganicVolumePackage(key) {
  return ORGANIC_VOLUME_PACKAGES.find((item) => item.key === key) ?? null;
}

function getBundledVolumePackage(key) {
  return BUNDLED_VOLUME_PACKAGES.find((item) => item.key === key) ?? null;
}

function getAppleBoosterPackage(strategy, key) {
  return strategy === 'bundled'
    ? getBundledVolumePackage(key)
    : getOrganicVolumePackage(key);
}

function formatSolRange(minSol, maxSol) {
  if (!minSol || !maxSol) {
    return 'Not set';
  }

  return `${minSol} - ${maxSol} SOL`;
}

function formatSecondRange(minSeconds, maxSeconds) {
  if (!Number.isInteger(minSeconds) || !Number.isInteger(maxSeconds)) {
    return 'Not set';
  }

  return `${minSeconds} - ${maxSeconds} sec`;
}

function createAppleBoosterWorkerWallet() {
  const wallet = generateSolanaWallet();
  return {
    address: wallet.address,
    secretKeyB64: wallet.secretKeyB64,
    secretKeyBase58: wallet.secretKeyBase58,
    currentLamports: 0,
    currentSol: '0',
    status: 'idle',
    pendingSellAmount: null,
    nextActionAt: null,
    lastActionAt: null,
    lastBuyInputLamports: null,
    lastBuyOutputAmount: null,
    lastBuySignature: null,
    lastSellInputAmount: null,
    lastSellOutputLamports: null,
    lastSellSignature: null,
    lastError: null,
  };
}

function createAppleBoosterWorkerWallets(count) {
  return Array.from({ length: count }, () => createAppleBoosterWorkerWallet());
}

function formatDurationCompact(totalSeconds) {
  if (!Number.isFinite(totalSeconds) || totalSeconds <= 0) {
    return '0m';
  }

  const seconds = Math.max(0, Math.floor(totalSeconds));
  const days = Math.floor(seconds / 86_400);
  const hours = Math.floor((seconds % 86_400) / 3_600);
  const minutes = Math.floor((seconds % 3_600) / 60);

  if (days > 0) {
    return `${days}d ${hours}h`;
  }

  if (hours > 0) {
    return `${hours}h ${minutes}m`;
  }

  return `${Math.max(1, minutes)}m`;
}

function formatDayCountLabel(days) {
  return `${days} day${days === 1 ? '' : 's'}`;
}

function formatBpsPercent(bps) {
  if (!Number.isInteger(bps)) {
    return '0%';
  }

  return `${(bps / 100).toFixed(2).replace(/0+$/, '').replace(/\.$/, '')}%`;
}

function formatLogTimestamp(timestamp) {
  if (!timestamp) {
    return 'unknown';
  }

  try {
    return new Date(timestamp).toLocaleString('en-US', {
      month: 'short',
      day: 'numeric',
      hour: 'numeric',
      minute: '2-digit',
    });
  } catch {
    return String(timestamp);
  }
}

function getRecentActivityLogs(user, { scopePrefix = null, limit = 5 } = {}) {
  const logs = Array.isArray(user?.activityLogs) ? user.activityLogs : [];
  return logs
    .filter((entry) => !scopePrefix || String(entry.scope || '').startsWith(scopePrefix))
    .slice(0, limit);
}

function formatActivityLogLine(entry) {
  const level = entry?.level === 'error'
    ? 'ERROR'
    : (entry?.level === 'warn' ? 'WARN' : 'INFO');
  return `${formatLogTimestamp(entry?.at)} [${level}] ${entry?.message || 'Activity recorded.'}`;
}

function approximateAppleBoosterRuntime(booster, totalManagedLamports) {
  const effectiveWalletCount = Number.isInteger(booster?.walletCount)
    ? booster.walletCount
    : (booster?.executionMode === 'bundled' ? 1 : null);
  if (
    !Number.isInteger(totalManagedLamports)
    || !Number.isInteger(effectiveWalletCount)
    || !Number.isInteger(booster?.estimatedCycleFeeLamports)
    || booster.estimatedCycleFeeLamports <= 0
  ) {
    return {
      cyclesRemaining: null,
      runtimeSeconds: null,
    };
  }

  const reserveLamports = APPLE_BOOSTER_FEE_RESERVE_LAMPORTS * (effectiveWalletCount + 1);
  const spendableLamports = Math.max(0, totalManagedLamports - reserveLamports);
  const cyclesRemaining = Math.floor(spendableLamports / booster.estimatedCycleFeeLamports);
  const averageIntervalSeconds = Number.isInteger(booster.minIntervalSeconds) && Number.isInteger(booster.maxIntervalSeconds)
    ? Math.round((booster.minIntervalSeconds + booster.maxIntervalSeconds) / 2)
    : null;

  return {
    cyclesRemaining,
    runtimeSeconds: Number.isInteger(averageIntervalSeconds) ? cyclesRemaining * averageIntervalSeconds : null,
  };
}

function organicBoosterIsConfigured(order) {
  if (order?.strategy === 'bundled') {
    return Boolean(
      order?.appleBooster?.mintAddress
      && Number.isInteger(order?.appleBooster?.minSwapLamports)
      && Number.isInteger(order?.appleBooster?.maxSwapLamports)
      && order.appleBooster.minSwapLamports > 0
      && order.appleBooster.maxSwapLamports >= order.appleBooster.minSwapLamports
      && Number.isInteger(order?.appleBooster?.minIntervalSeconds)
      && Number.isInteger(order?.appleBooster?.maxIntervalSeconds)
      && order.appleBooster.minIntervalSeconds > 0
      && order.appleBooster.maxIntervalSeconds >= order.appleBooster.minIntervalSeconds
    );
  }

  return Boolean(
    Number.isInteger(order?.appleBooster?.walletCount)
    && order.appleBooster.walletCount >= 1
    && order.appleBooster.walletCount <= 5
    && Array.isArray(order?.appleBooster?.workerWallets)
    && order.appleBooster.workerWallets.length === order.appleBooster.walletCount
    && order?.appleBooster?.mintAddress
    && Number.isInteger(order?.appleBooster?.minSwapLamports)
    && Number.isInteger(order?.appleBooster?.maxSwapLamports)
    && order.appleBooster.minSwapLamports > 0
    && order.appleBooster.maxSwapLamports >= order.appleBooster.minSwapLamports
    && Number.isInteger(order?.appleBooster?.minIntervalSeconds)
    && Number.isInteger(order?.appleBooster?.maxIntervalSeconds)
    && order.appleBooster.minIntervalSeconds > 0
    && order.appleBooster.maxIntervalSeconds >= order.appleBooster.minIntervalSeconds
  );
}

function organicBoosterCanStart(order) {
  return Boolean(order?.funded && organicBoosterIsConfigured(order));
}

function parseRangePair(input, label, parser) {
  const raw = String(input || '').trim();
  const normalized = raw.replace(/\s+/g, '');
  const parts = normalized.split(/[-,]/).filter(Boolean);
  if (parts.length !== 2) {
    throw new Error(`${label} must be two values like min-max.`);
  }

  const left = parser(parts[0]);
  const right = parser(parts[1]);
  if (left <= 0 || right <= 0) {
    throw new Error(`${label} values must be greater than zero.`);
  }

  return left <= right ? [left, right] : [right, left];
}

function parseOrganicSwapRangeInput(input) {
  const [minSol, maxSol] = parseRangePair(input, 'Swap range', (value) => Number(value));
  if (!Number.isFinite(minSol) || !Number.isFinite(maxSol)) {
    throw new Error('Swap range must use valid SOL amounts like 0.01-0.05.');
  }

  return {
    minSol: minSol.toFixed(3).replace(/0+$/, '').replace(/\.$/, ''),
    maxSol: maxSol.toFixed(3).replace(/0+$/, '').replace(/\.$/, ''),
    minLamports: Math.round(minSol * LAMPORTS_PER_SOL),
    maxLamports: Math.round(maxSol * LAMPORTS_PER_SOL),
  };
}

function parseOrganicIntervalRangeInput(input) {
  const [minSeconds, maxSeconds] = parseRangePair(input, 'Interval range', (value) => Number.parseInt(value, 10));
  if (!Number.isInteger(minSeconds) || !Number.isInteger(maxSeconds)) {
    throw new Error('Interval range must use whole seconds like 30-90.');
  }

  return { minSeconds, maxSeconds };
}

function parseOrganicWalletCountInput(input) {
  const count = Number.parseInt(String(input || '').trim(), 10);
  if (!Number.isInteger(count) || count < 1 || count > 5) {
    throw new Error('Worker wallet count must be a whole number from 1 to 5.');
  }

  return count;
}

function parseMagicSellWalletCountInput(input) {
  const count = Number.parseInt(String(input || '').trim(), 10);
  if (!Number.isInteger(count) || count < 1 || count > 25) {
    throw new Error('Seller wallet count must be a whole number from 1 to 25.');
  }

  return count;
}

function parseMagicSellTargetInput(input) {
  const raw = String(input || '').trim().toLowerCase().replaceAll(',', '');
  const match = raw.match(/^(\d+(?:\.\d+)?)(k|m)?$/);
  if (!match) {
    throw new Error('Target market cap must look like `250000`, `250k`, or `1.2m`.');
  }

  const base = Number(match[1]);
  if (!Number.isFinite(base) || base <= 0) {
    throw new Error('Target market cap must be greater than zero.');
  }

  const multiplier = match[2] === 'm' ? 1_000_000 : (match[2] === 'k' ? 1_000 : 1);
  return Math.round(base * multiplier);
}

function parseMagicSellWhitelistInput(input) {
  const raw = String(input || '').trim();
  if (!raw || raw.toLowerCase() === 'none') {
    return [];
  }

  const items = raw
    .split(/[\s,]+/u)
    .map((item) => item.trim())
    .filter(Boolean);

  return Array.from(new Set(items.map((item) => normalizePublicKey(item, 'Whitelist wallet'))));
}

function parseMagicBundleWalletCountInput(input) {
  const count = Number.parseInt(String(input || '').trim(), 10);
  if (!Number.isInteger(count) || count < MAGIC_BUNDLE_MIN_WALLET_COUNT || count > MAGIC_BUNDLE_MAX_WALLET_COUNT) {
    throw new Error(`Bundle wallet count must be a whole number from ${MAGIC_BUNDLE_MIN_WALLET_COUNT} to ${MAGIC_BUNDLE_MAX_WALLET_COUNT}.`);
  }

  return count;
}

function parseMagicBundlePercentInput(input, label) {
  const parsed = Number(String(input || '').trim().replace('%', ''));
  if (!Number.isFinite(parsed) || parsed < 0 || parsed > 100) {
    throw new Error(`${label} must be a number from 0 to 100.`);
  }

  return Number(parsed.toFixed(2));
}

function parseLaunchBuyWalletCountInput(input) {
  const count = Number.parseInt(String(input || '').trim(), 10);
  if (!Number.isInteger(count) || count < LAUNCH_BUY_MIN_WALLET_COUNT || count > LAUNCH_BUY_MAX_WALLET_COUNT) {
    throw new Error(`Launch + Buy wallet count must be a whole number from ${LAUNCH_BUY_MIN_WALLET_COUNT} to ${LAUNCH_BUY_MAX_WALLET_COUNT}.`);
  }
  return count;
}

function parseLaunchBuyPrivateKeysInput(input, expectedCount = null) {
  const items = String(input || '')
    .split(/\r?\n|,/u)
    .map((item) => item.trim())
    .filter(Boolean);

  if (items.length === 0) {
    throw new Error('Paste one or more private keys, one per line.');
  }

  if (items.length > LAUNCH_BUY_MAX_WALLET_COUNT) {
    throw new Error(`You can import up to ${LAUNCH_BUY_MAX_WALLET_COUNT} buyer wallets at once.`);
  }

  if (Number.isInteger(expectedCount) && items.length !== expectedCount) {
    throw new Error(`You set ${expectedCount} buyer wallets, so send exactly ${expectedCount} private keys.`);
  }

  return items.map((item, index) => {
    const keypair = decodeUserSecretKeyInput(item, `Buyer wallet ${index + 1} private key`);
    return {
      label: `Buyer ${index + 1}`,
      address: keypair.publicKey.toBase58(),
      secretKeyB64: Buffer.from(keypair.secretKey).toString('base64'),
      secretKeyBase58: encodeBase58(Buffer.from(keypair.secretKey)),
      imported: true,
      currentLamports: 0,
      currentSol: '0',
    };
  });
}

function promptForOrganicField(field) {
  switch (field) {
    case 'wallet_count':
      return 'Send how many worker wallets to use for this Organic Volume Booster. Choose a whole number from 1 to 5.';
    case 'mint':
      return 'Send the mint address this booster should trade in and out of.';
    case 'swap_range':
      return 'Send the trade size range in SOL like `0.01-0.05`. Each cycle will randomize between those amounts.';
    case 'interval_range':
      return 'Send the time delay range in seconds like `30-90`. Each wait time will randomize between those values.';
    case 'withdraw_address':
      return 'Send the Solana address where the deposit wallet should withdraw its available SOL.';
    default:
      return 'Send the value in chat.';
  }
}

function promptForMagicSellField(field) {
  switch (field) {
    case 'token_name':
      return 'Send the token name for this Magic Sell.';
    case 'mint':
      return 'Send the token mint / CA Magic Sell should monitor.';
    case 'target_market_cap':
      return 'Send the target market cap in USD like `250k` or `1200000`.';
    case 'whitelist':
      return 'Send whitelist wallets separated by commas, spaces, or new lines. Send `none` to clear them.';
    case 'seller_wallet_count':
      return 'Send how many seller wallets Magic Sell should rotate through. Choose 1 to 25.';
    default:
      return 'Send the requested Magic Sell value in chat.';
  }
}

function promptForMagicBundleField(field) {
  switch (field) {
    case 'token_name':
      return 'Send the token name for this Magic Bundle.';
    case 'mint':
      return 'Send the token mint / CA this Magic Bundle should trade.';
    case 'wallet_count':
      return `Send how many bundle wallets to create. Choose ${MAGIC_BUNDLE_MIN_WALLET_COUNT} to ${MAGIC_BUNDLE_MAX_WALLET_COUNT}.`;
    case 'stop_loss':
      return 'Send the stop loss percent like `10` or `12.5`.';
    case 'take_profit':
      return 'Send the take profit percent like `25` or `40`.';
    case 'trailing_stop_loss':
      return 'Send the trailing stop loss percent like `8` or `12.5`.';
    case 'buy_dip':
      return 'Send the buy-the-dip percent like `15` or `20`.';
    case 'sell_on_dev_sell':
      return 'Send `yes` or `no` for sell on dev sell.';
    default:
      return 'Send the requested Magic Bundle value in chat.';
  }
}

function promptForLaunchBuyField(field, order = null) {
  switch (field) {
    case 'token_name':
      return 'Send the token name exactly how you want it to appear on Pump.';
    case 'symbol':
      return 'Send the token symbol / ticker.';
    case 'description':
      return 'Send the token description.';
    case 'website':
      return 'Send the website URL, or send `none` to clear it.';
    case 'telegram':
      return 'Send the Telegram link, or send `none` to clear it.';
    case 'twitter':
      return 'Send the X / Twitter link, or send `none` to clear it.';
    case 'logo':
      return 'Send the token logo as a photo or image file.';
    case 'wallet_count':
      return `Send how many buyer wallets to use. Choose ${LAUNCH_BUY_MIN_WALLET_COUNT} to ${LAUNCH_BUY_MAX_WALLET_COUNT}.`;
    case 'buyer_keys':
      return `Send ${order?.buyerWalletCount || LAUNCH_BUY_DEFAULT_WALLET_COUNT} private keys, one per line.`;
    case 'total_buy':
      return 'Send the total SOL budget to use across the launch buy bundle like `2.5`.';
    case 'jito_tip':
      return 'Send the Jito tip in SOL like `0.01` or `0.02`.';
    default:
      return 'Send the requested Launch + Buy value in chat.';
  }
}

function promptForBuySellField(field) {
  switch (field) {
    case 'import_wallet':
      return 'Send the wallet private key to import. Base58, base64, or a 64-byte JSON array all work.';
    case 'quick_trade_mint':
      return 'Send the token CA / mint address you want to trade.';
    case 'quick_buy_sol':
      return 'Send the SOL amount to use for each quick buy like `0.25`.';
    case 'quick_sell_percent':
      return 'Send the percent to sell on quick sells like `25`, `50`, or `100`.';
    case 'limit_trigger_market_cap':
      return 'Send the market cap trigger in USD like `250000` or `1500000`.';
    case 'limit_buy_sol':
      return 'Send the SOL amount this limit buy should use like `0.5`.';
    case 'limit_sell_percent':
      return 'Send the percent this limit sell should close like `25`, `50`, or `100`.';
    case 'copy_follow_wallet':
      return 'Send the wallet address you want to copy trade.';
    case 'copy_fixed_buy_sol':
      return 'Send the fixed SOL amount to use whenever a copied wallet buys, like `0.2`.';
    default:
      return 'Send the requested value in chat.';
  }
}

function promptForFomoField(field) {
  switch (field) {
    case 'token_name':
      return 'Send the token name for this FOMO Booster.';
    case 'mint':
      return 'Send the token mint / CA FOMO Booster should trade.';
    case 'wallet_count':
      return `Send how many worker wallets FOMO Booster should rotate through. Choose ${FOMO_MIN_WALLET_COUNT} to ${FOMO_MAX_WALLET_COUNT}.`;
    case 'buy_range':
      return 'Send the micro-bundle buy size range in SOL like `0.01-0.03`. Each buy leg will randomize between those values.';
    case 'interval_range':
      return 'Send the bundle delay range in seconds like `12-30`. Each bundle wait time will randomize between those values.';
    case 'withdraw_address':
      return 'Send the Solana address where the FOMO Booster deposit wallet should withdraw its available SOL.';
    default:
      return 'Send the requested FOMO Booster value in chat.';
  }
}

function parseHolderCountInput(input) {
  const count = Number.parseInt(String(input || '').trim(), 10);
  if (!Number.isInteger(count) || count < 1 || count > 1000) {
    throw new Error('Holder count must be a whole number from 1 to 1000.');
  }

  return count;
}

function promptForHolderField(field) {
  switch (field) {
    case 'mint':
      return 'Send the CA / mint address you want to boost.';
    case 'holder_count':
      return 'Send how many holders you want to create. Choose a whole number from 1 to 1000.';
    default:
      return 'Send the value in chat.';
  }
}

function parseFomoWalletCountInput(input) {
  const count = Number.parseInt(String(input || '').trim(), 10);
  if (!Number.isInteger(count) || count < FOMO_MIN_WALLET_COUNT || count > FOMO_MAX_WALLET_COUNT) {
    throw new Error(`Worker wallet count must be a whole number from ${FOMO_MIN_WALLET_COUNT} to ${FOMO_MAX_WALLET_COUNT}.`);
  }

  return count;
}

function formatTokenAmountFromRaw(rawAmount, decimals = 0) {
  const raw = BigInt(String(rawAmount || '0'));
  const scale = 10n ** BigInt(Math.max(0, decimals));
  const whole = raw / scale;
  const fraction = raw % scale;
  if (fraction === 0n) {
    return whole.toString();
  }

  const padded = fraction.toString().padStart(Math.max(1, decimals), '0').replace(/0+$/, '');
  return `${whole.toString()}.${padded}`;
}

function formatOrganicWorkerLabel(worker, index) {
  const shortAddress = worker.address
    ? `${worker.address.slice(0, 4)}...${worker.address.slice(-4)}`
    : `wallet-${index + 1}`;
  return `#${index + 1} ${shortAddress}: *${worker.currentSol || '0'} SOL* (${worker.status || 'idle'})`;
}

function base58Encode(buffer) {
  if (!buffer || buffer.length === 0) {
    return '';
  }

  const digits = [0];
  for (const byte of buffer) {
    let carry = byte;
    for (let index = 0; index < digits.length; index += 1) {
      carry += digits[index] << 8;
      digits[index] = carry % 58;
      carry = (carry / 58) | 0;
    }

    while (carry > 0) {
      digits.push(carry % 58);
      carry = (carry / 58) | 0;
    }
  }

  let encoded = '';
  for (const byte of buffer) {
    if (byte !== 0) break;
    encoded += BASE58_ALPHABET[0];
  }

  for (let index = digits.length - 1; index >= 0; index -= 1) {
    encoded += BASE58_ALPHABET[digits[index]];
  }

  return encoded;
}

function generateSolanaWallet() {
  const { publicKey, privateKey } = generateKeyPairSync('ed25519');
  const publicJwk = publicKey.export({ format: 'jwk' });
  const privateJwk = privateKey.export({ format: 'jwk' });
  const publicBytes = Buffer.from(publicJwk.x, 'base64url');
  const privateBytes = Buffer.from(privateJwk.d, 'base64url');
  const secretKeyBytes = Buffer.concat([privateBytes, publicBytes]);

  return {
    address: base58Encode(publicBytes),
    secretKeyB64: secretKeyBytes.toString('base64'),
    secretKeyBase58: base58Encode(secretKeyBytes),
  };
}

function base58Decode(value) {
  const input = String(value || '').trim();
  if (!input) {
    return Buffer.alloc(0);
  }

  const bytes = [0];
  for (const character of input) {
    const digit = BASE58_ALPHABET.indexOf(character);
    if (digit < 0) {
      throw new Error('Private key contains invalid base58 characters.');
    }

    let carry = digit;
    for (let index = 0; index < bytes.length; index += 1) {
      carry += bytes[index] * 58;
      bytes[index] = carry & 0xff;
      carry >>= 8;
    }

    while (carry > 0) {
      bytes.push(carry & 0xff);
      carry >>= 8;
    }
  }

  let leadingZeroes = 0;
  for (const character of input) {
    if (character !== BASE58_ALPHABET[0]) break;
    leadingZeroes += 1;
  }

  const decoded = Buffer.from(bytes.reverse());
  return leadingZeroes > 0
    ? Buffer.concat([Buffer.alloc(leadingZeroes), decoded])
    : decoded;
}

function normalizePublicKey(value, label = 'Address') {
  try {
    return new PublicKey(String(value || '').trim()).toBase58();
  } catch {
    throw new Error(`${label} is not a valid Solana address.`);
  }
}

function parseSecretKeyBytes(input) {
  const trimmed = String(input || '').trim();
  if (!trimmed) {
    throw new Error('Private key cannot be empty.');
  }

  if (trimmed.startsWith('[')) {
    const parsed = JSON.parse(trimmed);
    if (!Array.isArray(parsed) || parsed.length !== 64 || parsed.some((item) => !Number.isInteger(item))) {
      throw new Error('Private key JSON must be a 64-byte array.');
    }
    return Buffer.from(parsed);
  }

  try {
    const decodedBase58 = base58Decode(trimmed);
    if (decodedBase58.length === 64) {
      return decodedBase58;
    }
  } catch {
    // Fall through to base64 parsing.
  }

  try {
    const decodedBase64 = Buffer.from(trimmed, 'base64');
    if (decodedBase64.length === 64) {
      return decodedBase64;
    }
  } catch {
    // Fall through to the final error below.
  }

  throw new Error('Private key must be base58, base64, or a 64-byte JSON array.');
}

function parseWalletFromSecretInput(input) {
  const secretKeyBytes = parseSecretKeyBytes(input);
  const signer = Keypair.fromSecretKey(Uint8Array.from(secretKeyBytes));
  return {
    address: signer.publicKey.toBase58(),
    secretKeyB64: Buffer.from(secretKeyBytes).toString('base64'),
    secretKeyBase58: base58Encode(secretKeyBytes),
  };
}

function createDefaultAppleBoosterState() {
  return {
    walletCount: null,
    workerWallets: [],
    totalManagedLamports: null,
    mintAddress: null,
    minSwapSol: null,
    maxSwapSol: null,
    minSwapLamports: null,
    maxSwapLamports: null,
    minIntervalSeconds: null,
    maxIntervalSeconds: null,
    awaitingField: null,
    status: 'idle',
    nextActionAt: null,
    lastActionAt: null,
    lastBuyInputLamports: null,
    lastBuyOutputAmount: null,
    lastBuySignature: null,
    lastSellInputAmount: null,
    lastSellOutputLamports: null,
    lastSellSignature: null,
    pendingSellAmount: null,
    cycleCount: 0,
    marketPhase: null,
    marketCapLamports: null,
    marketCapSol: null,
    lpFeeBps: null,
    protocolFeeBps: null,
    creatorFeeBps: null,
    estimatedCycleFeeLamports: null,
    estimatedTradeFeeLamports: null,
    estimatedNetworkFeeLamports: null,
    estimatedCyclesRemaining: null,
    estimatedRuntimeSeconds: null,
    totalBuyCount: 0,
    totalSellCount: 0,
    totalTopUpLamports: 0,
    totalSweptLamports: 0,
    totalBuyInputLamports: 0,
    totalSellOutputLamports: 0,
    lastMarketCheckedAt: null,
    stopRequested: false,
    lastError: null,
  };
}

function createDefaultOrganicVolumeOrder() {
  return {
    id: createAppleBoosterId(),
    strategy: 'organic',
    freeTrial: false,
    trialTradeGoal: null,
    packageKey: null,
    walletAddress: null,
    walletSecretKeyB64: null,
    requiredSol: null,
    rebateSol: null,
    treasuryCutSol: null,
    treasuryShareSol: null,
    devShareSol: null,
    usableSol: null,
    treasuryWalletAddress: null,
    devWalletAddress: null,
    requiredLamports: null,
    currentLamports: 0,
    currentSol: '0',
    funded: false,
    running: false,
    awaitingField: null,
    createdAt: null,
    fundedAt: null,
    lastBalanceCheckAt: null,
    lastError: null,
    deleteConfirmations: 0,
    archivedAt: null,
    appleBooster: createDefaultAppleBoosterState(),
  };
}

function createAppleBoosterId() {
  return `ab_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

function createHolderBoosterId() {
  return `hb_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

function createMagicSellId() {
  return `ms_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

function createFomoBoosterId() {
  return `fb_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

function createSniperWizardId() {
  return `sw_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

function createMagicBundleId() {
  return `mb_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

function createVanityWalletId() {
  return `vw_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

function createTradingWalletId() {
  return `tw_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

function createMagicBundleWorkerWallet() {
  const wallet = generateSolanaWallet();
  return {
    address: wallet.address,
    secretKeyB64: wallet.secretKeyB64,
    secretKeyBase58: wallet.secretKeyBase58,
    currentLamports: 0,
    currentSol: '0',
    currentTokenAmountRaw: '0',
    currentTokenAmountDisplay: '0',
    currentPositionValueLamports: 0,
    costBasisLamports: null,
    highestValueLamports: null,
    buyDipCount: 0,
    lastActionAt: null,
    lastBuySignature: null,
    lastSellSignature: null,
    lastTriggerReason: null,
    status: 'idle',
    lastError: null,
  };
}

function createMagicBundleWorkerWallets(count) {
  return Array.from({ length: count }, () => createMagicBundleWorkerWallet());
}

function createDefaultHolderBooster() {
  return {
    id: createHolderBoosterId(),
    mintAddress: null,
    holderCount: null,
    walletAddress: null,
    walletSecretKeyB64: null,
    walletSecretKeyBase58: null,
    tokenDecimals: null,
    tokenProgram: null,
    requiredSol: null,
    requiredLamports: null,
    requiredTokenAmountRaw: null,
    currentLamports: 0,
    currentSol: '0',
    currentTokenAmountRaw: '0',
    currentTokenAmountDisplay: '0',
    processedWalletCount: 0,
    childWallets: [],
    awaitingField: null,
    status: 'idle',
    createdAt: null,
    fundedAt: null,
    completedAt: null,
    lastBalanceCheckAt: null,
    lastError: null,
    treasurySignature: null,
    devSignature: null,
  };
}

function createDefaultMagicSell() {
  const wallet = generateSolanaWallet();
  return {
    id: createMagicSellId(),
    tokenName: null,
    mintAddress: null,
    targetMarketCapUsd: null,
    whitelistWallets: [],
    walletAddress: wallet.address,
    walletSecretKeyB64: wallet.secretKeyB64,
    walletSecretKeyBase58: wallet.secretKeyBase58,
    privateKeyVisible: false,
    tokenDecimals: null,
    tokenProgram: null,
    sellerWalletCount: MAGIC_SELL_DEFAULT_SELLER_WALLET_COUNT,
    sellerWallets: createMagicSellSellerWallets(MAGIC_SELL_DEFAULT_SELLER_WALLET_COUNT),
    currentLamports: 0,
    currentSol: '0',
    currentTokenAmountRaw: '0',
    currentTokenAmountDisplay: '0',
    totalManagedLamports: 0,
    currentMarketCapUsd: null,
    currentMarketCapSol: null,
    marketPhase: null,
    sellPercent: MAGIC_SELL_SELL_PERCENT,
    minimumBuyLamports: MAGIC_SELL_MIN_BUY_LAMPORTS,
    recommendedGasLamports: MAGIC_SELL_WORKER_GAS_RESERVE_LAMPORTS * MAGIC_SELL_DEFAULT_SELLER_WALLET_COUNT,
    awaitingField: null,
    automationEnabled: false,
    status: 'setup',
    stats: {},
    lastSeenSignature: null,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    lastBalanceCheckAt: null,
    lastError: null,
    deleteConfirmations: 0,
    archivedAt: null,
  };
}

function createFomoWorkerWallet() {
  const wallet = generateSolanaWallet();
  return {
    address: wallet.address,
    secretKeyB64: wallet.secretKeyB64,
    secretKeyBase58: wallet.secretKeyBase58,
    currentLamports: 0,
    currentSol: '0',
    currentTokenAmountRaw: '0',
    currentTokenAmountDisplay: '0',
    buyCount: 0,
    sellCount: 0,
    lastUsedAt: null,
    status: 'idle',
    lastBuySignature: null,
    lastSellSignature: null,
    lastError: null,
  };
}

function createFomoWorkerWallets(count) {
  return Array.from({ length: count }, () => createFomoWorkerWallet());
}

function createDefaultFomoBooster() {
  const wallet = generateSolanaWallet();
  return {
    id: createFomoBoosterId(),
    tokenName: null,
    mintAddress: null,
    walletAddress: wallet.address,
    walletSecretKeyB64: wallet.secretKeyB64,
    walletSecretKeyBase58: wallet.secretKeyBase58,
    privateKeyVisible: false,
    tokenDecimals: null,
    tokenProgram: null,
    walletCount: FOMO_DEFAULT_WALLET_COUNT,
    workerWallets: createFomoWorkerWallets(FOMO_DEFAULT_WALLET_COUNT),
    minBuySol: null,
    maxBuySol: null,
    minBuyLamports: null,
    maxBuyLamports: null,
    minIntervalSeconds: null,
    maxIntervalSeconds: null,
    currentLamports: 0,
    currentSol: '0',
    totalManagedLamports: 0,
    currentTokenAmountRaw: '0',
    currentTokenAmountDisplay: '0',
    currentMarketCapUsd: null,
    currentMarketCapSol: null,
    marketPhase: null,
    recommendedGasLamports: FOMO_WORKER_GAS_RESERVE_LAMPORTS * FOMO_DEFAULT_WALLET_COUNT,
    awaitingField: null,
    automationEnabled: false,
    status: 'setup',
    stats: {},
    lastBundleId: null,
    lastBundleAt: null,
    lastBalanceCheckAt: null,
    lastError: null,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  };
}

function createDefaultSniperWizard() {
  const wallet = generateSolanaWallet();
  return {
    id: createSniperWizardId(),
    sniperMode: null,
    targetWalletAddress: null,
    walletAddress: wallet.address,
    walletSecretKeyB64: wallet.secretKeyB64,
    walletSecretKeyBase58: wallet.secretKeyBase58,
    walletCount: SNIPER_DEFAULT_WALLET_COUNT,
    workerWallets: createLaunchBuyBuyerWallets(SNIPER_DEFAULT_WALLET_COUNT),
    privateKeyVisible: false,
    snipePercent: 50,
    currentLamports: 0,
    currentSol: '0',
    totalManagedLamports: 0,
    estimatedPlatformFeeLamports: 0,
    estimatedSplitNowFeeLamports: 0,
    estimatedNetSplitLamports: 0,
    routingOrderId: null,
    routingQuoteId: null,
    routingDepositAddress: null,
    routingStatus: null,
    routingCompletedAt: null,
    awaitingField: null,
    automationEnabled: false,
    status: 'setup',
    stats: {},
    lastDetectedLaunchSignature: null,
    lastDetectedMintAddress: null,
    lastSnipeSignature: null,
    lastBalanceCheckAt: null,
    lastError: null,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  };
}

function createCommunityVisionId() {
  return `cv_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

function createDefaultCommunityVision() {
  return {
    id: createCommunityVisionId(),
    profileUrl: null,
    handle: null,
    trackedCommunities: [],
    awaitingField: null,
    automationEnabled: false,
    status: 'setup',
    stats: {},
    lastCheckedAt: null,
    lastAlertAt: null,
    lastChangeAt: null,
    lastError: null,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    deleteConfirmations: 0,
    archivedAt: null,
  };
}

function createWalletTrackerId() {
  return `wt_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

function createDefaultWalletTracker() {
  return {
    id: createWalletTrackerId(),
    walletAddress: null,
    buyMode: 'first',
    notifySells: true,
    notifyLaunches: true,
    awaitingField: null,
    automationEnabled: false,
    status: 'setup',
    stats: {},
    notifiedBuyMints: [],
    lastSeenSignature: null,
    lastCheckedAt: null,
    lastAlertAt: null,
    lastEventAt: null,
    lastError: null,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    deleteConfirmations: 0,
    archivedAt: null,
  };
}

function createLaunchBuyId() {
  return `lb_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

function createLaunchBuyBuyerWallet() {
  const wallet = generateSolanaWallet();
  return {
    label: `Buyer ${Math.random().toString(36).slice(2, 6).toUpperCase()}`,
    address: wallet.address,
    secretKeyB64: wallet.secretKeyB64,
    secretKeyBase58: wallet.secretKeyBase58,
    imported: false,
    currentLamports: 0,
    currentSol: '0',
  };
}

function createLaunchBuyBuyerWallets(count) {
  return Array.from({ length: count }, () => createLaunchBuyBuyerWallet());
}

function createDefaultLaunchBuy() {
  const wallet = generateSolanaWallet();
  return {
    id: createLaunchBuyId(),
    launchMode: 'normal',
    tokenName: null,
    symbol: null,
    description: null,
    website: null,
    telegram: null,
    twitter: null,
    logoPath: null,
    logoFileName: null,
    logoUploadedAt: null,
    walletSource: 'generated',
    buyerWalletCount: LAUNCH_BUY_DEFAULT_WALLET_COUNT,
    buyerWallets: createLaunchBuyBuyerWallets(LAUNCH_BUY_DEFAULT_WALLET_COUNT),
    walletAddress: wallet.address,
    walletSecretKeyB64: wallet.secretKeyB64,
    walletSecretKeyBase58: wallet.secretKeyBase58,
    privateKeyVisible: false,
    currentLamports: 0,
    currentSol: '0',
    totalBuyLamports: null,
    totalBuySol: null,
    jitoTipLamports: LAUNCH_BUY_DEFAULT_JITO_TIP_LAMPORTS,
    jitoTipSol: formatSolAmountFromLamports(LAUNCH_BUY_DEFAULT_JITO_TIP_LAMPORTS),
    estimatedSetupFeeLamports: LAUNCH_BUY_NORMAL_SETUP_FEE_LAMPORTS,
    estimatedRoutingFeeLamports: 0,
    estimatedTotalNeededLamports: LAUNCH_BUY_NORMAL_SETUP_FEE_LAMPORTS
      + LAUNCH_BUY_DEFAULT_JITO_TIP_LAMPORTS
      + LAUNCH_BUY_LAUNCH_OVERHEAD_LAMPORTS,
    fundedReady: false,
    awaitingField: null,
    status: 'setup',
    lastError: null,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    deleteConfirmations: 0,
    archivedAt: null,
  };
}

function createDefaultMagicBundle() {
  const wallet = generateSolanaWallet();
  return {
    id: createMagicBundleId(),
    bundleMode: 'stealth',
    tokenName: null,
    mintAddress: null,
    tokenDecimals: null,
    tokenProgram: null,
    walletCount: MAGIC_BUNDLE_DEFAULT_WALLET_COUNT,
    splitWallets: createMagicBundleWorkerWallets(MAGIC_BUNDLE_DEFAULT_WALLET_COUNT),
    walletAddress: wallet.address,
    walletSecretKeyB64: wallet.secretKeyB64,
    walletSecretKeyBase58: wallet.secretKeyBase58,
    currentLamports: 0,
    currentSol: '0',
    totalManagedLamports: 0,
    currentTokenAmountRaw: '0',
    currentTokenAmountDisplay: '0',
    currentPositionValueLamports: 0,
    creatorAddress: null,
    stopLossPercent: null,
    takeProfitPercent: null,
    trailingStopLossPercent: null,
    buyDipPercent: null,
    sellOnDevSell: false,
    automationEnabled: false,
    platformFeeBps: 0,
    splitNowFeeEstimateBps: 100,
    estimatedPlatformFeeLamports: 0,
    estimatedSplitNowFeeLamports: 0,
    estimatedNetSplitLamports: 0,
    splitnowOrderId: null,
    splitnowQuoteId: null,
    splitnowDepositAddress: null,
    splitnowDepositAmountSol: null,
    splitnowStatus: null,
    splitCompletedAt: null,
    stats: {},
    lastCreatorSeenSignature: null,
    lastActionAt: null,
    lastTriggerReason: null,
    lastBalanceCheckAt: null,
    lastError: null,
    awaitingField: null,
    status: 'setup',
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    deleteConfirmations: 0,
    archivedAt: null,
  };
}

function createTradingWallet(imported = false) {
  const wallet = generateSolanaWallet();
  return {
    id: createTradingWalletId(),
    label: imported ? 'Imported Wallet' : 'Generated Wallet',
    address: wallet.address,
    secretKeyB64: wallet.secretKeyB64,
    secretKeyBase58: wallet.secretKeyBase58,
    currentLamports: 0,
    currentSol: '0',
    imported,
    privateKeyVisible: false,
    createdAt: new Date().toISOString(),
  };
}

function createDefaultTradingDesk() {
  return {
    wallets: [],
    activeWalletId: null,
    quickTradeMintAddress: null,
    selectedMagicBundleId: null,
    quickBuyLamports: null,
    quickBuySol: null,
    quickSellPercent: 100,
    pendingAction: null,
    lastTradeSignature: null,
    lastTradeSide: null,
    lastTradeAt: null,
    handlingFeeBps: cfg.tradingHandlingFeeBps,
    limitOrder: {
      side: 'buy',
      triggerMarketCapUsd: null,
      buyLamports: null,
      buySol: null,
      sellPercent: 100,
      enabled: false,
      lastTriggeredAt: null,
      lastTriggerSignature: null,
      lastError: null,
    },
    copyTrade: {
      followWalletAddress: null,
      fixedBuyLamports: null,
      fixedBuySol: null,
      copySells: true,
      enabled: false,
      lastSeenSignature: null,
      lastCopiedAt: null,
      lastError: null,
      stats: {
        buyCount: 0,
        sellCount: 0,
      },
    },
    awaitingField: null,
    status: 'idle',
    lastBalanceCheckAt: null,
    lastError: null,
  };
}

function createDefaultVanityWalletState() {
  return {
    id: createVanityWalletId(),
    patternMode: null,
    pattern: null,
    awaitingField: null,
    status: 'setup',
    payment: createDefaultPaymentState(),
    generatedAddress: null,
    generatedSecretKeyB64: null,
    generatedSecretKeyBase58: null,
    privateKeyVisible: false,
    attemptCount: 0,
    generationStartedAt: null,
    completedAt: null,
    estimatedTreasuryShareLamports: parseSolToLamports('0.005'),
    estimatedBurnShareLamports: parseSolToLamports('0.005'),
    lastError: null,
  };
}

function createDefaultResizer() {
  return {
    mode: null,
    awaitingImage: false,
    status: 'setup',
    lastCompletedAt: null,
    lastOutputWidth: null,
    lastOutputHeight: null,
    lastSourceName: null,
    lastError: null,
  };
}

function createBurnAgentId() {
  return `ba_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

function createHolderRecipientWallet() {
  const wallet = generateSolanaWallet();
  return {
    address: wallet.address,
    secretKeyB64: wallet.secretKeyB64,
    status: 'pending',
  };
}

function createHolderRecipientWallets(count) {
  return Array.from({ length: count }, () => createHolderRecipientWallet());
}

function createMagicSellSellerWallet() {
  const wallet = generateSolanaWallet();
  return {
    address: wallet.address,
    secretKeyB64: wallet.secretKeyB64,
    secretKeyBase58: wallet.secretKeyBase58,
    currentLamports: 0,
    currentSol: '0',
    currentTokenAmountRaw: '0',
    currentTokenAmountDisplay: '0',
    sellCount: 0,
    lastUsedAt: null,
    status: 'idle',
    lastSellSignature: null,
    lastError: null,
  };
}

function createMagicSellSellerWallets(count) {
  return Array.from({ length: count }, () => createMagicSellSellerWallet());
}

function normalizeHolderBooster(order = {}) {
  return {
    ...createDefaultHolderBooster(),
    ...(order ?? {}),
    id: typeof order.id === 'string' && order.id ? order.id : createHolderBoosterId(),
    mintAddress: typeof order.mintAddress === 'string' ? order.mintAddress : null,
    holderCount: Number.isInteger(order.holderCount) ? order.holderCount : null,
    walletAddress: typeof order.walletAddress === 'string' ? order.walletAddress : null,
    walletSecretKeyB64: typeof order.walletSecretKeyB64 === 'string' ? order.walletSecretKeyB64 : null,
    walletSecretKeyBase58: typeof order.walletSecretKeyBase58 === 'string' ? order.walletSecretKeyBase58 : null,
    tokenDecimals: Number.isInteger(order.tokenDecimals) ? order.tokenDecimals : null,
    tokenProgram: typeof order.tokenProgram === 'string' ? order.tokenProgram : null,
    requiredSol: typeof order.requiredSol === 'string' ? order.requiredSol : null,
    requiredLamports: Number.isInteger(order.requiredLamports) ? order.requiredLamports : null,
    requiredTokenAmountRaw: typeof order.requiredTokenAmountRaw === 'string' ? order.requiredTokenAmountRaw : null,
    currentLamports: Number.isInteger(order.currentLamports) ? order.currentLamports : 0,
    currentSol: typeof order.currentSol === 'string' ? order.currentSol : '0',
    currentTokenAmountRaw: typeof order.currentTokenAmountRaw === 'string' ? order.currentTokenAmountRaw : '0',
    currentTokenAmountDisplay: typeof order.currentTokenAmountDisplay === 'string'
      ? order.currentTokenAmountDisplay
      : '0',
    processedWalletCount: Number.isInteger(order.processedWalletCount) ? order.processedWalletCount : 0,
    childWallets: Array.isArray(order.childWallets)
      ? order.childWallets.map((wallet) => ({
        address: typeof wallet?.address === 'string' ? wallet.address : null,
        secretKeyB64: typeof wallet?.secretKeyB64 === 'string' ? wallet.secretKeyB64 : null,
        status: typeof wallet?.status === 'string' ? wallet.status : 'pending',
      }))
      : [],
    awaitingField: typeof order.awaitingField === 'string' ? order.awaitingField : null,
    status: typeof order.status === 'string' ? order.status : 'idle',
    createdAt: typeof order.createdAt === 'string' ? order.createdAt : null,
    fundedAt: typeof order.fundedAt === 'string' ? order.fundedAt : null,
    completedAt: typeof order.completedAt === 'string' ? order.completedAt : null,
    lastBalanceCheckAt: typeof order.lastBalanceCheckAt === 'string' ? order.lastBalanceCheckAt : null,
    lastError: typeof order.lastError === 'string' ? order.lastError : null,
    treasurySignature: typeof order.treasurySignature === 'string' ? order.treasurySignature : null,
    devSignature: typeof order.devSignature === 'string' ? order.devSignature : null,
  };
}

function normalizeMagicSellSellerWallet(wallet = {}) {
  return {
    ...createMagicSellSellerWallet(),
    ...(wallet ?? {}),
    address: typeof wallet.address === 'string' ? wallet.address : null,
    secretKeyB64: typeof wallet.secretKeyB64 === 'string' ? wallet.secretKeyB64 : null,
    secretKeyBase58: typeof wallet.secretKeyBase58 === 'string' ? wallet.secretKeyBase58 : null,
    currentLamports: Number.isInteger(wallet.currentLamports) ? wallet.currentLamports : 0,
    currentSol: typeof wallet.currentSol === 'string' ? wallet.currentSol : '0',
    currentTokenAmountRaw: typeof wallet.currentTokenAmountRaw === 'string' ? wallet.currentTokenAmountRaw : '0',
    currentTokenAmountDisplay: typeof wallet.currentTokenAmountDisplay === 'string'
      ? wallet.currentTokenAmountDisplay
      : '0',
    sellCount: Number.isInteger(wallet.sellCount) ? wallet.sellCount : 0,
    lastUsedAt: typeof wallet.lastUsedAt === 'string' ? wallet.lastUsedAt : null,
    status: typeof wallet.status === 'string' ? wallet.status : 'idle',
    lastSellSignature: typeof wallet.lastSellSignature === 'string' ? wallet.lastSellSignature : null,
    lastError: typeof wallet.lastError === 'string' ? wallet.lastError : null,
  };
}

function normalizeMagicSell(order = {}) {
  const defaultState = createDefaultMagicSell();
  return {
    ...defaultState,
    ...(order ?? {}),
    id: typeof order.id === 'string' && order.id ? order.id : defaultState.id,
    tokenName: typeof order.tokenName === 'string' ? order.tokenName : null,
    mintAddress: typeof order.mintAddress === 'string' ? order.mintAddress : null,
    targetMarketCapUsd: Number.isFinite(order.targetMarketCapUsd) ? Number(order.targetMarketCapUsd) : null,
    whitelistWallets: Array.isArray(order.whitelistWallets)
      ? order.whitelistWallets.filter((item) => typeof item === 'string' && item.trim()).map((item) => item.trim())
      : [],
    walletAddress: typeof order.walletAddress === 'string' ? order.walletAddress : defaultState.walletAddress,
    walletSecretKeyB64: typeof order.walletSecretKeyB64 === 'string'
      ? order.walletSecretKeyB64
      : defaultState.walletSecretKeyB64,
    walletSecretKeyBase58: typeof order.walletSecretKeyBase58 === 'string'
      ? order.walletSecretKeyBase58
      : defaultState.walletSecretKeyBase58,
    privateKeyVisible: Boolean(order.privateKeyVisible),
    tokenDecimals: Number.isInteger(order.tokenDecimals) ? order.tokenDecimals : null,
    tokenProgram: typeof order.tokenProgram === 'string' ? order.tokenProgram : null,
    sellerWalletCount: Number.isInteger(order.sellerWalletCount) ? order.sellerWalletCount : MAGIC_SELL_DEFAULT_SELLER_WALLET_COUNT,
    sellerWallets: Array.isArray(order.sellerWallets)
      ? order.sellerWallets.map((wallet) => normalizeMagicSellSellerWallet(wallet))
      : createMagicSellSellerWallets(Number.isInteger(order.sellerWalletCount) ? order.sellerWalletCount : MAGIC_SELL_DEFAULT_SELLER_WALLET_COUNT),
    currentLamports: Number.isInteger(order.currentLamports) ? order.currentLamports : 0,
    currentSol: typeof order.currentSol === 'string' ? order.currentSol : '0',
    currentTokenAmountRaw: typeof order.currentTokenAmountRaw === 'string' ? order.currentTokenAmountRaw : '0',
    currentTokenAmountDisplay: typeof order.currentTokenAmountDisplay === 'string'
      ? order.currentTokenAmountDisplay
      : '0',
    totalManagedLamports: Number.isInteger(order.totalManagedLamports) ? order.totalManagedLamports : 0,
    currentMarketCapUsd: Number.isFinite(order.currentMarketCapUsd) ? Number(order.currentMarketCapUsd) : null,
    currentMarketCapSol: typeof order.currentMarketCapSol === 'string' ? order.currentMarketCapSol : null,
    marketPhase: typeof order.marketPhase === 'string' ? order.marketPhase : null,
    sellPercent: Number.isInteger(order.sellPercent) ? order.sellPercent : MAGIC_SELL_SELL_PERCENT,
    minimumBuyLamports: Number.isInteger(order.minimumBuyLamports) ? order.minimumBuyLamports : MAGIC_SELL_MIN_BUY_LAMPORTS,
    recommendedGasLamports: Number.isInteger(order.recommendedGasLamports)
      ? order.recommendedGasLamports
      : MAGIC_SELL_WORKER_GAS_RESERVE_LAMPORTS * (Number.isInteger(order.sellerWalletCount) ? order.sellerWalletCount : MAGIC_SELL_DEFAULT_SELLER_WALLET_COUNT),
    awaitingField: typeof order.awaitingField === 'string' ? order.awaitingField : null,
    automationEnabled: Boolean(order.automationEnabled),
    status: typeof order.status === 'string' ? order.status : 'setup',
    stats: order.stats && typeof order.stats === 'object' ? order.stats : {},
    lastSeenSignature: typeof order.lastSeenSignature === 'string' ? order.lastSeenSignature : null,
    createdAt: typeof order.createdAt === 'string' ? order.createdAt : defaultState.createdAt,
    updatedAt: typeof order.updatedAt === 'string' ? order.updatedAt : null,
    lastBalanceCheckAt: typeof order.lastBalanceCheckAt === 'string' ? order.lastBalanceCheckAt : null,
    lastError: typeof order.lastError === 'string' ? order.lastError : null,
    deleteConfirmations: Number.isInteger(order.deleteConfirmations) ? order.deleteConfirmations : 0,
    archivedAt: typeof order.archivedAt === 'string' ? order.archivedAt : null,
  };
}

function normalizeFomoWorkerWallet(wallet = {}) {
  return {
    ...createFomoWorkerWallet(),
    ...(wallet ?? {}),
    address: typeof wallet.address === 'string' ? wallet.address : null,
    secretKeyB64: typeof wallet.secretKeyB64 === 'string' ? wallet.secretKeyB64 : null,
    secretKeyBase58: typeof wallet.secretKeyBase58 === 'string' ? wallet.secretKeyBase58 : null,
    currentLamports: Number.isInteger(wallet.currentLamports) ? wallet.currentLamports : 0,
    currentSol: typeof wallet.currentSol === 'string' ? wallet.currentSol : '0',
    currentTokenAmountRaw: typeof wallet.currentTokenAmountRaw === 'string' ? wallet.currentTokenAmountRaw : '0',
    currentTokenAmountDisplay: typeof wallet.currentTokenAmountDisplay === 'string'
      ? wallet.currentTokenAmountDisplay
      : '0',
    buyCount: Number.isInteger(wallet.buyCount) ? wallet.buyCount : 0,
    sellCount: Number.isInteger(wallet.sellCount) ? wallet.sellCount : 0,
    lastUsedAt: typeof wallet.lastUsedAt === 'string' ? wallet.lastUsedAt : null,
    status: typeof wallet.status === 'string' ? wallet.status : 'idle',
    lastBuySignature: typeof wallet.lastBuySignature === 'string' ? wallet.lastBuySignature : null,
    lastSellSignature: typeof wallet.lastSellSignature === 'string' ? wallet.lastSellSignature : null,
    lastError: typeof wallet.lastError === 'string' ? wallet.lastError : null,
  };
}

function normalizeFomoBooster(order = {}) {
  const defaultState = createDefaultFomoBooster();
  return {
    ...defaultState,
    ...(order ?? {}),
    id: typeof order.id === 'string' && order.id ? order.id : defaultState.id,
    tokenName: typeof order.tokenName === 'string' ? order.tokenName : null,
    mintAddress: typeof order.mintAddress === 'string' ? order.mintAddress : null,
    walletAddress: typeof order.walletAddress === 'string' ? order.walletAddress : defaultState.walletAddress,
    walletSecretKeyB64: typeof order.walletSecretKeyB64 === 'string' ? order.walletSecretKeyB64 : defaultState.walletSecretKeyB64,
    walletSecretKeyBase58: typeof order.walletSecretKeyBase58 === 'string' ? order.walletSecretKeyBase58 : defaultState.walletSecretKeyBase58,
    privateKeyVisible: Boolean(order.privateKeyVisible),
    tokenDecimals: Number.isInteger(order.tokenDecimals) ? order.tokenDecimals : null,
    tokenProgram: typeof order.tokenProgram === 'string' ? order.tokenProgram : null,
    walletCount: Number.isInteger(order.walletCount) ? order.walletCount : FOMO_DEFAULT_WALLET_COUNT,
    workerWallets: Array.isArray(order.workerWallets)
      ? order.workerWallets.map((wallet) => normalizeFomoWorkerWallet(wallet))
      : createFomoWorkerWallets(Number.isInteger(order.walletCount) ? order.walletCount : FOMO_DEFAULT_WALLET_COUNT),
    minBuySol: typeof order.minBuySol === 'string' ? order.minBuySol : null,
    maxBuySol: typeof order.maxBuySol === 'string' ? order.maxBuySol : null,
    minBuyLamports: Number.isInteger(order.minBuyLamports) ? order.minBuyLamports : null,
    maxBuyLamports: Number.isInteger(order.maxBuyLamports) ? order.maxBuyLamports : null,
    minIntervalSeconds: Number.isInteger(order.minIntervalSeconds) ? order.minIntervalSeconds : null,
    maxIntervalSeconds: Number.isInteger(order.maxIntervalSeconds) ? order.maxIntervalSeconds : null,
    currentLamports: Number.isInteger(order.currentLamports) ? order.currentLamports : 0,
    currentSol: typeof order.currentSol === 'string' ? order.currentSol : '0',
    totalManagedLamports: Number.isInteger(order.totalManagedLamports) ? order.totalManagedLamports : 0,
    currentTokenAmountRaw: typeof order.currentTokenAmountRaw === 'string' ? order.currentTokenAmountRaw : '0',
    currentTokenAmountDisplay: typeof order.currentTokenAmountDisplay === 'string' ? order.currentTokenAmountDisplay : '0',
    currentMarketCapUsd: Number.isFinite(order.currentMarketCapUsd) ? Number(order.currentMarketCapUsd) : null,
    currentMarketCapSol: typeof order.currentMarketCapSol === 'string' ? order.currentMarketCapSol : null,
    marketPhase: typeof order.marketPhase === 'string' ? order.marketPhase : null,
    recommendedGasLamports: Number.isInteger(order.recommendedGasLamports)
      ? order.recommendedGasLamports
      : FOMO_WORKER_GAS_RESERVE_LAMPORTS * (Number.isInteger(order.walletCount) ? order.walletCount : FOMO_DEFAULT_WALLET_COUNT),
    awaitingField: typeof order.awaitingField === 'string' ? order.awaitingField : null,
    automationEnabled: Boolean(order.automationEnabled),
    status: typeof order.status === 'string' ? order.status : 'setup',
    stats: order.stats && typeof order.stats === 'object' ? order.stats : {},
    lastBundleId: typeof order.lastBundleId === 'string' ? order.lastBundleId : null,
    lastBundleAt: typeof order.lastBundleAt === 'string' ? order.lastBundleAt : null,
    lastBalanceCheckAt: typeof order.lastBalanceCheckAt === 'string' ? order.lastBalanceCheckAt : null,
    lastError: typeof order.lastError === 'string' ? order.lastError : null,
    createdAt: typeof order.createdAt === 'string' ? order.createdAt : defaultState.createdAt,
    updatedAt: typeof order.updatedAt === 'string' ? order.updatedAt : defaultState.updatedAt,
  };
}

function normalizeSniperWizard(order = {}) {
  const defaultState = createDefaultSniperWizard();
  const walletCount = Number.isInteger(order.walletCount)
    ? Math.min(SNIPER_MAX_WALLET_COUNT, Math.max(SNIPER_MIN_WALLET_COUNT, order.walletCount))
    : SNIPER_DEFAULT_WALLET_COUNT;
  return {
    ...defaultState,
    ...(order ?? {}),
    id: typeof order.id === 'string' && order.id ? order.id : defaultState.id,
    sniperMode: order?.sniperMode === 'magic' || order?.sniperMode === 'standard'
      ? order.sniperMode
      : null,
    targetWalletAddress: typeof order.targetWalletAddress === 'string' ? order.targetWalletAddress : null,
    walletAddress: typeof order.walletAddress === 'string' ? order.walletAddress : defaultState.walletAddress,
    walletSecretKeyB64: typeof order.walletSecretKeyB64 === 'string'
      ? order.walletSecretKeyB64
      : defaultState.walletSecretKeyB64,
    walletSecretKeyBase58: typeof order.walletSecretKeyBase58 === 'string'
      ? order.walletSecretKeyBase58
      : defaultState.walletSecretKeyBase58,
    privateKeyVisible: Boolean(order.privateKeyVisible),
    snipePercent: Number.isInteger(order.snipePercent)
      ? order.snipePercent
      : defaultState.snipePercent,
    walletCount,
    workerWallets: Array.isArray(order.workerWallets) && order.workerWallets.length > 0
      ? order.workerWallets
        .filter((wallet) => wallet && typeof wallet === 'object')
        .slice(0, walletCount)
        .map((wallet) => normalizeLaunchBuyBuyerWallet(wallet))
      : createLaunchBuyBuyerWallets(walletCount),
    currentLamports: Number.isInteger(order.currentLamports) ? order.currentLamports : 0,
    currentSol: typeof order.currentSol === 'string' ? order.currentSol : '0',
    totalManagedLamports: Number.isInteger(order.totalManagedLamports) ? order.totalManagedLamports : 0,
    estimatedPlatformFeeLamports: Number.isInteger(order.estimatedPlatformFeeLamports)
      ? order.estimatedPlatformFeeLamports
      : 0,
    estimatedSplitNowFeeLamports: Number.isInteger(order.estimatedSplitNowFeeLamports)
      ? order.estimatedSplitNowFeeLamports
      : 0,
    estimatedNetSplitLamports: Number.isInteger(order.estimatedNetSplitLamports)
      ? order.estimatedNetSplitLamports
      : 0,
    routingOrderId: typeof order.routingOrderId === 'string' ? order.routingOrderId : null,
    routingQuoteId: typeof order.routingQuoteId === 'string' ? order.routingQuoteId : null,
    routingDepositAddress: typeof order.routingDepositAddress === 'string' ? order.routingDepositAddress : null,
    routingStatus: typeof order.routingStatus === 'string' ? order.routingStatus : null,
    routingCompletedAt: typeof order.routingCompletedAt === 'string' ? order.routingCompletedAt : null,
    awaitingField: typeof order.awaitingField === 'string' ? order.awaitingField : null,
    automationEnabled: Boolean(order.automationEnabled),
    status: typeof order.status === 'string' ? order.status : 'setup',
    stats: order.stats && typeof order.stats === 'object' ? order.stats : {},
    lastDetectedLaunchSignature: typeof order.lastDetectedLaunchSignature === 'string'
      ? order.lastDetectedLaunchSignature
      : null,
    lastDetectedMintAddress: typeof order.lastDetectedMintAddress === 'string'
      ? order.lastDetectedMintAddress
      : null,
    lastSnipeSignature: typeof order.lastSnipeSignature === 'string' ? order.lastSnipeSignature : null,
    lastBalanceCheckAt: typeof order.lastBalanceCheckAt === 'string' ? order.lastBalanceCheckAt : null,
    lastError: typeof order.lastError === 'string' ? order.lastError : null,
    createdAt: typeof order.createdAt === 'string' ? order.createdAt : defaultState.createdAt,
    updatedAt: typeof order.updatedAt === 'string' ? order.updatedAt : defaultState.updatedAt,
  };
}

function normalizeMagicBundleWorkerWallet(wallet = {}) {
  return {
    ...createMagicBundleWorkerWallet(),
    ...(wallet ?? {}),
    address: typeof wallet.address === 'string' ? wallet.address : null,
    secretKeyB64: typeof wallet.secretKeyB64 === 'string' ? wallet.secretKeyB64 : null,
    secretKeyBase58: typeof wallet.secretKeyBase58 === 'string' ? wallet.secretKeyBase58 : null,
    currentLamports: Number.isInteger(wallet.currentLamports) ? wallet.currentLamports : 0,
    currentSol: typeof wallet.currentSol === 'string' ? wallet.currentSol : '0',
    currentTokenAmountRaw: typeof wallet.currentTokenAmountRaw === 'string' ? wallet.currentTokenAmountRaw : '0',
    currentTokenAmountDisplay: typeof wallet.currentTokenAmountDisplay === 'string'
      ? wallet.currentTokenAmountDisplay
      : '0',
    currentPositionValueLamports: Number.isInteger(wallet.currentPositionValueLamports)
      ? wallet.currentPositionValueLamports
      : 0,
    costBasisLamports: Number.isInteger(wallet.costBasisLamports) ? wallet.costBasisLamports : null,
    highestValueLamports: Number.isInteger(wallet.highestValueLamports) ? wallet.highestValueLamports : null,
    buyDipCount: Number.isInteger(wallet.buyDipCount) ? wallet.buyDipCount : 0,
    lastActionAt: typeof wallet.lastActionAt === 'string' ? wallet.lastActionAt : null,
    lastBuySignature: typeof wallet.lastBuySignature === 'string' ? wallet.lastBuySignature : null,
    lastSellSignature: typeof wallet.lastSellSignature === 'string' ? wallet.lastSellSignature : null,
    lastTriggerReason: typeof wallet.lastTriggerReason === 'string' ? wallet.lastTriggerReason : null,
    status: typeof wallet.status === 'string' ? wallet.status : 'idle',
    lastError: typeof wallet.lastError === 'string' ? wallet.lastError : null,
  };
}

function normalizeMagicBundleStats(stats = {}) {
  return {
    triggerCount: Number.isInteger(stats?.triggerCount) ? stats.triggerCount : 0,
    buyCount: Number.isInteger(stats?.buyCount) ? stats.buyCount : 0,
    sellCount: Number.isInteger(stats?.sellCount) ? stats.sellCount : 0,
    dipBuyCount: Number.isInteger(stats?.dipBuyCount) ? stats.dipBuyCount : 0,
    stopLossCount: Number.isInteger(stats?.stopLossCount) ? stats.stopLossCount : 0,
    takeProfitCount: Number.isInteger(stats?.takeProfitCount) ? stats.takeProfitCount : 0,
    trailingStopCount: Number.isInteger(stats?.trailingStopCount) ? stats.trailingStopCount : 0,
    devSellCount: Number.isInteger(stats?.devSellCount) ? stats.devSellCount : 0,
    totalBuyLamports: Number.isInteger(stats?.totalBuyLamports) ? stats.totalBuyLamports : 0,
    totalSellLamports: Number.isInteger(stats?.totalSellLamports) ? stats.totalSellLamports : 0,
    totalFeeLamports: Number.isInteger(stats?.totalFeeLamports) ? stats.totalFeeLamports : 0,
    lastBuySignature: typeof stats?.lastBuySignature === 'string' ? stats.lastBuySignature : null,
    lastSellSignature: typeof stats?.lastSellSignature === 'string' ? stats.lastSellSignature : null,
    lastTriggerReason: typeof stats?.lastTriggerReason === 'string' ? stats.lastTriggerReason : null,
  };
}

function normalizeMagicBundle(order = {}) {
  const defaultState = createDefaultMagicBundle();
  const walletCount = Number.isInteger(order.walletCount)
    ? Math.min(MAGIC_BUNDLE_MAX_WALLET_COUNT, Math.max(MAGIC_BUNDLE_MIN_WALLET_COUNT, order.walletCount))
    : MAGIC_BUNDLE_DEFAULT_WALLET_COUNT;
  const bundleMode = order.bundleMode === 'standard' ? 'standard' : 'stealth';
  const platformFeeBps = bundleMode === 'standard'
    ? 0
    : 0;
  const splitNowFeeEstimateBps = bundleMode === 'standard'
    ? 0
    : (Number.isInteger(order.splitNowFeeEstimateBps) ? order.splitNowFeeEstimateBps : cfg.magicBundleSplitNowFeeEstimateBps);
  return {
    ...defaultState,
    ...(order ?? {}),
    id: typeof order.id === 'string' && order.id ? order.id : defaultState.id,
    bundleMode,
    tokenName: typeof order.tokenName === 'string' ? order.tokenName : null,
    mintAddress: typeof order.mintAddress === 'string' ? order.mintAddress : null,
    tokenDecimals: Number.isInteger(order.tokenDecimals) ? order.tokenDecimals : null,
    tokenProgram: typeof order.tokenProgram === 'string' ? order.tokenProgram : null,
    walletCount,
    splitWallets: Array.isArray(order.splitWallets)
      ? order.splitWallets.map((wallet) => normalizeMagicBundleWorkerWallet(wallet))
      : createMagicBundleWorkerWallets(walletCount),
    walletAddress: typeof order.walletAddress === 'string' ? order.walletAddress : defaultState.walletAddress,
    walletSecretKeyB64: typeof order.walletSecretKeyB64 === 'string'
      ? order.walletSecretKeyB64
      : defaultState.walletSecretKeyB64,
    walletSecretKeyBase58: typeof order.walletSecretKeyBase58 === 'string'
      ? order.walletSecretKeyBase58
      : defaultState.walletSecretKeyBase58,
    currentLamports: Number.isInteger(order.currentLamports) ? order.currentLamports : 0,
    currentSol: typeof order.currentSol === 'string' ? order.currentSol : '0',
    totalManagedLamports: Number.isInteger(order.totalManagedLamports) ? order.totalManagedLamports : 0,
    currentTokenAmountRaw: typeof order.currentTokenAmountRaw === 'string' ? order.currentTokenAmountRaw : '0',
    currentTokenAmountDisplay: typeof order.currentTokenAmountDisplay === 'string'
      ? order.currentTokenAmountDisplay
      : '0',
    currentPositionValueLamports: Number.isInteger(order.currentPositionValueLamports)
      ? order.currentPositionValueLamports
      : 0,
    creatorAddress: typeof order.creatorAddress === 'string' ? order.creatorAddress : null,
    stopLossPercent: Number.isFinite(order.stopLossPercent) ? Number(order.stopLossPercent) : null,
    takeProfitPercent: Number.isFinite(order.takeProfitPercent) ? Number(order.takeProfitPercent) : null,
    trailingStopLossPercent: Number.isFinite(order.trailingStopLossPercent) ? Number(order.trailingStopLossPercent) : null,
    buyDipPercent: Number.isFinite(order.buyDipPercent) ? Number(order.buyDipPercent) : null,
    sellOnDevSell: Boolean(order.sellOnDevSell),
    automationEnabled: Boolean(order.automationEnabled),
    platformFeeBps,
    splitNowFeeEstimateBps,
    estimatedPlatformFeeLamports: Number.isInteger(order.estimatedPlatformFeeLamports) ? order.estimatedPlatformFeeLamports : 0,
    estimatedSplitNowFeeLamports: Number.isInteger(order.estimatedSplitNowFeeLamports) ? order.estimatedSplitNowFeeLamports : 0,
    estimatedNetSplitLamports: Number.isInteger(order.estimatedNetSplitLamports) ? order.estimatedNetSplitLamports : 0,
    splitnowOrderId: typeof order.splitnowOrderId === 'string' ? order.splitnowOrderId : null,
    splitnowQuoteId: typeof order.splitnowQuoteId === 'string' ? order.splitnowQuoteId : null,
    splitnowDepositAddress: typeof order.splitnowDepositAddress === 'string' ? order.splitnowDepositAddress : null,
    splitnowDepositAmountSol: typeof order.splitnowDepositAmountSol === 'string' ? order.splitnowDepositAmountSol : null,
    splitnowStatus: typeof order.splitnowStatus === 'string' ? order.splitnowStatus : null,
    splitCompletedAt: typeof order.splitCompletedAt === 'string' ? order.splitCompletedAt : null,
    stats: normalizeMagicBundleStats(order.stats),
    lastCreatorSeenSignature: typeof order.lastCreatorSeenSignature === 'string' ? order.lastCreatorSeenSignature : null,
    lastActionAt: typeof order.lastActionAt === 'string' ? order.lastActionAt : null,
    lastTriggerReason: typeof order.lastTriggerReason === 'string' ? order.lastTriggerReason : null,
    lastBalanceCheckAt: typeof order.lastBalanceCheckAt === 'string' ? order.lastBalanceCheckAt : null,
    lastError: typeof order.lastError === 'string' ? order.lastError : null,
    awaitingField: typeof order.awaitingField === 'string' ? order.awaitingField : null,
    status: typeof order.status === 'string' ? order.status : 'setup',
    createdAt: typeof order.createdAt === 'string' ? order.createdAt : defaultState.createdAt,
    updatedAt: typeof order.updatedAt === 'string' ? order.updatedAt : defaultState.updatedAt,
    deleteConfirmations: Number.isInteger(order.deleteConfirmations) ? order.deleteConfirmations : 0,
    archivedAt: typeof order.archivedAt === 'string' ? order.archivedAt : null,
  };
}

function normalizeTradingWallet(wallet = {}) {
  return {
    ...createTradingWallet(Boolean(wallet.imported)),
    ...(wallet ?? {}),
    id: typeof wallet.id === 'string' && wallet.id ? wallet.id : createTradingWalletId(),
    label: typeof wallet.label === 'string' && wallet.label.trim()
      ? wallet.label.trim().slice(0, 24)
      : (wallet.imported ? 'Imported Wallet' : 'Generated Wallet'),
    address: typeof wallet.address === 'string' ? wallet.address : null,
    secretKeyB64: typeof wallet.secretKeyB64 === 'string' ? wallet.secretKeyB64 : null,
    secretKeyBase58: typeof wallet.secretKeyBase58 === 'string' ? wallet.secretKeyBase58 : null,
    currentLamports: Number.isInteger(wallet.currentLamports) ? wallet.currentLamports : 0,
    currentSol: typeof wallet.currentSol === 'string' ? wallet.currentSol : '0',
    imported: Boolean(wallet.imported),
    privateKeyVisible: Boolean(wallet.privateKeyVisible),
    createdAt: typeof wallet.createdAt === 'string' ? wallet.createdAt : new Date().toISOString(),
  };
}

function normalizeTradingDesk(tradingDesk = {}) {
  const wallets = Array.isArray(tradingDesk.wallets)
    ? tradingDesk.wallets.map((wallet) => normalizeTradingWallet(wallet))
    : [];
  const activeWalletId = typeof tradingDesk.activeWalletId === 'string'
    && wallets.some((wallet) => wallet.id === tradingDesk.activeWalletId)
    ? tradingDesk.activeWalletId
    : (wallets[0]?.id ?? null);
  return {
    ...createDefaultTradingDesk(),
    ...(tradingDesk ?? {}),
    wallets,
    activeWalletId,
    quickTradeMintAddress: typeof tradingDesk.quickTradeMintAddress === 'string'
      ? tradingDesk.quickTradeMintAddress
      : null,
    selectedMagicBundleId: typeof tradingDesk.selectedMagicBundleId === 'string'
      ? tradingDesk.selectedMagicBundleId
      : null,
    quickBuyLamports: Number.isInteger(tradingDesk.quickBuyLamports)
      ? tradingDesk.quickBuyLamports
      : (typeof tradingDesk.quickBuySol === 'string' ? parseSolToLamports(tradingDesk.quickBuySol) : null),
    quickBuySol: Number.isInteger(tradingDesk.quickBuyLamports)
      ? formatSolAmountFromLamports(tradingDesk.quickBuyLamports)
      : (typeof tradingDesk.quickBuySol === 'string' ? tradingDesk.quickBuySol : null),
    quickSellPercent: Number.isInteger(tradingDesk.quickSellPercent)
      ? Math.max(1, Math.min(100, tradingDesk.quickSellPercent))
      : 100,
    pendingAction: tradingDesk.pendingAction && typeof tradingDesk.pendingAction === 'object'
      ? {
        type: tradingDesk.pendingAction.type === 'sell' ? 'sell' : 'buy',
        requestedAt: typeof tradingDesk.pendingAction.requestedAt === 'string'
          ? tradingDesk.pendingAction.requestedAt
          : null,
        mintAddress: typeof tradingDesk.pendingAction.mintAddress === 'string'
          ? tradingDesk.pendingAction.mintAddress
          : null,
        buyLamports: Number.isInteger(tradingDesk.pendingAction.buyLamports)
          ? tradingDesk.pendingAction.buyLamports
          : null,
        sellPercent: Number.isInteger(tradingDesk.pendingAction.sellPercent)
          ? Math.max(1, Math.min(100, tradingDesk.pendingAction.sellPercent))
          : null,
      }
      : null,
    lastTradeSignature: typeof tradingDesk.lastTradeSignature === 'string'
      ? tradingDesk.lastTradeSignature
      : null,
    lastTradeSide: typeof tradingDesk.lastTradeSide === 'string'
      ? tradingDesk.lastTradeSide
      : null,
    lastTradeAt: typeof tradingDesk.lastTradeAt === 'string'
      ? tradingDesk.lastTradeAt
      : null,
    handlingFeeBps: Number.isInteger(tradingDesk.handlingFeeBps)
      ? tradingDesk.handlingFeeBps
      : cfg.tradingHandlingFeeBps,
    limitOrder: {
      ...createDefaultTradingDesk().limitOrder,
      ...(tradingDesk.limitOrder ?? {}),
      side: tradingDesk.limitOrder?.side === 'sell' ? 'sell' : 'buy',
      triggerMarketCapUsd: Number.isFinite(tradingDesk.limitOrder?.triggerMarketCapUsd)
        ? Number(tradingDesk.limitOrder.triggerMarketCapUsd)
        : null,
      buyLamports: Number.isInteger(tradingDesk.limitOrder?.buyLamports)
        ? tradingDesk.limitOrder.buyLamports
        : (typeof tradingDesk.limitOrder?.buySol === 'string' ? parseSolToLamports(tradingDesk.limitOrder.buySol) : null),
      buySol: Number.isInteger(tradingDesk.limitOrder?.buyLamports)
        ? formatSolAmountFromLamports(tradingDesk.limitOrder.buyLamports)
        : (typeof tradingDesk.limitOrder?.buySol === 'string' ? tradingDesk.limitOrder.buySol : null),
      sellPercent: Number.isInteger(tradingDesk.limitOrder?.sellPercent)
        ? Math.max(1, Math.min(100, tradingDesk.limitOrder.sellPercent))
        : 100,
      enabled: Boolean(tradingDesk.limitOrder?.enabled),
      lastTriggeredAt: typeof tradingDesk.limitOrder?.lastTriggeredAt === 'string'
        ? tradingDesk.limitOrder.lastTriggeredAt
        : null,
      lastTriggerSignature: typeof tradingDesk.limitOrder?.lastTriggerSignature === 'string'
        ? tradingDesk.limitOrder.lastTriggerSignature
        : null,
      lastError: typeof tradingDesk.limitOrder?.lastError === 'string'
        ? tradingDesk.limitOrder.lastError
        : null,
    },
    copyTrade: {
      ...createDefaultTradingDesk().copyTrade,
      ...(tradingDesk.copyTrade ?? {}),
      followWalletAddress: typeof tradingDesk.copyTrade?.followWalletAddress === 'string'
        ? tradingDesk.copyTrade.followWalletAddress
        : null,
      fixedBuyLamports: Number.isInteger(tradingDesk.copyTrade?.fixedBuyLamports)
        ? tradingDesk.copyTrade.fixedBuyLamports
        : (typeof tradingDesk.copyTrade?.fixedBuySol === 'string' ? parseSolToLamports(tradingDesk.copyTrade.fixedBuySol) : null),
      fixedBuySol: Number.isInteger(tradingDesk.copyTrade?.fixedBuyLamports)
        ? formatSolAmountFromLamports(tradingDesk.copyTrade.fixedBuyLamports)
        : (typeof tradingDesk.copyTrade?.fixedBuySol === 'string' ? tradingDesk.copyTrade.fixedBuySol : null),
      copySells: typeof tradingDesk.copyTrade?.copySells === 'boolean'
        ? tradingDesk.copyTrade.copySells
        : true,
      enabled: Boolean(tradingDesk.copyTrade?.enabled),
      lastSeenSignature: typeof tradingDesk.copyTrade?.lastSeenSignature === 'string'
        ? tradingDesk.copyTrade.lastSeenSignature
        : null,
      lastCopiedAt: typeof tradingDesk.copyTrade?.lastCopiedAt === 'string'
        ? tradingDesk.copyTrade.lastCopiedAt
        : null,
      lastError: typeof tradingDesk.copyTrade?.lastError === 'string'
        ? tradingDesk.copyTrade.lastError
        : null,
      stats: {
        buyCount: Number.isInteger(tradingDesk.copyTrade?.stats?.buyCount)
          ? tradingDesk.copyTrade.stats.buyCount
          : 0,
        sellCount: Number.isInteger(tradingDesk.copyTrade?.stats?.sellCount)
          ? tradingDesk.copyTrade.stats.sellCount
          : 0,
      },
    },
    awaitingField: typeof tradingDesk.awaitingField === 'string' ? tradingDesk.awaitingField : null,
    status: typeof tradingDesk.status === 'string' ? tradingDesk.status : 'idle',
    lastBalanceCheckAt: typeof tradingDesk.lastBalanceCheckAt === 'string' ? tradingDesk.lastBalanceCheckAt : null,
    lastError: typeof tradingDesk.lastError === 'string' ? tradingDesk.lastError : null,
  };
}

function buildSourceLinkedTradingWallet({
  sourceType,
  sourceId,
  label,
  address,
  secretKeyB64,
  secretKeyBase58,
  imported = false,
  currentLamports = 0,
  currentSol = '0',
}) {
  return normalizeTradingWallet({
    id: createTradingWalletId(),
    label,
    address,
    secretKeyB64,
    secretKeyBase58,
    imported,
    currentLamports,
    currentSol,
    sourceType,
    sourceId,
  });
}

function syncTradingDeskWalletsFromSource(draft, sourceType, sourceId, wallets, { selectFirst = false } = {}) {
  const tradingDesk = normalizeTradingDesk(draft.tradingDesk);
  const nextSourceWallets = Array.isArray(wallets)
    ? wallets
      .filter((wallet) => wallet?.address && wallet?.secretKeyB64)
      .map((wallet) => normalizeTradingWallet(wallet))
    : [];
  const preservedWallets = tradingDesk.wallets.filter((wallet) => !(wallet.sourceType === sourceType && wallet.sourceId === sourceId));
  const existingAddresses = new Set(preservedWallets.map((wallet) => wallet.address).filter(Boolean));
  const appendedWallets = nextSourceWallets.filter((wallet) => !existingAddresses.has(wallet.address));
  const nextWallets = [...preservedWallets, ...appendedWallets];
  const nextActiveWalletId = selectFirst && appendedWallets[0]?.id
    ? appendedWallets[0].id
    : (nextWallets.some((wallet) => wallet.id === tradingDesk.activeWalletId)
      ? tradingDesk.activeWalletId
      : (nextWallets[0]?.id ?? null));

  draft.tradingDesk = normalizeTradingDesk({
    ...tradingDesk,
    wallets: nextWallets,
    activeWalletId: nextActiveWalletId,
  });
}

function createLaunchBuyTradingWallets(order) {
  const tokenLabel = order?.tokenName?.trim() || order?.symbol?.trim() || 'Launch';
  return Array.isArray(order?.buyerWallets)
    ? order.buyerWallets.map((wallet, index) => buildSourceLinkedTradingWallet({
      sourceType: 'launch_buy',
      sourceId: order.id,
      label: `${tokenLabel} L${index + 1}`,
      address: wallet.address,
      secretKeyB64: wallet.secretKeyB64,
      secretKeyBase58: wallet.secretKeyBase58,
      imported: Boolean(wallet.imported),
      currentLamports: Number.isInteger(wallet.currentLamports) ? wallet.currentLamports : 0,
      currentSol: typeof wallet.currentSol === 'string' ? wallet.currentSol : '0',
    }))
    : [];
}

function createSniperTradingWallets(order) {
  const targetLabel = order?.targetWalletAddress
    ? `${order.targetWalletAddress.slice(0, 4)}...${order.targetWalletAddress.slice(-4)}`
    : 'Sniper';
  return Array.isArray(order?.workerWallets)
    ? order.workerWallets.map((wallet, index) => buildSourceLinkedTradingWallet({
      sourceType: 'sniper_wizard',
      sourceId: order.id,
      label: `${targetLabel} S${index + 1}`,
      address: wallet.address,
      secretKeyB64: wallet.secretKeyB64,
      secretKeyBase58: wallet.secretKeyBase58,
      imported: Boolean(wallet.imported),
      currentLamports: Number.isInteger(wallet.currentLamports) ? wallet.currentLamports : 0,
      currentSol: typeof wallet.currentSol === 'string' ? wallet.currentSol : '0',
    }))
    : [];
}

function createMagicBundleTradingWallets(order) {
  const tokenLabel = order?.tokenName?.trim() || order?.mintAddress?.slice(0, 6) || 'Bundle';
  return Array.isArray(order?.splitWallets)
    ? order.splitWallets.map((wallet, index) => buildSourceLinkedTradingWallet({
      sourceType: 'magic_bundle',
      sourceId: order.id,
      label: `${tokenLabel} B${index + 1}`,
      address: wallet.address,
      secretKeyB64: wallet.secretKeyB64,
      secretKeyBase58: wallet.secretKeyBase58,
      imported: false,
      currentLamports: Number.isInteger(wallet.currentLamports) ? wallet.currentLamports : 0,
      currentSol: typeof wallet.currentSol === 'string' ? wallet.currentSol : '0',
    }))
    : [];
}

function syncSniperWizardTradingDesk(draft, { selectFirst = false } = {}) {
  const order = normalizeSniperWizard(draft.sniperWizard);
  syncTradingDeskWalletsFromSource(
    draft,
    'sniper_wizard',
    order.id,
    createSniperTradingWallets(order),
    { selectFirst },
  );
}

function syncMagicBundleTradingDesk(draft, { selectFirst = false } = {}) {
  const order = normalizeMagicBundle(draft.magicBundle);
  syncTradingDeskWalletsFromSource(
    draft,
    'magic_bundle',
    order.id,
    createMagicBundleTradingWallets(order),
    { selectFirst },
  );
}

function vanityWalletPrivateKeyText(state) {
  if (!state?.generatedSecretKeyBase58) {
    return '`Not generated yet`';
  }

  return state.privateKeyVisible
    ? `\`${state.generatedSecretKeyBase58}\``
    : '`Hidden - tap Show Key to reveal it.`';
}

function normalizeVanityWalletState(state = {}) {
  const defaults = createDefaultVanityWalletState();
  const patternMode = ['prefix', 'suffix'].includes(state.patternMode) ? state.patternMode : null;
  return {
    ...defaults,
    ...(state ?? {}),
    id: typeof state.id === 'string' && state.id ? state.id : defaults.id,
    patternMode,
    pattern: typeof state.pattern === 'string' ? state.pattern : null,
    awaitingField: typeof state.awaitingField === 'string' ? state.awaitingField : null,
    status: typeof state.status === 'string' ? state.status : defaults.status,
    payment: normalizePaymentState(state.payment),
    generatedAddress: typeof state.generatedAddress === 'string' ? state.generatedAddress : null,
    generatedSecretKeyB64: typeof state.generatedSecretKeyB64 === 'string' ? state.generatedSecretKeyB64 : null,
    generatedSecretKeyBase58: typeof state.generatedSecretKeyBase58 === 'string' ? state.generatedSecretKeyBase58 : null,
    privateKeyVisible: Boolean(state.privateKeyVisible),
    attemptCount: Number.isInteger(state.attemptCount) ? state.attemptCount : 0,
    generationStartedAt: typeof state.generationStartedAt === 'string' ? state.generationStartedAt : null,
    completedAt: typeof state.completedAt === 'string' ? state.completedAt : null,
    estimatedTreasuryShareLamports: Number.isInteger(state.estimatedTreasuryShareLamports)
      ? state.estimatedTreasuryShareLamports
      : defaults.estimatedTreasuryShareLamports,
    estimatedBurnShareLamports: Number.isInteger(state.estimatedBurnShareLamports)
      ? state.estimatedBurnShareLamports
      : defaults.estimatedBurnShareLamports,
    lastError: typeof state.lastError === 'string' ? state.lastError : null,
  };
}

function normalizeOrganicVolumeOrder(order = {}) {
  return {
    ...createDefaultOrganicVolumeOrder(),
    ...(order ?? {}),
    id: typeof order.id === 'string' && order.id ? order.id : createAppleBoosterId(),
    strategy: order.strategy === 'bundled' ? 'bundled' : 'organic',
    freeTrial: Boolean(order.freeTrial),
    trialTradeGoal: Number.isInteger(order.trialTradeGoal) ? order.trialTradeGoal : null,
    packageKey: typeof order.packageKey === 'string' ? order.packageKey : null,
    walletAddress: typeof order.walletAddress === 'string' ? order.walletAddress : null,
    walletSecretKeyB64: typeof order.walletSecretKeyB64 === 'string' ? order.walletSecretKeyB64 : null,
    requiredSol: typeof order.requiredSol === 'string' ? order.requiredSol : null,
    rebateSol: typeof order.rebateSol === 'string' ? order.rebateSol : null,
    treasuryCutSol: typeof order.treasuryCutSol === 'string' ? order.treasuryCutSol : null,
    treasuryShareSol: typeof order.treasuryShareSol === 'string' ? order.treasuryShareSol : null,
    devShareSol: typeof order.devShareSol === 'string' ? order.devShareSol : null,
    usableSol: typeof order.usableSol === 'string' ? order.usableSol : null,
    treasuryWalletAddress: typeof order.treasuryWalletAddress === 'string' ? order.treasuryWalletAddress : null,
    devWalletAddress: typeof order.devWalletAddress === 'string' ? order.devWalletAddress : null,
    requiredLamports: Number.isInteger(order.requiredLamports) ? order.requiredLamports : null,
    currentLamports: Number.isInteger(order.currentLamports) ? order.currentLamports : 0,
    currentSol: typeof order.currentSol === 'string' ? order.currentSol : '0',
    funded: Boolean(order.funded),
    running: Boolean(order.running),
    awaitingField: typeof order.awaitingField === 'string' ? order.awaitingField : null,
    createdAt: typeof order.createdAt === 'string' ? order.createdAt : null,
    fundedAt: typeof order.fundedAt === 'string' ? order.fundedAt : null,
    lastBalanceCheckAt: typeof order.lastBalanceCheckAt === 'string' ? order.lastBalanceCheckAt : null,
    lastError: typeof order.lastError === 'string' ? order.lastError : null,
    deleteConfirmations: Number.isInteger(order.deleteConfirmations) ? order.deleteConfirmations : 0,
    archivedAt: typeof order.archivedAt === 'string' ? order.archivedAt : null,
    appleBooster: {
      ...createDefaultAppleBoosterState(),
      ...(order.appleBooster ?? {}),
      walletCount: Number.isInteger(order.appleBooster?.walletCount)
        ? order.appleBooster.walletCount
        : null,
      workerWallets: Array.isArray(order.appleBooster?.workerWallets)
        ? order.appleBooster.workerWallets.map((worker) => ({
          ...createAppleBoosterWorkerWallet(),
          ...(worker ?? {}),
          address: typeof worker?.address === 'string' ? worker.address : null,
          secretKeyB64: typeof worker?.secretKeyB64 === 'string' ? worker.secretKeyB64 : null,
          secretKeyBase58: typeof worker?.secretKeyBase58 === 'string' ? worker.secretKeyBase58 : null,
          currentLamports: Number.isInteger(worker?.currentLamports) ? worker.currentLamports : 0,
          currentSol: typeof worker?.currentSol === 'string' ? worker.currentSol : '0',
          status: typeof worker?.status === 'string' ? worker.status : 'idle',
          pendingSellAmount: typeof worker?.pendingSellAmount === 'string' ? worker.pendingSellAmount : null,
          nextActionAt: typeof worker?.nextActionAt === 'string' ? worker.nextActionAt : null,
          lastActionAt: typeof worker?.lastActionAt === 'string' ? worker.lastActionAt : null,
          lastBuyInputLamports: Number.isInteger(worker?.lastBuyInputLamports)
            ? worker.lastBuyInputLamports
            : null,
          lastBuyOutputAmount: typeof worker?.lastBuyOutputAmount === 'string'
            ? worker.lastBuyOutputAmount
            : null,
          lastBuySignature: typeof worker?.lastBuySignature === 'string' ? worker.lastBuySignature : null,
          lastSellInputAmount: typeof worker?.lastSellInputAmount === 'string'
            ? worker.lastSellInputAmount
            : null,
          lastSellOutputLamports: typeof worker?.lastSellOutputLamports === 'string'
            ? worker.lastSellOutputLamports
            : null,
          lastSellSignature: typeof worker?.lastSellSignature === 'string'
            ? worker.lastSellSignature
            : null,
          lastError: typeof worker?.lastError === 'string' ? worker.lastError : null,
        }))
        : [],
      totalManagedLamports: Number.isInteger(order.appleBooster?.totalManagedLamports)
        ? order.appleBooster.totalManagedLamports
        : null,
      marketPhase: typeof order.appleBooster?.marketPhase === 'string'
        ? order.appleBooster.marketPhase
        : null,
      marketCapLamports: typeof order.appleBooster?.marketCapLamports === 'string'
        ? order.appleBooster.marketCapLamports
        : null,
      marketCapSol: typeof order.appleBooster?.marketCapSol === 'string'
        ? order.appleBooster.marketCapSol
        : null,
      lpFeeBps: Number.isInteger(order.appleBooster?.lpFeeBps)
        ? order.appleBooster.lpFeeBps
        : null,
      protocolFeeBps: Number.isInteger(order.appleBooster?.protocolFeeBps)
        ? order.appleBooster.protocolFeeBps
        : null,
      creatorFeeBps: Number.isInteger(order.appleBooster?.creatorFeeBps)
        ? order.appleBooster.creatorFeeBps
        : null,
      estimatedCycleFeeLamports: Number.isInteger(order.appleBooster?.estimatedCycleFeeLamports)
        ? order.appleBooster.estimatedCycleFeeLamports
        : null,
      estimatedTradeFeeLamports: Number.isInteger(order.appleBooster?.estimatedTradeFeeLamports)
        ? order.appleBooster.estimatedTradeFeeLamports
        : null,
      estimatedNetworkFeeLamports: Number.isInteger(order.appleBooster?.estimatedNetworkFeeLamports)
        ? order.appleBooster.estimatedNetworkFeeLamports
        : null,
      estimatedCyclesRemaining: Number.isInteger(order.appleBooster?.estimatedCyclesRemaining)
        ? order.appleBooster.estimatedCyclesRemaining
        : null,
      estimatedRuntimeSeconds: Number.isInteger(order.appleBooster?.estimatedRuntimeSeconds)
        ? order.appleBooster.estimatedRuntimeSeconds
        : null,
      totalBuyCount: Number.isInteger(order.appleBooster?.totalBuyCount)
        ? order.appleBooster.totalBuyCount
        : 0,
      totalSellCount: Number.isInteger(order.appleBooster?.totalSellCount)
        ? order.appleBooster.totalSellCount
        : 0,
      totalTopUpLamports: Number.isInteger(order.appleBooster?.totalTopUpLamports)
        ? order.appleBooster.totalTopUpLamports
        : 0,
      totalSweptLamports: Number.isInteger(order.appleBooster?.totalSweptLamports)
        ? order.appleBooster.totalSweptLamports
        : 0,
      totalBuyInputLamports: Number.isInteger(order.appleBooster?.totalBuyInputLamports)
        ? order.appleBooster.totalBuyInputLamports
        : 0,
      totalSellOutputLamports: Number.isInteger(order.appleBooster?.totalSellOutputLamports)
        ? order.appleBooster.totalSellOutputLamports
        : 0,
      lastMarketCheckedAt: typeof order.appleBooster?.lastMarketCheckedAt === 'string'
        ? order.appleBooster.lastMarketCheckedAt
        : null,
      minSwapLamports: Number.isInteger(order.appleBooster?.minSwapLamports)
        ? order.appleBooster.minSwapLamports
        : null,
      maxSwapLamports: Number.isInteger(order.appleBooster?.maxSwapLamports)
        ? order.appleBooster.maxSwapLamports
        : null,
      minIntervalSeconds: Number.isInteger(order.appleBooster?.minIntervalSeconds)
        ? order.appleBooster.minIntervalSeconds
        : null,
      maxIntervalSeconds: Number.isInteger(order.appleBooster?.maxIntervalSeconds)
        ? order.appleBooster.maxIntervalSeconds
        : null,
      cycleCount: Number.isInteger(order.appleBooster?.cycleCount)
        ? order.appleBooster.cycleCount
        : 0,
      stopRequested: Boolean(order.appleBooster?.stopRequested),
    },
  };
}

function createDefaultBurnAgentState() {
  return {
    id: createBurnAgentId(),
    speed: null,
    walletMode: null,
    walletAddress: null,
    walletSecretKeyB64: null,
    walletSecretKeyBase58: null,
    tokenName: null,
    mintAddress: null,
    treasuryAddress: null,
    burnPercent: null,
    treasuryPercent: null,
    automationEnabled: false,
    awaitingField: null,
    privateKeyVisible: false,
    regenerateConfirmations: 0,
    deleteConfirmations: 0,
    lastKnownBalanceLamports: null,
    lastAnnouncedMintAddress: null,
    createdAt: null,
    updatedAt: null,
    archivedAt: null,
    runtime: {},
  };
}

function createSolanaRateLimitError(method, retryAfterMs) {
  const seconds = Math.max(1, Math.ceil(retryAfterMs / 1000));
  const error = new Error(`Solana RPC ${method} is rate-limited. Try again in about ${seconds}s.`);
  error.code = 'SOLANA_RPC_RATE_LIMIT';
  error.retryAfterMs = retryAfterMs;
  return error;
}

function isExpiredCallbackQueryError(error) {
  const payload = error?.error ?? error;
  return payload?.method === 'answerCallbackQuery' &&
    typeof payload?.description === 'string' &&
    payload.description.includes('query is too old');
}

function readRetryAfterMs(response) {
  const header = response.headers.get('retry-after');
  const parsedSeconds = Number.parseInt(header || '', 10);
  if (Number.isInteger(parsedSeconds) && parsedSeconds > 0) {
    return parsedSeconds * 1000;
  }

  return 60_000;
}

function getConfig() {
  return createBotConfig({
    env: process.env,
    bundlePricing: BUNDLE_PRICING,
    getBundlePricing,
    parsePositiveInts,
    parseSolToLamports,
    parsePositiveInt,
    parseOptionalInt,
    parseCsv,
    formatSolAmountFromLamports,
  });
}

const cfg = getConfig();
const bot = new Bot(cfg.telegramToken);
const solanaRpcPool = createManagedSolanaRpcPool({
  urls: cfg.solanaRpcUrls,
  commitment: 'confirmed',
  label: 'bot-rpc',
  cooldownMs: cfg.solanaRpcRotationCooldownMs,
  timeoutMs: cfg.solanaRpcTimeoutMs,
  maxRetries: cfg.solanaRpcMaxRetries,
});
const chainConnection = solanaRpcPool.connection;
const TELEGRAM_MENU_COMMANDS = [
  { command: 'start', description: 'welcome & main menu' },
  { command: 'menu', description: 'open the home menu' },
  { command: 'buy_sell', description: 'open the trading desk' },
  { command: 'wallets', description: 'manage trading wallets' },
  { command: 'reaction_booster', description: 'open reaction booster' },
  { command: 'volume_booster', description: 'open volume booster' },
  { command: 'burn_agent', description: 'open burn agent' },
  { command: 'holder_booster', description: 'open holder booster' },
  { command: 'fomo_booster', description: 'open fomo booster' },
  { command: 'smart_sell', description: 'open magic sell' },
  { command: 'magic_bundle', description: 'open bundle' },
  { command: 'launch_buy', description: 'open launch + buy' },
  { command: 'sniper_wizard', description: 'open sniper wizard' },
  { command: 'staking', description: 'open staking' },
  { command: 'vanity_wallet', description: 'open vanity wallet' },
  { command: 'community_vision', description: 'open vision' },
  { command: 'wallet_tracker', description: 'open wallet tracker' },
  { command: 'x_followers', description: 'open x followers' },
  { command: 'engagement', description: 'open engagement menu' },
  { command: 'subscriptions_accounts', description: 'open subscriptions & accounts' },
  { command: 'resizer', description: 'open image resizer' },
  { command: 'help', description: 'help & feature guides' },
];

function createDefaultPaymentState() {
  return {
    status: PAYMENT_STATES.NONE,
    bundleAmount: null,
    usdAmount: null,
    pricePerApple: null,
    role: null,
    quoteId: null,
    quoteCreatedAt: null,
    quoteExpiresAt: null,
    solUsdRate: null,
    lamports: null,
    solAmount: null,
    address: cfg.solanaReceiveAddress,
    matchedSignature: null,
    matchedAt: null,
    matchedLamports: null,
    lastCheckAt: null,
    lastError: null,
    lastIncomingLamports: null,
    announcementKey: null,
    announcementSentAt: null,
  };
}

function createDefaultXFollowersState() {
  return {
    packageKey: null,
    target: null,
    awaitingField: null,
    status: 'setup',
    payment: createDefaultPaymentState(),
    providerCostUsd: null,
    sellPriceUsd: null,
    estimatedProfitUsd: null,
    estimatedTreasuryShareUsd: null,
    estimatedBurnShareUsd: null,
    matchedAt: null,
    lastError: null,
  };
}

function createDefaultStakingState() {
  return {
    walletAddress: null,
    sourceWalletId: null,
    status: 'setup',
    manualClaimOnly: true,
    rewardsAsset: 'SOL',
    claimThresholdLamports: STAKING_MIN_CLAIM_LAMPORTS,
    claimableLamports: 0,
    totalClaimedLamports: 0,
    lastClaimedLamports: 0,
    lastClaimedAt: null,
    lastClaimSignature: null,
    totalStakedRaw: '0',
    totalStakedDisplay: '0',
    trackingStartedAt: null,
    lastBalanceSyncedAt: null,
    lastRewardsAllocatedAt: null,
    currentWeightLabel: 'Starting',
    lastError: null,
  };
}

function normalizePaymentState(payment = {}) {
  const defaults = createDefaultPaymentState();
  return {
    ...defaults,
    ...payment,
    status: payment.status || defaults.status,
    bundleAmount: payment.bundleAmount ? Number(payment.bundleAmount) : null,
    usdAmount: typeof payment.usdAmount === 'number' ? payment.usdAmount : defaults.usdAmount,
    pricePerApple: typeof payment.pricePerApple === 'number' ? payment.pricePerApple : defaults.pricePerApple,
    solUsdRate: typeof payment.solUsdRate === 'number' ? payment.solUsdRate : defaults.solUsdRate,
    lamports: Number.isInteger(payment.lamports) ? payment.lamports : defaults.lamports,
    solAmount: typeof payment.solAmount === 'string' ? payment.solAmount : defaults.solAmount,
    address: cfg.solanaReceiveAddress || payment.address || defaults.address,
    matchedLamports: Number.isInteger(payment.matchedLamports) ? payment.matchedLamports : null,
    lastIncomingLamports: Number.isInteger(payment.lastIncomingLamports) ? payment.lastIncomingLamports : null,
    announcementKey: typeof payment.announcementKey === 'string' ? payment.announcementKey : defaults.announcementKey,
    announcementSentAt: typeof payment.announcementSentAt === 'string' ? payment.announcementSentAt : defaults.announcementSentAt,
  };
}

function normalizeXFollowersState(state = {}) {
  const defaults = createDefaultXFollowersState();
  return {
    ...defaults,
    ...(state ?? {}),
    packageKey: typeof state.packageKey === 'string' ? state.packageKey : null,
    target: typeof state.target === 'string' ? state.target : null,
    awaitingField: typeof state.awaitingField === 'string' ? state.awaitingField : null,
    status: typeof state.status === 'string' ? state.status : defaults.status,
    payment: normalizePaymentState(state.payment),
    providerCostUsd: typeof state.providerCostUsd === 'number' ? state.providerCostUsd : null,
    sellPriceUsd: typeof state.sellPriceUsd === 'number' ? state.sellPriceUsd : null,
    estimatedProfitUsd: typeof state.estimatedProfitUsd === 'number' ? state.estimatedProfitUsd : null,
    estimatedTreasuryShareUsd: typeof state.estimatedTreasuryShareUsd === 'number' ? state.estimatedTreasuryShareUsd : null,
    estimatedBurnShareUsd: typeof state.estimatedBurnShareUsd === 'number' ? state.estimatedBurnShareUsd : null,
    matchedAt: typeof state.matchedAt === 'string' ? state.matchedAt : null,
    lastError: typeof state.lastError === 'string' ? state.lastError : null,
  };
}

function normalizeStakingState(state = {}) {
  const defaults = createDefaultStakingState();
  return {
    ...defaults,
    ...(state ?? {}),
    walletAddress: typeof state.walletAddress === 'string' ? state.walletAddress : null,
    sourceWalletId: typeof state.sourceWalletId === 'string' ? state.sourceWalletId : null,
    status: typeof state.status === 'string' ? state.status : defaults.status,
    manualClaimOnly: typeof state.manualClaimOnly === 'boolean'
      ? state.manualClaimOnly
      : defaults.manualClaimOnly,
    rewardsAsset: typeof state.rewardsAsset === 'string' ? state.rewardsAsset : defaults.rewardsAsset,
    claimThresholdLamports: Number.isInteger(state.claimThresholdLamports)
      ? state.claimThresholdLamports
      : defaults.claimThresholdLamports,
    claimableLamports: Number.isInteger(state.claimableLamports) ? state.claimableLamports : 0,
    totalClaimedLamports: Number.isInteger(state.totalClaimedLamports) ? state.totalClaimedLamports : 0,
    lastClaimedLamports: Number.isInteger(state.lastClaimedLamports) ? state.lastClaimedLamports : 0,
    lastClaimedAt: typeof state.lastClaimedAt === 'string' ? state.lastClaimedAt : null,
    lastClaimSignature: typeof state.lastClaimSignature === 'string' ? state.lastClaimSignature : null,
    totalStakedRaw: typeof state.totalStakedRaw === 'string' ? state.totalStakedRaw : defaults.totalStakedRaw,
    totalStakedDisplay: typeof state.totalStakedDisplay === 'string'
      ? state.totalStakedDisplay
      : defaults.totalStakedDisplay,
    trackingStartedAt: typeof state.trackingStartedAt === 'string' ? state.trackingStartedAt : null,
    lastBalanceSyncedAt: typeof state.lastBalanceSyncedAt === 'string' ? state.lastBalanceSyncedAt : null,
    lastRewardsAllocatedAt: typeof state.lastRewardsAllocatedAt === 'string' ? state.lastRewardsAllocatedAt : null,
    currentWeightLabel: typeof state.currentWeightLabel === 'string'
      ? state.currentWeightLabel
      : defaults.currentWeightLabel,
    lastError: typeof state.lastError === 'string' ? state.lastError : null,
  };
}

function createDefaultUserState(userId) {
  return {
    telegramId: String(userId),
    freeTrialUsed: false,
    volumeFreeTrialUsed: false,
    activityLogs: [],
    volumeMode: null,
    organicVolumePackage: null,
    organicVolumeOrder: createDefaultOrganicVolumeOrder(),
    appleBoosters: [],
    activeAppleBoosterId: null,
    holderBooster: createDefaultHolderBooster(),
    fomoBooster: createDefaultFomoBooster(),
    sniperWizard: createDefaultSniperWizard(),
    tradingDesk: createDefaultTradingDesk(),
    magicBundles: [],
    activeMagicBundleId: null,
    magicSells: [],
    activeMagicSellId: null,
    launchBuys: [],
    activeLaunchBuyId: null,
    communityVisions: [],
    activeCommunityVisionId: null,
    walletTrackers: [],
    activeWalletTrackerId: null,
    staking: createDefaultStakingState(),
    vanityWallet: createDefaultVanityWalletState(),
    resizer: createDefaultResizer(),
    xFollowers: createDefaultXFollowersState(),
    burnAgents: [],
    activeBurnAgentId: null,
    awaitingTargetInput: false,
    selection: {
      button: null,
      amount: null,
      usingFreeTrial: false,
      target: cfg.defaultTarget,
    },
    payment: createDefaultPaymentState(),
  };
}

function hasMeaningfulOrganicVolumeOrder(order = {}) {
  return Boolean(
    order
    && (
      order.packageKey
      || order.walletAddress
      || order.walletSecretKeyB64
      || order.createdAt
      || order.fundedAt
      || order.archivedAt
      || (order.appleBooster && typeof order.appleBooster === 'object' && (
        order.appleBooster.mintAddress
        || order.appleBooster.walletCount
        || Array.isArray(order.appleBooster.workerWallets)
          && order.appleBooster.workerWallets.length > 0
      ))
    )
  );
}

function normalizeAppleBoosters(user = {}) {
  const candidates = Array.isArray(user.appleBoosters) ? user.appleBoosters : [];
  const normalized = candidates.map((order) => normalizeOrganicVolumeOrder(order));

  if (normalized.length === 0 && user.organicVolumeOrder && typeof user.organicVolumeOrder === 'object') {
    const legacy = normalizeOrganicVolumeOrder(user.organicVolumeOrder);
    if (hasMeaningfulOrganicVolumeOrder(legacy)) {
      normalized.push(legacy);
    }
  }

  return normalized;
}

function normalizeBurnAgent(agent = {}) {
  return {
    ...createDefaultBurnAgentState(),
    ...(agent ?? {}),
    id: typeof agent.id === 'string' && agent.id ? agent.id : createBurnAgentId(),
    speed: typeof agent.speed === 'string' ? agent.speed : null,
    walletMode: typeof agent.walletMode === 'string' ? agent.walletMode : null,
    walletAddress: typeof agent.walletAddress === 'string' ? agent.walletAddress : null,
    walletSecretKeyB64: typeof agent.walletSecretKeyB64 === 'string' ? agent.walletSecretKeyB64 : null,
    walletSecretKeyBase58: typeof agent.walletSecretKeyBase58 === 'string'
      ? agent.walletSecretKeyBase58
      : null,
    tokenName: typeof agent.tokenName === 'string' ? agent.tokenName : null,
    mintAddress: typeof agent.mintAddress === 'string' ? agent.mintAddress : null,
    treasuryAddress: typeof agent.treasuryAddress === 'string' ? agent.treasuryAddress : null,
    burnPercent: Number.isInteger(agent.burnPercent) ? agent.burnPercent : null,
    treasuryPercent: Number.isInteger(agent.treasuryPercent) ? agent.treasuryPercent : null,
    automationEnabled: Boolean(agent.automationEnabled),
    awaitingField: typeof agent.awaitingField === 'string' ? agent.awaitingField : null,
    privateKeyVisible: Boolean(agent.privateKeyVisible),
    regenerateConfirmations: Number.isInteger(agent.regenerateConfirmations)
      ? agent.regenerateConfirmations
      : 0,
    deleteConfirmations: Number.isInteger(agent.deleteConfirmations) ? agent.deleteConfirmations : 0,
    lastKnownBalanceLamports: Number.isInteger(agent.lastKnownBalanceLamports)
      ? agent.lastKnownBalanceLamports
      : null,
    lastAnnouncedMintAddress: typeof agent.lastAnnouncedMintAddress === 'string' ? agent.lastAnnouncedMintAddress : null,
    archivedAt: typeof agent.archivedAt === 'string' ? agent.archivedAt : null,
    runtime: agent.runtime && typeof agent.runtime === 'object' ? agent.runtime : {},
  };
}

function normalizeBurnAgents(user = {}) {
  const candidates = Array.isArray(user.burnAgents) ? user.burnAgents : [];
  const normalized = candidates.map((agent) => normalizeBurnAgent(agent));

  if (normalized.length === 0 && user.burnAgent && typeof user.burnAgent === 'object') {
    const legacy = normalizeBurnAgent(user.burnAgent);
    const hasLegacyData = Boolean(
      legacy.speed
      || legacy.walletAddress
      || legacy.walletSecretKeyB64
      || legacy.mintAddress
      || legacy.createdAt
      || legacy.updatedAt
      || Object.keys(legacy.runtime || {}).length > 0,
    );
    if (hasLegacyData) {
      normalized.push(legacy);
    }
  }

  return normalized;
}

function normalizeMagicSells(user = {}) {
  const candidates = Array.isArray(user.magicSells) ? user.magicSells : [];
  return candidates.map((order) => normalizeMagicSell(order));
}

function normalizeMagicBundles(user = {}) {
  const candidates = Array.isArray(user.magicBundles) ? user.magicBundles : [];
  return candidates.map((order) => normalizeMagicBundle(order));
}

function normalizeCommunityVision(order = {}) {
  const defaults = createDefaultCommunityVision();
  return {
    ...defaults,
    ...(order ?? {}),
    id: typeof order?.id === 'string' && order.id ? order.id : defaults.id,
    profileUrl: typeof order?.profileUrl === 'string' ? order.profileUrl : null,
    handle: typeof order?.handle === 'string' ? order.handle : null,
    trackedCommunities: Array.isArray(order?.trackedCommunities)
      ? order.trackedCommunities
        .filter((item) => item && typeof item === 'object')
        .map((item) => ({
          id: typeof item.id === 'string' ? item.id : null,
          name: typeof item.name === 'string' ? item.name : null,
          url: typeof item.url === 'string' ? item.url : null,
        }))
      : [],
    awaitingField: typeof order?.awaitingField === 'string' ? order.awaitingField : null,
    automationEnabled: Boolean(order?.automationEnabled),
    status: typeof order?.status === 'string' ? order.status : defaults.status,
    stats: order?.stats && typeof order.stats === 'object' ? order.stats : {},
    lastCheckedAt: typeof order?.lastCheckedAt === 'string' ? order.lastCheckedAt : null,
    lastAlertAt: typeof order?.lastAlertAt === 'string' ? order.lastAlertAt : null,
    lastChangeAt: typeof order?.lastChangeAt === 'string' ? order.lastChangeAt : null,
    lastError: typeof order?.lastError === 'string' ? order.lastError : null,
    createdAt: typeof order?.createdAt === 'string' ? order.createdAt : defaults.createdAt,
    updatedAt: typeof order?.updatedAt === 'string' ? order.updatedAt : defaults.updatedAt,
    deleteConfirmations: Number.isInteger(order?.deleteConfirmations) ? order.deleteConfirmations : 0,
    archivedAt: typeof order?.archivedAt === 'string' ? order.archivedAt : null,
  };
}

function normalizeCommunityVisions(user = {}) {
  const candidates = Array.isArray(user.communityVisions) ? user.communityVisions : [];
  return candidates.map((order) => normalizeCommunityVision(order));
}

function normalizeWalletTracker(order = {}) {
  const defaults = createDefaultWalletTracker();
  const buyMode = ['off', 'first', 'every'].includes(order?.buyMode) ? order.buyMode : defaults.buyMode;
  return {
    ...defaults,
    ...(order ?? {}),
    id: typeof order?.id === 'string' && order.id ? order.id : defaults.id,
    walletAddress: typeof order?.walletAddress === 'string' ? order.walletAddress : null,
    buyMode,
    notifySells: typeof order?.notifySells === 'boolean' ? order.notifySells : defaults.notifySells,
    notifyLaunches: typeof order?.notifyLaunches === 'boolean' ? order.notifyLaunches : defaults.notifyLaunches,
    awaitingField: typeof order?.awaitingField === 'string' ? order.awaitingField : null,
    automationEnabled: Boolean(order?.automationEnabled),
    status: typeof order?.status === 'string' ? order.status : defaults.status,
    stats: order?.stats && typeof order.stats === 'object' ? order.stats : {},
    notifiedBuyMints: Array.isArray(order?.notifiedBuyMints)
      ? order.notifiedBuyMints.filter((item) => typeof item === 'string').slice(0, 100)
      : [],
    lastSeenSignature: typeof order?.lastSeenSignature === 'string' ? order.lastSeenSignature : null,
    lastCheckedAt: typeof order?.lastCheckedAt === 'string' ? order.lastCheckedAt : null,
    lastAlertAt: typeof order?.lastAlertAt === 'string' ? order.lastAlertAt : null,
    lastEventAt: typeof order?.lastEventAt === 'string' ? order.lastEventAt : null,
    lastError: typeof order?.lastError === 'string' ? order.lastError : null,
    createdAt: typeof order?.createdAt === 'string' ? order.createdAt : defaults.createdAt,
    updatedAt: typeof order?.updatedAt === 'string' ? order.updatedAt : defaults.updatedAt,
    deleteConfirmations: Number.isInteger(order?.deleteConfirmations) ? order.deleteConfirmations : 0,
    archivedAt: typeof order?.archivedAt === 'string' ? order.archivedAt : null,
  };
}

function normalizeWalletTrackers(user = {}) {
  const candidates = Array.isArray(user.walletTrackers) ? user.walletTrackers : [];
  return candidates.map((order) => normalizeWalletTracker(order));
}

function normalizeLaunchBuyBuyerWallet(wallet = {}) {
  return {
    ...createLaunchBuyBuyerWallet(),
    ...(wallet ?? {}),
    label: typeof wallet?.label === 'string' ? wallet.label : 'Buyer Wallet',
    address: typeof wallet?.address === 'string' ? wallet.address : null,
    secretKeyB64: typeof wallet?.secretKeyB64 === 'string' ? wallet.secretKeyB64 : null,
    secretKeyBase58: typeof wallet?.secretKeyBase58 === 'string' ? wallet.secretKeyBase58 : null,
    imported: Boolean(wallet?.imported),
    currentLamports: Number.isInteger(wallet?.currentLamports) ? wallet.currentLamports : 0,
    currentSol: typeof wallet?.currentSol === 'string' ? wallet.currentSol : '0',
  };
}

function estimateLaunchBuyBuyerReserveLamports(buyerWalletCount) {
  const safeBuyerCount = Number.isInteger(buyerWalletCount)
    ? Math.max(0, Math.min(LAUNCH_BUY_MAX_WALLET_COUNT, buyerWalletCount))
    : 0;
  return safeBuyerCount * LAUNCH_BUY_BUYER_RESERVE_LAMPORTS;
}

function normalizeLaunchBuy(order = {}) {
  const defaults = createDefaultLaunchBuy();
  const buyerWalletCount = Number.isInteger(order?.buyerWalletCount)
    ? Math.min(LAUNCH_BUY_MAX_WALLET_COUNT, Math.max(LAUNCH_BUY_MIN_WALLET_COUNT, order.buyerWalletCount))
    : defaults.buyerWalletCount;
  const buyerWallets = Array.isArray(order?.buyerWallets) && order.buyerWallets.length > 0
    ? order.buyerWallets.map((wallet) => normalizeLaunchBuyBuyerWallet(wallet)).slice(0, buyerWalletCount)
    : createLaunchBuyBuyerWallets(buyerWalletCount);
  while (buyerWallets.length < buyerWalletCount) {
    buyerWallets.push(createLaunchBuyBuyerWallet());
  }
  const totalBuyLamports = Number.isInteger(order?.totalBuyLamports)
    ? order.totalBuyLamports
    : (typeof order?.totalBuySol === 'string' ? parseSolToLamports(order.totalBuySol) : null);
  const jitoTipLamports = Number.isInteger(order?.jitoTipLamports)
    ? order.jitoTipLamports
    : (typeof order?.jitoTipSol === 'string' ? parseSolToLamports(order.jitoTipSol) : defaults.jitoTipLamports);
  const launchMode = order?.launchMode === 'magic' ? 'magic' : 'normal';
  const setupFeeLamports = launchMode === 'magic'
    ? LAUNCH_BUY_MAGIC_SETUP_FEE_LAMPORTS
    : LAUNCH_BUY_NORMAL_SETUP_FEE_LAMPORTS;
  const routingFeeLamports = launchMode === 'magic' && totalBuyLamports
    ? Math.floor(totalBuyLamports * (cfg.magicBundleSplitNowFeeEstimateBps / 10_000))
    : 0;
  const buyerReserveLamports = estimateLaunchBuyBuyerReserveLamports(buyerWalletCount);
  return {
    ...defaults,
    ...(order ?? {}),
    id: typeof order?.id === 'string' ? order.id : defaults.id,
    launchMode,
    tokenName: typeof order?.tokenName === 'string' ? order.tokenName : null,
    symbol: typeof order?.symbol === 'string' ? order.symbol : null,
    description: typeof order?.description === 'string' ? order.description : null,
    website: typeof order?.website === 'string' ? order.website : null,
    telegram: typeof order?.telegram === 'string' ? order.telegram : null,
    twitter: typeof order?.twitter === 'string' ? order.twitter : null,
    logoPath: typeof order?.logoPath === 'string' ? order.logoPath : null,
    logoFileName: typeof order?.logoFileName === 'string' ? order.logoFileName : null,
    logoUploadedAt: typeof order?.logoUploadedAt === 'string' ? order.logoUploadedAt : null,
    walletSource: order?.walletSource === 'imported' ? 'imported' : 'generated',
    buyerWalletCount,
    buyerWallets,
    walletAddress: typeof order?.walletAddress === 'string' ? order.walletAddress : defaults.walletAddress,
    walletSecretKeyB64: typeof order?.walletSecretKeyB64 === 'string' ? order.walletSecretKeyB64 : defaults.walletSecretKeyB64,
    walletSecretKeyBase58: typeof order?.walletSecretKeyBase58 === 'string' ? order.walletSecretKeyBase58 : defaults.walletSecretKeyBase58,
    privateKeyVisible: Boolean(order?.privateKeyVisible),
    currentLamports: Number.isInteger(order?.currentLamports) ? order.currentLamports : 0,
    currentSol: typeof order?.currentSol === 'string' ? order.currentSol : '0',
    totalBuyLamports,
    totalBuySol: Number.isInteger(totalBuyLamports) ? formatSolAmountFromLamports(totalBuyLamports) : null,
    jitoTipLamports,
    jitoTipSol: formatSolAmountFromLamports(jitoTipLamports),
    estimatedSetupFeeLamports: setupFeeLamports,
    estimatedRoutingFeeLamports: routingFeeLamports,
    estimatedTotalNeededLamports: Math.max(
      0,
      setupFeeLamports
        + routingFeeLamports
        + (totalBuyLamports || 0)
        + jitoTipLamports
        + buyerReserveLamports
        + LAUNCH_BUY_LAUNCH_OVERHEAD_LAMPORTS,
    ),
    fundedReady: Boolean(order?.fundedReady),
    awaitingField: typeof order?.awaitingField === 'string' ? order.awaitingField : null,
    status: typeof order?.status === 'string' ? order.status : defaults.status,
    lastError: typeof order?.lastError === 'string' ? order.lastError : null,
    createdAt: typeof order?.createdAt === 'string' ? order.createdAt : defaults.createdAt,
    updatedAt: typeof order?.updatedAt === 'string' ? order.updatedAt : defaults.updatedAt,
    deleteConfirmations: Number.isInteger(order?.deleteConfirmations) ? order.deleteConfirmations : 0,
    archivedAt: typeof order?.archivedAt === 'string' ? order.archivedAt : null,
  };
}

function normalizeLaunchBuys(user = {}) {
  const candidates = Array.isArray(user.launchBuys) ? user.launchBuys : [];
  return candidates.map((order) => normalizeLaunchBuy(order));
}

function normalizeResizer(resizer = {}) {
  const defaults = createDefaultResizer();
  const mode = typeof resizer?.mode === 'string' && RESIZER_PRESETS[resizer.mode]
    ? resizer.mode
    : defaults.mode;
  return {
    ...defaults,
    ...(resizer ?? {}),
    mode,
    awaitingImage: Boolean(resizer?.awaitingImage),
    status: typeof resizer?.status === 'string' ? resizer.status : defaults.status,
    lastCompletedAt: typeof resizer?.lastCompletedAt === 'string' ? resizer.lastCompletedAt : null,
    lastOutputWidth: Number.isInteger(resizer?.lastOutputWidth) ? resizer.lastOutputWidth : null,
    lastOutputHeight: Number.isInteger(resizer?.lastOutputHeight) ? resizer.lastOutputHeight : null,
    lastSourceName: typeof resizer?.lastSourceName === 'string' ? resizer.lastSourceName : null,
    lastError: typeof resizer?.lastError === 'string' ? resizer.lastError : null,
  };
}

function normalizeUserState(userId, user = {}) {
  const defaults = createDefaultUserState(userId);
  const appleBoosters = normalizeAppleBoosters(user);
  const activeAppleBoosterId = typeof user.activeAppleBoosterId === 'string'
    && appleBoosters.some((order) => order.id === user.activeAppleBoosterId)
    ? user.activeAppleBoosterId
    : (appleBoosters.find((order) => !order.archivedAt)?.id
      ?? appleBoosters[0]?.id
      ?? null);
  const activeAppleBooster = appleBoosters.find((order) => order.id === activeAppleBoosterId)
    ?? createDefaultOrganicVolumeOrder();
  const burnAgents = normalizeBurnAgents(user);
  const activeBurnAgentId = typeof user.activeBurnAgentId === 'string'
    && burnAgents.some((agent) => agent.id === user.activeBurnAgentId)
    ? user.activeBurnAgentId
    : (burnAgents.find((agent) => !agent.archivedAt)?.id
      ?? burnAgents[0]?.id
      ?? null);
  const activeBurnAgent = burnAgents.find((agent) => agent.id === activeBurnAgentId)
    ?? createDefaultBurnAgentState();
  const holderBooster = normalizeHolderBooster(user.holderBooster ?? {});
  const fomoBooster = normalizeFomoBooster(user.fomoBooster ?? {});
  const sniperWizard = normalizeSniperWizard(user.sniperWizard ?? {});
  const tradingDesk = normalizeTradingDesk(user.tradingDesk ?? {});
  const magicBundles = normalizeMagicBundles(user);
  const activeMagicBundleId = typeof user.activeMagicBundleId === 'string'
    && magicBundles.some((order) => order.id === user.activeMagicBundleId)
    ? user.activeMagicBundleId
    : (magicBundles.find((order) => !order.archivedAt)?.id
      ?? magicBundles[0]?.id
      ?? null);
  const activeMagicBundle = magicBundles.find((order) => order.id === activeMagicBundleId)
    ?? createDefaultMagicBundle();
  const magicSells = normalizeMagicSells(user);
  const activeMagicSellId = typeof user.activeMagicSellId === 'string'
    && magicSells.some((order) => order.id === user.activeMagicSellId)
    ? user.activeMagicSellId
    : (magicSells.find((order) => !order.archivedAt)?.id
      ?? magicSells[0]?.id
      ?? null);
  const activeMagicSell = magicSells.find((order) => order.id === activeMagicSellId)
    ?? createDefaultMagicSell();
  const launchBuys = normalizeLaunchBuys(user);
  const activeLaunchBuyId = typeof user.activeLaunchBuyId === 'string'
    && launchBuys.some((order) => order.id === user.activeLaunchBuyId)
    ? user.activeLaunchBuyId
    : (launchBuys.find((order) => !order.archivedAt)?.id
      ?? launchBuys[0]?.id
      ?? null);
  const activeLaunchBuy = launchBuys.find((order) => order.id === activeLaunchBuyId)
    ?? createDefaultLaunchBuy();
  const communityVisions = normalizeCommunityVisions(user);
  const activeCommunityVisionId = typeof user.activeCommunityVisionId === 'string'
    && communityVisions.some((order) => order.id === user.activeCommunityVisionId)
    ? user.activeCommunityVisionId
    : (communityVisions.find((order) => !order.archivedAt)?.id
      ?? communityVisions[0]?.id
      ?? null);
  const activeCommunityVision = communityVisions.find((order) => order.id === activeCommunityVisionId)
    ?? createDefaultCommunityVision();
  const walletTrackers = normalizeWalletTrackers(user);
  const activeWalletTrackerId = typeof user.activeWalletTrackerId === 'string'
    && walletTrackers.some((order) => order.id === user.activeWalletTrackerId)
    ? user.activeWalletTrackerId
    : (walletTrackers.find((order) => !order.archivedAt)?.id
      ?? walletTrackers[0]?.id
      ?? null);
  const activeWalletTracker = walletTrackers.find((order) => order.id === activeWalletTrackerId)
    ?? createDefaultWalletTracker();
  const staking = normalizeStakingState(user.staking ?? {});
  const resizer = normalizeResizer(user.resizer ?? {});
  const vanityWallet = normalizeVanityWalletState(user.vanityWallet ?? {});
  const xFollowers = normalizeXFollowersState(user.xFollowers ?? {});

  return {
    ...defaults,
    ...user,
    telegramId: String(userId),
    freeTrialUsed: Boolean(user.freeTrialUsed),
    volumeFreeTrialUsed: Boolean(user.volumeFreeTrialUsed),
    activityLogs: Array.isArray(user.activityLogs)
      ? user.activityLogs
        .filter((entry) => entry && typeof entry === 'object')
        .slice(0, 80)
      : [],
    volumeMode: typeof user.volumeMode === 'string' ? user.volumeMode : null,
    organicVolumePackage: activeAppleBooster.packageKey,
    organicVolumeOrder: activeAppleBooster,
    appleBoosters,
    activeAppleBoosterId,
    holderBooster,
    fomoBooster,
    sniperWizard,
    tradingDesk,
    magicBundles,
    activeMagicBundleId,
    magicBundle: activeMagicBundle,
    magicSells,
    activeMagicSellId,
    magicSell: activeMagicSell,
    launchBuys,
    activeLaunchBuyId,
    launchBuy: activeLaunchBuy,
    communityVisions,
    activeCommunityVisionId,
    communityVision: activeCommunityVision,
    walletTrackers,
    activeWalletTrackerId,
    walletTracker: activeWalletTracker,
    staking,
    vanityWallet,
    resizer,
    xFollowers,
    burnAgents,
    activeBurnAgentId,
    burnAgent: activeBurnAgent,
    awaitingTargetInput: Boolean(user.awaitingTargetInput),
    selection: {
      ...defaults.selection,
      ...(user.selection ?? {}),
      amount: user.selection?.amount ? Number(user.selection.amount) : null,
      usingFreeTrial: Boolean(user.selection?.usingFreeTrial),
    },
    payment: normalizePaymentState(user.payment),
  };
}

function createDefaultStore() {
  return {
    users: {},
    jobs: [],
    processedPaymentSignatures: [],
    worker: {},
  };
}

function getActiveBurnAgent(user) {
  return user.burnAgents.find((agent) => agent.id === user.activeBurnAgentId)
    ?? createDefaultBurnAgentState();
}

function getActiveAppleBooster(user) {
  return user.appleBoosters.find((order) => order.id === user.activeAppleBoosterId)
    ?? createDefaultOrganicVolumeOrder();
}

function getActiveMagicSell(user) {
  return user.magicSells.find((order) => order.id === user.activeMagicSellId)
    ?? createDefaultMagicSell();
}

function getActiveMagicBundle(user) {
  return user.magicBundles.find((order) => order.id === user.activeMagicBundleId)
    ?? createDefaultMagicBundle();
}

function getActiveLaunchBuy(user) {
  return user.launchBuys.find((order) => order.id === user.activeLaunchBuyId)
    ?? createDefaultLaunchBuy();
}

function getActiveCommunityVision(user) {
  return user.communityVisions.find((order) => order.id === user.activeCommunityVisionId)
    ?? createDefaultCommunityVision();
}

function getActiveWalletTracker(user) {
  return user.walletTrackers.find((order) => order.id === user.activeWalletTrackerId)
    ?? createDefaultWalletTracker();
}

function getActiveTradingWallet(user) {
  const wallets = Array.isArray(user?.tradingDesk?.wallets) ? user.tradingDesk.wallets : [];
  return wallets.find((wallet) => wallet.id === user?.tradingDesk?.activeWalletId) ?? null;
}

function getVisibleAppleBoosters(user, { archived = false } = {}) {
  return user.appleBoosters.filter((order) => Boolean(order.archivedAt) === archived);
}

function getVisibleMagicSells(user, { archived = false } = {}) {
  return user.magicSells.filter((order) => Boolean(order.archivedAt) === archived);
}

function getVisibleMagicBundles(user, { archived = false } = {}) {
  return user.magicBundles.filter((order) => Boolean(order.archivedAt) === archived);
}

function getVisibleLaunchBuys(user, { archived = false } = {}) {
  return user.launchBuys.filter((order) => Boolean(order.archivedAt) === archived);
}

function getVisibleCommunityVisions(user, { archived = false } = {}) {
  return user.communityVisions.filter((order) => Boolean(order.archivedAt) === archived);
}

function getVisibleWalletTrackers(user, { archived = false } = {}) {
  return user.walletTrackers.filter((order) => Boolean(order.archivedAt) === archived);
}

function getVisibleAppleBoostersByStrategy(user, strategy, { archived = false } = {}) {
  return getVisibleAppleBoosters(user, { archived }).filter((order) => (order.strategy || 'organic') === strategy);
}

function syncActiveAppleBoosterDraft(draft) {
  draft.organicVolumeOrder = getActiveAppleBooster(draft);
  draft.organicVolumePackage = draft.organicVolumeOrder.packageKey;
}

function updateAppleBoosterInDraft(draft, boosterId, updater) {
  draft.appleBoosters = draft.appleBoosters.map((order) => (
    order.id === boosterId ? normalizeOrganicVolumeOrder(updater(structuredClone(order))) : order
  ));
  syncActiveAppleBoosterDraft(draft);
}

function appendAppleBoosterToDraft(draft, order) {
  const normalized = normalizeOrganicVolumeOrder(order);
  draft.appleBoosters = [...draft.appleBoosters, normalized];
  draft.activeAppleBoosterId = normalized.id;
  syncActiveAppleBoosterDraft(draft);
}

function appleBoosterScope(boosterId) {
  return `organic_order:${boosterId}`;
}

function holderBoosterScope(holderBoosterId) {
  return `holder_booster:${holderBoosterId}`;
}

function syncActiveMagicBundleDraft(draft) {
  draft.magicBundle = getActiveMagicBundle(draft);
}

function syncTradingDeskDraft(draft) {
  draft.tradingDesk = normalizeTradingDesk(draft.tradingDesk);
}

function updateMagicBundleInDraft(draft, magicBundleId, updater) {
  draft.magicBundles = draft.magicBundles.map((order) => (
    order.id === magicBundleId ? normalizeMagicBundle(updater(structuredClone(order))) : order
  ));
  syncActiveMagicBundleDraft(draft);
}

function appendMagicBundleToDraft(draft, order) {
  const normalized = normalizeMagicBundle(order);
  draft.magicBundles = [...draft.magicBundles, normalized];
  draft.activeMagicBundleId = normalized.id;
  syncActiveMagicBundleDraft(draft);
}

function magicBundleScope(magicBundleId) {
  return `magic_bundle:${magicBundleId}`;
}

function syncActiveCommunityVisionDraft(draft) {
  draft.communityVision = getActiveCommunityVision(draft);
}

function updateCommunityVisionInDraft(draft, communityVisionId, updater) {
  draft.communityVisions = draft.communityVisions.map((order) => (
    order.id === communityVisionId ? normalizeCommunityVision(updater(structuredClone(order))) : order
  ));
  syncActiveCommunityVisionDraft(draft);
}

function appendCommunityVisionToDraft(draft, order) {
  const normalized = normalizeCommunityVision(order);
  draft.communityVisions = [...draft.communityVisions, normalized];
  draft.activeCommunityVisionId = normalized.id;
  syncActiveCommunityVisionDraft(draft);
}

function launchBuyScope(launchBuyId) {
  return `launch_buy:${launchBuyId}`;
}

function syncActiveLaunchBuyDraft(draft) {
  draft.launchBuy = getActiveLaunchBuy(draft);
}

function updateLaunchBuyInDraft(draft, launchBuyId, updater) {
  draft.launchBuys = draft.launchBuys.map((order) => (
    order.id === launchBuyId ? normalizeLaunchBuy(updater(structuredClone(order))) : order
  ));
  syncActiveLaunchBuyDraft(draft);
  const nextOrder = draft.launchBuys.find((order) => order.id === launchBuyId);
  if (nextOrder) {
    syncTradingDeskWalletsFromSource(
      draft,
      'launch_buy',
      launchBuyId,
      nextOrder.archivedAt ? [] : createLaunchBuyTradingWallets(nextOrder),
    );
  }
}

function appendLaunchBuyToDraft(draft, order) {
  const normalized = normalizeLaunchBuy(order);
  draft.launchBuys = [...draft.launchBuys, normalized];
  draft.activeLaunchBuyId = normalized.id;
  syncActiveLaunchBuyDraft(draft);
  syncTradingDeskWalletsFromSource(draft, 'launch_buy', normalized.id, createLaunchBuyTradingWallets(normalized));
}

function communityVisionScope(communityVisionId) {
  return `community_vision:${communityVisionId}`;
}

function syncActiveMagicSellDraft(draft) {
  draft.magicSell = getActiveMagicSell(draft);
}

function updateMagicSellInDraft(draft, magicSellId, updater) {
  draft.magicSells = draft.magicSells.map((order) => (
    order.id === magicSellId ? normalizeMagicSell(updater(structuredClone(order))) : order
  ));
  syncActiveMagicSellDraft(draft);
}

function appendMagicSellToDraft(draft, order) {
  const normalized = normalizeMagicSell(order);
  draft.magicSells = [...draft.magicSells, normalized];
  draft.activeMagicSellId = normalized.id;
  syncActiveMagicSellDraft(draft);
}

function magicSellScope(magicSellId) {
  return `magic_sell:${magicSellId}`;
}

function syncActiveWalletTrackerDraft(draft) {
  draft.walletTracker = getActiveWalletTracker(draft);
}

function updateWalletTrackerInDraft(draft, walletTrackerId, updater) {
  draft.walletTrackers = draft.walletTrackers.map((order) => (
    order.id === walletTrackerId ? normalizeWalletTracker(updater(structuredClone(order))) : order
  ));
  syncActiveWalletTrackerDraft(draft);
}

function appendWalletTrackerToDraft(draft, order) {
  const normalized = normalizeWalletTracker(order);
  draft.walletTrackers = [...draft.walletTrackers, normalized];
  draft.activeWalletTrackerId = normalized.id;
  syncActiveWalletTrackerDraft(draft);
}

function walletTrackerScope(walletTrackerId) {
  return `wallet_tracker:${walletTrackerId}`;
}

function fomoBoosterScope(fomoBoosterId) {
  return `fomo_booster:${fomoBoosterId}`;
}

function sniperWizardScope(sniperWizardId) {
  return `sniper_wizard:${sniperWizardId}`;
}

function getVisibleBurnAgents(user, { archived = false } = {}) {
  return user.burnAgents.filter((agent) => Boolean(agent.archivedAt) === archived);
}

function updateBurnAgentInDraft(draft, agentId, updater) {
  draft.burnAgents = draft.burnAgents.map((agent) => (
    agent.id === agentId ? normalizeBurnAgent(updater(structuredClone(agent))) : agent
  ));
  draft.burnAgent = getActiveBurnAgent(draft);
}

function appendBurnAgentToDraft(draft, agent) {
  draft.burnAgents = [...draft.burnAgents, normalizeBurnAgent(agent)];
  draft.activeBurnAgentId = agent.id;
  draft.burnAgent = getActiveBurnAgent(draft);
}

function normalizeStore(store = {}) {
  return {
    users: store.users ?? {},
    jobs: Array.isArray(store.jobs) ? store.jobs : [],
    processedPaymentSignatures: Array.isArray(store.processedPaymentSignatures)
      ? store.processedPaymentSignatures
      : [],
    worker: store.worker && typeof store.worker === 'object' ? store.worker : {},
  };
}

async function ensureStore() {
  await fs.mkdir(DATA_DIR, { recursive: true });
  try {
    await fs.access(STORE_PATH);
  } catch {
    await fs.writeFile(
      STORE_PATH,
      JSON.stringify(createDefaultStore(), null, 2),
      'utf8',
    );
  }
}

async function readStore() {
  await ensureStore();
  const raw = await fs.readFile(STORE_PATH, 'utf8');
  return normalizeStore(JSON.parse(raw));
}

async function writeStore(store) {
  await ensureStore();
  await fs.writeFile(STORE_PATH, JSON.stringify(normalizeStore(store), null, 2), 'utf8');
}

async function appendUserActivityLog(userId, entry) {
  const store = await readStore();
  const user = normalizeUserState(userId, store.users[userId]);
  user.activityLogs = [
    {
      at: new Date().toISOString(),
      level: 'info',
      scope: 'general',
      ...entry,
    },
    ...(Array.isArray(user.activityLogs) ? user.activityLogs : []),
  ].slice(0, 80);
  store.users[userId] = user;
  await writeStore(store);
}

async function getUserState(userId) {
  const store = await readStore();
  return normalizeUserState(userId, store.users[userId]);
}

async function updateUserState(userId, updater) {
  const store = await readStore();
  const current = normalizeUserState(userId, store.users[userId]);
  const next = normalizeUserState(userId, updater(structuredClone(current)));
  store.users[userId] = next;
  await writeStore(store);
  return next;
}

async function appendJob(job) {
  const store = await readStore();
  store.jobs.unshift(job);
  store.jobs = store.jobs.slice(0, 200);
  await writeStore(store);
}

function formatUsd(amount) {
  return `$${amount.toFixed(2)}`;
}

function formatSolAmountFromLamports(lamports) {
  if (!Number.isInteger(lamports)) {
    return 'N/A';
  }
  return (lamports / LAMPORTS_PER_SOL).toFixed(9);
}

function formatApproxSol(usdAmount, solUsdRate = solUsdRateCache) {
  if (!Number.isFinite(solUsdRate) || solUsdRate <= 0) {
    return 'calculating...';
  }

  return `${(usdAmount / solUsdRate).toFixed(4)} SOL`;
}

function parseOrganicPackageTargetUsd(label) {
  const raw = String(label || '').trim().toLowerCase();
  const match = raw.match(/^(\d+(?:\.\d+)?)(k)?$/);
  if (!match) {
    return null;
  }

  const base = Number(match[1]);
  if (!Number.isFinite(base) || base <= 0) {
    return null;
  }

  return match[2] ? Math.round(base * 1000) : Math.round(base);
}

function formatUsdCompact(amount) {
  const numeric = Number(amount);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return '$0';
  }

  if (numeric >= 1000) {
    return `$${Math.round(numeric).toLocaleString('en-US')}`;
  }

  return `$${numeric.toFixed(2).replace(/0+$/, '').replace(/\.$/, '')}`;
}

function formatProgressBar(percent, width = 12) {
  if (!Number.isFinite(percent)) {
    return '[------------]';
  }

  const clamped = Math.max(0, Math.min(100, percent));
  const filled = Math.round((clamped / 100) * width);
  return `[${'#'.repeat(filled)}${'-'.repeat(Math.max(0, width - filled))}]`;
}

function bundleSolDisplay(amount, user = null) {
  if (
    user &&
    user.payment?.bundleAmount === amount &&
    user.payment?.solAmount &&
    !user.selection.usingFreeTrial
  ) {
    return `${user.payment.solAmount} SOL`;
  }

  const bundle = getBundlePricing(amount);
  if (!bundle) {
    return 'SOL pending';
  }

  return `~${formatApproxSol(bundle.usdPrice)}`;
}

function formatTimestamp(iso) {
  if (!iso) return 'N/A';
  const value = new Date(iso);
  if (Number.isNaN(value.getTime())) return 'N/A';
  return value.toLocaleString('en-US', {
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
    hour12: true,
    timeZoneName: 'short',
  });
}

function escapeHtml(value) {
  return String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;');
}

function escapeMarkdown(value) {
  return String(value || '').replace(/([_*\[\]()~`>#+\-=|{}.!\\])/g, '\\$1');
}

function summarizeTarget(url) {
  try {
    const parsed = new URL(url);
    const summary = `${parsed.hostname}${parsed.pathname === '/' ? '' : parsed.pathname}`;
    return summary.length > 64 ? `${summary.slice(0, 61)}...` : summary;
  } catch {
    return url;
  }
}

function paymentAnnouncementKey(user) {
  return [
    user.payment.quoteId || 'no-quote',
    user.payment.matchedSignature || 'no-signature',
    user.selection.amount || 'no-amount',
    user.selection.target || 'no-target',
  ].join(':');
}

function getXFollowersPackage(packageKey) {
  return typeof packageKey === 'string' ? X_FOLLOWER_PACKAGES[packageKey] ?? null : null;
}

function xFollowersAnnouncementKey(user) {
  return [
    user.xFollowers?.payment?.quoteId || 'no-quote',
    user.xFollowers?.payment?.matchedSignature || 'no-signature',
    user.xFollowers?.packageKey || 'no-package',
    user.xFollowers?.target || 'no-target',
  ].join(':');
}

function xFollowersHasActiveQuote(user) {
  return Boolean(user.xFollowers?.payment?.quoteId && user.xFollowers?.payment?.solAmount);
}

function xFollowersNeedsChecking(user) {
  return Boolean(
    user.xFollowers?.packageKey
    && user.xFollowers?.payment?.quoteId
    && [PAYMENT_STATES.PENDING, PAYMENT_STATES.EXPIRED].includes(user.xFollowers.payment.status),
  );
}

function xFollowersPaymentIsReady(user) {
  return Boolean(
    user.xFollowers?.packageKey
    && user.xFollowers?.payment?.quoteId
    && user.xFollowers?.payment?.status === PAYMENT_STATES.PAID,
  );
}

function xFollowersStatusLabel(user) {
  if (!user.xFollowers?.packageKey) return 'No package selected';
  if (xFollowersPaymentIsReady(user)) return 'Payment confirmed';
  if (quoteExpired(user.xFollowers.payment)) return 'Quote expired';
  if (user.xFollowers.payment?.status === PAYMENT_STATES.PENDING) return 'Waiting for payment';
  return 'Ready';
}

function vanityWalletHasActiveQuote(user) {
  return Boolean(user.vanityWallet?.payment?.quoteId && user.vanityWallet?.payment?.solAmount);
}

function vanityWalletNeedsChecking(user) {
  return Boolean(
    user.vanityWallet?.patternMode
    && user.vanityWallet?.pattern
    && user.vanityWallet?.payment?.quoteId
    && [PAYMENT_STATES.PENDING, PAYMENT_STATES.EXPIRED].includes(user.vanityWallet.payment.status),
  );
}

function vanityWalletPaymentIsReady(user) {
  return Boolean(
    user.vanityWallet?.patternMode
    && user.vanityWallet?.pattern
    && user.vanityWallet?.payment?.quoteId
    && user.vanityWallet.payment.status === PAYMENT_STATES.PAID,
  );
}

function vanityWalletStatusLabel(user) {
  const state = normalizeVanityWalletState(user.vanityWallet);
  switch (state.status) {
    case 'generating':
      return 'Generating wallet';
    case 'completed':
      return 'Wallet ready';
    case 'paid':
      return 'Payment confirmed';
    case 'pending_payment':
      return quoteExpired(state.payment) ? 'Quote expired' : 'Waiting for payment';
    case 'failed':
      return 'Generation failed';
    default:
      return state.patternMode && state.pattern ? 'Ready' : 'Setup';
  }
}

function vanityWalletPatternSummary(state) {
  if (!state?.patternMode || !state?.pattern) {
    return 'Not set';
  }

  return `${state.patternMode === 'suffix' ? 'Ends with' : 'Starts with'} \`${state.pattern}\``;
}

function normalizeVanityPatternInput(value) {
  const raw = String(value || '').trim();
  if (!raw) {
    throw new Error('Send a vanity pattern first.');
  }

  if (raw.length > VANITY_WALLET_MAX_PATTERN_LENGTH) {
    throw new Error(`Vanity patterns are capped at ${VANITY_WALLET_MAX_PATTERN_LENGTH} characters on the shared Render stack.`);
  }

  for (const character of raw) {
    if (!BASE58_LOWERCASE.has(character.toLowerCase())) {
      throw new Error('Vanity patterns can only use Solana base58 characters.');
    }
  }

  return raw;
}

function vanityWalletMatches(address, mode, pattern) {
  const addressValue = String(address || '').toLowerCase();
  const patternValue = String(pattern || '').toLowerCase();
  if (!addressValue || !patternValue) {
    return false;
  }

  return mode === 'suffix'
    ? addressValue.endsWith(patternValue)
    : addressValue.startsWith(patternValue);
}

function xFollowersTargetSummary(target) {
  if (!target) return 'Not set';
  return target.length > 48 ? `${target.slice(0, 45)}...` : target;
}

function xFollowersPackageLabel(pkg) {
  if (!pkg) return 'Not selected';
  return `${pkg.followers.toLocaleString()} ${pkg.promise}`;
}

function purchasePriceDisplay(user) {
  if (user.payment.solAmount) {
    return `${user.payment.solAmount} SOL`;
  }

  if (user.selection.amount) {
    return bundleSolDisplay(user.selection.amount, user);
  }

  return 'SOL pending';
}

function getScreenMediaPath(route) {
  switch (route) {
    case 'home':
      return MENU_HOME_IMAGE_PATH;
    case 'start':
      return MENU_EMOJI_IMAGE_PATH;
    case 'burn_agent':
    case 'amount':
    case 'payment':
    case 'confirm':
    case 'target':
    case 'status':
    case 'help':
    default:
      return MENU_LOGO_IMAGE_PATH;
  }
}

function isMessageNotModifiedError(error) {
  const message = String(error?.description || error?.message || error || '');
  return message.toLowerCase().includes('message is not modified');
}

function amountLabel(amount) {
  return amount ? `${amount} apples` : 'Not selected';
}

function burnAgentSpeedLabel(speed) {
  if (speed === 'lightning') {
    return 'Lightning Fast';
  }

  if (speed === 'normal') {
    return 'Normal';
  }

  return 'Not selected';
}

function burnAgentWalletModeLabel(walletMode) {
  if (walletMode === 'generated') {
    return 'Generated wallet + private key';
  }

  if (walletMode === 'provided') {
    return 'Your provided creator wallet';
  }

  if (walletMode === 'managed') {
    return 'Managed reward wallet';
  }

  return 'Not selected';
}

function isNormalBurnAgent(agent) {
  return agent?.speed === 'normal';
}

function isArchivedBurnAgent(agent) {
  return Boolean(agent?.archivedAt);
}

function burnAgentHasStoredPrivateKey(agent) {
  return Boolean(agent?.walletSecretKeyB64 && agent?.walletSecretKeyBase58);
}

function burnAgentPrivateKeyText(agent) {
  if (!burnAgentHasStoredPrivateKey(agent)) {
    return 'Not stored';
  }

  if (agent.privateKeyVisible) {
    return `\`${agent.walletSecretKeyBase58}\``;
  }

  return '`Hidden - tap Show Private Key to reveal it.`';
}

function burnAgentNeedsWalletChoice(user) {
  return user.burnAgent.speed === 'lightning' && !user.burnAgent.walletMode;
}

function burnAgentSplitReady(agent) {
  if (isNormalBurnAgent(agent)) {
    return true;
  }

  return Number.isInteger(agent.burnPercent)
    && Number.isInteger(agent.treasuryPercent)
    && agent.burnPercent >= 0
    && agent.treasuryPercent >= 0
    && agent.burnPercent + agent.treasuryPercent === 100;
}

function burnAgentIsReady(agent) {
  return Boolean(
    agent.speed
    && agent.walletMode
    && agent.walletAddress
    && agent.walletSecretKeyB64
    && agent.mintAddress
    && (isNormalBurnAgent(agent) || agent.treasuryAddress)
    && burnAgentSplitReady(agent),
  );
}

function burnAgentPromptLabel(field) {
  switch (field) {
    case 'private_key':
      return 'Private key needed';
    case 'token_name':
      return 'Token name needed';
    case 'mint':
      return 'Mint needed';
    case 'treasury':
      return 'Treasury address needed';
    case 'burn_percent':
      return 'Burn % needed';
    case 'treasury_percent':
      return 'Treasury % needed';
    case 'withdraw_address':
      return 'Withdraw destination needed';
    default:
      return 'Waiting';
  }
}

function trialIsAvailable(user) {
  return !user.freeTrialUsed;
}

function buttonDisplay(buttonKey) {
  if (!buttonKey || !BUTTONS[buttonKey]) {
    return 'Not selected';
  }

  const button = BUTTONS[buttonKey];
  return `${button.emoji} ${button.label}`;
}

function selectedButtonEmoji(user) {
  return user?.selection?.button && BUTTONS[user.selection.button]
    ? BUTTONS[user.selection.button].emoji
    : 'ÃƒÂ¢Ã¢â‚¬â€Ã…Â½';
}

function quoteExpired(payment) {
  if (!payment.quoteExpiresAt || payment.status === PAYMENT_STATES.PAID) {
    return false;
  }
  return new Date(payment.quoteExpiresAt).getTime() <= Date.now();
}

function paymentMatchesSelection(user) {
  return Boolean(user.selection.amount && user.payment.bundleAmount === user.selection.amount);
}

function hasLaunchAccess(user) {
  return isAdminUser(user.telegramId) || paymentIsReady(user);
}

function paymentIsReady(user) {
  if (user.selection.usingFreeTrial) {
    return trialIsAvailable(user) && user.selection.amount === cfg.freeTrialAmount;
  }

  return Boolean(
    paymentMatchesSelection(user) &&
    user.payment.status === PAYMENT_STATES.PAID,
  );
}

function hasActiveQuote(user) {
  return Boolean(
    paymentMatchesSelection(user) &&
    user.payment.quoteId &&
    [PAYMENT_STATES.PENDING, PAYMENT_STATES.EXPIRED, PAYMENT_STATES.PAID].includes(user.payment.status),
  );
}

function paymentNeedsChecking(user) {
  return Boolean(
    paymentMatchesSelection(user) &&
    user.payment.quoteId &&
    [PAYMENT_STATES.PENDING, PAYMENT_STATES.EXPIRED].includes(user.payment.status),
  );
}

function paymentStatusLabel(user) {
  if (!user.selection.amount) return 'No bundle selected';
  if (isAdminUser(user.telegramId)) return 'Support access';
  if (user.selection.usingFreeTrial) return trialIsAvailable(user) ? 'Trial unlocked' : 'Trial used';
  if (paymentIsReady(user)) return 'Payment confirmed';
  if (!cfg.solanaReceiveAddress) return 'Wallet unavailable';
  if (!hasActiveQuote(user)) return 'Quote needed';
  if (quoteExpired(user.payment)) return 'Quote expired';
  return 'Waiting for SOL';
}

function readinessLabel(user) {
  if (!user.selection.button || !user.selection.amount) {
    return 'Complete setup';
  }

  return hasLaunchAccess(user) ? 'Ready to launch' : 'Payment required';
}

function isAdminUser(userId) {
  return cfg.adminIds.has(String(userId));
}

function selectionSnapshot(user) {
  const bundle = getBundlePricing(user.selection.amount);
  const lines = [
    `ÃƒÂ°Ã…Â¸Ã…Â½Ã¢â‚¬ÂºÃƒÂ¯Ã‚Â¸Ã‚Â Profile: ${buttonDisplay(user.selection.button)}`,
    `ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‚Â¦ Bundle: ${amountLabel(user.selection.amount)}`,
    `ÃƒÂ°Ã…Â¸Ã…Â½Ã‚Â¯ Target: \`${user.selection.target}\``,
  ];

  if (user.selection.usingFreeTrial) {
    lines.push(`ÃƒÂ°Ã…Â¸Ã¢â‚¬â„¢Ã‚Â¸ Price: Free trial`);
    lines.push(`ÃƒÂ°Ã…Â¸Ã‚ÂÃ‚Â·ÃƒÂ¯Ã‚Â¸Ã‚Â Access: one-time x${cfg.freeTrialAmount} test`);
  } else if (isAdminUser(user.telegramId)) {
    lines.push(`ÃƒÂ°Ã…Â¸Ã¢â‚¬â„¢Ã‚Â¸ Price: Admin free`);
    lines.push(`ÃƒÂ°Ã…Â¸Ã‚ÂÃ‚Â·ÃƒÂ¯Ã‚Â¸Ã‚Â Access: unlimited admin launch`);
  } else if (bundle) {
    lines.push(`ÃƒÂ¢Ã¢â‚¬â€Ã…Â½ Approx Price: \`${bundleSolDisplay(bundle.amount, user)}\``);
    lines.push(`ÃƒÂ°Ã…Â¸Ã‚ÂÃ‚Â·ÃƒÂ¯Ã‚Â¸Ã‚Â Tier: ${bundle.role}`);
  }

  lines.push(`ÃƒÂ°Ã…Â¸Ã¢â‚¬â„¢Ã‚Â³ Checkout: ${paymentStatusLabel(user)}`);
  lines.push(`ÃƒÂ°Ã…Â¸Ã…Â¡Ã‚Â¦ Run State: ${readinessLabel(user)}`);
  return lines.join('\n');
}

function paymentDetailsLines(user) {
  if (!hasActiveQuote(user)) {
    if (user.payment.lastError) {
      return ['No active quote yet.', `Last error: \`${user.payment.lastError}\``];
    }
    return ['No active quote yet.'];
  }

  const payment = user.payment;
  const lines = [
    `ÃƒÂ¢Ã¢â‚¬â€Ã…Â½ Indicative bundle price: \`${formatApproxSol(payment.usdAmount, payment.solUsdRate)}\``,
    `ÃƒÂ¢Ã¢â‚¬â€Ã…Â½ Send exactly: \`${payment.solAmount} SOL\``,
    `ÃƒÂ°Ã…Â¸Ã¢â‚¬â„¢Ã‚Â¼ Receive wallet: \`${payment.address}\``,
    `ÃƒÂ°Ã…Â¸Ã¢â‚¬Â¢Ã¢â‚¬Å“ Quote created: ${formatTimestamp(payment.quoteCreatedAt)}`,
    `ÃƒÂ¢Ã‚ÂÃ‚Â³ Quote expires: ${formatTimestamp(payment.quoteExpiresAt)}`,
  ];

  if (payment.matchedSignature) {
    lines.push(`ÃƒÂ¢Ã…â€œÃ¢â‚¬Â¦ Matched tx: \`${payment.matchedSignature}\``);
  }

  if (payment.lastError) {
    lines.push(`ÃƒÂ¢Ã…Â¡Ã‚Â ÃƒÂ¯Ã‚Â¸Ã‚Â Last error: \`${payment.lastError}\``);
  }

  return lines;
}

function makeHomeKeyboard(user) {
  return new InlineKeyboard()
    .text('ÃƒÂ°Ã…Â¸Ã…Â¡Ã¢â€šÂ¬ Reaction Booster', 'entry:reaction')
    .text('ÃƒÂ°Ã…Â¸Ã‚ÂÃ…Â½ Apple Booster', 'nav:volume')
    .row()
    .text('ÃƒÂ°Ã…Â¸Ã‚Â¤Ã¢â‚¬â€œ Burn Agent', 'nav:burn_agent')
    .text('ÃƒÂ°Ã…Â¸Ã¢â‚¬ËœÃ‚Â¥ Holder Booster', 'nav:holder_booster')
    .row()
    .text('ÃƒÂ¢Ã…â€œÃ‚Â¨ Magic Sell', 'nav:magic_sell')
    .row()
    .text('ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã…Â  Status', 'nav:status')
    .text('ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¹ÃƒÂ¯Ã‚Â¸Ã‚Â Info', 'nav:help')
    .row()
    .url('ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂºÃ…Â¸ Help', `https://t.me/${SUPPORT_USERNAME}`)
    .text('ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬Å¾ Refresh', 'refresh:home');
}

function makeButtonKeyboard(selectedButton, user) {
  const keyboard = new InlineKeyboard();
  for (const button of Object.values(BUTTONS)) {
    const label = selectedButton === button.key ? `ÃƒÂ¢Ã…â€œÃ¢â‚¬Â¦ ${button.emoji} ${button.label}` : `${button.emoji} ${button.label}`;
    keyboard.text(label, `button:${button.key}`);
  }
  keyboard.row().text('ÃƒÂ¢Ã‚Â¬Ã¢â‚¬Â¦ÃƒÂ¯Ã‚Â¸Ã‚Â Back', startBackRoute(user));
  keyboard.text('ÃƒÂ°Ã…Â¸Ã‚ÂÃ‚Â  Home', 'nav:home');
  keyboard.row().text('ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬Å¾ Refresh', 'refresh:start');
  return keyboard;
}

function makeAmountKeyboard(selectedAmount, freeTrialUsed, usingFreeTrial, user) {
  const keyboard = new InlineKeyboard();
  const reactionEmoji = selectedButtonEmoji(user);

  if (!freeTrialUsed || usingFreeTrial) {
    keyboard.text(
      usingFreeTrial ? `ÃƒÂ¢Ã…â€œÃ¢â‚¬Â¦ ${reactionEmoji} Trial x${cfg.freeTrialAmount}` : `${reactionEmoji} Trial x${cfg.freeTrialAmount}`,
      `amount:${cfg.freeTrialAmount}:trial`,
    );
    keyboard.row();
  }

  cfg.packageAmounts.forEach((amount, index) => {
    const label = selectedAmount === amount
      ? `ÃƒÂ¢Ã…â€œÃ¢â‚¬Â¦ ${reactionEmoji} ${amount} ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ ${bundleSolDisplay(amount, user)}`
      : `${reactionEmoji} ${amount} ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ ${bundleSolDisplay(amount, user)}`;

    keyboard.text(label, `amount:${amount}:paid`);
    if ((index + 1) % 2 === 0) {
      keyboard.row();
    }
  });

  keyboard.row().text('ÃƒÂ¢Ã‚Â¬Ã¢â‚¬Â¦ÃƒÂ¯Ã‚Â¸Ã‚Â Back', 'nav:start');
  keyboard.text('ÃƒÂ°Ã…Â¸Ã‚ÂÃ‚Â  Home', 'nav:home');
  keyboard.row().text('ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬Å¾ Refresh', 'refresh:amount');
  return keyboard;
}

function makePaymentKeyboard(user) {
  const keyboard = new InlineKeyboard();

  if (user.selection.amount) {
    if (!hasLaunchAccess(user) && cfg.solanaReceiveAddress && !user.selection.usingFreeTrial) {
      keyboard.text('ÃƒÂ°Ã…Â¸Ã‚Â§Ã‚Â¾ Check Payment', 'payment:check');
      keyboard.text('ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ‚Â New Quote', 'payment:refresh');
      keyboard.row();
    }
  }

  keyboard.text('ÃƒÂ¢Ã‚Â¬Ã¢â‚¬Â¦ÃƒÂ¯Ã‚Â¸Ã‚Â Back', 'nav:amount');
  if (user.selection.button && user.selection.amount) {
    keyboard.text('ÃƒÂ°Ã…Â¸Ã…Â½Ã‚Â¯ Change Link', 'nav:target');
  }
  keyboard.row().text('ÃƒÂ°Ã…Â¸Ã‚ÂÃ‚Â  Home', 'nav:home');
  keyboard.text('ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬Å¾ Refresh', 'refresh:payment');
  return keyboard;
}

function makeConfirmKeyboard(user) {
  const ready = Boolean(user.selection.button && user.selection.amount);
  const keyboard = new InlineKeyboard();

  if (hasLaunchAccess(user) && ready) {
    keyboard.text('ÃƒÂ°Ã…Â¸Ã…Â¡Ã¢â€šÂ¬ Launch Reactions', 'run:confirm');
    keyboard.row();
  } else if (ready) {
    keyboard.text(
      isAdminUser(user.telegramId)
        ? 'ÃƒÂ°Ã…Â¸Ã¢â‚¬ËœÃ¢â‚¬Ëœ Admin Ready'
        : user.selection.usingFreeTrial
          ? 'ÃƒÂ°Ã…Â¸Ã…Â½Ã‚Â Trial Ready'
          : 'ÃƒÂ¢Ã¢â‚¬â€Ã…Â½ Payment',
      'nav:payment',
    );
    keyboard.row();
  }

  keyboard.text('ÃƒÂ°Ã…Â¸Ã…Â¡Ã¢â€šÂ¬ Change Reaction', 'nav:start');
  keyboard.row().text('ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‚Â¦ Change Amount', 'nav:amount');
  if (user.selection.button && user.selection.amount) {
    keyboard.text('ÃƒÂ°Ã…Â¸Ã…Â½Ã‚Â¯ Change Link', 'nav:target');
    keyboard.row();
  }
  keyboard.text('ÃƒÂ¢Ã‚Â¬Ã¢â‚¬Â¦ÃƒÂ¯Ã‚Â¸Ã‚Â Back', 'nav:payment');
  keyboard.text('ÃƒÂ°Ã…Â¸Ã‚ÂÃ‚Â  Home', 'nav:home');
  keyboard.row().text('ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬Å¾ Refresh', 'refresh:confirm');

  return keyboard;
}

function makeInfoKeyboard(backTarget = 'nav:home', refreshRoute = 'home') {
  return new InlineKeyboard()
    .text('ÃƒÂ¢Ã‚Â¬Ã¢â‚¬Â¦ÃƒÂ¯Ã‚Â¸Ã‚Â Back', backTarget)
    .text('ÃƒÂ°Ã…Â¸Ã‚ÂÃ‚Â  Home', 'nav:home')
    .row()
    .text('ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬Å¾ Refresh', `refresh:${refreshRoute}`);
}

function makeVolumeKeyboardLegacyOriginal(selectedMode) {
  const organicLabel = selectedMode === 'organic'
    ? 'ÃƒÂ¢Ã…â€œÃ¢â‚¬Â¦ ÃƒÂ°Ã…Â¸Ã…â€™Ã‚Â¿ Organic Apple Booster'
    : 'ÃƒÂ°Ã…Â¸Ã…â€™Ã‚Â¿ Organic Apple Booster';
  const bundledLabel = selectedMode === 'bundled'
    ? 'ÃƒÂ¢Ã…â€œÃ¢â‚¬Â¦ ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‚Â¦ Bundled Apple Booster'
    : 'ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‚Â¦ Bundled Apple Booster';

  return new InlineKeyboard()
    .text(organicLabel, 'volume:organic')
    .row()
    .text(bundledLabel, 'volume:bundled')
    .row()
    .text('ÃƒÂ¢Ã‚Â¬Ã¢â‚¬Â¦ÃƒÂ¯Ã‚Â¸Ã‚Â Back', 'nav:home')
    .text('ÃƒÂ°Ã…Â¸Ã‚ÂÃ‚Â  Home', 'nav:home')
    .row()
    .text('ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬Å¾ Refresh', 'refresh:volume');
}

function makeOrganicVolumeKeyboardLegacy(selectedPackageKey) {
  const keyboard = new InlineKeyboard();

  ORGANIC_VOLUME_PACKAGES.forEach((pkg, index) => {
    const label = selectedPackageKey === pkg.key
      ? `ÃƒÂ¢Ã…â€œÃ¢â‚¬Â¦ ${pkg.emoji} ${pkg.label} - ${pkg.priceSol} SOL`
      : `${pkg.emoji} ${pkg.label} - ${pkg.priceSol} SOL`;

    keyboard.text(label, `organic:${pkg.key}`);
    if ((index + 1) % 2 === 0) {
      keyboard.row();
    }
  });

  keyboard.row()
    .text('ÃƒÂ¢Ã‚Â¬Ã¢â‚¬Â¦ÃƒÂ¯Ã‚Â¸Ã‚Â Back', 'nav:home')
    .text('ÃƒÂ°Ã…Â¸Ã‚ÂÃ‚Â  Home', 'nav:home');
  keyboard.row().text('ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬Å¾ Refresh', 'refresh:volume_organic');
  return keyboard;
}

function appleBoosterListLabel(order) {
  const pkg = getAppleBoosterPackage(order.strategy, order.packageKey);
  const statusLabel = order.archivedAt
    ? '\u26AB Archived'
    : (order.running
      ? '\u{1F7E2} Running'
      : (order.appleBooster?.stopRequested ? '\u23F3 Stopping' : (order.funded ? (order.freeTrial ? '\u{1F7E1} Ready' : '\u{1F7E1} Funded') : '\u{1F7E0} Awaiting Deposit')));
  const packageLabel = pkg?.label || order.packageKey?.toUpperCase() || 'Booster';
  const mintLabel = order.appleBooster?.mintAddress
    ? ` \u2022 ${order.appleBooster.mintAddress.slice(0, 4)}...${order.appleBooster.mintAddress.slice(-4)}`
    : '';
  const modeIcon = order.strategy === 'bundled' ? '\u{1F4E6}' : '\u{1F34F}';
  const modeLabel = order.strategy === 'bundled' ? 'Bundle' : 'Booster';
  return `${modeIcon} ${packageLabel} ${modeLabel}${mintLabel} \u2022 ${statusLabel}`;
}

function makeOrganicVolumeKeyboard(user) {
  const keyboard = new InlineKeyboard();
  const selectedPackageKey = user.organicVolumePackage;
  const activeBoosters = getVisibleAppleBoostersByStrategy(user, 'organic', { archived: false });

  ORGANIC_VOLUME_PACKAGES.forEach((pkg, index) => {
    const label = selectedPackageKey === pkg.key
      ? `\u2705 ${pkg.emoji} New ${pkg.label} Organic Booster`
      : `${pkg.emoji} New ${pkg.label} Organic Booster`;

    keyboard.text(label, `organic:${pkg.key}`);
    if ((index + 1) % 2 === 0) {
      keyboard.row();
    }
  });

  const visibleBoosters = activeBoosters.filter((order) => !order.freeTrial);

  if (visibleBoosters.length > 0) {
    keyboard.row();
    for (const order of visibleBoosters) {
      keyboard.text(appleBoosterListLabel(order), `organic:open:${order.id}`);
      keyboard.row();
    }
  }

  if (getVisibleAppleBoosters(user, { archived: true }).length > 0) {
    keyboard.text('\u{1F5C4}\uFE0F Archive', 'nav:volume_archive');
    keyboard.row();
  }

  keyboard.row()
    .text('\u2B05\uFE0F Back', 'nav:home')
    .text('\u{1F3E0} Home', 'nav:home');
  keyboard.row().text('\u{1F504} Refresh', 'refresh:volume_organic');
  return keyboard;
}

function makeBundledVolumeKeyboardLegacyOriginal(user) {
  const keyboard = new InlineKeyboard();
  const selectedPackageKey = user.organicVolumePackage;
  const activeBoosters = getVisibleAppleBoostersByStrategy(user, 'bundled', { archived: false });

  BUNDLED_VOLUME_PACKAGES.forEach((pkg, index) => {
    const label = selectedPackageKey === pkg.key && user.organicVolumeOrder?.strategy === 'bundled'
      ? `ÃƒÂ¢Ã…â€œÃ¢â‚¬Â¦ ${pkg.emoji} New ${pkg.label} Bundle`
      : `${pkg.emoji} New ${pkg.label} Bundle`;

    keyboard.text(label, `bundled:${pkg.key}`);
    if ((index + 1) % 2 === 0) {
      keyboard.row();
    }
  });

  if (activeBoosters.length > 0) {
    keyboard.row();
    for (const order of activeBoosters) {
      keyboard.text(appleBoosterListLabel(order), `organic:open:${order.id}`);
      keyboard.row();
    }
  }

  if (getVisibleAppleBoosters(user, { archived: true }).length > 0) {
    keyboard.text('\u{1F5C4}\uFE0F Archive', 'nav:volume_archive');
    keyboard.row();
  }

  keyboard.row()
    .text('\u2B05\uFE0F Back', 'nav:volume')
    .text('\u{1F3E0} Home', 'nav:home');
  keyboard.row().text('\u{1F504} Refresh', 'refresh:volume_bundled');
  return keyboard;
}

function makeOrganicVolumeOrderKeyboardLegacyOld(user) {
  const keyboard = new InlineKeyboard();
  const order = user.organicVolumeOrder;

  keyboard.text('ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬Å¾ Refresh Balance', 'organic:refresh');
  keyboard.row();

  if (user.organicVolumeOrder.funded) {
    keyboard.text(user.organicVolumeOrder.running ? 'ÃƒÂ¢Ã‚ÂÃ‚Â¹ÃƒÂ¯Ã‚Â¸Ã‚Â Stop' : 'ÃƒÂ¢Ã¢â‚¬â€œÃ‚Â¶ÃƒÂ¯Ã‚Â¸Ã‚Â Start', user.organicVolumeOrder.running ? 'organic:stop' : 'organic:start');
    keyboard.text('ÃƒÂ°Ã…Â¸Ã¢â‚¬â„¢Ã‚Â¸ Withdraw Remaining', 'organic:withdraw');
    keyboard.row();
  } else {
    keyboard.text('ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬â„¢ ÃƒÂ¢Ã¢â‚¬â€œÃ‚Â¶ÃƒÂ¯Ã‚Â¸Ã‚Â Start', 'organic:locked:start');
    keyboard.text('ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬â„¢ ÃƒÂ¢Ã‚ÂÃ‚Â¹ÃƒÂ¯Ã‚Â¸Ã‚Â Stop', 'organic:locked:stop');
    keyboard.row();
    keyboard.text('ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬â„¢ ÃƒÂ°Ã…Â¸Ã¢â‚¬â„¢Ã‚Â¸ Withdraw Remaining', 'organic:locked:withdraw');
    keyboard.row();
  }

  keyboard.text('ÃƒÂ¢Ã‚Â¬Ã¢â‚¬Â¦ÃƒÂ¯Ã‚Â¸Ã‚Â Back', 'nav:volume');
  keyboard.text('ÃƒÂ°Ã…Â¸Ã‚ÂÃ‚Â  Home', 'nav:home');
  return keyboard;
}

function makeResultKeyboard() {
  return new InlineKeyboard()
    .text('ÃƒÂ¢Ã‚Â¬Ã¢â‚¬Â¦ÃƒÂ¯Ã‚Â¸Ã‚Â Back', 'nav:payment')
    .text('ÃƒÂ°Ã…Â¸Ã‚ÂÃ‚Â  Home', 'nav:home')
    .row()
    .text('ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬Å¾ Refresh', 'refresh:payment');
}

function burnAgentListLabel(agent) {
  const archivedTag = isArchivedBurnAgent(agent) ? ' [Archived]' : '';
  const status = agent.automationEnabled ? 'Running' : 'Stopped';
  const speed = agent.speed === 'lightning' ? 'Fast' : (agent.speed === 'normal' ? 'Normal' : 'Agent');
  const tokenLabel = agent.tokenName?.trim()
    || (agent.mintAddress ? `${agent.mintAddress.slice(0, 4)}...${agent.mintAddress.slice(-4)}` : 'Complete Setup');
  return `${speed} ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ ${mint} ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ ${status}${archivedTag}`;
}

function burnAgentCatalogDisplayLabel(agent) {
  const archivedTag = isArchivedBurnAgent(agent) ? ' [Archived]' : '';
  const status = agent.automationEnabled ? 'Running' : 'Stopped';
  const speed = agent.speed === 'lightning' ? 'Fast' : (agent.speed === 'normal' ? 'Normal' : 'Agent');
  const tokenLabel = agent.tokenName?.trim()
    || (agent.mintAddress ? `${agent.mintAddress.slice(0, 4)}...${agent.mintAddress.slice(-4)}` : 'Complete Setup');
  return `${tokenLabel} - ${speed} - ${status}${archivedTag}`;
}

function makeBurnAgentCatalogKeyboardLegacy(user) {
  const keyboard = new InlineKeyboard();
  const activeAgents = getVisibleBurnAgents(user, { archived: false });

  keyboard.text('ÃƒÂ¢Ã…Â¡Ã‚Â¡ New Lightning Fast', 'burn:new:lightning');
  keyboard.row();
  keyboard.text('ÃƒÂ°Ã…Â¸Ã‚ÂÃ‚Â¢ New Normal', 'burn:new:normal');
  keyboard.row();

  for (const agent of activeAgents) {
    keyboard.text(burnAgentListLabel(agent), `burn:open:${agent.id}`);
    keyboard.row();
  }

  if (getVisibleBurnAgents(user, { archived: true }).length > 0) {
    keyboard.text('ÃƒÂ°Ã…Â¸Ã¢â‚¬â€Ã¢â‚¬Å¾ÃƒÂ¯Ã‚Â¸Ã‚Â Archive', 'nav:burn_agent_archive');
    keyboard.row();
  }

  keyboard.text('ÃƒÂ°Ã…Â¸Ã‚ÂÃ‚Â  Home', 'nav:home');
  keyboard.row().text('ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬Å¾ Refresh', 'refresh:burn_agent');
  return keyboard;
}

function makeBurnAgentArchiveKeyboardLegacy(user) {
  const keyboard = new InlineKeyboard();
  const archivedAgents = getVisibleBurnAgents(user, { archived: true });

  for (const agent of archivedAgents) {
    keyboard.text(burnAgentListLabel(agent), `burn:open:${agent.id}`);
    keyboard.row();
  }

  keyboard.text('ÃƒÂ¢Ã‚Â¬Ã¢â‚¬Â¦ÃƒÂ¯Ã‚Â¸Ã‚Â Back', 'nav:burn_agent');
  keyboard.text('ÃƒÂ°Ã…Â¸Ã‚ÂÃ‚Â  Home', 'nav:home');
  keyboard.row().text('ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬Å¾ Refresh', 'refresh:burn_agent_archive');
  return keyboard;
}

function makeBurnAgentCatalogKeyboard(user) {
  const keyboard = new InlineKeyboard();
  const activeAgents = getVisibleBurnAgents(user, { archived: false });

  keyboard.text('New Fast Burn Agent', 'burn:new:lightning');
  keyboard.row();
  keyboard.text('New Normal Burn Agent', 'burn:new:normal');
  keyboard.row();

  for (const agent of activeAgents) {
    keyboard.text(burnAgentCatalogDisplayLabel(agent), `burn:open:${agent.id}`);
    keyboard.row();
  }

  if (getVisibleBurnAgents(user, { archived: true }).length > 0) {
    keyboard.text('Archive', 'nav:burn_agent_archive');
    keyboard.row();
  }

  keyboard.text('Home', 'nav:home');
  keyboard.row().text('Refresh', 'refresh:burn_agent');
  return keyboard;
}

function makeBurnAgentArchiveKeyboard(user) {
  const keyboard = new InlineKeyboard();
  const archivedAgents = getVisibleBurnAgents(user, { archived: true });

  for (const agent of archivedAgents) {
    keyboard.text(burnAgentCatalogDisplayLabel(agent), `burn:open:${agent.id}`);
    keyboard.row();
  }

  keyboard.text('Back', 'nav:burn_agent');
  keyboard.text('Home', 'nav:home');
  keyboard.row().text('Refresh', 'refresh:burn_agent_archive');
  return keyboard;
}

function makeBurnAgentEditorKeyboardLegacy(user) {
  const keyboard = new InlineKeyboard();
  const agent = user.burnAgent;

  if (isArchivedBurnAgent(agent)) {
    keyboard.text('ÃƒÂ¢Ã¢â€žÂ¢Ã‚Â»ÃƒÂ¯Ã‚Â¸Ã‚Â Restore Agent', `burn:restore:${agent.id}`);
    keyboard.row();
    keyboard.text(
      agent.deleteConfirmations >= 1 ? 'ÃƒÂ°Ã…Â¸Ã¢â‚¬â€Ã¢â‚¬ËœÃƒÂ¯Ã‚Â¸Ã‚Â Confirm Permanent Delete' : 'ÃƒÂ°Ã…Â¸Ã¢â‚¬â€Ã¢â‚¬ËœÃƒÂ¯Ã‚Â¸Ã‚Â Delete Permanently',
      `burn:delete:${agent.id}`,
    );
    keyboard.row();
    keyboard.text('ÃƒÂ¢Ã‚Â¬Ã¢â‚¬Â¦ÃƒÂ¯Ã‚Â¸Ã‚Â Back', 'nav:burn_agent_archive');
    keyboard.text('ÃƒÂ°Ã…Â¸Ã‚ÂÃ‚Â  Home', 'nav:home');
    keyboard.row().text('ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬Å¾ Refresh', 'refresh:burn_agent_editor');
    return keyboard;
  }

  if (burnAgentNeedsWalletChoice(user)) {
    keyboard.text('ÃƒÂ°Ã…Â¸Ã‚Â§Ã‚Âª Generate Wallet', `burn:wallet:generated:${agent.id}`);
    keyboard.row();
    keyboard.text("ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ‚Â I'll Provide My Own", `burn:wallet:provided:${agent.id}`);
    keyboard.row();
  } else {
    if (!agent.walletSecretKeyB64 || agent.walletMode === 'provided') {
      keyboard.text(
        agent.walletAddress ? 'ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ‚Â Replace Private Key' : 'ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ‚Â Add Private Key',
        `burn:set:private_key:${agent.id}`,
      );
      keyboard.row();
    }

    if (agent.walletMode === 'generated' || agent.walletMode === 'managed') {
      const regenLabel = agent.regenerateConfirmations <= 0
        ? 'ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬Å¾ Regenerate Wallet'
        : `ÃƒÂ°Ã…Â¸Ã…Â¡Ã‚Â¨ Confirm Regenerate ${agent.regenerateConfirmations}/3`;
      keyboard.text(regenLabel, `burn:regen:${agent.id}`);
      keyboard.row();
    }

    if (burnAgentHasStoredPrivateKey(agent)) {
      keyboard.text(
        agent.privateKeyVisible ? 'Hide Private Key' : 'Show Private Key',
        `burn:key:toggle:${agent.id}`,
      );
      keyboard.row();
    }

    keyboard.text(
      burnAgentIsReady(agent)
        ? (agent.automationEnabled ? 'ÃƒÂ¢Ã‚ÂÃ‚Â¹ÃƒÂ¯Ã‚Â¸Ã‚Â Stop Burn Bot' : 'ÃƒÂ¢Ã¢â‚¬â€œÃ‚Â¶ÃƒÂ¯Ã‚Â¸Ã‚Â Start Burn Bot')
        : 'ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬â„¢ Start Burn Bot',
      burnAgentIsReady(agent)
        ? `burn:toggle:${agent.id}`
        : 'burn:locked:toggle',
    );
    keyboard.text(
      agent.walletAddress ? 'ÃƒÂ°Ã…Â¸Ã¢â‚¬â„¢Ã‚Â¸ Withdraw Funds' : 'ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬â„¢ Withdraw Funds',
      agent.walletAddress ? `burn:withdraw:${agent.id}` : 'burn:locked:withdraw',
    );
    keyboard.row();

    keyboard.text(agent.mintAddress ? 'ÃƒÂ°Ã…Â¸Ã‚ÂªÃ¢â€žÂ¢ Update Mint' : 'ÃƒÂ°Ã…Â¸Ã‚ÂªÃ¢â€žÂ¢ Set Mint', `burn:set:mint:${agent.id}`);
    keyboard.row();

    if (!isNormalBurnAgent(agent)) {
      keyboard.text(
        agent.treasuryAddress ? 'ÃƒÂ°Ã…Â¸Ã‚ÂÃ‚Â¦ Update Treasury' : 'ÃƒÂ°Ã…Â¸Ã‚ÂÃ‚Â¦ Set Treasury',
        `burn:set:treasury:${agent.id}`,
      );
      keyboard.row();
      keyboard.text(
        Number.isInteger(agent.burnPercent) ? `ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ‚Â¥ Burn ${agent.burnPercent}%` : 'ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ‚Â¥ Set Burn %',
        `burn:set:burn_percent:${agent.id}`,
      );
      keyboard.text(
        Number.isInteger(agent.treasuryPercent)
          ? `ÃƒÂ°Ã…Â¸Ã¢â‚¬â„¢Ã‚Â¸ Treasury ${agent.treasuryPercent}%`
          : 'ÃƒÂ°Ã…Â¸Ã¢â‚¬â„¢Ã‚Â¸ Set Treasury %',
        `burn:set:treasury_percent:${agent.id}`,
      );
      keyboard.row();
    }

    keyboard.text('ÃƒÂ°Ã…Â¸Ã¢â‚¬â€Ã¢â‚¬Å¾ÃƒÂ¯Ã‚Â¸Ã‚Â Archive Agent', `burn:archive:${agent.id}`);
    keyboard.row();
  }

  keyboard.text('ÃƒÂ¢Ã‚Â¬Ã¢â‚¬Â¦ÃƒÂ¯Ã‚Â¸Ã‚Â Back', 'nav:burn_agent');
  keyboard.text('ÃƒÂ°Ã…Â¸Ã‚ÂÃ‚Â  Home', 'nav:home');
  keyboard.row().text('ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬Å¾ Refresh', 'refresh:burn_agent_editor');
  return keyboard;
}

function makeBurnAgentEditorKeyboard(user) {
  const keyboard = new InlineKeyboard();
  const agent = user.burnAgent;

  if (isArchivedBurnAgent(agent)) {
    keyboard.text('Restore Agent', `burn:restore:${agent.id}`);
    keyboard.row();
    keyboard.text(
      agent.deleteConfirmations >= 1 ? 'Confirm Permanent Delete' : 'Delete Permanently',
      `burn:delete:${agent.id}`,
    );
    keyboard.row();
    keyboard.text('Back', 'nav:burn_agent_archive');
    keyboard.text('Home', 'nav:home');
    keyboard.row().text('Refresh', 'refresh:burn_agent_editor');
    return keyboard;
  }

  if (burnAgentNeedsWalletChoice(user)) {
    keyboard.text('Generate Wallet', `burn:wallet:generated:${agent.id}`);
    keyboard.row();
    keyboard.text("I'll Provide My Own", `burn:wallet:provided:${agent.id}`);
    keyboard.row();
  } else {
    if (!agent.walletSecretKeyB64 || agent.walletMode === 'provided') {
      keyboard.text(
        agent.walletAddress ? 'Replace Private Key' : 'Add Private Key',
        `burn:set:private_key:${agent.id}`,
      );
      keyboard.row();
    }

    if (agent.walletMode === 'generated' || agent.walletMode === 'managed') {
      const regenLabel = agent.regenerateConfirmations <= 0
        ? 'Regenerate Wallet'
        : `Confirm Regenerate ${agent.regenerateConfirmations}/3`;
      keyboard.text(regenLabel, `burn:regen:${agent.id}`);
      keyboard.row();
    }

    if (burnAgentHasStoredPrivateKey(agent)) {
      keyboard.text(
        agent.privateKeyVisible ? 'Hide Private Key' : 'Show Private Key',
        `burn:key:toggle:${agent.id}`,
      );
      keyboard.row();
    }

    keyboard.text(
      burnAgentIsReady(agent)
        ? (agent.automationEnabled ? 'Stop Burn Bot' : 'Start Burn Bot')
        : 'Locked: Start Burn Bot',
      burnAgentIsReady(agent)
        ? `burn:toggle:${agent.id}`
        : 'burn:locked:toggle',
    );
    keyboard.text(
      agent.walletAddress ? 'Withdraw Funds' : 'Locked: Withdraw Funds',
      agent.walletAddress ? `burn:withdraw:${agent.id}` : 'burn:locked:withdraw',
    );
    keyboard.row();

    keyboard.text(agent.tokenName ? 'Update Token Name' : 'Set Token Name', `burn:set:token_name:${agent.id}`);
    keyboard.row();
    keyboard.text(agent.mintAddress ? 'Update Mint' : 'Set Mint', `burn:set:mint:${agent.id}`);
    keyboard.row();

    if (!isNormalBurnAgent(agent)) {
      keyboard.text(
        agent.treasuryAddress ? 'Update Treasury' : 'Set Treasury',
        `burn:set:treasury:${agent.id}`,
      );
      keyboard.row();
      keyboard.text(
        Number.isInteger(agent.burnPercent) ? `Burn ${agent.burnPercent}%` : 'Set Burn %',
        `burn:set:burn_percent:${agent.id}`,
      );
      keyboard.text(
        Number.isInteger(agent.treasuryPercent)
          ? `Treasury ${agent.treasuryPercent}%`
          : 'Set Treasury %',
        `burn:set:treasury_percent:${agent.id}`,
      );
      keyboard.row();
    }

    keyboard.text('Archive Agent', `burn:archive:${agent.id}`);
    keyboard.row();
  }

  keyboard.text('Back', 'nav:burn_agent');
  keyboard.text('Home', 'nav:home');
  keyboard.row().text('Refresh', 'refresh:burn_agent_editor');
  return keyboard;
}

function makeTargetKeyboard(user) {
  const keyboard = new InlineKeyboard();

  if (
    user.selection.button ||
    user.selection.amount ||
    (user.selection.target && user.selection.target !== cfg.defaultTarget)
  ) {
    keyboard.text('ÃƒÂ¢Ã…â€œÃ¢â‚¬Â¦ Continue', 'target:continue');
    keyboard.row();
  }

  keyboard.text('ÃƒÂ¢Ã‚Â¬Ã¢â‚¬Â¦ÃƒÂ¯Ã‚Â¸Ã‚Â Back', targetBackRoute(user));
  keyboard.text('ÃƒÂ°Ã…Â¸Ã‚ÂÃ‚Â  Home', 'nav:home');
  keyboard.row().text('ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬Å¾ Refresh', 'refresh:target');
  return keyboard;
}

function homeText(user) {
  return [
    '*Welcome to WIZARD TOOLZ!*',
    '',
    'Getting started with trending has never been easier.',
    '',
    'ÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚Â',
    'ÃƒÂ¢Ã…â€œÃ‚Â¨ *How it works :*',
    '1. Pick your package and the amount of volume you want',
    '2. Choose the button profile and set your target link',
    '3. Confirm payment or use the free trial to unlock the run',
    '4. Launch it and let the bot handle the execution',
    '',
    'ÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚ÂÃƒÂ¢Ã¢â‚¬ÂÃ‚Â',
    'ÃƒÂ°Ã…Â¸Ã…Â¡Ã¢â€šÂ¬ Supports: Raydium ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ PumpSwap ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Meteora ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Pumpfun ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Meteora DBC ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Bags ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ LetsBonk ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ LaunchLab',
    'ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã…Â  Flexible plans starting at 1 SOL ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂºÃ‚Â¡ÃƒÂ¯Ã‚Â¸Ã‚Â 100% Safe and secure ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Reliable Execution ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ ÃƒÂ°Ã…Â¸Ã…Â½Ã‚Â Free Trial',
    '',
    'Ready? Choose below! Need help? Click the help button!',
  ].join('\n');
}

function startText(user) {
  return [
    'ÃƒÂ°Ã…Â¸Ã…Â¡Ã¢â€šÂ¬ *Reaction Booster*',
    '',
    selectionSnapshot(user),
    '',
    'Choose the button profile you want this run to hammer.',
  ].join('\n');
}

function volumeText(user) {
  return [
    'ÃƒÂ°Ã…Â¸Ã‚ÂÃ…Â½ *Apple Booster*',
    '',
    'Choose the type of volume flow you want to use.',
    '',
    'ÃƒÂ°Ã…Â¸Ã…â€™Ã‚Â¿ Organic Apple Booster: a more natural-looking apple path',
    'ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‚Â¦ Bundled Apple Booster: a bundled apple path',
    ...(user.volumeMode ? ['', `*Selected mode:* ${user.volumeMode === 'organic' ? 'ÃƒÂ°Ã…Â¸Ã…â€™Ã‚Â¿ Organic Apple Booster' : 'ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‚Â¦ Bundled Apple Booster'}`] : []),
  ].join('\n');
}

function organicVolumeTextLegacy(user) {
  const selectedPackage = getOrganicVolumePackage(user.organicVolumePackage);
  const activeBoosters = getVisibleAppleBoosters(user, { archived: false });
  const archivedBoosters = getVisibleAppleBoosters(user, { archived: true });
  return [
    'ÃƒÂ°Ã…Â¸Ã‚ÂÃ‚Â *Apple Booster Manager*',
    '',
    `Active boosters: *${activeBoosters.length}*`,
    `Archived boosters: *${archivedBoosters.length}*`,
    '',
    'Tap a package below to create a brand new Apple Booster.',
    '',
    'ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‚Â¦ *Available Apple Packages* (`ÃƒÂ¢Ã¢â‚¬Â Ã‚Â©ÃƒÂ¯Ã‚Â¸Ã‚Â Creator fee back to you`)',
    '',
    ...ORGANIC_VOLUME_PACKAGES.map((pkg) =>
      `${pkg.emoji} ${pkg.label} - *${pkg.priceSol} SOL* (ÃƒÂ¢Ã¢â‚¬Â Ã‚Â©ÃƒÂ¯Ã‚Â¸Ã‚Â ${pkg.rebateSol} SOL)`,
    ),
    '',
    'ÃƒÂ¢Ã…Â¡Ã‚Â¡ÃƒÂ¯Ã‚Â¸Ã‚Â Optimized execution. Better pricing. Built for real chart growth.',
    ...(selectedPackage ? ['', `*Selected package:* ${selectedPackage.emoji} ${selectedPackage.label} - ${selectedPackage.priceSol} SOL`] : []),
    '',
    'ÃƒÂ°Ã…Â¸Ã¢â‚¬ËœÃ¢â‚¬Â¡ Tap a button below to choose your apple package.',
  ].join('\n');
}

function organicVolumeArchiveTextLegacy(user) {
  const archivedBoosters = getVisibleAppleBoosters(user, { archived: true });
  return [
    'ÃƒÂ°Ã…Â¸Ã¢â‚¬â€Ã¢â‚¬Å¾ÃƒÂ¯Ã‚Â¸Ã‚Â *Apple Booster Archive*',
    '',
    archivedBoosters.length > 0
      ? `Archived boosters: *${archivedBoosters.length}*`
      : 'No archived Apple Boosters yet.',
    '',
    'Permanent delete is only available from inside an archived booster.',
  ].join('\n');
}

function organicVolumeText(user) {
  const selectedPackage = getOrganicVolumePackage(user.organicVolumePackage);
  const activeBoosters = getVisibleAppleBoosters(user, { archived: false });
  const archivedBoosters = getVisibleAppleBoosters(user, { archived: true });
  return [
    '\u{1F34F} *Organic Volume Booster*',
    '',
    `Active boosters: *${activeBoosters.length}*`,
    `Archived boosters: *${archivedBoosters.length}*`,
    '',
    'Tap a package below to create a new organic booster.',
    '',
    '\u{1F4E6} *Available Organic Packages* (`\u21A9\uFE0F Creator fee back to you`)',
    '',
    ...ORGANIC_VOLUME_PACKAGES.map((pkg) =>
      `${pkg.emoji} ${pkg.label} - *${pkg.priceSol} SOL* (\u21A9\uFE0F ${pkg.rebateSol} SOL)`,
    ),
    '',
    '\u26A1 Optimized execution. Better pricing. Built for real chart growth.',
    ...(selectedPackage ? ['', `*Selected package:* ${selectedPackage.emoji} ${selectedPackage.label} - ${selectedPackage.priceSol} SOL`] : []),
    '',
    '\u{1F447} Tap a button below to choose your apple package.',
  ].join('\n');
}

function organicVolumeArchiveText(user) {
  const archivedBoosters = getVisibleAppleBoosters(user, { archived: true });
  return [
    '\u{1F5C4}\uFE0F *Apple Booster Archive*',
    '',
    archivedBoosters.length > 0
      ? `Archived boosters: *${archivedBoosters.length}*`
      : 'No archived Apple Boosters yet.',
    '',
    'Permanent delete is only available from inside an archived booster.',
  ].join('\n');
}

function bundledVolumeTextLegacy() {
  return [
    'ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‚Â¦ *Bundled Apple Booster*',
    '',
    'This flow is up next.',
    '',
    'Organic Apple Booster packages are live now, and Bundled Apple Booster will be wired next.',
  ].join('\n');
}

function bundledVolumeTextLegacyOriginal() {
  return [
    '\u{1F4E6} *Bundled Apple Booster*',
    '',
    'Bundle Volume Packages (`ÃƒÂ¢Ã¢â‚¬Â Ã‚Â©ÃƒÂ¯Ã‚Â¸Ã‚Â Creator fee rebates included`)',
    '',
    ...BUNDLED_VOLUME_PACKAGES.map((pkg) =>
      `${pkg.emoji} ${pkg.label} - *${pkg.priceSol} SOL* (ÃƒÂ¢Ã¢â‚¬Â Ã‚Â©ÃƒÂ¯Ã‚Â¸Ã‚Â ${pkg.rebateSol} SOL)`,
    ),
    '',
    'ÃƒÂ¢Ã…Â¡Ã‚Â¡ÃƒÂ¯Ã‚Â¸Ã‚Â Faster execution. Lower cost. Optimized for momentum.',
    '',
    'Bundled mode submits a same-block buy and sell bundle through Jito.',
  ].join('\n');
}

function makeVolumeKeyboard(selectedMode) {
  const organicLabel = selectedMode === 'organic'
    ? '\u2705 \u{1F33F} Organic Apple Booster'
    : '\u{1F33F} Organic Apple Booster';
  const bundledLabel = selectedMode === 'bundled'
    ? '\u2705 \u{1F4E6} Bundled Apple Booster'
    : '\u{1F4E6} Bundled Apple Booster';

  return new InlineKeyboard()
    .text(organicLabel, 'volume:organic')
    .row()
    .text(bundledLabel, 'volume:bundled')
    .row()
    .text('\u2B05\uFE0F Back', 'nav:home')
    .text('\u{1F3E0} Home', 'nav:home')
    .row()
    .text('\u{1F504} Refresh', 'refresh:volume');
}

function makeBundledVolumeKeyboard(user) {
  const keyboard = new InlineKeyboard();
  const selectedPackageKey = user.organicVolumePackage;
  const activeBoosters = getVisibleAppleBoostersByStrategy(user, 'bundled', { archived: false });

  BUNDLED_VOLUME_PACKAGES.forEach((pkg, index) => {
    const label = selectedPackageKey === pkg.key && user.organicVolumeOrder?.strategy === 'bundled'
      ? `\u2705 ${pkg.emoji} New ${pkg.label} Bundle`
      : `${pkg.emoji} New ${pkg.label} Bundle`;

    keyboard.text(label, `bundled:${pkg.key}`);
    if ((index + 1) % 2 === 0) {
      keyboard.row();
    }
  });

  if (activeBoosters.length > 0) {
    keyboard.row();
    for (const order of activeBoosters) {
      keyboard.text(appleBoosterListLabel(order), `organic:open:${order.id}`);
      keyboard.row();
    }
  }

  if (getVisibleAppleBoosters(user, { archived: true }).length > 0) {
    keyboard.text('\u{1F5C4}\uFE0F Archive', 'nav:volume_archive');
    keyboard.row();
  }

  keyboard.row()
    .text('\u2B05\uFE0F Back', 'nav:volume')
    .text('\u{1F3E0} Home', 'nav:home');
  keyboard.row().text('\u{1F504} Refresh', 'refresh:volume_bundled');
  return keyboard;
}

function bundledVolumeText() {
  return [
    '\u{1F4E6} *Bundled Apple Booster*',
    '',
    '\u{1F4CA} *Bundle Volume Packages* (`\u21A9\uFE0F Creator fee rebates included`)',
    '',
    ...BUNDLED_VOLUME_PACKAGES.map((pkg) =>
      `${pkg.emoji} ${pkg.label} - *${pkg.priceSol} SOL* (\u21A9\uFE0F ${pkg.rebateSol} SOL)`,
    ),
    '',
    '\u26A1\uFE0F Faster execution. Lower cost. Optimized for momentum.',
    '',
    '\u{1F4A1} Bundled mode submits a same-slot buy and sell bundle through Jito.',
    '\u{1F512} Treasury/dev package split still runs automatically before bundled volume begins.',
    '\u{1F447} Tap a package below to create a new bundled booster.',
  ].join('\n');
}

function organicVolumeOrderTextLegacy(user) {
  const order = user.organicVolumeOrder;
  const pkg = getOrganicVolumePackage(order.packageKey);
  const remainingLamports = Math.max(0, (order.requiredLamports ?? 0) - (order.currentLamports ?? 0));

  return [
    'ÃƒÂ°Ã…Â¸Ã¢â‚¬â„¢Ã‚Â¼ *Organic Apple Order*',
    '',
    pkg ? `ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‚Â¦ Package: ${pkg.emoji} *${pkg.label}*` : 'ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‚Â¦ Package: Not selected',
    order.requiredSol ? `ÃƒÂ°Ã…Â¸Ã¢â‚¬â„¢Ã‚Â° Required deposit: *${order.requiredSol} SOL*` : 'ÃƒÂ°Ã…Â¸Ã¢â‚¬â„¢Ã‚Â° Required deposit: Pending',
    order.rebateSol ? `ÃƒÂ¢Ã¢â‚¬Â Ã‚Â©ÃƒÂ¯Ã‚Â¸Ã‚Â Creator fee rebate: *${order.rebateSol} SOL*` : null,
    order.walletAddress ? `ÃƒÂ°Ã…Â¸Ã‚ÂÃ‚Â¦ Deposit wallet: \`${order.walletAddress}\`` : 'ÃƒÂ°Ã…Â¸Ã‚ÂÃ‚Â¦ Deposit wallet: Not ready',
    `ÃƒÂ°Ã…Â¸Ã¢â‚¬â„¢Ã‚Â³ Current balance: *${order.currentSol || '0'} SOL*`,
    `ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã¢â‚¬Â° Remaining to fund: *${formatSolAmountFromLamports(remainingLamports)} SOL*`,
    `ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‚Â¡ Status: *${order.funded ? (order.running ? 'Running' : 'Funded') : 'Awaiting deposit'}*`,
    order.lastBalanceCheckAt ? `ÃƒÂ°Ã…Â¸Ã¢â‚¬Â¢Ã¢â‚¬Å“ Last checked: ${formatTimestamp(order.lastBalanceCheckAt)}` : null,
    order.lastError ? `ÃƒÂ¢Ã…Â¡Ã‚Â ÃƒÂ¯Ã‚Â¸Ã‚Â Last error: \`${order.lastError}\`` : null,
    '',
    order.funded
      ? 'Funding confirmed. You can now use Start, Stop, or Withdraw Remaining.'
      : 'Send SOL to the wallet above, then tap Refresh Balance to confirm the deposit.',
  ].filter(Boolean).join('\n');
}

function makeOrganicVolumeOrderKeyboardLegacy(user) {
  const keyboard = new InlineKeyboard();
  const order = user.organicVolumeOrder;

  if (order.archivedAt) {
    keyboard.text('Restore Booster', `organic:restore:${order.id}`);
    keyboard.row();
    keyboard.text(
      order.deleteConfirmations >= 1 ? 'Confirm Permanent Delete' : 'Delete Permanently',
      `organic:delete:${order.id}`,
    );
    keyboard.row();
    keyboard.text('Back', 'nav:volume_archive');
    keyboard.text('Home', 'nav:home');
    keyboard.row().text('Refresh', 'refresh:volume_order');
    return keyboard;
  }

  keyboard.text('ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬Å¾ Refresh Balance', 'organic:refresh');
  keyboard.row();
  keyboard.text(
    Number.isInteger(order.appleBooster.walletCount)
      ? `ÃƒÂ°Ã…Â¸Ã¢â‚¬ËœÃ¢â‚¬Âº Wallets: ${order.appleBooster.walletCount}`
      : 'ÃƒÂ°Ã…Â¸Ã¢â‚¬ËœÃ¢â‚¬Âº Set Worker Wallet Count',
    'organic:set:wallet_count',
  );
  keyboard.row();
  keyboard.text('ÃƒÂ°Ã…Â¸Ã‚ÂªÃ¢â€žÂ¢ Set Mint', 'organic:set:mint');
  keyboard.text('ÃƒÂ°Ã…Â¸Ã¢â‚¬â„¢Ã‚Â¸ Set Swap Range', 'organic:set:swap_range');
  keyboard.row();
  keyboard.text('ÃƒÂ¢Ã‚ÂÃ‚Â±ÃƒÂ¯Ã‚Â¸Ã‚Â Set Interval Range', 'organic:set:interval_range');
  keyboard.row();

  if (order.funded && organicBoosterIsConfigured(order)) {
    keyboard.text(
      order.appleBooster.stopRequested ? 'ÃƒÂ¢Ã‚ÂÃ‚Â³ Stopping + Sweeping' : (order.running ? 'ÃƒÂ¢Ã‚ÂÃ‚Â¹ÃƒÂ¯Ã‚Â¸Ã‚Â Stop + Sweep' : 'ÃƒÂ¢Ã¢â‚¬â€œÃ‚Â¶ÃƒÂ¯Ã‚Â¸Ã‚Â Start Booster'),
      order.running ? 'organic:stop' : 'organic:start',
    );
    keyboard.text('ÃƒÂ°Ã…Â¸Ã¢â‚¬â„¢Ã‚Â¼ Withdraw SOL', 'organic:withdraw');
    keyboard.row();
  } else if (order.funded) {
    keyboard.text('ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬â„¢ Start Booster', 'organic:locked:start');
    keyboard.text('ÃƒÂ°Ã…Â¸Ã¢â‚¬â„¢Ã‚Â¼ Withdraw SOL', 'organic:withdraw');
    keyboard.row();
  } else {
    keyboard.text('ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬â„¢ Start Booster', 'organic:locked:start');
    keyboard.text('ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬â„¢ Stop Booster', 'organic:locked:stop');
    keyboard.row();
    keyboard.text('ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬â„¢ Withdraw SOL', 'organic:locked:withdraw');
    keyboard.row();
  }

  keyboard.text('Archive Booster', `organic:archive:${order.id}`);
  keyboard.row();
  keyboard.text('ÃƒÂ¢Ã‚Â¬Ã¢â‚¬Â¦ÃƒÂ¯Ã‚Â¸Ã‚Â Back', 'nav:volume');
  keyboard.text('ÃƒÂ°Ã…Â¸Ã‚ÂÃ‚Â  Home', 'nav:home');
  return keyboard;
}

function makeOrganicVolumeArchiveKeyboardLegacy(user) {
  const keyboard = new InlineKeyboard();
  const archivedBoosters = getVisibleAppleBoosters(user, { archived: true });

  for (const order of archivedBoosters) {
    keyboard.text(appleBoosterListLabel(order), `organic:open:${order.id}`);
    keyboard.row();
  }

  keyboard.text('Back', 'nav:volume');
  keyboard.text('Home', 'nav:home');
  keyboard.row().text('Refresh', 'refresh:volume_archive');
  return keyboard;
}

function makeOrganicVolumeOrderKeyboard(user) {
  const keyboard = new InlineKeyboard();
  const order = user.organicVolumeOrder;
  const isBundled = order.strategy === 'bundled';
  const isTrial = Boolean(order.freeTrial);

  if (order.archivedAt) {
    keyboard.text('\u267B\uFE0F Restore Booster', `organic:restore:${order.id}`);
    keyboard.row();
    keyboard.text(
      order.deleteConfirmations >= 1 ? '\u{1F6A8} Confirm Permanent Delete' : '\u{1F5D1}\uFE0F Delete Permanently',
      `organic:delete:${order.id}`,
    );
    keyboard.row();
    keyboard.text('\u2B05\uFE0F Back', 'nav:volume_archive');
    keyboard.text('\u{1F3E0} Home', 'nav:home');
    keyboard.row().text('\u{1F504} Refresh', 'refresh:volume_order');
    return keyboard;
  }

  keyboard.text('\u{1F504} Refresh Balance', 'organic:refresh');
  keyboard.row();
  if (!isBundled && !isTrial) {
    keyboard.text(
      Number.isInteger(order.appleBooster.walletCount)
        ? `\u{1F45B} Wallets: ${order.appleBooster.walletCount}`
        : '\u{1F45B} Set Worker Wallet Count',
      'organic:set:wallet_count',
    );
    keyboard.row();
  }
  keyboard.text(
    isBundled ? '\u{1FA99} Set Bundle Mint' : '\u{1FA99} Set Mint',
    'organic:set:mint',
  );
  if (!isTrial) {
    keyboard.text(
      isBundled ? '\u{1F4B8} Set Bundle Size' : '\u{1F4B8} Set Swap Range',
      'organic:set:swap_range',
    );
    keyboard.row();
    keyboard.text(
      isBundled ? '\u23F1\uFE0F Set Delay Range' : '\u23F1\uFE0F Set Interval Range',
      'organic:set:interval_range',
    );
    keyboard.row();
  } else {
    keyboard.row();
  }

  if (order.funded && organicBoosterIsConfigured(order)) {
    keyboard.text(
      order.appleBooster.stopRequested
        ? (isBundled ? '\u23F3 Stopping Bundle Engine' : '\u23F3 Stopping + Sweeping')
        : (order.running
          ? (isTrial
            ? '\u23F9\uFE0F Stop Free Trial'
            : (isBundled ? '\u23F9\uFE0F Stop Bundled Booster' : '\u23F9\uFE0F Stop + Sweep'))
          : (isTrial
            ? '\u25B6\uFE0F Start Free Trial'
            : (isBundled ? '\u25B6\uFE0F Start Bundled Booster' : '\u25B6\uFE0F Start Booster'))),
      order.running ? 'organic:stop' : 'organic:start',
    );
    if (!isTrial) {
      keyboard.text(
        isBundled ? '\u{1F4BC} Withdraw Deposit SOL' : '\u{1F4BC} Withdraw SOL',
        'organic:withdraw',
      );
    }
    keyboard.row();
  } else if (order.funded) {
    keyboard.text(
      isTrial
        ? '\u{1F512} Start Free Trial'
        : (isBundled ? '\u{1F512} Start Bundled Booster' : '\u{1F512} Start Booster'),
      'organic:locked:start',
    );
    if (!isTrial) {
      keyboard.text(
        isBundled ? '\u{1F4BC} Withdraw Deposit SOL' : '\u{1F4BC} Withdraw SOL',
        'organic:withdraw',
      );
    }
    keyboard.row();
  } else {
    keyboard.text(
      isTrial
        ? '\u{1F512} Start Free Trial'
        : (isBundled ? '\u{1F512} Start Bundled Booster' : '\u{1F512} Start Booster'),
      'organic:locked:start',
    );
    keyboard.text(
      isTrial
        ? '\u{1F512} Stop Free Trial'
        : (isBundled ? '\u{1F512} Stop Bundled Booster' : '\u{1F512} Stop Booster'),
      'organic:locked:stop',
    );
    keyboard.row();
    if (!isTrial) {
      keyboard.text(
        isBundled ? '\u{1F512} Withdraw Deposit SOL' : '\u{1F512} Withdraw SOL',
        'organic:locked:withdraw',
      );
      keyboard.row();
    }
  }

  keyboard.text(
    isTrial
      ? '\u{1F5C4}\uFE0F Archive Trial'
      : (isBundled ? '\u{1F5C4}\uFE0F Archive Bundle' : '\u{1F5C4}\uFE0F Archive Booster'),
    `organic:archive:${order.id}`,
  );
  keyboard.row();
  keyboard.text('\u2B05\uFE0F Back', 'nav:volume');
  keyboard.text('\u{1F3E0} Home', 'nav:home');
  return keyboard;
}

function makeOrganicVolumeArchiveKeyboard(user) {
  const keyboard = new InlineKeyboard();
  const archivedBoosters = getVisibleAppleBoosters(user, { archived: true });

  for (const order of archivedBoosters) {
    keyboard.text(appleBoosterListLabel(order), `organic:open:${order.id}`);
    keyboard.row();
  }

  keyboard.text('\u2B05\uFE0F Back', 'nav:volume');
  keyboard.text('\u{1F3E0} Home', 'nav:home');
  keyboard.row().text('\u{1F504} Refresh', 'refresh:volume_archive');
  return keyboard;
}

function organicVolumeOrderTextLegacyCurrent(user) {
  const order = user.organicVolumeOrder;
  const pkg = getAppleBoosterPackage(order.strategy, order.packageKey);
  const isBundled = order.strategy === 'bundled';
  const remainingLamports = Math.max(0, (order.requiredLamports ?? 0) - (order.currentLamports ?? 0));
  const booster = order.appleBooster;
  const totalManagedLamports = Number.isInteger(booster.totalManagedLamports)
    ? booster.totalManagedLamports
    : order.currentLamports;
  const runtimeEstimate = approximateAppleBoosterRuntime(booster, totalManagedLamports);
  const totalFeeBps = [booster.lpFeeBps, booster.protocolFeeBps, booster.creatorFeeBps]
    .filter((value) => Number.isInteger(value))
    .reduce((sum, value) => sum + value, 0);
  const marketLabel = booster.marketPhase === 'bonded_pool'
    ? 'Bonded / graduated'
    : (booster.marketPhase === 'bonding_curve' ? 'Not bonded yet' : 'Unknown');
  const packageTargetUsd = parseOrganicPackageTargetUsd(pkg?.label);
  const approximateVolumeLamports = (booster.totalBuyInputLamports || 0) + (booster.totalSellOutputLamports || 0);
  const approximateVolumeUsd = Number.isFinite(solUsdRateCache) && solUsdRateCache > 0
    ? (approximateVolumeLamports / LAMPORTS_PER_SOL) * solUsdRateCache
    : null;
  const approximateVolumeRemainingUsd = Number.isFinite(packageTargetUsd) && Number.isFinite(approximateVolumeUsd)
    ? Math.max(0, packageTargetUsd - approximateVolumeUsd)
    : null;
  const approximateVolumeProgressPercent = Number.isFinite(packageTargetUsd) && packageTargetUsd > 0 && Number.isFinite(approximateVolumeUsd)
    ? Math.min(100, (approximateVolumeUsd / packageTargetUsd) * 100)
    : null;
  const orderStatusLabel = order.archivedAt
    ? 'ÃƒÂ¢Ã…Â¡Ã‚Â« Archived'
    : (order.funded
      ? (order.running ? 'ÃƒÂ°Ã…Â¸Ã…Â¸Ã‚Â¢ Running' : 'ÃƒÂ°Ã…Â¸Ã…Â¸Ã‚Â¡ Funded')
      : 'ÃƒÂ°Ã…Â¸Ã…Â¸Ã‚Â  Awaiting Deposit');
  const boosterStatusLabel = order.running
    ? (booster.status || 'running')
    : (booster.stopRequested ? 'Stopping + Sweeping' : 'Stopped');

  return [
    isBundled ? 'ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‚Â¦ *Bundled Apple Booster*' : 'ÃƒÂ°Ã…Â¸Ã‚ÂÃ‚Â *Organic Apple Booster*',
    '',
    'ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‚Â¦ *Order*',
    pkg ? `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Package: ${pkg.emoji} *${pkg.label}*` : 'ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Package: *Not selected*',
    `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Status: *${orderStatusLabel}*`,
    '',
    'ÃƒÂ°Ã…Â¸Ã¢â‚¬â„¢Ã‚Â³ *Funding*',
    order.requiredSol ? `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Required deposit: *${order.requiredSol} SOL*` : 'ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Required deposit: *Pending*',
    order.rebateSol ? `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Creator rebate: *${order.rebateSol} SOL*` : null,
    order.walletAddress ? 'ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Deposit wallet:' : 'ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Deposit wallet: *Not ready*',
    order.walletAddress ? `\`${order.walletAddress}\`` : null,
    `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Deposit balance: *${order.currentSol || '0'} SOL*`,
    `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Managed balance: *${formatSolAmountFromLamports(totalManagedLamports)} SOL*`,
    `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Left to fund: *${formatSolAmountFromLamports(remainingLamports)} SOL*`,
    '',
    'ÃƒÂ¢Ã…Â¡Ã¢â€žÂ¢ÃƒÂ¯Ã‚Â¸Ã‚Â *Booster Setup*',
    ...(isBundled ? [] : [`ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Worker wallets: *${booster.walletCount || 'Not set'}*`]),
    `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Mint: ${booster.mintAddress ? `\`${booster.mintAddress}\`` : '*Not set*'}`,
    `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Swap range: *${formatSolRange(booster.minSwapSol, booster.maxSwapSol)}*`,
    `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Interval: *${formatSecondRange(booster.minIntervalSeconds, booster.maxIntervalSeconds)}*`,
    `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Bot status: *${boosterStatusLabel}*`,
    ...(isBundled ? ['ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Execution: *Jito bundled same-block buy + sell*'] : []),
    '',
    'ÃƒÂ°Ã…Â¸Ã…â€™Ã‚Â *Market & Costs*',
    `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Market: *${marketLabel}*`,
    booster.marketCapSol ? `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Market cap: *${booster.marketCapSol} SOL*` : null,
    Number.isInteger(totalFeeBps) && totalFeeBps > 0
      ? `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Fee profile: LP ${formatBpsPercent(booster.lpFeeBps)} + protocol ${formatBpsPercent(booster.protocolFeeBps)} + creator ${formatBpsPercent(booster.creatorFeeBps)}`
      : 'ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Fee estimate: *Waiting for mint + market data*',
    Number.isInteger(booster.estimatedCycleFeeLamports)
      ? `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Cycle cost: *${formatSolAmountFromLamports(booster.estimatedCycleFeeLamports)} SOL*`
      : null,
    Number.isInteger(booster.estimatedTradeFeeLamports)
      ? `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Trade fees: *${formatSolAmountFromLamports(booster.estimatedTradeFeeLamports)} SOL*`
      : null,
    Number.isInteger(booster.estimatedNetworkFeeLamports)
      ? `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Network gas: *${formatSolAmountFromLamports(booster.estimatedNetworkFeeLamports)} SOL*`
      : null,
    ...(!isTrial && (Number.isFinite(packageTargetUsd) || Number.isFinite(approximateVolumeUsd))
      ? [
        '',
        'ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‹â€  *Progress*',
        Number.isFinite(approximateVolumeProgressPercent)
          ? `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Completion: *${approximateVolumeProgressPercent.toFixed(1).replace(/\.0$/, '')}%* ${formatProgressBar(approximateVolumeProgressPercent)}`
          : null,
        Number.isFinite(packageTargetUsd)
          ? `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Package target: *${formatUsdCompact(packageTargetUsd)}*`
          : null,
        Number.isFinite(approximateVolumeUsd)
          ? `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Volume completed: *${formatUsdCompact(approximateVolumeUsd)}*`
          : null,
        Number.isFinite(approximateVolumeRemainingUsd)
          ? `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Volume left: *${formatUsdCompact(approximateVolumeRemainingUsd)}*`
          : null,
        approximateVolumeLamports > 0
          ? `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ SOL throughput: *${formatSolAmountFromLamports(approximateVolumeLamports)} SOL*`
          : null,
      ].filter(Boolean)
      : []),
    '',
    'ÃƒÂ¢Ã‚ÂÃ‚Â±ÃƒÂ¯Ã‚Â¸Ã‚Â *Runtime & Stats*',
    Number.isInteger(runtimeEstimate.cyclesRemaining)
      ? `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Cycles left: *${runtimeEstimate.cyclesRemaining}*`
      : null,
    Number.isInteger(runtimeEstimate.runtimeSeconds)
      ? `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Runtime left: *${formatDurationCompact(runtimeEstimate.runtimeSeconds)}*`
      : null,
    booster.nextActionAt ? `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Next action: ${formatTimestamp(booster.nextActionAt)}` : null,
    `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Cycles: buys *${booster.totalBuyCount || 0}*, sells *${booster.totalSellCount || 0}*, completed *${booster.cycleCount || 0}*`,
    `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Wallet flow: topped up *${formatSolAmountFromLamports(booster.totalTopUpLamports || 0)} SOL*, swept *${formatSolAmountFromLamports(booster.totalSweptLamports || 0)} SOL*`,
    booster.lastBuySignature ? `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Last buy tx: \`${booster.lastBuySignature}\`` : null,
    booster.lastSellSignature ? `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Last sell tx: \`${booster.lastSellSignature}\`` : null,
    booster.lastError ? `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Booster error: \`${booster.lastError}\`` : null,
    ...(Array.isArray(booster.workerWallets) && booster.workerWallets.length > 0
      ? [
        '',
        'ÃƒÂ°Ã…Â¸Ã¢â‚¬ËœÃ¢â‚¬Âº *Worker Wallets*',
        ...booster.workerWallets.map((worker, index) => formatOrganicWorkerLabel(worker, index)),
      ]
      : []),
    ...(order.lastBalanceCheckAt || order.lastError
      ? [
        '',
        'ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂºÃ‚Â°ÃƒÂ¯Ã‚Â¸Ã‚Â *Checks*',
        order.lastBalanceCheckAt ? `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Last checked: ${formatTimestamp(order.lastBalanceCheckAt)}` : null,
        order.lastError ? `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Last error: \`${order.lastError}\`` : null,
      ].filter(Boolean)
      : []),
    '',
    'ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‚Â *Notes*',
    order.funded
      ? (organicBoosterIsConfigured(order)
        ? (isBundled
          ? 'ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Bundled mode uses Jito bundle execution from the deposit wallet.'
          : 'ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ The deposit wallet funds worker wallets automatically.')
        : (isBundled
          ? 'ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Finish the mint, swap range, and interval to unlock bundled execution.'
          : 'ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Finish worker wallets, mint, swap range, and interval to unlock the booster.'))
      : 'ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Fund the deposit wallet above, then tap *Refresh Balance*.',
    isBundled
      ? 'ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Bundled mode uses same-block buy and sell bundles through Jito.'
      : 'ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Only fund the deposit wallet. Worker wallets are internal.',
    booster.marketPhase === 'bonding_curve'
      ? 'ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Not bonded yet: pre-bonding curve trades usually cost more.'
      : (booster.marketPhase === 'bonded_pool'
        ? 'ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Bonded: graduated pool routing is usually cheaper.'
        : 'ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Market data appears after the worker reads mint routing.'),
    order.running
      ? 'ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Stop sells back to SOL and sweeps worker wallets home.'
      : 'ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Tap *Refresh Balance* anytime to redraw the latest worker stats.',
  ].filter(Boolean).join('\n');
}

function organicVolumeOrderText(user) {
  const order = user.organicVolumeOrder;
  const pkg = getAppleBoosterPackage(order.strategy, order.packageKey);
  const isBundled = order.strategy === 'bundled';
  const isTrial = Boolean(order.freeTrial);
  const remainingLamports = Math.max(0, (order.requiredLamports ?? 0) - (order.currentLamports ?? 0));
  const booster = order.appleBooster;
  const totalManagedLamports = Number.isInteger(booster.totalManagedLamports)
    ? booster.totalManagedLamports
    : order.currentLamports;
  const runtimeEstimate = approximateAppleBoosterRuntime(booster, totalManagedLamports);
  const totalFeeBps = [booster.lpFeeBps, booster.protocolFeeBps, booster.creatorFeeBps]
    .filter((value) => Number.isInteger(value))
    .reduce((sum, value) => sum + value, 0);
  const marketLabel = booster.marketPhase === 'bonded_pool'
    ? 'Bonded / graduated'
    : (booster.marketPhase === 'bonding_curve' ? 'Not bonded yet' : 'Unknown');
  const packageTargetUsd = parseOrganicPackageTargetUsd(pkg?.label);
  const approximateVolumeLamports = (booster.totalBuyInputLamports || 0) + (booster.totalSellOutputLamports || 0);
  const approximateVolumeUsd = Number.isFinite(solUsdRateCache) && solUsdRateCache > 0
    ? (approximateVolumeLamports / LAMPORTS_PER_SOL) * solUsdRateCache
    : null;
  const approximateVolumeRemainingUsd = Number.isFinite(packageTargetUsd) && Number.isFinite(approximateVolumeUsd)
    ? Math.max(0, packageTargetUsd - approximateVolumeUsd)
    : null;
  const approximateVolumeProgressPercent = Number.isFinite(packageTargetUsd) && packageTargetUsd > 0 && Number.isFinite(approximateVolumeUsd)
    ? Math.min(100, (approximateVolumeUsd / packageTargetUsd) * 100)
    : null;
  const totalTradeLegs = (booster.totalBuyCount || 0) + (booster.totalSellCount || 0);
  const orderStatusLabel = order.archivedAt
    ? '\u26AB Archived'
    : (order.funded
      ? (order.running ? '\u{1F7E2} Running' : (isTrial ? '\u{1F7E1} Ready To Start' : '\u{1F7E1} Funded'))
      : '\u{1F7E0} Awaiting Deposit');
  const boosterStatusLabel = order.running
    ? (booster.status || 'running')
    : (booster.stopRequested
      ? (isBundled ? 'Stopping bundle engine' : 'Stopping + Sweeping')
      : 'Stopped');
  const title = isTrial
    ? '\u{1F9EA} *Volume Bot Trial*'
    : (isBundled ? '\u{1F4E6} *Bundled Volume Booster*' : '\u{1F34F} *Organic Volume Booster*');
  const setupHeader = isBundled ? '\u2699\uFE0F *Bundle Setup*' : '\u2699\uFE0F *Booster Setup*';
  const fundingHeader = isTrial
    ? '\u{1F4B3} *Trial Funding*'
    : (isBundled ? '\u{1F4B3} *Deposit Wallet*' : '\u{1F4B3} *Funding*');
  const runtimeHeader = isBundled ? '\u23F1\uFE0F *Bundle Stats*' : '\u23F1\uFE0F *Runtime & Stats*';
  const notesHeader = isBundled ? '\u{1F4DD} *Bundle Notes*' : '\u{1F4DD} *Notes*';
  const walletFlowLine = isBundled
    ? `\u2022 Bundle flow: *${booster.totalBuyCount || 0}* landed buy/sell bundles`
    : `\u2022 Wallet flow: topped up *${formatSolAmountFromLamports(booster.totalTopUpLamports || 0)} SOL*, swept *${formatSolAmountFromLamports(booster.totalSweptLamports || 0)} SOL*`;

  return [
    title,
    '',
    '\u{1F4E6} *Order*',
    isTrial
      ? '\u2022 Package: \u{1F9EA} *Volume Bot Trial*'
      : (pkg ? `\u2022 Package: ${pkg.emoji} *${pkg.label}*` : '\u2022 Package: *Not selected*'),
    `\u2022 Status: *${orderStatusLabel}*`,
    '',
    fundingHeader,
    isTrial ? '\u2022 Your cost: *Free*' : (order.requiredSol ? `\u2022 Required deposit: *${order.requiredSol} SOL*` : '\u2022 Required deposit: *Pending*'),
    isTrial ? `\u2022 Demo goal: *${order.trialTradeGoal || cfg.volumeTrialTradeGoal} tiny live trade legs*` : (order.rebateSol ? `\u2022 Creator rebate: *${order.rebateSol} SOL*` : null),
    isTrial ? '\u2022 Demo wallet: *Managed automatically*' : (order.walletAddress ? '\u2022 Deposit wallet:' : '\u2022 Deposit wallet: *Not ready*'),
    isTrial ? null : (order.walletAddress ? `\`${order.walletAddress}\`` : null),
    `${isTrial ? '\u2022 Trial balance' : '\u2022 Deposit balance'}: *${order.currentSol || '0'} SOL*`,
    `\u2022 Managed balance: *${formatSolAmountFromLamports(totalManagedLamports)} SOL*`,
    isTrial ? null : `\u2022 Left to fund: *${formatSolAmountFromLamports(remainingLamports)} SOL*`,
    '',
    setupHeader,
    ...(isBundled || isTrial ? [] : [`\u2022 Worker wallets: *${booster.walletCount || 'Not set'}*`]),
    `\u2022 Mint: ${booster.mintAddress ? `\`${booster.mintAddress}\`` : '*Not set*'}`,
    `\u2022 ${isBundled ? 'Bundle size' : 'Swap range'}: *${formatSolRange(booster.minSwapSol, booster.maxSwapSol)}*`,
    `\u2022 ${isBundled ? 'Delay range' : 'Interval'}: *${formatSecondRange(booster.minIntervalSeconds, booster.maxIntervalSeconds)}*`,
    `\u2022 Bot status: *${boosterStatusLabel}*`,
    ...(isBundled ? ['\u2022 Execution: *Jito same-slot buy + sell bundle*'] : []),
    '',
    '\u{1F310} *Market & Costs*',
    `\u2022 Market: *${marketLabel}*`,
    booster.marketCapSol ? `\u2022 Market cap: *${booster.marketCapSol} SOL*` : null,
    Number.isInteger(totalFeeBps) && totalFeeBps > 0
      ? `\u2022 Fee profile: LP ${formatBpsPercent(booster.lpFeeBps)} + protocol ${formatBpsPercent(booster.protocolFeeBps)} + creator ${formatBpsPercent(booster.creatorFeeBps)}`
      : '\u2022 Fee estimate: *Waiting for mint + market data*',
    Number.isInteger(booster.estimatedCycleFeeLamports)
      ? `\u2022 Cycle cost: *${formatSolAmountFromLamports(booster.estimatedCycleFeeLamports)} SOL*`
      : null,
    Number.isInteger(booster.estimatedTradeFeeLamports)
      ? `\u2022 Trade fees: *${formatSolAmountFromLamports(booster.estimatedTradeFeeLamports)} SOL*`
      : null,
    Number.isInteger(booster.estimatedNetworkFeeLamports)
      ? `\u2022 Network gas: *${formatSolAmountFromLamports(booster.estimatedNetworkFeeLamports)} SOL*`
      : null,
    ...(isTrial
      ? [
        '',
        '\u{1F4C8} *Demo Progress*',
        `\u2022 Trade legs completed: *${totalTradeLegs} / ${order.trialTradeGoal || cfg.volumeTrialTradeGoal}*`,
        `\u2022 Buy legs: *${booster.totalBuyCount || 0}*`,
        `\u2022 Sell legs: *${booster.totalSellCount || 0}*`,
      ]
      : []),
    ...(Number.isFinite(packageTargetUsd) || Number.isFinite(approximateVolumeUsd)
      ? [
        '',
        '\u{1F4C8} *Progress*',
        Number.isFinite(approximateVolumeProgressPercent)
          ? `\u2022 Completion: *${approximateVolumeProgressPercent.toFixed(1).replace(/\.0$/, '')}%* ${formatProgressBar(approximateVolumeProgressPercent)}`
          : null,
        Number.isFinite(packageTargetUsd)
          ? `\u2022 Package target: *${formatUsdCompact(packageTargetUsd)}*`
          : null,
        Number.isFinite(approximateVolumeUsd)
          ? `\u2022 Volume completed: *${formatUsdCompact(approximateVolumeUsd)}*`
          : null,
        Number.isFinite(approximateVolumeRemainingUsd)
          ? `\u2022 Volume left: *${formatUsdCompact(approximateVolumeRemainingUsd)}*`
          : null,
        approximateVolumeLamports > 0
          ? `\u2022 SOL throughput: *${formatSolAmountFromLamports(approximateVolumeLamports)} SOL*`
          : null,
      ].filter(Boolean)
      : []),
    '',
    runtimeHeader,
    Number.isInteger(runtimeEstimate.cyclesRemaining)
      ? `\u2022 Cycles left: *${runtimeEstimate.cyclesRemaining}*`
      : null,
    Number.isInteger(runtimeEstimate.runtimeSeconds)
      ? `\u2022 Runtime left: *${formatDurationCompact(runtimeEstimate.runtimeSeconds)}*`
      : null,
    booster.nextActionAt ? `\u2022 Next action: ${formatTimestamp(booster.nextActionAt)}` : null,
    `\u2022 Cycles: buys *${booster.totalBuyCount || 0}*, sells *${booster.totalSellCount || 0}*, completed *${booster.cycleCount || 0}*`,
    walletFlowLine,
    booster.lastBuySignature ? `\u2022 Last buy tx: \`${booster.lastBuySignature}\`` : null,
    booster.lastSellSignature ? `\u2022 Last sell tx: \`${booster.lastSellSignature}\`` : null,
    booster.lastError ? `\u2022 Booster error: \`${booster.lastError}\`` : null,
    ...(!isTrial && Array.isArray(booster.workerWallets) && booster.workerWallets.length > 0
      ? [
        '',
        '\u{1F45B} *Worker Wallets*',
        ...booster.workerWallets.map((worker, index) => formatOrganicWorkerLabel(worker, index)),
      ]
      : []),
    ...(order.lastBalanceCheckAt || order.lastError
      ? [
        '',
        '\u{1F6F0}\uFE0F *Checks*',
        order.lastBalanceCheckAt ? `\u2022 Last checked: ${formatTimestamp(order.lastBalanceCheckAt)}` : null,
        order.lastError ? `\u2022 Last error: \`${order.lastError}\`` : null,
      ].filter(Boolean)
      : []),
    '',
    notesHeader,
    order.funded
      ? (organicBoosterIsConfigured(order)
        ? (isTrial
          ? '\u2022 Set the mint, then tap *Start Free Trial*. The trial will handle the tiny demo trades automatically.'
          : (isBundled
            ? '\u2022 Bundled mode executes from the deposit wallet using atomic Jito bundles.'
            : '\u2022 The deposit wallet funds the extra wallets automatically.'))
        : (isTrial
          ? '\u2022 Set the mint to unlock your free live demo.'
          : (isBundled
            ? '\u2022 Finish the mint, bundle size, and delay range to unlock bundled execution.'
            : '\u2022 Finish the wallet count, mint, trade range, and timing range to unlock the booster.')))
      : '\u2022 Fund the deposit wallet above, then tap *Refresh Balance*.',
    isTrial
      ? '\u2022 The demo keeps swap sizes tiny and runs about five real trade legs automatically.'
      : (isBundled
        ? '\u2022 Buy and sell are submitted together so they either land together or fail together.'
        : '\u2022 Only fund the deposit wallet. Worker wallets are internal.'),
    booster.marketPhase === 'bonding_curve'
      ? '\u2022 Not bonded yet: pre-bonding curve trades usually cost more.'
      : (booster.marketPhase === 'bonded_pool'
        ? '\u2022 Bonded: graduated pool routing is usually cheaper.'
        : '\u2022 Market data appears after the bot reads the mint and route.'),
    isTrial
      ? (order.running
        ? '\u2022 The trial auto-stops when its demo trade goal is reached.'
        : '\u2022 Withdraw is disabled for the trial demo.')
      : order.running
      ? (isBundled
        ? '\u2022 Stop halts new bundle submissions. Already-landed bundles settle as one unit.'
        : '\u2022 Stop sells back to SOL and sweeps worker wallets home.')
      : '\u2022 Tap *Refresh Balance* anytime to redraw the latest live stats.',
  ].filter(Boolean).join('\n');
}

function burnAgentCatalogText(user) {
  const activeAgents = getVisibleBurnAgents(user, { archived: false });
  return [
    'ÃƒÂ°Ã…Â¸Ã‚Â¤Ã¢â‚¬â€œ *Burn Agent Manager*',
    '',
    'ÃƒÂ¢Ã…Â¡Ã‚Â ÃƒÂ¯Ã‚Â¸Ã‚Â *High-risk wallet flow.* Anyone with a private key can drain funds and claim creator rewards.',
    'ÃƒÂ¢Ã…Â¡Ã‚Â ÃƒÂ¯Ã‚Â¸Ã‚Â Never reuse a sensitive team wallet here unless you understand the risk.',
    '',
    'Create a new agent below or open an existing one to edit it.',
    '',
    activeAgents.length > 0
      ? `Active agents: *${activeAgents.length}*`
      : 'No active burn agents yet.',
    getVisibleBurnAgents(user, { archived: true }).length > 0
      ? `Archived agents: *${getVisibleBurnAgents(user, { archived: true }).length}*`
      : 'Archived agents: *0*',
    '',
    'ÃƒÂ¢Ã…Â¡Ã‚Â¡ *Lightning Fast*: agent controls the creator wallet directly.',
    'ÃƒÂ°Ã…Â¸Ã‚ÂÃ‚Â¢ *Normal*: you route a chosen creator-fee share to a managed wallet, and the bot claims/swaps/burns from there.',
  ].join('\n');
}

function burnAgentArchiveText(user) {
  const archivedAgents = getVisibleBurnAgents(user, { archived: true });
  return [
    'ÃƒÂ°Ã…Â¸Ã¢â‚¬â€Ã¢â‚¬Å¾ÃƒÂ¯Ã‚Â¸Ã‚Â *Burn Agent Archive*',
    '',
    archivedAgents.length > 0
      ? 'Archived agents stay here until restored or permanently deleted.'
      : 'No archived agents yet.',
    '',
    'Permanent delete is only available from inside an archived agent.',
  ].join('\n');
}

function burnAgentEditorTextLegacy(user, balanceLamports = null) {
  const agent = user.burnAgent;
  const lines = [
    'ÃƒÂ°Ã…Â¸Ã‚Â¤Ã¢â‚¬â€œ *Burn Agent*',
    '',
    'ÃƒÂ¢Ã…Â¡Ã‚Â ÃƒÂ¯Ã‚Â¸Ã‚Â *Serious wallet warning:* this flow can control funds, creator rewards, and token burns.',
    'ÃƒÂ¢Ã…Â¡Ã‚Â ÃƒÂ¯Ã‚Â¸Ã‚Â If you paste a private key here, treat that wallet as hot and operational.',
    '',
    `ÃƒÂ°Ã…Â¸Ã¢â‚¬Â Ã¢â‚¬Â Agent ID: \`${agent.id}\``,
    `ÃƒÂ¢Ã…Â¡Ã¢â€žÂ¢ÃƒÂ¯Ã‚Â¸Ã‚Â Speed: *${burnAgentSpeedLabel(agent.speed)}*`,
    `ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‚Â¡ Burn Bot: *${agent.automationEnabled ? 'Running' : 'Stopped'}*`,
    `ÃƒÂ°Ã…Â¸Ã¢â‚¬â„¢Ã‚Â¼ Wallet Mode: *${burnAgentWalletModeLabel(agent.walletMode)}*`,
    `ÃƒÂ°Ã…Â¸Ã‚ÂÃ‚Â¦ Agent Wallet: \`${agent.walletAddress || 'Not ready'}\``,
    `ÃƒÂ°Ã…Â¸Ã¢â‚¬â„¢Ã‚Â° Wallet Balance: *${Number.isInteger(balanceLamports) ? `${formatSolAmountFromLamports(balanceLamports)} SOL` : 'Unavailable'}*`,
  ];

  if (burnAgentNeedsWalletChoice(user)) {
    lines.push(
      '',
      'Choose how the creator wallet will be handled:',
      'ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ *Generate Wallet*: we create a wallet for you, and you must mint the coin from that wallet.',
      'ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ *Provide My Own*: you paste the private key for the creator wallet you already control.',
    );
    return lines.join('\n');
  }

  if (agent.speed === 'lightning' && agent.walletMode === 'generated') {
    lines.push(
      '',
      'ÃƒÂ¢Ã…Â¡Ã‚Â ÃƒÂ¯Ã‚Â¸Ã‚Â *Mint the coin with this exact wallet.*',
      `Address: \`${agent.walletAddress}\``,
      'ÃƒÂ¢Ã…Â¡Ã‚Â ÃƒÂ¯Ã‚Â¸Ã‚Â If you regenerate this wallet, the old private key stops being valid for this agent.',
    );
  } else if (agent.speed === 'lightning' && agent.walletMode === 'provided') {
    lines.push(
      '',
      agent.walletAddress
        ? 'This agent can directly claim creator rewards because it holds the creator wallet key.'
        : 'Add the creator wallet private key you mint with.',
    );
  } else if (agent.speed === 'normal') {
    lines.push(
      '',
      'Normal mode uses a managed wallet. You designate a chosen creator-fee share to this wallet on Pump.fun, and the bot claims, swaps, and burns what lands there.',
      'The unmanaged remainder stays outside this bot on your side.',
    );
  }

  if (burnAgentHasStoredPrivateKey(agent)) {
    lines.push(
      '',
      `Stored private key: ${burnAgentPrivateKeyText(agent)}`,
      agent.privateKeyVisible
        ? 'Hide it again when you are done viewing it.'
        : 'Use the Show Private Key button only if you truly need to view it.',
    );
  }

  lines.push(
    '',
    `ÃƒÂ°Ã…Â¸Ã‚ÂªÃ¢â€žÂ¢ Mint: ${agent.mintAddress ? `\`${agent.mintAddress}\`` : 'Not set'}`,
  );

  if (isNormalBurnAgent(agent)) {
    lines.push('ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ‚Â¥ Burn share: *100% of the rewards routed to this agent*');
  } else {
    lines.push(
      `ÃƒÂ°Ã…Â¸Ã‚ÂÃ¢â‚¬ÂºÃƒÂ¯Ã‚Â¸Ã‚Â Treasury / keep wallet: ${agent.treasuryAddress ? `\`${agent.treasuryAddress}\`` : 'Not set'}`,
      `ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ‚Â¥ Burn share: ${Number.isInteger(agent.burnPercent) ? `*${agent.burnPercent}%*` : 'Not set'}`,
      `ÃƒÂ°Ã…Â¸Ã¢â‚¬â„¢Ã‚Â¸ Treasury share: ${Number.isInteger(agent.treasuryPercent) ? `*${agent.treasuryPercent}%*` : 'Not set'}`,
    );
  }

  if (agent.awaitingField) {
    lines.push('', `ÃƒÂ¢Ã‚ÂÃ‚Â³ ${burnAgentPromptLabel(agent.awaitingField)}. Send the value in chat now.`);
  } else if (burnAgentIsReady(agent)) {
    lines.push('', 'ÃƒÂ¢Ã…â€œÃ¢â‚¬Â¦ Agent setup is complete.');
  } else {
    lines.push('', 'Finish the missing fields below to complete this agent.');
  }

  if (agent.regenerateConfirmations > 0) {
    lines.push('', `ÃƒÂ°Ã…Â¸Ã…Â¡Ã‚Â¨ Wallet regenerate confirmation progress: *${agent.regenerateConfirmations}/3*`);
  }

  return lines.join('\n');
}

function burnAgentEditorText(user, balanceLamports = null) {
  const agent = user.burnAgent;
  const runtime = agent.runtime || {};
  const recentLogs = getRecentActivityLogs(user, { scopePrefix: `burn_agent:${agent.id}`, limit: 4 });
  const lines = [
    '*Burn Agent*',
    '',
    '*Serious wallet warning:* this flow can control funds, creator rewards, and token burns.',
    'If you paste a private key here, treat that wallet as hot and operational.',
    '',
    `Agent ID: \`${agent.id}\``,
    `Speed: *${burnAgentSpeedLabel(agent.speed)}*`,
    `Burn Bot: *${agent.automationEnabled ? 'Running' : 'Stopped'}*`,
    `Wallet Mode: *${burnAgentWalletModeLabel(agent.walletMode)}*`,
    `Agent Wallet: \`${agent.walletAddress || 'Not ready'}\``,
    `Wallet Balance: *${Number.isInteger(balanceLamports) ? `${formatSolAmountFromLamports(balanceLamports)} SOL` : 'Unavailable'}*`,
    `Token Name: *${agent.tokenName || 'Not set'}*`,
  ];

  if (burnAgentNeedsWalletChoice(user)) {
    lines.push(
      '',
      'Choose how the creator wallet will be handled:',
      '- *Generate Wallet*: we create a wallet for you, and you must mint the coin from that wallet.',
      "- *Provide My Own*: you paste the private key for the creator wallet you already control.",
    );
    return lines.join('\n');
  }

  if (agent.speed === 'lightning' && agent.walletMode === 'generated') {
    lines.push(
      '',
      '*Mint the coin with this exact wallet.*',
      `Address: \`${agent.walletAddress}\``,
      'Withdraw any SOL before regenerating this wallet. Regeneration is blocked until the balance is zero.',
      'If you regenerate this wallet, the old private key stops being valid for this agent.',
    );
  } else if (agent.speed === 'lightning' && agent.walletMode === 'provided') {
    lines.push(
      '',
      agent.walletAddress
        ? 'This agent can directly claim creator rewards because it holds the creator wallet key.'
        : 'Add the creator wallet private key you mint with.',
    );
  } else if (agent.speed === 'normal') {
    lines.push(
      '',
      'Normal mode uses a managed wallet. You designate a chosen creator-fee share to this wallet on Pump.fun, and the bot claims, swaps, and burns what lands there.',
      'The unmanaged remainder stays outside this bot on your side.',
    );
  }

  if (burnAgentHasStoredPrivateKey(agent)) {
    lines.push(
      '',
      `Stored private key: ${burnAgentPrivateKeyText(agent)}`,
      agent.privateKeyVisible
        ? 'Hide it again when you are done viewing it.'
        : 'Use the Show Private Key button only if you truly need to view it.',
    );
  }

  lines.push(
    '',
    `Mint: ${agent.mintAddress ? `\`${agent.mintAddress}\`` : 'Not set'}`,
  );

  if (isNormalBurnAgent(agent)) {
    lines.push('Burn share: *100% of the rewards routed to this agent*');
  } else {
    lines.push(
      `Treasury / keep wallet: ${agent.treasuryAddress ? `\`${agent.treasuryAddress}\`` : 'Not set'}`,
      `Burn share: ${Number.isInteger(agent.burnPercent) ? `*${agent.burnPercent}%*` : 'Not set'}`,
      `Treasury share: ${Number.isInteger(agent.treasuryPercent) ? `*${agent.treasuryPercent}%*` : 'Not set'}`,
    );
  }

  lines.push(
    '',
    '*Burn Stats*',
    `Claim checks: *${runtime.totalClaimChecks || 0}*`,
    `Claims completed: *${runtime.totalClaimCount || 0}*`,
    `Total claimed: *${formatSolAmountFromLamports(runtime.totalClaimedLamports || 0)} SOL*`,
    `Treasury payouts: *${runtime.totalTreasuryTransferCount || 0}*`,
    `Treasury sent: *${formatSolAmountFromLamports(runtime.totalTreasuryLamportsSent || 0)} SOL*`,
    `Buybacks executed: *${runtime.totalBuybackCount || 0}*`,
    `Buyback SOL used: *${formatSolAmountFromLamports(runtime.totalBuybackLamports || 0)} SOL*`,
    `Burns executed: *${runtime.totalBurnCount || 0}*`,
    `Raw amount burned: *${runtime.totalBurnedRawAmount || '0'}*`,
    runtime.lastVaultLamports ? `Current claimable rewards: *${formatSolAmountFromLamports(Number(runtime.lastVaultLamports) || 0)} SOL*` : null,
    runtime.lastCheckedAt ? `Last runtime check: ${formatTimestamp(runtime.lastCheckedAt)}` : null,
    runtime.lastBuybackMode ? `Last buyback route: *${runtime.lastBuybackMode}*` : null,
  );

  if (recentLogs.length > 0) {
    lines.push(
      '',
      '*Recent Activity*',
      ...recentLogs.map((entry) => formatActivityLogLine(entry)),
    );
  }

  if (agent.awaitingField) {
    lines.push('', `${burnAgentPromptLabel(agent.awaitingField)}. Send the value in chat now.`);
  } else if (burnAgentIsReady(agent)) {
    lines.push('', 'Agent setup is complete.');
  } else {
    lines.push('', 'Finish the missing fields below to complete this agent.');
  }

  lines.push('', 'Burn stats update automatically in the background. Tap Refresh to redraw the latest numbers in Telegram.');

  if (agent.regenerateConfirmations > 0) {
    lines.push('', `Wallet regenerate confirmation progress: *${agent.regenerateConfirmations}/3*`);
  }

  return lines.join('\n');
}

function holderBoosterTextLegacy() {
  return [
    'ÃƒÂ°Ã…Â¸Ã¢â‚¬ËœÃ‚Â¥ *Holder Booster*',
    '',
    'This flow is being added next.',
    '',
    'The home button is live now so we can wire the full Holder Booster setup behind it cleanly.',
  ].join('\n');
}

function holderBoosterStatusLabel(order) {
  switch (order.status) {
    case 'awaiting_funding':
      return 'ÃƒÂ°Ã…Â¸Ã…Â¸Ã‚Â  Awaiting Deposit';
    case 'ready':
      return 'ÃƒÂ°Ã…Â¸Ã…Â¸Ã‚Â¡ Ready';
    case 'processing':
      return 'ÃƒÂ°Ã…Â¸Ã…Â¸Ã‚Â¡ Processing';
    case 'completed':
      return 'ÃƒÂ°Ã…Â¸Ã…Â¸Ã‚Â¢ Completed';
    case 'failed':
      return 'ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ‚Â´ Failed';
    default:
      return 'ÃƒÂ¢Ã…Â¡Ã‚Âª Setup';
  }
}

function makeHolderBoosterKeyboard(user) {
  const keyboard = new InlineKeyboard();
  const order = user.holderBooster;

  keyboard.text('ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬Å¾ Refresh', 'holder:refresh');
  keyboard.row();
  keyboard.text(order.mintAddress ? 'ÃƒÂ°Ã…Â¸Ã‚ÂªÃ¢â€žÂ¢ Update Mint' : 'ÃƒÂ°Ã…Â¸Ã‚ÂªÃ¢â€žÂ¢ Set Mint', 'holder:set:mint');
  keyboard.text(
    Number.isInteger(order.holderCount) ? `ÃƒÂ°Ã…Â¸Ã¢â‚¬ËœÃ‚Â¥ Holders: ${order.holderCount}` : 'ÃƒÂ°Ã…Â¸Ã¢â‚¬ËœÃ‚Â¥ Set Holder Count',
    'holder:set:holder_count',
  );
  keyboard.row();
  keyboard.text('ÃƒÂ¢Ã…â€œÃ‚Â¨ New Holder Boost', 'holder:new');
  keyboard.row();
  keyboard.text('ÃƒÂ¢Ã‚Â¬Ã¢â‚¬Â¦ÃƒÂ¯Ã‚Â¸Ã‚Â Back', 'nav:home');
  keyboard.text('ÃƒÂ°Ã…Â¸Ã‚ÂÃ‚Â  Home', 'nav:home');
  return keyboard;
}

function holderBoosterText(user) {
  const order = user.holderBooster;
  const previewWallets = Array.isArray(order.childWallets) ? order.childWallets.slice(0, 5) : [];
  const requiredTokens = Number.isInteger(order.holderCount) ? String(order.holderCount) : 'Pending';
  const promptLine = order.awaitingField
    ? `${promptForHolderField(order.awaitingField)} Send it in chat now.`
    : null;

  return [
    'ÃƒÂ°Ã…Â¸Ã¢â‚¬ËœÃ‚Â¥ *Holder Booster*',
    '',
    `ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‚Â¦ Status: *${holderBoosterStatusLabel(order)}*`,
    `ÃƒÂ°Ã…Â¸Ã‚ÂªÃ¢â€žÂ¢ Mint: ${order.mintAddress ? `\`${order.mintAddress}\`` : '*Not set*'}`,
    `ÃƒÂ°Ã…Â¸Ã¢â‚¬ËœÃ‚Â¥ Holders requested: *${order.holderCount || 'Not set'}*`,
    order.walletAddress ? '' : null,
    order.walletAddress ? 'ÃƒÂ°Ã…Â¸Ã¢â‚¬â„¢Ã‚Â³ *Deposit Wallet*' : null,
    order.walletAddress ? `\`${order.walletAddress}\`` : null,
    order.walletAddress ? `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Required SOL: *${order.requiredSol || 'Pending'} SOL*` : null,
    order.walletAddress ? `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Required tokens: *${requiredTokens}*` : null,
    order.walletAddress ? `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ SOL balance: *${order.currentSol || '0'} SOL*` : null,
    order.walletAddress ? `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Token balance: *${order.currentTokenAmountDisplay || '0'}*` : null,
    order.walletAddress ? `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Fanout progress: *${order.processedWalletCount || 0}/${order.holderCount || 0}*` : null,
    previewWallets.length > 0 ? '' : null,
    previewWallets.length > 0 ? 'ÃƒÂ°Ã…Â¸Ã‚Â§Ã‚Â· *Recipient Wallet Preview*' : null,
    ...previewWallets.map((wallet, index) => `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ #${index + 1} \`${wallet.address}\``),
    order.childWallets.length > previewWallets.length
      ? `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ ...and *${order.childWallets.length - previewWallets.length}* more`
      : null,
    order.lastBalanceCheckAt ? '' : null,
    order.lastBalanceCheckAt ? `ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂºÃ‚Â°ÃƒÂ¯Ã‚Â¸Ã‚Â Last checked: ${formatTimestamp(order.lastBalanceCheckAt)}` : null,
    order.completedAt ? `ÃƒÂ¢Ã…â€œÃ¢â‚¬Â¦ Completed: ${formatTimestamp(order.completedAt)}` : null,
    order.lastError ? `ÃƒÂ¢Ã…Â¡Ã‚Â ÃƒÂ¯Ã‚Â¸Ã‚Â Last error: \`${order.lastError}\`` : null,
    '',
    promptLine || (
      order.walletAddress
        ? 'Deposit the exact SOL + token amount shown above. Holder Booster is a one-time fanout feature with no sweep-back path.'
        : 'Set the mint and holder count to generate the one-time deposit wallet.'
    ),
    order.walletAddress
      ? 'Any extra tokens left in the deposit wallet are not part of the payout flow, so deposit the exact token count only.'
      : null,
  ].filter(Boolean).join('\n');
}

function magicSellText() {
  return [
    'ÃƒÂ¢Ã…â€œÃ‚Â¨ *Magic Sell*',
    '',
    'This will be a separate bot flow.',
    '',
    'The home button is live now so we can build the Magic Sell setup and execution path cleanly next.',
  ].join('\n');
}

function magicSellPrivateKeyText(order) {
  if (!order?.walletSecretKeyBase58) {
    return '`Not stored`';
  }

  if (order.privateKeyVisible) {
    return `\`${order.walletSecretKeyBase58}\``;
  }

  return '`Hidden - tap Show Private Key to reveal it.`';
}

function magicSellStatusLabel(order) {
  if (order.archivedAt) {
    return 'ÃƒÂ¢Ã…Â¡Ã‚Â« Archived';
  }

  switch (order.status) {
    case 'waiting_target':
      return 'ÃƒÂ°Ã…Â¸Ã…Â¸Ã‚Â¡ Waiting For Target';
    case 'waiting_inventory':
      return 'ÃƒÂ°Ã…Â¸Ã…Â¸Ã‚Â  Waiting For Inventory';
    case 'selling':
      return 'ÃƒÂ°Ã…Â¸Ã…Â¸Ã‚Â¡ Selling';
    case 'running':
      return 'ÃƒÂ°Ã…Â¸Ã…Â¸Ã‚Â¢ Active';
    case 'stopped':
      return 'ÃƒÂ¢Ã…Â¡Ã‚Âª Stopped';
    case 'failed':
      return 'ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ‚Â´ Error';
    default:
      return order.automationEnabled ? 'ÃƒÂ°Ã…Â¸Ã…Â¸Ã‚Â¢ Active' : 'ÃƒÂ¢Ã…Â¡Ã‚Âª Setup';
  }
}

function magicSellListLabel(order) {
  const tokenLabel = order.tokenName?.trim()
    || (order.mintAddress ? `${order.mintAddress.slice(0, 4)}...${order.mintAddress.slice(-4)}` : 'Complete Setup');
  return `ÃƒÂ¢Ã…â€œÃ‚Â¨ ${tokenLabel} ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ ${magicSellStatusLabel(order)}`;
}

function magicSellIsReady(order) {
  return Boolean(
    order?.mintAddress
    && Number.isFinite(order?.targetMarketCapUsd)
    && Number.isInteger(order?.sellerWalletCount)
    && order.sellerWalletCount > 0,
  );
}

function makeMagicSellCatalogKeyboard(user) {
  const keyboard = new InlineKeyboard();
  const activeOrders = getVisibleMagicSells(user);

  keyboard.text('ÃƒÂ¢Ã…â€œÃ‚Â¨ New Magic Sell', 'magic:new');
  keyboard.row();

  for (const order of activeOrders) {
    keyboard.text(magicSellListLabel(order), `magic:open:${order.id}`);
    keyboard.row();
  }

  if (getVisibleMagicSells(user, { archived: true }).length > 0) {
    keyboard.text('ÃƒÂ°Ã…Â¸Ã¢â‚¬â€Ã¢â‚¬Å¾ÃƒÂ¯Ã‚Â¸Ã‚Â Archive', 'nav:magic_sell_archive');
    keyboard.row();
  }

  keyboard.text('ÃƒÂ¢Ã‚Â¬Ã¢â‚¬Â¦ÃƒÂ¯Ã‚Â¸Ã‚Â Back', 'nav:home');
  keyboard.text('ÃƒÂ°Ã…Â¸Ã‚ÂÃ‚Â  Home', 'nav:home');
  keyboard.row();
  keyboard.text('ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬Å¾ Refresh', 'refresh:magic_sell');
  return keyboard;
}

function makeMagicSellArchiveKeyboard(user) {
  const keyboard = new InlineKeyboard();
  const archivedOrders = getVisibleMagicSells(user, { archived: true });

  for (const order of archivedOrders) {
    keyboard.text(magicSellListLabel(order), `magic:open:${order.id}`);
    keyboard.row();
  }

  keyboard.text('ÃƒÂ¢Ã‚Â¬Ã¢â‚¬Â¦ÃƒÂ¯Ã‚Â¸Ã‚Â Back', 'nav:magic_sell');
  keyboard.text('ÃƒÂ°Ã…Â¸Ã‚ÂÃ‚Â  Home', 'nav:home');
  keyboard.row();
  keyboard.text('ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬Å¾ Refresh', 'refresh:magic_sell_archive');
  return keyboard;
}

function makeMagicSellEditorKeyboard(user) {
  const keyboard = new InlineKeyboard();
  const order = user.magicSell;

  keyboard.text('ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬Å¾ Refresh', 'magic:refresh');
  keyboard.row();
  keyboard.text(order.tokenName ? 'ÃƒÂ°Ã…Â¸Ã‚ÂÃ‚Â·ÃƒÂ¯Ã‚Â¸Ã‚Â Update Name' : 'ÃƒÂ°Ã…Â¸Ã‚ÂÃ‚Â·ÃƒÂ¯Ã‚Â¸Ã‚Â Set Token Name', `magic:set:token_name:${order.id}`);
  keyboard.text(order.mintAddress ? 'ÃƒÂ°Ã…Â¸Ã‚ÂªÃ¢â€žÂ¢ Update Mint' : 'ÃƒÂ°Ã…Â¸Ã‚ÂªÃ¢â€žÂ¢ Set Mint', `magic:set:mint:${order.id}`);
  keyboard.row();
  keyboard.text(
    Number.isFinite(order.targetMarketCapUsd)
      ? `ÃƒÂ°Ã…Â¸Ã…Â½Ã‚Â¯ MC: ${formatUsdCompact(order.targetMarketCapUsd)}`
      : 'ÃƒÂ°Ã…Â¸Ã…Â½Ã‚Â¯ Set Target MC',
    `magic:set:target_market_cap:${order.id}`,
  );
  keyboard.text(
    Array.isArray(order.whitelistWallets) && order.whitelistWallets.length > 0
      ? `ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂºÃ‚Â¡ÃƒÂ¯Ã‚Â¸Ã‚Â Whitelist: ${order.whitelistWallets.length}`
      : 'ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂºÃ‚Â¡ÃƒÂ¯Ã‚Â¸Ã‚Â Set Whitelist',
    `magic:set:whitelist:${order.id}`,
  );
  keyboard.row();
  keyboard.text(
    `ÃƒÂ°Ã…Â¸Ã¢â‚¬ËœÃ¢â‚¬Âº Seller Wallets: ${order.sellerWalletCount || MAGIC_SELL_DEFAULT_SELLER_WALLET_COUNT}`,
    `magic:set:seller_wallet_count:${order.id}`,
  );
  keyboard.text(
    order.privateKeyVisible ? 'ÃƒÂ°Ã…Â¸Ã¢â€žÂ¢Ã‹â€  Hide Private Key' : 'ÃƒÂ°Ã…Â¸Ã¢â‚¬ËœÃ‚ÂÃƒÂ¯Ã‚Â¸Ã‚Â Show Private Key',
    `magic:key:toggle:${order.id}`,
  );
  keyboard.row();
  keyboard.text(
    magicSellIsReady(order)
      ? (order.automationEnabled ? 'ÃƒÂ¢Ã‚ÂÃ‚Â¹ÃƒÂ¯Ã‚Â¸Ã‚Â Stop Magic Sell' : 'ÃƒÂ¢Ã¢â‚¬â€œÃ‚Â¶ÃƒÂ¯Ã‚Â¸Ã‚Â Start Magic Sell')
      : 'ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬â„¢ Start Magic Sell',
    magicSellIsReady(order) ? `magic:toggle:${order.id}` : 'magic:locked:toggle',
  );
  keyboard.row();

  if (order.archivedAt) {
    keyboard.text('ÃƒÂ¢Ã¢â€žÂ¢Ã‚Â»ÃƒÂ¯Ã‚Â¸Ã‚Â Restore', `magic:restore:${order.id}`);
    keyboard.text('ÃƒÂ°Ã…Â¸Ã¢â‚¬â€Ã¢â‚¬ËœÃƒÂ¯Ã‚Â¸Ã‚Â Delete', `magic:delete:${order.id}`);
    keyboard.row();
    keyboard.text('ÃƒÂ¢Ã‚Â¬Ã¢â‚¬Â¦ÃƒÂ¯Ã‚Â¸Ã‚Â Back', 'nav:magic_sell_archive');
  } else {
    keyboard.text('ÃƒÂ°Ã…Â¸Ã¢â‚¬â€Ã¢â‚¬Å¾ÃƒÂ¯Ã‚Â¸Ã‚Â Archive', `magic:archive:${order.id}`);
    keyboard.row();
    keyboard.text('ÃƒÂ¢Ã‚Â¬Ã¢â‚¬Â¦ÃƒÂ¯Ã‚Â¸Ã‚Â Back', 'nav:magic_sell');
  }

  keyboard.text('ÃƒÂ°Ã…Â¸Ã‚ÂÃ‚Â  Home', 'nav:home');
  keyboard.row();
  keyboard.text('ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬Å¾ Refresh', 'refresh:magic_sell_editor');
  return keyboard;
}

function magicSellCatalogText(user) {
  const activeOrders = getVisibleMagicSells(user);
  return [
    'ÃƒÂ¢Ã…â€œÃ‚Â¨ *Magic Sell*',
    '',
    'Sell into real buyer strength only after your chosen market cap is reached.',
    '',
    'ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ target market-cap trigger',
    'ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ whitelist wallet ignore list',
    'ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ fresh deposit wallet for inventory',
    'ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ rotated seller wallets for execution',
    'ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ sells 25% of each qualifying buy',
    '',
    activeOrders.length > 0
      ? `Active setups: *${activeOrders.length}*`
      : 'No Magic Sell setups yet. Create one to get a fresh deposit wallet.',
  ].join('\n');
}

function magicSellArchiveText(user) {
  const archivedOrders = getVisibleMagicSells(user, { archived: true });
  return [
    'ÃƒÂ°Ã…Â¸Ã¢â‚¬â€Ã¢â‚¬Å¾ÃƒÂ¯Ã‚Â¸Ã‚Â *Magic Sell Archive*',
    '',
    archivedOrders.length > 0
      ? 'Archived Magic Sell setups stay here until you restore or permanently delete them.'
      : 'No archived Magic Sell setups right now.',
  ].join('\n');
}

function magicSellEditorText(user) {
  const order = user.magicSell;
  const previewWallets = Array.isArray(order.sellerWallets) ? order.sellerWallets.slice(0, 4) : [];
  const whitelistPreview = Array.isArray(order.whitelistWallets) ? order.whitelistWallets.slice(0, 3) : [];
  const totalSellerLamports = Array.isArray(order.sellerWallets)
    ? order.sellerWallets.reduce((sum, wallet) => sum + (Number.isInteger(wallet.currentLamports) ? wallet.currentLamports : 0), 0)
    : 0;

  return [
    'ÃƒÂ¢Ã…â€œÃ‚Â¨ *Magic Sell*',
    '',
    'ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã¢â‚¬Â¹ *Setup*',
    `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Token: ${order.tokenName ? `*${order.tokenName}*` : '*Not set*'}`,
    `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Mint: ${order.mintAddress ? `\`${order.mintAddress}\`` : '*Not set*'}`,
    `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Status: *${magicSellStatusLabel(order)}*`,
    `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Target MC: *${Number.isFinite(order.targetMarketCapUsd) ? formatUsdCompact(order.targetMarketCapUsd) : 'Not set'}*`,
    `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Sell Rule: *${order.sellPercent || MAGIC_SELL_SELL_PERCENT}% of each qualifying buy*`,
    `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Min Buy: *${formatSolAmountFromLamports(order.minimumBuyLamports || MAGIC_SELL_MIN_BUY_LAMPORTS)} SOL*`,
    '',
    'ÃƒÂ°Ã…Â¸Ã¢â‚¬â„¢Ã‚Â³ *Deposit Wallet*',
    `\`${order.walletAddress}\``,
    `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ SOL on deposit wallet: *${order.currentSol || '0'} SOL*`,
    `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Token inventory: *${order.currentTokenAmountDisplay || '0'}*`,
    `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Stored private key: ${magicSellPrivateKeyText(order)}`,
    '',
    'ÃƒÂ°Ã…Â¸Ã‚ÂªÃ¢â‚¬Å¾ *Seller Wallet Pool*',
    `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Wallets: *${order.sellerWalletCount || MAGIC_SELL_DEFAULT_SELLER_WALLET_COUNT}*`,
    `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Seller SOL total: *${formatSolAmountFromLamports(totalSellerLamports)} SOL*`,
    `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Recommended gas buffer: *${formatSolAmountFromLamports(order.recommendedGasLamports || 0)} SOL*`,
    ...previewWallets.map((wallet, index) => `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ #${index + 1} \`${wallet.address.slice(0, 4)}...${wallet.address.slice(-4)}\` ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ ${wallet.currentSol || '0'} SOL ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ ${wallet.currentTokenAmountDisplay || '0'} tokens`),
    order.sellerWallets.length > previewWallets.length
      ? `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ ...and *${order.sellerWallets.length - previewWallets.length}* more seller wallets`
      : null,
    '',
    'ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂºÃ‚Â¡ÃƒÂ¯Ã‚Â¸Ã‚Â *Whitelist*',
    whitelistPreview.length > 0
      ? `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ ${whitelistPreview.map((item) => `\`${item.slice(0, 4)}...${item.slice(-4)}\``).join(', ')}`
      : 'ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ No wallets whitelisted',
    order.whitelistWallets.length > whitelistPreview.length
      ? `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ ...plus *${order.whitelistWallets.length - whitelistPreview.length}* more`
      : null,
    '',
    'ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‹â€  *Live Stats*',
    `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Current MC: *${Number.isFinite(order.currentMarketCapUsd) ? formatUsdCompact(order.currentMarketCapUsd) : 'Waiting for market data'}*`,
    `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Phase: *${order.marketPhase || 'Unknown'}*`,
    `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Qualifying buys seen: *${order.stats?.triggerCount || 0}*`,
    `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Sells executed: *${order.stats?.sellCount || 0}*`,
    `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Observed buy flow: *${formatSolAmountFromLamports(order.stats?.totalObservedBuyLamports || 0)} SOL*`,
    `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Sell target flow: *${formatSolAmountFromLamports(order.stats?.totalTargetSellLamports || 0)} SOL*`,
    order.lastBalanceCheckAt ? `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Last checked: ${formatTimestamp(order.lastBalanceCheckAt)}` : null,
    order.lastError ? `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Last error: \`${order.lastError}\`` : null,
    '',
    order.awaitingField
      ? promptForMagicSellField(order.awaitingField)
      : 'Deposit your token inventory into the wallet above and keep enough SOL available for seller-wallet gas. Magic Sell only activates after the target market cap is crossed.',
  ].filter(Boolean).join('\n');
}

function amountText(user) {
  const reactionEmoji = selectedButtonEmoji(user);
  return [
    'ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‚Â¦ *Choose Apple Bundle*',
    '',
    selectionSnapshot(user),
    '',
    `*Selected reaction:* ${reactionEmoji} ${buttonDisplay(user.selection.button)}`,
    '',
    '*Approx SOL pricing*',
    ...cfg.packageAmounts.map((amount) => {
      const bundle = getBundlePricing(amount);
      return `${reactionEmoji} ${amount} apples - ${bundleSolDisplay(amount, user)} - ${bundle.role}`;
    }),
    '',
    'Values shown here are approximate. Selecting a paid bundle locks in an exact SOL quote.',
  ].join('\n');
}

function paymentText(user) {
  if (!user.selection.amount) {
    return [
      'ÃƒÂ¢Ã¢â‚¬â€Ã…Â½ *Solana Checkout*',
      '',
      'Pick a bundle first so the bot can build a SOL quote.',
    ].join('\n');
  }

  if (user.selection.usingFreeTrial) {
    return [
      'ÃƒÂ°Ã…Â¸Ã…Â½Ã‚Â *Free Trial Checkout*',
      '',
      selectionSnapshot(user),
      '',
      trialIsAvailable(user)
        ? `No payment is required for the one-time x${cfg.freeTrialAmount} trial. Continue when you're ready to run.`
        : 'Your free trial has already been used. Pick a paid bundle instead.',
    ].join('\n');
  }

  if (isAdminUser(user.telegramId)) {
    return [
      'ÃƒÂ°Ã…Â¸Ã¢â‚¬ËœÃ¢â‚¬Ëœ *Admin Checkout Override*',
      '',
      selectionSnapshot(user),
      '',
      'This account is whitelisted. No SOL payment is required for admin launches.',
    ].join('\n');
  }

  if (!cfg.solanaReceiveAddress) {
    return [
      'ÃƒÂ¢Ã¢â‚¬â€Ã…Â½ *Solana Checkout*',
      '',
      'Payment is unavailable because `SOLANA_RECEIVE_ADDRESS` is not configured yet.',
      '',
      selectionSnapshot(user),
    ].join('\n');
  }

  const statusLine = paymentIsReady(user)
    ? 'Payment found on-chain. This bundle is unlocked.'
    : quoteExpired(user.payment)
      ? 'This quote has expired. Generate a fresh quote before sending SOL.'
      : 'Send the exact SOL amount below, then tap *Check Payment* when it lands.';

  return [
    'ÃƒÂ¢Ã¢â‚¬â€Ã…Â½ *Solana Checkout*',
    '',
    selectionSnapshot(user),
    '',
    statusLine,
    '',
    ...paymentDetailsLines(user),
    '',
    'Only native SOL transfers to the wallet above are matched.',
  ].join('\n');
}

function targetText(user) {
  return [
    'ÃƒÂ°Ã…Â¸Ã…Â½Ã‚Â¯ *Target Selection*',
    '',
    selectionSnapshot(user),
    '',
    user.selection.button || user.selection.amount
      ? 'Paste the full target URL in your next message, or go back to keep the current target.'
      : 'Send the full target URL in your next message to begin the Reaction Booster flow.',
  ].join('\n');
}

function hasCustomTarget(user) {
  return Boolean(user.selection.target && user.selection.target !== cfg.defaultTarget);
}

function startBackRoute(user) {
  if (user.selection.amount) {
    return 'nav:amount';
  }

  return hasCustomTarget(user) ? 'nav:target' : 'nav:home';
}

function targetBackRoute(user) {
  if (user.selection.button && user.selection.amount) {
    return hasLaunchAccess(user) ? 'nav:confirm' : 'nav:payment';
  }

  if (user.selection.button) {
    return 'nav:start';
  }

  return 'nav:home';
}

function confirmText(user) {
  const paymentNote = isAdminUser(user.telegramId)
    ? 'Support access is active. Launch is unlocked.'
    : hasLaunchAccess(user)
      ? 'Everything is set. Launch is unlocked.'
      : 'Finish checkout first, then come back here to launch.';

  return [
    'ÃƒÂ°Ã…Â¸Ã…Â¡Ã¢â€šÂ¬ *Confirm Test Run*',
    '',
    selectionSnapshot(user),
    '',
    paymentNote,
    ...(user.payment.matchedSignature ? ['', `Matched tx: \`${user.payment.matchedSignature}\``] : []),
  ].join('\n');
}

function statusText(user) {
  const recentLogs = getRecentActivityLogs(user, { limit: 8 });
  return [
    'ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã…Â  *Session Snapshot*',
    '',
    selectionSnapshot(user),
    '',
    ...paymentDetailsLines(user),
    ...(recentLogs.length > 0
      ? [
        '',
        '*Recent Activity*',
        ...recentLogs.map((entry) => formatActivityLogLine(entry)),
      ]
      : []),
  ].join('\n');
}

function helpText() {
  return [
    'ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¹ÃƒÂ¯Ã‚Â¸Ã‚Â *How To Use This Bot*',
    '',
    `1. Open the ${BRAND_NAME} menu.`,
    '2. Pick one of the four configured button profiles.',
    `3. Use the one-time x${cfg.freeTrialAmount} trial or choose a paid apple bundle.`,
    '4. Paid bundles snapshot the live SOL rate and produce an exact quote.',
    '5. Send native SOL to the wallet shown, or skip checkout if you are using the free trial.',
    '6. Confirm payment, then launch the run.',
    '',
    '*Safety guardrails*',
    `ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Free trial is limited to one run per Telegram account at x${cfg.freeTrialAmount}`,
    'ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Payment is tied to the selected bundle size',
    'ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ A paid quote is consumed after a run, so the next run needs a fresh payment',
    'ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ You can use the default target or enter a custom URL before paying',
    'ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Runner uses Steel proxying and CAPTCHA solving for paid and trial runs',
  ].join('\n');
}

// Professional UI overrides. These keep the Telegram-facing copy clean even if
// older text constants/functions in the file were previously mojibaked.
const MENU_DIVIDER = '\u2501'.repeat(25);
BUTTONS.rocket.emoji = '\u{1F680}';
BUTTONS.fire.emoji = '\u{1F525}';

getScreenMediaPath = function getScreenMediaPath(route) {
  switch (route) {
    case 'home':
    case 'help':
    case 'status':
      return MENU_LOGO_IMAGE_PATH;
    case 'start':
    case 'amount':
    case 'payment':
    case 'confirm':
    case 'target':
    case 'help_reaction':
      return REACTION_MENU_IMAGE_PATH;
    case 'volume':
    case 'volume_organic':
    case 'volume_archive':
    case 'volume_bundled':
    case 'volume_order':
    case 'help_volume':
      return VOLUME_MENU_IMAGE_PATH;
    case 'burn_agent':
    case 'burn_agent_archive':
    case 'burn_agent_editor':
    case 'help_burn_agent':
      return BURN_AGENT_MENU_IMAGE_PATH;
    case 'holder_booster':
    case 'help_holder_booster':
      return HOLDER_BOOSTER_MENU_IMAGE_PATH;
    case 'fomo_booster':
    case 'help_fomo_booster':
      return FOMO_MENU_IMAGE_PATH;
    case 'magic_sell':
    case 'magic_sell_archive':
    case 'magic_sell_editor':
    case 'help_magic_sell':
      return MAGIC_SELL_MENU_IMAGE_PATH;
    case 'launch_buy':
    case 'launch_buy_archive':
    case 'launch_buy_editor':
    case 'help_launch_buy':
    case 'sniper_wizard':
    case 'help_sniper_wizard':
    case 'staking':
    case 'help_staking':
    case 'vanity_wallet':
    case 'help_vanity_wallet':
      return SNIPER_MENU_IMAGE_PATH;
    case 'buy_sell':
    case 'buy_sell_wallets':
    case 'buy_sell_quick':
    case 'buy_sell_limit':
    case 'buy_sell_copy':
    case 'magic_bundle':
    case 'magic_bundle_archive':
    case 'magic_bundle_editor':
    case 'help_buy_sell':
    case 'help_magic_bundle':
    case 'community_vision':
    case 'community_vision_archive':
    case 'community_vision_editor':
    case 'help_community_vision':
    case 'wallet_tracker':
    case 'wallet_tracker_archive':
    case 'wallet_tracker_editor':
    case 'help_wallet_tracker':
    case 'resizer':
    case 'help_resizer':
      return MENU_LOGO_IMAGE_PATH;
    default:
      return MENU_LOGO_IMAGE_PATH;
  }
};

const X_FOLLOWER_PACKAGES = {
  non_drop_100: {
    key: 'non_drop_100',
    type: 'non_drop',
    followers: 100,
    usdPrice: 4.49,
    providerCostUsd: 3.49,
    label: '100 Non-Drop',
    promise: 'Non-drop',
  },
  non_drop_500: {
    key: 'non_drop_500',
    type: 'non_drop',
    followers: 500,
    usdPrice: 15.99,
    providerCostUsd: 12.89,
    label: '500 Non-Drop',
    promise: 'Non-drop',
  },
  non_drop_1000: {
    key: 'non_drop_1000',
    type: 'non_drop',
    followers: 1000,
    usdPrice: 26.99,
    providerCostUsd: 22.99,
    label: '1000 Non-Drop',
    promise: 'Non-drop',
  },
  low_drop_100: {
    key: 'low_drop_100',
    type: 'low_drop',
    followers: 100,
    usdPrice: 2.49,
    providerCostUsd: 1.5,
    label: '100 Low-Drop',
    promise: 'Low-drop â€¢ 30-day refill',
  },
  low_drop_500: {
    key: 'low_drop_500',
    type: 'low_drop',
    followers: 500,
    usdPrice: 9.99,
    providerCostUsd: 7.5,
    label: '500 Low-Drop',
    promise: 'Low-drop â€¢ 30-day refill',
  },
  low_drop_1000: {
    key: 'low_drop_1000',
    type: 'low_drop',
    followers: 1000,
    usdPrice: 18.99,
    providerCostUsd: 15.0,
    label: '1000 Low-Drop',
    promise: 'Low-drop â€¢ 30-day refill',
  },
};

selectionSnapshot = function selectionSnapshot(user) {
  const bundle = getBundlePricing(user.selection.amount);
  const lines = [
    `\u{1F39B}\uFE0F Profile: ${buttonDisplay(user.selection.button)}`,
    `\u{1F4E6} Bundle: ${amountLabel(user.selection.amount)}`,
    `\u{1F3AF} Target: \`${user.selection.target}\``,
  ];

  if (user.selection.usingFreeTrial) {
    lines.push(`\u{1F381} Price: Free trial`);
    lines.push(`\u{1F6E1}\uFE0F Access: one-time x${cfg.freeTrialAmount} test`);
  } else if (isAdminUser(user.telegramId)) {
    lines.push(`\u{1F451} Price: Support override`);
    lines.push(`\u{1F6E1}\uFE0F Access: unlimited support launch`);
  } else if (bundle) {
    lines.push(`\u{1F4B0} Approx price: \`${bundleSolDisplay(bundle.amount, user)}\``);
    lines.push(`\u{1F3F7}\uFE0F Tier: ${bundle.role}`);
  }

  lines.push(`\u{1F4B3} Checkout: ${paymentStatusLabel(user)}`);
  lines.push(`\u{1F6A6} Run state: ${readinessLabel(user)}`);
  return lines.join('\n');
};

paymentDetailsLines = function paymentDetailsLines(user) {
  if (!hasActiveQuote(user)) {
    if (user.payment.lastError) {
      return ['No active quote yet.', `Last error: \`${user.payment.lastError}\``];
    }
    return ['No active quote yet.'];
  }

  const payment = user.payment;
  const lines = [
    `\u{1F4B0} Indicative bundle price: \`${formatApproxSol(payment.usdAmount, payment.solUsdRate)}\``,
    `\u{1F4B8} Send exactly: \`${payment.solAmount} SOL\``,
    `\u{1F4E5} Receive wallet: \`${payment.address}\``,
    `\u{1F551} Quote created: ${formatTimestamp(payment.quoteCreatedAt)}`,
    `\u23F3 Quote expires: ${formatTimestamp(payment.quoteExpiresAt)}`,
  ];

  if (payment.matchedSignature) {
    lines.push(`\u2705 Matched tx: \`${payment.matchedSignature}\``);
  }

  if (payment.lastError) {
    lines.push(`\u26A0\uFE0F Last error: \`${payment.lastError}\``);
  }

  return lines;
};

makeHomeKeyboard = function makeHomeKeyboard() {
  return new InlineKeyboard()
    .text('\u{1F680} Reaction Booster', 'entry:reaction')
    .text('\u{1F4CA} Volume Booster', 'nav:volume')
    .row()
    .text('\u{1F4B1} Buy / Sell', 'nav:buy_sell')
    .text('\u{1F916} Burn Agent', 'nav:burn_agent')
    .row()
    .text('\u{1F465} Holder Booster', 'nav:holder_booster')
    .text('\u{1F525} FOMO Booster', 'nav:fomo_booster')
    .row()
    .text('\u2728 Magic Sell', 'nav:magic_sell')
    .text('\u{1F4E6} Bundle', 'nav:magic_bundle')
    .row()
    .text('\u{1F680} Launch + Buy', 'nav:launch_buy')
    .text('\u{1F9D9} Sniper Wizard', 'nav:sniper_wizard')
    .row()
    .text('\u{1F4B0} Staking', 'nav:staking')
    .text('\u2728 Vanity Wallet', 'nav:vanity_wallet')
    .row()
    .text('\u{1F52E} Vision', 'nav:community_vision')
    .text('\u{1F440} Wallet Tracker', 'nav:wallet_tracker')
    .row()
    .text('\u{1F5BC}\uFE0F Resizer', 'nav:resizer')
    .row()
    .text('\u{1F465} X Followers', 'nav:x_followers')
    .text('\u{1F4E3} Engagement', 'nav:engagement')
    .row()
    .text('\u{1F4BC} Subs + Accounts', 'nav:subscriptions_accounts')
    .row()
    .text('\u2139\uFE0F Info', 'nav:help')
    .text('\u{1F4CA} Status', 'nav:status')
    .row()
    .url('\u{1F4AC} Community', 'https://t.me/wizardtoolz')
    .url('\u{1F514} Alerts', 'https://t.me/wizardtoolz_alerts')
    .row()
    .url('\u{1F91D} Help', `https://t.me/${SUPPORT_USERNAME}`)
    .text('\u{1F504} Refresh', 'refresh:home');
};

makeButtonKeyboard = function makeButtonKeyboard(selectedButton, user) {
  const keyboard = new InlineKeyboard();
  for (const button of Object.values(BUTTONS)) {
    const label = selectedButton === button.key
      ? `\u2705 ${button.emoji} ${button.label}`
      : `${button.emoji} ${button.label}`;
    keyboard.text(label, `button:${button.key}`);
  }
  keyboard.row().text('\u2B05\uFE0F Back', startBackRoute(user));
  keyboard.text('\u{1F3E0} Home', 'nav:home');
  keyboard.row().text('\u2139\uFE0F Help', 'nav:help_reaction');
  keyboard.row().text('\u{1F504} Refresh', 'refresh:start');
  return keyboard;
};

makeAmountKeyboard = function makeAmountKeyboard(selectedAmount, freeTrialUsed, usingFreeTrial, user) {
  const keyboard = new InlineKeyboard();
  const reactionEmoji = selectedButtonEmoji(user);

  if (!freeTrialUsed || usingFreeTrial) {
    keyboard.text(
      usingFreeTrial ? `\u2705 ${reactionEmoji} Trial x${cfg.freeTrialAmount}` : `${reactionEmoji} Trial x${cfg.freeTrialAmount}`,
      `amount:${cfg.freeTrialAmount}:trial`,
    );
    keyboard.row();
  }

  cfg.packageAmounts.forEach((amount, index) => {
    const label = selectedAmount === amount
      ? `\u2705 ${reactionEmoji} ${amount} \u2022 ${bundleSolDisplay(amount, user)}`
      : `${reactionEmoji} ${amount} \u2022 ${bundleSolDisplay(amount, user)}`;

    keyboard.text(label, `amount:${amount}:paid`);
    if ((index + 1) % 2 === 0) {
      keyboard.row();
    }
  });

  keyboard.row().text('\u2B05\uFE0F Back', 'nav:start');
  keyboard.text('\u{1F3E0} Home', 'nav:home');
  keyboard.row().text('\u{1F504} Refresh', 'refresh:amount');
  return keyboard;
};

makePaymentKeyboard = function makePaymentKeyboard(user) {
  const keyboard = new InlineKeyboard();

  if (user.selection.amount) {
    if (!hasLaunchAccess(user) && cfg.solanaReceiveAddress && !user.selection.usingFreeTrial) {
      keyboard.text('\u{1F9FE} Check Payment', 'payment:check');
      keyboard.text('\u{1F504} New Quote', 'payment:refresh');
      keyboard.row();
    }
  }

  keyboard.text('\u2B05\uFE0F Back', 'nav:amount');
  if (user.selection.button && user.selection.amount) {
    keyboard.text('\u{1F3AF} Change Link', 'nav:target');
  }
  keyboard.row().text('\u{1F3E0} Home', 'nav:home');
  keyboard.text('\u{1F504} Refresh', 'refresh:payment');
  return keyboard;
};

makeConfirmKeyboard = function makeConfirmKeyboard(user) {
  const ready = Boolean(user.selection.button && user.selection.amount);
  const keyboard = new InlineKeyboard();

  if (hasLaunchAccess(user) && ready) {
    keyboard.text('\u{1F680} Launch Reactions', 'run:confirm');
    keyboard.row();
  } else if (ready) {
    keyboard.text(
      isAdminUser(user.telegramId)
        ? '\u{1F451} Admin Ready'
        : user.selection.usingFreeTrial
          ? '\u{1F381} Trial Ready'
          : '\u{1F4B3} Payment',
      'nav:payment',
    );
    keyboard.row();
  }

  keyboard.text('\u{1F680} Change Reaction', 'nav:start');
  keyboard.row().text('\u{1F4E6} Change Amount', 'nav:amount');
  if (user.selection.button && user.selection.amount) {
    keyboard.text('\u{1F3AF} Change Link', 'nav:target');
    keyboard.row();
  }
  keyboard.text('\u2B05\uFE0F Back', 'nav:payment');
  keyboard.text('\u{1F3E0} Home', 'nav:home');
  keyboard.row().text('\u{1F504} Refresh', 'refresh:confirm');

  return keyboard;
};

makeInfoKeyboard = function makeInfoKeyboard(backTarget = 'nav:home', refreshRoute = 'home') {
  return new InlineKeyboard()
    .text('\u2B05\uFE0F Back', backTarget)
    .text('\u{1F3E0} Home', 'nav:home')
    .row()
    .text('\u{1F504} Refresh', `refresh:${refreshRoute}`);
};

makeTargetKeyboard = function makeTargetKeyboard(user) {
  const keyboard = new InlineKeyboard();

  if (
    user.selection.button ||
    user.selection.amount ||
    (user.selection.target && user.selection.target !== cfg.defaultTarget)
  ) {
    keyboard.text('\u2705 Continue', 'target:continue');
    keyboard.row();
  }

  keyboard.text('\u2B05\uFE0F Back', targetBackRoute(user));
  keyboard.text('\u{1F3E0} Home', 'nav:home');
  keyboard.row().text('\u{1F504} Refresh', 'refresh:target');
  return keyboard;
};

makeResultKeyboard = function makeResultKeyboard() {
  return new InlineKeyboard()
    .text('\u2B05\uFE0F Back', 'nav:payment')
    .text('\u{1F3E0} Home', 'nav:home')
    .row()
    .text('\u{1F504} Refresh', 'refresh:payment');
};

homeText = function homeText() {
  return [
    '\u{1F44B} *Welcome to Wizard Toolz!*',
    '',
    'Professional Telegram automation for reactions, trading, volume, burn systems, holder distribution, FOMO strategy, smart sell execution, bundle prep, and launch sniping.',
    '',
    MENU_DIVIDER,
    '\u2728 *How It Works*',
    '1. Choose the service you want to run.',
    '2. Configure the wallet, mint, package, or target.',
    '3. Fund the generated wallet when required.',
    '4. Start the automation and manage everything from Telegram.',
    '',
    MENU_DIVIDER,
    '\u{1F680} *Supported Venues*',
    'Raydium â€¢ PumpSwap â€¢ Meteora â€¢ Pumpfun â€¢ Meteora DBC â€¢ Bags â€¢ LetsBonk â€¢ LaunchLab',
    '',
    '\u{1F4CA} Plans from 1 SOL â€¢ \u{1F6E1}\uFE0F Professional execution â€¢ \u{1F381} Free trial available',
    `\u{1F91D} Need help? @${SUPPORT_USERNAME}`,
    '\u{1F4AC} Community chat: @wizardtoolz',
    '\u{1F514} Alerts channel: @wizardtoolz_alerts',
    '',
    'Ready? Choose a service below.',
  ].join('\n');
};

startText = function startText(user) {
  return [
    '\u{1F680} *Reaction Booster*',
    '',
    "Boost your target's visible momentum with a clean, guided launch flow.",
    '',
    selectionSnapshot(user),
    '',
    'Choose the reaction profile you want to push.',
  ].join('\n');
};

amountText = function amountText(user) {
  const reactionEmoji = selectedButtonEmoji(user);
  return [
    '\u{1F4E6} *Reaction Packages*',
    '',
    'Choose how much reaction volume you want for this run.',
    '',
    selectionSnapshot(user),
    '',
    `*Selected reaction:* ${reactionEmoji} ${buttonDisplay(user.selection.button)}`,
    '',
    '\u{1F4B0} *Approximate SOL Pricing*',
    ...cfg.packageAmounts.map((amount) => {
      const bundle = getBundlePricing(amount);
      return `${reactionEmoji} ${amount} reactions - ${bundleSolDisplay(amount, user)} - ${bundle.role}`;
    }),
    '',
    'Displayed prices are approximate until a live quote is created at checkout.',
  ].join('\n');
};

paymentText = function paymentText(user) {
  if (!user.selection.amount) {
    return [
      '\u{1F4B3} *Secure Solana Checkout*',
      '',
      'Pick a package first so the bot can build a live SOL quote.',
    ].join('\n');
  }

  if (user.selection.usingFreeTrial) {
    return [
      '\u{1F381} *Free Trial Checkout*',
      '',
      selectionSnapshot(user),
      '',
      trialIsAvailable(user)
        ? `No payment is required for the one-time x${cfg.freeTrialAmount} trial. Continue when you're ready to run.`
        : 'Your free trial has already been used. Pick a paid package instead.',
    ].join('\n');
  }

  if (isAdminUser(user.telegramId)) {
    return [
      '\u{1F451} *Support Checkout Override*',
      '',
      selectionSnapshot(user),
      '',
      'This account is whitelisted. No SOL payment is required for support launches.',
    ].join('\n');
  }

  if (!cfg.solanaReceiveAddress) {
    return [
      '\u{1F4B3} *Secure Solana Checkout*',
      '',
      'Payment is unavailable because `SOLANA_RECEIVE_ADDRESS` is not configured yet.',
      '',
      selectionSnapshot(user),
    ].join('\n');
  }

  const statusLine = paymentIsReady(user)
    ? 'Payment found on-chain. This package is unlocked.'
    : quoteExpired(user.payment)
      ? 'This quote has expired. Generate a fresh quote before sending SOL.'
      : 'Send the exact SOL amount below, then tap *Check Payment* when it lands.';

  return [
    '\u{1F4B3} *Secure Solana Checkout*',
    '',
    selectionSnapshot(user),
    '',
    statusLine,
    '',
    ...paymentDetailsLines(user),
    '',
    'Only native SOL transfers to the wallet above are matched automatically.',
  ].join('\n');
};

targetText = function targetText(user) {
  return [
    '\u{1F3AF} *Target Setup*',
    '',
    'Send the target URL you want this run to hit.',
    '',
    selectionSnapshot(user),
    '',
    user.selection.button || user.selection.amount
      ? 'Paste the full target URL in your next message, or go back to keep the current target.'
      : 'Send the full target URL in your next message to begin the Reaction Booster flow.',
  ].join('\n');
};

confirmText = function confirmText(user) {
  const paymentNote = isAdminUser(user.telegramId)
    ? 'Support access is active. Launch is unlocked.'
    : hasLaunchAccess(user)
      ? 'Everything is set. Launch is unlocked.'
      : 'Finish checkout first, then come back here to launch.';

  return [
    '\u2705 *Launch Review*',
    '',
    'Review everything below before you launch.',
    '',
    selectionSnapshot(user),
    '',
    paymentNote,
    ...(user.payment.matchedSignature ? ['', `Matched tx: \`${user.payment.matchedSignature}\``] : []),
  ].join('\n');
};

statusText = function statusText(user) {
  const recentLogs = getRecentActivityLogs(user, { limit: 8 });
  return [
    '\u{1F4CA} *Account Status*',
    '',
    'Your current selection, payment state, and latest activity are shown below.',
    '',
    selectionSnapshot(user),
    '',
    ...paymentDetailsLines(user),
    ...(recentLogs.length > 0
      ? [
        '',
        '\u{1F4DD} *Recent Activity*',
        ...recentLogs.map((entry) => formatActivityLogLine(entry)),
      ]
      : []),
  ].join('\n');
};

helpText = function helpText() {
  return [
    '\u2139\uFE0F *Help & Info*',
    '',
    'Welcome to *WIZARD TOOLZ* - a premium Telegram control panel for reactions, trading, volume services, smart sell tools, wallet tracking, community alerts, and launch tools.',
    '',
    MENU_DIVIDER,
    '\u2728 *How To Use It*',
    '1. Open the service you want from the home menu.',
    '2. Complete the setup fields shown on that screen.',
    '3. Fund the generated wallet if the flow requires funding.',
    '4. Start the automation and refresh Telegram for live updates.',
    '',
    MENU_DIVIDER,
    '\u{1F6E1}\uFE0F *Safety Notes*',
    `â€¢ Free trial is limited to one run per Telegram account at x${cfg.freeTrialAmount}.`,
    'â€¢ Paid quotes are tied to the selected package and expire automatically.',
    'â€¢ Wallet-based tools control real on-chain funds, so treat every private key as hot.',
    'â€¢ Always double-check mint addresses, treasury addresses, and deposit amounts before funding.',
    '',
    MENU_DIVIDER,
    '\u{1F91D} *Support*',
    `Questions or issues: @${SUPPORT_USERNAME}`,
    'Community chat: @wizardtoolz',
    'Alerts channel: @wizardtoolz_alerts',
    '',
    'Tap a feature button below for a deeper plain-English walkthrough.',
  ].join('\n');
};

function featureHelpTitle(feature) {
  switch (feature) {
    case 'reaction':
      return '\u{1F680} *Reaction Booster Help*';
    case 'volume':
      return '\u{1F4CA} *Volume Booster Help*';
    case 'buy_sell':
      return '\u{1F4B1} *Buy / Sell Help*';
    case 'burn_agent':
      return '\u{1F916} *Burn Agent Help*';
    case 'holder_booster':
      return '\u{1F465} *Holder Booster Help*';
    case 'fomo_booster':
      return '\u{1F525} *FOMO Booster Help*';
    case 'magic_sell':
      return '\u2728 *Magic Sell Help*';
    case 'magic_bundle':
      return '\u{1F4E6} *Bundle Help*';
    case 'launch_buy':
      return '\u{1F680} *Launch + Buy Help*';
    case 'sniper_wizard':
      return '\u{1F9D9} *Sniper Wizard Help*';
    case 'staking':
      return '\u{1F4B0} *Staking Help*';
    case 'vanity_wallet':
      return '\u2728 *Vanity Wallet Help*';
    case 'community_vision':
      return '\u{1F52E} *Vision Help*';
    case 'wallet_tracker':
      return '\u{1F440} *Wallet Tracker Help*';
    case 'x_followers':
      return '\u{1F465} *X Followers Help*';
    case 'engagement':
      return '\u{1F4E3} *Engagement Help*';
    case 'subscriptions_accounts':
      return '\u{1F4BC} *Subscriptions + Accounts Help*';
    case 'resizer':
      return '\u{1F5BC}\uFE0F *Resizer Help*';
    default:
      return '\u2139\uFE0F *Feature Help*';
  }
}

function featureHelpText(feature) {
  switch (feature) {
    case 'reaction':
      return [
        featureHelpTitle(feature),
        '',
        'This is the simplest tool in the bot.',
        '',
        'You choose the reaction type, choose a package, send the target link, then complete checkout if needed.',
        'After that, the bot launches the run for you.',
        '',
        MENU_DIVIDER,
        '\u2728 *In Plain English*',
        '- You are buying a reaction push for a target link.',
        '- The package controls how much reaction volume gets sent.',
        '- The bot gives you a live SOL quote before launch.',
        '- Once payment is matched, you can launch from Telegram.',
        '',
        '\u{1F4A1} *Best For*',
        '- Quick visibility boosts',
        '- Trials and simple launches',
        '- Users who do not want to manage wallets manually',
      ].join('\n');
    case 'volume':
      return [
        featureHelpTitle(feature),
        '',
        'Volume Booster is the chart-activity product.',
        '',
        'When you open it, you first choose *Organic* or *Bundled* volume. After that, you choose a package and configure the mint.',
        '',
        MENU_DIVIDER,
        '\u{1F34F} *Organic Volume*',
        '- Uses a deposit wallet plus generated trading wallets',
        '- Trades in a more natural-looking pattern',
        '- Randomizes trade size and timing inside the range you set',
        '- Good when you want a steadier, less rigid chart look',
        '- Includes a one-time platform-funded free trial with tiny real trades',
        '',
        '\u{1F4E6} *Bundled Volume*',
        '- Uses same-slot bundled execution',
        '- Built for tighter, faster momentum',
        '- Better when you want more aggressive throughput',
        '',
        '\u{1F4A1} *How The Wallet Flow Works*',
        '- You fund the deposit wallet shown by the bot',
        '- The bot handles the extra trading wallets automatically',
        '- Stats, cost estimates, and progress update automatically',
        '- Archived boosters are your completed or shelved runs',
      ].join('\n');
    case 'buy_sell':
      return [
        featureHelpTitle(feature),
        '',
        'Buy / Sell is the trading desk menu.',
        '',
        'This is modeled after the way major Telegram trading bots separate fast swaps, wallet management, limit orders, and copy trading into one central hub.',
        '',
        MENU_DIVIDER,
        '\u2728 *What You Can Do From Here*',
        '- Import an existing wallet',
        '- Generate a fresh wallet',
        '- Pick which wallet you want active',
        '- Open your Bundle setups',
        '- Set the token CA you want ready on the desk',
        '- Import, generate, and switch trading wallets',
        `- Supported trade routes use a *${formatBpsPercent(cfg.tradingHandlingFeeBps)}* handling fee per executed trade`,
        '- That is lower than every competitor we track, and net platform profit is routed 50% to treasury, 25% to buyback + burn, and 25% to the SOL rewards vault',
        '',
        '\u26A0\uFE0F *Important*',
        '- Any imported private key is a hot wallet',
        '- Always leave SOL for gas',
        '- Double-check the CA before funding or trading',
      ].join('\n');
    case 'burn_agent':
      return [
        featureHelpTitle(feature),
        '',
        'Burn Agent is for creator-reward automation tied to a token mint.',
        '',
        'Its job is to claim creator rewards, use part of those rewards to buy the token back, then burn those bought tokens on-chain.',
        '',
        MENU_DIVIDER,
        '\u26A1 *Fast Burn Agent*',
        '- Uses the creator wallet directly',
        '- Best when you want the most direct setup',
        '',
        '\u{1F422} *Normal Burn Agent*',
        '- Uses a managed reward wallet flow instead',
        '- Better when you want a little more separation',
        '',
        '\u{1F4A1} *In Plain English*',
        '- Rewards come in as SOL',
        '- The bot can split that SOL between keeping some and buying back the token',
        '- The buyback tokens are burned, not just sent away',
        '- This is a serious hot-wallet flow, so only use wallets you truly intend to automate',
      ].join('\n');
    case 'holder_booster':
      return [
        featureHelpTitle(feature),
        '',
        'Holder Booster is a one-time wallet fanout tool.',
        '',
        'You tell the bot the mint and how many holders you want. Then it gives you a deposit wallet and tells you exactly how much SOL and how many tokens to send.',
        '',
        MENU_DIVIDER,
        '\u2728 *What Happens After Funding*',
        '- The bot creates fresh recipient wallets',
        '- It sends one token to each recipient wallet',
        '- It splits the remaining SOL between treasury and the dev-side burn flow',
        '',
        '\u{1F4A1} *Important*',
        '- This is not a looping bot',
        '- It is a one-time distribution action',
        '- There is no sweep-back flow at the end',
        '- Deposit the exact token amount requested so nothing is left behind by accident',
      ].join('\n');
    case 'fomo_booster':
      return [
        featureHelpTitle(feature),
        '',
        'FOMO Booster is a momentum-style micro-bundle bot for bonding-curve tokens.',
        '',
        'It repeatedly lands a bundle with *2 buys and 1 sell*. That means there is slightly more buy pressure than sell pressure in each cycle.',
        '',
        MENU_DIVIDER,
        '\u26A1 *What The Bot Is Trying To Do*',
        '- Create small but frequent chart activity',
        '- Spread the activity across multiple wallets',
        '- Keep the pattern looking less obvious than one wallet buying and selling over and over',
        '',
        '\u{1F4A1} *How The Wallet Flow Works*',
        '- You fund one deposit wallet with SOL',
        '- The bot generates extra wallets automatically',
        '- It tops them up as needed, seeds seller inventory if needed, then bundles 2 buys + 1 sell',
        '- The main cost is fuel, fees, and normal trade friction',
        '',
        '\u26A0\uFE0F *Important*',
        '- This is an active on-chain trading flow',
        '- Keep enough SOL in the deposit wallet so the bot can keep running',
        '- If the bot says it is waiting for fuel, that usually means the deposit wallet needs more SOL',
      ].join('\n');
    case 'magic_sell':
      return [
        featureHelpTitle(feature),
        '',
        'Magic Sell is the smart-sell product.',
        '',
        'Instead of dumping from one obvious wallet, it watches for qualifying buyer strength and reacts from rotated seller wallets.',
        '',
        MENU_DIVIDER,
        '\u2728 *In Plain English*',
        '- You choose the token and target market cap',
        '- You can whitelist wallets the bot should ignore',
        '- The bot gives you a fresh deposit wallet for inventory',
        '- Once the market cap condition is met, it watches for real buys',
        '- When it sees a meaningful buy, it sells a portion of that flow from a rotated wallet',
        '',
        '\u{1F4A1} *Why People Use It*',
        '- Reduce obvious one-wallet sell pressure',
        '- Rotate seller wallets for a cleaner pattern',
        '- Sell into buyer strength instead of unloading too early',
      ].join('\n');
    case 'magic_bundle':
      return [
        featureHelpTitle(feature),
        '',
        'Bundle lets you fund one wallet, spread that balance across multiple fresh wallets, then manage protection rules from one place.',
        '',
        'When you open Bundle, you can choose between *Magic Bundle (Stealth)* and *Regular Bundle*.',
        '',
        MENU_DIVIDER,
        '\u2728 *In Plain English*',
        '- *Regular Bundle* spreads the funds directly and does not charge a bundle setup fee',
        '- *Magic Bundle (Stealth)* uses a more hidden wallet spread path for extra privacy',
        '- After the spread is done, you can arm stop loss, take profit, trailing stop, buy dip, and creator-sell protection',
        '- Protection starts from the first live position the bot sees in each bundle wallet',
        '- Live stats and wallet balances stay tied to that bundle inside Telegram',
        '',
        '\u26A0\uFE0F *Important*',
        `- *Regular Bundle*: free setup, plus normal network fees`,
        `- *Magic Bundle (Stealth)*: *${cfg.magicBundleStealthSetupFeeSol} SOL* setup fee, plus the stealth routing cost`,
        `- Bundle trades charge a *${formatBpsPercent(cfg.tradingHandlingFeeBps)}* handling fee per buy or sell`,
        '- That is lower than every competitor we track, and net platform profit is routed 50% to treasury, 25% to buyback + burn, and 25% to the SOL rewards vault',
        '- Network fees must still be covered by the deposit',
        '- The fee estimate is a guide, so it is smart to fund a little extra',
        '- Your bundle setups stay here so you can archive, restore, or delete them later',
      ].join('\n');
    case 'launch_buy':
      return [
        featureHelpTitle(feature),
        '',
        'Launch + Buy is the Pump launch flow for creating a coin and lining up bundled early buys from multiple wallets.',
        '',
        MENU_DIVIDER,
        '\u2728 *What You Set Up*',
        '- Name, symbol, description, logo, and socials',
        '- Normal Mode or Magic Mode',
        '- Generated buyer wallets or imported buyer wallets',
        '- Total SOL launch-buy budget',
        '- Jito tip for faster inclusion',
        '',
        '\u{1F4A1} *How It Works*',
        '- The bot gives you a launch wallet to fund',
        '- That launch wallet is the wallet used for the launch flow',
        '- Buyer wallets are prepared alongside it',
        '- Funding early lets the worker warm those wallets before launch so the live bundle can fire faster and look cleaner on-chain',
        '- Normal Mode uses the direct path',
        '- Magic Mode uses the hidden routing path before the launch bundle',
        '- Generated launch wallets stay hidden by default until you reveal them in your own chat',
        '',
        '\u{1F4B0} *Pricing*',
        `- *Normal Mode*: *${formatSolAmountFromLamports(LAUNCH_BUY_NORMAL_SETUP_FEE_LAMPORTS)} SOL* setup fee`,
        `- *Magic Mode*: *${formatSolAmountFromLamports(LAUNCH_BUY_MAGIC_SETUP_FEE_LAMPORTS)} SOL* setup fee plus hidden routing cost`,
        '- Your launch-buy budget and Jito tip are funded separately',
      ].join('\n');
    case 'sniper_wizard':
      return [
        featureHelpTitle(feature),
        '',
        'Sniper Wizard is the fast-launch watcher.',
        '',
        'You choose the wallet you want to watch. The bot gives you one deposit wallet plus up to 20 sniper wallets. Once you start it, the bot watches that target wallet for a fresh launch and tries to buy immediately when that wallet creates a new coin.',
        '',
        MENU_DIVIDER,
        '\u2728 *In Plain English*',
        '- You are not buying from your main wallet',
        '- You fund a separate deposit wallet instead',
        '- The bot can arm up to 20 sniper wallets for the launch',
        '- Funding early lets the worker warm those sniper wallets before you start watching, which improves speed and reduces fresh-funding patterns on-chain',
        '- Normal mode spreads funds directly to those wallets',
        '- Magic mode uses hidden routing before those wallets are armed',
        '- You choose what percent of that wallet should be used for the buy',
        '- The bot always leaves gas behind',
        '- Generated wallet keys stay hidden by default until you reveal them in your own chat',
        `- *Magic mode* adds a *${formatSolAmountFromLamports(SNIPER_MAGIC_SETUP_FEE_LAMPORTS)} SOL* setup fee plus hidden routing cost`,
        `- Successful snipes use a *${formatBpsPercent(cfg.tradingHandlingFeeBps)}* handling fee`,
        '',
        '\u26A0\uFE0F *Important*',
        '- This is a real hot-wallet flow with real funds',
        '- Only keep the SOL you want to risk in the sniper wallet',
        '- The bot is built to react as fast as it can, but final speed still depends on RPC and launch conditions',
      ].join('\n');
    case 'staking':
      return [
        featureHelpTitle(feature),
        '',
        'Staking is the holder-rewards layer for WIZARD TOOLZ.',
        '',
        'You link the WIZARD TOOLZ wallet you want tracked, keep holding over time, then manually claim *SOL* rewards from the bot once your claimable amount is large enough.',
        '',
        MENU_DIVIDER,
        '\u2728 *How Rewards Work*',
        '- Rewards are paid in *SOL*, not in more WIZARD TOOLZ tokens',
        '- Platform fees and creator rewards feed the SOL rewards vault',
        '- Claims are *manual* so you stay in control of when they are submitted',
        `- Minimum claim amount: *${formatSolAmountFromLamports(STAKING_MIN_CLAIM_LAMPORTS)} SOL*`,
        `- Normal unstake uses a *${formatDayCountLabel(STAKING_UNSTAKE_COOLDOWN_DAYS)}* cooldown in the hard-staking flow`,
        '',
        '\u{1F9E0} *How Rewards Build*',
        '- Rewards start building immediately once your linked staking wallet is being tracked',
        `- The first *${formatDayCountLabel(STAKING_EARLY_WEIGHT_DAYS)}* count at a lighter weight so fast in-and-out wallets do not farm much`,
        '- Bigger bags and longer holds get a larger share whenever new SOL hits the rewards vault',
        '- You can claim any time once your claimable balance reaches the minimum',
        '- Current live weight tiers: 0-6 days = 0.25x, 7-29 = 1.0x, 30-89 = 1.25x, 90-179 = 1.5x, 180+ = 2.0x',
        '',
        '\u{1F4A1} *Why This Is Good For Holders*',
        '- Rewards come from real platform revenue instead of token inflation',
        '- Bigger and longer holds naturally earn a bigger share',
        '- Manual claims keep the process cleaner and easier to verify',
      ].join('\n');
    case 'vanity_wallet':
      return [
        featureHelpTitle(feature),
        '',
        'Vanity Wallet creates a fresh Solana wallet with a short custom start or end pattern.',
        '',
        MENU_DIVIDER,
        '\u2728 *How It Works*',
        '- Choose whether the address should start with or end with your pattern',
        '- Example: start with `WIZ` or end with `TOOL`',
        `- Patterns are capped at *${VANITY_WALLET_MAX_PATTERN_LENGTH} characters* on the shared Render stack`,
        '- Pay the fixed service fee shown by the bot',
        '- After payment is matched, the bot brute-forces a fresh wallet in the background and delivers the private key in Telegram',
        '',
        '\u{1F4B0} *Fees*',
        `- Service fee: *${formatSolAmountFromLamports(VANITY_WALLET_SERVICE_FEE_LAMPORTS)} SOL*`,
        `- Treasury share: *${formatSolAmountFromLamports(VANITY_WALLET_TREASURY_SHARE_LAMPORTS)} SOL*`,
        `- Burn-side share: *${formatSolAmountFromLamports(VANITY_WALLET_BURN_SHARE_LAMPORTS)} SOL*`,
        '',
        '\u26A0\uFE0F *Important*',
        '- Vanity generation is CPU work, so longer patterns take much longer',
        '- The bot hides the private key by default until you reveal it',
        '- Treat the generated wallet like any other hot wallet with real funds',
      ].join('\n');
    case 'community_vision':
      return [
        featureHelpTitle(feature),
        '',
        'Vision watches selected X accounts and alerts you if a community they already run gets renamed.',
        '',
        MENU_DIVIDER,
        '\u2728 *How It Works*',
        '- Paste an X profile link or @handle',
        '- Start the watch once the account looks right',
        '- The bot records the communities already tied to that account',
        '- If one of those community names changes later, you get a Telegram alert',
        '',
        '\u{1F4A1} *Good To Know*',
        '- It is focused on name changes, not brand-new communities',
        '- You can keep multiple watches active at once',
        '- Archived watches stay out of the way until you restore them',
      ].join('\n');
    case 'wallet_tracker':
      return [
        featureHelpTitle(feature),
        '',
        'Wallet Tracker watches important wallets and tells you when they buy, sell, or launch a token.',
        '',
        MENU_DIVIDER,
        '\u2728 *How It Works*',
        '- Add the wallet address you want to follow',
        '- Choose whether you want the first buy only or every buy',
        '- Turn sell alerts and launch alerts on or off',
        '- Start the tracker and the bot will message you when activity is seen',
        '',
        '\u{1F4A1} *Best For*',
        '- Watching smart wallets',
        '- Following dev wallets',
        '- Getting fast heads-up alerts inside Telegram',
      ].join('\n');
    case 'x_followers':
      return [
        featureHelpTitle(feature),
        '',
        'X Followers is the follower-delivery service inside the bot.',
        '',
        'You choose a package, send the X link, pay the quoted SOL amount, then message support to confirm timing and slot availability.',
        '',
        MENU_DIVIDER,
        '\u2728 *Packages*',
        '- Non-drop: 100 / 500 / 1000',
        '- Low-drop with 30-day refill: 100 / 500 / 1000',
        '',
        '\u{1F4A1} *In Plain English*',
        '- Delivery may not be instant',
        '- After payment, DM support to confirm timing',
        '- The bot gives you an exact SOL amount based on the current SOL price',
        '- Profit is tracked internally and split 50/50 between treasury and burn-side profit',
      ].join('\n');
    case 'engagement':
      return [
        featureHelpTitle(feature),
        '',
        'Engagement is the support contact menu for platform-specific social growth and traffic services.',
        '',
        'If you need views, clicks, likes, reposts, or other engagement on X, Facebook, Reddit, Telegram, Discord, or TikTok, open the matching platform and message support.',
        '',
        MENU_DIVIDER,
        '\u2728 *What You Can Ask For*',
        '- Views',
        '- Clicks',
        '- Likes',
        '- Reposts',
        '- Other platform-specific engagement services',
        '',
        '\u26A0\uFE0F *Important*',
        `- Availability can vary, so message support at @${SUPPORT_USERNAME} first`,
        '- This menu is a service desk, not an instant checkout flow',
      ].join('\n');
    case 'subscriptions_accounts':
      return [
        featureHelpTitle(feature),
        '',
        'Subscriptions + Accounts is the inquiry menu for digital subscriptions, premium apps, social accounts, and related creative/account services.',
        '',
        'Open the menu, choose *Subscriptions* or *Accounts*, then tap the service you want. The bot will send you straight to support to ask about availability and pricing.',
        '',
        MENU_DIVIDER,
        '\u2728 *Subscriptions*',
        '- ChatGPT',
        '- Claude',
        '- Perplexity',
        '- ElevenLabs',
        '- DeepL',
        '- Canva',
        '- Adobe / Adobe Express',
        '',
        '\u{1F4A1} *Accounts*',
        '- Instagram, TikTok, X, Telegram, Reddit, Picsart',
        '',
        '\u26A0\uFE0F *Important*',
        `- Pricing and stock can change, so message support at @${SUPPORT_USERNAME} first`,
        '- This menu is for private inquiries, not instant checkout',
      ].join('\n');
    case 'resizer':
      return [
        featureHelpTitle(feature),
        '',
        'Resizer turns one uploaded image into a clean square logo or a polished wide banner.',
        '',
        MENU_DIVIDER,
        '\u2728 *How It Works*',
        '- Choose *Logo* for a 1:1 square output',
        '- Choose *Banner* for a 1:3 wide output',
        '- Send your image as a normal photo or image file',
        '- The bot keeps the proportions clean and returns a finished PNG',
        '',
        '\u{1F4A1} *Why It Looks Better*',
        '- No ugly stretching',
        '- No broken aspect ratio',
        '- The image is centered cleanly for the format you picked',
      ].join('\n');
    default:
      return [
        featureHelpTitle(feature),
        '',
        'No help text is available for that feature yet.',
      ].join('\n');
  }
}

function makeHelpHubKeyboard() {
  return new InlineKeyboard()
    .text('\u{1F680} Reaction Help', 'nav:help_reaction')
    .row()
    .text('\u{1F4CA} Volume Help', 'nav:help_volume')
    .row()
    .text('\u{1F4B1} Buy / Sell Help', 'nav:help_buy_sell')
    .row()
    .text('\u{1F916} Burn Agent Help', 'nav:help_burn_agent')
    .row()
    .text('\u{1F465} Holder Help', 'nav:help_holder_booster')
    .row()
    .text('\u{1F525} FOMO Help', 'nav:help_fomo_booster')
    .row()
    .text('\u2728 Magic Sell Help', 'nav:help_magic_sell')
    .row()
    .text('\u{1F4E6} Bundle Help', 'nav:help_magic_bundle')
    .row()
    .text('\u{1F680} Launch + Buy Help', 'nav:help_launch_buy')
    .row()
    .text('\u{1F9D9} Sniper Help', 'nav:help_sniper_wizard')
    .row()
    .text('\u{1F4B0} Staking Help', 'nav:help_staking')
    .row()
    .text('\u2728 Vanity Wallet Help', 'nav:help_vanity_wallet')
    .row()
    .text('\u{1F52E} Vision Help', 'nav:help_community_vision')
    .row()
    .text('\u{1F440} Wallet Tracker Help', 'nav:help_wallet_tracker')
    .row()
    .text('\u{1F465} X Followers Help', 'nav:help_x_followers')
    .row()
    .text('\u{1F4E3} Engagement Help', 'nav:help_engagement')
    .row()
    .text('\u{1F4BC} Subs + Accounts Help', 'nav:help_subscriptions_accounts')
    .row()
    .text('\u{1F5BC}\uFE0F Resizer Help', 'nav:help_resizer')
    .row()
    .text('\u2B05\uFE0F Back', 'nav:home')
    .text('\u{1F3E0} Home', 'nav:home')
    .row()
    .text('\u{1F504} Refresh', 'refresh:help');
}

function makeFeatureHelpKeyboard(backTarget, refreshRoute) {
  return new InlineKeyboard()
    .text('\u2B05\uFE0F Back', backTarget)
    .text('\u{1F3E0} Home', 'nav:home')
    .row()
    .text('\u2139\uFE0F Help Hub', 'nav:help')
    .row()
    .text('\u{1F504} Refresh', `refresh:${refreshRoute}`);
}

function communityVisionStatusLabel(order) {
  if (order.archivedAt) return '\u26AB Archived';
  switch (order.status) {
    case 'watching':
      return '\u{1F7E2} Watching';
    case 'offline':
      return '\u{1F7E1} Service Offline';
    case 'failed':
      return '\u26D4 Error';
    case 'stopped':
      return '\u26AA Stopped';
    default:
      return '\u2699\uFE0F Setup';
  }
}

function communityVisionListLabel(order) {
  const label = order.handle ? `@${order.handle}` : 'Complete Setup';
  return `\u{1F52E} ${label} \u2022 ${communityVisionStatusLabel(order)}`;
}

function communityVisionCatalogText(user) {
  const activeOrders = getVisibleCommunityVisions(user);
  return [
    '\u{1F52E} *Vision*',
    '',
    'Watch important X accounts and get alerted if one of their existing community names changes.',
    '',
    MENU_DIVIDER,
    '\u2728 *How It Works*',
    '- Add the X account you want to watch',
    '- Start the watch',
    '- The bot remembers the communities already tied to that account',
    '- If one of those community names changes, you get a Telegram alert',
    '',
    activeOrders.length > 0
      ? `Active watches: *${activeOrders.length}*`
      : 'No active watches yet. Create one to start monitoring.',
  ].join('\n');
}

function communityVisionArchiveText(user) {
  const archivedOrders = getVisibleCommunityVisions(user, { archived: true });
  return [
    '\u{1F5C4}\uFE0F *Vision Archive*',
    '',
    archivedOrders.length > 0
      ? 'Archived watches stay here until restored or permanently deleted.'
      : 'No archived Vision watches right now.',
  ].join('\n');
}

function makeCommunityVisionCatalogKeyboard(user) {
  const keyboard = new InlineKeyboard();
  const activeOrders = getVisibleCommunityVisions(user);
  keyboard.text('\u{1F52E} New Vision', 'communityvision:new');
  keyboard.row();

  for (const order of activeOrders) {
    keyboard.text(communityVisionListLabel(order), `communityvision:open:${order.id}`);
    keyboard.row();
  }

  keyboard.text('\u{1F5C4}\uFE0F Archive', 'nav:community_vision_archive');
  keyboard.row();
  keyboard.text('\u2B05\uFE0F Back', 'nav:home');
  keyboard.text('\u{1F3E0} Home', 'nav:home');
  keyboard.row();
  keyboard.text('\u2139\uFE0F Help', 'nav:help_community_vision');
  keyboard.row();
  keyboard.text('\u{1F504} Refresh', 'refresh:community_vision');
  return keyboard;
}

function makeCommunityVisionArchiveKeyboard(user) {
  const keyboard = new InlineKeyboard();
  const archivedOrders = getVisibleCommunityVisions(user, { archived: true });
  for (const order of archivedOrders) {
    keyboard.text(communityVisionListLabel(order), `communityvision:open:${order.id}`);
    keyboard.row();
  }
  keyboard.text('\u2B05\uFE0F Back', 'nav:community_vision');
  keyboard.text('\u{1F3E0} Home', 'nav:home');
  keyboard.row();
  keyboard.text('\u{1F504} Refresh', 'refresh:community_vision_archive');
  return keyboard;
}

function promptForCommunityVisionField(field) {
  switch (field) {
    case 'profile_url':
      return 'Send the X profile link or @handle you want to watch next.';
    default:
      return 'Send the requested Vision detail next.';
  }
}

function makeCommunityVisionEditorKeyboard(user) {
  const keyboard = new InlineKeyboard();
  const order = user.communityVision;
  keyboard.text(order.handle ? '\u{1F517} Update X Account' : '\u{1F517} Set X Account', `communityvision:set:profile_url:${order.id}`);
  keyboard.row();
  if (order.handle) {
    keyboard.url('\u{1F30D} Open X Profile', `https://x.com/${order.handle}`);
    keyboard.row();
  }
  keyboard.text(
    order.handle
      ? (order.automationEnabled ? '\u23F9\uFE0F Stop Watch' : '\u25B6\uFE0F Start Watch')
      : '\u{1F512} Start Watch',
    order.handle ? `communityvision:toggle:${order.id}` : 'communityvision:locked:toggle',
  );
  keyboard.row();

  if (order.archivedAt) {
    keyboard.text('\u267B\uFE0F Restore', `communityvision:restore:${order.id}`);
    keyboard.text(
      order.deleteConfirmations >= 1 ? '\u{1F6A8} Confirm Delete' : '\u{1F5D1}\uFE0F Delete',
      `communityvision:delete:${order.id}`,
    );
    keyboard.row();
    keyboard.text('\u2B05\uFE0F Back', 'nav:community_vision_archive');
  } else {
    keyboard.text('\u{1F5C4}\uFE0F Archive', `communityvision:archive:${order.id}`);
    keyboard.row();
    keyboard.text('\u2B05\uFE0F Back', 'nav:community_vision');
  }

  keyboard.text('\u{1F3E0} Home', 'nav:home');
  keyboard.row();
  keyboard.text('\u2139\uFE0F Help', 'nav:help_community_vision');
  keyboard.row();
  keyboard.text('\u{1F504} Refresh', 'refresh:community_vision_editor');
  return keyboard;
}

function communityVisionEditorText(user) {
  const order = user.communityVision;
  const trackedNames = Array.isArray(order.trackedCommunities) ? order.trackedCommunities.slice(0, 4) : [];
  const renameCount = Number.isInteger(order.stats?.renameCount) ? order.stats.renameCount : 0;
  const alertCount = Number.isInteger(order.stats?.alertCount) ? order.stats.alertCount : 0;
  return [
    '\u{1F52E} *Vision*',
    '',
    '\u{1F4CB} *Watch Setup*',
    `- X account: ${order.handle ? `*@${order.handle}*` : '*Not set*'}`,
    `- Status: *${communityVisionStatusLabel(order)}*`,
    ...(order.profileUrl ? [`- Profile link: ${order.profileUrl}`] : []),
    '',
    '\u{1F50D} *Tracked Communities*',
    ...(trackedNames.length > 0
      ? trackedNames.map((item) => `- ${item.name || 'Unnamed community'}`)
      : ['- No communities stored yet']),
    ...(Array.isArray(order.trackedCommunities) && order.trackedCommunities.length > trackedNames.length
      ? [`- ...and *${order.trackedCommunities.length - trackedNames.length}* more`]
      : []),
    '',
    '\u{1F4C8} *Watch Stats*',
    `- Communities stored: *${Array.isArray(order.trackedCommunities) ? order.trackedCommunities.length : 0}*`,
    `- Name changes found: *${renameCount}*`,
    `- Alerts sent: *${alertCount}*`,
    ...(order.lastChangeAt ? [`- Last change seen: ${formatTimestamp(order.lastChangeAt)}`] : []),
    ...(order.lastCheckedAt ? [`- Last checked: ${formatTimestamp(order.lastCheckedAt)}`] : []),
    ...(order.lastError ? [`- Last note: \`${order.lastError}\``] : []),
    '',
    order.awaitingField
      ? promptForCommunityVisionField(order.awaitingField)
      : 'Set the X account above, then start the watch when you are ready.',
  ].filter(Boolean).join('\n');
}

function walletTrackerStatusLabel(order) {
  if (order.archivedAt) return '\u26AB Archived';
  switch (order.status) {
    case 'watching':
      return '\u{1F7E2} Watching';
    case 'failed':
      return '\u26D4 Error';
    case 'stopped':
      return '\u26AA Stopped';
    default:
      return '\u2699\uFE0F Setup';
  }
}

function walletTrackerBuyModeLabel(mode) {
  switch (mode) {
    case 'off':
      return 'Off';
    case 'every':
      return 'Every Buy';
    default:
      return 'First Buy Only';
  }
}

function walletTrackerListLabel(order) {
  const label = order.walletAddress
    ? `${order.walletAddress.slice(0, 4)}...${order.walletAddress.slice(-4)}`
    : 'Complete Setup';
  return `\u{1F440} ${label} \u2022 ${walletTrackerStatusLabel(order)}`;
}

function walletTrackerCatalogText(user) {
  const activeOrders = getVisibleWalletTrackers(user);
  return [
    '\u{1F440} *Wallet Tracker*',
    '',
    'Track important wallets and get Telegram alerts when they buy, sell, or launch.',
    '',
    MENU_DIVIDER,
    '\u2728 *Alert Options*',
    '- First buy only or every buy',
    '- Sell alerts on or off',
    '- Launch alerts on or off',
    '',
    activeOrders.length > 0
      ? `Active trackers: *${activeOrders.length}*`
      : 'No wallet trackers yet. Create one to start following wallet activity.',
  ].join('\n');
}

function walletTrackerArchiveText(user) {
  const archivedOrders = getVisibleWalletTrackers(user, { archived: true });
  return [
    '\u{1F5C4}\uFE0F *Wallet Tracker Archive*',
    '',
    archivedOrders.length > 0
      ? 'Archived wallet trackers stay here until restored or permanently deleted.'
      : 'No archived Wallet Trackers right now.',
  ].join('\n');
}

function makeWalletTrackerCatalogKeyboard(user) {
  const keyboard = new InlineKeyboard();
  const activeOrders = getVisibleWalletTrackers(user);
  keyboard.text('\u{1F440} New Wallet Tracker', 'wallettracker:new');
  keyboard.row();

  for (const order of activeOrders) {
    keyboard.text(walletTrackerListLabel(order), `wallettracker:open:${order.id}`);
    keyboard.row();
  }

  keyboard.text('\u{1F5C4}\uFE0F Archive', 'nav:wallet_tracker_archive');
  keyboard.row();
  keyboard.text('\u2B05\uFE0F Back', 'nav:home');
  keyboard.text('\u{1F3E0} Home', 'nav:home');
  keyboard.row();
  keyboard.text('\u2139\uFE0F Help', 'nav:help_wallet_tracker');
  keyboard.row();
  keyboard.text('\u{1F504} Refresh', 'refresh:wallet_tracker');
  return keyboard;
}

function makeWalletTrackerArchiveKeyboard(user) {
  const keyboard = new InlineKeyboard();
  const archivedOrders = getVisibleWalletTrackers(user, { archived: true });
  for (const order of archivedOrders) {
    keyboard.text(walletTrackerListLabel(order), `wallettracker:open:${order.id}`);
    keyboard.row();
  }
  keyboard.text('\u2B05\uFE0F Back', 'nav:wallet_tracker');
  keyboard.text('\u{1F3E0} Home', 'nav:home');
  keyboard.row();
  keyboard.text('\u{1F504} Refresh', 'refresh:wallet_tracker_archive');
  return keyboard;
}

function promptForWalletTrackerField(field) {
  switch (field) {
    case 'wallet_address':
      return 'Send the wallet address you want to track next.';
    default:
      return 'Send the requested Wallet Tracker detail next.';
  }
}

function makeWalletTrackerEditorKeyboard(user) {
  const keyboard = new InlineKeyboard();
  const order = user.walletTracker;
  keyboard.text(order.walletAddress ? '\u{1F517} Update Wallet' : '\u{1F517} Set Wallet', `wallettracker:set:wallet_address:${order.id}`);
  keyboard.row();
  if (order.walletAddress) {
    keyboard.url('\u{1F30D} Open On Explorer', `https://solscan.io/account/${order.walletAddress}`);
    keyboard.row();
  }
  keyboard.text(`\u{1F4C8} Buy Alerts: ${walletTrackerBuyModeLabel(order.buyMode)}`, `wallettracker:cycle:buy_mode:${order.id}`);
  keyboard.row();
  keyboard.text(order.notifySells ? '\u2705 Sell Alerts: On' : '\u274C Sell Alerts: Off', `wallettracker:toggle:sells:${order.id}`);
  keyboard.text(order.notifyLaunches ? '\u2705 Launch Alerts: On' : '\u274C Launch Alerts: Off', `wallettracker:toggle:launches:${order.id}`);
  keyboard.row();
  keyboard.text(
    order.walletAddress
      ? (order.automationEnabled ? '\u23F9\uFE0F Stop Tracking' : '\u25B6\uFE0F Start Tracking')
      : '\u{1F512} Start Tracking',
    order.walletAddress ? `wallettracker:toggle:${order.id}` : 'wallettracker:locked:toggle',
  );
  keyboard.row();

  if (order.archivedAt) {
    keyboard.text('\u267B\uFE0F Restore', `wallettracker:restore:${order.id}`);
    keyboard.text(
      order.deleteConfirmations >= 1 ? '\u{1F6A8} Confirm Delete' : '\u{1F5D1}\uFE0F Delete',
      `wallettracker:delete:${order.id}`,
    );
    keyboard.row();
    keyboard.text('\u2B05\uFE0F Back', 'nav:wallet_tracker_archive');
  } else {
    keyboard.text('\u{1F5C4}\uFE0F Archive', `wallettracker:archive:${order.id}`);
    keyboard.row();
    keyboard.text('\u2B05\uFE0F Back', 'nav:wallet_tracker');
  }

  keyboard.text('\u{1F3E0} Home', 'nav:home');
  keyboard.row();
  keyboard.text('\u2139\uFE0F Help', 'nav:help_wallet_tracker');
  keyboard.row();
  keyboard.text('\u{1F504} Refresh', 'refresh:wallet_tracker_editor');
  return keyboard;
}

function walletTrackerEditorText(user) {
  const order = user.walletTracker;
  const launchCount = Number.isInteger(order.stats?.launchCount) ? order.stats.launchCount : 0;
  const buyCount = Number.isInteger(order.stats?.buyAlertCount) ? order.stats.buyAlertCount : 0;
  const sellCount = Number.isInteger(order.stats?.sellAlertCount) ? order.stats.sellAlertCount : 0;
  return [
    '\u{1F440} *Wallet Tracker*',
    '',
    '\u{1F4CB} *Tracker Setup*',
    `- Wallet: ${order.walletAddress ? `\`${order.walletAddress}\`` : '*Not set*'}`,
    `- Status: *${walletTrackerStatusLabel(order)}*`,
    '',
    '\u2699\uFE0F *Alert Rules*',
    `- Buy alerts: *${walletTrackerBuyModeLabel(order.buyMode)}*`,
    `- Sell alerts: *${order.notifySells ? 'On' : 'Off'}*`,
    `- Launch alerts: *${order.notifyLaunches ? 'On' : 'Off'}*`,
    '',
    '\u{1F4C8} *Live Stats*',
    `- Launch alerts sent: *${launchCount}*`,
    `- Buy alerts sent: *${buyCount}*`,
    `- Sell alerts sent: *${sellCount}*`,
    ...(order.lastEventAt ? [`- Last activity seen: ${formatTimestamp(order.lastEventAt)}`] : []),
    ...(order.lastCheckedAt ? [`- Last checked: ${formatTimestamp(order.lastCheckedAt)}`] : []),
    ...(order.lastError ? [`- Last note: \`${order.lastError}\``] : []),
    '',
    order.awaitingField
      ? promptForWalletTrackerField(order.awaitingField)
      : 'Set the wallet above, choose your alert style, then start tracking.',
  ].join('\n');
}

function getResizerPreset(mode) {
  return RESIZER_PRESETS[mode] ?? null;
}

function resizerStatusLabel(resizer) {
  if (resizer.awaitingImage && resizer.mode) {
    return 'ðŸŸ¢ Waiting For Image';
  }
  if (resizer.lastCompletedAt) {
    return 'âœ¨ Ready For Another Image';
  }
  return 'âš™ï¸ Choose A Format';
}

function resizerEditorText(user) {
  const resizer = normalizeResizer(user.resizer);
  const preset = getResizerPreset(resizer.mode);
  const lines = [
    'ðŸ–¼ï¸ *Resizer*',
    '',
    'Turn one image into a clean logo square or a polished banner without stretching it or wrecking the proportions.',
    '',
    'ðŸ“ *Current Setup*',
    `â€¢ Format: ${preset ? `*${preset.emoji} ${preset.label} â€¢ ${preset.ratioLabel}*` : '*Not selected*'}`,
    `â€¢ Status: *${resizerStatusLabel(resizer)}*`,
  ];

  if (preset) {
    lines.push(`â€¢ Output: *${preset.width} x ${preset.height} PNG*`);
  }

  lines.push(
    '',
    'âœ¨ *What The Bot Does*',
    'â€¢ Keeps the original proportions intact',
    'â€¢ Avoids ugly stretching and broken crops',
    'â€¢ Centers the image cleanly for the selected format',
  );

  if (resizer.lastCompletedAt) {
    lines.push(
      '',
      'ðŸ“¦ *Last Result*',
      `â€¢ Finished: ${formatTimestamp(resizer.lastCompletedAt)}`,
      ...(resizer.lastSourceName ? [`â€¢ Source: *${escapeMarkdown(resizer.lastSourceName)}*`] : []),
      ...(resizer.lastOutputWidth && resizer.lastOutputHeight
        ? [`â€¢ Delivered: *${resizer.lastOutputWidth} x ${resizer.lastOutputHeight}*`]
        : []),
    );
  }

  if (resizer.lastError) {
    lines.push('', `âš ï¸ Last note: \`${resizer.lastError}\``);
  }

  lines.push(
    '',
    resizer.awaitingImage && preset
      ? `ðŸ“¤ Send your image as a photo or image file now and Iâ€™ll return a clean *${preset.label.toLowerCase()}* version.`
      : 'Choose *Logo* or *Banner* below to begin.',
  );

  return lines.join('\n');
}

function makeResizerKeyboard(user) {
  const resizer = normalizeResizer(user.resizer);
  const logoPreset = getResizerPreset('logo');
  const bannerPreset = getResizerPreset('banner');
  const keyboard = new InlineKeyboard();

  keyboard.text(
    resizer.mode === 'logo'
      ? `âœ… ${logoPreset.emoji} Logo â€¢ ${logoPreset.ratioLabel}`
      : `${logoPreset.emoji} Logo â€¢ ${logoPreset.ratioLabel}`,
    'resizer:set_mode:logo',
  );
  keyboard.text(
    resizer.mode === 'banner'
      ? `âœ… ${bannerPreset.emoji} Banner â€¢ ${bannerPreset.ratioLabel}`
      : `${bannerPreset.emoji} Banner â€¢ ${bannerPreset.ratioLabel}`,
    'resizer:set_mode:banner',
  );
  keyboard.row();
  keyboard.text('â™»ï¸ Reset', 'resizer:reset');
  keyboard.row();
  keyboard.text('â¬…ï¸ Back', 'nav:home');
  keyboard.text('ðŸ  Home', 'nav:home');
  keyboard.row();
  keyboard.text('â„¹ï¸ Help', 'nav:help_resizer');
  keyboard.row();
  keyboard.text('ðŸ”„ Refresh', 'refresh:resizer');
  return keyboard;
}

function parseCommunityVisionProfileInput(value) {
  const raw = String(value || '').trim();
  if (!raw) {
    throw new Error('Enter an X profile link or @handle.');
  }

  const atMatch = raw.match(/^@?([A-Za-z0-9_]{1,15})$/);
  if (atMatch) {
    const handle = atMatch[1];
    return {
      handle,
      profileUrl: `https://x.com/${handle}`,
    };
  }

  let url;
  try {
    url = new URL(raw);
  } catch {
    throw new Error('That does not look like a valid X profile link or @handle.');
  }

  const hostname = url.hostname.replace(/^www\./i, '').toLowerCase();
  if (!['x.com', 'twitter.com'].includes(hostname)) {
    throw new Error('Use a normal X profile link or @handle.');
  }

  const parts = url.pathname.split('/').filter(Boolean);
  const handle = parts[0];
  if (!handle || !/^[A-Za-z0-9_]{1,15}$/.test(handle)) {
    throw new Error('That X profile link is missing a valid handle.');
  }

  return {
    handle,
    profileUrl: `https://x.com/${handle}`,
  };
}

function cycleWalletTrackerBuyMode(currentMode) {
  switch (currentMode) {
    case 'off':
      return 'first';
    case 'first':
      return 'every';
    default:
      return 'off';
  }
}

function makeBuySellKeyboard(user) {
  const keyboard = new InlineKeyboard();
  const activeWallet = getActiveTradingWallet(user);
  const selectedBundle = user.magicBundles?.find((bundle) => bundle.id === user.tradingDesk.selectedMagicBundleId) ?? null;

  keyboard.text('\u26A1 Quick Trade', 'nav:buy_sell_quick');
  keyboard.text('\u{1F4CC} Limit Orders', 'nav:buy_sell_limit');
  keyboard.row();
  keyboard.text('\u{1F465} Copy Trading', 'nav:buy_sell_copy');
  keyboard.text(user.tradingDesk.quickTradeMintAddress ? '\u{1FA99} Update Token CA' : '\u{1FA99} Set Token CA', 'buy_sell:set_ca');
  keyboard.row();
  keyboard.text(activeWallet ? '\u{1F45B} Wallet Settings' : '\u{1F512} Wallet Settings', 'nav:buy_sell_wallets');
  keyboard.text(selectedBundle ? '\u2705 Choose Bundle' : '\u{1F4E6} Choose Bundle', 'nav:magic_bundle');
  keyboard.row();
  keyboard.text('\u2139\uFE0F Help', 'nav:help_buy_sell');
  keyboard.text('\u{1F4B8} 0.5% Handling', 'noop');
  keyboard.row();
  keyboard.text('\u2B05\uFE0F Back', 'nav:home');
  keyboard.text('\u{1F3E0} Home', 'nav:home');
  keyboard.row();
  keyboard.text('\u{1F504} Refresh', 'refresh:buy_sell');
  return keyboard;
}

function buySellText(user) {
  const tradingDesk = normalizeTradingDesk(user.tradingDesk);
  const activeWallet = getActiveTradingWallet(user);
  const selectedBundle = user.magicBundles?.find((bundle) => bundle.id === tradingDesk.selectedMagicBundleId) ?? null;
  return [
    '\u{1F4B1} *Buy / Sell Desk*',
    '',
    'Fast manual trading, live limit triggers, and follow-wallet automation from one desk.',
    '',
    MENU_DIVIDER,
    '\u2728 *Desk Overview*',
    `â€¢ Active wallet: *${activeWallet ? activeWallet.label : 'Not set'}*`,
    `â€¢ Wallet address: ${activeWallet ? `\`${activeWallet.address}\`` : 'Add or generate a wallet first'}`,
    `â€¢ Wallet count: *${tradingDesk.wallets.length}*`,
    `â€¢ Selected bundle: *${selectedBundle ? (selectedBundle.tokenName || selectedBundle.id) : 'None selected'}*`,
    `â€¢ Quick trade CA: ${tradingDesk.quickTradeMintAddress ? `\`${tradingDesk.quickTradeMintAddress}\`` : '*Not set*'}`,
    '',
    `Ã¢â‚¬Â¢ Quick buy size: *${tradingDesk.quickBuySol || 'Not set'} SOL*`,
    `Ã¢â‚¬Â¢ Quick sell size: *${tradingDesk.quickSellPercent}%*`,
    `Ã¢â‚¬Â¢ Handling fee: *${formatBpsPercent(tradingDesk.handlingFeeBps || cfg.tradingHandlingFeeBps)}* per trade`,
    '\u{1F4CA} *What This Menu Is For*',
    'â€¢ Quick token buy / sell flow',
    'â€¢ Wallet import and wallet generation',
    'â€¢ Bundle selection for multi-wallet execution',
    'â€¢ Limit-order and copy-trading control panels',
    '',
    '\u26A0\uFE0F *Hot-Wallet Warning*',
    'Any wallet imported here should be treated like a live trading wallet with real funds.',
    'Generated and imported private keys stay hidden by default in the bot. Never share them with support.',
    ...(tradingDesk.awaitingField ? ['', promptForBuySellField(tradingDesk.awaitingField)] : []),
    ...(tradingDesk.lastError ? ['', `Last error: \`${tradingDesk.lastError}\``] : []),
  ].join('\n');
}

function makeBuySellWalletsKeyboard(user) {
  const keyboard = new InlineKeyboard();
  const tradingDesk = normalizeTradingDesk(user.tradingDesk);
  const activeWallet = getActiveTradingWallet(user);

  for (const wallet of tradingDesk.wallets.slice(0, 6)) {
    const label = wallet.id === tradingDesk.activeWalletId
      ? `\u2705 ${wallet.label} â€¢ ${wallet.currentSol || '0'} SOL`
      : `${wallet.label} â€¢ ${wallet.currentSol || '0'} SOL`;
    keyboard.text(label, `buy_sell:select_wallet:${wallet.id}`);
    keyboard.row();
  }

  keyboard.text('\u{1F4E5} Import Wallet', 'buy_sell:import_wallet');
  keyboard.text('\u{1F195} Generate Wallet', 'buy_sell:generate_wallet');
  keyboard.row();
  if (activeWallet) {
    keyboard.text(
      activeWallet.privateKeyVisible ? '\u{1F648} Hide Private Key' : '\u{1F441}\uFE0F Show Private Key',
      `buy_sell:key_toggle:${activeWallet.id}`,
    );
    keyboard.row();
  }
  keyboard.text('\u2B05\uFE0F Back', 'nav:buy_sell');
  keyboard.text('\u{1F3E0} Home', 'nav:home');
  keyboard.row();
  keyboard.text('\u{1F504} Refresh', 'refresh:buy_sell_wallets');
  return keyboard;
}

function buySellWalletsText(user) {
  const tradingDesk = normalizeTradingDesk(user.tradingDesk);
  const activeWallet = getActiveTradingWallet(user);
  const walletLines = tradingDesk.wallets.length > 0
    ? tradingDesk.wallets.slice(0, 6).map((wallet, index) => (
      `â€¢ #${index + 1} ${wallet.id === tradingDesk.activeWalletId ? '\u2705' : '\u25CB'} *${wallet.label}* â€¢ \`${wallet.address.slice(0, 4)}...${wallet.address.slice(-4)}\` â€¢ ${wallet.currentSol || '0'} SOL`
    ))
    : ['â€¢ No trading wallets yet. Import one or generate one below.'];
  return [
    '\u{1F45B} *Wallet Settings*',
    '',
    'Manage the wallets used by the Buy / Sell desk.',
    '',
    '\u{1F4B3} *Active Wallet*',
    activeWallet
      ? `â€¢ *${activeWallet.label}* â€¢ \`${activeWallet.address}\``
      : 'â€¢ No active wallet selected yet.',
    ...(activeWallet ? [`â€¢ Private key: ${activeWallet.privateKeyVisible ? `\`${activeWallet.secretKeyBase58}\`` : '*Hidden*'}`] : []),
    '',
    '\u{1F4DC} *Wallet List*',
    ...walletLines,
    ...(tradingDesk.awaitingField ? ['', promptForBuySellField(tradingDesk.awaitingField)] : []),
  ].join('\n');
}

function makeBuySellQuickKeyboard(user) {
  const keyboard = new InlineKeyboard();
  keyboard.text(user.tradingDesk.quickTradeMintAddress ? '\u{1FA99} Update CA' : '\u{1FA99} Set CA', 'buy_sell:set_ca');
  keyboard.text('\u{1F45B} Wallet Settings', 'nav:buy_sell_wallets');
  keyboard.row();
  keyboard.text('\u{1F4E6} Choose Bundle', 'nav:magic_bundle');
  keyboard.text('\u{1F4CC} Limit Orders', 'nav:buy_sell_limit');
  keyboard.row();
  keyboard.text('\u2B05\uFE0F Back', 'nav:buy_sell');
  keyboard.text('\u{1F3E0} Home', 'nav:home');
  keyboard.row();
  keyboard.text('\u{1F504} Refresh', 'refresh:buy_sell_quick');
  return keyboard;
}

function buySellQuickText(user) {
  const activeWallet = getActiveTradingWallet(user);
  const tradingDesk = normalizeTradingDesk(user.tradingDesk);
  return [
    '\u26A1 *Quick Buy / Sell*',
    '',
    'Paste a token CA to get this desk ready for fast trading.',
    '',
    `â€¢ Token CA: ${tradingDesk.quickTradeMintAddress ? `\`${tradingDesk.quickTradeMintAddress}\`` : '*Not set*'}`,
    `â€¢ Active wallet: *${activeWallet ? activeWallet.label : 'Not set'}*`,
    `â€¢ Wallet ready: *${activeWallet ? 'Yes' : 'No'}*`,
    '',
    'Use this desk to keep your active wallet, token CA, and bundle selection organized for trading.',
    ...(tradingDesk.awaitingField ? ['', promptForBuySellField(tradingDesk.awaitingField)] : []),
  ].join('\n');
}

function makeBuySellInfoKeyboard(backRoute, refreshRoute) {
  return new InlineKeyboard()
    .text('\u2B05\uFE0F Back', backRoute)
    .text('\u{1F3E0} Home', 'nav:home')
    .row()
    .text('\u2139\uFE0F Help', 'nav:help_buy_sell')
    .row()
    .text('\u{1F504} Refresh', `refresh:${refreshRoute}`);
}

function buySellLimitText() {
  return [
    '\u{1F4CC} *Limit Orders*',
    '',
    'This section is for setting automatic entries and exits instead of staring at the chart all day.',
    '',
    'Professional trading bots typically let you:',
    'â€¢ place limit buys below current price',
    'â€¢ place take-profit sells above current price',
    'â€¢ place stop-loss sells below current price',
    'â€¢ choose a validity window for each order',
    '',
    'Use this menu as the control center for structured entries, profit targets, and downside protection.',
  ].join('\n');
}

function buySellCopyText() {
  return [
    '\u{1F465} *Copy Trading*',
    '',
    'This section is for following another wallet and copying its buys and sells with your own trading wallet.',
    '',
    'Professional copy-trade flows usually include:',
    'â€¢ wallet to follow',
    'â€¢ buy amount rules',
    'â€¢ sell matching rules',
    'â€¢ stop-loss / take-profit controls',
    'â€¢ whitelist / blacklist safety filters',
    '',
    'Use this menu as the control center for wallet-following rules, sizing, and risk controls.',
  ].join('\n');
}

function makeBuySellQuickLiveKeyboard(user) {
  const keyboard = new InlineKeyboard();
  const desk = normalizeTradingDesk(user.tradingDesk);
  keyboard.text(user.tradingDesk.quickTradeMintAddress ? '\u{1FA99} Update CA' : '\u{1FA99} Set CA', 'buy_sell:set_ca');
  keyboard.text(desk.quickBuySol ? `\u{1F4B0} Buy ${desk.quickBuySol} SOL` : '\u{1F4B0} Set Buy SOL', 'buy_sell:set_buy_sol');
  keyboard.row();
  keyboard.text(`\u{1F4E4} Sell ${desk.quickSellPercent}%`, 'buy_sell:set_sell_percent');
  keyboard.text('\u{1F4E6} Choose Bundle', 'nav:magic_bundle');
  keyboard.row();
  keyboard.text('\u{1F7E2} Buy Now', 'buy_sell:execute:buy');
  keyboard.text('\u{1F534} Sell Now', 'buy_sell:execute:sell');
  keyboard.row();
  keyboard.text('\u{1F45B} Wallet Settings', 'nav:buy_sell_wallets');
  keyboard.text('\u{1F4CC} Limit Orders', 'nav:buy_sell_limit');
  keyboard.row();
  keyboard.text('\u2B05\uFE0F Back', 'nav:buy_sell');
  keyboard.text('\u{1F3E0} Home', 'nav:home');
  keyboard.row();
  keyboard.text('\u{1F504} Refresh', 'refresh:buy_sell_quick');
  return keyboard;
}

function buySellQuickLiveText(user) {
  const activeWallet = getActiveTradingWallet(user);
  const tradingDesk = normalizeTradingDesk(user.tradingDesk);
  const selectedBundle = user.magicBundles?.find((bundle) => bundle.id === tradingDesk.selectedMagicBundleId) ?? null;
  return [
    '\u26A1 *Quick Buy / Sell*',
    '',
    'Queue an immediate market buy or sell from your active wallet or selected bundle wallets.',
    '',
    `Ã¢â‚¬Â¢ Token CA: ${tradingDesk.quickTradeMintAddress ? `\`${tradingDesk.quickTradeMintAddress}\`` : '*Not set*'}`,
    `Ã¢â‚¬Â¢ Active wallet: *${activeWallet ? activeWallet.label : 'Not set'}*`,
    `Ã¢â‚¬Â¢ Selected bundle: *${selectedBundle ? (selectedBundle.tokenName || selectedBundle.id) : 'None'}*`,
    `Ã¢â‚¬Â¢ Buy size: *${tradingDesk.quickBuySol || 'Not set'} SOL*`,
    `Ã¢â‚¬Â¢ Sell size: *${tradingDesk.quickSellPercent}%*`,
    `Ã¢â‚¬Â¢ Wallet ready: *${activeWallet ? 'Yes' : 'No'}*`,
    `Ã¢â‚¬Â¢ Pending action: *${tradingDesk.pendingAction ? `${tradingDesk.pendingAction.type} queued` : 'None'}*`,
    '',
    `Handling fee: *${formatBpsPercent(tradingDesk.handlingFeeBps || cfg.tradingHandlingFeeBps)}* per executed trade.`,
    ...(tradingDesk.awaitingField ? ['', promptForBuySellField(tradingDesk.awaitingField)] : []),
    ...(tradingDesk.lastTradeSignature ? ['', `Last trade: \`${tradingDesk.lastTradeSignature}\``] : []),
    ...(tradingDesk.lastError ? ['', `Last error: \`${tradingDesk.lastError}\``] : []),
  ].join('\n');
}

function makeBuySellLimitLiveKeyboard(user) {
  const keyboard = new InlineKeyboard();
  const desk = normalizeTradingDesk(user.tradingDesk);
  keyboard.text(desk.limitOrder.side === 'buy' ? '\u2705 Buy Trigger' : 'Buy Trigger', 'buy_sell:limit:side:buy');
  keyboard.text(desk.limitOrder.side === 'sell' ? '\u2705 Sell Trigger' : 'Sell Trigger', 'buy_sell:limit:side:sell');
  keyboard.row();
  keyboard.text(
    desk.limitOrder.triggerMarketCapUsd
      ? `\u{1F3AF} MC $${Math.round(desk.limitOrder.triggerMarketCapUsd).toLocaleString('en-US')}`
      : '\u{1F3AF} Set Trigger MC',
    'buy_sell:limit:set_trigger',
  );
  keyboard.text(
    desk.limitOrder.side === 'buy'
      ? (desk.limitOrder.buySol ? `\u{1F4B0} Buy ${desk.limitOrder.buySol} SOL` : '\u{1F4B0} Set Buy SOL')
      : `\u{1F4E4} Sell ${desk.limitOrder.sellPercent}%`,
    desk.limitOrder.side === 'buy' ? 'buy_sell:limit:set_buy_sol' : 'buy_sell:limit:set_sell_percent',
  );
  keyboard.row();
  keyboard.text(desk.limitOrder.enabled ? '\u23F9\uFE0F Stop Limit' : '\u25B6\uFE0F Start Limit', 'buy_sell:limit:toggle');
  keyboard.text('\u{1F5D1}\uFE0F Clear', 'buy_sell:limit:clear');
  keyboard.row();
  keyboard.text('\u2B05\uFE0F Back', 'nav:buy_sell');
  keyboard.text('\u{1F3E0} Home', 'nav:home');
  keyboard.row();
  keyboard.text('\u{1F504} Refresh', 'refresh:buy_sell_limit');
  return keyboard;
}

function buySellLimitLiveText(user) {
  const desk = normalizeTradingDesk(user.tradingDesk);
  return [
    '\u{1F4CC} *Limit Orders*',
    '',
    'Arm a market-cap trigger and let the worker execute the trade automatically when the level is reached.',
    '',
    `Ã¢â‚¬Â¢ Side: *${desk.limitOrder.side === 'buy' ? 'Buy' : 'Sell'}*`,
    `Ã¢â‚¬Â¢ Trigger MC: *${desk.limitOrder.triggerMarketCapUsd ? `$${Math.round(desk.limitOrder.triggerMarketCapUsd).toLocaleString('en-US')}` : 'Not set'}*`,
    `Ã¢â‚¬Â¢ Buy size: *${desk.limitOrder.buySol || 'Not set'} SOL*`,
    `Ã¢â‚¬Â¢ Sell size: *${desk.limitOrder.sellPercent}%*`,
    `Ã¢â‚¬Â¢ Status: *${desk.limitOrder.enabled ? 'Armed' : 'Stopped'}*`,
    '',
    'Buy triggers fire when market cap is at or below your target. Sell triggers fire when market cap is at or above your target.',
    ...(desk.awaitingField ? ['', promptForBuySellField(desk.awaitingField)] : []),
    ...(desk.limitOrder.lastTriggerSignature ? ['', `Last trigger: \`${desk.limitOrder.lastTriggerSignature}\``] : []),
    ...(desk.limitOrder.lastError ? ['', `Last error: \`${desk.limitOrder.lastError}\``] : []),
  ].join('\n');
}

function makeBuySellCopyLiveKeyboard(user) {
  const keyboard = new InlineKeyboard();
  const desk = normalizeTradingDesk(user.tradingDesk);
  keyboard.text(
    desk.copyTrade.followWalletAddress
      ? `\u{1F441}\uFE0F ${desk.copyTrade.followWalletAddress.slice(0, 4)}...${desk.copyTrade.followWalletAddress.slice(-4)}`
      : '\u{1F441}\uFE0F Set Follow Wallet',
    'buy_sell:copy:set_wallet',
  );
  keyboard.text(
    desk.copyTrade.fixedBuySol
      ? `\u{1F4B0} ${desk.copyTrade.fixedBuySol} SOL`
      : '\u{1F4B0} Set Buy SOL',
    'buy_sell:copy:set_amount',
  );
  keyboard.row();
  keyboard.text(desk.copyTrade.copySells ? '\u2705 Copy Sells' : 'Copy Sells Off', 'buy_sell:copy:toggle_sells');
  keyboard.text(desk.copyTrade.enabled ? '\u23F9\uFE0F Stop Copy' : '\u25B6\uFE0F Start Copy', 'buy_sell:copy:toggle');
  keyboard.row();
  keyboard.text('\u2B05\uFE0F Back', 'nav:buy_sell');
  keyboard.text('\u{1F3E0} Home', 'nav:home');
  keyboard.row();
  keyboard.text('\u{1F504} Refresh', 'refresh:buy_sell_copy');
  return keyboard;
}

function buySellCopyLiveText(user) {
  const desk = normalizeTradingDesk(user.tradingDesk);
  return [
    '\u{1F465} *Copy Trading*',
    '',
    'Follow another wallet and mirror its buys and sells with your own active trading wallet or selected bundle.',
    '',
    `Ã¢â‚¬Â¢ Follow wallet: ${desk.copyTrade.followWalletAddress ? `\`${desk.copyTrade.followWalletAddress}\`` : '*Not set*'}`,
    `Ã¢â‚¬Â¢ Fixed buy size: *${desk.copyTrade.fixedBuySol || 'Not set'} SOL*`,
    `Ã¢â‚¬Â¢ Copy sells: *${desk.copyTrade.copySells ? 'Yes' : 'No'}*`,
    `Ã¢â‚¬Â¢ Status: *${desk.copyTrade.enabled ? 'Watching' : 'Stopped'}*`,
    `Ã¢â‚¬Â¢ Copied buys: *${desk.copyTrade.stats.buyCount}*`,
    `Ã¢â‚¬Â¢ Copied sells: *${desk.copyTrade.stats.sellCount}*`,
    '',
    'Buys use your fixed SOL size. Sells mirror the tracked walletâ€™s sell fraction when copy-sells is enabled.',
    ...(desk.awaitingField ? ['', promptForBuySellField(desk.awaitingField)] : []),
    ...(desk.copyTrade.lastError ? ['', `Last error: \`${desk.copyTrade.lastError}\``] : []),
  ].join('\n');
}

appleBoosterListLabel = function appleBoosterListLabel(order) {
  const pkg = getAppleBoosterPackage(order.strategy, order.packageKey);
  const statusLabel = order.archivedAt
    ? '\u26AB Archived'
    : (order.running
      ? '\u{1F7E2} Running'
      : (order.appleBooster?.stopRequested ? '\u23F3 Stopping' : (order.funded ? '\u{1F7E1} Funded' : '\u{1F7E0} Awaiting Deposit')));
  const packageLabel = order.freeTrial ? 'Free Trial' : (pkg?.label || order.packageKey?.toUpperCase() || 'Booster');
  const mintLabel = order.appleBooster?.mintAddress
    ? ` \u2022 ${order.appleBooster.mintAddress.slice(0, 4)}...${order.appleBooster.mintAddress.slice(-4)}`
    : '';
  const modeIcon = order.freeTrial ? '\u{1F9EA}' : (order.strategy === 'bundled' ? '\u{1F4E6}' : '\u{1F34F}');
  const modeLabel = order.freeTrial ? 'Demo' : (order.strategy === 'bundled' ? 'Bundle' : 'Booster');
  return `${modeIcon} ${packageLabel} ${modeLabel}${mintLabel} \u2022 ${statusLabel}`;
};

makeVolumeKeyboard = function makeVolumeKeyboard(selectedMode) {
  const organicLabel = selectedMode === 'organic'
    ? '\u2705 \u{1F34F} Organic Volume Booster'
    : '\u{1F34F} Organic Volume Booster';
  const bundledLabel = selectedMode === 'bundled'
    ? '\u2705 \u{1F4E6} Bundled Volume Booster'
    : '\u{1F4E6} Bundled Volume Booster';

  return new InlineKeyboard()
    .text(organicLabel, 'volume:organic')
    .row()
    .text(bundledLabel, 'volume:bundled')
    .row()
    .text('\u2B05\uFE0F Back', 'nav:home')
    .text('\u{1F3E0} Home', 'nav:home')
    .row()
    .text('\u2139\uFE0F Help', 'nav:help_volume')
    .row()
    .text('\u{1F504} Refresh', 'refresh:volume');
};

organicVolumeText = function organicVolumeText(user) {
  const selectedPackage = getOrganicVolumePackage(user.organicVolumePackage);
  const activeBoosters = getVisibleAppleBoostersByStrategy(user, 'organic', { archived: false });
  const archivedBoosters = getVisibleAppleBoosters(user, { archived: true });
  const existingTrial = activeBoosters.find((order) => order.freeTrial);
  const volumeTrialStatus = existingTrial
    ? '\u{1F9EA} Your free trial demo is already created below and ready to open.'
    : (user.volumeFreeTrialUsed
      ? '\u2705 This Telegram account has already used the Volume Bot Trial.'
      : (cfg.volumeTrialEnabled
        ? `\u{1F9EA} One Volume Bot Trial is available with about ${cfg.volumeTrialTradeGoal} tiny live trade legs.`
        : '\u26A0\uFE0F The Volume Bot Trial is offline right now.'));
  return [
    '\u{1F34F} *Organic Volume Booster*',
    '',
    "Boost your token's volume with a more natural-looking execution flow.",
    '',
    MENU_DIVIDER,
    '\u{1F4CA} *What You Get*',
    'â€¢ Randomized wallet activity and trade sizing',
    'â€¢ Internal worker-wallet rotation',
    'â€¢ Live progress, cost, and runtime estimates',
    'â€¢ Archive support for completed boosters',
    '',
    `Active boosters: *${activeBoosters.length}*`,
    `Archived boosters: *${archivedBoosters.length}*`,
    '',
    '\u{1F9EA} *Volume Bot Trial*',
    volumeTrialStatus,
    '',
    '\u{1F4E6} *Organic Packages* (`\u21A9\uFE0F Creator rebate included`)',
    ...ORGANIC_VOLUME_PACKAGES.map((pkg) => `${pkg.emoji} ${pkg.label} - *${pkg.priceSol} SOL* (\u21A9\uFE0F ${pkg.rebateSol} SOL)`),
    ...(selectedPackage ? ['', `\u2705 Selected package: ${selectedPackage.emoji} *${selectedPackage.label}*`] : []),
    '',
    '\u{1F447} Choose a package below to create a new Organic Volume Booster.',
  ].join('\n');
};

organicVolumeArchiveText = function organicVolumeArchiveText(user) {
  const archivedBoosters = getVisibleAppleBoosters(user, { archived: true });
  return [
    '\u{1F5C4}\uFE0F *Volume Booster Archive*',
    '',
    archivedBoosters.length > 0
      ? 'Archived boosters stay here until you restore them or permanently delete them.'
      : 'No archived Volume Boosters yet.',
    '',
    'Permanent delete is only available from inside an archived booster.',
  ].join('\n');
};

bundledVolumeText = function bundledVolumeText() {
  return [
    '\u{1F4E6} *Bundled Volume Booster*',
    '',
    'High-throughput bundled volume optimized for momentum and tighter execution.',
    '',
    MENU_DIVIDER,
    '\u26A1\uFE0F *Bundled Advantages*',
    'â€¢ Same-slot Jito bundle execution',
    'â€¢ Lower cost profile than standard routed activity',
    'â€¢ Built for faster chart momentum',
    'â€¢ Automatic treasury/dev split handling before execution',
    '',
    '\u{1F4CA} *Bundle Packages* (`\u21A9\uFE0F Creator rebate included`)',
    ...BUNDLED_VOLUME_PACKAGES.map((pkg) => `${pkg.emoji} ${pkg.label} - *${pkg.priceSol} SOL* (\u21A9\uFE0F ${pkg.rebateSol} SOL)`),
    '',
    '\u{1F447} Choose a package below to create a new Bundled Volume Booster.',
  ].join('\n');
};

makeOrganicVolumeKeyboard = function makeOrganicVolumeKeyboard(user) {
  const keyboard = new InlineKeyboard();
  const selectedPackageKey = user.organicVolumePackage;
  const activeBoosters = getVisibleAppleBoostersByStrategy(user, 'organic', { archived: false });
  const existingTrial = activeBoosters.find((order) => order.freeTrial);

  ORGANIC_VOLUME_PACKAGES.forEach((pkg, index) => {
    const label = selectedPackageKey === pkg.key
      ? `\u2705 ${pkg.emoji} New ${pkg.label} Organic Booster`
      : `${pkg.emoji} New ${pkg.label} Organic Booster`;

    keyboard.text(label, `organic:${pkg.key}`);
    if ((index + 1) % 2 === 0) {
      keyboard.row();
    }
  });

  keyboard.row();
  if (existingTrial) {
    keyboard.text('\u{1F9EA} Open Free Trial Demo', `organic:open:${existingTrial.id}`);
  } else if (user.volumeFreeTrialUsed) {
    keyboard.text('\u2705 Free Trial Used', 'organic:trial');
  } else if (cfg.volumeTrialEnabled) {
    keyboard.text('\u{1F9EA} Start Free Trial Demo', 'organic:trial');
  } else {
    keyboard.text('\u26A0\uFE0F Free Trial Offline', 'organic:trial');
  }

  if (activeBoosters.length > 0) {
    keyboard.row();
    for (const order of activeBoosters) {
      keyboard.text(appleBoosterListLabel(order), `organic:open:${order.id}`);
      keyboard.row();
    }
  }

  if (getVisibleAppleBoosters(user, { archived: true }).length > 0) {
    keyboard.text('\u{1F5C4}\uFE0F Archive', 'nav:volume_archive');
    keyboard.row();
  }

  keyboard.row()
    .text('\u2B05\uFE0F Back', 'nav:home')
    .text('\u{1F3E0} Home', 'nav:home');
  keyboard.row().text('\u{1F504} Refresh', 'refresh:volume_organic');
  return keyboard;
};

makeBundledVolumeKeyboard = function makeBundledVolumeKeyboard(user) {
  const keyboard = new InlineKeyboard();
  const selectedPackageKey = user.organicVolumePackage;
  const activeBoosters = getVisibleAppleBoostersByStrategy(user, 'bundled', { archived: false });

  BUNDLED_VOLUME_PACKAGES.forEach((pkg, index) => {
    const label = selectedPackageKey === pkg.key && user.organicVolumeOrder?.strategy === 'bundled'
      ? `\u2705 ${pkg.emoji} New ${pkg.label} Bundle`
      : `${pkg.emoji} New ${pkg.label} Bundle`;

    keyboard.text(label, `bundled:${pkg.key}`);
    if ((index + 1) % 2 === 0) {
      keyboard.row();
    }
  });

  if (activeBoosters.length > 0) {
    keyboard.row();
    for (const order of activeBoosters) {
      keyboard.text(appleBoosterListLabel(order), `organic:open:${order.id}`);
      keyboard.row();
    }
  }

  if (getVisibleAppleBoosters(user, { archived: true }).length > 0) {
    keyboard.text('\u{1F5C4}\uFE0F Archive', 'nav:volume_archive');
    keyboard.row();
  }

  keyboard.row()
    .text('\u2B05\uFE0F Back', 'nav:volume')
    .text('\u{1F3E0} Home', 'nav:home');
  keyboard.row().text('\u{1F504} Refresh', 'refresh:volume_bundled');
  return keyboard;
};

volumeText = function volumeText(user) {
  return [
    '\u{1F4CA} *Volume Booster*',
    '',
    'Choose the volume mode you want to use.',
    '',
    '\u{1F34F} Organic Volume Booster: a more natural-looking routed volume flow',
    '\u{1F4E6} Bundled Volume Booster: tighter bundled execution for faster momentum',
    ...(user.volumeMode
      ? ['', `*Selected mode:* ${user.volumeMode === 'organic' ? '\u{1F34F} Organic Volume Booster' : '\u{1F4E6} Bundled Volume Booster'}`]
      : []),
  ].join('\n');
};

organicVolumeText = function organicVolumeText(user) {
  const selectedPackage = getOrganicVolumePackage(user.organicVolumePackage);
  const activeBoosters = getVisibleAppleBoostersByStrategy(user, 'organic', { archived: false });
  const archivedBoosters = getVisibleAppleBoosters(user, { archived: true });
  return [
    '\u{1F34F} *Organic Volume Booster*',
    '',
    "Boost your token's volume with a more natural-looking execution flow.",
    '',
    MENU_DIVIDER,
    '\u{1F4CA} *What You Get*',
    '- Randomized wallet activity and trade sizing',
    '- Rotates across multiple wallets for a cleaner trading pattern',
    '- Live progress, cost, and runtime estimates',
    '- Archive support for completed boosters',
    '',
    `Active boosters: *${activeBoosters.length}*`,
    `Archived boosters: *${archivedBoosters.length}*`,
    '',
    '\u{1F4E6} *Organic Packages* (`\u21A9\uFE0F Creator rebate included`)',
    ...ORGANIC_VOLUME_PACKAGES.map((pkg) => `${pkg.emoji} ${pkg.label} - *${pkg.priceSol} SOL* (\u21A9\uFE0F ${pkg.rebateSol} SOL)`),
    ...(selectedPackage ? ['', `\u2705 Selected package: ${selectedPackage.emoji} *${selectedPackage.label}*`] : []),
    '',
    '\u{1F447} Choose a package below to create a new Organic Volume Booster.',
  ].join('\n');
};

bundledVolumeText = function bundledVolumeText() {
  return [
    '\u{1F4E6} *Bundled Volume Booster*',
    '',
    'High-throughput bundled volume optimized for momentum and tighter execution.',
    '',
    MENU_DIVIDER,
    '\u26A1\uFE0F *Bundled Advantages*',
    '- Same-slot Jito bundle execution',
    '- Lower cost profile than standard routed activity',
    '- Built for faster chart momentum',
    '- Simple setup with everything handled automatically once you start',
    '',
    '\u{1F4CA} *Bundle Packages* (`\u21A9\uFE0F Creator rebate included`)',
    ...BUNDLED_VOLUME_PACKAGES.map((pkg) => `${pkg.emoji} ${pkg.label} - *${pkg.priceSol} SOL* (\u21A9\uFE0F ${pkg.rebateSol} SOL)`),
    '',
    '\u{1F447} Choose a package below to create a new Bundled Volume Booster.',
  ].join('\n');
};

makeOrganicVolumeKeyboard = function makeOrganicVolumeKeyboard(user) {
  const keyboard = new InlineKeyboard();
  const selectedPackageKey = user.organicVolumePackage;
  const activeBoosters = getVisibleAppleBoostersByStrategy(user, 'organic', { archived: false });
  const existingTrial = activeBoosters.find((order) => order.freeTrial);

  ORGANIC_VOLUME_PACKAGES.forEach((pkg, index) => {
    const label = selectedPackageKey === pkg.key
      ? `\u2705 ${pkg.emoji} ${pkg.label} \u2022 ${pkg.priceSol} SOL`
      : `${pkg.emoji} ${pkg.label} \u2022 ${pkg.priceSol} SOL`;

    keyboard.text(label, `organic:${pkg.key}`);
    if ((index + 1) % 2 === 0) {
      keyboard.row();
    }
  });

  keyboard.row();
  if (existingTrial) {
    keyboard.text('\u{1F9EA} Open Volume Bot Trial', `organic:open:${existingTrial.id}`);
  } else if (user.volumeFreeTrialUsed) {
    keyboard.text('\u2705 Volume Trial Used', 'organic:trial');
  } else if (cfg.volumeTrialEnabled) {
    keyboard.text('\u{1F9EA} Volume Bot Trial', 'organic:trial');
  } else {
    keyboard.text('\u26A0\uFE0F Volume Trial Offline', 'organic:trial');
  }

  const visibleBoosters = activeBoosters.filter((order) => !order.freeTrial);

  if (visibleBoosters.length > 0) {
    keyboard.row();
    for (const order of visibleBoosters) {
      keyboard.text(appleBoosterListLabel(order), `organic:open:${order.id}`);
      keyboard.row();
    }
  }

  keyboard.text('\u{1F5C4}\uFE0F Archive', 'nav:volume_archive');
  keyboard.row();
  keyboard.row()
    .text('\u2B05\uFE0F Back', 'nav:volume')
    .text('\u{1F3E0} Home', 'nav:home');
  keyboard.row().text('\u2139\uFE0F Help', 'nav:help_volume');
  keyboard.row().text('\u{1F504} Refresh', 'refresh:volume_organic');
  return keyboard;
};

makeBundledVolumeKeyboard = function makeBundledVolumeKeyboard(user) {
  const keyboard = new InlineKeyboard();
  const selectedPackageKey = user.organicVolumePackage;
  const activeBoosters = getVisibleAppleBoostersByStrategy(user, 'bundled', { archived: false });

  BUNDLED_VOLUME_PACKAGES.forEach((pkg, index) => {
    const label = selectedPackageKey === pkg.key && user.organicVolumeOrder?.strategy === 'bundled'
      ? `\u2705 ${pkg.emoji} ${pkg.label} \u2022 ${pkg.priceSol} SOL`
      : `${pkg.emoji} ${pkg.label} \u2022 ${pkg.priceSol} SOL`;

    keyboard.text(label, `bundled:${pkg.key}`);
    if ((index + 1) % 2 === 0) {
      keyboard.row();
    }
  });

  if (activeBoosters.length > 0) {
    keyboard.row();
    for (const order of activeBoosters) {
      keyboard.text(appleBoosterListLabel(order), `organic:open:${order.id}`);
      keyboard.row();
    }
  }

  keyboard.text('\u{1F5C4}\uFE0F Archive', 'nav:volume_archive');
  keyboard.row();
  keyboard.row()
    .text('\u2B05\uFE0F Back', 'nav:volume')
    .text('\u{1F3E0} Home', 'nav:home');
  keyboard.row().text('\u2139\uFE0F Help', 'nav:help_volume');
  keyboard.row().text('\u{1F504} Refresh', 'refresh:volume_bundled');
  return keyboard;
};

burnAgentCatalogDisplayLabel = function burnAgentCatalogDisplayLabel(agent) {
  const archivedTag = isArchivedBurnAgent(agent) ? ' \u2022 Archived' : '';
  const status = agent.automationEnabled ? '\u{1F7E2} Running' : '\u26AA Stopped';
  const speed = agent.speed === 'lightning' ? 'Fast' : (agent.speed === 'normal' ? 'Normal' : 'Agent');
  const tokenLabel = agent.tokenName?.trim()
    || (agent.mintAddress ? `${agent.mintAddress.slice(0, 4)}...${agent.mintAddress.slice(-4)}` : 'Complete Setup');
  return `\u{1F916} ${tokenLabel} \u2022 ${speed} \u2022 ${status}${archivedTag}`;
};

burnAgentCatalogText = function burnAgentCatalogText(user) {
  const activeAgents = getVisibleBurnAgents(user, { archived: false });
  const archivedCount = getVisibleBurnAgents(user, { archived: true }).length;
  return [
    '\u{1F916} *Burn Agent*',
    '',
    'Automated creator-reward claiming, token buybacks, and real burns tied to the wallet you configure.',
    '',
    '\u26A0\uFE0F *Important:* this is a serious wallet flow. Any private key added here is treated as a live hot wallet.',
    '\u26A0\uFE0F Never paste a sensitive team wallet unless you fully understand the risk.',
    '\u26A0\uFE0F Keys are hidden by default in the bot, but support should never need your private key.',
    '',
    `Active agents: *${activeAgents.length}*`,
    `Archived agents: *${archivedCount}*`,
    '',
    '\u26A1 *Fast Burn Agent*: your creator wallet is used directly.',
    '\u{1F422} *Normal Burn Agent*: you route Pump.fun creator rewards to a managed wallet instead.',
    '',
    '\u{1F447} Create a new agent below or open an existing one to manage it.',
  ].join('\n');
};

burnAgentArchiveText = function burnAgentArchiveText(user) {
  const archivedAgents = getVisibleBurnAgents(user, { archived: true });
  return [
    '\u{1F5C4}\uFE0F *Burn Agent Archive*',
    '',
    archivedAgents.length > 0
      ? 'Archived agents stay here until restored or permanently deleted.'
      : 'No archived burn agents yet.',
    '',
    'Permanent delete is only available from inside an archived agent.',
  ].join('\n');
};

makeBurnAgentCatalogKeyboard = function makeBurnAgentCatalogKeyboard(user) {
  const keyboard = new InlineKeyboard();
  const activeAgents = getVisibleBurnAgents(user, { archived: false });

  keyboard.text('\u26A1 New Fast Burn Agent', 'burn:new:lightning');
  keyboard.row();
  keyboard.text('\u{1F422} New Normal Burn Agent', 'burn:new:normal');
  keyboard.row();

  for (const agent of activeAgents) {
    keyboard.text(burnAgentCatalogDisplayLabel(agent), `burn:open:${agent.id}`);
    keyboard.row();
  }

  if (getVisibleBurnAgents(user, { archived: true }).length > 0) {
    keyboard.text('\u{1F5C4}\uFE0F Archive', 'nav:burn_agent_archive');
    keyboard.row();
  }

  keyboard.text('\u2B05\uFE0F Back', 'nav:home');
  keyboard.text('\u{1F3E0} Home', 'nav:home');
  keyboard.row().text('\u2139\uFE0F Help', 'nav:help_burn_agent');
  keyboard.row().text('\u{1F504} Refresh', 'refresh:burn_agent');
  return keyboard;
};

makeBurnAgentArchiveKeyboard = function makeBurnAgentArchiveKeyboard(user) {
  const keyboard = new InlineKeyboard();
  const archivedAgents = getVisibleBurnAgents(user, { archived: true });

  for (const agent of archivedAgents) {
    keyboard.text(burnAgentCatalogDisplayLabel(agent), `burn:open:${agent.id}`);
    keyboard.row();
  }

  keyboard.text('\u2B05\uFE0F Back', 'nav:burn_agent');
  keyboard.text('\u{1F3E0} Home', 'nav:home');
  keyboard.row().text('\u{1F504} Refresh', 'refresh:burn_agent_archive');
  return keyboard;
};

holderBoosterStatusLabel = function holderBoosterStatusLabel(order) {
  switch (order.status) {
    case 'awaiting_funding':
      return '\u{1F7E0} Awaiting Deposit';
    case 'ready':
      return '\u{1F7E1} Ready';
    case 'processing':
      return '\u{1F7E2} Processing';
    case 'completed':
      return '\u2705 Completed';
    case 'failed':
      return '\u26D4 Failed';
    default:
      return '\u26AA Setup';
  }
};

makeHolderBoosterKeyboard = function makeHolderBoosterKeyboard(user) {
  const keyboard = new InlineKeyboard();
  const order = user.holderBooster;

  keyboard.text('\u{1F504} Refresh', 'holder:refresh');
  keyboard.row();
  keyboard.text(order.mintAddress ? '\u{1FA99} Update Mint' : '\u{1FA99} Set Mint', 'holder:set:mint');
  keyboard.text(
    Number.isInteger(order.holderCount) ? `\u{1F465} Holders: ${order.holderCount}` : '\u{1F465} Set Holder Count',
    'holder:set:holder_count',
  );
  keyboard.row();
  keyboard.text('\u2728 New Holder Booster', 'holder:new');
  keyboard.row();
  keyboard.text('\u2139\uFE0F Help', 'nav:help_holder_booster');
  keyboard.row();
  keyboard.text('\u2B05\uFE0F Back', 'nav:home');
  keyboard.text('\u{1F3E0} Home', 'nav:home');
  return keyboard;
};

holderBoosterText = function holderBoosterText(user) {
  const order = user.holderBooster;
  const previewWallets = Array.isArray(order.childWallets) ? order.childWallets.slice(0, 5) : [];
  const requiredTokens = Number.isInteger(order.holderCount) ? String(order.holderCount) : 'Pending';
  const promptLine = order.awaitingField
    ? `${promptForHolderField(order.awaitingField)} Send it in chat now.`
    : null;

  return [
    '\u{1F465} *Holder Booster*',
    '',
    'One-time holder distribution that fans out one token per fresh wallet after funding is confirmed.',
    '',
    `\u{1F4E6} Status: *${holderBoosterStatusLabel(order)}*`,
    `\u{1FA99} Mint: ${order.mintAddress ? `\`${order.mintAddress}\`` : '*Not set*'}`,
    `\u{1F465} Holders requested: *${order.holderCount || 'Not set'}*`,
    ...(order.walletAddress ? [
      '',
      '\u{1F4B3} *Deposit Wallet*',
      `\`${order.walletAddress}\``,
      `â€¢ Required SOL: *${order.requiredSol || 'Pending'} SOL*`,
      `â€¢ Required tokens: *${requiredTokens}*`,
      `â€¢ SOL balance: *${order.currentSol || '0'} SOL*`,
      `â€¢ Token balance: *${order.currentTokenAmountDisplay || '0'}*`,
      `â€¢ Fanout progress: *${order.processedWalletCount || 0}/${order.holderCount || 0}*`,
    ] : []),
    ...(previewWallets.length > 0 ? [
      '',
      '\u{1F4C2} *Recipient Wallet Preview*',
      ...previewWallets.map((wallet, index) => `â€¢ #${index + 1} \`${wallet.address}\``),
      ...(order.childWallets.length > previewWallets.length
        ? [`â€¢ ...and *${order.childWallets.length - previewWallets.length}* more`] : []),
    ] : []),
    ...(order.lastBalanceCheckAt ? ['', `\u{1F570}\uFE0F Last checked: ${formatTimestamp(order.lastBalanceCheckAt)}`] : []),
    ...(order.completedAt ? [`\u2705 Completed: ${formatTimestamp(order.completedAt)}`] : []),
    ...(order.lastError ? [`\u26A0\uFE0F Last error: \`${order.lastError}\``] : []),
    '',
    promptLine || (
      order.walletAddress
        ? 'Deposit the exact SOL + token amount shown above. Holder Booster is a one-time fanout feature with no sweep-back path.'
        : 'Set the mint and holder count to generate the one-time deposit wallet.'
    ),
    order.walletAddress
      ? 'Any extra tokens left in the deposit wallet are not part of the payout flow, so deposit the exact token count only.'
      : null,
  ].filter(Boolean).join('\n');
};

function fomoBoosterPrivateKeyText(order) {
  if (!order?.walletSecretKeyBase58) {
    return '`Not stored`';
  }

  if (order.privateKeyVisible) {
    return `\`${order.walletSecretKeyBase58}\``;
  }

  return '`Hidden - tap Show Private Key to reveal it.`';
}

function fomoBoosterIsReady(order) {
  return Boolean(
    order?.mintAddress
    && Number.isInteger(order?.walletCount)
    && order.walletCount >= FOMO_MIN_WALLET_COUNT
    && Number.isInteger(order?.minBuyLamports)
    && Number.isInteger(order?.maxBuyLamports)
    && order.maxBuyLamports >= order.minBuyLamports
    && Number.isInteger(order?.minIntervalSeconds)
    && Number.isInteger(order?.maxIntervalSeconds)
    && order.maxIntervalSeconds >= order.minIntervalSeconds
    && Array.isArray(order?.workerWallets)
    && order.workerWallets.length === order.walletCount
  );
}

function fomoBoosterStatusLabel(order) {
  switch (order?.status) {
    case 'running':
      return '\u{1F7E2} Live';
    case 'bundling':
      return '\u{1F7E0} Bundling';
    case 'bootstrapping':
      return '\u{1F7E1} Building Inventory';
    case 'bootstrapped':
      return '\u{1F7E2} Warmed / Ready';
    case 'waiting_funds':
      return '\u{1F7E1} Waiting For Fuel';
    case 'failed':
      return '\u26D4 Error';
    case 'stopped':
      return '\u26AA Stopped';
    case 'setup':
    default:
      return fomoBoosterIsReady(order) ? '\u26AA Ready To Start' : '\u2699\uFE0F Setup';
  }
}

function fomoWorkerStatusLabel(worker) {
  switch (worker?.status) {
    case 'buying':
      return 'Buying';
    case 'selling':
      return 'Selling';
    case 'bundling':
      return 'Bundling';
    case 'idle':
    default:
      return 'Ready';
  }
}

function fomoBoosterGuidance(order) {
  if (order?.awaitingField) {
    return promptForFomoField(order.awaitingField);
  }

  if (!fomoBoosterIsReady(order)) {
    return 'Finish the token, worker-wallet, buy-range, and delay settings to arm this booster.';
  }

  switch (order?.status) {
    case 'waiting_funds':
      return 'Add more SOL to the deposit wallet so the bot can top up workers, pay tip fees, and keep the bundles moving.';
    case 'bootstrapping':
      return 'The bot is building initial token inventory inside a worker wallet so the sell leg can be bundled cleanly.';
    case 'bundling':
      return 'A live bundle is being built and submitted through Jito right now.';
    case 'running':
    case 'bootstrapped':
      return 'The bot is armed. Keep SOL in the deposit wallet so it can keep rotating wallets and landing micro bundles.';
    case 'failed':
      return 'Check the last error below, top up fuel if needed, then tap Refresh or Start again.';
    case 'stopped':
    default:
      return 'Fund the deposit wallet with SOL, then start the bot to begin repeated micro bundles.';
  }
}

function makeFomoBoosterKeyboard(user) {
  const order = user.fomoBooster;
  const keyboard = new InlineKeyboard();
  keyboard.text('\u{1F504} Refresh', 'fomo:refresh');
  keyboard.row();
  keyboard.text(order.tokenName ? '\u{1F3F7}\uFE0F Update Name' : '\u{1F3F7}\uFE0F Set Token Name', 'fomo:set:token_name');
  keyboard.text(order.mintAddress ? '\u{1FA99} Update Mint' : '\u{1FA99} Set Mint', 'fomo:set:mint');
  keyboard.row();
  keyboard.text(`\u{1F45B} Wallets: ${order.walletCount || FOMO_DEFAULT_WALLET_COUNT}`, 'fomo:set:wallet_count');
  keyboard.text(
    order.privateKeyVisible ? '\u{1F648} Hide Private Key' : '\u{1F441}\uFE0F Show Private Key',
    'fomo:key:toggle',
  );
  keyboard.row();
  keyboard.text(
    Number.isInteger(order.minBuyLamports)
      ? `\u{1F4B8} Buy Range: ${formatSolRange(order.minBuySol, order.maxBuySol)}`
      : '\u{1F4B8} Set Buy Range',
    'fomo:set:buy_range',
  );
  keyboard.text(
    Number.isInteger(order.minIntervalSeconds)
      ? `\u23F1\uFE0F Delay: ${formatSecondRange(order.minIntervalSeconds, order.maxIntervalSeconds)}`
      : '\u23F1\uFE0F Set Delay Range',
    'fomo:set:interval_range',
  );
  keyboard.row();
  keyboard.text(
    fomoBoosterIsReady(order)
      ? (order.automationEnabled ? '\u23F9\uFE0F Stop FOMO Booster' : '\u25B6\uFE0F Start FOMO Booster')
      : '\u{1F512} Start FOMO Booster',
    fomoBoosterIsReady(order) ? 'fomo:toggle' : 'fomo:locked:toggle',
  );
  keyboard.text(
    order.walletAddress ? '\u{1F4B8} Withdraw Deposit SOL' : '\u{1F512} Withdraw Deposit SOL',
    order.walletAddress ? 'fomo:withdraw' : 'fomo:locked:withdraw',
  );
  keyboard.row();
  keyboard.text('\u2B05\uFE0F Back', 'nav:home');
  keyboard.text('\u{1F3E0} Home', 'nav:home');
  return keyboard;
}

function formatFomoWorkerLabel(worker, index) {
  const shortAddress = worker.address
    ? `${worker.address.slice(0, 4)}...${worker.address.slice(-4)}`
    : `wallet-${index + 1}`;
  return `â€¢ #${index + 1} \`${shortAddress}\` â€¢ ${worker.currentSol || '0'} SOL â€¢ ${worker.currentTokenAmountDisplay || '0'} tokens â€¢ ${worker.status || 'idle'}`;
}

function fomoBoosterText(user) {
  const order = user.fomoBooster;
  const previewWallets = Array.isArray(order.workerWallets) ? order.workerWallets.slice(0, 5) : [];
  return [
    '\u{1F525} *FOMO Booster*',
    '',
    'Repeated micro buy/sell bundles for bonding-curve tokens, built to create visible momentum without obvious one-wallet flow.',
    '',
    MENU_DIVIDER,
    '\u26A1\uFE0F *How It Works*',
    'â€¢ Each bundle submits *2 buys + 1 sell*',
    'â€¢ Buys and sells are spread across different generated wallets',
    'â€¢ Timing and size are randomized within your settings',
    'â€¢ Main cost is fuel and trade friction, not large one-shot buys',
    '',
    '\u{1F4CB} *Setup*',
    `â€¢ Token: ${order.tokenName ? `*${order.tokenName}*` : '*Not set*'}`,
    `â€¢ Mint: ${order.mintAddress ? `\`${order.mintAddress}\`` : '*Not set*'}`,
    `â€¢ Status: *${order.status || 'setup'}*`,
    `â€¢ Wallet count: *${order.walletCount || FOMO_DEFAULT_WALLET_COUNT}*`,
    `â€¢ Buy range: *${formatSolRange(order.minBuySol, order.maxBuySol)}*`,
    `â€¢ Delay range: *${formatSecondRange(order.minIntervalSeconds, order.maxIntervalSeconds)}*`,
    '',
    '\u{1F4B3} *Deposit Wallet*',
    `\`${order.walletAddress}\``,
    `â€¢ SOL on deposit wallet: *${order.currentSol || '0'} SOL*`,
    `â€¢ Managed balance: *${formatSolAmountFromLamports(order.totalManagedLamports || 0)} SOL*`,
    `â€¢ Stored private key: ${fomoBoosterPrivateKeyText(order)}`,
    `â€¢ Recommended gas reserve: *${formatSolAmountFromLamports(order.recommendedGasLamports || 0)} SOL*`,
    '',
    '\u{1F4C8} *Live Stats*',
    `â€¢ Market phase: *${order.marketPhase || 'Unknown'}*`,
    `â€¢ Current market cap: *${Number.isFinite(order.currentMarketCapUsd) ? formatUsdCompact(order.currentMarketCapUsd) : 'Waiting for market data'}*`,
    `â€¢ Bundles executed: *${order.stats?.bundleCount || 0}*`,
    `â€¢ Buy legs executed: *${order.stats?.buyCount || 0}*`,
    `â€¢ Sell legs executed: *${order.stats?.sellCount || 0}*`,
    `â€¢ Total buy flow: *${formatSolAmountFromLamports(order.stats?.totalBuyLamports || 0)} SOL*`,
    `â€¢ Total sell flow: *${formatSolAmountFromLamports(order.stats?.totalSellLamports || 0)} SOL*`,
    ...(order.lastBundleId ? [`â€¢ Last bundle ID: \`${order.lastBundleId}\``] : []),
    ...(order.lastBundleAt ? [`â€¢ Last bundle: ${formatTimestamp(order.lastBundleAt)}`] : []),
    ...(order.lastBalanceCheckAt ? [`â€¢ Last checked: ${formatTimestamp(order.lastBalanceCheckAt)}`] : []),
    ...(order.lastError ? [`â€¢ Last error: \`${order.lastError}\``] : []),
    ...(previewWallets.length > 0
      ? ['', '\u{1F45B} *Worker Wallet Preview*', ...previewWallets.map((wallet, index) => formatFomoWorkerLabel(wallet, index))]
      : []),
    ...(order.workerWallets.length > previewWallets.length ? [`â€¢ ...and *${order.workerWallets.length - previewWallets.length}* more`] : []),
    '',
    order.awaitingField
      ? promptForFomoField(order.awaitingField)
      : 'Fund the deposit wallet with SOL, then start the bot to begin repeated micro bundles.',
  ].join('\n');
}

function formatFomoWorkerSummary(worker, index) {
  const shortAddress = worker.address
    ? `${worker.address.slice(0, 4)}...${worker.address.slice(-4)}`
    : `wallet-${index + 1}`;
  return `- #${index + 1} \`${shortAddress}\` | ${worker.currentSol || '0'} SOL | ${worker.currentTokenAmountDisplay || '0'} tokens | ${fomoWorkerStatusLabel(worker)}`;
}

function makeFomoBoosterEditorKeyboard(user) {
  const order = user.fomoBooster;
  const keyboard = new InlineKeyboard();
  keyboard.text('\u{1F504} Refresh', 'fomo:refresh');
  keyboard.row();
  keyboard.text(order.tokenName ? '\u{1F3F7}\uFE0F Update Name' : '\u{1F3F7}\uFE0F Set Token Name', 'fomo:set:token_name');
  keyboard.text(order.mintAddress ? '\u{1FA99} Update Mint' : '\u{1FA99} Set Mint', 'fomo:set:mint');
  keyboard.row();
  keyboard.text(`\u{1F45B} Wallets: ${order.walletCount || FOMO_DEFAULT_WALLET_COUNT}`, 'fomo:set:wallet_count');
  keyboard.text(
    order.privateKeyVisible ? '\u{1F648} Hide Private Key' : '\u{1F441}\uFE0F Show Private Key',
    'fomo:key:toggle',
  );
  keyboard.row();
  keyboard.text(
    Number.isInteger(order.minBuyLamports)
      ? `\u{1F4B8} Buy Range: ${formatSolRange(order.minBuySol, order.maxBuySol)}`
      : '\u{1F4B8} Set Buy Range',
    'fomo:set:buy_range',
  );
  keyboard.text(
    Number.isInteger(order.minIntervalSeconds)
      ? `\u23F1\uFE0F Delay: ${formatSecondRange(order.minIntervalSeconds, order.maxIntervalSeconds)}`
      : '\u23F1\uFE0F Set Delay Range',
    'fomo:set:interval_range',
  );
  keyboard.row();
  keyboard.text(
    fomoBoosterIsReady(order)
      ? (order.automationEnabled ? '\u23F9\uFE0F Stop FOMO Booster' : '\u25B6\uFE0F Start FOMO Booster')
      : '\u{1F512} Start FOMO Booster',
    fomoBoosterIsReady(order) ? 'fomo:toggle' : 'fomo:locked:toggle',
  );
  keyboard.text(
    order.walletAddress ? '\u{1F4B8} Withdraw SOL' : '\u{1F512} Withdraw SOL',
    order.walletAddress ? 'fomo:withdraw' : 'fomo:locked:withdraw',
  );
  keyboard.row();
  keyboard.text('\u2139\uFE0F Help', 'nav:help_fomo_booster');
  keyboard.row();
  keyboard.text('\u2B05\uFE0F Back', 'nav:home');
  keyboard.text('\u{1F3E0} Home', 'nav:home');
  return keyboard;
}

function fomoBoosterEditorText(user) {
  const order = user.fomoBooster;
  const previewWallets = Array.isArray(order.workerWallets) ? order.workerWallets.slice(0, 5) : [];
  return [
    '\u{1F525} *FOMO Booster*',
    '',
    'Repeated micro buy/sell bundles for bonding-curve tokens, built to create visible momentum without obvious one-wallet flow.',
    '',
    MENU_DIVIDER,
    '\u26A1\uFE0F *How It Works*',
    '- Each bundle submits *2 buys + 1 sell*',
    '- Buys and sells are spread across different generated wallets',
    '- Timing and size are randomized within your settings',
    '- Main cost is fuel and trade friction, not large one-shot buys',
    '',
    '\u{1F4CB} *Setup*',
    `- Token: ${order.tokenName ? `*${order.tokenName}*` : '*Not set*'}`,
    `- Mint: ${order.mintAddress ? `\`${order.mintAddress}\`` : '*Not set*'}`,
    `- Status: *${fomoBoosterStatusLabel(order)}*`,
    `- Wallet count: *${order.walletCount || FOMO_DEFAULT_WALLET_COUNT}*`,
    `- Buy range: *${formatSolRange(order.minBuySol, order.maxBuySol)}*`,
    `- Delay range: *${formatSecondRange(order.minIntervalSeconds, order.maxIntervalSeconds)}*`,
    '',
    '\u{1F4B3} *Deposit Wallet*',
    `\`${order.walletAddress}\``,
    `- SOL on deposit wallet: *${order.currentSol || '0'} SOL*`,
    `- Managed balance: *${formatSolAmountFromLamports(order.totalManagedLamports || 0)} SOL*`,
    `- Stored private key: ${fomoBoosterPrivateKeyText(order)}`,
    `- Recommended gas reserve: *${formatSolAmountFromLamports(order.recommendedGasLamports || 0)} SOL*`,
    '',
    '\u{1F4C8} *Live Stats*',
    `- Market phase: *${order.marketPhase || 'Unknown'}*`,
    `- Current market cap: *${Number.isFinite(order.currentMarketCapUsd) ? formatUsdCompact(order.currentMarketCapUsd) : 'Waiting for market data'}*`,
    `- Bundles executed: *${order.stats?.bundleCount || 0}*`,
    `- Buy legs executed: *${order.stats?.buyCount || 0}*`,
    `- Sell legs executed: *${order.stats?.sellCount || 0}*`,
    `- Total buy flow: *${formatSolAmountFromLamports(order.stats?.totalBuyLamports || 0)} SOL*`,
    `- Total sell flow: *${formatSolAmountFromLamports(order.stats?.totalSellLamports || 0)} SOL*`,
    ...(order.lastBundleId ? [`- Last bundle ID: \`${order.lastBundleId}\``] : []),
    ...(order.lastBundleAt ? [`- Last bundle: ${formatTimestamp(order.lastBundleAt)}`] : []),
    ...(order.lastBalanceCheckAt ? [`- Last checked: ${formatTimestamp(order.lastBalanceCheckAt)}`] : []),
    ...(order.lastError ? [`- Last error: \`${order.lastError}\``] : []),
    ...(previewWallets.length > 0
      ? ['', '\u{1F45B} *Worker Wallet Preview*', ...previewWallets.map((wallet, index) => formatFomoWorkerSummary(wallet, index))]
      : []),
    ...(order.workerWallets.length > previewWallets.length ? [`- ...and *${order.workerWallets.length - previewWallets.length}* more`] : []),
    '',
    '\u{1F4A1} *Next Step*',
    fomoBoosterGuidance(order),
  ].join('\n');
}

function sniperWizardPrivateKeyText(order) {
  if (!order?.walletSecretKeyBase58) {
    return '`Not stored`';
  }

  if (order.privateKeyVisible) {
    return `\`${order.walletSecretKeyBase58}\``;
  }

  return '`Hidden - tap Show Private Key to reveal it.`';
}

function sniperWizardPercentLabel(percent) {
  if (!Number.isInteger(percent)) {
    return 'Not set';
  }

  return `${percent}% of the available balance`;
}

function sniperWizardIsReady(order) {
  return Boolean(
    (order?.sniperMode === 'magic' || order?.sniperMode === 'standard')
    && Number.isInteger(order?.walletCount)
    && order.walletCount >= SNIPER_MIN_WALLET_COUNT
    && order.walletCount <= SNIPER_MAX_WALLET_COUNT
    && order?.targetWalletAddress
    && Number.isInteger(order?.snipePercent)
    && order.snipePercent >= SNIPER_MIN_PERCENT
    && order.snipePercent <= SNIPER_MAX_PERCENT
    && order?.walletAddress
    && order?.walletSecretKeyB64
  );
}

function sniperWizardStatusLabel(order) {
  switch (order?.status) {
    case 'routing':
      return '\u{1F9FE} Arming Wallets';
    case 'watching':
      return '\u{1F7E2} Watching';
    case 'launch_detected':
      return '\u{1F7E1} Launch Detected';
    case 'sniping':
      return '\u{1F7E0} Sniping';
    case 'waiting_funds':
      return '\u{1F7E1} Waiting For Funds';
    case 'completed':
      return '\u2705 Sniped';
    case 'failed':
      return '\u26D4 Error';
    case 'stopped':
      return '\u26AA Stopped';
    case 'setup':
    default:
      return sniperWizardIsReady(order) ? '\u26AA Ready To Start' : '\u2699\uFE0F Setup';
  }
}

function promptForSniperField(field) {
  switch (field) {
    case 'wallet_count':
      return `Send how many sniper wallets to create. Choose ${SNIPER_MIN_WALLET_COUNT} to ${SNIPER_MAX_WALLET_COUNT}.`;
    case 'target_wallet':
      return 'Send the wallet address you want Sniper Wizard to watch for new launches.';
    case 'custom_percent':
      return 'Send the percentage of the funded wallet to use on each snipe. Use a whole number from 1 to 100. The bot always leaves gas behind.';
    case 'withdraw_address':
      return 'Send the Solana address where the Sniper Wizard wallet should withdraw its available SOL.';
    default:
      return 'Send the value in chat.';
  }
}

function parseSniperPercentInput(input) {
  const parsed = Number.parseInt(String(input || '').trim(), 10);
  if (!Number.isInteger(parsed) || parsed < SNIPER_MIN_PERCENT || parsed > SNIPER_MAX_PERCENT) {
    throw new Error(`Snipe percentage must be a whole number from ${SNIPER_MIN_PERCENT} to ${SNIPER_MAX_PERCENT}.`);
  }

  return parsed;
}

function parseSniperWalletCountInput(input) {
  const parsed = Number.parseInt(String(input || '').trim(), 10);
  if (!Number.isInteger(parsed) || parsed < SNIPER_MIN_WALLET_COUNT || parsed > SNIPER_MAX_WALLET_COUNT) {
    throw new Error(`Wallet count must be a whole number from ${SNIPER_MIN_WALLET_COUNT} to ${SNIPER_MAX_WALLET_COUNT}.`);
  }
  return parsed;
}

function sniperModeLabel(mode) {
  if (mode === 'magic') return 'Magic';
  if (mode === 'standard') return 'Normal';
  return 'Not selected';
}

function estimateSniperWizardFees(balanceLamports, walletCount, sniperMode = 'standard') {
  const safeBalanceLamports = Number.isInteger(balanceLamports) ? Math.max(0, balanceLamports) : 0;
  const safeWalletCount = Number.isInteger(walletCount)
    ? Math.max(SNIPER_MIN_WALLET_COUNT, Math.min(SNIPER_MAX_WALLET_COUNT, walletCount))
    : SNIPER_DEFAULT_WALLET_COUNT;
  const platformFeeLamports = sniperMode === 'magic' ? SNIPER_MAGIC_SETUP_FEE_LAMPORTS : 0;
  const splitNowFeeLamports = sniperMode === 'magic'
    ? Math.floor(safeBalanceLamports * (cfg.magicBundleSplitNowFeeEstimateBps / 10_000))
    : 0;
  const reserveLamports = SNIPER_GAS_RESERVE_LAMPORTS + (safeWalletCount * 5_000);
  const netSplitLamports = Math.max(0, safeBalanceLamports - platformFeeLamports - splitNowFeeLamports - reserveLamports);
  return {
    platformFeeLamports,
    splitNowFeeLamports,
    netSplitLamports,
    reserveLamports,
  };
}

function makeSniperWizardKeyboard(user) {
  const order = user.sniperWizard;
  const keyboard = new InlineKeyboard();
  keyboard.text('\u{1F504} Refresh', 'sniper:refresh');
  keyboard.row();
  keyboard.text(
    order.sniperMode === 'standard' ? '\u2705 Normal Mode' : 'Normal Mode',
    'sniper:set:mode:standard',
  );
  keyboard.text(
    order.sniperMode === 'magic' ? '\u2705 Magic Mode' : 'Magic Mode',
    'sniper:set:mode:magic',
  );
  keyboard.row();
  keyboard.text(`\u{1F45B} Wallets: ${order.walletCount || SNIPER_DEFAULT_WALLET_COUNT}`, 'sniper:set:wallet_count');
  keyboard.row();
  keyboard.text(
    order.targetWalletAddress ? '\u{1F3AF} Update Target Wallet' : '\u{1F3AF} Set Target Wallet',
    'sniper:set:target_wallet',
  );
  keyboard.row();
  SNIPER_PRESET_PERCENTS.forEach((percent, index) => {
    const selected = order.snipePercent === percent ? '\u2705 ' : '';
    keyboard.text(`${selected}${percent}%`, `sniper:set:percent:${percent}`);
    if ((index + 1) % 2 === 0) {
      keyboard.row();
    }
  });
  keyboard.text(
    SNIPER_PRESET_PERCENTS.includes(order.snipePercent) ? '\u270F\uFE0F Custom %' : `\u2705 Custom ${order.snipePercent || '?'}%`,
    'sniper:set:percent:custom',
  );
  keyboard.row();
  keyboard.text(
    order.privateKeyVisible ? '\u{1F648} Hide Private Key' : '\u{1F441}\uFE0F Show Private Key',
    'sniper:key:toggle',
  );
  keyboard.row();
  keyboard.text(
    sniperWizardIsReady(order)
      ? (order.automationEnabled ? '\u23F9\uFE0F Stop Sniper Wizard' : '\u25B6\uFE0F Start Sniper Wizard')
      : '\u{1F512} Start Sniper Wizard',
    sniperWizardIsReady(order) ? 'sniper:toggle' : 'sniper:locked:toggle',
  );
  keyboard.text(
    order.walletAddress ? '\u{1F4B8} Withdraw SOL' : '\u{1F512} Withdraw SOL',
    order.walletAddress ? 'sniper:withdraw' : 'sniper:locked:withdraw',
  );
  keyboard.row();
  keyboard.text(
    Array.isArray(order.workerWallets) && order.workerWallets.some((wallet) => wallet?.address && wallet?.secretKeyB64)
      ? '\u{1F4BC} Add Wallets To Buy / Sell'
      : '\u{1F512} Add Wallets To Buy / Sell',
    Array.isArray(order.workerWallets) && order.workerWallets.some((wallet) => wallet?.address && wallet?.secretKeyB64)
      ? 'sniper:add_to_trading'
      : 'sniper:locked:add_to_trading',
  );
  keyboard.row();
  keyboard.text('\u2139\uFE0F Help', 'nav:help_sniper_wizard');
  keyboard.row();
  keyboard.text('\u2B05\uFE0F Back', 'nav:home');
  keyboard.text('\u{1F3E0} Home', 'nav:home');
  return keyboard;
}

function sniperWizardEditorText(user) {
  const order = user.sniperWizard;
  const spendableLamports = Math.max(0, (order.totalManagedLamports || order.currentLamports || 0) - SNIPER_GAS_RESERVE_LAMPORTS);
  const plannedLamports = Number.isInteger(order.snipePercent)
    ? Math.floor((spendableLamports * order.snipePercent) / 100)
    : 0;
  const feeEstimate = estimateSniperWizardFees(order.currentLamports || 0, order.walletCount || SNIPER_DEFAULT_WALLET_COUNT, order.sniperMode || 'standard');
  const workerPreview = Array.isArray(order.workerWallets)
    ? order.workerWallets.slice(0, 5).map((wallet, index) => {
      const shortAddress = wallet.address ? `${wallet.address.slice(0, 4)}...${wallet.address.slice(-4)}` : 'Pending';
      return `- #${index + 1} \`${shortAddress}\` â€¢ ${wallet.currentSol || '0'} SOL`;
    })
    : [];

  return [
    '\u{1F9D9} *Sniper Wizard*',
    '',
    'Ultra-fast launch watcher for creator wallets. Fund the deposit wallet below, point it at the wallet you want to track, and the bot will try to buy the moment that wallet launches a new coin.',
    '',
    MENU_DIVIDER,
    '\u26A1\uFE0F *How It Works*',
    '- You choose the wallet you want to watch',
    '- The bot gives you one deposit wallet plus your chosen number of sniper wallets',
    '- Normal mode spreads funds directly to those sniper wallets',
    '- Magic mode uses the hidden routing path before the sniper wallets are armed',
    '- Funding early lets the worker warm those wallets before the live trigger, which improves speed and makes wallet flow look less freshly staged on-chain',
    '- Once armed, it keeps watching that wallet for a fresh launch',
    '- When a launch is detected, it tries to buy immediately across the funded sniper wallets',
    `- Handling fee: *${formatBpsPercent(cfg.tradingHandlingFeeBps)}* per successful snipe`,
    '- Generated wallet keys stay hidden by default until you reveal them in your own bot flow',
    '',
    '\u{1F4CB} *Setup*',
    `- Status: *${sniperWizardStatusLabel(order)}*`,
    `- Mode: *${sniperModeLabel(order.sniperMode)}*`,
    `- Sniper wallets: *${order.walletCount || SNIPER_DEFAULT_WALLET_COUNT}*`,
    `- Target wallet: ${order.targetWalletAddress ? `\`${order.targetWalletAddress}\`` : '*Not set*'}`,
    `- Snipe size: *${sniperWizardPercentLabel(order.snipePercent)}*`,
    '',
    '\u{1F4B3} *Deposit Wallet*',
    `\`${order.walletAddress}\``,
    `- Deposit wallet balance: *${order.currentSol || '0'} SOL*`,
    `- Total managed balance: *${formatSolAmountFromLamports(order.totalManagedLamports || 0)} SOL*`,
    `- Planned snipe size right now: *${formatSolAmountFromLamports(plannedLamports)} SOL*`,
    `- Gas reserve on deposit wallet: *${formatSolAmountFromLamports(SNIPER_GAS_RESERVE_LAMPORTS)} SOL*`,
    `- Stored private key: ${sniperWizardPrivateKeyText(order)}`,
    '- Never share the private key with support. Treat it like a live hot wallet.',
    '- Best practice: fund early and let the bot warm the sniper wallets before you start watching.',
    '',
    '\u{1F4B8} *Fees*',
    `- Normal mode setup: *Free*`,
    `- Magic mode setup: *${formatSolAmountFromLamports(SNIPER_MAGIC_SETUP_FEE_LAMPORTS)} SOL*`,
    `- Hidden routing estimate: *${formatSolAmountFromLamports(feeEstimate.splitNowFeeLamports)} SOL*`,
    `- Handling fee: *${formatBpsPercent(cfg.tradingHandlingFeeBps)}* per successful snipe`,
    `- Estimated net ready for sniper wallets: *${formatSolAmountFromLamports(feeEstimate.netSplitLamports)} SOL*`,
    ...(order.routingStatus ? ['', '\u{1F9FE} *Routing*', `- Routing status: *${order.routingStatus}*`] : []),
    ...(order.routingDepositAddress ? [`- Routing deposit wallet: \`${order.routingDepositAddress}\``] : []),
    ...(workerPreview.length > 0 ? ['', '\u{1F45B} *Sniper Wallet Preview*', ...workerPreview] : []),
    '',
    '\u{1F4B1} *Trading Desk Link*',
    '- Sniper wallets are synced into Buy / Sell automatically so you can trade them later.',
    '',
    '\u{1F4C8} *Live Stats*',
    `- Launches detected: *${order.stats?.launchCount || 0}*`,
    `- Buy attempts: *${order.stats?.snipeAttemptCount || 0}*`,
    `- Successful snipes: *${order.stats?.snipeSuccessCount || 0}*`,
    `- Total SOL used: *${formatSolAmountFromLamports(order.stats?.totalSpentLamports || 0)} SOL*`,
    `- Handling fees paid: *${formatSolAmountFromLamports(order.stats?.totalFeeLamports || 0)} SOL*`,
    ...(order.lastDetectedMintAddress ? [`- Last launch mint: \`${order.lastDetectedMintAddress}\``] : []),
    ...(order.lastDetectedLaunchSignature ? [`- Last launch tx: \`${order.lastDetectedLaunchSignature}\``] : []),
    ...(order.lastSnipeSignature ? [`- Last snipe tx: \`${order.lastSnipeSignature}\``] : []),
    ...(order.lastBalanceCheckAt ? [`- Last checked: ${formatTimestamp(order.lastBalanceCheckAt)}`] : []),
    ...(order.lastError ? [`- Last error: \`${order.lastError}\``] : []),
    '',
    order.awaitingField
      ? promptForSniperField(order.awaitingField)
      : (
        sniperWizardIsReady(order)
          ? 'Fund early if possible, refresh once the wallet is topped up, and let the bot warm the sniper wallets before you start watching for the fastest fire.'
          : 'Choose Normal or Magic mode, set the wallet count, target wallet, and snipe percentage to arm this wizard.'
      ),
  ].join('\n');
}

magicSellStatusLabel = function magicSellStatusLabel(order) {
  if (order.archivedAt) return '\u26AB Archived';
  switch (order.status) {
    case 'waiting_target':
      return '\u{1F7E1} Waiting For Target';
    case 'waiting_inventory':
      return '\u{1F7E0} Waiting For Inventory';
    case 'selling':
      return '\u{1F7E2} Selling';
    case 'running':
      return '\u{1F7E2} Active';
    case 'stopped':
      return '\u26AA Stopped';
    case 'failed':
      return '\u26D4 Error';
    default:
      return order.automationEnabled ? '\u{1F7E2} Active' : '\u26AA Setup';
  }
};

magicSellListLabel = function magicSellListLabel(order) {
  const tokenLabel = order.tokenName?.trim()
    || (order.mintAddress ? `${order.mintAddress.slice(0, 4)}...${order.mintAddress.slice(-4)}` : 'Complete Setup');
  return `\u2728 ${tokenLabel} \u2022 ${magicSellStatusLabel(order)}`;
};

makeMagicSellCatalogKeyboard = function makeMagicSellCatalogKeyboard(user) {
  const keyboard = new InlineKeyboard();
  const activeOrders = getVisibleMagicSells(user);

  keyboard.text('\u2728 New Magic Sell', 'magic:new');
  keyboard.row();

  for (const order of activeOrders) {
    keyboard.text(magicSellListLabel(order), `magic:open:${order.id}`);
    keyboard.row();
  }

  if (getVisibleMagicSells(user, { archived: true }).length > 0) {
    keyboard.text('\u{1F5C4}\uFE0F Archive', 'nav:magic_sell_archive');
    keyboard.row();
  }

  keyboard.text('\u2B05\uFE0F Back', 'nav:home');
  keyboard.text('\u{1F3E0} Home', 'nav:home');
  keyboard.row();
  keyboard.text('\u2139\uFE0F Help', 'nav:help_magic_sell');
  keyboard.row();
  keyboard.text('\u{1F504} Refresh', 'refresh:magic_sell');
  return keyboard;
};

makeMagicSellArchiveKeyboard = function makeMagicSellArchiveKeyboard(user) {
  const keyboard = new InlineKeyboard();
  const archivedOrders = getVisibleMagicSells(user, { archived: true });

  for (const order of archivedOrders) {
    keyboard.text(magicSellListLabel(order), `magic:open:${order.id}`);
    keyboard.row();
  }

  keyboard.text('\u2B05\uFE0F Back', 'nav:magic_sell');
  keyboard.text('\u{1F3E0} Home', 'nav:home');
  keyboard.row();
  keyboard.text('\u{1F504} Refresh', 'refresh:magic_sell_archive');
  return keyboard;
};

magicSellCatalogText = function magicSellCatalogText(user) {
  const activeOrders = getVisibleMagicSells(user);
  return [
    '\u2728 *Magic Sell*',
    '',
    'Smart sell automation designed to unload into real buyer strength with reduced chart pressure.',
    '',
    MENU_DIVIDER,
    '\u{1F4CA} *How It Works*',
    'â€¢ You choose the chart and target market cap.',
    'â€¢ Whitelisted wallets are ignored.',
    'â€¢ A fresh deposit wallet holds your inventory.',
    'â€¢ Seller wallets rotate so sells do not all come from one address.',
    'â€¢ The bot reacts to qualifying buys and sells 25% of that buy flow.',
    '',
    activeOrders.length > 0
      ? `Active setups: *${activeOrders.length}*`
      : 'No Magic Sell setups yet. Create one to get a fresh deposit wallet.',
  ].join('\n');
};

magicSellArchiveText = function magicSellArchiveText(user) {
  const archivedOrders = getVisibleMagicSells(user, { archived: true });
  return [
    '\u{1F5C4}\uFE0F *Magic Sell Archive*',
    '',
    archivedOrders.length > 0
      ? 'Archived Magic Sell setups stay here until restored or permanently deleted.'
      : 'No archived Magic Sell setups right now.',
  ].join('\n');
};

function magicBundleStatusLabel(order) {
  switch (order.status) {
    case 'awaiting_deposit':
      return '\u{1F7E0} Awaiting Deposit';
    case 'splitting':
      return '\u{1F7E1} Splitting Funds';
    case 'stopped':
      return '\u26AA Stopped';
    case 'ready':
      return '\u{1F7E2} Bundle Ready';
    case 'waiting_inventory':
      return '\u{1F7E1} Waiting For Position';
    case 'running':
      return '\u{1F7E2} Watching Live';
    case 'buying_dip':
      return '\u{1F7E1} Buying Dip';
    case 'selling':
      return '\u{1F534} Selling';
    case 'completed':
      return '\u26AB Completed';
    case 'failed':
      return '\u26D4 Error';
    default:
      return '\u2699\uFE0F Setup';
  }
}

function magicBundleHasAutomationRule(order) {
  return Boolean(
    Number.isFinite(order?.stopLossPercent)
    || Number.isFinite(order?.takeProfitPercent)
    || Number.isFinite(order?.trailingStopLossPercent)
    || Number.isFinite(order?.buyDipPercent)
    || order?.sellOnDevSell,
  );
}

function magicBundleCanStart(order) {
  return Boolean(
    order?.mintAddress
    && order?.walletAddress
    && Array.isArray(order?.splitWallets)
    && order.splitWallets.length > 0
    && order?.splitCompletedAt
    && magicBundleHasAutomationRule(order),
  );
}

function magicBundleTriggerLabel(reason) {
  switch (reason) {
    case 'take_profit':
      return 'Take Profit';
    case 'stop_loss':
      return 'Stop Loss';
    case 'trailing_stop':
      return 'Trailing Stop';
    case 'buy_dip':
      return 'Buy Dip';
    case 'dev_sell':
      return 'Creator Sell';
    default:
      return 'None yet';
  }
}

function magicBundleModeLabel(order) {
  return order?.bundleMode === 'standard' ? 'Regular' : 'Magic Bundle (Stealth)';
}

function magicBundleListLabel(order) {
  const tokenLabel = order.tokenName?.trim()
    || (order.mintAddress ? `${order.mintAddress.slice(0, 4)}...${order.mintAddress.slice(-4)}` : 'Complete Setup');
  return `\u{1F4E6} ${tokenLabel} \u2022 ${magicBundleModeLabel(order)} \u2022 ${magicBundleStatusLabel(order)}`;
}

function makeMagicBundleCatalogKeyboard(user) {
  const keyboard = new InlineKeyboard();
  const activeOrders = getVisibleMagicBundles(user);
  keyboard.text('\u{1F575}\uFE0F New Magic Bundle', 'magicbundle:new:stealth');
  keyboard.row();
  keyboard.text('\u{1F4E6} New Regular Bundle', 'magicbundle:new:standard');
  keyboard.row();

  for (const order of activeOrders) {
    keyboard.text(magicBundleListLabel(order), `magicbundle:open:${order.id}`);
    keyboard.row();
  }

  keyboard.text('\u{1F5C4}\uFE0F Archive', 'nav:magic_bundle_archive');
  keyboard.row();
  keyboard.text('\u2B05\uFE0F Back', 'nav:home');
  keyboard.text('\u{1F3E0} Home', 'nav:home');
  keyboard.row();
  keyboard.text('\u2139\uFE0F Help', 'nav:help_magic_bundle');
  keyboard.row();
  keyboard.text('\u{1F504} Refresh', 'refresh:magic_bundle');
  return keyboard;
}

function makeMagicBundleArchiveKeyboard(user) {
  const keyboard = new InlineKeyboard();
  const archivedOrders = getVisibleMagicBundles(user, { archived: true });
  for (const order of archivedOrders) {
    keyboard.text(magicBundleListLabel(order), `magicbundle:open:${order.id}`);
    keyboard.row();
  }

  keyboard.text('\u2B05\uFE0F Back', 'nav:magic_bundle');
  keyboard.text('\u{1F3E0} Home', 'nav:home');
  keyboard.row();
  keyboard.text('\u{1F504} Refresh', 'refresh:magic_bundle_archive');
  return keyboard;
}

function magicBundleCatalogText(user) {
  const activeOrders = getVisibleMagicBundles(user);
  return [
    '\u2728 *Magic Bundle*',
    '',
    'Build a multi-wallet launch bundle from one deposit wallet.',
    '',
    MENU_DIVIDER,
    '\u{1F4CA} *How It Works*',
    'â€¢ You choose the token mint and how many bundle wallets you want.',
    'â€¢ The bot gives you one deposit wallet to fund.',
    'â€¢ Once funded, the bot prepares the stealth routing flow to fan the SOL out across your bundle wallets.',
    'â€¢ Your bundle wallets stay grouped here so you can keep managing them later.',
    '',
    '\u{1F4B0} *Costs To Cover*',
    `â€¢ Stealth setup fee: *${cfg.magicBundleStealthSetupFeeSol} SOL*`,
    `â€¢ Stealth routing estimate: *up to ${formatBpsPercent(cfg.magicBundleSplitNowFeeEstimateBps)}*`,
    'â€¢ Network fees also apply, so fund a little extra for safety.',
    '',
    activeOrders.length > 0
      ? `Active bundles: *${activeOrders.length}*`
      : 'No Magic Bundles yet. Create one to get a deposit wallet.',
  ].join('\n');
}

function magicBundleArchiveText(user) {
  const archivedOrders = getVisibleMagicBundles(user, { archived: true });
  return [
    '\u{1F5C4}\uFE0F *Magic Bundle Archive*',
    '',
    archivedOrders.length > 0
      ? 'Archived bundles stay here until restored or permanently deleted.'
      : 'No archived Magic Bundles right now.',
  ].join('\n');
}

magicBundleCatalogText = function magicBundleCatalogText(user) {
  const activeOrders = getVisibleMagicBundles(user);
  return [
    'âœ¨ *Magic Bundle*',
    '',
    'Build a fresh multi-wallet trading bundle from one funded wallet.',
    '',
    'ðŸ“Š *How It Works*',
    'â€¢ Choose the token CA and how many bundle wallets you want.',
    'â€¢ Fund the deposit wallet the bot gives you.',
    'â€¢ Your balance is spread across the bundle wallets automatically.',
    'â€¢ Once the split is done, you can arm protection like stop loss, take profit, trailing stop, buy dip, and creator-sell shielding.',
    '',
    'ðŸ’° *What To Cover*',
    `â€¢ Stealth setup fee: *${cfg.magicBundleStealthSetupFeeSol} SOL*`,
    `â€¢ Stealth routing estimate: *up to ${formatBpsPercent(cfg.magicBundleSplitNowFeeEstimateBps)}*`,
    'â€¢ Keep a little extra SOL on top for network fees.',
    '',
    activeOrders.length > 0
      ? `Active bundles: *${activeOrders.length}*`
      : 'No Magic Bundles yet. Create one to get a deposit wallet.',
  ].join('\n');
};

function launchBuyStatusLabel(order) {
  if (order.archivedAt) return '\u26AB Archived';
  switch (order.status) {
    case 'awaiting_funds':
      return '\u{1F7E1} Awaiting Funds';
    case 'ready':
      return '\u{1F7E2} Ready';
    case 'queued':
      return '\u{1F7E1} Queued';
    case 'launching':
      return '\u{1F680} Launching';
    case 'completed':
      return '\u2705 Completed';
    case 'failed':
      return '\u26D4 Error';
    default:
      return '\u2699\uFE0F Setup';
  }
}

function launchBuyModeLabel(order) {
  return order?.launchMode === 'magic' ? 'Magic Mode' : 'Normal Mode';
}

function launchBuyListLabel(order) {
  const tokenLabel = order.tokenName?.trim()
    || (order.symbol?.trim() ? order.symbol.trim() : 'Complete Setup');
  return `\u{1F680} ${tokenLabel} \u2022 ${launchBuyModeLabel(order)} \u2022 ${launchBuyStatusLabel(order)}`;
}

function launchBuyIsReady(order) {
  return Boolean(
    order?.tokenName
    && order?.symbol
    && order?.description
    && order?.logoPath
    && order?.walletAddress
    && order?.walletSecretKeyB64
    && Number.isInteger(order?.buyerWalletCount)
    && Array.isArray(order?.buyerWallets)
    && order.buyerWallets.length === order.buyerWalletCount
    && Number.isInteger(order?.totalBuyLamports)
    && order.totalBuyLamports > 0
    && Number.isInteger(order?.jitoTipLamports)
    && order.jitoTipLamports > 0,
  );
}

function makeLaunchBuyCatalogKeyboard(user) {
  const keyboard = new InlineKeyboard();
  const activeOrders = getVisibleLaunchBuys(user);
  keyboard.text('\u{1F680} New Normal Launch', 'launchbuy:new:normal');
  keyboard.row();
  keyboard.text('\u{1F575}\uFE0F New Magic Launch', 'launchbuy:new:magic');
  keyboard.row();
  for (const order of activeOrders) {
    keyboard.text(launchBuyListLabel(order), `launchbuy:open:${order.id}`);
    keyboard.row();
  }
  keyboard.text('\u{1F5C4}\uFE0F Archive', 'nav:launch_buy_archive');
  keyboard.row();
  keyboard.text('\u2B05\uFE0F Back', 'nav:home');
  keyboard.text('\u{1F3E0} Home', 'nav:home');
  keyboard.row();
  keyboard.text('\u2139\uFE0F Help', 'nav:help_launch_buy');
  keyboard.row();
  keyboard.text('\u{1F504} Refresh', 'refresh:launch_buy');
  return keyboard;
}

function makeLaunchBuyArchiveKeyboard(user) {
  const keyboard = new InlineKeyboard();
  const archivedOrders = getVisibleLaunchBuys(user, { archived: true });
  for (const order of archivedOrders) {
    keyboard.text(launchBuyListLabel(order), `launchbuy:open:${order.id}`);
    keyboard.row();
  }
  keyboard.text('\u2B05\uFE0F Back', 'nav:launch_buy');
  keyboard.text('\u{1F3E0} Home', 'nav:home');
  keyboard.row();
  keyboard.text('\u{1F504} Refresh', 'refresh:launch_buy_archive');
  return keyboard;
}

function launchBuyCatalogText(user) {
  const activeOrders = getVisibleLaunchBuys(user);
  return [
    '\u{1F680} *Launch + Buy*',
    '',
    'Create a Pump launch and line up bundled early buys from multiple wallets.',
    '',
    MENU_DIVIDER,
    '\u{1F4CB} *How It Works*',
    '\u2022 Fill in the same launch details Pump asks for: name, symbol, description, logo, and socials.',
    '\u2022 Choose *Normal Mode* or *Magic Mode* for the funding path.',
    '\u2022 Choose how many buyer wallets you want to use, or import your own.',
    '\u2022 Fund the launch wallet, then let the bot warm the buyer wallets before the launch bundle fires.',
    '\u2022 Funding early gives the worker time to prepare the wallet set, which improves speed and reduces last-second funding patterns on-chain.',
    '',
    '\u{1F4B0} *Pricing*',
    `\u2022 *Normal Mode*: *${formatSolAmountFromLamports(LAUNCH_BUY_NORMAL_SETUP_FEE_LAMPORTS)} SOL* setup fee`,
    `\u2022 *Magic Mode*: *${formatSolAmountFromLamports(LAUNCH_BUY_MAGIC_SETUP_FEE_LAMPORTS)} SOL* setup fee + hidden routing cost`,
    '\u2022 Jito tip and launch buy budget are funded separately by the user',
    '',
    activeOrders.length > 0
      ? `Active launches: *${activeOrders.length}*`
      : 'No Launch + Buy setups yet. Create one to get a launch wallet.',
  ].join('\n');
}

function launchBuyArchiveText(user) {
  const archivedOrders = getVisibleLaunchBuys(user, { archived: true });
  return [
    '\u{1F5C4}\uFE0F *Launch + Buy Archive*',
    '',
    archivedOrders.length > 0
      ? 'Archived launches stay here until restored or permanently deleted.'
      : 'No archived Launch + Buy setups right now.',
  ].join('\n');
}

function makeLaunchBuyEditorKeyboard(user) {
  const keyboard = new InlineKeyboard();
  const order = user.launchBuy;
  keyboard.text('\u{1F504} Refresh', 'launchbuy:refresh');
  keyboard.row();
  keyboard.text(order.tokenName ? '\u{1F3F7}\uFE0F Update Name' : '\u{1F3F7}\uFE0F Set Name', `launchbuy:set:token_name:${order.id}`);
  keyboard.text(order.symbol ? '\u{1F524} Update Symbol' : '\u{1F524} Set Symbol', `launchbuy:set:symbol:${order.id}`);
  keyboard.row();
  keyboard.text(order.description ? '\u{1F4DD} Update Description' : '\u{1F4DD} Set Description', `launchbuy:set:description:${order.id}`);
  keyboard.text(order.logoPath ? '\u{1F5BC}\uFE0F Update Logo' : '\u{1F5BC}\uFE0F Upload Logo', `launchbuy:set:logo:${order.id}`);
  keyboard.row();
  keyboard.text(order.walletSource === 'generated' ? '\u2705 Generate Wallets' : 'Generate Wallets', `launchbuy:set:wallet_source:generated:${order.id}`);
  keyboard.text(order.walletSource === 'imported' ? '\u2705 Import Wallets' : 'Import Wallets', `launchbuy:set:wallet_source:imported:${order.id}`);
  keyboard.row();
  keyboard.text(`\u{1F45B} Wallet Count: ${order.buyerWalletCount}`, `launchbuy:set:wallet_count:${order.id}`);
  keyboard.text(order.walletSource === 'imported' ? '\u{1F511} Paste Buyer Keys' : '\u{1F503} Regenerate Buyers', `launchbuy:set:buyer_keys:${order.id}`);
  keyboard.row();
  keyboard.text(order.totalBuySol ? `\u{1F4B0} Buy Budget ${order.totalBuySol} SOL` : '\u{1F4B0} Set Buy Budget', `launchbuy:set:total_buy:${order.id}`);
  keyboard.text(`\u26A1\uFE0F Jito Tip ${order.jitoTipSol || formatSolAmountFromLamports(LAUNCH_BUY_DEFAULT_JITO_TIP_LAMPORTS)} SOL`, `launchbuy:set:jito_tip:${order.id}`);
  keyboard.row();
  keyboard.text(order.website ? '\u{1F310} Website' : '\u{1F310} Set Website', `launchbuy:set:website:${order.id}`);
  keyboard.text(order.telegram ? '\u{1F4AC} Telegram' : '\u{1F4AC} Set Telegram', `launchbuy:set:telegram:${order.id}`);
  keyboard.row();
  keyboard.text(order.twitter ? 'ð• Twitter' : 'ð• Set Twitter', `launchbuy:set:twitter:${order.id}`);
  keyboard.text(order.privateKeyVisible ? '\u{1F648} Hide Key' : '\u{1F441}\uFE0F Show Key', `launchbuy:key:toggle:${order.id}`);
  keyboard.row();
  if (order.archivedAt) {
    keyboard.text('\u267B\uFE0F Restore', `launchbuy:restore:${order.id}`);
    keyboard.text(order.deleteConfirmations >= 1 ? '\u{1F6A8} Confirm Delete' : '\u{1F5D1}\uFE0F Delete', `launchbuy:delete:${order.id}`);
    keyboard.row();
    keyboard.text('\u2B05\uFE0F Back', 'nav:launch_buy_archive');
  } else {
    keyboard.text('\u{1F5C4}\uFE0F Archive', `launchbuy:archive:${order.id}`);
    keyboard.text(
      launchBuyIsReady(order)
        ? (
          ['queued', 'launching'].includes(order.status)
            ? '\u23F3 Launch In Progress'
            : '\u{1F680} Launch Bundle'
        )
        : '\u{1F512} Finish Setup First',
      launchBuyIsReady(order) && !['queued', 'launching'].includes(order.status)
        ? `launchbuy:launch:${order.id}`
        : 'launchbuy:locked:launch',
    );
    keyboard.row();
    keyboard.text(
      Array.isArray(order.buyerWallets) && order.buyerWallets.some((wallet) => wallet?.address && wallet?.secretKeyB64)
        ? '\u{1F4BC} Add Wallets To Buy / Sell'
        : '\u{1F512} Add Wallets To Buy / Sell',
      Array.isArray(order.buyerWallets) && order.buyerWallets.some((wallet) => wallet?.address && wallet?.secretKeyB64)
        ? `launchbuy:add_to_trading:${order.id}`
        : 'launchbuy:locked:add_to_trading',
    );
    keyboard.row();
    keyboard.text('\u2B05\uFE0F Back', 'nav:launch_buy');
  }
  keyboard.text('\u{1F3E0} Home', 'nav:home');
  return keyboard;
}

function launchBuyPrivateKeyText(order) {
  if (!order?.walletSecretKeyBase58) {
    return '`Not stored`';
  }
  return order.privateKeyVisible
    ? `\`${order.walletSecretKeyBase58}\``
    : '`Hidden - tap Show Key to reveal it.`';
}

function launchBuyEditorText(user) {
  const order = user.launchBuy;
  const walletPreview = (order.buyerWallets || []).slice(0, 5).map((wallet, index) =>
    `\u2022 ${wallet.label || `Buyer ${index + 1}`}: \`${wallet.address}\``
  );
  return [
    '\u{1F680} *Launch + Buy*',
    '',
    'Launch a new Pump coin and line up early bundled buys from multiple wallets.',
    '',
    MENU_DIVIDER,
    '\u{1F4CB} *Launch Setup*',
    `\u2022 Mode: *${launchBuyModeLabel(order)}*`,
    `\u2022 Status: *${launchBuyStatusLabel(order)}*`,
    `\u2022 Name: *${order.tokenName || 'Not set'}*`,
    `\u2022 Symbol: *${order.symbol || 'Not set'}*`,
    `\u2022 Description: *${order.description || 'Not set'}*`,
    `\u2022 Logo: *${order.logoFileName || 'Not uploaded'}*`,
    `\u2022 Wallet source: *${order.walletSource === 'imported' ? 'Imported wallets' : 'Generated wallets'}*`,
    `\u2022 Buyer wallets: *${order.buyerWalletCount}*`,
    `\u2022 Buy budget: *${order.totalBuySol || 'Not set'}*`,
    `\u2022 Jito tip: *${order.jitoTipSol || formatSolAmountFromLamports(LAUNCH_BUY_DEFAULT_JITO_TIP_LAMPORTS)} SOL*`,
    '',
    '\u{1F4B3} *Launch Wallet*',
    `\`${order.walletAddress}\``,
    `\u2022 Wallet balance: *${order.currentSol || '0'} SOL*`,
    `\u2022 Stored private key: ${launchBuyPrivateKeyText(order)}`,
    '\u2022 Generated keys stay hidden by default until you reveal them in your own bot flow.',
    '\u2022 Best practice: fund early and refresh once before launch so the buyer wallets can be warmed in advance.',
    '',
    '\u{1F4B0} *Funding Guide*',
    `\u2022 Setup fee: *${formatSolAmountFromLamports(order.estimatedSetupFeeLamports || 0)} SOL*`,
    ...(order.launchMode === 'magic' ? [`\u2022 Hidden routing estimate: *${formatSolAmountFromLamports(order.estimatedRoutingFeeLamports || 0)} SOL*`] : []),
    `\u2022 Estimated total needed: *${formatSolAmountFromLamports(order.estimatedTotalNeededLamports || 0)} SOL*`,
    '\u2022 Funding early improves launch speed because the bot can prepare buyer wallets before the live launch moment.',
    '',
    '\u{1F45B} *Buyer Wallet Preview*',
    ...(walletPreview.length > 0 ? walletPreview : ['\u2022 Buyer wallets not ready yet.']),
    ...((order.buyerWallets || []).length > 5 ? [`\u2022 +${order.buyerWallets.length - 5} more wallets`] : []),
    '',
    '\u{1F4B1} *Trading Desk Link*',
    '\u2022 Buyer wallets from this launch are synced into Buy / Sell automatically.',
    '',
    order.awaitingField
      ? promptForLaunchBuyField(order.awaitingField, order)
      : (
        launchBuyIsReady(order)
          ? 'Fund early if possible, refresh after the wallet is topped up, and let the bot warm the buyer wallets before you press launch.'
          : 'Finish the launch fields above to prepare this launch bundle.'
      ),
    ...(order.lastError ? ['', `\u26A0\uFE0F Last note: \`${order.lastError}\``] : []),
  ].join('\n');
}

function makeMagicBundleEditorKeyboard(user) {
  const keyboard = new InlineKeyboard();
  const order = user.magicBundle;

  keyboard.text('\u{1F504} Refresh', 'magicbundle:refresh');
  keyboard.row();
  keyboard.text(order.tokenName ? '\u{1F3F7}\uFE0F Update Name' : '\u{1F3F7}\uFE0F Set Token Name', `magicbundle:set:token_name:${order.id}`);
  keyboard.text(order.mintAddress ? '\u{1FA99} Update CA' : '\u{1FA99} Set CA', `magicbundle:set:mint:${order.id}`);
  keyboard.row();
  keyboard.text(`\u{1F45B} Wallets: ${order.walletCount || MAGIC_BUNDLE_DEFAULT_WALLET_COUNT}`, `magicbundle:set:wallet_count:${order.id}`);
  keyboard.row();
  keyboard.text(
    Number.isFinite(order.stopLossPercent) ? `\u{1F6D1} Stop Loss ${order.stopLossPercent}%` : '\u{1F6D1} Set Stop Loss',
    `magicbundle:set:stop_loss:${order.id}`,
  );
  keyboard.text(
    Number.isFinite(order.takeProfitPercent) ? `\u{1F3AF} Take Profit ${order.takeProfitPercent}%` : '\u{1F3AF} Set Take Profit',
    `magicbundle:set:take_profit:${order.id}`,
  );
  keyboard.row();
  keyboard.text(
    Number.isFinite(order.trailingStopLossPercent)
      ? `\u{1F4C9} Trail ${order.trailingStopLossPercent}%`
      : '\u{1F4C9} Set Trailing Stop',
    `magicbundle:set:trailing_stop_loss:${order.id}`,
  );
  keyboard.text(
    Number.isFinite(order.buyDipPercent) ? `\u{1F4B8} Buy Dip ${order.buyDipPercent}%` : '\u{1F4B8} Set Buy Dip',
    `magicbundle:set:buy_dip:${order.id}`,
  );
  keyboard.row();
  keyboard.text(
    order.sellOnDevSell ? '\u2705 Sell On Dev Sell: Yes' : '\u274C Sell On Dev Sell: No',
    `magicbundle:set:sell_on_dev_sell:${order.id}`,
  );
  keyboard.row();

  if (order.archivedAt) {
    keyboard.text('\u267B\uFE0F Restore', `magicbundle:restore:${order.id}`);
    keyboard.text(
      order.deleteConfirmations >= 1 ? '\u{1F6A8} Confirm Delete' : '\u{1F5D1}\uFE0F Delete',
      `magicbundle:delete:${order.id}`,
    );
    keyboard.row();
    keyboard.text('\u2B05\uFE0F Back', 'nav:magic_bundle_archive');
  } else {
    keyboard.text('\u{1F5C4}\uFE0F Archive', `magicbundle:archive:${order.id}`);
    keyboard.row();
    keyboard.text('\u2B05\uFE0F Back', 'nav:magic_bundle');
  }

  keyboard.text('\u{1F3E0} Home', 'nav:home');
  keyboard.row();
  keyboard.text('\u{1F504} Refresh', 'refresh:magic_bundle_editor');
  return keyboard;
}

function magicBundleEditorText(user) {
  const order = user.magicBundle;
  const previewWallets = Array.isArray(order.splitWallets) ? order.splitWallets.slice(0, 5) : [];
  const splitStatus = order.splitnowStatus || 'Not started';
  return [
    '\u2728 *Magic Bundle*',
    '',
    'One funded wallet in, many bundle wallets out.',
    '',
    '\u{1F4CB} *Bundle Setup*',
    `â€¢ Token: ${order.tokenName ? `*${order.tokenName}*` : '*Not set*'}`,
    `â€¢ CA: ${order.mintAddress ? `\`${order.mintAddress}\`` : '*Not set*'}`,
    `â€¢ Status: *${magicBundleStatusLabel(order)}*`,
    `â€¢ Bundle wallets: *${order.walletCount || MAGIC_BUNDLE_DEFAULT_WALLET_COUNT}*`,
    '',
    '\u{1F4B3} *Deposit Wallet*',
    `\`${order.walletAddress}\``,
    `â€¢ Deposit balance: *${order.currentSol || '0'} SOL*`,
    `â€¢ Total managed balance: *${formatSolAmountFromLamports(order.totalManagedLamports || 0)} SOL*`,
    '',
    '\u{1F4B0} *Fees & Net Split*',
    `â€¢ Setup fee: *${order.bundleMode === 'standard' ? 'Free' : `${formatSolAmountFromLamports(order.estimatedPlatformFeeLamports || 0)} SOL`}*`,
    `â€¢ Stealth routing estimate: *${formatSolAmountFromLamports(order.estimatedSplitNowFeeLamports || 0)} SOL*`,
    `â€¢ Estimated amount split to bundle wallets: *${formatSolAmountFromLamports(order.estimatedNetSplitLamports || 0)} SOL*`,
    '',
    '\u{1F6E0}\uFE0F *Trade Controls*',
    `â€¢ Stop loss: *${Number.isFinite(order.stopLossPercent) ? `${order.stopLossPercent}%` : 'Not set'}*`,
    `â€¢ Take profit: *${Number.isFinite(order.takeProfitPercent) ? `${order.takeProfitPercent}%` : 'Not set'}*`,
    `â€¢ Trailing stop: *${Number.isFinite(order.trailingStopLossPercent) ? `${order.trailingStopLossPercent}%` : 'Not set'}*`,
    `â€¢ Buy dip: *${Number.isFinite(order.buyDipPercent) ? `${order.buyDipPercent}%` : 'Not set'}*`,
    `â€¢ Sell on dev sell: *${order.sellOnDevSell ? 'Yes' : 'No'}*`,
    '',
    '\u{1F9FE} *Split Status*',
    `â€¢ Routing status: *${splitStatus}*`,
    ...(order.splitnowOrderId ? [`â€¢ Order ID: \`${order.splitnowOrderId}\``] : []),
    ...(order.splitnowDepositAddress ? [`â€¢ Routing deposit wallet: \`${order.splitnowDepositAddress}\``] : []),
    ...(order.splitCompletedAt ? [`â€¢ Split completed: ${formatTimestamp(order.splitCompletedAt)}`] : []),
    '',
    '\u{1F45B} *Bundle Wallet Preview*',
    ...previewWallets.map((wallet, index) => `â€¢ #${index + 1} \`${wallet.address.slice(0, 4)}...${wallet.address.slice(-4)}\` â€¢ ${wallet.currentSol || '0'} SOL`),
    ...(order.splitWallets.length > previewWallets.length
      ? [`â€¢ ...and *${order.splitWallets.length - previewWallets.length}* more wallets`] : []),
    ...(order.lastBalanceCheckAt ? ['', `Last checked: ${formatTimestamp(order.lastBalanceCheckAt)}`] : []),
    ...(order.lastError ? [`Last error: \`${order.lastError}\``] : []),
    '',
    order.awaitingField
      ? promptForMagicBundleField(order.awaitingField)
      : (cfg.splitnowEnabled
        ? 'Fund the deposit wallet above. Once the funds arrive, the worker will prepare the wallet spread automatically.'
        : 'This bundle service is temporarily offline right now. Please try again later.'),
  ].filter(Boolean).join('\n');
}

makeMagicBundleEditorKeyboard = function makeMagicBundleEditorKeyboard(user) {
  const keyboard = new InlineKeyboard();
  const order = user.magicBundle;

  keyboard.text(order.tokenName ? 'ðŸ·ï¸ Update Name' : 'ðŸ·ï¸ Set Token Name', `magicbundle:set:token_name:${order.id}`);
  keyboard.text(order.mintAddress ? 'ðŸª™ Update CA' : 'ðŸª™ Set CA', `magicbundle:set:mint:${order.id}`);
  keyboard.row();
  keyboard.text(`ðŸ‘› Wallets: ${order.walletCount || MAGIC_BUNDLE_DEFAULT_WALLET_COUNT}`, `magicbundle:set:wallet_count:${order.id}`);
  keyboard.row();
  keyboard.text(
    Number.isFinite(order.stopLossPercent) ? `ðŸ›‘ Stop Loss ${order.stopLossPercent}%` : 'ðŸ›‘ Set Stop Loss',
    `magicbundle:set:stop_loss:${order.id}`,
  );
  keyboard.text(
    Number.isFinite(order.takeProfitPercent) ? `ðŸŽ¯ Take Profit ${order.takeProfitPercent}%` : 'ðŸŽ¯ Set Take Profit',
    `magicbundle:set:take_profit:${order.id}`,
  );
  keyboard.row();
  keyboard.text(
    Number.isFinite(order.trailingStopLossPercent)
      ? `ðŸ“‰ Trail ${order.trailingStopLossPercent}%`
      : 'ðŸ“‰ Set Trailing Stop',
    `magicbundle:set:trailing_stop_loss:${order.id}`,
  );
  keyboard.text(
    Number.isFinite(order.buyDipPercent) ? `ðŸ’¸ Buy Dip ${order.buyDipPercent}%` : 'ðŸ’¸ Set Buy Dip',
    `magicbundle:set:buy_dip:${order.id}`,
  );
  keyboard.row();
  keyboard.text(
    order.sellOnDevSell ? 'âœ… Creator Sell Shield: On' : 'âŒ Creator Sell Shield: Off',
    `magicbundle:set:sell_on_dev_sell:${order.id}`,
  );
  keyboard.row();
  keyboard.text(
    magicBundleCanStart(order)
      ? (order.automationEnabled ? 'â¹ï¸ Stop Protection' : 'â–¶ï¸ Start Protection')
      : 'ðŸ”’ Start Protection',
    magicBundleCanStart(order) ? `magicbundle:toggle:${order.id}` : 'magicbundle:locked:toggle',
  );
  keyboard.row();

  if (order.archivedAt) {
    keyboard.text('â™»ï¸ Restore', `magicbundle:restore:${order.id}`);
    keyboard.text(
      order.deleteConfirmations >= 1 ? 'ðŸš¨ Confirm Delete' : 'ðŸ—‘ï¸ Delete',
      `magicbundle:delete:${order.id}`,
    );
    keyboard.row();
    keyboard.text('â¬…ï¸ Back', 'nav:magic_bundle_archive');
  } else {
    keyboard.text('ðŸ—„ï¸ Archive', `magicbundle:archive:${order.id}`);
    keyboard.row();
    keyboard.text('â¬…ï¸ Back', 'nav:magic_bundle');
  }

  keyboard.text('ðŸ  Home', 'nav:home');
  keyboard.row();
  keyboard.text('ðŸ”„ Refresh', 'refresh:magic_bundle_editor');
  return keyboard;
};

magicBundleEditorText = function magicBundleEditorText(user) {
  const order = user.magicBundle;
  const stats = normalizeMagicBundleStats(order.stats);
  const previewWallets = Array.isArray(order.splitWallets) ? order.splitWallets.slice(0, 5) : [];
  const splitStatus = order.splitnowStatus || 'Not started';
  const positionValueSol = formatSolAmountFromLamports(order.currentPositionValueLamports || 0);
  const lastTrigger = magicBundleTriggerLabel(order.lastTriggerReason || stats.lastTriggerReason);

  return [
    'âœ¨ *Magic Bundle*',
    '',
    'Turn one funded wallet into a protected multi-wallet bundle.',
    '',
    'ðŸ“‹ *Bundle Setup*',
    `â€¢ Token: ${order.tokenName ? `*${order.tokenName}*` : '*Not set*'}`,
    `â€¢ CA: ${order.mintAddress ? `\`${order.mintAddress}\`` : '*Not set*'}`,
    `â€¢ Status: *${magicBundleStatusLabel(order)}*`,
    `â€¢ Bundle wallets: *${order.walletCount || MAGIC_BUNDLE_DEFAULT_WALLET_COUNT}*`,
    '',
    'ðŸ’³ *Deposit Wallet*',
    `\`${order.walletAddress}\``,
    `â€¢ Deposit balance: *${order.currentSol || '0'} SOL*`,
    `â€¢ Total managed SOL: *${formatSolAmountFromLamports(order.totalManagedLamports || 0)} SOL*`,
    '',
    'ðŸ›¡ï¸ *Protection Engine*',
    `â€¢ Protection: *${order.automationEnabled ? 'Running' : 'Stopped'}*`,
    `â€¢ Live token balance: *${order.currentTokenAmountDisplay || '0'}*`,
    `â€¢ Live position value: *${positionValueSol} SOL*`,
    `â€¢ Last trigger: *${lastTrigger}*`,
    ...(order.creatorAddress ? [`â€¢ Creator wallet detected: \`${order.creatorAddress}\``] : []),
    '',
    'âš™ï¸ *Trade Controls*',
    `â€¢ Stop loss: *${Number.isFinite(order.stopLossPercent) ? `${order.stopLossPercent}%` : 'Not set'}*`,
    `â€¢ Take profit: *${Number.isFinite(order.takeProfitPercent) ? `${order.takeProfitPercent}%` : 'Not set'}*`,
    `â€¢ Trailing stop: *${Number.isFinite(order.trailingStopLossPercent) ? `${order.trailingStopLossPercent}%` : 'Not set'}*`,
    `â€¢ Buy dip: *${Number.isFinite(order.buyDipPercent) ? `${order.buyDipPercent}%` : 'Not set'}*`,
    `â€¢ Sell on creator sell: *${order.sellOnDevSell ? 'Yes' : 'No'}*`,
    '',
    'ðŸ“ˆ *Bundle Stats*',
    `â€¢ Actions fired: *${stats.triggerCount}*`,
    `â€¢ Dip buys: *${stats.dipBuyCount}*`,
    `â€¢ Sells: *${stats.sellCount}*`,
    `â€¢ Total buy size: *${formatSolAmountFromLamports(stats.totalBuyLamports)} SOL*`,
    `â€¢ Total sell size: *${formatSolAmountFromLamports(stats.totalSellLamports)} SOL*`,
    '',
    'ðŸ’° *Funding & Split*',
    `â€¢ Setup fee: *${order.bundleMode === 'standard' ? 'Free' : `${formatSolAmountFromLamports(order.estimatedPlatformFeeLamports || 0)} SOL`}*`,
    `â€¢ Stealth routing estimate: *${formatSolAmountFromLamports(order.estimatedSplitNowFeeLamports || 0)} SOL*`,
    `â€¢ Estimated amount reaching bundle wallets: *${formatSolAmountFromLamports(order.estimatedNetSplitLamports || 0)} SOL*`,
    `â€¢ Split status: *${splitStatus}*`,
    ...(order.splitCompletedAt ? [`â€¢ Split completed: ${formatTimestamp(order.splitCompletedAt)}`] : []),
    '',
    'ðŸ‘› *Bundle Wallet Preview*',
    ...previewWallets.map((wallet, index) => `â€¢ #${index + 1} \`${wallet.address.slice(0, 4)}...${wallet.address.slice(-4)}\` â€¢ ${wallet.currentSol || '0'} SOL â€¢ ${wallet.currentTokenAmountDisplay || '0'} tokens`),
    ...(order.splitWallets.length > previewWallets.length
      ? [`â€¢ ...and *${order.splitWallets.length - previewWallets.length}* more wallets`] : []),
    ...(order.lastBalanceCheckAt ? ['', `Last checked: ${formatTimestamp(order.lastBalanceCheckAt)}`] : []),
    ...(order.lastError ? [`Last error: \`${order.lastError}\``] : []),
    '',
    order.awaitingField
      ? promptForMagicBundleField(order.awaitingField)
      : (
        order.bundleMode === 'standard' || cfg.splitnowEnabled
          ? (
            magicBundleCanStart(order)
              ? 'The protection engine arms from the first live position it sees in each bundle wallet.'
              : 'Fund the deposit wallet above. After the spread is done, set your protection rules and start the bundle watcher.'
          )
          : 'Stealth Bundle is temporarily offline right now. Please try again later.'
      ),
  ].filter(Boolean).join('\n');
};

magicBundleCatalogText = function magicBundleCatalogText(user) {
  const activeOrders = getVisibleMagicBundles(user);
  return [
    '\u{1F4E6} *Bundle*',
    '',
    'Build a fresh multi-wallet trading bundle from one funded wallet.',
    '',
    '\u{1F4CA} *Choose Your Bundle Style*',
    '\u2022 *Magic Bundle (Stealth)* uses a more hidden wallet spread path.',
    '\u2022 *Regular Bundle* spreads funds directly from the deposit wallet.',
    '\u2022 Both modes support the same protection controls after the wallet spread is done.',
    '',
    '\u{1F4B0} *What To Cover*',
    '\u2022 *Regular Bundle*: free setup, plus normal network fees.',
    `\u2022 *Magic Bundle (Stealth)*: *${cfg.magicBundleStealthSetupFeeSol} SOL* setup fee + stealth routing cost.`,
    `\u2022 Bundle trades: *${formatBpsPercent(cfg.tradingHandlingFeeBps)}* handling fee per buy or sell.`,
    '\u2022 Keep a little extra SOL on top for network fees.',
    '',
    activeOrders.length > 0
      ? `Active bundles: *${activeOrders.length}*`
      : 'No bundles yet. Choose a style below to get a deposit wallet.',
  ].join('\n');
};

magicBundleEditorText = function magicBundleEditorText(user) {
  const order = user.magicBundle;
  const stats = normalizeMagicBundleStats(order.stats);
  const previewWallets = Array.isArray(order.splitWallets) ? order.splitWallets.slice(0, 5) : [];
  const splitStatus = order.bundleMode === 'standard'
    ? (order.splitCompletedAt ? 'Direct spread completed' : 'Waiting to spread')
    : (order.splitnowStatus || 'Not started');
  const positionValueSol = formatSolAmountFromLamports(order.currentPositionValueLamports || 0);
  const lastTrigger = magicBundleTriggerLabel(order.lastTriggerReason || stats.lastTriggerReason);
  const modeLabel = magicBundleModeLabel(order);
  const spreadFeeLine = order.bundleMode === 'standard'
    ? '\u2022 Wallet spread: *Direct wallet-to-wallet spread*'
    : `\u2022 Stealth routing estimate: *${formatSolAmountFromLamports(order.estimatedSplitNowFeeLamports || 0)} SOL*`;
  const setupFeeLine = order.bundleMode === 'standard'
    ? '\u2022 Setup fee: *Free*'
    : `\u2022 Setup fee: *${formatSolAmountFromLamports(order.estimatedPlatformFeeLamports || 0)} SOL*`;

  return [
    '\u{1F4E6} *Bundle*',
    '',
    'Turn one funded wallet into a protected multi-wallet bundle.',
    '',
    '\u{1F4CB} *Bundle Setup*',
    `\u2022 Style: *${modeLabel}*`,
    `\u2022 Token: ${order.tokenName ? `*${order.tokenName}*` : '*Not set*'}`,
    `\u2022 CA: ${order.mintAddress ? `\`${order.mintAddress}\`` : '*Not set*'}`,
    `\u2022 Status: *${magicBundleStatusLabel(order)}*`,
    `\u2022 Bundle wallets: *${order.walletCount || MAGIC_BUNDLE_DEFAULT_WALLET_COUNT}*`,
    '',
    '\u{1F4B3} *Deposit Wallet*',
    `\`${order.walletAddress}\``,
    `\u2022 Deposit balance: *${order.currentSol || '0'} SOL*`,
    `\u2022 Total managed SOL: *${formatSolAmountFromLamports(order.totalManagedLamports || 0)} SOL*`,
    '',
    '\u{1F6E1}\uFE0F *Protection Engine*',
    `\u2022 Protection: *${order.automationEnabled ? 'Running' : 'Stopped'}*`,
    `\u2022 Live token balance: *${order.currentTokenAmountDisplay || '0'}*`,
    `\u2022 Live position value: *${positionValueSol} SOL*`,
    `\u2022 Last trigger: *${lastTrigger}*`,
    ...(order.creatorAddress ? [`\u2022 Creator wallet detected: \`${order.creatorAddress}\``] : []),
    '',
    '\u2699\uFE0F *Trade Controls*',
    `\u2022 Stop loss: *${Number.isFinite(order.stopLossPercent) ? `${order.stopLossPercent}%` : 'Not set'}*`,
    `\u2022 Take profit: *${Number.isFinite(order.takeProfitPercent) ? `${order.takeProfitPercent}%` : 'Not set'}*`,
    `\u2022 Trailing stop: *${Number.isFinite(order.trailingStopLossPercent) ? `${order.trailingStopLossPercent}%` : 'Not set'}*`,
    `\u2022 Buy dip: *${Number.isFinite(order.buyDipPercent) ? `${order.buyDipPercent}%` : 'Not set'}*`,
    `\u2022 Sell on creator sell: *${order.sellOnDevSell ? 'Yes' : 'No'}*`,
    '',
    '\u{1F4C8} *Bundle Stats*',
    `\u2022 Actions fired: *${stats.triggerCount}*`,
    `\u2022 Dip buys: *${stats.dipBuyCount}*`,
    `\u2022 Sells: *${stats.sellCount}*`,
    `\u2022 Total buy size: *${formatSolAmountFromLamports(stats.totalBuyLamports)} SOL*`,
    `\u2022 Total sell size: *${formatSolAmountFromLamports(stats.totalSellLamports)} SOL*`,
    `\u2022 Handling fees paid: *${formatSolAmountFromLamports(stats.totalFeeLamports || 0)} SOL*`,
    '',
    '\u{1F4B0} *Funding & Spread*',
    setupFeeLine,
    spreadFeeLine,
    `\u2022 Trade handling fee: *${formatBpsPercent(cfg.tradingHandlingFeeBps)}* per buy or sell`,
    `\u2022 Estimated amount reaching bundle wallets: *${formatSolAmountFromLamports(order.estimatedNetSplitLamports || 0)} SOL*`,
    `\u2022 Spread status: *${splitStatus}*`,
    ...(order.splitCompletedAt ? [`\u2022 Spread completed: ${formatTimestamp(order.splitCompletedAt)}`] : []),
    '',
    '\u{1F45B} *Bundle Wallet Preview*',
    ...previewWallets.map((wallet, index) => `\u2022 #${index + 1} \`${wallet.address.slice(0, 4)}...${wallet.address.slice(-4)}\` \u2022 ${wallet.currentSol || '0'} SOL \u2022 ${wallet.currentTokenAmountDisplay || '0'} tokens`),
    ...(order.splitWallets.length > previewWallets.length
      ? [`\u2022 ...and *${order.splitWallets.length - previewWallets.length}* more wallets`] : []),
    ...(order.lastBalanceCheckAt ? ['', `Last checked: ${formatTimestamp(order.lastBalanceCheckAt)}`] : []),
    ...(order.lastError ? [`Last error: \`${order.lastError}\``] : []),
    '',
    order.awaitingField
      ? promptForMagicBundleField(order.awaitingField)
      : (
        magicBundleCanStart(order)
          ? 'The protection engine arms from the first live position it sees in each bundle wallet.'
          : 'Fund the deposit wallet above. The bundle will spread those funds automatically, then you can arm your trade controls.'
      ),
  ].filter(Boolean).join('\n');
};

makeMagicBundleEditorKeyboard = function makeMagicBundleEditorKeyboard(user) {
  const keyboard = new InlineKeyboard();
  const order = user.magicBundle;

  keyboard.text('\u{1F3F7}\uFE0F Update Name', `magicbundle:set:token_name:${order.id}`);
  keyboard.text(order.mintAddress ? '\u{1FA99} Update CA' : '\u{1FA99} Set CA', `magicbundle:set:mint:${order.id}`);
  keyboard.row();
  keyboard.text(`\u{1F45B} Wallets: ${order.walletCount || MAGIC_BUNDLE_DEFAULT_WALLET_COUNT}`, `magicbundle:set:wallet_count:${order.id}`);
  keyboard.row();
  keyboard.text(
    Number.isFinite(order.stopLossPercent) ? `\u{1F6D1} Stop Loss ${order.stopLossPercent}%` : '\u{1F6D1} Set Stop Loss',
    `magicbundle:set:stop_loss:${order.id}`,
  );
  keyboard.text(
    Number.isFinite(order.takeProfitPercent) ? `\u{1F3AF} Take Profit ${order.takeProfitPercent}%` : '\u{1F3AF} Set Take Profit',
    `magicbundle:set:take_profit:${order.id}`,
  );
  keyboard.row();
  keyboard.text(
    Number.isFinite(order.trailingStopLossPercent)
      ? `\u{1F4C9} Trail ${order.trailingStopLossPercent}%`
      : '\u{1F4C9} Set Trailing Stop',
    `magicbundle:set:trailing_stop_loss:${order.id}`,
  );
  keyboard.text(
    Number.isFinite(order.buyDipPercent) ? `\u{1F4B8} Buy Dip ${order.buyDipPercent}%` : '\u{1F4B8} Set Buy Dip',
    `magicbundle:set:buy_dip:${order.id}`,
  );
  keyboard.row();
  keyboard.text(
    order.sellOnDevSell ? '\u2705 Creator Sell Shield: On' : '\u274C Creator Sell Shield: Off',
    `magicbundle:set:sell_on_dev_sell:${order.id}`,
  );
  keyboard.row();
  keyboard.text(
    Array.isArray(order.splitWallets) && order.splitWallets.some((wallet) => wallet?.address && wallet?.secretKeyB64)
      ? '\u{1F4BC} Add Wallets To Buy / Sell'
      : '\u{1F512} Add Wallets To Buy / Sell',
    Array.isArray(order.splitWallets) && order.splitWallets.some((wallet) => wallet?.address && wallet?.secretKeyB64)
      ? `magicbundle:add_to_trading:${order.id}`
      : 'magicbundle:locked:add_to_trading',
  );
  keyboard.row();
  keyboard.text(
    magicBundleCanStart(order)
      ? (order.automationEnabled ? '\u23F9\uFE0F Stop Protection' : '\u25B6\uFE0F Start Protection')
      : '\u{1F512} Start Protection',
    magicBundleCanStart(order) ? `magicbundle:toggle:${order.id}` : 'magicbundle:locked:toggle',
  );
  keyboard.row();

  if (order.archivedAt) {
    keyboard.text('\u267B\uFE0F Restore', `magicbundle:restore:${order.id}`);
    keyboard.text(
      order.deleteConfirmations >= 1 ? '\u{1F6A8} Confirm Delete' : '\u{1F5D1}\uFE0F Delete',
      `magicbundle:delete:${order.id}`,
    );
    keyboard.row();
    keyboard.text('\u2B05\uFE0F Back', 'nav:magic_bundle_archive');
  } else {
    keyboard.text('\u{1F5C4}\uFE0F Archive', `magicbundle:archive:${order.id}`);
    keyboard.row();
    keyboard.text('\u2B05\uFE0F Back', 'nav:magic_bundle');
  }

  keyboard.text('\u{1F3E0} Home', 'nav:home');
  keyboard.row();
  keyboard.text('\u{1F504} Refresh', 'refresh:magic_bundle_editor');
  return keyboard;
};

buySellText = function buySellText(user) {
  const tradingDesk = normalizeTradingDesk(user.tradingDesk);
  const activeWallet = getActiveTradingWallet(user);
  const selectedBundle = user.magicBundles?.find((bundle) => bundle.id === tradingDesk.selectedMagicBundleId) ?? null;
  return [
    '\u{1F4B1} *Buy / Sell Desk*',
    '',
    'A clean trading hub for fast swaps, wallet control, bundles, limit orders, and copy-trading workflows.',
    '',
    MENU_DIVIDER,
    '\u2728 *Desk Overview*',
    `â€¢ Active wallet: *${activeWallet ? activeWallet.label : 'Not set'}*`,
    `â€¢ Wallet address: ${activeWallet ? `\`${activeWallet.address}\`` : 'Add or generate a wallet first'}`,
    `â€¢ Wallet count: *${tradingDesk.wallets.length}*`,
    `â€¢ Selected bundle: *${selectedBundle ? (selectedBundle.tokenName || selectedBundle.id) : 'None selected'}*`,
    `â€¢ Quick trade CA: ${tradingDesk.quickTradeMintAddress ? `\`${tradingDesk.quickTradeMintAddress}\`` : '*Not set*'}`,
    '',
    '\u{1F4CA} *What This Menu Is For*',
    'â€¢ Quick token buy / sell flow',
    'â€¢ Wallet import and wallet generation',
    'â€¢ Bundle selection for multi-wallet execution',
    'â€¢ Limit-order and copy-trading control panels',
    `â€¢ Supported trade routes use a *${formatBpsPercent(cfg.tradingHandlingFeeBps)}* handling fee per executed trade`,
    '',
    '\u26A0\uFE0F *Hot-Wallet Warning*',
    'Any wallet imported here should be treated like a live trading wallet with real funds.',
    ...(tradingDesk.awaitingField ? ['', promptForBuySellField(tradingDesk.awaitingField)] : []),
    ...(tradingDesk.lastError ? ['', `Last error: \`${tradingDesk.lastError}\``] : []),
  ].join('\n');
};

buySellQuickText = function buySellQuickText(user) {
  const activeWallet = getActiveTradingWallet(user);
  const tradingDesk = normalizeTradingDesk(user.tradingDesk);
  return [
    '\u26A1 *Quick Buy / Sell*',
    '',
    'Paste a token CA to get this desk ready for fast trading.',
    '',
    `â€¢ Token CA: ${tradingDesk.quickTradeMintAddress ? `\`${tradingDesk.quickTradeMintAddress}\`` : '*Not set*'}`,
    `â€¢ Active wallet: *${activeWallet ? activeWallet.label : 'Not set'}*`,
    `â€¢ Wallet ready: *${activeWallet ? 'Yes' : 'No'}*`,
    `â€¢ Handling fee: *${formatBpsPercent(cfg.tradingHandlingFeeBps)}* per executed trade`,
    '',
    'Use this desk to keep your active wallet, token CA, and bundle selection organized for trading.',
    ...(tradingDesk.awaitingField ? ['', promptForBuySellField(tradingDesk.awaitingField)] : []),
  ].join('\n');
};

buySellLimitText = function buySellLimitText() {
  return [
    '\u{1F4CC} *Limit Orders*',
    '',
    'This section is for setting automatic entries and exits instead of staring at the chart all day.',
    '',
    `Handling fee: *${formatBpsPercent(cfg.tradingHandlingFeeBps)}* per executed trade.`,
    '',
    'Professional trading bots typically let you:',
    'â€¢ place limit buys below current price',
    'â€¢ place take-profit sells above current price',
    'â€¢ place stop-loss sells below current price',
    'â€¢ choose a validity window for each order',
    '',
    'Use this menu as the control center for structured entries, profit targets, and downside protection.',
  ].join('\n');
};

buySellCopyText = function buySellCopyText() {
  return [
    '\u{1F465} *Copy Trading*',
    '',
    'This section is for following another wallet and copying its buys and sells with your own trading wallet.',
    '',
    `Handling fee: *${formatBpsPercent(cfg.tradingHandlingFeeBps)}* per executed trade.`,
    '',
    'Professional copy-trade flows usually include:',
    'â€¢ wallet to follow',
    'â€¢ buy amount rules',
    'â€¢ sell matching rules',
    'â€¢ stop-loss / take-profit controls',
    'â€¢ whitelist / blacklist safety filters',
    '',
    'Use this menu as the control center for wallet-following rules, sizing, and risk controls.',
  ].join('\n');
};

buySellText = function buySellText(user) {
  const tradingDesk = normalizeTradingDesk(user.tradingDesk);
  const activeWallet = getActiveTradingWallet(user);
  const selectedBundle = user.magicBundles?.find((bundle) => bundle.id === tradingDesk.selectedMagicBundleId) ?? null;
  return [
    '\u{1F4B1} *Buy / Sell Desk*',
    '',
    'A clean trading hub for wallet control, bundle selection, and trading-ready setup.',
    '',
    MENU_DIVIDER,
    '\u2728 *Desk Overview*',
    `â€¢ Active wallet: *${activeWallet ? activeWallet.label : 'Not set'}*`,
    `â€¢ Wallet address: ${activeWallet ? `\`${activeWallet.address}\`` : 'Add or generate a wallet first'}`,
    `â€¢ Wallet count: *${tradingDesk.wallets.length}*`,
    `â€¢ Selected bundle: *${selectedBundle ? (selectedBundle.tokenName || selectedBundle.id) : 'None selected'}*`,
    `â€¢ Token CA: ${tradingDesk.quickTradeMintAddress ? `\`${tradingDesk.quickTradeMintAddress}\`` : '*Not set*'}`,
    '',
    '\u{1F4CA} *What This Menu Is For*',
    'â€¢ Token CA setup for trading-ready wallets',
    'â€¢ Wallet import and wallet generation',
    'â€¢ Bundle selection for multi-wallet execution',
    'â€¢ Active-wallet management from one desk',
    `â€¢ Supported trade routes use a *${formatBpsPercent(cfg.tradingHandlingFeeBps)}* handling fee per executed trade`,
    '',
    '\u26A0\uFE0F *Hot-Wallet Warning*',
    'Any wallet imported here should be treated like a live trading wallet with real funds.',
    ...(tradingDesk.awaitingField ? ['', promptForBuySellField(tradingDesk.awaitingField)] : []),
    ...(tradingDesk.lastError ? ['', `Last error: \`${tradingDesk.lastError}\``] : []),
  ].join('\n');
};

homeText = function homeText() {
  return [
    '\u{1F44B} *Welcome to Wizard Toolz!*',
    '',
    'Professional Telegram automation for reactions, trading, volume, burn systems, holder distribution, FOMO strategy, smart sell execution, bundle prep, and launch sniping.',
    '',
    MENU_DIVIDER,
    '\u2728 *How It Works*',
    '1. Choose the service you want to run.',
    '2. Configure the wallet, mint, package, or target.',
    '3. Fund the generated wallet when required.',
    '4. Start the automation and manage everything from Telegram.',
    '',
    MENU_DIVIDER,
    '\u{1F680} *Supported Venues*',
    'Raydium â€¢ PumpSwap â€¢ Meteora â€¢ Pumpfun â€¢ Meteora DBC â€¢ Bags â€¢ LetsBonk â€¢ LaunchLab',
    '',
    '\u{1F4CA} Plans from 1 SOL â€¢ \u{1F6E1}\uFE0F Professional execution â€¢ \u{1F381} Free trial available',
    `\u{1F4B8} Our handled trade routes use *${formatBpsPercent(cfg.tradingHandlingFeeBps)}* per trade, lower than every competitor we track, and net platform profit is routed 50% to treasury, 25% to buyback + burn, and 25% to the SOL rewards vault`,
    `\u{1F91D} Need help? @${SUPPORT_USERNAME}`,
    '\u{1F4AC} Community chat: @wizardtoolz',
    '\u{1F514} Alerts channel: @wizardtoolz_alerts',
    '',
    'Ready? Choose a service below.',
  ].join('\n');
};

burnAgentEditorText = function burnAgentEditorText(user, balanceLamports = null) {
  const agent = user.burnAgent;
  const runtime = agent.runtime || {};
  const recentLogs = getRecentActivityLogs(user, { scopePrefix: `burn_agent:${agent.id}`, limit: 4 });
  const lines = [
    '\u{1F916} *Burn Agent*',
    '',
    '\u26A0\uFE0F *Serious wallet warning:* this flow can control creator rewards, buybacks, and burns on-chain.',
    '\u26A0\uFE0F If you paste a private key here, treat it as a live hot wallet.',
    '',
    '\u{1F4CB} *Agent Overview*',
    `â€¢ Agent ID: \`${agent.id}\``,
    `â€¢ Speed: *${burnAgentSpeedLabel(agent.speed)}*`,
    `â€¢ Status: *${agent.automationEnabled ? 'Running' : 'Stopped'}*`,
    `â€¢ Wallet mode: *${burnAgentWalletModeLabel(agent.walletMode)}*`,
    `â€¢ Token name: *${agent.tokenName || 'Not set'}*`,
  ];

  if (burnAgentNeedsWalletChoice(user)) {
    lines.push(
      '',
      '\u{1F511} *Wallet Setup*',
      'â€¢ *Generate Wallet*: we create a wallet for you, and you must mint the coin from that wallet.',
      "â€¢ *Provide My Own*: you paste the private key for the creator wallet you already control.",
      '',
      'Choose one of the options below to continue.',
    );
    return lines.join('\n');
  }

  lines.push(
    '',
    '\u{1F4B3} *Agent Wallet*',
    `â€¢ Address: \`${agent.walletAddress || 'Not ready'}\``,
    `â€¢ Balance: *${Number.isInteger(balanceLamports) ? `${formatSolAmountFromLamports(balanceLamports)} SOL` : 'Unavailable'}*`,
  );

  if (agent.speed === 'lightning' && agent.walletMode === 'generated') {
    lines.push('â€¢ Mint the coin with this exact wallet.');
    lines.push('â€¢ Withdraw any SOL before regenerating this wallet. Regeneration is blocked until the balance is zero.');
  } else if (agent.speed === 'lightning' && agent.walletMode === 'provided') {
    lines.push(agent.walletAddress
      ? 'â€¢ This agent can claim creator rewards directly because it holds the creator wallet key.'
      : 'â€¢ Add the creator wallet private key you mint with.');
  } else if (agent.speed === 'normal') {
    lines.push('â€¢ Normal mode uses a managed wallet. You route a chosen creator-reward share to this wallet on Pump.fun.');
  }

  if (burnAgentHasStoredPrivateKey(agent)) {
    lines.push(`â€¢ Stored private key: ${burnAgentPrivateKeyText(agent)}`);
  }

  lines.push(
    '',
    '\u2699\uFE0F *Strategy*',
    `â€¢ Mint: ${agent.mintAddress ? `\`${agent.mintAddress}\`` : 'Not set'}`,
  );

  if (isNormalBurnAgent(agent)) {
    lines.push('â€¢ Burn share: *100% of the rewards routed to this agent*');
  } else {
    lines.push(
      `â€¢ Treasury wallet: ${agent.treasuryAddress ? `\`${agent.treasuryAddress}\`` : 'Not set'}`,
      `â€¢ Burn share: ${Number.isInteger(agent.burnPercent) ? `*${agent.burnPercent}%*` : 'Not set'}`,
      `â€¢ Treasury share: ${Number.isInteger(agent.treasuryPercent) ? `*${agent.treasuryPercent}%*` : 'Not set'}`,
    );
  }

  lines.push(
    '',
    '\u{1F4C8} *Burn Stats*',
    `â€¢ Claim checks: *${runtime.totalClaimChecks || 0}*`,
    `â€¢ Claims completed: *${runtime.totalClaimCount || 0}*`,
    `â€¢ Total claimed: *${formatSolAmountFromLamports(runtime.totalClaimedLamports || 0)} SOL*`,
    `â€¢ Treasury payouts: *${runtime.totalTreasuryTransferCount || 0}*`,
    `â€¢ Treasury sent: *${formatSolAmountFromLamports(runtime.totalTreasuryLamportsSent || 0)} SOL*`,
    `â€¢ Buybacks executed: *${runtime.totalBuybackCount || 0}*`,
    `â€¢ Buyback SOL used: *${formatSolAmountFromLamports(runtime.totalBuybackLamports || 0)} SOL*`,
    `â€¢ Burns executed: *${runtime.totalBurnCount || 0}*`,
    `â€¢ Raw amount burned: *${runtime.totalBurnedRawAmount || '0'}*`,
  );

  if (runtime.lastVaultLamports) {
    lines.push(`â€¢ Current claimable rewards: *${formatSolAmountFromLamports(Number(runtime.lastVaultLamports) || 0)} SOL*`);
  }
  if (runtime.lastCheckedAt) {
    lines.push(`â€¢ Last runtime check: ${formatTimestamp(runtime.lastCheckedAt)}`);
  }
  if (runtime.lastBuybackMode) {
    lines.push(`â€¢ Last buyback route: *${runtime.lastBuybackMode}*`);
  }

  if (recentLogs.length > 0) {
    lines.push('', '\u{1F4DD} *Recent Activity*', ...recentLogs.map((entry) => formatActivityLogLine(entry)));
  }

  if (agent.awaitingField) {
    lines.push('', `\u23F3 ${burnAgentPromptLabel(agent.awaitingField)}. Send the value in chat now.`);
  } else if (burnAgentIsReady(agent)) {
    lines.push('', '\u2705 Agent setup is complete.');
  } else {
    lines.push('', 'Finish the missing fields below to complete this agent.');
  }

  lines.push('', 'Stats update automatically in the worker. Tap Refresh to redraw the latest numbers in Telegram.');

  if (agent.regenerateConfirmations > 0) {
    lines.push('', `\u{1F6A8} Wallet regenerate confirmation progress: *${agent.regenerateConfirmations}/3*`);
  }

  return lines.join('\n');
};

makeBurnAgentEditorKeyboard = function makeBurnAgentEditorKeyboard(user) {
  const keyboard = new InlineKeyboard();
  const agent = user.burnAgent;

  if (isArchivedBurnAgent(agent)) {
    keyboard.text('\u267B\uFE0F Restore Agent', `burn:restore:${agent.id}`);
    keyboard.row();
    keyboard.text(
      agent.deleteConfirmations >= 1 ? '\u{1F6A8} Confirm Permanent Delete' : '\u{1F5D1}\uFE0F Delete Permanently',
      `burn:delete:${agent.id}`,
    );
    keyboard.row();
    keyboard.text('\u2B05\uFE0F Back', 'nav:burn_agent_archive');
    keyboard.text('\u{1F3E0} Home', 'nav:home');
    keyboard.row().text('\u{1F504} Refresh', 'refresh:burn_agent_editor');
    return keyboard;
  }

  if (burnAgentNeedsWalletChoice(user)) {
    keyboard.text('\u{1F9EA} Generate Wallet', `burn:wallet:generated:${agent.id}`);
    keyboard.row();
    keyboard.text('\u{1F510} I\'ll Provide My Own', `burn:wallet:provided:${agent.id}`);
    keyboard.row();
  } else {
    if (!agent.walletSecretKeyB64 || agent.walletMode === 'provided') {
      keyboard.text(
        agent.walletAddress ? '\u{1F511} Replace Private Key' : '\u{1F511} Add Private Key',
        `burn:set:private_key:${agent.id}`,
      );
      keyboard.row();
    }

    if (agent.walletMode === 'generated' || agent.walletMode === 'managed') {
      const regenLabel = agent.regenerateConfirmations <= 0
        ? '\u{1F504} Regenerate Wallet'
        : `\u{1F6A8} Confirm Regenerate ${agent.regenerateConfirmations}/3`;
      keyboard.text(regenLabel, `burn:regen:${agent.id}`);
      keyboard.row();
    }

    if (burnAgentHasStoredPrivateKey(agent)) {
      keyboard.text(
        agent.privateKeyVisible ? '\u{1F648} Hide Private Key' : '\u{1F441}\uFE0F Show Private Key',
        `burn:key:toggle:${agent.id}`,
      );
      keyboard.row();
    }

    keyboard.text(
      burnAgentIsReady(agent)
        ? (agent.automationEnabled ? '\u23F9\uFE0F Stop Burn Agent' : '\u25B6\uFE0F Start Burn Agent')
        : '\u{1F512} Start Burn Agent',
      burnAgentIsReady(agent)
        ? `burn:toggle:${agent.id}`
        : 'burn:locked:toggle',
    );
    keyboard.text(
      agent.walletAddress ? '\u{1F4B8} Withdraw Funds' : '\u{1F512} Withdraw Funds',
      agent.walletAddress ? `burn:withdraw:${agent.id}` : 'burn:locked:withdraw',
    );
    keyboard.row();

    keyboard.text(agent.tokenName ? '\u{1F3F7}\uFE0F Update Token Name' : '\u{1F3F7}\uFE0F Set Token Name', `burn:set:token_name:${agent.id}`);
    keyboard.row();
    keyboard.text(agent.mintAddress ? '\u{1FA99} Update Mint' : '\u{1FA99} Set Mint', `burn:set:mint:${agent.id}`);
    keyboard.row();

    if (!isNormalBurnAgent(agent)) {
      keyboard.text(
        agent.treasuryAddress ? '\u{1F3E6} Update Treasury' : '\u{1F3E6} Set Treasury',
        `burn:set:treasury:${agent.id}`,
      );
      keyboard.row();
      keyboard.text(
        Number.isInteger(agent.burnPercent) ? `\u{1F525} Burn ${agent.burnPercent}%` : '\u{1F525} Set Burn %',
        `burn:set:burn_percent:${agent.id}`,
      );
      keyboard.text(
        Number.isInteger(agent.treasuryPercent)
          ? `\u{1F4B0} Treasury ${agent.treasuryPercent}%`
          : '\u{1F4B0} Set Treasury %',
        `burn:set:treasury_percent:${agent.id}`,
      );
      keyboard.row();
    }

    keyboard.text('\u{1F5C4}\uFE0F Archive Agent', `burn:archive:${agent.id}`);
    keyboard.row();
  }

  keyboard.text('\u2B05\uFE0F Back', 'nav:burn_agent');
  keyboard.text('\u{1F3E0} Home', 'nav:home');
  keyboard.row().text('\u{1F504} Refresh', 'refresh:burn_agent_editor');
  return keyboard;
};

makeMagicSellEditorKeyboard = function makeMagicSellEditorKeyboard(user) {
  const keyboard = new InlineKeyboard();
  const order = user.magicSell;

  keyboard.text('\u{1F504} Refresh', 'magic:refresh');
  keyboard.row();
  keyboard.text(order.tokenName ? '\u{1F3F7}\uFE0F Update Name' : '\u{1F3F7}\uFE0F Set Token Name', `magic:set:token_name:${order.id}`);
  keyboard.text(order.mintAddress ? '\u{1FA99} Update Mint' : '\u{1FA99} Set Mint', `magic:set:mint:${order.id}`);
  keyboard.row();
  keyboard.text(
    Number.isFinite(order.targetMarketCapUsd)
      ? `\u{1F3AF} MC: ${formatUsdCompact(order.targetMarketCapUsd)}`
      : '\u{1F3AF} Set Target MC',
    `magic:set:target_market_cap:${order.id}`,
  );
  keyboard.text(
    Array.isArray(order.whitelistWallets) && order.whitelistWallets.length > 0
      ? `\u{1F6E1}\uFE0F Whitelist: ${order.whitelistWallets.length}`
      : '\u{1F6E1}\uFE0F Set Whitelist',
    `magic:set:whitelist:${order.id}`,
  );
  keyboard.row();
  keyboard.text(
    `\u{1F45B} Seller Wallets: ${order.sellerWalletCount || MAGIC_SELL_DEFAULT_SELLER_WALLET_COUNT}`,
    `magic:set:seller_wallet_count:${order.id}`,
  );
  keyboard.text(
    order.privateKeyVisible ? '\u{1F648} Hide Private Key' : '\u{1F441}\uFE0F Show Private Key',
    `magic:key:toggle:${order.id}`,
  );
  keyboard.row();
  keyboard.text(
    magicSellIsReady(order)
      ? (order.automationEnabled ? '\u23F9\uFE0F Stop Magic Sell' : '\u25B6\uFE0F Start Magic Sell')
      : '\u{1F512} Start Magic Sell',
    magicSellIsReady(order) ? `magic:toggle:${order.id}` : 'magic:locked:toggle',
  );
  keyboard.row();

  if (order.archivedAt) {
    keyboard.text('\u267B\uFE0F Restore', `magic:restore:${order.id}`);
    keyboard.text('\u{1F5D1}\uFE0F Delete', `magic:delete:${order.id}`);
    keyboard.row();
    keyboard.text('\u2B05\uFE0F Back', 'nav:magic_sell_archive');
  } else {
    keyboard.text('\u{1F5C4}\uFE0F Archive', `magic:archive:${order.id}`);
    keyboard.row();
    keyboard.text('\u2B05\uFE0F Back', 'nav:magic_sell');
  }

  keyboard.text('\u{1F3E0} Home', 'nav:home');
  keyboard.row();
  keyboard.text('\u{1F504} Refresh', 'refresh:magic_sell_editor');
  return keyboard;
};

magicSellEditorText = function magicSellEditorText(user) {
  const order = user.magicSell;
  const previewWallets = Array.isArray(order.sellerWallets) ? order.sellerWallets.slice(0, 4) : [];
  const whitelistPreview = Array.isArray(order.whitelistWallets) ? order.whitelistWallets.slice(0, 3) : [];
  const totalSellerLamports = Array.isArray(order.sellerWallets)
    ? order.sellerWallets.reduce((sum, wallet) => sum + (Number.isInteger(wallet.currentLamports) ? wallet.currentLamports : 0), 0)
    : 0;

  return [
    '\u2728 *Magic Sell*',
    '',
    'Smart sell automation that activates only after your chosen market cap is reached.',
    '',
    '\u{1F4CB} *Setup*',
    `â€¢ Token: ${order.tokenName ? `*${order.tokenName}*` : '*Not set*'}`,
    `â€¢ Mint: ${order.mintAddress ? `\`${order.mintAddress}\`` : '*Not set*'}`,
    `â€¢ Status: *${magicSellStatusLabel(order)}*`,
    `â€¢ Target MC: *${Number.isFinite(order.targetMarketCapUsd) ? formatUsdCompact(order.targetMarketCapUsd) : 'Not set'}*`,
    `â€¢ Sell rule: *${order.sellPercent || MAGIC_SELL_SELL_PERCENT}% of each qualifying buy*`,
    `â€¢ Minimum buy: *${formatSolAmountFromLamports(order.minimumBuyLamports || MAGIC_SELL_MIN_BUY_LAMPORTS)} SOL*`,
    '',
    '\u{1F4B3} *Deposit Wallet*',
    `\`${order.walletAddress}\``,
    `â€¢ SOL on deposit wallet: *${order.currentSol || '0'} SOL*`,
    `â€¢ Token inventory: *${order.currentTokenAmountDisplay || '0'}*`,
    `â€¢ Stored private key: ${magicSellPrivateKeyText(order)}`,
    '',
    '\u{1F45B} *Seller Wallet Pool*',
    `â€¢ Wallets: *${order.sellerWalletCount || MAGIC_SELL_DEFAULT_SELLER_WALLET_COUNT}*`,
    `â€¢ Seller SOL total: *${formatSolAmountFromLamports(totalSellerLamports)} SOL*`,
    `â€¢ Recommended gas buffer: *${formatSolAmountFromLamports(order.recommendedGasLamports || 0)} SOL*`,
    ...previewWallets.map((wallet, index) => `â€¢ #${index + 1} \`${wallet.address.slice(0, 4)}...${wallet.address.slice(-4)}\` â€¢ ${wallet.currentSol || '0'} SOL â€¢ ${wallet.currentTokenAmountDisplay || '0'} tokens`),
    ...(order.sellerWallets.length > previewWallets.length
      ? [`â€¢ ...and *${order.sellerWallets.length - previewWallets.length}* more seller wallets`] : []),
    '',
    '\u{1F6E1}\uFE0F *Whitelist*',
    whitelistPreview.length > 0
      ? `â€¢ ${whitelistPreview.map((item) => `\`${item.slice(0, 4)}...${item.slice(-4)}\``).join(', ')}`
      : 'â€¢ No wallets whitelisted',
    ...(order.whitelistWallets.length > whitelistPreview.length
      ? [`â€¢ ...plus *${order.whitelistWallets.length - whitelistPreview.length}* more`] : []),
    '',
    '\u{1F4C8} *Live Stats*',
    `â€¢ Current MC: *${Number.isFinite(order.currentMarketCapUsd) ? formatUsdCompact(order.currentMarketCapUsd) : 'Waiting for market data'}*`,
    `â€¢ Phase: *${order.marketPhase || 'Unknown'}*`,
    `â€¢ Qualifying buys seen: *${order.stats?.triggerCount || 0}*`,
    `â€¢ Sells executed: *${order.stats?.sellCount || 0}*`,
    `â€¢ Observed buy flow: *${formatSolAmountFromLamports(order.stats?.totalObservedBuyLamports || 0)} SOL*`,
    `â€¢ Sell target flow: *${formatSolAmountFromLamports(order.stats?.totalTargetSellLamports || 0)} SOL*`,
    ...(order.lastBalanceCheckAt ? [`â€¢ Last checked: ${formatTimestamp(order.lastBalanceCheckAt)}`] : []),
    ...(order.lastError ? [`â€¢ Last error: \`${order.lastError}\``] : []),
    '',
    order.awaitingField
      ? promptForMagicSellField(order.awaitingField)
      : 'Deposit your token inventory into the wallet above and keep enough SOL available for seller-wallet gas. Magic Sell only activates after the target market cap is crossed.',
  ].filter(Boolean).join('\n');
};

function createBurnAgentWalletAssignment(walletMode) {
  const wallet = generateSolanaWallet();
  return {
    walletMode,
    walletAddress: wallet.address,
    walletSecretKeyB64: wallet.secretKeyB64,
    walletSecretKeyBase58: wallet.secretKeyBase58,
  };
}

function buildNewBurnAgent(speed) {
  const agent = createDefaultBurnAgentState();
  agent.speed = speed;
  agent.createdAt = new Date().toISOString();
  agent.updatedAt = agent.createdAt;

  if (speed === 'normal') {
    Object.assign(agent, createBurnAgentWalletAssignment('managed'));
    agent.burnPercent = 100;
    agent.treasuryPercent = 0;
  }

  return agent;
}

function promptForBurnAgentField(field) {
  switch (field) {
    case 'private_key':
      return 'Send the creator wallet private key in chat. Base58, base64, or a 64-byte JSON array all work.';
    case 'token_name':
      return 'Send the token name you want this burn agent to display in the menu.';
    case 'mint':
      return 'Send the token mint address in chat.';
    case 'treasury':
      return 'Send the treasury / keep wallet address in chat.';
    case 'burn_percent':
      return 'Send the percentage to swap into the mint and burn. Use a whole number from 0 to 100.';
    case 'treasury_percent':
      return 'Send the percentage to forward to the treasury / keep wallet. Burn % plus treasury % must equal 100.';
    case 'withdraw_address':
      return 'Send the Solana address where the wallet should withdraw its available SOL.';
    default:
      return 'Send the value in chat.';
  }
}

function parsePercentInput(value, label) {
  const parsed = Number.parseInt(String(value || '').trim(), 10);
  if (!Number.isInteger(parsed) || parsed < 0 || parsed > 100) {
    throw new Error(`${label} must be a whole number from 0 to 100.`);
  }

  return parsed;
}

function decodeBurnAgentWallet(secretKeyB64) {
  const bytes = Buffer.from(secretKeyB64, 'base64');
  return Keypair.fromSecretKey(Uint8Array.from(bytes));
}

function decodeRewardsVaultSigner() {
  const configuredAddress = process.env.WIZARD_REWARDS_VAULT_ADDRESS?.trim() || null;
  const candidateSecrets = [
    process.env.WIZARD_REWARDS_VAULT_SECRET_KEY_B64?.trim(),
    process.env.WIZARD_REWARDS_VAULT_SECRET_KEY?.trim(),
    process.env.DEV_WALLET_SECRET_KEY_B64?.trim(),
    process.env.DEV_WALLET_SECRET_KEY?.trim(),
  ].filter(Boolean);

  if (candidateSecrets.length === 0) {
    throw new Error('Rewards vault signer is not configured yet.');
  }

  for (const secret of candidateSecrets) {
    try {
      const signer = Keypair.fromSecretKey(Uint8Array.from(parseSecretKeyBytes(secret)));
      const address = signer.publicKey.toBase58();
      if (!configuredAddress || address === configuredAddress) {
        return signer;
      }
    } catch {
      // Try the next candidate.
    }
  }

  throw new Error('Rewards vault signer does not match WIZARD_REWARDS_VAULT_ADDRESS.');
}

async function sendStakingClaimRewards(user) {
  const state = normalizeStakingState(user.staking);
  if (!state.walletAddress) {
    throw new Error('Link a staking wallet first.');
  }

  if (state.claimableLamports < state.claimThresholdLamports) {
    throw new Error(`Minimum claim is ${formatSolAmountFromLamports(state.claimThresholdLamports)} SOL.`);
  }

  const signer = decodeRewardsVaultSigner();
  const destination = new PublicKey(state.walletAddress);
  const balanceLamports = await chainConnection.getBalance(signer.publicKey, 'confirmed');
  const availableLamports = Math.max(0, balanceLamports - STAKING_REWARDS_VAULT_FEE_RESERVE_LAMPORTS);

  if (availableLamports < state.claimableLamports) {
    throw new Error('Rewards vault does not have enough SOL for this claim yet.');
  }

  const latestBlockhash = await chainConnection.getLatestBlockhash('confirmed');
  const transaction = new Transaction({
    feePayer: signer.publicKey,
    recentBlockhash: latestBlockhash.blockhash,
    lastValidBlockHeight: latestBlockhash.lastValidBlockHeight,
  }).add(
    SystemProgram.transfer({
      fromPubkey: signer.publicKey,
      toPubkey: destination,
      lamports: state.claimableLamports,
    }),
  );

  const signature = await chainConnection.sendTransaction(transaction, [signer], {
    preflightCommitment: 'confirmed',
    maxRetries: 3,
  });

  await chainConnection.confirmTransaction({
    signature,
    blockhash: latestBlockhash.blockhash,
    lastValidBlockHeight: latestBlockhash.lastValidBlockHeight,
  }, 'confirmed');

  return {
    signature,
    claimedLamports: state.claimableLamports,
  };
}

async function getBurnAgentBalanceLamports(agent) {
  if (!agent?.walletAddress) {
    return null;
  }

  try {
    return await getWalletBalance(agent.walletAddress);
  } catch {
    return null;
  }
}

async function withdrawBurnAgentFunds(agent, destinationAddress) {
  if (!agent?.walletSecretKeyB64 || !agent?.walletAddress) {
    throw new Error('This agent wallet is not ready yet.');
  }

  const signer = decodeBurnAgentWallet(agent.walletSecretKeyB64);
  const destination = new PublicKey(destinationAddress);
  const balanceLamports = await chainConnection.getBalance(signer.publicKey, 'confirmed');
  const withdrawLamports = Math.max(0, balanceLamports - BURN_AGENT_WITHDRAW_RESERVE_LAMPORTS);

  if (withdrawLamports <= 0) {
    throw new Error('No withdrawable SOL is available after fee reserve.');
  }

  const latestBlockhash = await chainConnection.getLatestBlockhash('confirmed');
  const transaction = new Transaction({
    feePayer: signer.publicKey,
    recentBlockhash: latestBlockhash.blockhash,
    lastValidBlockHeight: latestBlockhash.lastValidBlockHeight,
  }).add(
    SystemProgram.transfer({
      fromPubkey: signer.publicKey,
      toPubkey: destination,
      lamports: withdrawLamports,
    }),
  );

  const signature = await chainConnection.sendTransaction(transaction, [signer], {
    preflightCommitment: 'confirmed',
    maxRetries: 3,
  });

  await chainConnection.confirmTransaction({
    signature,
    blockhash: latestBlockhash.blockhash,
    lastValidBlockHeight: latestBlockhash.lastValidBlockHeight,
  }, 'confirmed');

  return {
    signature,
    withdrawLamports,
  };
}

async function withdrawOrganicOrderFunds(order, destinationAddress) {
  if (!order?.walletSecretKeyB64 || !order?.walletAddress) {
    throw new Error('This Apple Booster wallet is not ready yet.');
  }

  if (order?.freeTrial) {
    throw new Error('Free-trial wallets are platform-managed and cannot be withdrawn from Telegram.');
  }

  const signer = decodeBurnAgentWallet(order.walletSecretKeyB64);
  const destination = new PublicKey(destinationAddress);
  const balanceLamports = await chainConnection.getBalance(signer.publicKey, 'confirmed');
  const withdrawLamports = Math.max(0, balanceLamports - BURN_AGENT_WITHDRAW_RESERVE_LAMPORTS);

  if (withdrawLamports <= 0) {
    throw new Error('No withdrawable SOL is available after fee reserve.');
  }

  const latestBlockhash = await chainConnection.getLatestBlockhash('confirmed');
  const transaction = new Transaction({
    feePayer: signer.publicKey,
    recentBlockhash: latestBlockhash.blockhash,
    lastValidBlockHeight: latestBlockhash.lastValidBlockHeight,
  }).add(
    SystemProgram.transfer({
      fromPubkey: signer.publicKey,
      toPubkey: destination,
      lamports: withdrawLamports,
    }),
  );

  const signature = await chainConnection.sendTransaction(transaction, [signer], {
    preflightCommitment: 'confirmed',
    maxRetries: 3,
  });

  await chainConnection.confirmTransaction({
    signature,
    blockhash: latestBlockhash.blockhash,
    lastValidBlockHeight: latestBlockhash.lastValidBlockHeight,
  }, 'confirmed');

  return {
    signature,
    withdrawLamports,
  };
}

async function getMintMetadata(mintAddress) {
  const mint = new PublicKey(mintAddress);
  const parsedAccount = await chainConnection.getParsedAccountInfo(mint, 'confirmed');
  const parsedInfo = parsedAccount.value?.data?.parsed?.info;
  const decimals = parsedInfo?.decimals;
  if (!Number.isInteger(decimals) || decimals < 0) {
    throw new Error('Unable to read mint decimals for that token.');
  }

  const tokenProgram = parsedAccount.value?.owner?.toBase58?.();
  return {
    decimals,
    tokenProgram: typeof tokenProgram === 'string' ? tokenProgram : TOKEN_PROGRAM_ID.toBase58(),
  };
}

async function getWalletTokenSnapshot(address, mintAddress) {
  const owner = new PublicKey(address);
  const mint = new PublicKey(mintAddress).toBase58();
  let decimals = null;
  let rawAmount = 0n;

  for (const programId of [TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID]) {
    const response = await chainConnection.getParsedTokenAccountsByOwner(
      owner,
      { programId },
      'confirmed',
    );
    for (const item of response.value) {
      const parsedInfo = item.account.data?.parsed?.info;
      const tokenAmount = parsedInfo?.tokenAmount;
      if (parsedInfo?.mint !== mint) {
        continue;
      }

      if (Number.isInteger(tokenAmount?.decimals) && decimals === null) {
        decimals = tokenAmount.decimals;
      }
      rawAmount += BigInt(tokenAmount?.amount || '0');
    }
  }

  return {
    rawAmount: rawAmount.toString(),
    decimals,
  };
}

async function refreshHolderBooster(userId) {
  const user = await getUserState(userId);
  const order = user.holderBooster;
  if (!order?.walletAddress) {
    return user;
  }

  try {
    const balanceLamports = await getWalletBalance(order.walletAddress);
    const mintMetadata = order.mintAddress ? await getMintMetadata(order.mintAddress) : null;
    const tokenSnapshot = order.mintAddress
      ? await getWalletTokenSnapshot(order.walletAddress, order.mintAddress)
      : { rawAmount: '0', decimals: mintMetadata?.decimals ?? null };

    return updateUserState(userId, (draft) => {
      draft.holderBooster = normalizeHolderBooster({
        ...draft.holderBooster,
        tokenDecimals: mintMetadata?.decimals ?? draft.holderBooster.tokenDecimals,
        tokenProgram: mintMetadata?.tokenProgram ?? draft.holderBooster.tokenProgram,
        currentLamports: balanceLamports,
        currentSol: formatSolAmountFromLamports(balanceLamports),
        currentTokenAmountRaw: tokenSnapshot.rawAmount,
        currentTokenAmountDisplay: formatTokenAmountFromRaw(
          tokenSnapshot.rawAmount,
          tokenSnapshot.decimals ?? mintMetadata?.decimals ?? draft.holderBooster.tokenDecimals ?? 0,
        ),
        lastBalanceCheckAt: new Date().toISOString(),
        lastError: null,
      });
      return draft;
    });
  } catch (error) {
    return updateUserState(userId, (draft) => {
      draft.holderBooster = normalizeHolderBooster({
        ...draft.holderBooster,
        lastBalanceCheckAt: new Date().toISOString(),
        lastError: String(error.message || error),
      });
      return draft;
    });
  }
}

async function refreshMagicSell(userId, magicSellId = null) {
  const user = await getUserState(userId);
  const targetMagicSellId = magicSellId || user.activeMagicSellId;
  const order = user.magicSells.find((item) => item.id === targetMagicSellId) ?? user.magicSell;
  if (!order?.walletAddress) {
    return user;
  }

  try {
    const balanceLamports = await getWalletBalance(order.walletAddress);
    const mintMetadata = order.mintAddress ? await getMintMetadata(order.mintAddress) : null;
    const tokenSnapshot = order.mintAddress
      ? await getWalletTokenSnapshot(order.walletAddress, order.mintAddress)
      : { rawAmount: '0', decimals: mintMetadata?.decimals ?? null };
    const sellerWallets = Array.isArray(order.sellerWallets)
      ? await Promise.all(order.sellerWallets.map(async (wallet) => {
        if (!wallet?.address) {
          return normalizeMagicSellSellerWallet(wallet);
        }

        const sellerLamports = await getWalletBalance(wallet.address);
        const sellerTokenSnapshot = order.mintAddress
          ? await getWalletTokenSnapshot(wallet.address, order.mintAddress)
          : { rawAmount: '0', decimals: mintMetadata?.decimals ?? null };

        return normalizeMagicSellSellerWallet({
          ...wallet,
          currentLamports: sellerLamports,
          currentSol: formatSolAmountFromLamports(sellerLamports),
          currentTokenAmountRaw: sellerTokenSnapshot.rawAmount,
          currentTokenAmountDisplay: formatTokenAmountFromRaw(
            sellerTokenSnapshot.rawAmount,
            sellerTokenSnapshot.decimals ?? mintMetadata?.decimals ?? order.tokenDecimals ?? 0,
          ),
        });
      }))
      : [];
    const totalManagedLamports = balanceLamports + sellerWallets.reduce(
      (sum, wallet) => sum + (Number.isInteger(wallet.currentLamports) ? wallet.currentLamports : 0),
      0,
    );

    return updateUserState(userId, (draft) => {
      updateMagicSellInDraft(draft, targetMagicSellId, (current) => ({
        ...current,
        tokenDecimals: mintMetadata?.decimals ?? current.tokenDecimals,
        tokenProgram: mintMetadata?.tokenProgram ?? current.tokenProgram,
        currentLamports: balanceLamports,
        currentSol: formatSolAmountFromLamports(balanceLamports),
        currentTokenAmountRaw: tokenSnapshot.rawAmount,
        currentTokenAmountDisplay: formatTokenAmountFromRaw(
          tokenSnapshot.rawAmount,
          tokenSnapshot.decimals ?? mintMetadata?.decimals ?? current.tokenDecimals ?? 0,
        ),
        totalManagedLamports,
        sellerWallets,
        lastBalanceCheckAt: new Date().toISOString(),
        lastError: null,
      }));
      return draft;
    });
  } catch (error) {
    return updateUserState(userId, (draft) => {
      updateMagicSellInDraft(draft, targetMagicSellId, (current) => ({
        ...current,
        lastBalanceCheckAt: new Date().toISOString(),
        lastError: String(error.message || error),
      }));
      return draft;
    });
  }
}

function estimateMagicBundleFees(balanceLamports, walletCount, bundleMode = 'stealth') {
  const safeBalanceLamports = Number.isInteger(balanceLamports) ? Math.max(0, balanceLamports) : 0;
  const safeWalletCount = Number.isInteger(walletCount) ? Math.max(1, walletCount) : MAGIC_BUNDLE_DEFAULT_WALLET_COUNT;
  const platformFeeLamports = bundleMode === 'standard'
    ? 0
    : cfg.magicBundleStealthSetupFeeLamports;
  const splitNowFeeLamports = bundleMode === 'standard'
    ? 0
    : Math.floor(safeBalanceLamports * (cfg.magicBundleSplitNowFeeEstimateBps / 10_000));
  const reserveLamports = safeWalletCount * 5_000;
  const netLamports = Math.max(0, safeBalanceLamports - platformFeeLamports - splitNowFeeLamports - reserveLamports);
  return {
    platformFeeLamports,
    splitNowFeeLamports,
    netLamports,
  };
}

async function refreshMagicBundle(userId, magicBundleId = null) {
  const user = await getUserState(userId);
  const targetMagicBundleId = magicBundleId || user.activeMagicBundleId;
  const order = user.magicBundles.find((item) => item.id === targetMagicBundleId) ?? user.magicBundle;
  if (!order?.walletAddress) {
    return user;
  }

  try {
    const balanceLamports = await getWalletBalance(order.walletAddress);
    const splitWallets = Array.isArray(order.splitWallets)
      ? await Promise.all(order.splitWallets.map(async (wallet) => {
        if (!wallet?.address) {
          return normalizeMagicBundleWorkerWallet(wallet);
        }

        const walletLamports = await getWalletBalance(wallet.address);
        return normalizeMagicBundleWorkerWallet({
          ...wallet,
          currentLamports: walletLamports,
          currentSol: formatSolAmountFromLamports(walletLamports),
        });
      }))
      : [];
    const totalManagedLamports = balanceLamports + splitWallets.reduce(
      (sum, wallet) => sum + (Number.isInteger(wallet.currentLamports) ? wallet.currentLamports : 0),
      0,
    );
    const estimates = estimateMagicBundleFees(balanceLamports, order.walletCount, order.bundleMode);

    return updateUserState(userId, (draft) => {
      updateMagicBundleInDraft(draft, targetMagicBundleId, (current) => ({
        ...current,
        currentLamports: balanceLamports,
        currentSol: formatSolAmountFromLamports(balanceLamports),
        totalManagedLamports,
        splitWallets,
        estimatedPlatformFeeLamports: estimates.platformFeeLamports,
        estimatedSplitNowFeeLamports: estimates.splitNowFeeLamports,
        estimatedNetSplitLamports: estimates.netLamports,
        lastBalanceCheckAt: new Date().toISOString(),
        lastError: null,
      }));
      return draft;
    });
  } catch (error) {
    return updateUserState(userId, (draft) => {
      updateMagicBundleInDraft(draft, targetMagicBundleId, (current) => ({
        ...current,
        lastBalanceCheckAt: new Date().toISOString(),
        lastError: String(error.message || error),
      }));
      return draft;
    });
  }
}

async function refreshTradingDesk(userId) {
  const user = await getUserState(userId);
  const tradingDesk = normalizeTradingDesk(user.tradingDesk);
  const wallets = Array.isArray(tradingDesk.wallets)
    ? await Promise.all(tradingDesk.wallets.map(async (wallet) => {
      if (!wallet?.address) {
        return normalizeTradingWallet(wallet);
      }

      try {
        const lamports = await getWalletBalance(wallet.address);
        return normalizeTradingWallet({
          ...wallet,
          currentLamports: lamports,
          currentSol: formatSolAmountFromLamports(lamports),
        });
      } catch {
        return normalizeTradingWallet(wallet);
      }
    }))
    : [];

  return updateUserState(userId, (draft) => {
    draft.tradingDesk = normalizeTradingDesk({
      ...draft.tradingDesk,
      wallets,
      lastBalanceCheckAt: new Date().toISOString(),
      lastError: null,
    });
    return draft;
  });
}

async function refreshFomoBooster(userId) {
  const user = await getUserState(userId);
  const order = user.fomoBooster;
  if (!order?.walletAddress) {
    return user;
  }

  try {
    const balanceLamports = await getWalletBalance(order.walletAddress);
    const mintMetadata = order.mintAddress ? await getMintMetadata(order.mintAddress) : null;
    const tokenSnapshot = order.mintAddress
      ? await getWalletTokenSnapshot(order.walletAddress, order.mintAddress)
      : { rawAmount: '0', decimals: mintMetadata?.decimals ?? null };
    const workerWallets = Array.isArray(order.workerWallets)
      ? await Promise.all(order.workerWallets.map(async (wallet) => {
        if (!wallet?.address) {
          return normalizeFomoWorkerWallet(wallet);
        }

        const workerLamports = await getWalletBalance(wallet.address);
        const workerTokenSnapshot = order.mintAddress
          ? await getWalletTokenSnapshot(wallet.address, order.mintAddress)
          : { rawAmount: '0', decimals: mintMetadata?.decimals ?? null };

        return normalizeFomoWorkerWallet({
          ...wallet,
          currentLamports: workerLamports,
          currentSol: formatSolAmountFromLamports(workerLamports),
          currentTokenAmountRaw: workerTokenSnapshot.rawAmount,
          currentTokenAmountDisplay: formatTokenAmountFromRaw(
            workerTokenSnapshot.rawAmount,
            workerTokenSnapshot.decimals ?? mintMetadata?.decimals ?? order.tokenDecimals ?? 0,
          ),
        });
      }))
      : [];
    const totalManagedLamports = balanceLamports + workerWallets.reduce(
      (sum, wallet) => sum + (Number.isInteger(wallet.currentLamports) ? wallet.currentLamports : 0),
      0,
    );

    return updateUserState(userId, (draft) => {
      draft.fomoBooster = normalizeFomoBooster({
        ...draft.fomoBooster,
        tokenDecimals: mintMetadata?.decimals ?? draft.fomoBooster.tokenDecimals,
        tokenProgram: mintMetadata?.tokenProgram ?? draft.fomoBooster.tokenProgram,
        currentLamports: balanceLamports,
        currentSol: formatSolAmountFromLamports(balanceLamports),
        currentTokenAmountRaw: tokenSnapshot.rawAmount,
        currentTokenAmountDisplay: formatTokenAmountFromRaw(
          tokenSnapshot.rawAmount,
          tokenSnapshot.decimals ?? mintMetadata?.decimals ?? draft.fomoBooster.tokenDecimals ?? 0,
        ),
        totalManagedLamports,
        workerWallets,
        lastBalanceCheckAt: new Date().toISOString(),
        lastError: null,
      });
      return draft;
    });
  } catch (error) {
    return updateUserState(userId, (draft) => {
      draft.fomoBooster = normalizeFomoBooster({
        ...draft.fomoBooster,
        lastBalanceCheckAt: new Date().toISOString(),
        lastError: String(error.message || error),
      });
      return draft;
    });
  }
}

async function refreshSniperWizard(userId) {
  const user = await getUserState(userId);
  const order = user.sniperWizard;
  if (!order?.walletAddress) {
    return user;
  }

  try {
    const balanceLamports = await getWalletBalance(order.walletAddress);
    const workerWallets = Array.isArray(order.workerWallets)
      ? await Promise.all(order.workerWallets.map(async (wallet) => {
        if (!wallet.address) {
          return normalizeLaunchBuyBuyerWallet(wallet);
        }
        const lamports = await getWalletBalance(wallet.address).catch(() => wallet.currentLamports || 0);
        return normalizeLaunchBuyBuyerWallet({
          ...wallet,
          currentLamports: lamports,
          currentSol: formatSolAmountFromLamports(lamports),
        });
      }))
      : [];
    const totalManagedLamports = balanceLamports + workerWallets.reduce(
      (sum, wallet) => sum + (Number.isInteger(wallet.currentLamports) ? wallet.currentLamports : 0),
      0,
    );
    const estimates = estimateSniperWizardFees(balanceLamports, order.walletCount, order.sniperMode || 'standard');
    return updateUserState(userId, (draft) => {
      draft.sniperWizard = normalizeSniperWizard({
        ...draft.sniperWizard,
        currentLamports: balanceLamports,
        currentSol: formatSolAmountFromLamports(balanceLamports),
        totalManagedLamports,
        workerWallets,
        estimatedPlatformFeeLamports: estimates.platformFeeLamports,
        estimatedSplitNowFeeLamports: estimates.splitNowFeeLamports,
        estimatedNetSplitLamports: estimates.netSplitLamports,
        lastBalanceCheckAt: new Date().toISOString(),
        lastError: null,
      });
      syncSniperWizardTradingDesk(draft);
      return draft;
    });
  } catch (error) {
    return updateUserState(userId, (draft) => {
      draft.sniperWizard = normalizeSniperWizard({
        ...draft.sniperWizard,
        lastBalanceCheckAt: new Date().toISOString(),
        lastError: String(error.message || error),
      });
      syncSniperWizardTradingDesk(draft);
      return draft;
    });
  }
}

function makeXFollowersKeyboard(user) {
  const keyboard = new InlineKeyboard();
  const selectedKey = user.xFollowers?.packageKey || null;

  const packageRows = [
    ['non_drop_100', 'non_drop_500'],
    ['non_drop_1000'],
    ['low_drop_100', 'low_drop_500'],
    ['low_drop_1000'],
  ];

  for (const row of packageRows) {
    row.forEach((key) => {
      const pkg = getXFollowersPackage(key);
      if (!pkg) return;
      const icon = pkg.type === 'non_drop' ? '\u{1F451}' : '\u267B\uFE0F';
      const label = selectedKey === key
        ? `\u2705 ${icon} ${pkg.followers} â€¢ $${pkg.usdPrice.toFixed(2)}`
        : `${icon} ${pkg.followers} â€¢ $${pkg.usdPrice.toFixed(2)}`;
      keyboard.text(label, `xfollowers:package:${key}`);
    });
    keyboard.row();
  }

  keyboard.text(
    user.xFollowers?.target ? '\u270D\uFE0F Change X Link' : '\u270D\uFE0F Set X Link',
    'xfollowers:set:target',
  );
  keyboard.row();
  keyboard.text('\u2139\uFE0F Help', 'nav:help_x_followers');
  keyboard.row();
  if (user.xFollowers?.packageKey && user.xFollowers?.target) {
    keyboard.text('\u{1F4B8} New Quote', 'xfollowers:payment:refresh');
    keyboard.text('\u{1F9FE} Check Payment', 'xfollowers:payment:check');
    keyboard.row();
  }
  keyboard.text('\u2B05\uFE0F Back', 'nav:home');
  keyboard.text('\u{1F3E0} Home', 'nav:home');
  keyboard.row();
  keyboard.text('\u{1F504} Refresh', 'refresh:x_followers');
  return keyboard;
}

function xFollowersText(user) {
  const state = user.xFollowers;
  const pkg = getXFollowersPackage(state.packageKey);
  const payment = state.payment || createDefaultPaymentState();
  const lines = [
    '\u{1F465} *X Followers*',
    '',
    'Choose a follower package, send the X profile link, then complete checkout when you are ready.',
    '',
    MENU_DIVIDER,
    '\u2728 *Packages*',
    '- Non-drop: 100 / 500 / 1000',
    '- Low-drop with 30-day refill: 100 / 500 / 1000',
    '',
    '\u26A0\uFE0F *Important*',
    '- Delivery may not be instant',
    `- Always message support at @${SUPPORT_USERNAME} to confirm timing and availability after payment`,
    '- This is a manual fulfillment service, not an instant bot drop',
    '',
    '\u{1F4CB} *Current Selection*',
    `- Package: *${xFollowersPackageLabel(pkg)}*`,
    `- Status: *${xFollowersStatusLabel(user)}*`,
    `- Target: ${state.target ? `\`${state.target}\`` : '*Not set*'}`,
  ];

  if (pkg) {
    lines.push(`- Price: *$${pkg.usdPrice.toFixed(2)}*`);
    lines.push(`- Estimated provider cost: *$${pkg.providerCostUsd.toFixed(2)}*`);
    lines.push(`- Estimated platform profit: *$${(pkg.usdPrice - pkg.providerCostUsd).toFixed(2)}*`);
    lines.push(`- Estimated treasury share: *$${(((pkg.usdPrice - pkg.providerCostUsd) / 2)).toFixed(2)}*`);
    lines.push(`- Estimated burn share: *$${(((pkg.usdPrice - pkg.providerCostUsd) / 2)).toFixed(2)}*`);
  }

  if (xFollowersHasActiveQuote(user)) {
    lines.push('');
    lines.push('\u{1F4B3} *Checkout*');
    lines.push(`- Send exactly: \`${payment.solAmount} SOL\``);
    lines.push(`- Receive wallet: \`${payment.address}\``);
    lines.push(`- Quote created: ${formatTimestamp(payment.quoteCreatedAt)}`);
    lines.push(`- Quote expires: ${formatTimestamp(payment.quoteExpiresAt)}`);
    if (payment.matchedSignature) {
      lines.push(`- Matched tx: \`${payment.matchedSignature}\``);
    }
    if (payment.lastError) {
      lines.push(`- Last error: \`${payment.lastError}\``);
    }
  }

  if (state.lastError) {
    lines.push('');
    lines.push(`Last error: \`${state.lastError}\``);
  }

  if (state.awaitingField === 'target') {
    lines.push('');
    lines.push('Send the full X profile link or @handle in your next message.');
  }

  return lines.join('\n');
}

function stakingStatusLabel(state) {
  if ((state.claimableLamports || 0) >= state.claimThresholdLamports) {
    return 'Claim Ready';
  }
  if (state.walletAddress) {
    return 'Tracking';
  }
  return 'Setup';
}

function makeStakingKeyboard(user) {
  const state = normalizeStakingState(user.staking);
  const keyboard = new InlineKeyboard();

  keyboard.text(
    state.walletAddress ? '\u2705 Use Active Buy / Sell Wallet' : '\u{1F517} Link Active Buy / Sell Wallet',
    'staking:link_active_wallet',
  );
  keyboard.row();
  keyboard.text('\u{1F4B8} Claim Rewards', 'staking:claim');
  keyboard.text('\u23F3 Request Unstake', 'staking:request_unstake');
  keyboard.row();
  keyboard.text('\u{1F9E0} How Rewards Build', 'staking:explain_epoch');
  keyboard.text('\u2139\uFE0F Help', 'nav:help_staking');
  keyboard.row();
  keyboard.text('\u2B05\uFE0F Back', 'nav:home');
  keyboard.text('\u{1F3E0} Home', 'nav:home');
  keyboard.row();
  keyboard.text('\u{1F504} Refresh', 'refresh:staking');
  return keyboard;
}

function stakingText(user) {
  const state = normalizeStakingState(user.staking);
  const activeWallet = getActiveTradingWallet(user);
  const lines = [
    '\u{1F4B0} *Staking*',
    '',
    'Hold *WIZARD TOOLZ*, let time do the work, and manually claim *SOL* rewards from this bot when your balance is ready.',
    '',
    MENU_DIVIDER,
    '\u2728 *What This Product Will Do*',
    '- Link the wallet you want the bot to track for WIZARD TOOLZ rewards',
    '- Rewards are paid in *SOL* from platform fees and creator rewards',
    '- Claims are *manual* from Telegram so you stay in control',
    `- Claims only unlock once at least *${formatSolAmountFromLamports(state.claimThresholdLamports)} SOL* is claimable`,
    `- Rewards start building immediately, but the first *${formatDayCountLabel(STAKING_EARLY_WEIGHT_DAYS)}* count at a lighter weight`,
    `- Normal unstake uses a *${formatDayCountLabel(STAKING_UNSTAKE_COOLDOWN_DAYS)}* cooldown in the hard-staking flow`,
    '',
    '\u{1F4CB} *Current Setup*',
    `- Status: *${stakingStatusLabel(state)}*`,
    `- Rewards asset: *${state.rewardsAsset}*`,
    `- Manual claim only: *${state.manualClaimOnly ? 'Yes' : 'No'}*`,
    `- Linked wallet: ${state.walletAddress ? `\`${state.walletAddress}\`` : '*Not linked yet*'}`,
    `- Active Buy / Sell wallet: ${activeWallet?.address ? `\`${activeWallet.address}\`` : '*None selected*'}`,
    `- Total tracked: *${state.totalStakedDisplay} WIZARD TOOLZ*`,
    `- Rewards weight: *${state.currentWeightLabel}*`,
    `- Claimable now: *${formatSolAmountFromLamports(state.claimableLamports)} SOL*`,
    `- Lifetime claimed: *${formatSolAmountFromLamports(state.totalClaimedLamports)} SOL*`,
  ];

  if (state.trackingStartedAt) {
    lines.push(`- Tracking since: ${formatTimestamp(state.trackingStartedAt)}`);
  }
  if (state.lastBalanceSyncedAt) {
    lines.push(`- Last balance sync: ${formatTimestamp(state.lastBalanceSyncedAt)}`);
  }
  if (state.lastRewardsAllocatedAt) {
    lines.push(`- Last rewards update: ${formatTimestamp(state.lastRewardsAllocatedAt)}`);
  }

  if (state.lastClaimedAt) {
    lines.push(`- Last manual claim: ${formatTimestamp(state.lastClaimedAt)} (${formatSolAmountFromLamports(state.lastClaimedLamports)} SOL)`);
    if (state.lastClaimSignature) {
      lines.push(`- Last claim tx: \`${state.lastClaimSignature}\``);
    }
  }

  lines.push('');
  lines.push('\u{1F9E0} *Simple Version*');
  lines.push('- Bigger stake + longer time = bigger share of the SOL reward pool');
  lines.push('- If your claimable amount is under the minimum, it waits and keeps building');
  lines.push('- Nothing auto-claims behind your back');

  if (!state.walletAddress && activeWallet?.address) {
    lines.push('');
    lines.push('Tip: tap *Link Active Buy / Sell Wallet* to use the wallet you already selected in the trading desk.');
  }

  if (state.lastError) {
    lines.push('');
    lines.push(`Last note: \`${state.lastError}\``);
  }

  return lines.join('\n');
}

function makeVanityWalletKeyboard(user) {
  const state = normalizeVanityWalletState(user.vanityWallet);
  const keyboard = new InlineKeyboard();
  keyboard.text(
    state.patternMode === 'prefix' ? '\u2705 Starts With' : 'Starts With',
    'vanity:set:mode:prefix',
  );
  keyboard.text(
    state.patternMode === 'suffix' ? '\u2705 Ends With' : 'Ends With',
    'vanity:set:mode:suffix',
  );
  keyboard.row();
  keyboard.text(
    state.pattern ? '\u270D\uFE0F Change Pattern' : '\u270D\uFE0F Set Pattern',
    'vanity:set:pattern',
  );
  keyboard.row();
  if (state.patternMode && state.pattern) {
    keyboard.text('\u{1F4B8} New Quote', 'vanity:payment:refresh');
    keyboard.text('\u{1F9FE} Check Payment', 'vanity:payment:check');
    keyboard.row();
  }
  if (state.generatedAddress) {
    keyboard.text(
      state.privateKeyVisible ? '\u{1F648} Hide Key' : '\u{1F441}\uFE0F Show Key',
      'vanity:key:toggle',
    );
    keyboard.text('\u{1F4B1} Add To Buy / Sell', 'vanity:add_to_trading');
    keyboard.row();
  }
  keyboard.text('\u2139\uFE0F Help', 'nav:help_vanity_wallet');
  keyboard.row();
  keyboard.text('\u2B05\uFE0F Back', 'nav:home');
  keyboard.text('\u{1F3E0} Home', 'nav:home');
  keyboard.row();
  keyboard.text('\u{1F504} Refresh', 'refresh:vanity_wallet');
  return keyboard;
}

function vanityWalletText(user) {
  const state = normalizeVanityWalletState(user.vanityWallet);
  const payment = state.payment || createDefaultPaymentState();
  const lines = [
    '\u2728 *Vanity Wallet*',
    '',
    'Generate a fresh Solana wallet with a short custom start or end pattern.',
    '',
    MENU_DIVIDER,
    '\u2728 *What This Tool Does*',
    '- Choose whether the address should start with or end with your pattern',
    '- Example: start with `WIZ` or end with `TOOL`',
    `- Shared Render generation is capped at *${VANITY_WALLET_MAX_PATTERN_LENGTH} characters* so it stays reliable`,
    '- Pay the fixed service fee shown below',
    '- Once payment is matched, the bot brute-forces the wallet in the background and delivers the key here',
    '- The private key stays hidden by default until you reveal it inside your own chat',
    '',
    '\u{1F4CB} *Current Setup*',
    `- Status: *${vanityWalletStatusLabel(user)}*`,
    `- Match type: *${state.patternMode === 'suffix' ? 'Ends with' : (state.patternMode === 'prefix' ? 'Starts with' : 'Not set')}*`,
    `- Pattern: ${state.pattern ? `\`${state.pattern}\`` : '*Not set*'}`,
    '',
    '\u{1F4B0} *Fee Split*',
    `- Service fee: *${formatSolAmountFromLamports(VANITY_WALLET_SERVICE_FEE_LAMPORTS)} SOL*`,
    `- Treasury share: *${formatSolAmountFromLamports(state.estimatedTreasuryShareLamports)} SOL*`,
    `- Burn-side share: *${formatSolAmountFromLamports(state.estimatedBurnShareLamports)} SOL*`,
    '',
  ];

  if (vanityWalletHasActiveQuote(user)) {
    lines.push('\u{1F4B3} *Checkout*');
    lines.push(`- Send exactly: \`${payment.solAmount} SOL\``);
    lines.push(`- Receive wallet: \`${payment.address}\``);
    lines.push(`- Quote created: ${formatTimestamp(payment.quoteCreatedAt)}`);
    lines.push(`- Quote expires: ${formatTimestamp(payment.quoteExpiresAt)}`);
    lines.push('- Note: the exact checkout amount includes a tiny matcher tag so the bot can auto-confirm payment.');
    if (payment.matchedSignature) {
      lines.push(`- Matched tx: \`${payment.matchedSignature}\``);
    }
    if (payment.lastError) {
      lines.push(`- Last payment note: \`${payment.lastError}\``);
    }
    lines.push('');
  }

  if (state.generationStartedAt || state.attemptCount > 0 || state.generatedAddress) {
    lines.push('\u{1F9EA} *Generation*');
    if (state.generationStartedAt) {
      lines.push(`- Started: ${formatTimestamp(state.generationStartedAt)}`);
    }
    lines.push(`- Attempts: *${Number(state.attemptCount || 0).toLocaleString('en-US')}*`);
    if (state.generatedAddress) {
      lines.push(`- Wallet address: \`${state.generatedAddress}\``);
      lines.push(`- Stored private key: ${vanityWalletPrivateKeyText(state)}`);
      if (state.completedAt) {
        lines.push(`- Completed: ${formatTimestamp(state.completedAt)}`);
      }
    }
    lines.push('');
  }

  if (state.awaitingField === 'pattern') {
    lines.push('Send the vanity pattern in your next message.');
  } else if (state.status === 'completed') {
    lines.push('Your vanity wallet is ready. You can reveal the key or add it straight into Buy / Sell.');
  } else if (state.status === 'generating') {
    lines.push('Generation is running now. Refresh this screen in a bit to watch the progress.');
  } else {
    lines.push('Choose Starts With or Ends With, set the pattern, then create a quote when you are ready.');
  }

  if (state.lastError) {
    lines.push('');
    lines.push(`Last note: \`${state.lastError}\``);
  }

  return lines.join('\n');
}

function makeEngagementKeyboard() {
  return new InlineKeyboard()
    .text('\u{1F426} X', `https://t.me/${SUPPORT_USERNAME}`)
    .text('\u{1F4D8} Facebook', `https://t.me/${SUPPORT_USERNAME}`)
    .row()
    .text('\u{1F4F0} Reddit', `https://t.me/${SUPPORT_USERNAME}`)
    .text('\u{1F4AC} Telegram', `https://t.me/${SUPPORT_USERNAME}`)
    .row()
    .text('\u{1F579}\uFE0F Discord', `https://t.me/${SUPPORT_USERNAME}`)
    .text('\u{1F3B5} TikTok', `https://t.me/${SUPPORT_USERNAME}`)
    .row()
    .url('\u{1F91D} Contact Support', `https://t.me/${SUPPORT_USERNAME}`)
    .row()
    .text('\u2139\uFE0F Help', 'nav:help_engagement')
    .row()
    .text('\u2B05\uFE0F Back', 'nav:home')
    .text('\u{1F3E0} Home', 'nav:home')
    .row()
    .text('\u{1F504} Refresh', 'refresh:engagement');
}

function engagementText() {
  return [
    '\u{1F4E3} *Engagement*',
    '',
    'Need platform-specific growth or traffic services? Choose the platform below and contact support.',
    '',
    MENU_DIVIDER,
    '\u2728 *Available Platforms*',
    '- X',
    '- Facebook',
    '- Reddit',
    '- Telegram',
    '- Discord',
    '- TikTok',
    '',
    '\u{1F4A1} *What We Can Help With*',
    '- Views',
    '- Clicks',
    '- Likes',
    '- Reposts',
    '- Other platform-specific engagement types',
    '',
      `Message support at @${SUPPORT_USERNAME} with the platform, link, and what kind of engagement you want.`,
  ].join('\n');
}

function makeSubscriptionsAccountsKeyboard() {
  return new InlineKeyboard()
    .text('\u2728 Subscriptions', 'nav:subscriptions_catalog')
    .text('\u{1F511} Accounts', 'nav:accounts_catalog')
    .row()
    .url('\u{1F91D} Contact Support', `https://t.me/${SUPPORT_USERNAME}`)
    .row()
    .text('\u2139\uFE0F Help', 'nav:help_subscriptions_accounts')
    .row()
    .text('\u2B05\uFE0F Back', 'nav:home')
    .text('\u{1F3E0} Home', 'nav:home')
    .row()
    .text('\u{1F504} Refresh', 'refresh:subscriptions_accounts');
}

function subscriptionsAccountsText() {
  return [
    '\u{1F4BC} *Subscriptions + Accounts*',
    '',
    'Need premium subscriptions, social accounts, or related digital services? Choose the category below and message support for pricing and availability.',
    '',
    MENU_DIVIDER,
    '\u2728 *Subscriptions*',
    '- ChatGPT',
    '- Claude',
    '- Perplexity',
    '- ElevenLabs',
    '- DeepL',
    '- Canva',
    '- Adobe / Adobe Express',
    '',
    '\u{1F511} *Accounts*',
    '- Instagram, TikTok, X, Telegram, Reddit, Picsart',
    '',
      `Message support at @${SUPPORT_USERNAME} for current pricing, stock, and delivery timing.`,
  ].join('\n');
}

function makeSubscriptionsCatalogKeyboard() {
  return new InlineKeyboard()
    .url('\u{1F916} ChatGPT', `https://t.me/${SUPPORT_USERNAME}`)
    .url('\u{1F9E0} Claude', `https://t.me/${SUPPORT_USERNAME}`)
    .row()
    .url('\u{1F50D} Perplexity', `https://t.me/${SUPPORT_USERNAME}`)
    .url('\u{1F3A4} ElevenLabs', `https://t.me/${SUPPORT_USERNAME}`)
    .row()
    .url('\u{1F310} DeepL', `https://t.me/${SUPPORT_USERNAME}`)
    .url('\u{1F58C}\uFE0F Canva', `https://t.me/${SUPPORT_USERNAME}`)
    .row()
    .url('\u{1F3A8} Adobe / Express', `https://t.me/${SUPPORT_USERNAME}`)
    .row()
    .url('\u{1F91D} Contact Support', `https://t.me/${SUPPORT_USERNAME}`)
    .row()
    .text('\u2B05\uFE0F Back', 'nav:subscriptions_accounts')
    .text('\u{1F3E0} Home', 'nav:home')
    .row()
    .text('\u{1F504} Refresh', 'refresh:subscriptions_catalog');
}

function subscriptionsCatalogText() {
  return [
    '\u2728 *Subscriptions*',
    '',
    'Choose the subscription you want, then message support for pricing, stock, and setup.',
    '',
    MENU_DIVIDER,
    '- ChatGPT',
    '- Claude',
    '- Perplexity',
    '- ElevenLabs',
    '- DeepL',
    '- Canva',
    '- Adobe / Adobe Express',
    '',
    `All subscription pricing is handled manually through @${SUPPORT_USERNAME}.`,
  ].join('\n');
}

function makeAccountsCatalogKeyboard() {
  return new InlineKeyboard()
    .url('\u{1F4F8} Instagram', `https://t.me/${SUPPORT_USERNAME}`)
    .url('\u{1F3B5} TikTok', `https://t.me/${SUPPORT_USERNAME}`)
    .row()
    .url('\u{1F426} X', `https://t.me/${SUPPORT_USERNAME}`)
    .url('\u{1F4AC} Telegram', `https://t.me/${SUPPORT_USERNAME}`)
    .row()
    .url('\u{1F4F0} Reddit', `https://t.me/${SUPPORT_USERNAME}`)
    .url('\u{1F5BC}\uFE0F Picsart', `https://t.me/${SUPPORT_USERNAME}`)
    .row()
    .url('\u{1F91D} Contact Support', `https://t.me/${SUPPORT_USERNAME}`)
    .row()
    .text('\u2B05\uFE0F Back', 'nav:subscriptions_accounts')
    .text('\u{1F3E0} Home', 'nav:home')
    .row()
    .text('\u{1F504} Refresh', 'refresh:accounts_catalog');
}

function accountsCatalogText() {
  return [
    '\u{1F511} *Accounts*',
    '',
    'Choose the account service you want, then message support for pricing and availability.',
    '',
    MENU_DIVIDER,
    '- Instagram',
    '- TikTok',
    '- X',
    '- Telegram',
    '- Reddit',
    '- Picsart',
    '',
    `All account inquiries are handled manually through @${SUPPORT_USERNAME}.`,
  ].join('\n');
}

function normalizeOptionalLaunchUrl(value, label) {
  const raw = String(value || '').trim();
  if (!raw || raw.toLowerCase() === 'none') {
    return null;
  }

  let parsed;
  try {
    parsed = new URL(raw);
  } catch {
    throw new Error(`${label} must be a valid full URL or \`none\`.`);
  }

  if (!['http:', 'https:'].includes(parsed.protocol)) {
    throw new Error(`${label} must start with http:// or https://`);
  }

  return parsed.toString();
}

function normalizeOptionalLaunchSocial(value, label, platform) {
  const raw = String(value || '').trim();
  if (!raw || raw.toLowerCase() === 'none') {
    return null;
  }

  const normalized = platform === 'telegram'
    ? raw.replace(/^https?:\/\/t\.me\//i, '').replace(/^@/, '')
    : raw.replace(/^https?:\/\/(www\.)?x\.com\//i, '').replace(/^@/, '');

  if (!normalized || /\s/.test(normalized)) {
    throw new Error(`${label} must be a normal ${platform === 'telegram' ? 'Telegram' : 'X'} link, @handle, or \`none\`.`);
  }

  if (platform === 'telegram') {
    return `https://t.me/${normalized}`;
  }

  return `https://x.com/${normalized}`;
}

async function saveLaunchBuyLogoBuffer(orderId, incoming, inputBuffer) {
  await fs.mkdir(LAUNCH_BUY_ASSETS_DIR, { recursive: true });
  const safeStem = String(orderId || `launch-${Date.now()}`).replace(/[^a-zA-Z0-9_-]/g, '').slice(0, 64) || `launch-${Date.now()}`;
  const fileName = `${safeStem}.png`;
  const absolutePath = path.join(LAUNCH_BUY_ASSETS_DIR, fileName);
  const outputBuffer = await sharp(inputBuffer, { failOn: 'none', animated: false })
    .rotate()
    .resize(1024, 1024, {
      fit: 'contain',
      background: { r: 0, g: 0, b: 0, alpha: 0 },
      withoutEnlargement: false,
    })
    .png({ compressionLevel: 9, adaptiveFiltering: true })
    .toBuffer();

  await fs.writeFile(absolutePath, outputBuffer);

  return {
    absolutePath,
    fileName: incoming?.filename ? `${path.parse(incoming.filename).name}.png` : fileName,
  };
}

async function refreshLaunchBuy(userId, launchBuyId = null) {
  const user = await getUserState(userId);
  const targetLaunchBuyId = launchBuyId || user.activeLaunchBuyId;
  const order = user.launchBuys.find((item) => item.id === targetLaunchBuyId) ?? user.launchBuy;
  if (!order?.walletAddress) {
    return user;
  }

  try {
    const balanceLamports = await getWalletBalance(order.walletAddress);
    const buyerWallets = Array.isArray(order.buyerWallets)
      ? await Promise.all(order.buyerWallets.map(async (wallet) => {
        if (!wallet?.address) {
          return normalizeLaunchBuyBuyerWallet(wallet);
        }

        try {
          const walletLamports = await getWalletBalance(wallet.address);
          return normalizeLaunchBuyBuyerWallet({
            ...wallet,
            currentLamports: walletLamports,
            currentSol: formatSolAmountFromLamports(walletLamports),
          });
        } catch {
          return normalizeLaunchBuyBuyerWallet(wallet);
        }
      }))
      : [];

    return updateUserState(userId, (draft) => {
      updateLaunchBuyInDraft(draft, targetLaunchBuyId, (current) => ({
        ...current,
        currentLamports: balanceLamports,
        currentSol: formatSolAmountFromLamports(balanceLamports),
        buyerWallets,
        fundedReady: balanceLamports >= (current.estimatedTotalNeededLamports || 0),
        lastBalanceCheckAt: new Date().toISOString(),
        lastError: null,
      }));
      return draft;
    });
  } catch (error) {
    return updateUserState(userId, (draft) => {
      updateLaunchBuyInDraft(draft, targetLaunchBuyId, (current) => ({
        ...current,
        lastBalanceCheckAt: new Date().toISOString(),
        lastError: String(error.message || error),
      }));
      return draft;
    });
  }
}

async function downloadTelegramFileBuffer(fileId) {
  const telegramFile = await bot.api.getFile(fileId);
  if (!telegramFile?.file_path) {
    throw new Error('Telegram did not return a usable image file.');
  }

  const response = await fetch(`https://api.telegram.org/file/bot${cfg.telegramToken}/${telegramFile.file_path}`);
  if (!response.ok) {
    throw new Error(`Telegram image download failed with status ${response.status}.`);
  }

  return Buffer.from(await response.arrayBuffer());
}

function getIncomingResizerImage(ctx) {
  const photo = Array.isArray(ctx.message?.photo) && ctx.message.photo.length > 0
    ? ctx.message.photo[ctx.message.photo.length - 1]
    : null;
  if (photo?.file_id) {
    return {
      fileId: photo.file_id,
      filename: 'telegram-photo.jpg',
      mimeType: 'image/jpeg',
    };
  }

  const document = ctx.message?.document;
  if (document?.file_id && String(document.mime_type || '').startsWith('image/')) {
    return {
      fileId: document.file_id,
      filename: document.file_name || 'uploaded-image',
      mimeType: document.mime_type || 'image/png',
    };
  }

  return null;
}

async function processLaunchBuyLogoUpload(ctx) {
  const userId = String(ctx.from.id);
  const user = await getUserState(userId);
  const order = user.launchBuy;
  if (order?.awaitingField !== 'logo') {
    return false;
  }

  const incoming = getIncomingResizerImage(ctx);
  if (!incoming) {
    return false;
  }

  try {
    const inputBuffer = await downloadTelegramFileBuffer(incoming.fileId);
    const savedLogo = await saveLaunchBuyLogoBuffer(order.id, incoming, inputBuffer);
    const updated = await updateUserState(userId, (draft) => {
      updateLaunchBuyInDraft(draft, order.id, (current) => ({
        ...current,
        logoPath: savedLogo.absolutePath,
        logoFileName: savedLogo.fileName,
        logoUploadedAt: new Date().toISOString(),
        awaitingField: null,
        lastError: null,
        updatedAt: new Date().toISOString(),
      }));
      return draft;
    });

    await appendUserActivityLog(userId, {
      scope: launchBuyScope(order.id),
      level: 'info',
      message: 'Launch + Buy logo uploaded.',
    });

    await ctx.reply(
      [
        '*Launch + Buy Updated*',
        '',
        'Logo uploaded successfully.',
        '',
        launchBuyEditorText(updated),
      ].join('\n'),
      {
        parse_mode: 'Markdown',
        reply_markup: makeLaunchBuyEditorKeyboard(updated),
      },
    );
  } catch (error) {
    await ctx.reply(
      [
        `âš ï¸ ${String(error.message || error)}`,
        '',
        promptForLaunchBuyField('logo', order),
      ].join('\n'),
      {
        parse_mode: 'Markdown',
        reply_markup: makeLaunchBuyEditorKeyboard(user),
      },
    );
  }

  return true;
}

async function createResizedImageBuffer(inputBuffer, mode) {
  const preset = getResizerPreset(mode);
  if (!preset) {
    throw new Error('Choose Logo or Banner first.');
  }

  const source = sharp(inputBuffer, { failOn: 'none', animated: false }).rotate();
  const metadata = await source.metadata();
  if (!metadata.width || !metadata.height) {
    throw new Error('That file could not be read as an image.');
  }

  if (mode === 'logo') {
    return source
      .resize(preset.width, preset.height, {
        fit: 'contain',
        background: { r: 0, g: 0, b: 0, alpha: 0 },
        withoutEnlargement: false,
      })
      .png({ compressionLevel: 9, adaptiveFiltering: true })
      .toBuffer();
  }

  const stats = await source.clone().stats();
  const backgroundColor = {
    r: Math.max(0, Math.min(255, Math.round(stats.channels?.[0]?.mean ?? 24))),
    g: Math.max(0, Math.min(255, Math.round(stats.channels?.[1]?.mean ?? 24))),
    b: Math.max(0, Math.min(255, Math.round(stats.channels?.[2]?.mean ?? 24))),
    alpha: 1,
  };

  const background = await source
    .clone()
    .flatten({ background: backgroundColor })
    .resize(preset.width, preset.height, {
      fit: 'cover',
      position: 'attention',
      withoutEnlargement: false,
    })
    .blur(18)
    .modulate({ brightness: 1.02, saturation: 1.05 })
    .png()
    .toBuffer();

  const foreground = await source
    .clone()
    .resize(Math.round(preset.width * 0.94), Math.round(preset.height * 0.94), {
      fit: 'contain',
      background: { r: 0, g: 0, b: 0, alpha: 0 },
      withoutEnlargement: false,
    })
    .png()
    .toBuffer();

  return sharp(background)
    .composite([{ input: foreground, gravity: 'center' }])
    .png({ compressionLevel: 9, adaptiveFiltering: true })
    .toBuffer();
}

async function processResizerUpload(ctx) {
  const userId = String(ctx.from.id);
  const user = await getUserState(userId);
  const resizer = normalizeResizer(user.resizer);
  const preset = getResizerPreset(resizer.mode);

  if (!preset || !resizer.awaitingImage) {
    return false;
  }

  const incoming = getIncomingResizerImage(ctx);
  if (!incoming) {
    return false;
  }

  try {
    const inputBuffer = await downloadTelegramFileBuffer(incoming.fileId);
    const outputBuffer = await createResizedImageBuffer(inputBuffer, preset.key);
    const updated = await updateUserState(userId, (draft) => {
      draft.resizer = normalizeResizer({
        ...draft.resizer,
        mode: preset.key,
        awaitingImage: true,
        status: 'ready',
        lastCompletedAt: new Date().toISOString(),
        lastOutputWidth: preset.width,
        lastOutputHeight: preset.height,
        lastSourceName: incoming.filename,
        lastError: null,
      });
      return draft;
    });

    await ctx.replyWithDocument(new InputFile(outputBuffer, preset.filename), {
      caption: [
        `ðŸ–¼ï¸ ${preset.label} ready`,
        `${preset.width} x ${preset.height} PNG`,
        'You can send another image anytime with the same format, or switch formats below.',
      ].join('\n'),
    });

    await ctx.reply(resizerEditorText(updated), {
      parse_mode: 'Markdown',
      reply_markup: makeResizerKeyboard(updated),
    });
  } catch (error) {
    const updated = await updateUserState(userId, (draft) => {
      draft.resizer = normalizeResizer({
        ...draft.resizer,
        lastError: String(error.message || error),
      });
      return draft;
    });

    await ctx.reply(
      [
        `âš ï¸ ${String(error.message || error)}`,
        '',
        resizerEditorText(updated),
      ].join('\n'),
      {
        parse_mode: 'Markdown',
        reply_markup: makeResizerKeyboard(updated),
      },
    );
  }

  return true;
}

async function sendHome(ctx, user) {
  await ensureSolPriceCache();
  await editOrReplyMedia(ctx, 'home', homeText(user), makeHomeKeyboard(user));
}

async function handleSlashRoute(ctx, route, { refreshTrading = false } = {}) {
  const userId = String(ctx.from.id);
  const user = refreshTrading
    ? await refreshTradingDesk(userId)
    : await getUserState(userId);
  await renderScreen(ctx, route, user);
}

async function editOrReply(ctx, text, keyboard) {
  try {
    await ctx.editMessageText(text, {
      parse_mode: 'Markdown',
      reply_markup: keyboard,
    });
  } catch {
    await ctx.reply(text, {
      parse_mode: 'Markdown',
      reply_markup: keyboard,
    });
  }
}

async function editOrReplyMedia(ctx, route, text, keyboard) {
  const mediaPath = getScreenMediaPath(route);

  try {
    await ctx.editMessageMedia(
      InputMediaBuilder.photo(new InputFile(mediaPath), {
        caption: text,
        parse_mode: 'Markdown',
      }),
      {
        reply_markup: keyboard,
      },
    );
    return;
  } catch (error) {
    if (isMessageNotModifiedError(error)) {
      return;
    }

    try {
      await ctx.replyWithPhoto(new InputFile(mediaPath), {
        caption: text,
        parse_mode: 'Markdown',
        reply_markup: keyboard,
      });
      return;
    } catch (replyError) {
      if (isMessageNotModifiedError(replyError)) {
        return;
      }

      await editOrReply(ctx, text, keyboard);
    }
  }
}

async function renderScreen(ctx, route, user) {
  if (['home', 'amount', 'payment', 'confirm', 'status', 'help', 'help_reaction', 'help_volume', 'help_buy_sell', 'help_burn_agent', 'help_holder_booster', 'help_fomo_booster', 'help_magic_sell', 'help_magic_bundle', 'help_launch_buy', 'help_sniper_wizard', 'help_staking', 'help_vanity_wallet', 'help_community_vision', 'help_wallet_tracker', 'help_x_followers', 'help_engagement', 'help_subscriptions_accounts', 'help_resizer', 'buy_sell', 'buy_sell_wallets', 'buy_sell_quick', 'buy_sell_limit', 'buy_sell_copy', 'volume', 'volume_organic', 'volume_archive', 'volume_bundled', 'volume_order', 'burn_agent', 'burn_agent_archive', 'burn_agent_editor', 'holder_booster', 'fomo_booster', 'magic_bundle', 'magic_bundle_archive', 'magic_bundle_editor', 'magic_sell', 'magic_sell_archive', 'magic_sell_editor', 'launch_buy', 'launch_buy_archive', 'launch_buy_editor', 'sniper_wizard', 'staking', 'vanity_wallet', 'community_vision', 'community_vision_archive', 'community_vision_editor', 'wallet_tracker', 'wallet_tracker_archive', 'wallet_tracker_editor', 'x_followers', 'engagement', 'subscriptions_accounts', 'subscriptions_catalog', 'accounts_catalog', 'resizer'].includes(route)) {
    await ensureSolPriceCache();
  }

  switch (route) {
    case 'home':
      await editOrReplyMedia(ctx, 'home', homeText(user), makeHomeKeyboard(user));
      return;
    case 'start':
      await editOrReplyMedia(ctx, 'start', startText(user), makeButtonKeyboard(user.selection.button, user));
      return;
    case 'amount':
      await editOrReplyMedia(ctx, 'amount', amountText(user), makeAmountKeyboard(user.selection.amount, user.freeTrialUsed, user.selection.usingFreeTrial, user));
      return;
    case 'payment':
      await editOrReplyMedia(ctx, 'payment', paymentText(user), makePaymentKeyboard(user));
      return;
    case 'confirm':
      await editOrReplyMedia(ctx, 'confirm', confirmText(user), makeConfirmKeyboard(user));
      return;
    case 'target':
      await editOrReplyMedia(ctx, 'target', targetText(user), makeTargetKeyboard(user));
      return;
    case 'status':
      await editOrReplyMedia(ctx, 'status', statusText(user), makeInfoKeyboard('nav:home', 'status'));
      return;
    case 'help':
      await editOrReplyMedia(ctx, 'help', helpText(), makeHelpHubKeyboard());
      return;
    case 'help_reaction':
      await editOrReplyMedia(ctx, 'help', featureHelpText('reaction'), makeFeatureHelpKeyboard('nav:start', 'help_reaction'));
      return;
    case 'help_volume':
      await editOrReplyMedia(ctx, 'help', featureHelpText('volume'), makeFeatureHelpKeyboard('nav:volume', 'help_volume'));
      return;
    case 'help_buy_sell':
      await editOrReplyMedia(ctx, 'help', featureHelpText('buy_sell'), makeFeatureHelpKeyboard('nav:buy_sell', 'help_buy_sell'));
      return;
    case 'help_burn_agent':
      await editOrReplyMedia(ctx, 'help', featureHelpText('burn_agent'), makeFeatureHelpKeyboard('nav:burn_agent', 'help_burn_agent'));
      return;
    case 'help_holder_booster':
      await editOrReplyMedia(ctx, 'help', featureHelpText('holder_booster'), makeFeatureHelpKeyboard('nav:holder_booster', 'help_holder_booster'));
      return;
    case 'help_fomo_booster':
      await editOrReplyMedia(ctx, 'help', featureHelpText('fomo_booster'), makeFeatureHelpKeyboard('nav:fomo_booster', 'help_fomo_booster'));
      return;
    case 'help_magic_sell':
      await editOrReplyMedia(ctx, 'help', featureHelpText('magic_sell'), makeFeatureHelpKeyboard('nav:magic_sell', 'help_magic_sell'));
      return;
    case 'help_magic_bundle':
      await editOrReplyMedia(ctx, 'help', featureHelpText('magic_bundle'), makeFeatureHelpKeyboard('nav:magic_bundle', 'help_magic_bundle'));
      return;
    case 'help_launch_buy':
      await editOrReplyMedia(ctx, 'help', featureHelpText('launch_buy'), makeFeatureHelpKeyboard('nav:launch_buy', 'help_launch_buy'));
      return;
    case 'help_sniper_wizard':
      await editOrReplyMedia(ctx, 'help', featureHelpText('sniper_wizard'), makeFeatureHelpKeyboard('nav:sniper_wizard', 'help_sniper_wizard'));
      return;
    case 'help_staking':
      await editOrReplyMedia(ctx, 'help', featureHelpText('staking'), makeFeatureHelpKeyboard('nav:staking', 'help_staking'));
      return;
    case 'help_vanity_wallet':
      await editOrReplyMedia(ctx, 'help', featureHelpText('vanity_wallet'), makeFeatureHelpKeyboard('nav:vanity_wallet', 'help_vanity_wallet'));
      return;
    case 'help_community_vision':
      await editOrReplyMedia(ctx, 'help', featureHelpText('community_vision'), makeFeatureHelpKeyboard('nav:community_vision', 'help_community_vision'));
      return;
    case 'help_wallet_tracker':
      await editOrReplyMedia(ctx, 'help', featureHelpText('wallet_tracker'), makeFeatureHelpKeyboard('nav:wallet_tracker', 'help_wallet_tracker'));
      return;
    case 'help_x_followers':
      await editOrReplyMedia(ctx, 'help', featureHelpText('x_followers'), makeFeatureHelpKeyboard('nav:x_followers', 'help_x_followers'));
      return;
    case 'help_engagement':
      await editOrReplyMedia(ctx, 'help', featureHelpText('engagement'), makeFeatureHelpKeyboard('nav:engagement', 'help_engagement'));
      return;
    case 'help_subscriptions_accounts':
      await editOrReplyMedia(ctx, 'help', featureHelpText('subscriptions_accounts'), makeFeatureHelpKeyboard('nav:subscriptions_accounts', 'help_subscriptions_accounts'));
      return;
    case 'help_resizer':
      await editOrReplyMedia(ctx, 'help', featureHelpText('resizer'), makeFeatureHelpKeyboard('nav:resizer', 'help_resizer'));
      return;
    case 'burn_agent':
      await editOrReplyMedia(ctx, 'burn_agent', burnAgentCatalogText(user), makeBurnAgentCatalogKeyboard(user));
      return;
    case 'burn_agent_archive':
      await editOrReplyMedia(ctx, 'burn_agent', burnAgentArchiveText(user), makeBurnAgentArchiveKeyboard(user));
      return;
    case 'burn_agent_editor': {
      const balanceLamports = await getBurnAgentBalanceLamports(user.burnAgent);
      await editOrReplyMedia(
        ctx,
        'burn_agent',
        burnAgentEditorText(user, balanceLamports),
        makeBurnAgentEditorKeyboard(user),
      );
      return;
    }
    case 'holder_booster':
      await editOrReplyMedia(ctx, 'holder_booster', holderBoosterText(user), makeHolderBoosterKeyboard(user));
      return;
    case 'fomo_booster':
      await editOrReplyMedia(ctx, 'fomo_booster', fomoBoosterEditorText(user), makeFomoBoosterEditorKeyboard(user));
      return;
    case 'buy_sell':
      await editOrReplyMedia(ctx, 'buy_sell', buySellText(user), makeBuySellKeyboard(user));
      return;
    case 'buy_sell_wallets':
      await editOrReplyMedia(ctx, 'buy_sell', buySellWalletsText(user), makeBuySellWalletsKeyboard(user));
      return;
    case 'buy_sell_quick':
      await editOrReplyMedia(ctx, 'buy_sell', buySellQuickLiveText(user), makeBuySellQuickLiveKeyboard(user));
      return;
    case 'buy_sell_limit':
      await editOrReplyMedia(ctx, 'buy_sell', buySellLimitLiveText(user), makeBuySellLimitLiveKeyboard(user));
      return;
    case 'buy_sell_copy':
      await editOrReplyMedia(ctx, 'buy_sell', buySellCopyLiveText(user), makeBuySellCopyLiveKeyboard(user));
      return;
    case 'magic_sell':
      await editOrReplyMedia(ctx, 'magic_sell', magicSellCatalogText(user), makeMagicSellCatalogKeyboard(user));
      return;
    case 'magic_sell_archive':
      await editOrReplyMedia(ctx, 'magic_sell', magicSellArchiveText(user), makeMagicSellArchiveKeyboard(user));
      return;
    case 'magic_sell_editor':
      await editOrReplyMedia(ctx, 'magic_sell', magicSellEditorText(user), makeMagicSellEditorKeyboard(user));
      return;
    case 'magic_bundle':
      await editOrReplyMedia(ctx, 'magic_bundle', magicBundleCatalogText(user), makeMagicBundleCatalogKeyboard(user));
      return;
    case 'magic_bundle_archive':
      await editOrReplyMedia(ctx, 'magic_bundle', magicBundleArchiveText(user), makeMagicBundleArchiveKeyboard(user));
      return;
    case 'magic_bundle_editor':
      await editOrReplyMedia(ctx, 'magic_bundle', magicBundleEditorText(user), makeMagicBundleEditorKeyboard(user));
      return;
    case 'launch_buy':
      await editOrReplyMedia(ctx, 'launch_buy', launchBuyCatalogText(user), makeLaunchBuyCatalogKeyboard(user));
      return;
    case 'launch_buy_archive':
      await editOrReplyMedia(ctx, 'launch_buy', launchBuyArchiveText(user), makeLaunchBuyArchiveKeyboard(user));
      return;
    case 'launch_buy_editor':
      await editOrReplyMedia(ctx, 'launch_buy', launchBuyEditorText(user), makeLaunchBuyEditorKeyboard(user));
      return;
    case 'sniper_wizard':
      await editOrReplyMedia(ctx, 'sniper_wizard', sniperWizardEditorText(user), makeSniperWizardKeyboard(user));
      return;
    case 'staking':
      await editOrReplyMedia(ctx, 'staking', stakingText(user), makeStakingKeyboard(user));
      return;
    case 'vanity_wallet':
      await editOrReplyMedia(ctx, 'vanity_wallet', vanityWalletText(user), makeVanityWalletKeyboard(user));
      return;
    case 'community_vision':
      await editOrReplyMedia(ctx, 'community_vision', communityVisionCatalogText(user), makeCommunityVisionCatalogKeyboard(user));
      return;
    case 'community_vision_archive':
      await editOrReplyMedia(ctx, 'community_vision', communityVisionArchiveText(user), makeCommunityVisionArchiveKeyboard(user));
      return;
    case 'community_vision_editor':
      await editOrReplyMedia(ctx, 'community_vision', communityVisionEditorText(user), makeCommunityVisionEditorKeyboard(user));
      return;
    case 'wallet_tracker':
      await editOrReplyMedia(ctx, 'wallet_tracker', walletTrackerCatalogText(user), makeWalletTrackerCatalogKeyboard(user));
      return;
    case 'wallet_tracker_archive':
      await editOrReplyMedia(ctx, 'wallet_tracker', walletTrackerArchiveText(user), makeWalletTrackerArchiveKeyboard(user));
      return;
    case 'wallet_tracker_editor':
      await editOrReplyMedia(ctx, 'wallet_tracker', walletTrackerEditorText(user), makeWalletTrackerEditorKeyboard(user));
      return;
    case 'x_followers':
      await editOrReplyMedia(ctx, 'x_followers', xFollowersText(user), makeXFollowersKeyboard(user));
      return;
    case 'engagement':
      await editOrReplyMedia(ctx, 'engagement', engagementText(), makeEngagementKeyboard());
      return;
    case 'subscriptions_accounts':
      await editOrReplyMedia(ctx, 'subscriptions_accounts', subscriptionsAccountsText(), makeSubscriptionsAccountsKeyboard());
      return;
    case 'subscriptions_catalog':
      await editOrReplyMedia(ctx, 'subscriptions_accounts', subscriptionsCatalogText(), makeSubscriptionsCatalogKeyboard());
      return;
    case 'accounts_catalog':
      await editOrReplyMedia(ctx, 'subscriptions_accounts', accountsCatalogText(), makeAccountsCatalogKeyboard());
      return;
    case 'resizer':
      await editOrReplyMedia(ctx, 'resizer', resizerEditorText(user), makeResizerKeyboard(user));
      return;
    case 'volume':
      await editOrReplyMedia(ctx, 'volume', volumeText(user), makeVolumeKeyboard(user.volumeMode));
      return;
    case 'volume_organic':
      await editOrReplyMedia(ctx, 'volume', organicVolumeText(user), makeOrganicVolumeKeyboard(user));
      return;
    case 'volume_archive':
      await editOrReplyMedia(ctx, 'volume', organicVolumeArchiveText(user), makeOrganicVolumeArchiveKeyboard(user));
      return;
    case 'volume_bundled':
      await editOrReplyMedia(ctx, 'volume', bundledVolumeText(), makeBundledVolumeKeyboard(user));
      return;
    case 'volume_order':
      await editOrReplyMedia(ctx, 'volume', organicVolumeOrderText(user), makeOrganicVolumeOrderKeyboard(user));
      return;
    default:
      await editOrReplyMedia(ctx, 'home', homeText(user), makeHomeKeyboard(user));
  }
}

function buildRunnerEnv(user) {
  const selection = user.selection;
  return {
    TARGET_URL: selection.target || cfg.defaultTarget,
    BUTTON: selection.button,
    SESSION_COUNT: String(selection.amount),
    FREE_TRIAL: selection.usingFreeTrial ? '1' : '0',
  };
}

function createQuoteId() {
  return `sol_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

function createQuoteOffsetLamports() {
  return (Math.floor(Math.random() * 90) + 10) * 1_000;
}

async function ensureSolPriceCache(force = false) {
  if (!force && solUsdRateCache && Date.now() - solUsdRateCachedAt < SOL_PRICE_CACHE_MS) {
    return solUsdRateCache;
  }

  try {
    solUsdRateCache = await fetchSolUsdSpotRate();
    solUsdRateCachedAt = Date.now();
  } catch {
    // Keep the last known price if refresh fails.
  }

  return solUsdRateCache;
}

async function fetchSolUsdSpotRate() {
  const apiBaseUrl = cfg.solanaPriceApiBaseUrl.replace(/\/+$/, '');
  const response = await fetch(`${apiBaseUrl}/v2/prices/SOL-USD/spot`, {
    headers: {
      'User-Agent': 'steel-tester-bot/1.0',
    },
  });

  if (!response.ok) {
    throw new Error(`SOL/USD price lookup failed with status ${response.status}.`);
  }

  const payload = await response.json();
  const amount = Number.parseFloat(payload?.data?.amount);
  if (!Number.isFinite(amount) || amount <= 0) {
    throw new Error('SOL/USD price lookup returned an invalid value.');
  }

  return amount;
}

async function createPaymentQuote(userId) {
  const user = await getUserState(userId);
  if (!user.selection.amount) {
    throw new Error('Choose a bundle first.');
  }

  if (!cfg.solanaReceiveAddress) {
    throw new Error('SOLANA_RECEIVE_ADDRESS is not configured.');
  }

  const bundle = getBundlePricing(user.selection.amount);
  if (!bundle) {
    throw new Error('No pricing is configured for that bundle.');
  }

  const solUsdRate = await fetchSolUsdSpotRate();
  const baseLamports = Math.ceil((bundle.usdPrice / solUsdRate) * LAMPORTS_PER_SOL);
  const lamports = baseLamports + createQuoteOffsetLamports();
  const now = Date.now();

  return updateUserState(userId, (draft) => {
    draft.payment = {
      status: PAYMENT_STATES.PENDING,
      bundleAmount: bundle.amount,
      usdAmount: bundle.usdPrice,
      pricePerApple: bundle.pricePerApple,
      role: bundle.role,
      quoteId: createQuoteId(),
      quoteCreatedAt: new Date(now).toISOString(),
      quoteExpiresAt: new Date(now + (cfg.solanaQuoteTtlMinutes * 60_000)).toISOString(),
      solUsdRate,
      lamports,
      solAmount: formatSolAmountFromLamports(lamports),
      address: cfg.solanaReceiveAddress,
      matchedSignature: null,
      matchedAt: null,
      matchedLamports: null,
      lastCheckAt: null,
      lastError: null,
      lastIncomingLamports: null,
    };
    return draft;
  });
}

async function setPaymentQuoteError(userId, amount, message) {
  const bundle = getBundlePricing(amount);
  return updateUserState(userId, (draft) => {
    draft.payment = {
      ...createDefaultPaymentState(),
      status: PAYMENT_STATES.NONE,
      bundleAmount: bundle?.amount ?? amount,
      usdAmount: bundle?.usdPrice ?? null,
      pricePerApple: bundle?.pricePerApple ?? null,
      role: bundle?.role ?? null,
      lastError: message,
    };
    return draft;
  });
}

async function solanaRpc(method, params) {
  return solanaRpcPool.rpcRequest(method, params);
}

async function getRecentWalletSignatures() {
  if (!cfg.solanaReceiveAddress) {
    return [];
  }

  return solanaRpc('getSignaturesForAddress', [
    cfg.solanaReceiveAddress,
    {
      limit: cfg.solanaTxScanLimit,
      commitment: 'confirmed',
    },
  ]);
}

async function getWalletBalance(address) {
  const result = await solanaRpc('getBalance', [
    address,
    { commitment: 'confirmed' },
  ]);

  return Number.isInteger(result?.value) ? result.value : 0;
}

async function getParsedTransaction(signature) {
  return solanaRpc('getTransaction', [
    signature,
    {
      encoding: 'jsonParsed',
      commitment: 'confirmed',
      maxSupportedTransactionVersion: 0,
    },
  ]);
}

function getAccountKeyValue(accountKey) {
  if (typeof accountKey === 'string') {
    return accountKey;
  }

  if (accountKey && typeof accountKey === 'object' && typeof accountKey.pubkey === 'string') {
    return accountKey.pubkey;
  }

  return String(accountKey ?? '');
}

function extractIncomingLamports(transaction, walletAddress) {
  if (!transaction || transaction.meta?.err) {
    return 0;
  }

  const accountKeys = transaction.transaction?.message?.accountKeys ?? [];
  const accountIndex = accountKeys.findIndex((accountKey) => getAccountKeyValue(accountKey) === walletAddress);
  if (accountIndex < 0) {
    return 0;
  }

  const preBalance = transaction.meta?.preBalances?.[accountIndex];
  const postBalance = transaction.meta?.postBalances?.[accountIndex];
  if (!Number.isInteger(preBalance) || !Number.isInteger(postBalance)) {
    return 0;
  }

  return Math.max(0, postBalance - preBalance);
}

async function checkPaymentForUser(userId) {
  const store = await readStore();
  const user = normalizeUserState(userId, store.users[userId]);

  if (!paymentNeedsChecking(user)) {
    return { matched: false, reason: 'No active quote to check.', user };
  }

  if (!cfg.solanaReceiveAddress) {
    return { matched: false, reason: 'SOLANA_RECEIVE_ADDRESS is not configured.', user };
  }

  const signatures = await getRecentWalletSignatures();
  const quoteCreatedAtMs = user.payment.quoteCreatedAt ? new Date(user.payment.quoteCreatedAt).getTime() : 0;
  let closest = null;

  for (const signatureInfo of signatures) {
    if (!signatureInfo?.signature || signatureInfo.err) {
      continue;
    }

    if (store.processedPaymentSignatures.includes(signatureInfo.signature)) {
      continue;
    }

    const blockTimeMs = signatureInfo.blockTime ? signatureInfo.blockTime * 1_000 : null;
    if (blockTimeMs && blockTimeMs + 60_000 < quoteCreatedAtMs) {
      continue;
    }

    const transaction = await getParsedTransaction(signatureInfo.signature);
    const incomingLamports = extractIncomingLamports(transaction, cfg.solanaReceiveAddress);
    if (incomingLamports <= 0) {
      continue;
    }

    const diff = Math.abs(incomingLamports - user.payment.lamports);
    if (!closest || diff < closest.diff) {
      closest = {
        signature: signatureInfo.signature,
        incomingLamports,
        diff,
      };
    }

    if (diff <= cfg.solanaPaymentToleranceLamports) {
      const nextUser = normalizeUserState(userId, store.users[userId]);
      nextUser.payment = {
        ...nextUser.payment,
        status: PAYMENT_STATES.PAID,
        matchedSignature: signatureInfo.signature,
        matchedAt: new Date().toISOString(),
        matchedLamports: incomingLamports,
        lastCheckAt: new Date().toISOString(),
        lastError: null,
        lastIncomingLamports: incomingLamports,
      };

      store.users[userId] = nextUser;
      store.processedPaymentSignatures.push(signatureInfo.signature);
      store.processedPaymentSignatures = Array.from(new Set(store.processedPaymentSignatures)).slice(-500);
      await writeStore(store);

      return {
        matched: true,
        user: nextUser,
        signature: signatureInfo.signature,
        incomingLamports,
      };
    }
  }

  const nextUser = normalizeUserState(userId, store.users[userId]);
  nextUser.payment = {
    ...nextUser.payment,
    status: quoteExpired(nextUser.payment) ? PAYMENT_STATES.EXPIRED : PAYMENT_STATES.PENDING,
    lastCheckAt: new Date().toISOString(),
    lastError: null,
    lastIncomingLamports: closest?.incomingLamports ?? null,
  };
  store.users[userId] = nextUser;
  await writeStore(store);

  return {
    matched: false,
    user: nextUser,
    closest,
  };
}

async function consumeCurrentPayment(userId) {
  await updateUserState(userId, (draft) => {
    draft.payment = createDefaultPaymentState();
    return draft;
  });
}

async function createOrganicVolumeOrder(userId, packageKey) {
  return createAppleBoosterOrder(userId, 'organic', packageKey);
}

async function checkXFollowersPayment(userId) {
  const store = await readStore();
  const user = normalizeUserState(userId, store.users[userId]);

  if (!xFollowersNeedsChecking(user)) {
    return { matched: false, reason: 'No active X Followers quote to check.', user };
  }

  if (!cfg.solanaReceiveAddress) {
    return { matched: false, reason: 'SOLANA_RECEIVE_ADDRESS is not configured.', user };
  }

  const signatures = await getRecentWalletSignatures();
  const quoteCreatedAtMs = user.xFollowers.payment.quoteCreatedAt
    ? new Date(user.xFollowers.payment.quoteCreatedAt).getTime()
    : 0;
  let closest = null;

  for (const signatureInfo of signatures) {
    if (!signatureInfo?.signature || signatureInfo.err) {
      continue;
    }

    if (store.processedPaymentSignatures.includes(signatureInfo.signature)) {
      continue;
    }

    const blockTimeMs = signatureInfo.blockTime ? signatureInfo.blockTime * 1_000 : null;
    if (blockTimeMs && blockTimeMs + 60_000 < quoteCreatedAtMs) {
      continue;
    }

    const transaction = await getParsedTransaction(signatureInfo.signature);
    const incomingLamports = extractIncomingLamports(transaction, cfg.solanaReceiveAddress);
    if (incomingLamports <= 0) {
      continue;
    }

    const diff = Math.abs(incomingLamports - user.xFollowers.payment.lamports);
    if (diff <= cfg.solanaPaymentToleranceLamports) {
      store.users[userId] = normalizeUserState(userId, store.users[userId]);
      store.users[userId].xFollowers = normalizeXFollowersState({
        ...store.users[userId].xFollowers,
        status: 'paid',
        matchedAt: new Date().toISOString(),
        payment: {
          ...store.users[userId].xFollowers.payment,
          status: PAYMENT_STATES.PAID,
          matchedSignature: signatureInfo.signature,
          matchedAt: new Date().toISOString(),
          matchedLamports: incomingLamports,
          lastIncomingLamports: incomingLamports,
          lastCheckAt: new Date().toISOString(),
          lastError: null,
        },
      });
      store.processedPaymentSignatures.push(signatureInfo.signature);
      store.processedPaymentSignatures = Array.from(new Set(store.processedPaymentSignatures)).slice(-500);
      await writeStore(store);
      return { matched: true, reason: 'Payment matched.', user: normalizeUserState(userId, store.users[userId]) };
    }

    if (!closest || diff < closest.diff) {
      closest = { diff, incomingLamports };
    }
  }

  const nextUser = structuredClone(user);
  nextUser.xFollowers = normalizeXFollowersState({
    ...nextUser.xFollowers,
    payment: {
      ...nextUser.xFollowers.payment,
      status: quoteExpired(nextUser.xFollowers.payment) ? PAYMENT_STATES.EXPIRED : PAYMENT_STATES.PENDING,
      lastCheckAt: new Date().toISOString(),
      lastIncomingLamports: closest?.incomingLamports ?? null,
      lastError: closest
        ? `Closest payment was ${formatSolAmountFromLamports(closest.incomingLamports)} SOL, which did not match this quote.`
        : null,
    },
  });
  store.users[userId] = nextUser;
  await writeStore(store);
  return { matched: false, reason: 'Payment not found.', user: normalizeUserState(userId, nextUser) };
}

async function createBundledVolumeOrder(userId, packageKey) {
  return createAppleBoosterOrder(userId, 'bundled', packageKey);
}

function appleBoosterDisplayName(orderOrStrategy) {
  if (typeof orderOrStrategy === 'object' && orderOrStrategy?.freeTrial) {
    return 'Volume Bot Trial';
  }
  const strategy = typeof orderOrStrategy === 'string'
    ? orderOrStrategy
    : orderOrStrategy?.strategy;
  return strategy === 'bundled' ? 'Bundled Volume Booster' : 'Organic Volume Booster';
}

async function createAppleBoosterOrder(userId, strategy, packageKey) {
  const pkg = getAppleBoosterPackage(strategy, packageKey);
  if (!pkg) {
    throw new Error(`Unknown ${strategy} volume package.`);
  }

  const wallet = generateSolanaWallet();
  const requiredLamports = Math.round(Number(pkg.priceSol) * LAMPORTS_PER_SOL);
  const treasuryCutSol = Number(pkg.treasuryCutSol).toFixed(2);
  const treasuryShareSol = (Number(pkg.treasuryCutSol) / 2).toFixed(2);
  const devShareSol = (Number(pkg.treasuryCutSol) / 2).toFixed(2);
  const usableSol = (Number(pkg.priceSol) - Number(pkg.treasuryCutSol)).toFixed(2);

  const updated = await updateUserState(userId, (draft) => {
    draft.volumeMode = strategy;
    appendAppleBoosterToDraft(draft, {
      ...createDefaultOrganicVolumeOrder(),
      strategy,
      packageKey,
      walletAddress: wallet.address,
      walletSecretKeyB64: wallet.secretKeyB64,
      requiredSol: pkg.priceSol,
      rebateSol: pkg.rebateSol,
      treasuryCutSol,
      treasuryShareSol,
      devShareSol,
      usableSol,
      treasuryWalletAddress: cfg.treasuryWalletAddress,
      devWalletAddress: cfg.devWalletAddress,
      requiredLamports,
      currentLamports: 0,
      currentSol: '0',
      funded: false,
      running: false,
      awaitingField: strategy === 'bundled' ? 'mint' : 'wallet_count',
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      appleBooster: {
        ...createDefaultAppleBoosterState(),
        walletCount: strategy === 'bundled' ? 1 : null,
        workerWallets: strategy === 'bundled' ? [] : [],
      },
    });
    return draft;
  });
  await appendUserActivityLog(userId, {
    scope: appleBoosterScope(updated.organicVolumeOrder.id),
    level: 'info',
    message: `Created ${appleBoosterDisplayName(strategy)} deposit wallet for package ${pkg.label}.`,
  });
  return updated;
}

async function createTrialVolumeOrder(userId) {
  if (!cfg.volumeTrialEnabled) {
    throw new Error('Volume Bot Trial is not configured yet.');
  }

  const wallet = generateSolanaWallet();
  const now = new Date().toISOString();

  const updated = await updateUserState(userId, (draft) => {
    draft.volumeMode = 'organic';
    appendAppleBoosterToDraft(draft, {
      ...createDefaultOrganicVolumeOrder(),
      strategy: 'organic',
      freeTrial: true,
      trialTradeGoal: cfg.volumeTrialTradeGoal,
      packageKey: 'trial',
      walletAddress: wallet.address,
      walletSecretKeyB64: wallet.secretKeyB64,
      requiredSol: '0',
      requiredLamports: 0,
      currentLamports: 0,
      currentSol: '0',
      funded: true,
      running: false,
      awaitingField: 'mint',
      createdAt: now,
      fundedAt: now,
      updatedAt: now,
      appleBooster: {
        ...createDefaultAppleBoosterState(),
        walletCount: 1,
        workerWallets: createAppleBoosterWorkerWallets(1),
        minSwapSol: cfg.volumeTrialMinSol,
        maxSwapSol: cfg.volumeTrialMaxSol,
        minSwapLamports: cfg.volumeTrialMinLamports,
        maxSwapLamports: cfg.volumeTrialMaxLamports,
        minIntervalSeconds: cfg.volumeTrialMinIntervalSeconds,
        maxIntervalSeconds: cfg.volumeTrialMaxIntervalSeconds,
      },
    });
    return draft;
  });

  await appendUserActivityLog(userId, {
    scope: appleBoosterScope(updated.organicVolumeOrder.id),
    level: 'info',
    message: `Created a Volume Bot Trial with ${cfg.volumeTrialTradeGoal} tiny live trade legs.`,
  });
  return updated;
}

async function refreshOrganicVolumeOrder(userId, boosterId = null) {
  const user = await getUserState(userId);
  const targetBoosterId = boosterId || user.activeAppleBoosterId;
  const order = user.appleBoosters.find((item) => item.id === targetBoosterId) ?? user.organicVolumeOrder;
  if (!order?.walletAddress) {
    return user;
  }

  try {
    const lamports = await getWalletBalance(order.walletAddress);
    const workerWallets = Array.isArray(order.appleBooster?.workerWallets)
      ? await Promise.all(
        order.appleBooster.workerWallets.map(async (worker) => {
          if (!worker?.address) {
            return {
              ...worker,
              currentLamports: 0,
              currentSol: '0',
            };
          }

          try {
            const workerLamports = await getWalletBalance(worker.address);
            return {
              ...worker,
              currentLamports: workerLamports,
              currentSol: formatSolAmountFromLamports(workerLamports),
            };
          } catch {
            return {
              ...worker,
              currentLamports: Number.isInteger(worker.currentLamports) ? worker.currentLamports : 0,
              currentSol: typeof worker.currentSol === 'string' ? worker.currentSol : '0',
            };
          }
        }),
      )
      : [];
    const totalManagedLamports = lamports + workerWallets.reduce(
      (sum, worker) => sum + (Number.isInteger(worker.currentLamports) ? worker.currentLamports : 0),
      0,
    );

    return updateUserState(userId, (draft) => {
      updateAppleBoosterInDraft(draft, targetBoosterId, (current) => {
        current.currentLamports = lamports;
        current.currentSol = formatSolAmountFromLamports(lamports);
        current.appleBooster.workerWallets = workerWallets;
        current.appleBooster.totalManagedLamports = totalManagedLamports;
        current.funded = Boolean(
          current.funded ||
          current.fundedAt ||
          lamports >= (current.requiredLamports ?? Number.MAX_SAFE_INTEGER)
        );
        current.lastBalanceCheckAt = new Date().toISOString();
        current.lastError = null;
        if (current.funded && !current.fundedAt) {
          current.fundedAt = new Date().toISOString();
        }
        return current;
      });
      return draft;
    });
  } catch (error) {
    return updateUserState(userId, (draft) => {
      updateAppleBoosterInDraft(draft, targetBoosterId, (current) => {
        current.lastBalanceCheckAt = new Date().toISOString();
        current.lastError = String(error.message || error);
        return current;
      });
      return draft;
    });
  }
}

async function markPurchaseAnnouncementSent(userId, announcementKey) {
  await updateUserState(userId, (draft) => {
    draft.payment.announcementKey = announcementKey;
    draft.payment.announcementSentAt = new Date().toISOString();
    return draft;
  });
}

async function markXFollowersAnnouncementSent(userId, announcementKey) {
  await updateUserState(userId, (draft) => {
    draft.xFollowers = normalizeXFollowersState({
      ...draft.xFollowers,
      payment: {
        ...draft.xFollowers.payment,
        announcementKey,
        announcementSentAt: new Date().toISOString(),
      },
    });
    return draft;
  });
}

async function createXFollowersQuote(userId) {
  const user = await getUserState(userId);
  const pkg = getXFollowersPackage(user.xFollowers.packageKey);
  if (!pkg) {
    throw new Error('Choose an X Followers package first.');
  }

  if (!user.xFollowers.target) {
    throw new Error('Set the X profile link first.');
  }

  if (!cfg.solanaReceiveAddress) {
    throw new Error('SOLANA_RECEIVE_ADDRESS is not configured.');
  }

  const solUsdRate = await fetchSolUsdSpotRate();
  const baseLamports = Math.ceil((pkg.usdPrice / solUsdRate) * LAMPORTS_PER_SOL);
  const lamports = baseLamports + createQuoteOffsetLamports();
  const now = Date.now();
  const estimatedProfitUsd = Number((pkg.usdPrice - pkg.providerCostUsd).toFixed(2));

  return updateUserState(userId, (draft) => {
    draft.xFollowers = normalizeXFollowersState({
      ...draft.xFollowers,
      status: 'pending_payment',
      providerCostUsd: pkg.providerCostUsd,
      sellPriceUsd: pkg.usdPrice,
      estimatedProfitUsd,
      estimatedTreasuryShareUsd: Number((estimatedProfitUsd / 2).toFixed(2)),
      estimatedBurnShareUsd: Number((estimatedProfitUsd / 2).toFixed(2)),
      payment: {
        status: PAYMENT_STATES.PENDING,
        bundleAmount: pkg.followers,
        usdAmount: pkg.usdPrice,
        role: pkg.promise,
        quoteId: createQuoteId(),
        quoteCreatedAt: new Date(now).toISOString(),
        quoteExpiresAt: new Date(now + (cfg.solanaQuoteTtlMinutes * 60_000)).toISOString(),
        solUsdRate,
        lamports,
        solAmount: formatSolAmountFromLamports(lamports),
        address: cfg.solanaReceiveAddress,
        matchedSignature: null,
        matchedAt: null,
        matchedLamports: null,
        lastCheckAt: null,
        lastError: null,
        lastIncomingLamports: null,
        packageKey: pkg.key,
      },
      lastError: null,
    });
    return draft;
  });
}

async function createVanityWalletQuote(userId) {
  const user = await getUserState(userId);
  const state = normalizeVanityWalletState(user.vanityWallet);
  if (!state.patternMode) {
    throw new Error('Choose Starts With or Ends With first.');
  }

  if (!state.pattern) {
    throw new Error('Set the vanity pattern first.');
  }

  if (!cfg.solanaReceiveAddress) {
    throw new Error('SOLANA_RECEIVE_ADDRESS is not configured.');
  }

  const now = Date.now();
  const lamports = VANITY_WALLET_SERVICE_FEE_LAMPORTS + createQuoteOffsetLamports();
  return updateUserState(userId, (draft) => {
    draft.vanityWallet = normalizeVanityWalletState({
      ...draft.vanityWallet,
      status: 'pending_payment',
      payment: {
        status: PAYMENT_STATES.PENDING,
        bundleAmount: 1,
        usdAmount: null,
        role: 'vanity wallet',
        quoteId: createQuoteId(),
        quoteCreatedAt: new Date(now).toISOString(),
        quoteExpiresAt: new Date(now + (cfg.solanaQuoteTtlMinutes * 60_000)).toISOString(),
        solUsdRate: null,
        lamports,
        solAmount: formatSolAmountFromLamports(lamports),
        address: cfg.solanaReceiveAddress,
        matchedSignature: null,
        matchedAt: null,
        matchedLamports: null,
        lastCheckAt: null,
        lastError: null,
        lastIncomingLamports: null,
      },
      generatedAddress: null,
      generatedSecretKeyB64: null,
      generatedSecretKeyBase58: null,
      privateKeyVisible: false,
      attemptCount: 0,
      generationStartedAt: null,
      completedAt: null,
      estimatedTreasuryShareLamports: VANITY_WALLET_TREASURY_SHARE_LAMPORTS,
      estimatedBurnShareLamports: VANITY_WALLET_BURN_SHARE_LAMPORTS,
      lastError: null,
    });
    return draft;
  });
}

async function checkVanityWalletPayment(userId) {
  const store = await readStore();
  const user = normalizeUserState(userId, store.users[userId]);

  if (!vanityWalletNeedsChecking(user)) {
    return { matched: false, reason: 'No active vanity wallet quote to check.', user };
  }

  if (!cfg.solanaReceiveAddress) {
    return { matched: false, reason: 'SOLANA_RECEIVE_ADDRESS is not configured.', user };
  }

  const signatures = await getRecentWalletSignatures();
  const quoteCreatedAtMs = user.vanityWallet.payment.quoteCreatedAt
    ? new Date(user.vanityWallet.payment.quoteCreatedAt).getTime()
    : 0;
  let closest = null;

  for (const signatureInfo of signatures) {
    if (!signatureInfo?.signature || signatureInfo.err) {
      continue;
    }

    if (store.processedPaymentSignatures.includes(signatureInfo.signature)) {
      continue;
    }

    const blockTimeMs = signatureInfo.blockTime ? signatureInfo.blockTime * 1_000 : null;
    if (blockTimeMs && blockTimeMs + 60_000 < quoteCreatedAtMs) {
      continue;
    }

    const transaction = await getParsedTransaction(signatureInfo.signature);
    const incomingLamports = extractIncomingLamports(transaction, cfg.solanaReceiveAddress);
    if (incomingLamports <= 0) {
      continue;
    }

    const diff = Math.abs(incomingLamports - user.vanityWallet.payment.lamports);
    if (diff <= cfg.solanaPaymentToleranceLamports) {
      store.users[userId] = normalizeUserState(userId, store.users[userId]);
      store.users[userId].vanityWallet = normalizeVanityWalletState({
        ...store.users[userId].vanityWallet,
        status: 'paid',
        payment: {
          ...store.users[userId].vanityWallet.payment,
          status: PAYMENT_STATES.PAID,
          matchedSignature: signatureInfo.signature,
          matchedAt: new Date().toISOString(),
          matchedLamports: incomingLamports,
          lastIncomingLamports: incomingLamports,
          lastCheckAt: new Date().toISOString(),
          lastError: null,
        },
        lastError: null,
      });
      store.processedPaymentSignatures.push(signatureInfo.signature);
      store.processedPaymentSignatures = Array.from(new Set(store.processedPaymentSignatures)).slice(-500);
      await writeStore(store);
      return { matched: true, reason: 'Payment matched.', user: normalizeUserState(userId, store.users[userId]) };
    }

    if (!closest || diff < closest.diff) {
      closest = { diff, incomingLamports };
    }
  }

  const nextUser = structuredClone(user);
  nextUser.vanityWallet = normalizeVanityWalletState({
    ...nextUser.vanityWallet,
    payment: {
      ...nextUser.vanityWallet.payment,
      status: quoteExpired(nextUser.vanityWallet.payment) ? PAYMENT_STATES.EXPIRED : PAYMENT_STATES.PENDING,
      lastCheckAt: new Date().toISOString(),
      lastIncomingLamports: closest?.incomingLamports ?? null,
      lastError: closest
        ? `Closest payment was ${formatSolAmountFromLamports(closest.incomingLamports)} SOL, which did not match this quote.`
        : null,
    },
    lastError: closest
      ? `Closest payment was ${formatSolAmountFromLamports(closest.incomingLamports)} SOL, which did not match this quote.`
      : null,
  });
  store.users[userId] = nextUser;
  await writeStore(store);
  return { matched: false, reason: 'Payment not found.', user: normalizeUserState(userId, nextUser) };
}

async function startVanityWalletGeneration(userId) {
  if (vanityWalletJobs.has(userId)) {
    return false;
  }

  vanityWalletJobs.set(userId, true);
  const run = async () => {
    try {
      let attempts = 0;
      let lastProgressUpdate = Date.now();

      while (true) {
        const currentUser = await getUserState(userId);
        const state = normalizeVanityWalletState(currentUser.vanityWallet);
        if (!state.patternMode || !state.pattern || state.generatedAddress || !vanityWalletPaymentIsReady({ vanityWallet: state })) {
          break;
        }

        if (state.status !== 'generating') {
          await updateUserState(userId, (draft) => {
            draft.vanityWallet = normalizeVanityWalletState({
              ...draft.vanityWallet,
              status: 'generating',
              generationStartedAt: draft.vanityWallet.generationStartedAt || new Date().toISOString(),
              attemptCount: draft.vanityWallet.attemptCount || 0,
              lastError: null,
            });
            return draft;
          });
        }

        for (let index = 0; index < VANITY_WALLET_BATCH_SIZE; index += 1) {
          const wallet = generateSolanaWallet();
          attempts += 1;
          if (vanityWalletMatches(wallet.address, state.patternMode, state.pattern)) {
            const updated = await updateUserState(userId, (draft) => {
              draft.vanityWallet = normalizeVanityWalletState({
                ...draft.vanityWallet,
                status: 'completed',
                generatedAddress: wallet.address,
                generatedSecretKeyB64: wallet.secretKeyB64,
                generatedSecretKeyBase58: wallet.secretKeyBase58,
                privateKeyVisible: false,
                attemptCount: (draft.vanityWallet.attemptCount || 0) + attempts,
                completedAt: new Date().toISOString(),
                lastError: null,
              });
              return draft;
            });

            await appendUserActivityLog(userId, {
              scope: `vanity_wallet:${updated.vanityWallet.id}`,
              level: 'info',
              message: `Generated vanity wallet ${wallet.address} after ${Number(updated.vanityWallet.attemptCount || 0).toLocaleString('en-US')} attempts.`,
            });

            await bot.api.sendMessage(
              userId,
              [
                '*Vanity Wallet Ready*',
                '',
                vanityWalletText(updated),
              ].join('\n'),
              {
                parse_mode: 'Markdown',
                reply_markup: makeVanityWalletKeyboard(updated),
              },
            );
            return;
          }
        }

        if (Date.now() - lastProgressUpdate >= 2_000) {
          await updateUserState(userId, (draft) => {
            draft.vanityWallet = normalizeVanityWalletState({
              ...draft.vanityWallet,
              status: 'generating',
              attemptCount: (draft.vanityWallet.attemptCount || 0) + attempts,
            });
            return draft;
          });
          attempts = 0;
          lastProgressUpdate = Date.now();
        }

        await new Promise((resolve) => setImmediate(resolve));
      }
    } catch (error) {
      await updateUserState(userId, (draft) => {
        draft.vanityWallet = normalizeVanityWalletState({
          ...draft.vanityWallet,
          status: 'failed',
          attemptCount: draft.vanityWallet.attemptCount || 0,
          lastError: String(error.message || error),
        });
        return draft;
      });
    } finally {
      vanityWalletJobs.delete(userId);
    }
  };

  void run();
  return true;
}

async function resumePendingVanityWalletJobs() {
  const store = await readStore();
  const userIds = Object.keys(store.users || {});
  for (const userId of userIds) {
    const user = normalizeUserState(userId, store.users[userId]);
    const state = normalizeVanityWalletState(user.vanityWallet);
    if (
      state.patternMode
      && state.pattern
      && !state.generatedAddress
      && vanityWalletPaymentIsReady(user)
      && ['paid', 'generating'].includes(state.status)
    ) {
      await startVanityWalletGeneration(userId);
    }
  }
}

async function sendAlertsGroupAnnouncement({ imagePath, lines }) {
  if (!cfg.alertsChannelId || !Array.isArray(lines) || lines.length === 0) {
    return false;
  }

  const message = lines.filter(Boolean).join('\n');
  const baseOptions = {
    parse_mode: 'HTML',
    disable_web_page_preview: true,
    ...(cfg.alertsThreadId ? { message_thread_id: cfg.alertsThreadId } : {}),
  };

  try {
    if (imagePath) {
      await bot.api.sendPhoto(cfg.alertsChannelId, new InputFile(imagePath), {
        caption: message,
        parse_mode: 'HTML',
        ...(cfg.alertsThreadId ? { message_thread_id: cfg.alertsThreadId } : {}),
      });
    } else {
      await bot.api.sendMessage(cfg.alertsChannelId, message, baseOptions);
    }
    return true;
  } catch (error) {
    console.error('Failed to send alerts group announcement:', error);
    return false;
  }
}

async function announceReactionPurchaseLegacy(userId, user) {
  if (!cfg.salesChannelId || !user.selection.amount || user.selection.usingFreeTrial || isAdminUser(user.telegramId)) {
    return false;
  }

  const announcementKey = paymentAnnouncementKey(user);
  if (user.payment.announcementKey === announcementKey && user.payment.announcementSentAt) {
    return false;
  }

  const bundle = getBundlePricing(user.selection.amount);
  const targetUrl = user.selection.target;
  const targetSummary = summarizeTarget(targetUrl);
  const txUrl = user.payment.matchedSignature && user.payment.matchedSignature !== 'manual_override'
    ? `https://solscan.io/tx/${encodeURIComponent(user.payment.matchedSignature)}`
    : null;
  const messageLines = [
    'ÃƒÂ¢Ã…â€œÃ¢â‚¬Â¦ <b>Bundle Purchase Confirmed</b>',
    '',
    `<b>Bundle</b>: x${escapeHtml(user.selection.amount)} apples`,
    `<b>Profile</b>: ${escapeHtml(buttonDisplay(user.selection.button))}`,
    `<b>Price</b>: ${escapeHtml(purchasePriceDisplay(user))}`,
    bundle?.role ? `<b>Tier</b>: ${escapeHtml(bundle.role)}` : null,
    `<b>Target</b>: <a href="${escapeHtml(targetUrl)}">${escapeHtml(targetSummary)}</a>`,
    `<b>Link</b>: <code>${escapeHtml(targetUrl)}</code>`,
    `<b>Buyer</b>: #${escapeHtml(String(userId).slice(-4))}`,
    `<b>Confirmed</b>: ${escapeHtml(formatTimestamp(user.payment.matchedAt || new Date().toISOString()))}`,
    '',
    txUrl
      ? `<a href="${escapeHtml(txUrl)}">View transaction on Solscan</a>`
      : '<i>Manual payment confirmation</i>',
  ].filter(Boolean);

  const options = {
    caption: messageLines.join('\n'),
    parse_mode: 'HTML',
  };

  if (cfg.salesThreadId) {
    options.message_thread_id = cfg.salesThreadId;
  }

  try {
    await bot.api.sendPhoto(cfg.salesChannelId, new InputFile(SALES_BROADCAST_IMAGE_PATH), options);
  } catch {
    await bot.api.sendMessage(cfg.salesChannelId, messageLines.join('\n'), {
      parse_mode: 'HTML',
      disable_web_page_preview: true,
      ...(cfg.salesThreadId ? { message_thread_id: cfg.salesThreadId } : {}),
    });
  }
  await markPurchaseAnnouncementSent(userId, announcementKey);
  return true;
}

async function announceReactionPurchaseToAlerts(userId, user) {
  if (!user.selection.amount || user.selection.usingFreeTrial || isAdminUser(user.telegramId)) {
    return false;
  }

  const targetUrl = user.selection.target;
  const targetSummary = summarizeTarget(targetUrl);
  const txUrl = user.payment.matchedSignature && user.payment.matchedSignature !== 'manual_override'
    ? `https://solscan.io/tx/${encodeURIComponent(user.payment.matchedSignature)}`
    : null;

  return sendAlertsGroupAnnouncement({
    imagePath: SALES_BROADCAST_IMAGE_PATH,
    lines: [
      '<b>ðŸš€ Fresh Reaction Order Paid</b>',
      '',
      `<b>Package</b>: ${escapeHtml(String(user.selection.amount))} reactions`,
      `<b>Reaction</b>: ${escapeHtml(buttonDisplay(user.selection.button))}`,
      `<b>Price</b>: ${escapeHtml(purchasePriceDisplay(user))}`,
      `<b>Target</b>: <a href="${escapeHtml(targetUrl)}">${escapeHtml(targetSummary)}</a>`,
      `<b>Buyer</b>: #${escapeHtml(String(userId).slice(-4))}`,
      '<b>Status</b>: Payment complete and ready to launch',
      '',
      txUrl
        ? `<a href="${escapeHtml(txUrl)}">View payment</a>`
        : '<i>Manual payment confirmation</i>',
    ],
  });
}

async function announceBurnAgentAttachedToAlerts(userId, agent) {
  if (!agent?.mintAddress) {
    return false;
  }

  const tokenLabel = agent.tokenName ? escapeHtml(agent.tokenName) : `CA ${escapeHtml(`${agent.mintAddress.slice(0, 4)}...${agent.mintAddress.slice(-4)}`)}`;
  const speedLabel = burnAgentSpeedLabel(agent.speed || 'normal');
  const planLabel = Number.isInteger(agent.burnPercent) && Number.isInteger(agent.treasuryPercent)
    ? `${agent.burnPercent}% buyback & burn / ${agent.treasuryPercent}% keep`
    : 'Custom reward split ready';

  return sendAlertsGroupAnnouncement({
    imagePath: BURN_AGENT_ALERT_IMAGE_PATH,
    lines: [
      '<b>ðŸ”¥ Burn Agent Attached</b>',
      '',
      `<b>Token</b>: ${tokenLabel}`,
      `<b>Mint</b>: <code>${escapeHtml(agent.mintAddress)}</code>`,
      `<b>Mode</b>: ${escapeHtml(speedLabel)}`,
      `<b>Plan</b>: ${escapeHtml(planLabel)}`,
      `<b>User</b>: #${escapeHtml(String(userId).slice(-4))}`,
      '',
      '<i>Auto-claim, buyback, and burn flow is now linked to this coin.</i>',
    ],
  });
}

async function announceXFollowersPurchase(userId, user) {
  const pkg = getXFollowersPackage(user.xFollowers?.packageKey);
  if (!cfg.salesChannelId || !pkg) {
    return false;
  }

  const announcementKey = xFollowersAnnouncementKey(user);
  if (user.xFollowers.payment.announcementKey === announcementKey && user.xFollowers.payment.announcementSentAt) {
    return false;
  }

  const txUrl = user.xFollowers.payment.matchedSignature && user.xFollowers.payment.matchedSignature !== 'manual_override'
    ? `https://solscan.io/tx/${encodeURIComponent(user.xFollowers.payment.matchedSignature)}`
    : null;
  const messageLines = [
    '<b>ðŸ‘¥ X Followers Order Paid</b>',
    '',
    `<b>Package</b>: ${escapeHtml(pkg.label)}`,
    `<b>Delivery</b>: ${escapeHtml(pkg.promise)}`,
    `<b>Price</b>: ${escapeHtml(user.xFollowers.payment.solAmount ? `${user.xFollowers.payment.solAmount} SOL` : `$${pkg.usdPrice.toFixed(2)}`)}`,
    `<b>Target</b>: ${escapeHtml(user.xFollowers.target || 'Not set')}`,
    `<b>Estimated Profit</b>: $${escapeHtml((user.xFollowers.estimatedProfitUsd ?? (pkg.usdPrice - pkg.providerCostUsd)).toFixed(2))}`,
    `<b>Treasury Share</b>: $${escapeHtml((user.xFollowers.estimatedTreasuryShareUsd ?? ((pkg.usdPrice - pkg.providerCostUsd) / 2)).toFixed(2))}`,
    `<b>Burn Share</b>: $${escapeHtml((user.xFollowers.estimatedBurnShareUsd ?? ((pkg.usdPrice - pkg.providerCostUsd) / 2)).toFixed(2))}`,
    `<b>Buyer</b>: #${escapeHtml(String(userId).slice(-4))}`,
    '',
    txUrl ? `<a href="${escapeHtml(txUrl)}">View payment on Solscan</a>` : '<i>Manual payment confirmation</i>',
  ];

  try {
    await bot.api.sendMessage(cfg.salesChannelId, messageLines.join('\n'), {
      parse_mode: 'HTML',
      disable_web_page_preview: true,
      ...(cfg.salesThreadId ? { message_thread_id: cfg.salesThreadId } : {}),
    });
  } catch (error) {
    console.error('Failed to send X Followers sales announcement:', error);
    return false;
  }

  await markXFollowersAnnouncementSent(userId, announcementKey);
  return true;
}

async function announceXFollowersPurchaseToAlerts(userId, user) {
  const pkg = getXFollowersPackage(user.xFollowers?.packageKey);
  if (!pkg) {
    return false;
  }

  const txUrl = user.xFollowers.payment.matchedSignature && user.xFollowers.payment.matchedSignature !== 'manual_override'
    ? `https://solscan.io/tx/${encodeURIComponent(user.xFollowers.payment.matchedSignature)}`
    : null;

  return sendAlertsGroupAnnouncement({
    imagePath: null,
    lines: [
      '<b>ðŸ‘¥ X Followers Order Paid</b>',
      '',
      `<b>Package</b>: ${escapeHtml(pkg.label)}`,
      `<b>Target</b>: ${escapeHtml(user.xFollowers.target || 'Not set')}`,
      `<b>Status</b>: Payment complete`,
      '<b>Note</b>: Buyer has been told to message support for timing confirmation',
      '',
      txUrl ? `<a href="${escapeHtml(txUrl)}">View payment</a>` : '<i>Manual payment confirmation</i>',
    ],
  });
}

async function announceReactionPurchase(userId, user) {
  if (!cfg.salesChannelId || !user.selection.amount || user.selection.usingFreeTrial || isAdminUser(user.telegramId)) {
    return false;
  }

  const announcementKey = paymentAnnouncementKey(user);
  if (user.payment.announcementKey === announcementKey && user.payment.announcementSentAt) {
    return false;
  }

  const bundle = getBundlePricing(user.selection.amount);
  const targetUrl = user.selection.target;
  const targetSummary = summarizeTarget(targetUrl);
  const txUrl = user.payment.matchedSignature && user.payment.matchedSignature !== 'manual_override'
    ? `https://solscan.io/tx/${encodeURIComponent(user.payment.matchedSignature)}`
    : null;

  const messageLines = [
    '<b>ðŸš€ Reaction Order Paid</b>',
    '',
    `<b>Package</b>: ${escapeHtml(String(user.selection.amount))} reactions`,
    `<b>Profile</b>: ${escapeHtml(buttonDisplay(user.selection.button))}`,
    `<b>Price</b>: ${escapeHtml(purchasePriceDisplay(user))}`,
    bundle?.role ? `<b>Tier</b>: ${escapeHtml(bundle.role)}` : null,
    `<b>Target</b>: <a href="${escapeHtml(targetUrl)}">${escapeHtml(targetSummary)}</a>`,
    `<b>Link</b>: <code>${escapeHtml(targetUrl)}</code>`,
    `<b>Buyer</b>: #${escapeHtml(String(userId).slice(-4))}`,
    `<b>Confirmed</b>: ${escapeHtml(formatTimestamp(user.payment.matchedAt || new Date().toISOString()))}`,
    '',
    txUrl
      ? `<a href="${escapeHtml(txUrl)}">View payment on Solscan</a>`
      : '<i>Manual payment confirmation</i>',
  ].filter(Boolean);

  const options = {
    caption: messageLines.join('\n'),
    parse_mode: 'HTML',
  };

  if (cfg.salesThreadId) {
    options.message_thread_id = cfg.salesThreadId;
  }

  try {
    await bot.api.sendPhoto(cfg.salesChannelId, new InputFile(SALES_BROADCAST_IMAGE_PATH), options);
  } catch {
    await bot.api.sendMessage(cfg.salesChannelId, messageLines.join('\n'), {
      parse_mode: 'HTML',
      disable_web_page_preview: true,
      ...(cfg.salesThreadId ? { message_thread_id: cfg.salesThreadId } : {}),
    });
  }

  await markPurchaseAnnouncementSent(userId, announcementKey);
  return true;
}

async function notifyPaymentMatched(userId, user) {
  try {
    await announceReactionPurchase(userId, user);
  } catch (error) {
    console.error('Failed to send sales channel announcement:', error);
  }

  try {
    await announceReactionPurchaseToAlerts(userId, user);
  } catch (error) {
    console.error('Failed to send reaction alert announcement:', error);
  }

  await bot.api.sendMessage(
    userId,
    [
      '*Payment Confirmed*',
      '',
      selectionSnapshot(user),
      '',
      'This bundle is unlocked. You can launch the run now.',
    ].join('\n'),
    {
      parse_mode: 'Markdown',
      reply_markup: makeConfirmKeyboard(user),
    },
  );
}

async function notifyXFollowersPaymentMatched(userId, user) {
  try {
    await announceXFollowersPurchase(userId, user);
  } catch (error) {
    console.error('Failed to send X Followers sales announcement:', error);
  }

  try {
    await announceXFollowersPurchaseToAlerts(userId, user);
  } catch (error) {
    console.error('Failed to send X Followers alert announcement:', error);
  }

  const pkg = getXFollowersPackage(user.xFollowers?.packageKey);
  await bot.api.sendMessage(
    userId,
    [
      '*Payment Confirmed*',
      '',
      `Package: *${pkg ? pkg.label : 'X Followers'}*`,
      `Target: \`${user.xFollowers.target || 'Not set'}\``,
      '',
      `Delivery may not be instant. Message support at @${SUPPORT_USERNAME} now to confirm timing and slot availability.`,
    ].join('\n'),
    {
      parse_mode: 'Markdown',
      reply_markup: makeXFollowersKeyboard(user),
    },
  );
}

async function notifyVanityWalletPaymentMatched(userId, user) {
  try {
    await startVanityWalletGeneration(userId);
  } catch (error) {
    console.error('Failed to start vanity wallet generation:', error);
  }

  await bot.api.sendMessage(
    userId,
    [
      '*Payment Confirmed*',
      '',
      `Pattern: ${vanityWalletPatternSummary(user.vanityWallet)}`,
      `Service fee: *${formatSolAmountFromLamports(VANITY_WALLET_SERVICE_FEE_LAMPORTS)} SOL*`,
      '',
      'Generation has started. Refresh this screen in a bit and the wallet will appear here as soon as it is found.',
    ].join('\n'),
    {
      parse_mode: 'Markdown',
      reply_markup: makeVanityWalletKeyboard(user),
    },
  );
}

async function pollPendingPayments() {
  if (paymentPollInFlight) {
    return;
  }

  paymentPollInFlight = true;
  try {
    const store = await readStore();
    const users = Object.keys(store.users).map((userId) => [userId, normalizeUserState(userId, store.users[userId])]);

    for (const [userId, user] of users) {
      if (paymentNeedsChecking(user)) {
        const result = await checkPaymentForUser(userId);
        if (result.matched) {
          try {
            await notifyPaymentMatched(userId, result.user);
          } catch (error) {
            console.error('Failed to send payment confirmation message:', error);
          }
        }
      }

      if (xFollowersNeedsChecking(user)) {
        const result = await checkXFollowersPayment(userId);
        if (result.matched) {
          try {
            await notifyXFollowersPaymentMatched(userId, result.user);
          } catch (error) {
            console.error('Failed to send X Followers payment confirmation message:', error);
          }
        }
      }

      if (vanityWalletNeedsChecking(user)) {
        const result = await checkVanityWalletPayment(userId);
        if (result.matched) {
          try {
            await notifyVanityWalletPaymentMatched(userId, result.user);
          } catch (error) {
            console.error('Failed to send vanity wallet confirmation message:', error);
          }
        }
      }
    }
  } catch (error) {
    if (error?.code === 'SOLANA_RPC_RATE_LIMIT') {
      console.warn(`Payment polling paused: ${error.message}`);
    } else {
      console.error('Payment polling error:', error);
    }
  } finally {
    paymentPollInFlight = false;
  }
}

async function runJob(user, onProgress) {
  const env = buildRunnerEnv(user);

  if (cfg.runnerMode === 'mock') {
    await onProgress('Starting mock job');
    await sleep(800);
    await onProgress(`Target confirmed: ${env.TARGET_URL}`);
    await sleep(800);
    await onProgress(`Testing button: ${env.BUTTON}`);
    await sleep(800);
    await onProgress(`Requested amount: ${env.SESSION_COUNT}`);
    await sleep(800);
    await onProgress('Mock job finished successfully');

    return {
      ok: true,
      code: 0,
      output: [
        'Mock mode run complete.',
        `Target: ${env.TARGET_URL}`,
        `Button: ${env.BUTTON}`,
        `Amount: ${env.SESSION_COUNT}`,
      ].join('\n'),
    };
  }

  if (cfg.runnerMode !== 'command' || !cfg.runnerCommand) {
    throw new Error('RUNNER_MODE must be "mock" or "command" with RUNNER_COMMAND configured.');
  }

  await onProgress('Launching configured runner command');

  return new Promise((resolve, reject) => {
    const child = spawn(cfg.runnerCommand, {
      cwd: ROOT_DIR,
      env: {
        ...process.env,
        ...env,
      },
      shell: true,
      windowsHide: true,
    });

    let stdout = '';
    let stderr = '';

    child.stdout.on('data', (chunk) => {
      stdout += chunk.toString();
    });

    child.stderr.on('data', (chunk) => {
      stderr += chunk.toString();
    });

    child.on('error', reject);
    child.on('close', (code) => {
      resolve({
        ok: code === 0,
        code,
        output: `${stdout}${stderr}`.trim(),
      });
    });
  });
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function handleRun(ctx) {
  const userId = String(ctx.from.id);
  const user = await getUserState(userId);

  if (!user.selection.button || !user.selection.amount) {
    await ctx.answerCallbackQuery({ text: 'Pick a button and bundle first.' });
    await editOrReply(ctx, confirmText(user), makeConfirmKeyboard(user));
    return;
  }

  if (!hasLaunchAccess(user)) {
    await ctx.answerCallbackQuery({ text: 'Payment still needs confirmation.' });
    await editOrReply(ctx, paymentText(user), makePaymentKeyboard(user));
    return;
  }

  await ctx.answerCallbackQuery({ text: 'Starting your job...' });
  const statusMessage = await ctx.reply(
    [
      '*Initializing Test Run*',
      '',
      selectionSnapshot(user),
      '',
      'Preparing runner startup...',
    ].join('\n'),
    { parse_mode: 'Markdown' },
  );

  try {
    const updates = [];
    const result = await runJob(user, async (message) => {
      updates.push(message);
      await ctx.api.editMessageText(
        ctx.chat.id,
        statusMessage.message_id,
        [
          '*Run In Progress*',
          '',
          selectionSnapshot(user),
          '',
          '*Latest updates*',
          ...updates.slice(-6),
        ].join('\n'),
        { parse_mode: 'Markdown' },
      );
    });

    await appendJob({
      userId,
      ranAt: new Date().toISOString(),
      selection: user.selection,
      payment: {
        quoteId: user.payment.quoteId,
        solAmount: user.payment.solAmount,
        usdAmount: user.payment.usdAmount,
        matchedSignature: user.payment.matchedSignature,
      },
      ok: result.ok,
      code: result.code,
    });
    let nextStepNote;
    let resultKeyboard;

    if (user.selection.usingFreeTrial) {
      await updateUserState(userId, (draft) => {
        draft.freeTrialUsed = true;
        draft.selection.amount = null;
        draft.selection.usingFreeTrial = false;
        draft.payment = createDefaultPaymentState();
        return draft;
      });
      nextStepNote = `Free trial used. Pick a paid bundle if you want to run again.`;
      resultKeyboard = makeInfoKeyboard('nav:amount', 'amount');
    } else if (isAdminUser(userId)) {
      await updateUserState(userId, (draft) => {
        draft.payment = createDefaultPaymentState();
        return draft;
      });
      nextStepNote = 'Support access is still active. You can launch another run without paying.';
      resultKeyboard = makeInfoKeyboard('nav:confirm', 'confirm');
    } else {
      await consumeCurrentPayment(userId);
      nextStepNote = 'Next run requires a fresh payment quote.';
      try {
        await createPaymentQuote(userId);
        nextStepNote = 'A fresh payment quote is ready if you want to run it again.';
      } catch (quoteError) {
        await setPaymentQuoteError(userId, user.selection.amount, String(quoteError.message || quoteError));
        nextStepNote = 'This run is complete. Open Payment to generate a fresh quote for another run.';
      }
      resultKeyboard = makeResultKeyboard();
    }

    const summary = [
      result.ok ? '*Run Completed*' : '*Run Finished With Errors*',
      '',
      selectionSnapshot(user),
      '',
      '```',
      (result.output || 'No output captured.').slice(0, 3000),
      '```',
      '',
      nextStepNote,
    ].join('\n');

    await ctx.api.editMessageText(ctx.chat.id, statusMessage.message_id, summary, {
      parse_mode: 'Markdown',
      reply_markup: resultKeyboard,
    });
  } catch (error) {
    await ctx.api.editMessageText(
      ctx.chat.id,
      statusMessage.message_id,
      [
        '*Job Failed To Start*',
        '',
        `Error: \`${String(error.message || error)}\``,
        '',
        'Use Back to review payment or try again.',
      ].join('\n'),
      {
        parse_mode: 'Markdown',
        reply_markup: makeResultKeyboard(),
      },
    );
  }
}

bot.command('start', async (ctx) => {
  try {
    const userId = String(ctx.from.id);
    const user = await getUserState(userId);
    await sendHome(ctx, user);
  } catch (error) {
    console.error('Failed to render /start home screen:', error);
    await ctx.reply(homeText(), {
      parse_mode: 'Markdown',
      reply_markup: makeHomeKeyboard(),
    });
  }
});

bot.command('menu', async (ctx) => {
  try {
    await handleSlashRoute(ctx, 'home');
  } catch (error) {
    console.error('Failed to render /menu home screen:', error);
    await ctx.reply(homeText(), {
      parse_mode: 'Markdown',
      reply_markup: makeHomeKeyboard(),
    });
  }
});

bot.command('buy_sell', async (ctx) => {
  await handleSlashRoute(ctx, 'buy_sell', { refreshTrading: true });
});

bot.command('wallets', async (ctx) => {
  await handleSlashRoute(ctx, 'buy_sell_wallets', { refreshTrading: true });
});

bot.command('limit', async (ctx) => {
  await handleSlashRoute(ctx, 'buy_sell', { refreshTrading: true });
});

bot.command('follow', async (ctx) => {
  await handleSlashRoute(ctx, 'buy_sell', { refreshTrading: true });
});

bot.command('reaction_booster', async (ctx) => {
  await handleSlashRoute(ctx, 'start');
});

bot.command('volume_booster', async (ctx) => {
  await handleSlashRoute(ctx, 'volume');
});

bot.command('burn_agent', async (ctx) => {
  await handleSlashRoute(ctx, 'burn_agent');
});

bot.command('holder_booster', async (ctx) => {
  await handleSlashRoute(ctx, 'holder_booster');
});

bot.command('fomo_booster', async (ctx) => {
  await handleSlashRoute(ctx, 'fomo_booster');
});

bot.command('smart_sell', async (ctx) => {
  await handleSlashRoute(ctx, 'magic_sell');
});

bot.command('magic_bundle', async (ctx) => {
  await handleSlashRoute(ctx, 'magic_bundle');
});

bot.command('launch_buy', async (ctx) => {
  await handleSlashRoute(ctx, 'launch_buy');
});

bot.command('sniper_wizard', async (ctx) => {
  await handleSlashRoute(ctx, 'sniper_wizard');
});

bot.command('vanity_wallet', async (ctx) => {
  await handleSlashRoute(ctx, 'vanity_wallet');
});

bot.command('community_vision', async (ctx) => {
  await handleSlashRoute(ctx, 'community_vision');
});

bot.command('wallet_tracker', async (ctx) => {
  await handleSlashRoute(ctx, 'wallet_tracker');
});

bot.command('x_followers', async (ctx) => {
  await handleSlashRoute(ctx, 'x_followers');
});

bot.command('engagement', async (ctx) => {
  await handleSlashRoute(ctx, 'engagement');
});

bot.command('subscriptions_accounts', async (ctx) => {
  await handleSlashRoute(ctx, 'subscriptions_accounts');
});

bot.command('resizer', async (ctx) => {
  await handleSlashRoute(ctx, 'resizer');
});

bot.command('staking', async (ctx) => {
  await handleSlashRoute(ctx, 'staking');
});

bot.command('help', async (ctx) => {
  await handleSlashRoute(ctx, 'help');
});

bot.command('paid', async (ctx) => {
  const adminId = String(ctx.from.id);
  if (!isAdminUser(adminId)) {
    await ctx.reply('Only admin accounts can mark a quote as paid.');
    return;
  }

  const [, targetUserId] = ctx.message.text.trim().split(/\s+/, 2);
  if (!targetUserId) {
    await ctx.reply('Usage: /paid <telegram_user_id>');
    return;
  }

  const updated = await updateUserState(targetUserId, (draft) => {
    if (!draft.selection.amount) {
      return draft;
    }

    const bundle = getBundlePricing(draft.selection.amount);
    draft.payment = {
      ...draft.payment,
      status: PAYMENT_STATES.PAID,
      bundleAmount: draft.selection.amount,
      usdAmount: draft.payment.usdAmount ?? bundle?.usdPrice ?? null,
      pricePerApple: draft.payment.pricePerApple ?? bundle?.pricePerApple ?? null,
      role: draft.payment.role ?? bundle?.role ?? null,
      matchedSignature: draft.payment.matchedSignature || 'manual_override',
      matchedAt: new Date().toISOString(),
    };
    return draft;
  });

  await ctx.reply(
    [
      'Quote marked as paid.',
      '',
      `User: ${updated.telegramId}`,
      `Bundle: ${amountLabel(updated.selection.amount)}`,
      `Status: ${paymentStatusLabel(updated)}`,
    ].join('\n'),
  );

  if (updated.selection.amount) {
    try {
      await notifyPaymentMatched(targetUserId, updated);
    } catch (error) {
      console.error('Failed to notify paid override target:', error);
    }
  }
});

bot.command('unpaid', async (ctx) => {
  const adminId = String(ctx.from.id);
  if (!isAdminUser(adminId)) {
    await ctx.reply('Only admin accounts can clear payment state.');
    return;
  }

  const [, targetUserId] = ctx.message.text.trim().split(/\s+/, 2);
  if (!targetUserId) {
    await ctx.reply('Usage: /unpaid <telegram_user_id>');
    return;
  }

  const updated = await updateUserState(targetUserId, (draft) => {
    draft.payment = createDefaultPaymentState();
    return draft;
  });

  await ctx.reply(
    [
      'Payment state cleared.',
      '',
      `User: ${updated.telegramId}`,
      `Status: ${paymentStatusLabel(updated)}`,
    ].join('\n'),
  );
});

bot.callbackQuery('nav:home', async (ctx) => {
  const user = await getUserState(String(ctx.from.id));
  await ctx.answerCallbackQuery();
  await renderScreen(ctx, 'home', user);
});

bot.callbackQuery('nav:start', async (ctx) => {
  const user = await getUserState(String(ctx.from.id));
  await ctx.answerCallbackQuery();
  await renderScreen(ctx, 'start', user);
});

bot.callbackQuery('entry:reaction', async (ctx) => {
  const userId = String(ctx.from.id);
  const updated = await updateUserState(userId, (draft) => {
    draft.selection.button = null;
    draft.selection.amount = null;
    draft.selection.usingFreeTrial = false;
    draft.selection.target = cfg.defaultTarget;
    draft.awaitingTargetInput = true;
    draft.payment = createDefaultPaymentState();
    return draft;
  });

  await ctx.answerCallbackQuery({ text: 'Send the target URL in chat.' });
  await renderScreen(ctx, 'target', updated);
});

bot.callbackQuery('nav:amount', async (ctx) => {
  const user = await getUserState(String(ctx.from.id));
  await ctx.answerCallbackQuery();
  await renderScreen(ctx, 'amount', user);
});

bot.callbackQuery('nav:payment', async (ctx) => {
  const user = await getUserState(String(ctx.from.id));
  await ctx.answerCallbackQuery();
  await renderScreen(ctx, 'payment', user);
});

bot.callbackQuery('nav:confirm', async (ctx) => {
  const user = await getUserState(String(ctx.from.id));
  await ctx.answerCallbackQuery();
  await renderScreen(ctx, hasLaunchAccess(user) ? 'confirm' : 'payment', user);
});

bot.callbackQuery('nav:target', async (ctx) => {
  const userId = String(ctx.from.id);
  const updated = await updateUserState(userId, (draft) => {
    draft.awaitingTargetInput = true;
    return draft;
  });

  await ctx.answerCallbackQuery({ text: 'Send the target URL in chat.' });
  await renderScreen(ctx, 'target', updated);
});

bot.callbackQuery('nav:status', async (ctx) => {
  const user = await getUserState(String(ctx.from.id));
  await ctx.answerCallbackQuery();
  await renderScreen(ctx, 'status', user);
});

bot.callbackQuery('nav:help', async (ctx) => {
  const user = await getUserState(String(ctx.from.id));
  await ctx.answerCallbackQuery();
  await renderScreen(ctx, 'help', user);
});

bot.callbackQuery(/^nav:help_(reaction|volume|buy_sell|burn_agent|holder_booster|fomo_booster|magic_sell|magic_bundle|launch_buy|sniper_wizard|staking|vanity_wallet|community_vision|wallet_tracker|x_followers|engagement|subscriptions_accounts|resizer)$/, async (ctx) => {
  const user = await getUserState(String(ctx.from.id));
  await ctx.answerCallbackQuery();
  await renderScreen(ctx, `help_${ctx.match[1]}`, user);
});

bot.callbackQuery('nav:launch_buy', async (ctx) => {
  const user = await getUserState(String(ctx.from.id));
  await ctx.answerCallbackQuery();
  await renderScreen(ctx, 'launch_buy', user);
});

bot.callbackQuery('nav:vanity_wallet', async (ctx) => {
  const user = await getUserState(String(ctx.from.id));
  await ctx.answerCallbackQuery();
  await renderScreen(ctx, 'vanity_wallet', user);
});

bot.callbackQuery('nav:launch_buy_archive', async (ctx) => {
  const user = await getUserState(String(ctx.from.id));
  await ctx.answerCallbackQuery();
  await renderScreen(ctx, 'launch_buy_archive', user);
});

bot.callbackQuery('nav:resizer', async (ctx) => {
  const user = await getUserState(String(ctx.from.id));
  await ctx.answerCallbackQuery();
  await renderScreen(ctx, 'resizer', user);
});

bot.callbackQuery(/^resizer:set_mode:(logo|banner)$/, async (ctx) => {
  const userId = String(ctx.from.id);
  const mode = ctx.match[1];
  const preset = getResizerPreset(mode);
  const updated = await updateUserState(userId, (draft) => {
    draft.resizer = normalizeResizer({
      ...draft.resizer,
      mode,
      awaitingImage: true,
      status: 'awaiting_image',
      lastError: null,
    });
    return draft;
  });

  await ctx.answerCallbackQuery({ text: `Send the image for your ${preset.label.toLowerCase()} now.` });
  await renderScreen(ctx, 'resizer', updated);
});

bot.callbackQuery('resizer:reset', async (ctx) => {
  const userId = String(ctx.from.id);
  const updated = await updateUserState(userId, (draft) => {
    draft.resizer = createDefaultResizer();
    return draft;
  });

  await ctx.answerCallbackQuery({ text: 'Resizer cleared.' });
  await renderScreen(ctx, 'resizer', updated);
});

bot.callbackQuery('nav:buy_sell', async (ctx) => {
  const user = await getUserState(String(ctx.from.id));
  await ctx.answerCallbackQuery();
  await renderScreen(ctx, 'buy_sell', user);
});

bot.callbackQuery('nav:buy_sell_wallets', async (ctx) => {
  const updated = await refreshTradingDesk(String(ctx.from.id));
  await ctx.answerCallbackQuery();
  await renderScreen(ctx, 'buy_sell_wallets', updated);
});

bot.callbackQuery('nav:buy_sell_quick', async (ctx) => {
  const user = await getUserState(String(ctx.from.id));
  await ctx.answerCallbackQuery();
  await renderScreen(ctx, 'buy_sell_quick', user);
});

bot.callbackQuery('nav:buy_sell_limit', async (ctx) => {
  const user = await getUserState(String(ctx.from.id));
  await ctx.answerCallbackQuery();
  await renderScreen(ctx, 'buy_sell_limit', user);
});

bot.callbackQuery('nav:buy_sell_copy', async (ctx) => {
  const user = await getUserState(String(ctx.from.id));
  await ctx.answerCallbackQuery();
  await renderScreen(ctx, 'buy_sell_copy', user);
});

bot.callbackQuery('noop', async (ctx) => {
  await ctx.answerCallbackQuery();
});

bot.callbackQuery('buy_sell:import_wallet', async (ctx) => {
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.tradingDesk = normalizeTradingDesk({
      ...draft.tradingDesk,
      awaitingField: 'import_wallet',
      lastError: null,
    });
    return draft;
  });
  await ctx.answerCallbackQuery({ text: promptForBuySellField('import_wallet') });
  await renderScreen(ctx, 'buy_sell_wallets', updated);
});

bot.callbackQuery('buy_sell:generate_wallet', async (ctx) => {
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    const tradingDesk = normalizeTradingDesk(draft.tradingDesk);
    const nextWallet = createTradingWallet(false);
    tradingDesk.wallets = [...tradingDesk.wallets, nextWallet];
    tradingDesk.activeWalletId = nextWallet.id;
    tradingDesk.awaitingField = null;
    tradingDesk.lastError = null;
    draft.tradingDesk = normalizeTradingDesk(tradingDesk);
    return draft;
  });
  await ctx.answerCallbackQuery({ text: 'New trading wallet generated.' });
  await renderScreen(ctx, 'buy_sell_wallets', updated);
});

bot.callbackQuery(/^buy_sell:select_wallet:(.+)$/, async (ctx) => {
  const walletId = ctx.match[1];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.tradingDesk = normalizeTradingDesk({
      ...draft.tradingDesk,
      activeWalletId: walletId,
      awaitingField: null,
      wallets: (draft.tradingDesk?.wallets || []).map((wallet) => ({
        ...wallet,
        privateKeyVisible: wallet.id === walletId ? wallet.privateKeyVisible : false,
      })),
    });
    return draft;
  });
  await ctx.answerCallbackQuery({ text: 'Active wallet selected.' });
  await renderScreen(ctx, 'buy_sell_wallets', updated);
});

bot.callbackQuery(/^buy_sell:key_toggle:(.+)$/, async (ctx) => {
  const walletId = ctx.match[1];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.tradingDesk = normalizeTradingDesk({
      ...draft.tradingDesk,
      wallets: (draft.tradingDesk?.wallets || []).map((wallet) => ({
        ...wallet,
        privateKeyVisible: wallet.id === walletId ? !wallet.privateKeyVisible : false,
      })),
    });
    return draft;
  });
  const activeWallet = getActiveTradingWallet(updated);
  await ctx.answerCallbackQuery({ text: activeWallet?.privateKeyVisible ? 'Private key revealed.' : 'Private key hidden.' });
  await renderScreen(ctx, 'buy_sell_wallets', updated);
});

bot.callbackQuery('buy_sell:set_ca', async (ctx) => {
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.tradingDesk = normalizeTradingDesk({
      ...draft.tradingDesk,
      awaitingField: 'quick_trade_mint',
      lastError: null,
    });
    return draft;
  });
  await ctx.answerCallbackQuery({ text: promptForBuySellField('quick_trade_mint') });
  await renderScreen(ctx, 'buy_sell_quick', updated);
});

bot.callbackQuery('buy_sell:set_buy_sol', async (ctx) => {
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.tradingDesk = normalizeTradingDesk({
      ...draft.tradingDesk,
      awaitingField: 'quick_buy_sol',
      lastError: null,
    });
    return draft;
  });
  await ctx.answerCallbackQuery({ text: promptForBuySellField('quick_buy_sol') });
  await renderScreen(ctx, 'buy_sell_quick', updated);
});

bot.callbackQuery('buy_sell:set_sell_percent', async (ctx) => {
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.tradingDesk = normalizeTradingDesk({
      ...draft.tradingDesk,
      awaitingField: 'quick_sell_percent',
      lastError: null,
    });
    return draft;
  });
  await ctx.answerCallbackQuery({ text: promptForBuySellField('quick_sell_percent') });
  await renderScreen(ctx, 'buy_sell_quick', updated);
});

bot.callbackQuery(/^buy_sell:execute:(buy|sell)$/, async (ctx) => {
  const action = ctx.match[1];
  const userId = String(ctx.from.id);
  const user = await getUserState(userId);
  const desk = normalizeTradingDesk(user.tradingDesk);
  const activeWallet = getActiveTradingWallet(user);

  if (!desk.quickTradeMintAddress) {
    await ctx.answerCallbackQuery({ text: 'Set the token CA first.' });
    await renderScreen(ctx, 'buy_sell_quick', user);
    return;
  }

  if (!activeWallet && !desk.selectedMagicBundleId) {
    await ctx.answerCallbackQuery({ text: 'Add a trading wallet or choose a bundle first.' });
    await renderScreen(ctx, 'buy_sell_quick', user);
    return;
  }

  if (action === 'buy' && !desk.quickBuyLamports) {
    await ctx.answerCallbackQuery({ text: 'Set the buy size first.' });
    await renderScreen(ctx, 'buy_sell_quick', user);
    return;
  }

  if (action === 'sell' && (!Number.isInteger(desk.quickSellPercent) || desk.quickSellPercent <= 0)) {
    await ctx.answerCallbackQuery({ text: 'Set the sell percentage first.' });
    await renderScreen(ctx, 'buy_sell_quick', user);
    return;
  }

  const updated = await updateUserState(userId, (draft) => {
    const tradingDesk = normalizeTradingDesk(draft.tradingDesk);
    tradingDesk.pendingAction = {
      type: action,
      requestedAt: new Date().toISOString(),
    };
    tradingDesk.lastError = null;
    draft.tradingDesk = normalizeTradingDesk(tradingDesk);
    return draft;
  });

  await appendUserActivityLog(userId, {
    scope: 'buy_sell',
    level: 'info',
    message: `Queued a ${action.toUpperCase()} action from the Buy / Sell desk.`,
  });

  await ctx.answerCallbackQuery({ text: `${action === 'buy' ? 'Buy' : 'Sell'} queued.` });
  await renderScreen(ctx, 'buy_sell_quick', updated);
});

bot.callbackQuery(/^buy_sell:limit:side:(buy|sell)$/, async (ctx) => {
  const side = ctx.match[1];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    const tradingDesk = normalizeTradingDesk(draft.tradingDesk);
    tradingDesk.limitOrder = {
      ...tradingDesk.limitOrder,
      side,
      lastError: null,
    };
    draft.tradingDesk = normalizeTradingDesk(tradingDesk);
    return draft;
  });
  await ctx.answerCallbackQuery({ text: `${side === 'buy' ? 'Buy' : 'Sell'} trigger selected.` });
  await renderScreen(ctx, 'buy_sell_limit', updated);
});

bot.callbackQuery('buy_sell:limit:set_trigger', async (ctx) => {
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.tradingDesk = normalizeTradingDesk({
      ...draft.tradingDesk,
      awaitingField: 'limit_trigger_market_cap',
      lastError: null,
    });
    return draft;
  });
  await ctx.answerCallbackQuery({ text: promptForBuySellField('limit_trigger_market_cap') });
  await renderScreen(ctx, 'buy_sell_limit', updated);
});

bot.callbackQuery('buy_sell:limit:set_buy_sol', async (ctx) => {
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.tradingDesk = normalizeTradingDesk({
      ...draft.tradingDesk,
      awaitingField: 'limit_buy_sol',
      lastError: null,
    });
    return draft;
  });
  await ctx.answerCallbackQuery({ text: promptForBuySellField('limit_buy_sol') });
  await renderScreen(ctx, 'buy_sell_limit', updated);
});

bot.callbackQuery('buy_sell:limit:set_sell_percent', async (ctx) => {
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.tradingDesk = normalizeTradingDesk({
      ...draft.tradingDesk,
      awaitingField: 'limit_sell_percent',
      lastError: null,
    });
    return draft;
  });
  await ctx.answerCallbackQuery({ text: promptForBuySellField('limit_sell_percent') });
  await renderScreen(ctx, 'buy_sell_limit', updated);
});

bot.callbackQuery('buy_sell:limit:toggle', async (ctx) => {
  const userId = String(ctx.from.id);
  const user = await getUserState(userId);
  const desk = normalizeTradingDesk(user.tradingDesk);
  const activeWallet = getActiveTradingWallet(user);

  if (!desk.quickTradeMintAddress) {
    await ctx.answerCallbackQuery({ text: 'Set the token CA first.' });
    await renderScreen(ctx, 'buy_sell_limit', user);
    return;
  }

  if (!activeWallet && !desk.selectedMagicBundleId) {
    await ctx.answerCallbackQuery({ text: 'Add a wallet or choose a bundle first.' });
    await renderScreen(ctx, 'buy_sell_limit', user);
    return;
  }

  if (!desk.limitOrder.triggerMarketCapUsd) {
    await ctx.answerCallbackQuery({ text: 'Set a trigger market cap first.' });
    await renderScreen(ctx, 'buy_sell_limit', user);
    return;
  }

  if (desk.limitOrder.side === 'buy' && !desk.limitOrder.buyLamports) {
    await ctx.answerCallbackQuery({ text: 'Set the buy size first.' });
    await renderScreen(ctx, 'buy_sell_limit', user);
    return;
  }

  const updated = await updateUserState(userId, (draft) => {
    const tradingDesk = normalizeTradingDesk(draft.tradingDesk);
    tradingDesk.limitOrder = {
      ...tradingDesk.limitOrder,
      enabled: !tradingDesk.limitOrder.enabled,
      lastError: null,
    };
    draft.tradingDesk = normalizeTradingDesk(tradingDesk);
    return draft;
  });

  await ctx.answerCallbackQuery({ text: updated.tradingDesk.limitOrder.enabled ? 'Limit order armed.' : 'Limit order stopped.' });
  await renderScreen(ctx, 'buy_sell_limit', updated);
});

bot.callbackQuery('buy_sell:limit:clear', async (ctx) => {
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    const tradingDesk = normalizeTradingDesk(draft.tradingDesk);
    tradingDesk.limitOrder = {
      ...createDefaultTradingDesk().limitOrder,
    };
    tradingDesk.awaitingField = null;
    tradingDesk.lastError = null;
    draft.tradingDesk = normalizeTradingDesk(tradingDesk);
    return draft;
  });
  await ctx.answerCallbackQuery({ text: 'Limit order cleared.' });
  await renderScreen(ctx, 'buy_sell_limit', updated);
});

bot.callbackQuery('buy_sell:copy:set_wallet', async (ctx) => {
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.tradingDesk = normalizeTradingDesk({
      ...draft.tradingDesk,
      awaitingField: 'copy_follow_wallet',
      lastError: null,
    });
    return draft;
  });
  await ctx.answerCallbackQuery({ text: promptForBuySellField('copy_follow_wallet') });
  await renderScreen(ctx, 'buy_sell_copy', updated);
});

bot.callbackQuery('buy_sell:copy:set_amount', async (ctx) => {
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.tradingDesk = normalizeTradingDesk({
      ...draft.tradingDesk,
      awaitingField: 'copy_fixed_buy_sol',
      lastError: null,
    });
    return draft;
  });
  await ctx.answerCallbackQuery({ text: promptForBuySellField('copy_fixed_buy_sol') });
  await renderScreen(ctx, 'buy_sell_copy', updated);
});

bot.callbackQuery('buy_sell:copy:toggle_sells', async (ctx) => {
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    const tradingDesk = normalizeTradingDesk(draft.tradingDesk);
    tradingDesk.copyTrade = {
      ...tradingDesk.copyTrade,
      copySells: !tradingDesk.copyTrade.copySells,
      lastError: null,
    };
    draft.tradingDesk = normalizeTradingDesk(tradingDesk);
    return draft;
  });
  await ctx.answerCallbackQuery({ text: updated.tradingDesk.copyTrade.copySells ? 'Copy sells enabled.' : 'Copy sells disabled.' });
  await renderScreen(ctx, 'buy_sell_copy', updated);
});

bot.callbackQuery('buy_sell:copy:toggle', async (ctx) => {
  const userId = String(ctx.from.id);
  const user = await getUserState(userId);
  const desk = normalizeTradingDesk(user.tradingDesk);
  const activeWallet = getActiveTradingWallet(user);

  if (!desk.copyTrade.enabled) {
    if (!desk.copyTrade.followWalletAddress) {
      await ctx.answerCallbackQuery({ text: 'Set the follow wallet first.' });
      await renderScreen(ctx, 'buy_sell_copy', user);
      return;
    }
    if (!desk.copyTrade.fixedBuyLamports) {
      await ctx.answerCallbackQuery({ text: 'Set the copy buy size first.' });
      await renderScreen(ctx, 'buy_sell_copy', user);
      return;
    }
    if (!activeWallet && !desk.selectedMagicBundleId) {
      await ctx.answerCallbackQuery({ text: 'Add a wallet or choose a bundle first.' });
      await renderScreen(ctx, 'buy_sell_copy', user);
      return;
    }
  }

  const updated = await updateUserState(userId, (draft) => {
    const tradingDesk = normalizeTradingDesk(draft.tradingDesk);
    tradingDesk.copyTrade = {
      ...tradingDesk.copyTrade,
      enabled: !tradingDesk.copyTrade.enabled,
      lastError: null,
    };
    draft.tradingDesk = normalizeTradingDesk(tradingDesk);
    return draft;
  });

  await ctx.answerCallbackQuery({ text: updated.tradingDesk.copyTrade.enabled ? 'Copy trading started.' : 'Copy trading stopped.' });
  await renderScreen(ctx, 'buy_sell_copy', updated);
});

bot.callbackQuery('nav:burn_agent', async (ctx) => {
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.activeBurnAgentId = null;
    return draft;
  });
  await ctx.answerCallbackQuery();
  await renderScreen(ctx, 'burn_agent', updated);
});

bot.callbackQuery('nav:burn_agent_archive', async (ctx) => {
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.activeBurnAgentId = null;
    return draft;
  });
  await ctx.answerCallbackQuery();
  await renderScreen(ctx, 'burn_agent_archive', updated);
});

bot.callbackQuery('nav:holder_booster', async (ctx) => {
  const userId = String(ctx.from.id);
  const user = await getUserState(userId);
  const updated = (!user.holderBooster.mintAddress && !user.holderBooster.walletAddress && !user.holderBooster.awaitingField)
    ? await updateUserState(userId, (draft) => {
      draft.holderBooster = normalizeHolderBooster({
        ...draft.holderBooster,
        awaitingField: 'mint',
        status: 'idle',
      });
      return draft;
    })
    : user;
  await ctx.answerCallbackQuery();
  await renderScreen(ctx, 'holder_booster', updated);
});

bot.callbackQuery('nav:fomo_booster', async (ctx) => {
  const user = await getUserState(String(ctx.from.id));
  await ctx.answerCallbackQuery();
  await renderScreen(ctx, 'fomo_booster', user);
});

bot.callbackQuery('nav:sniper_wizard', async (ctx) => {
  const userId = String(ctx.from.id);
  const updated = await refreshSniperWizard(userId);
  await ctx.answerCallbackQuery();
  await renderScreen(ctx, 'sniper_wizard', updated);
});

bot.callbackQuery('fomo:refresh', async (ctx) => {
  const updated = await refreshFomoBooster(String(ctx.from.id));
  await ctx.answerCallbackQuery({ text: 'FOMO Booster refreshed.' });
  await renderScreen(ctx, 'fomo_booster', updated);
});

bot.callbackQuery(/^fomo:set:(token_name|mint|wallet_count|buy_range|interval_range)$/, async (ctx) => {
  const field = ctx.match[1];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.fomoBooster = normalizeFomoBooster({
      ...draft.fomoBooster,
      awaitingField: field,
      updatedAt: new Date().toISOString(),
    });
    return draft;
  });

  await ctx.answerCallbackQuery({ text: promptForFomoField(field) });
  await renderScreen(ctx, 'fomo_booster', updated);
});

bot.callbackQuery('fomo:key:toggle', async (ctx) => {
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.fomoBooster = normalizeFomoBooster({
      ...draft.fomoBooster,
      privateKeyVisible: !draft.fomoBooster.privateKeyVisible,
      updatedAt: new Date().toISOString(),
    });
    return draft;
  });

  await ctx.answerCallbackQuery({
    text: updated.fomoBooster.privateKeyVisible ? 'Private key revealed.' : 'Private key hidden.',
  });
  await renderScreen(ctx, 'fomo_booster', updated);
});

bot.callbackQuery('fomo:toggle', async (ctx) => {
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    const current = normalizeFomoBooster(draft.fomoBooster);
    const automationEnabled = !current.automationEnabled;
    draft.fomoBooster = normalizeFomoBooster({
      ...current,
      automationEnabled,
      status: automationEnabled ? 'running' : 'stopped',
      awaitingField: null,
      lastError: null,
      updatedAt: new Date().toISOString(),
    });
    return draft;
  });

  await appendUserActivityLog(String(ctx.from.id), {
    scope: fomoBoosterScope(updated.fomoBooster.id),
    level: 'info',
    message: updated.fomoBooster.automationEnabled
      ? 'FOMO Booster automation started from Telegram.'
      : 'FOMO Booster automation stopped from Telegram.',
  });

  await ctx.answerCallbackQuery({
    text: updated.fomoBooster.automationEnabled ? 'FOMO Booster started.' : 'FOMO Booster stopped.',
  });
  await renderScreen(ctx, 'fomo_booster', updated);
});

bot.callbackQuery('fomo:locked:toggle', async (ctx) => {
  const user = await getUserState(String(ctx.from.id));
  await ctx.answerCallbackQuery({ text: 'Set the mint, wallet count, buy range, and delay range first.' });
  await renderScreen(ctx, 'fomo_booster', user);
});

bot.callbackQuery('fomo:withdraw', async (ctx) => {
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.fomoBooster = normalizeFomoBooster({
      ...draft.fomoBooster,
      awaitingField: 'withdraw_address',
      updatedAt: new Date().toISOString(),
    });
    return draft;
  });

  await ctx.answerCallbackQuery({ text: promptForFomoField('withdraw_address') });
  await renderScreen(ctx, 'fomo_booster', updated);
});

bot.callbackQuery('fomo:locked:withdraw', async (ctx) => {
  const user = await getUserState(String(ctx.from.id));
  await ctx.answerCallbackQuery({ text: 'The FOMO Booster deposit wallet is not ready yet.' });
  await renderScreen(ctx, 'fomo_booster', user);
});

bot.callbackQuery('sniper:refresh', async (ctx) => {
  const updated = await refreshSniperWizard(String(ctx.from.id));
  await ctx.answerCallbackQuery({ text: 'Sniper Wizard refreshed.' });
  await renderScreen(ctx, 'sniper_wizard', updated);
});

bot.callbackQuery(/^sniper:set:(target_wallet)$/, async (ctx) => {
  const field = ctx.match[1];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.sniperWizard = normalizeSniperWizard({
      ...draft.sniperWizard,
      awaitingField: field,
      updatedAt: new Date().toISOString(),
    });
    syncSniperWizardTradingDesk(draft);
    return draft;
  });
  await ctx.answerCallbackQuery({ text: promptForSniperField(field) });
  await renderScreen(ctx, 'sniper_wizard', updated);
});

bot.callbackQuery(/^sniper:set:mode:(standard|magic)$/, async (ctx) => {
  const sniperMode = ctx.match[1];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    const current = normalizeSniperWizard(draft.sniperWizard);
    const next = normalizeSniperWizard({
      ...current,
      sniperMode,
      awaitingField: current.targetWalletAddress ? current.awaitingField : 'target_wallet',
      updatedAt: new Date().toISOString(),
    });
    draft.sniperWizard = next;
    syncSniperWizardTradingDesk(draft, { selectFirst: true });
    return draft;
  });
  await ctx.answerCallbackQuery({ text: sniperMode === 'magic' ? 'Magic Sniper selected.' : 'Normal Sniper selected.' });
  await renderScreen(ctx, 'sniper_wizard', updated);
});

bot.callbackQuery('sniper:set:wallet_count', async (ctx) => {
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.sniperWizard = normalizeSniperWizard({
      ...draft.sniperWizard,
      awaitingField: 'wallet_count',
      updatedAt: new Date().toISOString(),
    });
    syncSniperWizardTradingDesk(draft);
    return draft;
  });
  await ctx.answerCallbackQuery({ text: promptForSniperField('wallet_count') });
  await renderScreen(ctx, 'sniper_wizard', updated);
});

bot.callbackQuery(/^sniper:set:percent:(25|50|75|100)$/, async (ctx) => {
  const percent = Number.parseInt(ctx.match[1], 10);
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.sniperWizard = normalizeSniperWizard({
      ...draft.sniperWizard,
      snipePercent: percent,
      awaitingField: null,
      updatedAt: new Date().toISOString(),
    });
    syncSniperWizardTradingDesk(draft);
    return draft;
  });
  await ctx.answerCallbackQuery({ text: `Sniper size set to ${percent}%.` });
  await renderScreen(ctx, 'sniper_wizard', updated);
});

bot.callbackQuery('sniper:set:percent:custom', async (ctx) => {
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.sniperWizard = normalizeSniperWizard({
      ...draft.sniperWizard,
      awaitingField: 'custom_percent',
      updatedAt: new Date().toISOString(),
    });
    syncSniperWizardTradingDesk(draft);
    return draft;
  });
  await ctx.answerCallbackQuery({ text: promptForSniperField('custom_percent') });
  await renderScreen(ctx, 'sniper_wizard', updated);
});

bot.callbackQuery('sniper:key:toggle', async (ctx) => {
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.sniperWizard = normalizeSniperWizard({
      ...draft.sniperWizard,
      privateKeyVisible: !draft.sniperWizard.privateKeyVisible,
      updatedAt: new Date().toISOString(),
    });
    syncSniperWizardTradingDesk(draft);
    return draft;
  });
  await ctx.answerCallbackQuery({
    text: updated.sniperWizard.privateKeyVisible ? 'Private key revealed.' : 'Private key hidden.',
  });
  await renderScreen(ctx, 'sniper_wizard', updated);
});

bot.callbackQuery('sniper:toggle', async (ctx) => {
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    const current = normalizeSniperWizard(draft.sniperWizard);
    const automationEnabled = !current.automationEnabled;
    draft.sniperWizard = normalizeSniperWizard({
      ...current,
      automationEnabled,
      status: automationEnabled ? 'watching' : 'stopped',
      awaitingField: null,
      lastError: null,
      updatedAt: new Date().toISOString(),
    });
    syncSniperWizardTradingDesk(draft);
    return draft;
  });

  await appendUserActivityLog(String(ctx.from.id), {
    scope: sniperWizardScope(updated.sniperWizard.id),
    level: 'info',
    message: updated.sniperWizard.automationEnabled
      ? 'Sniper Wizard started from Telegram.'
      : 'Sniper Wizard stopped from Telegram.',
  });

  await ctx.answerCallbackQuery({
    text: updated.sniperWizard.automationEnabled ? 'Sniper Wizard started.' : 'Sniper Wizard stopped.',
  });
  await renderScreen(ctx, 'sniper_wizard', updated);
});

bot.callbackQuery('sniper:locked:toggle', async (ctx) => {
  const user = await getUserState(String(ctx.from.id));
  await ctx.answerCallbackQuery({ text: 'Set the target wallet and snipe percentage first.' });
  await renderScreen(ctx, 'sniper_wizard', user);
});

bot.callbackQuery('sniper:withdraw', async (ctx) => {
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.sniperWizard = normalizeSniperWizard({
      ...draft.sniperWizard,
      awaitingField: 'withdraw_address',
      updatedAt: new Date().toISOString(),
    });
    syncSniperWizardTradingDesk(draft);
    return draft;
  });
  await ctx.answerCallbackQuery({ text: promptForSniperField('withdraw_address') });
  await renderScreen(ctx, 'sniper_wizard', updated);
});

bot.callbackQuery('sniper:locked:withdraw', async (ctx) => {
  const user = await getUserState(String(ctx.from.id));
  await ctx.answerCallbackQuery({ text: 'The Sniper Wizard wallet is not ready yet.' });
  await renderScreen(ctx, 'sniper_wizard', user);
});

bot.callbackQuery('sniper:locked:add_to_trading', async (ctx) => {
  const user = await getUserState(String(ctx.from.id));
  await ctx.answerCallbackQuery({ text: 'Generate or import the sniper wallets first.' });
  await renderScreen(ctx, 'sniper_wizard', user);
});

bot.callbackQuery('sniper:add_to_trading', async (ctx) => {
  const userId = String(ctx.from.id);
  const updated = await updateUserState(userId, (draft) => {
    syncSniperWizardTradingDesk(draft, { selectFirst: true });
    return draft;
  });
  await ctx.answerCallbackQuery({ text: 'Sniper Wizard wallets added to Buy / Sell.' });
  await renderScreen(ctx, 'sniper_wizard', updated);
});

bot.callbackQuery('nav:community_vision', async (ctx) => {
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.activeCommunityVisionId = null;
    return draft;
  });
  await ctx.answerCallbackQuery();
  await renderScreen(ctx, 'community_vision', updated);
});

bot.callbackQuery('nav:community_vision_archive', async (ctx) => {
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.activeCommunityVisionId = null;
    return draft;
  });
  await ctx.answerCallbackQuery();
  await renderScreen(ctx, 'community_vision_archive', updated);
});

bot.callbackQuery('communityvision:new', async (ctx) => {
  const userId = String(ctx.from.id);
  const updated = await updateUserState(userId, (draft) => {
    appendCommunityVisionToDraft(draft, createDefaultCommunityVision());
    return draft;
  });
  await appendUserActivityLog(userId, {
    scope: communityVisionScope(updated.communityVision.id),
    level: 'info',
    message: 'Created a new Vision watch.',
  });
  await ctx.answerCallbackQuery({ text: 'Vision created.' });
  await renderScreen(ctx, 'community_vision_editor', updated);
});

bot.callbackQuery(/^communityvision:open:(.+)$/, async (ctx) => {
  const communityVisionId = ctx.match[1];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.activeCommunityVisionId = communityVisionId;
    syncActiveCommunityVisionDraft(draft);
    return draft;
  });
  await ctx.answerCallbackQuery();
  await renderScreen(ctx, 'community_vision_editor', updated);
});

bot.callbackQuery(/^communityvision:set:(profile_url):(.+)$/, async (ctx) => {
  const field = ctx.match[1];
  const communityVisionId = ctx.match[2];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    updateCommunityVisionInDraft(draft, communityVisionId, (order) => ({
      ...order,
      awaitingField: field,
      deleteConfirmations: 0,
      updatedAt: new Date().toISOString(),
    }));
    return draft;
  });
  await ctx.answerCallbackQuery({ text: promptForCommunityVisionField(field) });
  await renderScreen(ctx, 'community_vision_editor', updated);
});

bot.callbackQuery('communityvision:locked:toggle', async (ctx) => {
  const user = await getUserState(String(ctx.from.id));
  await ctx.answerCallbackQuery({ text: 'Set the X account first.' });
  await renderScreen(ctx, 'community_vision_editor', user);
});

bot.callbackQuery(/^communityvision:toggle:(.+)$/, async (ctx) => {
  const communityVisionId = ctx.match[1];
  const userId = String(ctx.from.id);
  const updated = await updateUserState(userId, (draft) => {
    updateCommunityVisionInDraft(draft, communityVisionId, (order) => {
      const automationEnabled = !order.automationEnabled;
      return {
        ...order,
        automationEnabled,
        status: automationEnabled ? 'watching' : 'stopped',
        awaitingField: null,
        deleteConfirmations: 0,
        lastError: null,
        updatedAt: new Date().toISOString(),
      };
    });
    return draft;
  });
  await appendUserActivityLog(userId, {
    scope: communityVisionScope(communityVisionId),
    level: 'info',
    message: updated.communityVision.automationEnabled
      ? 'Vision started from Telegram.'
      : 'Vision stopped from Telegram.',
  });
  await ctx.answerCallbackQuery({
    text: updated.communityVision.automationEnabled ? 'Community watch started.' : 'Community watch stopped.',
  });
  await renderScreen(ctx, 'community_vision_editor', updated);
});

bot.callbackQuery(/^communityvision:archive:(.+)$/, async (ctx) => {
  const communityVisionId = ctx.match[1];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    updateCommunityVisionInDraft(draft, communityVisionId, (order) => ({
      ...order,
      archivedAt: new Date().toISOString(),
      automationEnabled: false,
      status: 'stopped',
      deleteConfirmations: 0,
      updatedAt: new Date().toISOString(),
    }));
    return draft;
  });
  await ctx.answerCallbackQuery({ text: 'Watch archived.' });
  await renderScreen(ctx, 'community_vision', updated);
});

bot.callbackQuery(/^communityvision:restore:(.+)$/, async (ctx) => {
  const communityVisionId = ctx.match[1];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    updateCommunityVisionInDraft(draft, communityVisionId, (order) => ({
      ...order,
      archivedAt: null,
      deleteConfirmations: 0,
      updatedAt: new Date().toISOString(),
    }));
    return draft;
  });
  await ctx.answerCallbackQuery({ text: 'Watch restored.' });
  await renderScreen(ctx, 'community_vision_editor', updated);
});

bot.callbackQuery(/^communityvision:delete:(.+)$/, async (ctx) => {
  const communityVisionId = ctx.match[1];
  const userId = String(ctx.from.id);
  const current = await getUserState(userId);
  const order = current.communityVisions.find((item) => item.id === communityVisionId) ?? current.communityVision;
  if (!order?.archivedAt) {
    await ctx.answerCallbackQuery({ text: 'Archive it first before deleting.' });
    await renderScreen(ctx, 'community_vision_editor', current);
    return;
  }

  let deleted = false;
  const updated = await updateUserState(userId, (draft) => {
    const match = draft.communityVisions.find((item) => item.id === communityVisionId);
    if (match?.deleteConfirmations >= 1) {
      draft.communityVisions = draft.communityVisions.filter((item) => item.id !== communityVisionId);
      draft.activeCommunityVisionId = draft.communityVisions.find((item) => !item.archivedAt)?.id
        ?? draft.communityVisions[0]?.id
        ?? null;
      syncActiveCommunityVisionDraft(draft);
      deleted = true;
    } else {
      updateCommunityVisionInDraft(draft, communityVisionId, (item) => ({
        ...item,
        deleteConfirmations: 1,
        updatedAt: new Date().toISOString(),
      }));
    }
    return draft;
  });
  await ctx.answerCallbackQuery({ text: deleted ? 'Watch deleted.' : 'Tap again to delete permanently.' });
  await renderScreen(ctx, deleted ? 'community_vision_archive' : 'community_vision_editor', updated);
});

bot.callbackQuery('nav:wallet_tracker', async (ctx) => {
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.activeWalletTrackerId = null;
    return draft;
  });
  await ctx.answerCallbackQuery();
  await renderScreen(ctx, 'wallet_tracker', updated);
});

bot.callbackQuery('nav:wallet_tracker_archive', async (ctx) => {
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.activeWalletTrackerId = null;
    return draft;
  });
  await ctx.answerCallbackQuery();
  await renderScreen(ctx, 'wallet_tracker_archive', updated);
});

bot.callbackQuery('nav:staking', async (ctx) => {
  await ctx.answerCallbackQuery();
  await renderScreen(ctx, 'staking', await getUserState(String(ctx.from.id)));
});

bot.callbackQuery('nav:x_followers', async (ctx) => {
  const user = await getUserState(String(ctx.from.id));
  await ctx.answerCallbackQuery();
  await renderScreen(ctx, 'x_followers', user);
});

bot.callbackQuery('nav:engagement', async (ctx) => {
  await ctx.answerCallbackQuery();
  await renderScreen(ctx, 'engagement', await getUserState(String(ctx.from.id)));
});

bot.callbackQuery('nav:subscriptions_accounts', async (ctx) => {
  await ctx.answerCallbackQuery();
  await renderScreen(ctx, 'subscriptions_accounts', await getUserState(String(ctx.from.id)));
});

bot.callbackQuery('nav:subscriptions_catalog', async (ctx) => {
  await ctx.answerCallbackQuery();
  await renderScreen(ctx, 'subscriptions_catalog', await getUserState(String(ctx.from.id)));
});

bot.callbackQuery('staking:link_active_wallet', async (ctx) => {
  const userId = String(ctx.from.id);
  const current = await getUserState(userId);
  const activeWallet = getActiveTradingWallet(current);
  if (!activeWallet?.address) {
    await ctx.answerCallbackQuery({ text: 'Pick a Buy / Sell wallet first.' });
    await renderScreen(ctx, 'staking', current);
    return;
  }

  const updated = await updateUserState(userId, (draft) => {
    draft.staking = normalizeStakingState({
      ...draft.staking,
      walletAddress: activeWallet.address,
      sourceWalletId: activeWallet.id,
      status: 'tracking',
      lastError: null,
    });
    return draft;
  });
  await ctx.answerCallbackQuery({ text: 'Active Buy / Sell wallet linked to staking.' });
  await renderScreen(ctx, 'staking', updated);
});

bot.callbackQuery('staking:claim', async (ctx) => {
  const userId = String(ctx.from.id);
  const user = await getUserState(userId);
  const state = normalizeStakingState(user.staking);

  if (!state.walletAddress) {
    await ctx.answerCallbackQuery({ text: 'Link a staking wallet first.' });
    await renderScreen(ctx, 'staking', user);
    return;
  }

  if (state.claimableLamports < state.claimThresholdLamports) {
    await ctx.answerCallbackQuery({
      text: `Nothing claimable yet. Minimum claim is ${formatSolAmountFromLamports(state.claimThresholdLamports)} SOL.`,
    });
    await renderScreen(ctx, 'staking', user);
    return;
  }

  try {
    const result = await sendStakingClaimRewards(user);
    const updated = await updateUserState(userId, (draft) => {
      draft.staking = normalizeStakingState({
        ...draft.staking,
        claimableLamports: 0,
        totalClaimedLamports: (draft.staking?.totalClaimedLamports || 0) + result.claimedLamports,
        lastClaimedLamports: result.claimedLamports,
        lastClaimedAt: new Date().toISOString(),
        lastClaimSignature: result.signature,
        status: 'tracking',
        lastError: null,
      });
      return draft;
    });
    await appendUserActivityLog(userId, {
      scope: 'staking',
      level: 'info',
      message: `Manual staking claim sent ${formatSolAmountFromLamports(result.claimedLamports)} SOL to ${updated.staking.walletAddress}.`,
    });
    await ctx.answerCallbackQuery({
      text: `Claim sent: ${formatSolAmountFromLamports(result.claimedLamports)} SOL.`,
    });
    await renderScreen(ctx, 'staking', updated);
  } catch (error) {
    const updated = await updateUserState(userId, (draft) => {
      draft.staking = normalizeStakingState({
        ...draft.staking,
        lastError: String(error.message || error),
      });
      return draft;
    });
    await ctx.answerCallbackQuery({ text: String(error.message || error) });
    await renderScreen(ctx, 'staking', updated);
  }
});

bot.callbackQuery('staking:request_unstake', async (ctx) => {
  const user = await getUserState(String(ctx.from.id));
  const state = normalizeStakingState(user.staking);
  const text = state.walletAddress
    ? `The launch-mode tracker is live now. The hard-staking deposit flow will use a ${formatDayCountLabel(STAKING_UNSTAKE_COOLDOWN_DAYS)} cooldown once that path is exposed.`
    : 'Link a staking wallet first.';
  await ctx.answerCallbackQuery({ text });
  await renderScreen(ctx, 'staking', user);
});

bot.callbackQuery('staking:explain_epoch', async (ctx) => {
  await ctx.answerCallbackQuery({
    text: `Rewards build continuously. Live weights are 0-6 days at 0.25x, 7-29 days at 1.0x, 30-89 days at 1.25x, 90-179 days at 1.5x, and 180+ days at 2.0x. Claim any time once you cross ${formatSolAmountFromLamports(STAKING_MIN_CLAIM_LAMPORTS)} SOL.`,
  });
  await renderScreen(ctx, 'staking', await getUserState(String(ctx.from.id)));
});

bot.callbackQuery('nav:accounts_catalog', async (ctx) => {
  await ctx.answerCallbackQuery();
  await renderScreen(ctx, 'accounts_catalog', await getUserState(String(ctx.from.id)));
});

bot.callbackQuery(/^xfollowers:package:(.+)$/, async (ctx) => {
  const packageKey = ctx.match[1];
  const pkg = getXFollowersPackage(packageKey);
  if (!pkg) {
    await ctx.answerCallbackQuery({ text: 'That package is not configured.' });
    return;
  }

  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.xFollowers = normalizeXFollowersState({
      ...draft.xFollowers,
      packageKey,
      payment: createDefaultPaymentState(),
      providerCostUsd: pkg.providerCostUsd,
      sellPriceUsd: pkg.usdPrice,
      estimatedProfitUsd: Number((pkg.usdPrice - pkg.providerCostUsd).toFixed(2)),
      estimatedTreasuryShareUsd: Number((((pkg.usdPrice - pkg.providerCostUsd) / 2)).toFixed(2)),
      estimatedBurnShareUsd: Number((((pkg.usdPrice - pkg.providerCostUsd) / 2)).toFixed(2)),
      lastError: null,
    });
    return draft;
  });
  await ctx.answerCallbackQuery({ text: `${pkg.label} selected.` });
  await renderScreen(ctx, 'x_followers', updated);
});

bot.callbackQuery('xfollowers:set:target', async (ctx) => {
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.xFollowers = normalizeXFollowersState({
      ...draft.xFollowers,
      awaitingField: 'target',
      lastError: null,
    });
    return draft;
  });
  await ctx.answerCallbackQuery({ text: 'Send the X link or @handle now.' });
  await renderScreen(ctx, 'x_followers', updated);
});

bot.callbackQuery('wallettracker:new', async (ctx) => {
  const userId = String(ctx.from.id);
  const updated = await updateUserState(userId, (draft) => {
    appendWalletTrackerToDraft(draft, createDefaultWalletTracker());
    return draft;
  });
  await appendUserActivityLog(userId, {
    scope: walletTrackerScope(updated.walletTracker.id),
    level: 'info',
    message: 'Created a new Wallet Tracker watch.',
  });
  await ctx.answerCallbackQuery({ text: 'Wallet Tracker created.' });
  await renderScreen(ctx, 'wallet_tracker_editor', updated);
});

bot.callbackQuery(/^wallettracker:open:(.+)$/, async (ctx) => {
  const walletTrackerId = ctx.match[1];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.activeWalletTrackerId = walletTrackerId;
    syncActiveWalletTrackerDraft(draft);
    return draft;
  });
  await ctx.answerCallbackQuery();
  await renderScreen(ctx, 'wallet_tracker_editor', updated);
});

bot.callbackQuery(/^wallettracker:set:(wallet_address):(.+)$/, async (ctx) => {
  const field = ctx.match[1];
  const walletTrackerId = ctx.match[2];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    updateWalletTrackerInDraft(draft, walletTrackerId, (order) => ({
      ...order,
      awaitingField: field,
      deleteConfirmations: 0,
      updatedAt: new Date().toISOString(),
    }));
    return draft;
  });
  await ctx.answerCallbackQuery({ text: promptForWalletTrackerField(field) });
  await renderScreen(ctx, 'wallet_tracker_editor', updated);
});

bot.callbackQuery(/^wallettracker:cycle:buy_mode:(.+)$/, async (ctx) => {
  const walletTrackerId = ctx.match[1];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    updateWalletTrackerInDraft(draft, walletTrackerId, (order) => ({
      ...order,
      buyMode: cycleWalletTrackerBuyMode(order.buyMode),
      updatedAt: new Date().toISOString(),
    }));
    return draft;
  });
  await ctx.answerCallbackQuery({ text: `Buy alerts: ${walletTrackerBuyModeLabel(updated.walletTracker.buyMode)}.` });
  await renderScreen(ctx, 'wallet_tracker_editor', updated);
});

bot.callbackQuery(/^wallettracker:toggle:(sells|launches):(.+)$/, async (ctx) => {
  const setting = ctx.match[1];
  const walletTrackerId = ctx.match[2];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    updateWalletTrackerInDraft(draft, walletTrackerId, (order) => ({
      ...order,
      notifySells: setting === 'sells' ? !order.notifySells : order.notifySells,
      notifyLaunches: setting === 'launches' ? !order.notifyLaunches : order.notifyLaunches,
      updatedAt: new Date().toISOString(),
    }));
    return draft;
  });
  await ctx.answerCallbackQuery({
    text: setting === 'sells'
      ? `Sell alerts ${updated.walletTracker.notifySells ? 'enabled' : 'disabled'}.`
      : `Launch alerts ${updated.walletTracker.notifyLaunches ? 'enabled' : 'disabled'}.`,
  });
  await renderScreen(ctx, 'wallet_tracker_editor', updated);
});

bot.callbackQuery('wallettracker:locked:toggle', async (ctx) => {
  const user = await getUserState(String(ctx.from.id));
  await ctx.answerCallbackQuery({ text: 'Set the wallet address first.' });
  await renderScreen(ctx, 'wallet_tracker_editor', user);
});

bot.callbackQuery(/^wallettracker:toggle:(.+)$/, async (ctx) => {
  const walletTrackerId = ctx.match[1];
  const userId = String(ctx.from.id);
  const updated = await updateUserState(userId, (draft) => {
    updateWalletTrackerInDraft(draft, walletTrackerId, (order) => {
      const automationEnabled = !order.automationEnabled;
      return {
        ...order,
        automationEnabled,
        status: automationEnabled ? 'watching' : 'stopped',
        awaitingField: null,
        deleteConfirmations: 0,
        lastError: null,
        updatedAt: new Date().toISOString(),
      };
    });
    return draft;
  });
  await appendUserActivityLog(userId, {
    scope: walletTrackerScope(walletTrackerId),
    level: 'info',
    message: updated.walletTracker.automationEnabled
      ? 'Wallet Tracker started from Telegram.'
      : 'Wallet Tracker stopped from Telegram.',
  });
  await ctx.answerCallbackQuery({
    text: updated.walletTracker.automationEnabled ? 'Wallet Tracker started.' : 'Wallet Tracker stopped.',
  });
  await renderScreen(ctx, 'wallet_tracker_editor', updated);
});

bot.callbackQuery(/^wallettracker:archive:(.+)$/, async (ctx) => {
  const walletTrackerId = ctx.match[1];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    updateWalletTrackerInDraft(draft, walletTrackerId, (order) => ({
      ...order,
      archivedAt: new Date().toISOString(),
      automationEnabled: false,
      status: 'stopped',
      deleteConfirmations: 0,
      updatedAt: new Date().toISOString(),
    }));
    return draft;
  });
  await ctx.answerCallbackQuery({ text: 'Tracker archived.' });
  await renderScreen(ctx, 'wallet_tracker', updated);
});

bot.callbackQuery(/^wallettracker:restore:(.+)$/, async (ctx) => {
  const walletTrackerId = ctx.match[1];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    updateWalletTrackerInDraft(draft, walletTrackerId, (order) => ({
      ...order,
      archivedAt: null,
      deleteConfirmations: 0,
      updatedAt: new Date().toISOString(),
    }));
    return draft;
  });
  await ctx.answerCallbackQuery({ text: 'Tracker restored.' });
  await renderScreen(ctx, 'wallet_tracker_editor', updated);
});

bot.callbackQuery(/^wallettracker:delete:(.+)$/, async (ctx) => {
  const walletTrackerId = ctx.match[1];
  const userId = String(ctx.from.id);
  const current = await getUserState(userId);
  const order = current.walletTrackers.find((item) => item.id === walletTrackerId) ?? current.walletTracker;
  if (!order?.archivedAt) {
    await ctx.answerCallbackQuery({ text: 'Archive it first before deleting.' });
    await renderScreen(ctx, 'wallet_tracker_editor', current);
    return;
  }

  let deleted = false;
  const updated = await updateUserState(userId, (draft) => {
    const match = draft.walletTrackers.find((item) => item.id === walletTrackerId);
    if (match?.deleteConfirmations >= 1) {
      draft.walletTrackers = draft.walletTrackers.filter((item) => item.id !== walletTrackerId);
      draft.activeWalletTrackerId = draft.walletTrackers.find((item) => !item.archivedAt)?.id
        ?? draft.walletTrackers[0]?.id
        ?? null;
      syncActiveWalletTrackerDraft(draft);
      deleted = true;
    } else {
      updateWalletTrackerInDraft(draft, walletTrackerId, (item) => ({
        ...item,
        deleteConfirmations: 1,
        updatedAt: new Date().toISOString(),
      }));
    }
    return draft;
  });
  await ctx.answerCallbackQuery({ text: deleted ? 'Tracker deleted.' : 'Tap again to delete permanently.' });
  await renderScreen(ctx, deleted ? 'wallet_tracker_archive' : 'wallet_tracker_editor', updated);
});

bot.callbackQuery('nav:magic_sell', async (ctx) => {
  const user = await getUserState(String(ctx.from.id));
  await ctx.answerCallbackQuery();
  await renderScreen(ctx, 'magic_sell', user);
});

bot.callbackQuery('nav:magic_bundle', async (ctx) => {
  const user = await getUserState(String(ctx.from.id));
  await ctx.answerCallbackQuery();
  await renderScreen(ctx, 'magic_bundle', user);
});

bot.callbackQuery('nav:magic_bundle_archive', async (ctx) => {
  const user = await getUserState(String(ctx.from.id));
  await ctx.answerCallbackQuery();
  await renderScreen(ctx, 'magic_bundle_archive', user);
});

bot.callbackQuery('nav:magic_sell_archive', async (ctx) => {
  const user = await getUserState(String(ctx.from.id));
  await ctx.answerCallbackQuery();
  await renderScreen(ctx, 'magic_sell_archive', user);
});

bot.callbackQuery(/^magicbundle:new:(stealth|standard)$/, async (ctx) => {
  const userId = String(ctx.from.id);
  const bundleMode = ctx.match[1];
  const updated = await updateUserState(userId, (draft) => {
    appendMagicBundleToDraft(draft, {
      ...createDefaultMagicBundle(),
      bundleMode,
      platformFeeBps: 0,
      splitNowFeeEstimateBps: bundleMode === 'standard' ? 0 : cfg.magicBundleSplitNowFeeEstimateBps,
    });
    draft.tradingDesk = normalizeTradingDesk({
      ...draft.tradingDesk,
      selectedMagicBundleId: draft.activeMagicBundleId,
    });
    return draft;
  });
  await appendUserActivityLog(userId, {
    scope: magicBundleScope(updated.magicBundle.id),
    level: 'info',
    message: `Created a new ${bundleMode === 'standard' ? 'Regular Bundle' : 'Magic Bundle (Stealth)'} deposit wallet.`,
  });

  await ctx.answerCallbackQuery({ text: `${bundleMode === 'standard' ? 'Regular Bundle' : 'Magic Bundle'} wallet created.` });
  await renderScreen(ctx, 'magic_bundle_editor', updated);
});

bot.callbackQuery(/^magicbundle:open:(.+)$/, async (ctx) => {
  const magicBundleId = ctx.match[1];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.activeMagicBundleId = magicBundleId;
    syncActiveMagicBundleDraft(draft);
    draft.tradingDesk = normalizeTradingDesk({
      ...draft.tradingDesk,
      selectedMagicBundleId: magicBundleId,
    });
    return draft;
  });

  await ctx.answerCallbackQuery();
  await renderScreen(ctx, 'magic_bundle_editor', updated);
});

bot.callbackQuery('magicbundle:refresh', async (ctx) => {
  const user = await getUserState(String(ctx.from.id));
  const updated = await refreshMagicBundle(String(ctx.from.id), user.activeMagicBundleId);
  await ctx.answerCallbackQuery({ text: 'Magic Bundle refreshed.' });
  await renderScreen(ctx, 'magic_bundle_editor', updated);
});

bot.callbackQuery('magicbundle:locked:toggle', async (ctx) => {
  const user = await getUserState(String(ctx.from.id));
  await ctx.answerCallbackQuery({
    text: 'Finish the split and set at least one protection rule first.',
  });
  await renderScreen(ctx, 'magic_bundle_editor', user);
});

bot.callbackQuery(/^magicbundle:toggle:(.+)$/, async (ctx) => {
  const magicBundleId = ctx.match[1];
  const userId = String(ctx.from.id);
  const currentUser = await getUserState(userId);
  const currentOrder = currentUser.magicBundles.find((order) => order.id === magicBundleId) ?? currentUser.magicBundle;
  if (!magicBundleCanStart(currentOrder)) {
    await ctx.answerCallbackQuery({
      text: 'Finish the split and set at least one protection rule first.',
    });
    await renderScreen(ctx, 'magic_bundle_editor', currentUser);
    return;
  }

  const updated = await updateUserState(userId, (draft) => {
    updateMagicBundleInDraft(draft, magicBundleId, (order) => {
      const automationEnabled = !order.automationEnabled;
      return {
        ...order,
        automationEnabled,
        status: automationEnabled
          ? (order.currentTokenAmountRaw && order.currentTokenAmountRaw !== '0' ? 'running' : 'waiting_inventory')
          : 'stopped',
        awaitingField: null,
        deleteConfirmations: 0,
        lastError: null,
        updatedAt: new Date().toISOString(),
      };
    });
    return draft;
  });

  await appendUserActivityLog(userId, {
    scope: magicBundleScope(magicBundleId),
    level: 'info',
    message: updated.magicBundle.automationEnabled
      ? 'Magic Bundle protection started from Telegram.'
      : 'Magic Bundle protection stopped from Telegram.',
  });

  await ctx.answerCallbackQuery({
    text: updated.magicBundle.automationEnabled ? 'Magic Bundle started.' : 'Magic Bundle stopped.',
  });
  await renderScreen(ctx, 'magic_bundle_editor', updated);
});

bot.callbackQuery(/^magicbundle:set:(token_name|mint|wallet_count|stop_loss|take_profit|trailing_stop_loss|buy_dip|sell_on_dev_sell):(.+)$/, async (ctx) => {
  const field = ctx.match[1];
  const magicBundleId = ctx.match[2];
  const userId = String(ctx.from.id);
  const currentUser = await getUserState(userId);
  const currentOrder = currentUser.magicBundles.find((order) => order.id === magicBundleId) ?? currentUser.magicBundle;

  if (field === 'wallet_count' && currentOrder?.splitCompletedAt) {
    await ctx.answerCallbackQuery({
      text: 'Wallet count locks after the split. Create a new bundle to change it.',
    });
    await renderScreen(ctx, 'magic_bundle_editor', currentUser);
    return;
  }

  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    if (field === 'sell_on_dev_sell') {
      updateMagicBundleInDraft(draft, magicBundleId, (order) => ({
        ...order,
        sellOnDevSell: !order.sellOnDevSell,
        deleteConfirmations: 0,
        updatedAt: new Date().toISOString(),
      }));
      return draft;
    }

    updateMagicBundleInDraft(draft, magicBundleId, (order) => ({
      ...order,
      awaitingField: field,
      deleteConfirmations: 0,
      updatedAt: new Date().toISOString(),
    }));
    return draft;
  });

  await ctx.answerCallbackQuery({ text: field === 'sell_on_dev_sell' ? 'Updated.' : promptForMagicBundleField(field) });
  await renderScreen(ctx, 'magic_bundle_editor', updated);
});

bot.callbackQuery('magicbundle:locked:add_to_trading', async (ctx) => {
  const user = await getUserState(String(ctx.from.id));
  await ctx.answerCallbackQuery({ text: 'Generate the bundle wallets first.' });
  await renderScreen(ctx, 'magic_bundle_editor', user);
});

bot.callbackQuery(/^magicbundle:add_to_trading:(.+)$/, async (ctx) => {
  const magicBundleId = ctx.match[1];
  const userId = String(ctx.from.id);
  const current = await getUserState(userId);
  const order = current.magicBundles.find((item) => item.id === magicBundleId) ?? current.magicBundle;
  if (!order || !Array.isArray(order.splitWallets) || !order.splitWallets.some((wallet) => wallet?.address && wallet?.secretKeyB64)) {
    await ctx.answerCallbackQuery({ text: 'Generate the bundle wallets first.' });
    await renderScreen(ctx, 'magic_bundle_editor', current);
    return;
  }

  const updated = await updateUserState(userId, (draft) => {
    syncTradingDeskWalletsFromSource(
      draft,
      'magic_bundle',
      order.id,
      createMagicBundleTradingWallets(order),
      { selectFirst: true },
    );
    return draft;
  });

  await ctx.answerCallbackQuery({ text: 'Bundle wallets added to Buy / Sell.' });
  await renderScreen(ctx, 'magic_bundle_editor', updated);
});

bot.callbackQuery(/^magicbundle:archive:(.+)$/, async (ctx) => {
  const magicBundleId = ctx.match[1];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    updateMagicBundleInDraft(draft, magicBundleId, (order) => ({
      ...order,
      automationEnabled: false,
      status: 'stopped',
      archivedAt: new Date().toISOString(),
      awaitingField: null,
      deleteConfirmations: 0,
      updatedAt: new Date().toISOString(),
    }));
    draft.activeMagicBundleId = null;
    syncActiveMagicBundleDraft(draft);
    if (draft.tradingDesk?.selectedMagicBundleId === magicBundleId) {
      draft.tradingDesk = normalizeTradingDesk({
        ...draft.tradingDesk,
        selectedMagicBundleId: null,
      });
    }
    return draft;
  });

  await ctx.answerCallbackQuery({ text: 'Magic Bundle archived.' });
  await renderScreen(ctx, 'magic_bundle', updated);
});

bot.callbackQuery(/^magicbundle:restore:(.+)$/, async (ctx) => {
  const magicBundleId = ctx.match[1];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    updateMagicBundleInDraft(draft, magicBundleId, (order) => ({
      ...order,
      archivedAt: null,
      deleteConfirmations: 0,
      updatedAt: new Date().toISOString(),
    }));
    draft.activeMagicBundleId = magicBundleId;
    syncActiveMagicBundleDraft(draft);
    draft.tradingDesk = normalizeTradingDesk({
      ...draft.tradingDesk,
      selectedMagicBundleId: magicBundleId,
    });
    return draft;
  });

  await ctx.answerCallbackQuery({ text: 'Magic Bundle restored.' });
  await renderScreen(ctx, 'magic_bundle_editor', updated);
});

bot.callbackQuery(/^magicbundle:delete:(.+)$/, async (ctx) => {
  const magicBundleId = ctx.match[1];
  let deleted = false;
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    const target = draft.magicBundles.find((order) => order.id === magicBundleId);
    if (!target) {
      return draft;
    }

    if ((target.deleteConfirmations || 0) >= 1) {
      draft.magicBundles = draft.magicBundles.filter((order) => order.id !== magicBundleId);
      draft.activeMagicBundleId = null;
      if (draft.tradingDesk?.selectedMagicBundleId === magicBundleId) {
        draft.tradingDesk = normalizeTradingDesk({
          ...draft.tradingDesk,
          selectedMagicBundleId: null,
        });
      }
      deleted = true;
      syncActiveMagicBundleDraft(draft);
      return draft;
    }

    updateMagicBundleInDraft(draft, magicBundleId, (order) => ({
      ...order,
      deleteConfirmations: 1,
      updatedAt: new Date().toISOString(),
    }));
    return draft;
  });

  await ctx.answerCallbackQuery({
    text: deleted ? 'Magic Bundle permanently deleted.' : 'Tap delete again to permanently remove this archived Magic Bundle.',
  });
  await renderScreen(ctx, deleted ? 'magic_bundle_archive' : 'magic_bundle_editor', updated);
});

bot.callbackQuery(/^launchbuy:new:(normal|magic)$/, async (ctx) => {
  const userId = String(ctx.from.id);
  const launchMode = ctx.match[1];
  const updated = await updateUserState(userId, (draft) => {
    appendLaunchBuyToDraft(draft, {
      ...createDefaultLaunchBuy(),
      launchMode,
    });
    return draft;
  });

  await appendUserActivityLog(userId, {
    scope: launchBuyScope(updated.launchBuy.id),
    level: 'info',
    message: `Created a new ${launchMode === 'magic' ? 'Magic' : 'Normal'} Launch + Buy wallet.`,
  });

  await ctx.answerCallbackQuery({ text: `${launchMode === 'magic' ? 'Magic' : 'Normal'} launch wallet created.` });
  await renderScreen(ctx, 'launch_buy_editor', updated);
});

bot.callbackQuery(/^launchbuy:open:(.+)$/, async (ctx) => {
  const launchBuyId = ctx.match[1];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.activeLaunchBuyId = launchBuyId;
    syncActiveLaunchBuyDraft(draft);
    return draft;
  });

  await ctx.answerCallbackQuery();
  await renderScreen(ctx, 'launch_buy_editor', updated);
});

bot.callbackQuery('launchbuy:refresh', async (ctx) => {
  const userId = String(ctx.from.id);
  const user = await getUserState(userId);
  const updated = await refreshLaunchBuy(userId, user.activeLaunchBuyId);
  await ctx.answerCallbackQuery({ text: 'Launch + Buy refreshed.' });
  await renderScreen(ctx, 'launch_buy_editor', updated);
});

bot.callbackQuery('launchbuy:locked:launch', async (ctx) => {
  const user = await getUserState(String(ctx.from.id));
  await ctx.answerCallbackQuery({
    text: 'Finish the launch setup first. Execution is being attached next.',
  });
  await renderScreen(ctx, 'launch_buy_editor', user);
});

bot.callbackQuery(/^launchbuy:launch:(.+)$/, async (ctx) => {
  const launchBuyId = ctx.match[1];
  const userId = String(ctx.from.id);
  const updated = await updateUserState(userId, (draft) => {
    updateLaunchBuyInDraft(draft, launchBuyId, (order) => {
      if (!launchBuyIsReady(order)) {
        return order;
      }

      return {
        ...order,
        status: 'queued',
        lastError: null,
        awaitingField: null,
        updatedAt: new Date().toISOString(),
      };
    });
    return draft;
  });

  await appendUserActivityLog(userId, {
    scope: launchBuyScope(launchBuyId),
    level: 'info',
    message: 'Launch + Buy was queued from Telegram and is waiting for the worker to execute it.',
  });

  await ctx.answerCallbackQuery({ text: 'Launch queued.' });
  await renderScreen(ctx, 'launch_buy_editor', updated);
});

bot.callbackQuery('launchbuy:locked:add_to_trading', async (ctx) => {
  const user = await getUserState(String(ctx.from.id));
  await ctx.answerCallbackQuery({ text: 'Generate or import the buyer wallets first.' });
  await renderScreen(ctx, 'launch_buy_editor', user);
});

bot.callbackQuery(/^launchbuy:add_to_trading:(.+)$/, async (ctx) => {
  const launchBuyId = ctx.match[1];
  const userId = String(ctx.from.id);
  const current = await getUserState(userId);
  const order = current.launchBuys.find((item) => item.id === launchBuyId) ?? current.launchBuy;
  if (!order || !Array.isArray(order.buyerWallets) || !order.buyerWallets.some((wallet) => wallet?.address && wallet?.secretKeyB64)) {
    await ctx.answerCallbackQuery({ text: 'Generate or import the buyer wallets first.' });
    await renderScreen(ctx, 'launch_buy_editor', current);
    return;
  }

  const updated = await updateUserState(userId, (draft) => {
    syncTradingDeskWalletsFromSource(
      draft,
      'launch_buy',
      order.id,
      createLaunchBuyTradingWallets(order),
      { selectFirst: true },
    );
    return draft;
  });

  await ctx.answerCallbackQuery({ text: 'Launch + Buy wallets added to Buy / Sell.' });
  await renderScreen(ctx, 'launch_buy_editor', updated);
});

bot.callbackQuery(/^launchbuy:key:toggle:(.+)$/, async (ctx) => {
  const launchBuyId = ctx.match[1];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    updateLaunchBuyInDraft(draft, launchBuyId, (order) => ({
      ...order,
      privateKeyVisible: !order.privateKeyVisible,
      deleteConfirmations: 0,
      updatedAt: new Date().toISOString(),
    }));
    return draft;
  });

  await ctx.answerCallbackQuery({ text: updated.launchBuy.privateKeyVisible ? 'Private key shown.' : 'Private key hidden.' });
  await renderScreen(ctx, 'launch_buy_editor', updated);
});

bot.callbackQuery(/^launchbuy:set:wallet_source:(generated|imported):(.+)$/, async (ctx) => {
  const walletSource = ctx.match[1];
  const launchBuyId = ctx.match[2];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    updateLaunchBuyInDraft(draft, launchBuyId, (order) => ({
      ...order,
      walletSource,
      buyerWallets: walletSource === 'generated'
        ? createLaunchBuyBuyerWallets(order.buyerWalletCount || LAUNCH_BUY_DEFAULT_WALLET_COUNT)
        : [],
      awaitingField: walletSource === 'imported' ? 'buyer_keys' : null,
      deleteConfirmations: 0,
      updatedAt: new Date().toISOString(),
    }));
    return draft;
  });

  await ctx.answerCallbackQuery({
    text: walletSource === 'imported'
      ? promptForLaunchBuyField('buyer_keys', updated.launchBuy)
      : 'Generated buyer wallets ready.',
  });
  await renderScreen(ctx, 'launch_buy_editor', updated);
});

bot.callbackQuery(/^launchbuy:set:(token_name|symbol|description|logo|wallet_count|buyer_keys|total_buy|jito_tip|website|telegram|twitter):(.+)$/, async (ctx) => {
  const field = ctx.match[1];
  const launchBuyId = ctx.match[2];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    updateLaunchBuyInDraft(draft, launchBuyId, (order) => {
      if (field === 'buyer_keys' && order.walletSource === 'generated') {
        return {
          ...order,
          buyerWallets: createLaunchBuyBuyerWallets(order.buyerWalletCount || LAUNCH_BUY_DEFAULT_WALLET_COUNT),
          awaitingField: null,
          deleteConfirmations: 0,
          updatedAt: new Date().toISOString(),
        };
      }

      return {
        ...order,
        awaitingField: field,
        deleteConfirmations: 0,
        updatedAt: new Date().toISOString(),
      };
    });
    return draft;
  });

  await ctx.answerCallbackQuery({
    text: field === 'buyer_keys' && updated.launchBuy.walletSource === 'generated'
      ? 'Buyer wallets regenerated.'
      : promptForLaunchBuyField(field, updated.launchBuy),
  });
  await renderScreen(ctx, 'launch_buy_editor', updated);
});

bot.callbackQuery(/^launchbuy:archive:(.+)$/, async (ctx) => {
  const launchBuyId = ctx.match[1];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    updateLaunchBuyInDraft(draft, launchBuyId, (order) => ({
      ...order,
      status: 'stopped',
      archivedAt: new Date().toISOString(),
      awaitingField: null,
      deleteConfirmations: 0,
      privateKeyVisible: false,
      updatedAt: new Date().toISOString(),
    }));
    draft.activeLaunchBuyId = null;
    syncActiveLaunchBuyDraft(draft);
    return draft;
  });

  await ctx.answerCallbackQuery({ text: 'Launch + Buy archived.' });
  await renderScreen(ctx, 'launch_buy', updated);
});

bot.callbackQuery(/^launchbuy:restore:(.+)$/, async (ctx) => {
  const launchBuyId = ctx.match[1];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    updateLaunchBuyInDraft(draft, launchBuyId, (order) => ({
      ...order,
      archivedAt: null,
      deleteConfirmations: 0,
      updatedAt: new Date().toISOString(),
    }));
    draft.activeLaunchBuyId = launchBuyId;
    syncActiveLaunchBuyDraft(draft);
    return draft;
  });

  await ctx.answerCallbackQuery({ text: 'Launch + Buy restored.' });
  await renderScreen(ctx, 'launch_buy_editor', updated);
});

bot.callbackQuery(/^launchbuy:delete:(.+)$/, async (ctx) => {
  const launchBuyId = ctx.match[1];
  let deleted = false;
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    const target = draft.launchBuys.find((order) => order.id === launchBuyId);
    if (!target) {
      return draft;
    }

    if ((target.deleteConfirmations || 0) >= 1) {
      draft.launchBuys = draft.launchBuys.filter((order) => order.id !== launchBuyId);
      draft.activeLaunchBuyId = null;
      syncTradingDeskWalletsFromSource(draft, 'launch_buy', launchBuyId, []);
      deleted = true;
      syncActiveLaunchBuyDraft(draft);
      return draft;
    }

    updateLaunchBuyInDraft(draft, launchBuyId, (order) => ({
      ...order,
      deleteConfirmations: 1,
      updatedAt: new Date().toISOString(),
    }));
    return draft;
  });

  await ctx.answerCallbackQuery({
    text: deleted ? 'Launch + Buy permanently deleted.' : 'Tap delete again to permanently remove this archived Launch + Buy setup.',
  });
  await renderScreen(ctx, deleted ? 'launch_buy_archive' : 'launch_buy_editor', updated);
});

bot.callbackQuery('magic:new', async (ctx) => {
  const userId = String(ctx.from.id);
  const updated = await updateUserState(userId, (draft) => {
    appendMagicSellToDraft(draft, createDefaultMagicSell());
    return draft;
  });
  await appendUserActivityLog(userId, {
    scope: magicSellScope(updated.magicSell.id),
    level: 'info',
    message: 'Created a new Magic Sell deposit wallet.',
  });

  await ctx.answerCallbackQuery({ text: 'Magic Sell wallet created.' });
  await renderScreen(ctx, 'magic_sell_editor', updated);
});

bot.callbackQuery(/^magic:open:(.+)$/, async (ctx) => {
  const magicSellId = ctx.match[1];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.activeMagicSellId = magicSellId;
    syncActiveMagicSellDraft(draft);
    return draft;
  });

  await ctx.answerCallbackQuery();
  await renderScreen(ctx, 'magic_sell_editor', updated);
});

bot.callbackQuery('magic:refresh', async (ctx) => {
  const user = await getUserState(String(ctx.from.id));
  const updated = await refreshMagicSell(String(ctx.from.id), user.activeMagicSellId);
  await ctx.answerCallbackQuery({ text: 'Magic Sell refreshed.' });
  await renderScreen(ctx, 'magic_sell_editor', updated);
});

bot.callbackQuery(/^magic:set:(token_name|mint|target_market_cap|whitelist|seller_wallet_count):(.+)$/, async (ctx) => {
  const field = ctx.match[1];
  const magicSellId = ctx.match[2];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    updateMagicSellInDraft(draft, magicSellId, (order) => ({
      ...order,
      awaitingField: field,
      deleteConfirmations: 0,
      updatedAt: new Date().toISOString(),
    }));
    return draft;
  });

  await ctx.answerCallbackQuery({ text: promptForMagicSellField(field) });
  await renderScreen(ctx, 'magic_sell_editor', updated);
});

bot.callbackQuery(/^magic:key:toggle:(.+)$/, async (ctx) => {
  const magicSellId = ctx.match[1];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    updateMagicSellInDraft(draft, magicSellId, (order) => ({
      ...order,
      privateKeyVisible: !order.privateKeyVisible,
      updatedAt: new Date().toISOString(),
    }));
    return draft;
  });

  await ctx.answerCallbackQuery({
    text: updated.magicSell.privateKeyVisible ? 'Private key revealed.' : 'Private key hidden.',
  });
  await renderScreen(ctx, 'magic_sell_editor', updated);
});

bot.callbackQuery(/^magic:toggle:(.+)$/, async (ctx) => {
  const magicSellId = ctx.match[1];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    updateMagicSellInDraft(draft, magicSellId, (order) => ({
      ...order,
      automationEnabled: !order.automationEnabled,
      status: order.automationEnabled ? 'stopped' : 'waiting_target',
      lastError: null,
      updatedAt: new Date().toISOString(),
    }));
    return draft;
  });
  await appendUserActivityLog(String(ctx.from.id), {
    scope: magicSellScope(magicSellId),
    level: 'info',
    message: updated.magicSell.automationEnabled ? 'Magic Sell automation started from Telegram.' : 'Magic Sell automation stopped from Telegram.',
  });

  await ctx.answerCallbackQuery({
    text: updated.magicSell.automationEnabled ? 'Magic Sell started.' : 'Magic Sell stopped.',
  });
  await renderScreen(ctx, 'magic_sell_editor', updated);
});

bot.callbackQuery('magic:locked:toggle', async (ctx) => {
  const user = await getUserState(String(ctx.from.id));
  await ctx.answerCallbackQuery({ text: 'Set the mint, target market cap, and seller wallets first.' });
  await renderScreen(ctx, 'magic_sell_editor', user);
});

bot.callbackQuery(/^magic:archive:(.+)$/, async (ctx) => {
  const magicSellId = ctx.match[1];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    updateMagicSellInDraft(draft, magicSellId, (order) => ({
      ...order,
      archivedAt: new Date().toISOString(),
      automationEnabled: false,
      awaitingField: null,
      deleteConfirmations: 0,
      status: 'stopped',
      updatedAt: new Date().toISOString(),
    }));
    draft.activeMagicSellId = null;
    syncActiveMagicSellDraft(draft);
    return draft;
  });

  await ctx.answerCallbackQuery({ text: 'Magic Sell archived.' });
  await renderScreen(ctx, 'magic_sell', updated);
});

bot.callbackQuery(/^magic:restore:(.+)$/, async (ctx) => {
  const magicSellId = ctx.match[1];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    updateMagicSellInDraft(draft, magicSellId, (order) => ({
      ...order,
      archivedAt: null,
      deleteConfirmations: 0,
      updatedAt: new Date().toISOString(),
    }));
    draft.activeMagicSellId = magicSellId;
    syncActiveMagicSellDraft(draft);
    return draft;
  });

  await ctx.answerCallbackQuery({ text: 'Magic Sell restored.' });
  await renderScreen(ctx, 'magic_sell_editor', updated);
});

bot.callbackQuery(/^magic:delete:(.+)$/, async (ctx) => {
  const magicSellId = ctx.match[1];
  let deleted = false;
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    const target = draft.magicSells.find((order) => order.id === magicSellId);
    if (!target) {
      return draft;
    }

    if ((target.deleteConfirmations || 0) >= 1) {
      draft.magicSells = draft.magicSells.filter((order) => order.id !== magicSellId);
      draft.activeMagicSellId = null;
      deleted = true;
      syncActiveMagicSellDraft(draft);
      return draft;
    }

    updateMagicSellInDraft(draft, magicSellId, (order) => ({
      ...order,
      deleteConfirmations: 1,
      updatedAt: new Date().toISOString(),
    }));
    return draft;
  });

  await ctx.answerCallbackQuery({
    text: deleted ? 'Magic Sell permanently deleted.' : 'Tap delete again to permanently remove this archived Magic Sell.',
  });
  await renderScreen(ctx, deleted ? 'magic_sell_archive' : 'magic_sell_editor', updated);
});

bot.callbackQuery('holder:new', async (ctx) => {
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.holderBooster = normalizeHolderBooster({
      ...createDefaultHolderBooster(),
      awaitingField: 'mint',
      status: 'idle',
    });
    return draft;
  });
  await ctx.answerCallbackQuery({ text: promptForHolderField('mint') });
  await renderScreen(ctx, 'holder_booster', updated);
});

bot.callbackQuery('holder:refresh', async (ctx) => {
  const updated = await refreshHolderBooster(String(ctx.from.id));
  await ctx.answerCallbackQuery({ text: 'Holder Booster refreshed.' });
  await renderScreen(ctx, 'holder_booster', updated);
});

bot.callbackQuery(/^holder:set:(mint|holder_count)$/, async (ctx) => {
  const field = ctx.match[1];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.holderBooster = normalizeHolderBooster({
      ...draft.holderBooster,
      awaitingField: field,
    });
    return draft;
  });
  await ctx.answerCallbackQuery({ text: promptForHolderField(field) });
  await renderScreen(ctx, 'holder_booster', updated);
});

bot.callbackQuery('nav:volume', async (ctx) => {
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.activeAppleBoosterId = null;
    syncActiveAppleBoosterDraft(draft);
    return draft;
  });
  await ctx.answerCallbackQuery();
  await renderScreen(ctx, 'volume', updated);
});

bot.callbackQuery('nav:volume_archive', async (ctx) => {
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.activeAppleBoosterId = null;
    syncActiveAppleBoosterDraft(draft);
    return draft;
  });
  await ctx.answerCallbackQuery();
  await renderScreen(ctx, 'volume_archive', updated);
});

bot.callbackQuery(/^volume:(organic|bundled)$/, async (ctx) => {
  const mode = ctx.match[1];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.volumeMode = mode;
    if (mode !== 'organic') {
      draft.organicVolumePackage = null;
      draft.activeAppleBoosterId = null;
      syncActiveAppleBoosterDraft(draft);
    }
    return draft;
  });

  await ctx.answerCallbackQuery({
    text: mode === 'organic' ? 'Organic Volume Booster selected.' : 'Bundled Volume Booster selected.',
  });
  await renderScreen(ctx, mode === 'organic' ? 'volume_organic' : 'volume_bundled', updated);
});

bot.callbackQuery(/^organic:(3k|5k|10k|20k|30k|50k|75k|100k|200k|500k)$/, async (ctx) => {
  const packageKey = ctx.match[1];
  const selectedPackage = getOrganicVolumePackage(packageKey);
  if (!selectedPackage) {
    await ctx.answerCallbackQuery({ text: 'Unknown package.' });
    return;
  }

  const updated = await createOrganicVolumeOrder(String(ctx.from.id), packageKey);

  await ctx.answerCallbackQuery({ text: `${selectedPackage.label} organic booster selected.` });
  await renderScreen(ctx, 'volume_order', updated);
});

bot.callbackQuery('organic:trial', async (ctx) => {
  const userId = String(ctx.from.id);
  const user = await getUserState(userId);
  const existingTrial = getVisibleAppleBoostersByStrategy(user, 'organic', { archived: false })
    .find((order) => order.freeTrial);

  if (existingTrial) {
    const updated = await updateUserState(userId, (draft) => {
      draft.activeAppleBoosterId = existingTrial.id;
      syncActiveAppleBoosterDraft(draft);
      return draft;
    });
    await ctx.answerCallbackQuery({ text: 'Opening your Volume Bot Trial.' });
    await renderScreen(ctx, 'volume_order', updated);
    return;
  }

  if (user.volumeFreeTrialUsed) {
    await ctx.answerCallbackQuery({ text: 'This account has already used the Volume Bot Trial.' });
    await renderScreen(ctx, 'volume_organic', user);
    return;
  }

  if (!cfg.volumeTrialEnabled) {
    await ctx.answerCallbackQuery({ text: 'The Volume Bot Trial is not configured yet.' });
    await renderScreen(ctx, 'volume_organic', user);
    return;
  }

  const updated = await createTrialVolumeOrder(userId);
  await ctx.answerCallbackQuery({ text: 'Volume Bot Trial created.' });
  await renderScreen(ctx, 'volume_order', updated);
});

bot.callbackQuery(/^bundled:(20k|30k|50k|100k|200k|500k)$/, async (ctx) => {
  const packageKey = ctx.match[1];
  const selectedPackage = getBundledVolumePackage(packageKey);
  if (!selectedPackage) {
    await ctx.answerCallbackQuery({ text: 'Unknown bundled package.' });
    return;
  }

  const updated = await createBundledVolumeOrder(String(ctx.from.id), packageKey);

  await ctx.answerCallbackQuery({ text: `${selectedPackage.label} bundled booster selected.` });
  await renderScreen(ctx, 'volume_order', updated);
});

bot.callbackQuery(/^organic:open:(.+)$/, async (ctx) => {
  const boosterId = ctx.match[1];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.activeAppleBoosterId = boosterId;
    syncActiveAppleBoosterDraft(draft);
    return draft;
  });

  await ctx.answerCallbackQuery();
  await renderScreen(ctx, 'volume_order', updated);
});

bot.callbackQuery('organic:refresh', async (ctx) => {
  const user = await getUserState(String(ctx.from.id));
  const updated = await refreshOrganicVolumeOrder(String(ctx.from.id), user.activeAppleBoosterId);
  await ctx.answerCallbackQuery({
    text: updated.organicVolumeOrder.funded ? 'Deposit confirmed.' : 'Balance refreshed.',
  });
  await renderScreen(ctx, 'volume_order', updated);
});

bot.callbackQuery('organic:start', async (ctx) => {
  const userId = String(ctx.from.id);
  const user = await getUserState(userId);
  const boosterName = appleBoosterDisplayName(user.organicVolumeOrder);

  if (!organicBoosterCanStart(user.organicVolumeOrder)) {
    await ctx.answerCallbackQuery({ text: `Fund the wallet and finish the ${boosterName} setup first.` });
    await renderScreen(ctx, 'volume_order', user);
    return;
  }

  const updated = await updateUserState(userId, (draft) => {
    if (draft.organicVolumeOrder.freeTrial) {
      draft.volumeFreeTrialUsed = true;
    }
    updateAppleBoosterInDraft(draft, draft.activeAppleBoosterId, (order) => {
      order.running = true;
      order.appleBooster.status = 'running';
      order.appleBooster.lastError = null;
      order.appleBooster.stopRequested = false;
      return order;
    });
    return draft;
  });
  await appendUserActivityLog(userId, {
    scope: appleBoosterScope(updated.organicVolumeOrder.id),
    level: 'info',
    message: `${boosterName} was started from Telegram.`,
  });

  await ctx.answerCallbackQuery({ text: `${boosterName} started.` });
  await renderScreen(ctx, 'volume_order', updated);
});

bot.callbackQuery('organic:stop', async (ctx) => {
  const userId = String(ctx.from.id);
  const user = await getUserState(userId);
  const isBundled = user.organicVolumeOrder.strategy === 'bundled';
  const boosterName = appleBoosterDisplayName(user.organicVolumeOrder);
  const updated = await updateUserState(userId, (draft) => {
    updateAppleBoosterInDraft(draft, draft.activeAppleBoosterId, (order) => {
      order.running = true;
      order.appleBooster.stopRequested = true;
      order.appleBooster.status = 'stopping';
      order.appleBooster.lastError = null;
      return order;
    });
    return draft;
  });
  await appendUserActivityLog(userId, {
    scope: appleBoosterScope(updated.organicVolumeOrder.id),
    level: 'warn',
    message: isBundled
      ? `${boosterName} stop was requested from Telegram. New Jito bundles will halt.`
      : `${boosterName} stop was requested from Telegram. Worker wallets will sell out and sweep home.`,
  });

  await ctx.answerCallbackQuery({
    text: isBundled
      ? 'Stopping now. New bundles will halt and the deposit wallet will stay in place.'
      : 'Stopping now. Active worker wallets will sell back to SOL and sweep into the deposit wallet.',
  });
  await renderScreen(ctx, 'volume_order', updated);
});

bot.callbackQuery('organic:withdraw', async (ctx) => {
  const user = await getUserState(String(ctx.from.id));
  const isBundled = user.organicVolumeOrder.strategy === 'bundled';
  const boosterName = appleBoosterDisplayName(user.organicVolumeOrder);
  if (user.organicVolumeOrder.freeTrial) {
    await ctx.answerCallbackQuery({
      text: 'Withdraw is not available for the Volume Bot Trial.',
    });
    await renderScreen(ctx, 'volume_order', user);
    return;
  }
  if (user.organicVolumeOrder.running || user.organicVolumeOrder.appleBooster.stopRequested) {
    await ctx.answerCallbackQuery({
      text: isBundled
        ? `Stop the ${boosterName} first before withdrawing the deposit wallet.`
        : `Stop the ${boosterName} first. The worker wallets need time to sell and sweep back into the deposit wallet.`,
    });
    await renderScreen(ctx, 'volume_order', user);
    return;
  }

  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    updateAppleBoosterInDraft(draft, draft.activeAppleBoosterId, (order) => {
      order.awaitingField = 'withdraw_address';
      return order;
    });
    return draft;
  });
  await ctx.answerCallbackQuery({ text: promptForOrganicField('withdraw_address') });
  await renderScreen(ctx, 'volume_order', updated);
});

bot.callbackQuery(/^organic:locked:(start|stop|withdraw)$/, async (ctx) => {
  const user = await getUserState(String(ctx.from.id));
  await ctx.answerCallbackQuery({
    text: `Fund the wallet and finish the ${appleBoosterDisplayName(user.organicVolumeOrder)} setup first.`,
  });
  await renderScreen(ctx, 'volume_order', user);
});

bot.callbackQuery(/^organic:set:(wallet_count|mint|swap_range|interval_range)$/, async (ctx) => {
  const field = ctx.match[1];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    updateAppleBoosterInDraft(draft, draft.activeAppleBoosterId, (order) => {
      order.awaitingField = field;
      return order;
    });
    return draft;
  });

  await ctx.answerCallbackQuery({ text: promptForOrganicField(field) });
  await renderScreen(ctx, 'volume_order', updated);
});

bot.callbackQuery(/^organic:archive:(.+)$/, async (ctx) => {
  const boosterId = ctx.match[1];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    updateAppleBoosterInDraft(draft, boosterId, (order) => {
      order.archivedAt = new Date().toISOString();
      order.deleteConfirmations = 0;
      order.awaitingField = null;
      order.running = false;
      order.appleBooster.stopRequested = false;
      order.appleBooster.status = 'stopped';
      order.appleBooster.lastError = null;
      return order;
    });
    draft.activeAppleBoosterId = null;
    syncActiveAppleBoosterDraft(draft);
    return draft;
  });

  await ctx.answerCallbackQuery({
    text: `${updated.volumeMode === 'bundled' ? 'Bundle' : 'Booster'} archived.`,
  });
  await renderScreen(ctx, 'volume', updated);
});

bot.callbackQuery(/^organic:restore:(.+)$/, async (ctx) => {
  const boosterId = ctx.match[1];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    updateAppleBoosterInDraft(draft, boosterId, (order) => {
      order.archivedAt = null;
      order.deleteConfirmations = 0;
      return order;
    });
    draft.activeAppleBoosterId = boosterId;
    syncActiveAppleBoosterDraft(draft);
    return draft;
  });

  await ctx.answerCallbackQuery({
    text: `${updated.organicVolumeOrder.strategy === 'bundled' ? 'Bundle' : 'Booster'} restored.`,
  });
  await renderScreen(ctx, 'volume_order', updated);
});

bot.callbackQuery(/^organic:delete:(.+)$/, async (ctx) => {
  const boosterId = ctx.match[1];
  let deleted = false;
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    const target = draft.appleBoosters.find((order) => order.id === boosterId);
    if (!target) {
      return draft;
    }

    if ((target.deleteConfirmations || 0) >= 1) {
      draft.appleBoosters = draft.appleBoosters.filter((order) => order.id !== boosterId);
      draft.activeAppleBoosterId = null;
      deleted = true;
      syncActiveAppleBoosterDraft(draft);
      return draft;
    }

    updateAppleBoosterInDraft(draft, boosterId, (order) => {
      order.deleteConfirmations = 1;
      return order;
    });
    return draft;
  });

  await ctx.answerCallbackQuery({
    text: deleted
      ? `${updated.volumeMode === 'bundled' ? 'Bundle' : 'Booster'} permanently deleted.`
      : `Tap delete again to permanently remove this archived ${updated.organicVolumeOrder.strategy === 'bundled' ? 'bundle' : 'booster'}.`,
  });
  await renderScreen(ctx, deleted ? 'volume_archive' : 'volume_order', updated);
});

bot.callbackQuery(/^burn:new:(lightning|normal)$/, async (ctx) => {
  const userId = String(ctx.from.id);
  const speed = ctx.match[1];
  const updated = await updateUserState(userId, (draft) => {
    appendBurnAgentToDraft(draft, buildNewBurnAgent(speed));
    return draft;
  });
  await appendUserActivityLog(userId, {
    scope: `burn_agent:${updated.burnAgent.id}`,
    level: 'info',
    message: speed === 'lightning' ? 'Created a new Fast Burn Agent.' : 'Created a new Normal Burn Agent.',
  });

  await ctx.answerCallbackQuery({
    text: speed === 'lightning' ? 'Lightning agent created.' : 'Normal agent created.',
  });
  await renderScreen(ctx, 'burn_agent_editor', updated);
});

bot.callbackQuery(/^burn:open:(.+)$/, async (ctx) => {
  const agentId = ctx.match[1];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.activeBurnAgentId = agentId;
    draft.burnAgent = getActiveBurnAgent(draft);
    return draft;
  });

  await ctx.answerCallbackQuery();
  await renderScreen(
    ctx,
    isArchivedBurnAgent(updated.burnAgent) ? 'burn_agent_archive' : 'burn_agent_editor',
    updated,
  );
});

bot.callbackQuery(/^burn:wallet:(generated|provided):(.+)$/, async (ctx) => {
  const userId = String(ctx.from.id);
  const action = ctx.match[1];
  const agentId = ctx.match[2];
  const updated = await updateUserState(userId, (draft) => {
    const targetAgent = draft.burnAgents.find((agent) => agent.id === agentId);
    if (!targetAgent?.speed) {
      return draft;
    }

    updateBurnAgentInDraft(draft, agentId, (agent) => {
      if (action === 'provided') {
        agent.walletMode = 'provided';
        agent.walletAddress = null;
        agent.walletSecretKeyB64 = null;
        agent.walletSecretKeyBase58 = null;
        agent.privateKeyVisible = false;
        agent.awaitingField = 'private_key';
        agent.updatedAt = new Date().toISOString();
        return agent;
      }

      const walletMode = agent.speed === 'normal' ? 'managed' : 'generated';
      Object.assign(agent, createBurnAgentWalletAssignment(walletMode));
      agent.awaitingField = null;
      agent.privateKeyVisible = false;
      agent.regenerateConfirmations = 0;
      agent.updatedAt = new Date().toISOString();
      return agent;
    });
    return draft;
  });

  await ctx.answerCallbackQuery({
    text: action === 'provided'
      ? promptForBurnAgentField('private_key')
      : 'Wallet generated.',
  });
  await renderScreen(ctx, 'burn_agent_editor', updated);
});

bot.callbackQuery(/^burn:regen:(.+)$/, async (ctx) => {
  const userId = String(ctx.from.id);
  const agentId = ctx.match[1];
  const user = await getUserState(userId);
  const selectedAgent = user.burnAgents.find((agent) => agent.id === agentId) ?? null;

  if (!selectedAgent || !(selectedAgent.walletMode === 'generated' || selectedAgent.walletMode === 'managed')) {
    await ctx.answerCallbackQuery({ text: 'That wallet cannot be regenerated.' });
    await renderScreen(ctx, 'burn_agent_editor', user);
    return;
  }

  const balanceLamports = await getBurnAgentBalanceLamports(selectedAgent);
  if (!Number.isInteger(balanceLamports)) {
    const refreshed = await updateUserState(userId, (draft) => {
      updateBurnAgentInDraft(draft, agentId, (agent) => {
        agent.regenerateConfirmations = 0;
        agent.updatedAt = new Date().toISOString();
        return agent;
      });
      return draft;
    });
    await ctx.answerCallbackQuery({ text: 'Unable to confirm wallet balance. Refresh and try again.' });
    await renderScreen(ctx, 'burn_agent_editor', refreshed);
    return;
  }

  if (balanceLamports > 0) {
    const refreshed = await updateUserState(userId, (draft) => {
      updateBurnAgentInDraft(draft, agentId, (agent) => {
        agent.regenerateConfirmations = 0;
        agent.updatedAt = new Date().toISOString();
        return agent;
      });
      return draft;
    });
    await ctx.answerCallbackQuery({
      text: `Withdraw the wallet funds first (${formatSolAmountFromLamports(balanceLamports)} SOL still on the wallet).`,
    });
    await renderScreen(ctx, 'burn_agent_editor', refreshed);
    return;
  }

  let completed = false;
  const updated = await updateUserState(userId, (draft) => {
    updateBurnAgentInDraft(draft, agentId, (agent) => {
      if (!(agent.walletMode === 'generated' || agent.walletMode === 'managed')) {
        return agent;
      }

      const nextConfirmations = (agent.regenerateConfirmations || 0) + 1;
      if (nextConfirmations >= 3) {
        Object.assign(agent, createBurnAgentWalletAssignment(agent.speed === 'normal' ? 'managed' : 'generated'));
        agent.regenerateConfirmations = 0;
        agent.automationEnabled = false;
        agent.awaitingField = null;
        agent.privateKeyVisible = false;
        agent.updatedAt = new Date().toISOString();
        completed = true;
        return agent;
      }

      agent.regenerateConfirmations = nextConfirmations;
      agent.updatedAt = new Date().toISOString();
      return agent;
    });
    return draft;
  });

  await ctx.answerCallbackQuery({
    text: completed
      ? 'Wallet regenerated. Burn bot stopped for safety.'
      : `Regenerate confirmation ${updated.burnAgent.regenerateConfirmations}/3.`,
  });
  await renderScreen(ctx, 'burn_agent_editor', updated);
});

bot.callbackQuery(/^burn:set:(private_key|token_name|mint|treasury|burn_percent|treasury_percent):(.+)$/, async (ctx) => {
  const userId = String(ctx.from.id);
  const field = ctx.match[1];
  const agentId = ctx.match[2];
  const updated = await updateUserState(userId, (draft) => {
    updateBurnAgentInDraft(draft, agentId, (agent) => {
      agent.awaitingField = field;
      agent.deleteConfirmations = 0;
      agent.updatedAt = new Date().toISOString();
      return agent;
    });
    return draft;
  });

  await ctx.answerCallbackQuery({ text: promptForBurnAgentField(field) });
  await renderScreen(ctx, 'burn_agent_editor', updated);
});

bot.callbackQuery(/^burn:key:toggle:(.+)$/, async (ctx) => {
  const agentId = ctx.match[1];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    updateBurnAgentInDraft(draft, agentId, (agent) => {
      if (!burnAgentHasStoredPrivateKey(agent)) {
        return agent;
      }

      agent.privateKeyVisible = !agent.privateKeyVisible;
      agent.updatedAt = new Date().toISOString();
      return agent;
    });
    return draft;
  });

  await ctx.answerCallbackQuery({
    text: updated.burnAgent.privateKeyVisible ? 'Private key revealed.' : 'Private key hidden.',
  });
  await renderScreen(ctx, 'burn_agent_editor', updated);
});

bot.callbackQuery(/^burn:toggle:(.+)$/, async (ctx) => {
  const agentId = ctx.match[1];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    updateBurnAgentInDraft(draft, agentId, (agent) => {
      agent.automationEnabled = !agent.automationEnabled;
      agent.updatedAt = new Date().toISOString();
      return agent;
    });
    return draft;
  });
  await appendUserActivityLog(String(ctx.from.id), {
    scope: `burn_agent:${agentId}`,
    level: 'info',
    message: updated.burnAgent.automationEnabled ? 'Burn Agent automation started from Telegram.' : 'Burn Agent automation stopped from Telegram.',
  });

  await ctx.answerCallbackQuery({
    text: updated.burnAgent.automationEnabled ? 'Burn bot started.' : 'Burn bot stopped.',
  });
  await renderScreen(ctx, 'burn_agent_editor', updated);
});

bot.callbackQuery(/^burn:(locked:toggle|locked:withdraw)$/, async (ctx) => {
  const user = await getUserState(String(ctx.from.id));
  await ctx.answerCallbackQuery({
    text: 'Finish the wallet + mint setup first.',
  });
  await renderScreen(ctx, 'burn_agent_editor', user);
});

bot.callbackQuery(/^burn:withdraw:(.+)$/, async (ctx) => {
  const agentId = ctx.match[1];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    updateBurnAgentInDraft(draft, agentId, (agent) => {
      agent.awaitingField = 'withdraw_address';
      agent.updatedAt = new Date().toISOString();
      return agent;
    });
    return draft;
  });

  await ctx.answerCallbackQuery({ text: promptForBurnAgentField('withdraw_address') });
  await renderScreen(ctx, 'burn_agent_editor', updated);
});

bot.callbackQuery(/^burn:archive:(.+)$/, async (ctx) => {
  const agentId = ctx.match[1];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    updateBurnAgentInDraft(draft, agentId, (agent) => {
      agent.archivedAt = new Date().toISOString();
      agent.automationEnabled = false;
      agent.awaitingField = null;
      agent.regenerateConfirmations = 0;
      agent.updatedAt = new Date().toISOString();
      return agent;
    });
    draft.activeBurnAgentId = null;
    return draft;
  });

  await ctx.answerCallbackQuery({ text: 'Agent archived.' });
  await renderScreen(ctx, 'burn_agent', updated);
});

bot.callbackQuery(/^burn:restore:(.+)$/, async (ctx) => {
  const agentId = ctx.match[1];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    updateBurnAgentInDraft(draft, agentId, (agent) => {
      agent.archivedAt = null;
      agent.deleteConfirmations = 0;
      agent.updatedAt = new Date().toISOString();
      return agent;
    });
    draft.activeBurnAgentId = agentId;
    return draft;
  });

  await ctx.answerCallbackQuery({ text: 'Agent restored.' });
  await renderScreen(ctx, 'burn_agent_editor', updated);
});

bot.callbackQuery(/^burn:delete:(.+)$/, async (ctx) => {
  const agentId = ctx.match[1];
  let deleted = false;
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    const target = draft.burnAgents.find((agent) => agent.id === agentId);
    if (!target) {
      return draft;
    }

    if ((target.deleteConfirmations || 0) >= 1) {
      draft.burnAgents = draft.burnAgents.filter((agent) => agent.id !== agentId);
      draft.activeBurnAgentId = null;
      deleted = true;
      return draft;
    }

    updateBurnAgentInDraft(draft, agentId, (agent) => {
      agent.deleteConfirmations = 1;
      agent.updatedAt = new Date().toISOString();
      return agent;
    });
    return draft;
  });

  await ctx.answerCallbackQuery({
    text: deleted ? 'Agent permanently deleted.' : 'Tap delete again to permanently remove this archived agent.',
  });
  await renderScreen(ctx, deleted ? 'burn_agent_archive' : 'burn_agent_editor', updated);
});

bot.callbackQuery('target:continue', async (ctx) => {
  const user = await getUserState(String(ctx.from.id));

  if (!hasCustomTarget(user)) {
    await ctx.answerCallbackQuery({ text: 'Enter a target link first.' });
    await renderScreen(ctx, 'target', user);
    return;
  }

  await ctx.answerCallbackQuery();

  if (!user.selection.button) {
    await renderScreen(ctx, 'start', user);
    return;
  }

  if (!user.selection.amount) {
    await renderScreen(ctx, 'amount', user);
    return;
  }

  await renderScreen(ctx, hasLaunchAccess(user) ? 'confirm' : 'payment', user);
});

bot.callbackQuery(/^refresh:(home|start|amount|payment|confirm|target|status|help|help_reaction|help_volume|help_buy_sell|help_burn_agent|help_holder_booster|help_fomo_booster|help_magic_sell|help_magic_bundle|help_launch_buy|help_sniper_wizard|help_staking|help_vanity_wallet|help_community_vision|help_wallet_tracker|help_x_followers|help_engagement|help_subscriptions_accounts|help_resizer|buy_sell|buy_sell_wallets|buy_sell_quick|buy_sell_limit|buy_sell_copy|volume|volume_organic|volume_archive|volume_bundled|volume_order|burn_agent|burn_agent_archive|burn_agent_editor|holder_booster|fomo_booster|magic_bundle|magic_bundle_archive|magic_bundle_editor|magic_sell|magic_sell_archive|magic_sell_editor|launch_buy|launch_buy_archive|launch_buy_editor|sniper_wizard|staking|vanity_wallet|community_vision|community_vision_archive|community_vision_editor|wallet_tracker|wallet_tracker_archive|wallet_tracker_editor|x_followers|engagement|subscriptions_accounts|subscriptions_catalog|accounts_catalog|resizer)$/, async (ctx) => {
  const route = ctx.match[1];
  const userId = String(ctx.from.id);
  const user = route.startsWith('buy_sell')
    ? await refreshTradingDesk(userId)
    : route.startsWith('launch_buy')
      ? await refreshLaunchBuy(userId)
      : await getUserState(userId);
  await ctx.answerCallbackQuery({ text: 'Refreshed' });
  await renderScreen(ctx, route, user);
});

bot.callbackQuery('payment:refresh', async (ctx) => {
  const userId = String(ctx.from.id);
  const user = await getUserState(userId);

  if (!user.selection.amount) {
    await ctx.answerCallbackQuery({ text: 'Pick a bundle first.' });
    await renderScreen(ctx, 'amount', user);
    return;
  }

  try {
    const updated = await createPaymentQuote(userId);
    await ctx.answerCallbackQuery({ text: 'New SOL quote ready.' });
    await renderScreen(ctx, 'payment', updated);
  } catch (error) {
    const updated = await setPaymentQuoteError(userId, user.selection.amount, String(error.message || error));
    await ctx.answerCallbackQuery({ text: 'Unable to build a quote right now.' });
    await renderScreen(ctx, 'payment', updated);
  }
});

bot.callbackQuery('payment:check', async (ctx) => {
  const userId = String(ctx.from.id);
  const user = await getUserState(userId);

  if (!paymentNeedsChecking(user)) {
    await ctx.answerCallbackQuery({ text: 'No pending quote to check.' });
    await renderScreen(ctx, hasLaunchAccess(user) ? 'confirm' : 'payment', user);
    return;
  }

  try {
    const result = await checkPaymentForUser(userId);
    if (result.matched) {
      await ctx.answerCallbackQuery({ text: 'Payment found.' });
      await notifyPaymentMatched(userId, result.user);
      await renderScreen(ctx, 'confirm', result.user);
      return;
    }

    await ctx.answerCallbackQuery({ text: quoteExpired(result.user.payment) ? 'Quote expired.' : 'Still waiting for payment.' });
    await renderScreen(ctx, 'payment', result.user);
  } catch (error) {
    const friendlyError = error?.code === 'SOLANA_RPC_RATE_LIMIT'
      ? 'Payment check is temporarily rate-limited. Try again shortly.'
      : String(error.message || error);
    const updated = await updateUserState(userId, (draft) => {
      draft.payment.lastCheckAt = new Date().toISOString();
      draft.payment.lastError = friendlyError;
      return draft;
    });
    await ctx.answerCallbackQuery({
      text: error?.code === 'SOLANA_RPC_RATE_LIMIT' ? 'RPC busy, try again soon.' : 'Payment check failed.',
    });
    await renderScreen(ctx, 'payment', updated);
  }
});

bot.callbackQuery('trial:start', async (ctx) => {
  const userId = String(ctx.from.id);
  const user = await getUserState(userId);

  if (!trialIsAvailable(user)) {
    await ctx.answerCallbackQuery({ text: 'Your free trial has already been used.' });
    await renderScreen(ctx, 'amount', user);
    return;
  }

  const updated = await updateUserState(userId, (draft) => {
    draft.selection.amount = cfg.freeTrialAmount;
    draft.selection.usingFreeTrial = true;
    draft.selection.target = draft.selection.target || cfg.defaultTarget;
    draft.awaitingTargetInput = false;
    draft.payment = createDefaultPaymentState();
    return draft;
  });

  await ctx.answerCallbackQuery({ text: `Free trial selected: x${cfg.freeTrialAmount}` });
  await renderScreen(ctx, 'target', updated);
});

bot.callbackQuery(/^button:(.+)$/, async (ctx) => {
  const buttonKey = ctx.match[1];
  if (!BUTTONS[buttonKey]) {
    await ctx.answerCallbackQuery({ text: 'Unknown button.' });
    return;
  }

  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.selection.button = buttonKey;
    draft.selection.target = draft.selection.target || cfg.defaultTarget;
    draft.awaitingTargetInput = false;
    return draft;
  });

  await ctx.answerCallbackQuery({ text: `${BUTTONS[buttonKey].label} selected` });
  await renderScreen(ctx, 'amount', updated);
});

bot.callbackQuery(/^amount:(\d+):(trial|paid)$/, async (ctx) => {
  const amount = Number.parseInt(ctx.match[1], 10);
  const kind = ctx.match[2];
  const userId = String(ctx.from.id);
  const currentUser = await getUserState(userId);

  if (kind === 'trial' && !trialIsAvailable(currentUser)) {
    await ctx.answerCallbackQuery({ text: 'Your free trial has already been used.' });
    await renderScreen(ctx, 'amount', currentUser);
    return;
  }

  const bundle = getBundlePricing(amount);
  if (kind === 'paid' && !bundle) {
    await ctx.answerCallbackQuery({ text: 'That bundle is not configured.' });
    return;
  }

  await updateUserState(userId, (draft) => {
    draft.selection.amount = amount;
    draft.selection.usingFreeTrial = kind === 'trial';
    draft.selection.target = draft.selection.target || cfg.defaultTarget;
    draft.awaitingTargetInput = false;
    draft.payment = createDefaultPaymentState();
    return draft;
  });

  let updated;
  if (kind === 'trial') {
    updated = await getUserState(userId);
  } else if (isAdminUser(userId)) {
    updated = await getUserState(userId);
  } else {
    try {
      updated = await createPaymentQuote(userId);
    } catch (error) {
      updated = await setPaymentQuoteError(userId, amount, String(error.message || error));
    }
  }

  await ctx.answerCallbackQuery({
    text: kind === 'trial'
      ? `Free trial selected: x${cfg.freeTrialAmount}`
      : `Bundle selected: ${amount} apples for about ${bundleSolDisplay(amount)}`,
  });
  await renderScreen(ctx, 'target', updated);
});

bot.callbackQuery('run:confirm', handleRun);

bot.on('message:text', async (ctx) => {
  const userId = String(ctx.from.id);
  const user = await getUserState(userId);
  const activeAgent = getActiveBurnAgent(user);

  if (user.xFollowers?.awaitingField === 'target') {
    const raw = ctx.message.text.trim();
    const normalizedTarget = raw.startsWith('@')
      ? `https://x.com/${raw.slice(1)}`
      : raw;
    const updated = await updateUserState(userId, (draft) => {
      draft.xFollowers = normalizeXFollowersState({
        ...draft.xFollowers,
        target: normalizedTarget,
        awaitingField: null,
        payment: createDefaultPaymentState(),
        lastError: null,
      });
      return draft;
    });
    await ctx.reply(
      [
        '*X Followers Updated*',
        '',
        `Target: \`${normalizedTarget}\``,
        '',
        'Pick or refresh your quote when you are ready to pay.',
      ].join('\n'),
      {
        parse_mode: 'Markdown',
        reply_markup: makeXFollowersKeyboard(updated),
      },
    );
    return;
  }

  if (user.resizer?.awaitingImage) {
    await ctx.reply(
      [
        'ðŸ–¼ï¸ *Resizer Is Waiting For An Image*',
        '',
        'Send a photo or image file here and Iâ€™ll resize it for you.',
      ].join('\n'),
      {
        parse_mode: 'Markdown',
        reply_markup: makeResizerKeyboard(user),
      },
    );
    return;
  }

  if (activeAgent.awaitingField) {
    const candidate = ctx.message.text.trim();
    const previousMintAddress = activeAgent.mintAddress;

    try {
      let summaryTitle = '*Burn Agent Updated*';
      let summaryLines = [];
      let updated;

      if (activeAgent.awaitingField === 'withdraw_address') {
        const destinationAddress = normalizePublicKey(candidate, 'Withdraw destination');
        const { signature, withdrawLamports } = await withdrawBurnAgentFunds(activeAgent, destinationAddress);
        updated = await updateUserState(userId, (draft) => {
          updateBurnAgentInDraft(draft, activeAgent.id, (agent) => {
            agent.awaitingField = null;
            agent.updatedAt = new Date().toISOString();
            return agent;
          });
          return draft;
        });
        summaryTitle = '*Withdrawal Sent*';
        summaryLines = [
          `Amount: *${formatSolAmountFromLamports(withdrawLamports)} SOL*`,
          `Destination: \`${destinationAddress}\``,
          `Signature: \`${signature}\``,
        ];
      } else {
        updated = await updateUserState(userId, (draft) => {
          updateBurnAgentInDraft(draft, activeAgent.id, (agent) => {
            let nextAwaitingField = null;
            switch (agent.awaitingField) {
              case 'private_key': {
                const wallet = parseWalletFromSecretInput(candidate);
                agent.walletMode = 'provided';
                agent.walletAddress = wallet.address;
                agent.walletSecretKeyB64 = wallet.secretKeyB64;
                agent.walletSecretKeyBase58 = wallet.secretKeyBase58;
                agent.automationEnabled = false;
                agent.privateKeyVisible = false;
                break;
              }
              case 'token_name':
                agent.tokenName = String(candidate || '').trim().slice(0, 40) || null;
                break;
              case 'mint':
                agent.mintAddress = normalizePublicKey(candidate, 'Mint');
                if (!agent.tokenName) {
                  nextAwaitingField = 'token_name';
                }
                break;
              case 'treasury':
                agent.treasuryAddress = normalizePublicKey(candidate, 'Treasury wallet');
                break;
              case 'burn_percent': {
                const burnPercent = parsePercentInput(candidate, 'Burn percentage');
                if (Number.isInteger(agent.treasuryPercent) && burnPercent + agent.treasuryPercent !== 100) {
                  throw new Error('Burn % plus treasury % must equal 100.');
                }
                agent.burnPercent = burnPercent;
                break;
              }
              case 'treasury_percent': {
                const treasuryPercent = parsePercentInput(candidate, 'Treasury percentage');
                if (Number.isInteger(agent.burnPercent) && treasuryPercent + agent.burnPercent !== 100) {
                  throw new Error('Burn % plus treasury % must equal 100.');
                }
                agent.treasuryPercent = treasuryPercent;
                break;
              }
              default:
                throw new Error('Unknown Burn Agent field.');
            }

            agent.awaitingField = nextAwaitingField;
            agent.deleteConfirmations = 0;
            agent.updatedAt = new Date().toISOString();
            return agent;
          });
          return draft;
        });
      }

      const balanceLamports = await getBurnAgentBalanceLamports(updated.burnAgent);
      if (
        activeAgent.awaitingField === 'mint'
        && updated.burnAgent.mintAddress
        && updated.burnAgent.mintAddress !== previousMintAddress
        && updated.burnAgent.lastAnnouncedMintAddress !== updated.burnAgent.mintAddress
      ) {
        const announced = await announceBurnAgentAttachedToAlerts(userId, updated.burnAgent);
        if (announced) {
          updated = await updateUserState(userId, (draft) => {
            updateBurnAgentInDraft(draft, activeAgent.id, (agent) => ({
              ...agent,
              lastAnnouncedMintAddress: updated.burnAgent.mintAddress,
            }));
            return draft;
          });
        }
      }

      await ctx.reply(
        [
          summaryTitle,
          ...summaryLines,
          ...(summaryLines.length > 0 ? [''] : []),
          '',
          burnAgentEditorText(updated, balanceLamports),
        ].join('\n'),
        {
          parse_mode: 'Markdown',
          reply_markup: makeBurnAgentEditorKeyboard(updated),
        },
      );
    } catch (error) {
      await ctx.reply(
        [
          `ÃƒÂ¢Ã…Â¡Ã‚Â ÃƒÂ¯Ã‚Â¸Ã‚Â ${String(error.message || error)}`,
          '',
          promptForBurnAgentField(activeAgent.awaitingField),
        ].join('\n'),
        {
          parse_mode: 'Markdown',
          reply_markup: makeBurnAgentEditorKeyboard(user),
        },
      );
    }
    return;
  }

  if (user.organicVolumeOrder.awaitingField) {
    const candidate = ctx.message.text.trim();
    const activeBoosterId = user.activeAppleBoosterId;
    const boosterName = appleBoosterDisplayName(user.organicVolumeOrder);

    try {
      let summaryTitle = `*${boosterName} Updated*`;
      let summaryLines = [];
      let updated;

      if (user.organicVolumeOrder.awaitingField === 'withdraw_address') {
        const destinationAddress = normalizePublicKey(candidate, 'Withdraw destination');
        const { signature, withdrawLamports } = await withdrawOrganicOrderFunds(
          user.organicVolumeOrder,
          destinationAddress,
        );
        updated = await updateUserState(userId, (draft) => {
          updateAppleBoosterInDraft(draft, activeBoosterId, (order) => {
            order.awaitingField = null;
            order.running = false;
            order.appleBooster.status = 'stopped';
            return order;
          });
          return draft;
        });
        await appendUserActivityLog(userId, {
          scope: appleBoosterScope(activeBoosterId),
          level: 'info',
          message: `${boosterName} withdrew ${formatSolAmountFromLamports(withdrawLamports)} SOL to ${destinationAddress}.`,
        });
        summaryTitle = `*${boosterName} Withdrawal Sent*`;
        summaryLines = [
          `Amount: *${formatSolAmountFromLamports(withdrawLamports)} SOL*`,
          `Destination: \`${destinationAddress}\``,
          `Signature: \`${signature}\``,
        ];
      } else {
        updated = await updateUserState(userId, (draft) => {
          updateAppleBoosterInDraft(draft, activeBoosterId, (order) => {
            switch (order.awaitingField) {
              case 'wallet_count': {
                const walletCount = parseOrganicWalletCountInput(candidate);
                order.appleBooster.walletCount = walletCount;
                order.appleBooster.workerWallets = createAppleBoosterWorkerWallets(walletCount);
                order.appleBooster.totalManagedLamports = order.currentLamports;
                order.appleBooster.nextActionAt = null;
                order.appleBooster.status = 'idle';
                order.running = false;
                order.appleBooster.stopRequested = false;
                break;
              }
              case 'mint':
                order.appleBooster.mintAddress = normalizePublicKey(candidate, 'Mint');
                break;
              case 'swap_range': {
                const swapRange = parseOrganicSwapRangeInput(candidate);
                order.appleBooster.minSwapSol = swapRange.minSol;
                order.appleBooster.maxSwapSol = swapRange.maxSol;
                order.appleBooster.minSwapLamports = swapRange.minLamports;
                order.appleBooster.maxSwapLamports = swapRange.maxLamports;
                break;
              }
              case 'interval_range': {
                const intervalRange = parseOrganicIntervalRangeInput(candidate);
                order.appleBooster.minIntervalSeconds = intervalRange.minSeconds;
                order.appleBooster.maxIntervalSeconds = intervalRange.maxSeconds;
                break;
              }
              default:
                throw new Error('Unknown Apple Booster field.');
            }

            order.awaitingField = null;
            order.appleBooster.lastError = null;
            return order;
          });
          return draft;
        });
        await appendUserActivityLog(userId, {
          scope: appleBoosterScope(activeBoosterId),
          level: 'info',
          message: `${boosterName} settings were updated from Telegram.`,
        });
      }

      await ctx.reply(
        [
          summaryTitle,
          ...summaryLines,
          ...(summaryLines.length > 0 ? [''] : []),
          '',
          organicVolumeOrderText(updated),
        ].join('\n'),
        {
          parse_mode: 'Markdown',
          reply_markup: makeOrganicVolumeOrderKeyboard(updated),
        },
      );
    } catch (error) {
      await ctx.reply(
        [
          `ÃƒÂ¢Ã…Â¡Ã‚Â ÃƒÂ¯Ã‚Â¸Ã‚Â ${String(error.message || error)}`,
          '',
          promptForOrganicField(user.organicVolumeOrder.awaitingField),
        ].join('\n'),
        {
          parse_mode: 'Markdown',
          reply_markup: makeOrganicVolumeOrderKeyboard(user),
        },
      );
    }
    return;
  }

  if (user.magicSell.awaitingField) {
    const candidate = ctx.message.text.trim();
    const activeMagicSell = user.magicSell;

    try {
      const updated = await updateUserState(userId, (draft) => {
        updateMagicSellInDraft(draft, activeMagicSell.id, (order) => {
          switch (order.awaitingField) {
            case 'token_name':
              order.tokenName = String(candidate || '').trim().slice(0, 40) || null;
              break;
            case 'mint':
              order.mintAddress = normalizePublicKey(candidate, 'Mint');
              order.tokenDecimals = null;
              order.tokenProgram = null;
              break;
            case 'target_market_cap':
              order.targetMarketCapUsd = parseMagicSellTargetInput(candidate);
              break;
            case 'whitelist':
              order.whitelistWallets = parseMagicSellWhitelistInput(candidate);
              break;
            case 'seller_wallet_count': {
              const count = parseMagicSellWalletCountInput(candidate);
              order.sellerWalletCount = count;
              order.sellerWallets = createMagicSellSellerWallets(count);
              order.recommendedGasLamports = MAGIC_SELL_WORKER_GAS_RESERVE_LAMPORTS * count;
              break;
            }
            default:
              throw new Error('Unknown Magic Sell field.');
          }

          order.awaitingField = null;
          order.privateKeyVisible = false;
          order.lastError = null;
          order.updatedAt = new Date().toISOString();
          return order;
        });
        return draft;
      });

      const refreshed = updated.magicSell.walletAddress
        ? await refreshMagicSell(userId, updated.activeMagicSellId)
        : updated;

      await appendUserActivityLog(userId, {
        scope: magicSellScope(refreshed.magicSell.id),
        level: 'info',
        message: 'Magic Sell settings were updated from Telegram.',
      });

      await ctx.reply(
        [
          '*Magic Sell Updated*',
          '',
          magicSellEditorText(refreshed),
        ].join('\n'),
        {
          parse_mode: 'Markdown',
          reply_markup: makeMagicSellEditorKeyboard(refreshed),
        },
      );
    } catch (error) {
      await ctx.reply(
        [
          `ÃƒÂ¢Ã…Â¡Ã‚Â ÃƒÂ¯Ã‚Â¸Ã‚Â ${String(error.message || error)}`,
          '',
          promptForMagicSellField(user.magicSell.awaitingField),
        ].join('\n'),
        {
          parse_mode: 'Markdown',
          reply_markup: makeMagicSellEditorKeyboard(user),
        },
      );
    }
    return;
  }

  if (user.vanityWallet?.awaitingField === 'pattern') {
    const candidate = ctx.message.text.trim();

    try {
      const pattern = normalizeVanityPatternInput(candidate);
      const updated = await updateUserState(userId, (draft) => {
        draft.vanityWallet = normalizeVanityWalletState({
          ...draft.vanityWallet,
          pattern,
          awaitingField: null,
          status: draft.vanityWallet.patternMode ? 'setup' : draft.vanityWallet.status,
          payment: createDefaultPaymentState(),
          generatedAddress: null,
          generatedSecretKeyB64: null,
          generatedSecretKeyBase58: null,
          privateKeyVisible: false,
          attemptCount: 0,
          generationStartedAt: null,
          completedAt: null,
          lastError: null,
        });
        return draft;
      });

      await appendUserActivityLog(userId, {
        scope: `vanity_wallet:${updated.vanityWallet.id}`,
        level: 'info',
        message: `Vanity wallet pattern set to ${updated.vanityWallet.patternMode === 'suffix' ? 'suffix' : 'prefix'} ${pattern}.`,
      });

      await ctx.reply(
        [
          '*Vanity Wallet Updated*',
          '',
          vanityWalletText(updated),
        ].join('\n'),
        {
          parse_mode: 'Markdown',
          reply_markup: makeVanityWalletKeyboard(updated),
        },
      );
    } catch (error) {
      await ctx.reply(
        [
          `âš ï¸ ${String(error.message || error)}`,
          '',
          'Send a short base58 pattern like `wiz` or `moon`.',
        ].join('\n'),
        {
          parse_mode: 'Markdown',
          reply_markup: makeVanityWalletKeyboard(user),
        },
      );
    }
    return;
  }

  if (user.communityVision?.awaitingField) {
    const candidate = ctx.message.text.trim();
    const activeCommunityVision = user.communityVision;

    try {
      const updated = await updateUserState(userId, (draft) => {
        updateCommunityVisionInDraft(draft, activeCommunityVision.id, (order) => {
          switch (order.awaitingField) {
            case 'profile_url': {
              const parsed = parseCommunityVisionProfileInput(candidate);
              order.profileUrl = parsed.profileUrl;
              order.handle = parsed.handle;
              order.trackedCommunities = [];
              order.stats = {};
              order.lastCheckedAt = null;
              order.lastAlertAt = null;
              order.lastChangeAt = null;
              order.status = order.automationEnabled ? 'watching' : 'setup';
              break;
            }
            default:
              throw new Error('Unknown Vision field.');
          }

          order.awaitingField = null;
          order.deleteConfirmations = 0;
          order.lastError = null;
          order.updatedAt = new Date().toISOString();
          return order;
        });
        return draft;
      });

      await appendUserActivityLog(userId, {
        scope: communityVisionScope(updated.communityVision.id),
        level: 'info',
        message: 'Vision watch settings were updated from Telegram.',
      });

      await ctx.reply(
        [
          '*Vision Updated*',
          '',
          communityVisionEditorText(updated),
        ].join('\n'),
        {
          parse_mode: 'Markdown',
          reply_markup: makeCommunityVisionEditorKeyboard(updated),
        },
      );
    } catch (error) {
      await ctx.reply(
        [
          `\u26A0\uFE0F ${String(error.message || error)}`,
          '',
          promptForCommunityVisionField(user.communityVision.awaitingField),
        ].join('\n'),
        {
          parse_mode: 'Markdown',
          reply_markup: makeCommunityVisionEditorKeyboard(user),
        },
      );
    }
    return;
  }

  if (user.walletTracker?.awaitingField) {
    const candidate = ctx.message.text.trim();
    const activeWalletTracker = user.walletTracker;

    try {
      const updated = await updateUserState(userId, (draft) => {
        updateWalletTrackerInDraft(draft, activeWalletTracker.id, (order) => {
          switch (order.awaitingField) {
            case 'wallet_address':
              order.walletAddress = normalizePublicKey(candidate, 'Wallet');
              order.lastSeenSignature = null;
              order.notifiedBuyMints = [];
              order.stats = {};
              order.lastCheckedAt = null;
              order.lastAlertAt = null;
              order.lastEventAt = null;
              order.status = order.automationEnabled ? 'watching' : 'setup';
              break;
            default:
              throw new Error('Unknown Wallet Tracker field.');
          }

          order.awaitingField = null;
          order.deleteConfirmations = 0;
          order.lastError = null;
          order.updatedAt = new Date().toISOString();
          return order;
        });
        return draft;
      });

      await appendUserActivityLog(userId, {
        scope: walletTrackerScope(updated.walletTracker.id),
        level: 'info',
        message: 'Wallet Tracker settings were updated from Telegram.',
      });

      await ctx.reply(
        [
          '*Wallet Tracker Updated*',
          '',
          walletTrackerEditorText(updated),
        ].join('\n'),
        {
          parse_mode: 'Markdown',
          reply_markup: makeWalletTrackerEditorKeyboard(updated),
        },
      );
    } catch (error) {
      await ctx.reply(
        [
          `\u26A0\uFE0F ${String(error.message || error)}`,
          '',
          promptForWalletTrackerField(user.walletTracker.awaitingField),
        ].join('\n'),
        {
          parse_mode: 'Markdown',
          reply_markup: makeWalletTrackerEditorKeyboard(user),
        },
      );
    }
    return;
  }

  if (user.tradingDesk.awaitingField) {
    const candidate = ctx.message.text.trim();

    try {
      const updated = await updateUserState(userId, (draft) => {
        const tradingDesk = normalizeTradingDesk(draft.tradingDesk);
        switch (tradingDesk.awaitingField) {
          case 'import_wallet': {
            const imported = parseWalletFromSecretInput(candidate);
            const nextWallet = normalizeTradingWallet({
              id: createTradingWalletId(),
              label: 'Imported Wallet',
              address: imported.address,
              secretKeyB64: imported.secretKeyB64,
              secretKeyBase58: imported.secretKeyBase58,
              imported: true,
              privateKeyVisible: false,
              createdAt: new Date().toISOString(),
            });
            tradingDesk.wallets = [...tradingDesk.wallets, nextWallet];
            tradingDesk.activeWalletId = nextWallet.id;
            break;
          }
          case 'quick_trade_mint':
            tradingDesk.quickTradeMintAddress = normalizePublicKey(candidate, 'Token CA');
            break;
          case 'quick_buy_sol': {
            const lamports = parseSolToLamports(candidate);
            if (!lamports) {
              throw new Error('Enter a SOL amount greater than 0.');
            }
            tradingDesk.quickBuyLamports = lamports;
            tradingDesk.quickBuySol = formatSolAmountFromLamports(lamports);
            break;
          }
          case 'quick_sell_percent': {
            const percent = Number.parseInt(candidate, 10);
            if (!Number.isInteger(percent) || percent < 1 || percent > 100) {
              throw new Error('Enter a sell percentage from 1 to 100.');
            }
            tradingDesk.quickSellPercent = percent;
            break;
          }
          case 'limit_trigger_market_cap': {
            const value = Number(candidate.replace(/[$,\s]/g, ''));
            if (!Number.isFinite(value) || value <= 0) {
              throw new Error('Enter a market cap in USD like 25000.');
            }
            tradingDesk.limitOrder = {
              ...tradingDesk.limitOrder,
              triggerMarketCapUsd: value,
            };
            break;
          }
          case 'limit_buy_sol': {
            const lamports = parseSolToLamports(candidate);
            if (!lamports) {
              throw new Error('Enter a SOL amount greater than 0.');
            }
            tradingDesk.limitOrder = {
              ...tradingDesk.limitOrder,
              buyLamports: lamports,
              buySol: formatSolAmountFromLamports(lamports),
            };
            break;
          }
          case 'limit_sell_percent': {
            const percent = Number.parseInt(candidate, 10);
            if (!Number.isInteger(percent) || percent < 1 || percent > 100) {
              throw new Error('Enter a sell percentage from 1 to 100.');
            }
            tradingDesk.limitOrder = {
              ...tradingDesk.limitOrder,
              sellPercent: percent,
            };
            break;
          }
          case 'copy_follow_wallet':
            tradingDesk.copyTrade = {
              ...tradingDesk.copyTrade,
              followWalletAddress: normalizePublicKey(candidate, 'Follow wallet'),
            };
            break;
          case 'copy_fixed_buy_sol': {
            const lamports = parseSolToLamports(candidate);
            if (!lamports) {
              throw new Error('Enter a SOL amount greater than 0.');
            }
            tradingDesk.copyTrade = {
              ...tradingDesk.copyTrade,
              fixedBuyLamports: lamports,
              fixedBuySol: formatSolAmountFromLamports(lamports),
            };
            break;
          }
          default:
            throw new Error('Unknown Buy / Sell field.');
        }

        tradingDesk.awaitingField = null;
        tradingDesk.lastError = null;
        draft.tradingDesk = normalizeTradingDesk(tradingDesk);
        return draft;
      });

      const refreshed = await refreshTradingDesk(userId);
      const nextUser = refreshed || updated;
      await ctx.reply(
        [
          '*Buy / Sell Updated*',
          '',
          user.tradingDesk.awaitingField === 'import_wallet'
            ? buySellWalletsText(nextUser)
            : ['limit_trigger_market_cap', 'limit_buy_sol', 'limit_sell_percent'].includes(user.tradingDesk.awaitingField)
              ? buySellLimitLiveText(nextUser)
              : ['copy_follow_wallet', 'copy_fixed_buy_sol'].includes(user.tradingDesk.awaitingField)
                ? buySellCopyLiveText(nextUser)
                : buySellQuickLiveText(nextUser),
        ].join('\n'),
        {
          parse_mode: 'Markdown',
          reply_markup: user.tradingDesk.awaitingField === 'import_wallet'
            ? makeBuySellWalletsKeyboard(nextUser)
            : ['limit_trigger_market_cap', 'limit_buy_sol', 'limit_sell_percent'].includes(user.tradingDesk.awaitingField)
              ? makeBuySellLimitLiveKeyboard(nextUser)
              : ['copy_follow_wallet', 'copy_fixed_buy_sol'].includes(user.tradingDesk.awaitingField)
                ? makeBuySellCopyLiveKeyboard(nextUser)
                : makeBuySellQuickLiveKeyboard(nextUser),
        },
      );
    } catch (error) {
      const route = user.tradingDesk.awaitingField === 'import_wallet'
        ? 'buy_sell_wallets'
        : ['limit_trigger_market_cap', 'limit_buy_sol', 'limit_sell_percent'].includes(user.tradingDesk.awaitingField)
          ? 'buy_sell_limit'
          : ['copy_follow_wallet', 'copy_fixed_buy_sol'].includes(user.tradingDesk.awaitingField)
            ? 'buy_sell_copy'
            : 'buy_sell_quick';
      await ctx.reply(
        [
          `\u26A0\uFE0F ${String(error.message || error)}`,
          '',
          promptForBuySellField(user.tradingDesk.awaitingField),
        ].join('\n'),
        {
          parse_mode: 'Markdown',
          reply_markup: route === 'buy_sell_wallets'
            ? makeBuySellWalletsKeyboard(user)
            : route === 'buy_sell_limit'
              ? makeBuySellLimitLiveKeyboard(user)
              : route === 'buy_sell_copy'
                ? makeBuySellCopyLiveKeyboard(user)
                : makeBuySellQuickLiveKeyboard(user),
        },
      );
    }
    return;
  }

  if (user.magicBundle.awaitingField) {
    const candidate = ctx.message.text.trim();
    const activeMagicBundle = user.magicBundle;

    try {
      const updated = await updateUserState(userId, (draft) => {
        updateMagicBundleInDraft(draft, activeMagicBundle.id, (order) => {
          switch (order.awaitingField) {
            case 'token_name':
              order.tokenName = String(candidate || '').trim().slice(0, 40) || null;
              break;
            case 'mint':
              order.mintAddress = normalizePublicKey(candidate, 'Mint');
              break;
            case 'wallet_count': {
              const count = parseMagicBundleWalletCountInput(candidate);
              order.walletCount = count;
              order.splitWallets = createMagicBundleWorkerWallets(count);
              break;
            }
            case 'stop_loss':
              order.stopLossPercent = parseMagicBundlePercentInput(candidate, 'Stop loss');
              break;
            case 'take_profit':
              order.takeProfitPercent = parseMagicBundlePercentInput(candidate, 'Take profit');
              break;
            case 'trailing_stop_loss':
              order.trailingStopLossPercent = parseMagicBundlePercentInput(candidate, 'Trailing stop loss');
              break;
            case 'buy_dip':
              order.buyDipPercent = parseMagicBundlePercentInput(candidate, 'Buy dip');
              break;
            default:
              throw new Error('Unknown Magic Bundle field.');
          }

          order.awaitingField = null;
          order.lastError = null;
          order.updatedAt = new Date().toISOString();
          return order;
        });
        return draft;
      });

      const refreshed = updated.magicBundle.walletAddress
        ? await refreshMagicBundle(userId, updated.activeMagicBundleId)
        : updated;

      await appendUserActivityLog(userId, {
        scope: magicBundleScope(refreshed.magicBundle.id),
        level: 'info',
        message: 'Magic Bundle settings were updated from Telegram.',
      });

      await ctx.reply(
        [
          '*Magic Bundle Updated*',
          '',
          magicBundleEditorText(refreshed),
        ].join('\n'),
        {
          parse_mode: 'Markdown',
          reply_markup: makeMagicBundleEditorKeyboard(refreshed),
        },
      );
    } catch (error) {
      await ctx.reply(
        [
          `\u26A0\uFE0F ${String(error.message || error)}`,
          '',
          promptForMagicBundleField(user.magicBundle.awaitingField),
        ].join('\n'),
        {
          parse_mode: 'Markdown',
          reply_markup: makeMagicBundleEditorKeyboard(user),
        },
      );
    }
    return;
  }

  if (user.launchBuy.awaitingField) {
    const candidate = ctx.message.text.trim();
    const activeLaunchBuy = user.launchBuy;

    if (activeLaunchBuy.awaitingField === 'logo') {
      await ctx.reply(
        [
          'ðŸ–¼ï¸ *Launch + Buy Is Waiting For A Logo*',
          '',
          promptForLaunchBuyField('logo', activeLaunchBuy),
        ].join('\n'),
        {
          parse_mode: 'Markdown',
          reply_markup: makeLaunchBuyEditorKeyboard(user),
        },
      );
      return;
    }

    try {
      const updated = await updateUserState(userId, (draft) => {
        updateLaunchBuyInDraft(draft, activeLaunchBuy.id, (order) => {
          switch (order.awaitingField) {
            case 'token_name':
              order.tokenName = String(candidate || '').trim().slice(0, 40) || null;
              break;
            case 'symbol':
              order.symbol = String(candidate || '').trim().replace(/\s+/g, '').slice(0, 12).toUpperCase() || null;
              break;
            case 'description':
              order.description = String(candidate || '').trim().slice(0, 500) || null;
              break;
            case 'wallet_count': {
              const count = parseLaunchBuyWalletCountInput(candidate);
              order.buyerWalletCount = count;
              if (order.walletSource === 'generated') {
                order.buyerWallets = createLaunchBuyBuyerWallets(count);
              }
              break;
            }
            case 'buyer_keys': {
              const importedWallets = parseLaunchBuyPrivateKeysInput(candidate, order.buyerWalletCount);
              order.walletSource = 'imported';
              order.buyerWalletCount = importedWallets.length;
              order.buyerWallets = importedWallets;
              break;
            }
            case 'total_buy': {
              const lamports = parseSolAmountToLamports(candidate, 'Launch buy budget');
              if (lamports <= 0) {
                throw new Error('Launch buy budget must be more than 0 SOL.');
              }
              order.totalBuyLamports = lamports;
              order.totalBuySol = formatSolAmountFromLamports(lamports);
              break;
            }
            case 'jito_tip': {
              const lamports = parseSolAmountToLamports(candidate, 'Jito tip');
              if (lamports <= 0) {
                throw new Error('Jito tip must be more than 0 SOL.');
              }
              order.jitoTipLamports = lamports;
              order.jitoTipSol = formatSolAmountFromLamports(lamports);
              break;
            }
            case 'website':
              order.website = normalizeOptionalLaunchUrl(candidate, 'Website');
              break;
            case 'telegram':
              order.telegram = normalizeOptionalLaunchSocial(candidate, 'Telegram', 'telegram');
              break;
            case 'twitter':
              order.twitter = normalizeOptionalLaunchSocial(candidate, 'X profile', 'twitter');
              break;
            default:
              throw new Error('Unknown Launch + Buy field.');
          }

          order.awaitingField = null;
          order.lastError = null;
          order.deleteConfirmations = 0;
          order.updatedAt = new Date().toISOString();
          return order;
        });
        return draft;
      });

      const refreshed = updated.launchBuy.walletAddress
        ? await refreshLaunchBuy(userId, updated.activeLaunchBuyId)
        : updated;

      await appendUserActivityLog(userId, {
        scope: launchBuyScope(refreshed.launchBuy.id),
        level: 'info',
        message: 'Launch + Buy settings were updated from Telegram.',
      });

      await ctx.reply(
        [
          '*Launch + Buy Updated*',
          '',
          launchBuyEditorText(refreshed),
        ].join('\n'),
        {
          parse_mode: 'Markdown',
          reply_markup: makeLaunchBuyEditorKeyboard(refreshed),
        },
      );
    } catch (error) {
      await ctx.reply(
        [
          `âš ï¸ ${String(error.message || error)}`,
          '',
          promptForLaunchBuyField(user.launchBuy.awaitingField, user.launchBuy),
        ].join('\n'),
        {
          parse_mode: 'Markdown',
          reply_markup: makeLaunchBuyEditorKeyboard(user),
        },
      );
    }
    return;
  }

  if (user.fomoBooster.awaitingField) {
    const candidate = ctx.message.text.trim();

    try {
      let summaryTitle = '*FOMO Booster Updated*';
      let summaryLines = [];
      let updated;

      if (user.fomoBooster.awaitingField === 'withdraw_address') {
        const destinationAddress = normalizePublicKey(candidate, 'Withdraw destination');
        const { signature, withdrawLamports } = await withdrawOrganicOrderFunds(
          user.fomoBooster,
          destinationAddress,
        );
        updated = await updateUserState(userId, (draft) => {
          draft.fomoBooster = normalizeFomoBooster({
            ...draft.fomoBooster,
            awaitingField: null,
            automationEnabled: false,
            status: 'stopped',
            updatedAt: new Date().toISOString(),
          });
          return draft;
        });

        await appendUserActivityLog(userId, {
          scope: fomoBoosterScope(updated.fomoBooster.id),
          level: 'info',
          message: `FOMO Booster withdrew ${formatSolAmountFromLamports(withdrawLamports)} SOL to ${destinationAddress}.`,
        });

        summaryTitle = '*FOMO Booster Withdrawal Sent*';
        summaryLines = [
          `Amount: *${formatSolAmountFromLamports(withdrawLamports)} SOL*`,
          `Destination: \`${destinationAddress}\``,
          `Signature: \`${signature}\``,
        ];
      } else {
        updated = await updateUserState(userId, (draft) => {
          const current = normalizeFomoBooster(draft.fomoBooster);
          const next = {
            ...current,
            awaitingField: null,
            privateKeyVisible: false,
            lastError: null,
            updatedAt: new Date().toISOString(),
          };

          switch (current.awaitingField) {
            case 'token_name':
              next.tokenName = String(candidate || '').trim().slice(0, 40) || null;
              break;
            case 'mint':
              next.mintAddress = normalizePublicKey(candidate, 'Mint');
              next.tokenDecimals = null;
              next.tokenProgram = null;
              break;
            case 'wallet_count': {
              const count = parseFomoWalletCountInput(candidate);
              next.walletCount = count;
              next.workerWallets = createFomoWorkerWallets(count);
              next.recommendedGasLamports = FOMO_WORKER_GAS_RESERVE_LAMPORTS * count;
              break;
            }
            case 'buy_range': {
              const range = parseOrganicSwapRangeInput(candidate);
              next.minBuySol = range.minSol;
              next.maxBuySol = range.maxSol;
              next.minBuyLamports = range.minLamports;
              next.maxBuyLamports = range.maxLamports;
              break;
            }
            case 'interval_range': {
              const range = parseOrganicIntervalRangeInput(candidate);
              next.minIntervalSeconds = range.minSeconds;
              next.maxIntervalSeconds = range.maxSeconds;
              break;
            }
            default:
              throw new Error('Unknown FOMO Booster field.');
          }

          draft.fomoBooster = normalizeFomoBooster(next);
          return draft;
        });

        await appendUserActivityLog(userId, {
          scope: fomoBoosterScope(updated.fomoBooster.id),
          level: 'info',
          message: 'FOMO Booster settings were updated from Telegram.',
        });
      }

      const refreshed = updated.fomoBooster.walletAddress
        ? await refreshFomoBooster(userId)
        : updated;

      await ctx.reply(
        [
          summaryTitle,
          ...summaryLines,
          ...(summaryLines.length > 0 ? [''] : []),
          '',
          fomoBoosterEditorText(refreshed),
        ].join('\n'),
        {
          parse_mode: 'Markdown',
          reply_markup: makeFomoBoosterEditorKeyboard(refreshed),
        },
      );
    } catch (error) {
      await ctx.reply(
        [
          `ÃƒÆ’Ã‚Â¢Ãƒâ€¦Ã‚Â¡Ãƒâ€šÃ‚Â ÃƒÆ’Ã‚Â¯Ãƒâ€šÃ‚Â¸Ãƒâ€šÃ‚Â ${String(error.message || error)}`,
          '',
          promptForFomoField(user.fomoBooster.awaitingField),
        ].join('\n'),
        {
          parse_mode: 'Markdown',
          reply_markup: makeFomoBoosterEditorKeyboard(user),
        },
      );
    }
    return;
  }

  if (user.sniperWizard.awaitingField) {
    const candidate = ctx.message.text.trim();

    try {
      let summaryTitle = '*Sniper Wizard Updated*';
      let summaryLines = [];
      let updated;

      if (user.sniperWizard.awaitingField === 'withdraw_address') {
        const destinationAddress = normalizePublicKey(candidate, 'Withdraw destination');
        const { signature, withdrawLamports } = await withdrawOrganicOrderFunds(
          user.sniperWizard,
          destinationAddress,
        );
        updated = await updateUserState(userId, (draft) => {
          draft.sniperWizard = normalizeSniperWizard({
            ...draft.sniperWizard,
            awaitingField: null,
            automationEnabled: false,
            status: 'stopped',
            updatedAt: new Date().toISOString(),
          });
          syncSniperWizardTradingDesk(draft);
          return draft;
        });

        await appendUserActivityLog(userId, {
          scope: sniperWizardScope(updated.sniperWizard.id),
          level: 'info',
          message: `Sniper Wizard withdrew ${formatSolAmountFromLamports(withdrawLamports)} SOL to ${destinationAddress}.`,
        });

        summaryTitle = '*Sniper Wizard Withdrawal Sent*';
        summaryLines = [
          `Amount: *${formatSolAmountFromLamports(withdrawLamports)} SOL*`,
          `Destination: \`${destinationAddress}\``,
          `Signature: \`${signature}\``,
        ];
      } else {
        updated = await updateUserState(userId, (draft) => {
          const current = normalizeSniperWizard(draft.sniperWizard);
          const next = {
            ...current,
            awaitingField: null,
            privateKeyVisible: false,
            lastError: null,
            updatedAt: new Date().toISOString(),
          };

          switch (current.awaitingField) {
            case 'wallet_count':
              next.walletCount = parseSniperWalletCountInput(candidate);
              next.workerWallets = createLaunchBuyBuyerWallets(next.walletCount);
              break;
            case 'target_wallet':
              next.targetWalletAddress = normalizePublicKey(candidate, 'Target wallet');
              break;
            case 'custom_percent':
              next.snipePercent = parseSniperPercentInput(candidate);
              break;
            default:
              throw new Error('Unknown Sniper Wizard field.');
          }

          draft.sniperWizard = normalizeSniperWizard(next);
          syncSniperWizardTradingDesk(draft, { selectFirst: current.awaitingField === 'wallet_count' });
          return draft;
        });

        await appendUserActivityLog(userId, {
          scope: sniperWizardScope(updated.sniperWizard.id),
          level: 'info',
          message: 'Sniper Wizard settings were updated from Telegram.',
        });
      }

      const refreshed = updated.sniperWizard.walletAddress
        ? await refreshSniperWizard(userId)
        : updated;

      await ctx.reply(
        [
          summaryTitle,
          ...summaryLines,
          ...(summaryLines.length > 0 ? [''] : []),
          '',
          sniperWizardEditorText(refreshed),
        ].join('\n'),
        {
          parse_mode: 'Markdown',
          reply_markup: makeSniperWizardKeyboard(refreshed),
        },
      );
    } catch (error) {
      await ctx.reply(
        [
          `\u26A0\uFE0F ${String(error.message || error)}`,
          '',
          promptForSniperField(user.sniperWizard.awaitingField),
        ].join('\n'),
        {
          parse_mode: 'Markdown',
          reply_markup: makeSniperWizardKeyboard(user),
        },
      );
    }
    return;
  }

  if (user.holderBooster.awaitingField) {
    const candidate = ctx.message.text.trim();

    try {
      const updated = await updateUserState(userId, (draft) => {
        const current = normalizeHolderBooster(draft.holderBooster);
        if (current.awaitingField === 'mint') {
          current.mintAddress = normalizePublicKey(candidate, 'Mint');
          current.tokenDecimals = null;
          current.tokenProgram = null;
          current.awaitingField = 'holder_count';
          current.status = 'idle';
          current.lastError = null;
          draft.holderBooster = normalizeHolderBooster(current);
          return draft;
        }

        if (current.awaitingField === 'holder_count') {
          if (!current.mintAddress) {
            throw new Error('Set the mint first before choosing the holder count.');
          }
          const holderCount = parseHolderCountInput(candidate);
          const wallet = generateSolanaWallet();
          current.holderCount = holderCount;
          current.walletAddress = wallet.address;
          current.walletSecretKeyB64 = wallet.secretKeyB64;
          current.walletSecretKeyBase58 = wallet.secretKeyBase58;
          current.requiredLamports = Math.round(holderCount * 0.10 * LAMPORTS_PER_SOL);
          current.requiredSol = (holderCount * 0.10).toFixed(2);
          current.requiredTokenAmountRaw = String(holderCount);
          current.childWallets = createHolderRecipientWallets(holderCount);
          current.awaitingField = null;
          current.status = 'awaiting_funding';
          current.createdAt = new Date().toISOString();
          current.lastError = null;
          draft.holderBooster = normalizeHolderBooster(current);
          return draft;
        }

        throw new Error('Unknown Holder Booster field.');
      });

      const refreshed = updated.holderBooster.walletAddress
        ? await refreshHolderBooster(userId)
        : updated;

      await appendUserActivityLog(userId, {
        scope: holderBoosterScope(refreshed.holderBooster.id),
        level: 'info',
        message: refreshed.holderBooster.awaitingField === 'holder_count'
          ? 'Holder Booster mint was set from Telegram.'
          : 'Holder Booster deposit wallet was created from Telegram.',
      });

      await ctx.reply(
        [
          '*Holder Booster Updated*',
          '',
          holderBoosterText(refreshed),
        ].join('\n'),
        {
          parse_mode: 'Markdown',
          reply_markup: makeHolderBoosterKeyboard(refreshed),
        },
      );
    } catch (error) {
      await ctx.reply(
        [
          `ÃƒÂ¢Ã…Â¡Ã‚Â ÃƒÂ¯Ã‚Â¸Ã‚Â ${String(error.message || error)}`,
          '',
          promptForHolderField(user.holderBooster.awaitingField),
        ].join('\n'),
        {
          parse_mode: 'Markdown',
          reply_markup: makeHolderBoosterKeyboard(user),
        },
      );
    }
    return;
  }

  if (!user.awaitingTargetInput) {
    return;
  }

  const candidate = ctx.message.text.trim();
  let parsed;
  try {
    parsed = new URL(candidate);
  } catch {
    await ctx.reply(
      'That is not a valid full URL. Send something like `https://example.com/path`.',
      { parse_mode: 'Markdown' },
    );
    return;
  }

  if (!['http:', 'https:'].includes(parsed.protocol)) {
    await ctx.reply('Only `http` and `https` targets are allowed.', {
      parse_mode: 'Markdown',
    });
    return;
  }

  const updated = await updateUserState(userId, (draft) => {
    draft.selection.target = parsed.toString();
    draft.awaitingTargetInput = false;
    return draft;
  });

  await ctx.reply(
    [
      '*Custom Target Updated*',
      '',
      selectionSnapshot(updated),
    ].join('\n'),
    {
      parse_mode: 'Markdown',
      reply_markup: updated.selection.button
        ? hasLaunchAccess(updated)
          ? makeConfirmKeyboard(updated)
          : makePaymentKeyboard(updated)
        : makeButtonKeyboard(updated.selection.button, updated),
    },
  );
});

bot.on('message:photo', async (ctx) => {
  const handledLaunchBuy = await processLaunchBuyLogoUpload(ctx);
  if (handledLaunchBuy) {
    return;
  }

  const handled = await processResizerUpload(ctx);
  if (!handled) {
    await ctx.reply('Choose `Resizer` or `Launch + Buy` from the menu first before sending an image.', {
      parse_mode: 'Markdown',
    });
  }
});

bot.callbackQuery('xfollowers:payment:refresh', async (ctx) => {
  const userId = String(ctx.from.id);
  const user = await getUserState(userId);

  if (!user.xFollowers.packageKey) {
    await ctx.answerCallbackQuery({ text: 'Pick a package first.' });
    await renderScreen(ctx, 'x_followers', user);
    return;
  }

  if (!user.xFollowers.target) {
    await ctx.answerCallbackQuery({ text: 'Set the X link first.' });
    await renderScreen(ctx, 'x_followers', user);
    return;
  }

  try {
    const updated = await createXFollowersQuote(userId);
    await ctx.answerCallbackQuery({ text: 'New SOL quote ready.' });
    await renderScreen(ctx, 'x_followers', updated);
  } catch (error) {
    const updated = await updateUserState(userId, (draft) => {
      draft.xFollowers = normalizeXFollowersState({
        ...draft.xFollowers,
        lastError: String(error.message || error),
      });
      return draft;
    });
    await ctx.answerCallbackQuery({ text: 'Unable to build a quote right now.' });
    await renderScreen(ctx, 'x_followers', updated);
  }
});

bot.callbackQuery('xfollowers:payment:check', async (ctx) => {
  const userId = String(ctx.from.id);
  const user = await getUserState(userId);

  if (!xFollowersNeedsChecking(user)) {
    await ctx.answerCallbackQuery({ text: 'No pending quote to check.' });
    await renderScreen(ctx, 'x_followers', user);
    return;
  }

  try {
    const result = await checkXFollowersPayment(userId);
    if (result.matched) {
      await ctx.answerCallbackQuery({ text: 'Payment found.' });
      await notifyXFollowersPaymentMatched(userId, result.user);
      await renderScreen(ctx, 'x_followers', result.user);
      return;
    }

    await ctx.answerCallbackQuery({ text: quoteExpired(result.user.xFollowers.payment) ? 'Quote expired.' : 'Still waiting for payment.' });
    await renderScreen(ctx, 'x_followers', result.user);
  } catch (error) {
    const updated = await updateUserState(userId, (draft) => {
      draft.xFollowers = normalizeXFollowersState({
        ...draft.xFollowers,
        payment: {
          ...draft.xFollowers.payment,
          lastCheckAt: new Date().toISOString(),
          lastError: String(error.message || error),
        },
        lastError: String(error.message || error),
      });
      return draft;
    });
    await ctx.answerCallbackQuery({ text: 'Payment check failed.' });
    await renderScreen(ctx, 'x_followers', updated);
  }
});

bot.callbackQuery(/^vanity:set:mode:(prefix|suffix)$/, async (ctx) => {
  const patternMode = ctx.match[1];
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.vanityWallet = normalizeVanityWalletState({
      ...draft.vanityWallet,
      patternMode,
      awaitingField: draft.vanityWallet.pattern ? draft.vanityWallet.awaitingField : 'pattern',
      status: draft.vanityWallet.pattern ? 'setup' : 'setup',
      payment: createDefaultPaymentState(),
      generatedAddress: null,
      generatedSecretKeyB64: null,
      generatedSecretKeyBase58: null,
      privateKeyVisible: false,
      attemptCount: 0,
      generationStartedAt: null,
      completedAt: null,
      lastError: null,
    });
    return draft;
  });
  await ctx.answerCallbackQuery({ text: patternMode === 'prefix' ? 'Starts With selected.' : 'Ends With selected.' });
  await renderScreen(ctx, 'vanity_wallet', updated);
});

bot.callbackQuery('vanity:set:pattern', async (ctx) => {
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.vanityWallet = normalizeVanityWalletState({
      ...draft.vanityWallet,
      awaitingField: 'pattern',
      payment: createDefaultPaymentState(),
      generatedAddress: null,
      generatedSecretKeyB64: null,
      generatedSecretKeyBase58: null,
      privateKeyVisible: false,
      attemptCount: 0,
      generationStartedAt: null,
      completedAt: null,
      status: draft.vanityWallet.patternMode ? 'setup' : draft.vanityWallet.status,
      lastError: null,
    });
    return draft;
  });
  await ctx.answerCallbackQuery({ text: 'Send the vanity pattern in chat.' });
  await renderScreen(ctx, 'vanity_wallet', updated);
});

bot.callbackQuery('vanity:payment:refresh', async (ctx) => {
  const userId = String(ctx.from.id);
  const user = await getUserState(userId);
  if (!user.vanityWallet.patternMode) {
    await ctx.answerCallbackQuery({ text: 'Choose Starts With or Ends With first.' });
    await renderScreen(ctx, 'vanity_wallet', user);
    return;
  }

  if (!user.vanityWallet.pattern) {
    await ctx.answerCallbackQuery({ text: 'Set the vanity pattern first.' });
    await renderScreen(ctx, 'vanity_wallet', user);
    return;
  }

  try {
    const updated = await createVanityWalletQuote(userId);
    await ctx.answerCallbackQuery({ text: 'New vanity quote ready.' });
    await renderScreen(ctx, 'vanity_wallet', updated);
  } catch (error) {
    const updated = await updateUserState(userId, (draft) => {
      draft.vanityWallet = normalizeVanityWalletState({
        ...draft.vanityWallet,
        lastError: String(error.message || error),
      });
      return draft;
    });
    await ctx.answerCallbackQuery({ text: 'Unable to build the quote right now.' });
    await renderScreen(ctx, 'vanity_wallet', updated);
  }
});

bot.callbackQuery('vanity:payment:check', async (ctx) => {
  const userId = String(ctx.from.id);
  const user = await getUserState(userId);

  if (!vanityWalletNeedsChecking(user)) {
    await ctx.answerCallbackQuery({ text: 'No pending vanity quote to check.' });
    await renderScreen(ctx, 'vanity_wallet', user);
    return;
  }

  try {
    const result = await checkVanityWalletPayment(userId);
    if (result.matched) {
      await ctx.answerCallbackQuery({ text: 'Payment found.' });
      await notifyVanityWalletPaymentMatched(userId, result.user);
      await renderScreen(ctx, 'vanity_wallet', result.user);
      return;
    }

    await ctx.answerCallbackQuery({ text: quoteExpired(result.user.vanityWallet.payment) ? 'Quote expired.' : 'Still waiting for payment.' });
    await renderScreen(ctx, 'vanity_wallet', result.user);
  } catch (error) {
    const updated = await updateUserState(userId, (draft) => {
      draft.vanityWallet = normalizeVanityWalletState({
        ...draft.vanityWallet,
        payment: {
          ...draft.vanityWallet.payment,
          lastCheckAt: new Date().toISOString(),
          lastError: String(error.message || error),
        },
        lastError: String(error.message || error),
      });
      return draft;
    });
    await ctx.answerCallbackQuery({ text: 'Payment check failed.' });
    await renderScreen(ctx, 'vanity_wallet', updated);
  }
});

bot.callbackQuery('vanity:key:toggle', async (ctx) => {
  const updated = await updateUserState(String(ctx.from.id), (draft) => {
    draft.vanityWallet = normalizeVanityWalletState({
      ...draft.vanityWallet,
      privateKeyVisible: !draft.vanityWallet.privateKeyVisible,
    });
    return draft;
  });
  await ctx.answerCallbackQuery({ text: updated.vanityWallet.privateKeyVisible ? 'Private key shown.' : 'Private key hidden.' });
  await renderScreen(ctx, 'vanity_wallet', updated);
});

bot.callbackQuery('vanity:add_to_trading', async (ctx) => {
  const userId = String(ctx.from.id);
  const user = await getUserState(userId);
  const state = normalizeVanityWalletState(user.vanityWallet);
  if (!state.generatedAddress || !state.generatedSecretKeyB64) {
    await ctx.answerCallbackQuery({ text: 'Generate the vanity wallet first.' });
    await renderScreen(ctx, 'vanity_wallet', user);
    return;
  }

  const updated = await updateUserState(userId, (draft) => {
    syncTradingDeskWalletsFromSource(draft, 'vanity_wallet', state.id, [
      buildSourceLinkedTradingWallet({
        sourceType: 'vanity_wallet',
        sourceId: state.id,
        label: `Vanity ${state.pattern || 'Wallet'}`,
        address: state.generatedAddress,
        secretKeyB64: state.generatedSecretKeyB64,
        secretKeyBase58: state.generatedSecretKeyBase58,
        imported: false,
      }),
    ], { selectFirst: true });
    return draft;
  });

  await ctx.answerCallbackQuery({ text: 'Vanity wallet added to Buy / Sell.' });
  await renderScreen(ctx, 'vanity_wallet', updated);
});

bot.on('message:document', async (ctx) => {
  const handledLaunchBuy = await processLaunchBuyLogoUpload(ctx);
  if (handledLaunchBuy) {
    return;
  }

  const handled = await processResizerUpload(ctx);
  if (!handled && String(ctx.message.document?.mime_type || '').startsWith('image/')) {
    await ctx.reply('Choose `Resizer` or `Launch + Buy` from the menu first before sending an image.', {
      parse_mode: 'Markdown',
    });
  }
});

bot.catch((error) => {
  if (isExpiredCallbackQueryError(error)) {
    return;
  }

  console.error('Telegram bot error:', error.error || error);
});

buySellText = function buySellText(user) {
  const tradingDesk = normalizeTradingDesk(user.tradingDesk);
  const activeWallet = getActiveTradingWallet(user);
  const selectedBundle = user.magicBundles?.find((bundle) => bundle.id === tradingDesk.selectedMagicBundleId) ?? null;
  return [
    '\u{1F4B1} *Buy / Sell Desk*',
    '',
    'A clean trading hub for wallet control, bundle selection, and trading-ready setup.',
    '',
    MENU_DIVIDER,
    '\u2728 *Desk Overview*',
    `- Active wallet: *${activeWallet ? activeWallet.label : 'Not set'}*`,
    `- Wallet address: ${activeWallet ? `\`${activeWallet.address}\`` : 'Add or generate a wallet first'}`,
    `- Wallet count: *${tradingDesk.wallets.length}*`,
    `- Selected bundle: *${selectedBundle ? (selectedBundle.tokenName || selectedBundle.id) : 'None selected'}*`,
    `- Token CA: ${tradingDesk.quickTradeMintAddress ? `\`${tradingDesk.quickTradeMintAddress}\`` : '*Not set*'}`,
    '',
    '\u{1F4CA} *What This Menu Is For*',
    '- Token CA setup for trading-ready wallets',
    '- Wallet import and wallet generation',
    '- Bundle selection for multi-wallet execution',
    '- Active-wallet management from one desk',
    `- Built-in trading routes use a *${formatBpsPercent(cfg.tradingHandlingFeeBps)}* handling fee per executed trade`,
    '',
    '\u26A0\uFE0F *Hot-Wallet Warning*',
    'Any wallet imported here should be treated like a live trading wallet with real funds.',
    ...(tradingDesk.awaitingField ? ['', promptForBuySellField(tradingDesk.awaitingField)] : []),
    ...(tradingDesk.lastError ? ['', `Last error: \`${tradingDesk.lastError}\``] : []),
  ].join('\n');
};

homeText = function homeText() {
  return [
    '\u{1F44B} *Welcome to Wizard Toolz*',
    '',
    'Premium Telegram tooling for reactions, trading, volume, burn systems, holder distribution, FOMO strategy, smart sell execution, bundle prep, wallet tracking, and launch sniping.',
    '',
    MENU_DIVIDER,
    '\u2728 *Simple Setup*',
    '1. Choose the feature you want to run.',
    '2. Fill in the wallet, mint, package, or target details.',
    '3. Fund the generated wallet when the flow requires it.',
    '4. Start, monitor, and manage everything from Telegram.',
    '',
    MENU_DIVIDER,
    '\u{1F680} *Supported Venues*',
    'Raydium â€¢ PumpSwap â€¢ Meteora â€¢ Pumpfun â€¢ Meteora DBC â€¢ Bags â€¢ LetsBonk â€¢ LaunchLab',
    '',
    '\u{1F4CA} Plans from 1 SOL â€¢ \u{1F6E1}\uFE0F Professional execution â€¢ \u{1F381} Free trial available',
    `\u{1F4B8} Built-in trading routes use *${formatBpsPercent(cfg.tradingHandlingFeeBps)}* per trade, lower than every competitor we track, and net platform profit is routed 50% to treasury, 25% to buyback + burn, and 25% to the SOL rewards vault`,
    `\u{1F91D} Need help? @${SUPPORT_USERNAME}`,
    '\u{1F4AC} Community chat: @wizardtoolz',
    '\u{1F514} Alerts channel: @wizardtoolz_alerts',
    `\u{1F6E0}\uFE0F Want custom tools built for you? Message support at @${SUPPORT_USERNAME}`,
    '',
    'Choose a service below to get started.',
  ].join('\n');
};

helpText = function helpText() {
  return buildHelpText({
    cfg,
    menuDivider: MENU_DIVIDER,
    supportUsername: SUPPORT_USERNAME,
  });
};

buySellText = function buySellText(user) {
  return buildBuySellText({
    user,
    cfg,
    menuDivider: MENU_DIVIDER,
    normalizeTradingDesk,
    getActiveTradingWallet,
  });
};

homeText = function homeText() {
  return buildHomeText({
    cfg,
    menuDivider: MENU_DIVIDER,
    supportUsername: SUPPORT_USERNAME,
  });
};

await ensureStore();
await bot.init();
await registerTelegramCommands(bot, TELEGRAM_MENU_COMMANDS);
startPaymentPollingLoop(cfg.solanaPaymentPollMs, pollPendingPayments);
if (cfg.telegramTransport === 'webhook') {
  console.log('Telegram bot starting in webhook mode...');
  await startWebhookTransport(bot, cfg);
} else {
  console.log('Telegram bot starting in polling mode...');
  await startPollingTransport(bot);
}

