import 'dotenv/config';
import fsSync from 'node:fs';
import fs from 'node:fs/promises';
import path from 'node:path';
import {
  ComputeBudgetProgram,
  Connection,
  Keypair,
  LAMPORTS_PER_SOL,
  PublicKey,
  SystemProgram,
  Transaction,
  TransactionMessage,
  VersionedTransaction,
} from '@solana/web3.js';
import {
  createAssociatedTokenAccountInstruction,
  createAssociatedTokenAccountIdempotentInstruction,
  createBurnCheckedInstruction,
  createTransferCheckedInstruction,
  getAssociatedTokenAddressSync,
  ASSOCIATED_TOKEN_PROGRAM_ID,
  TOKEN_2022_PROGRAM_ID,
  TOKEN_PROGRAM_ID,
} from '@solana/spl-token';
import BN from 'bn.js';
import {
  OnlinePumpSdk,
  PUMP_SDK,
  PUMP_PROGRAM_ID,
  bondingCurveMarketCap,
  canonicalPumpPoolPda,
  getBuyTokenAmountFromSolAmount,
  newBondingCurve,
} from '@pump-fun/pump-sdk';
import { OnlinePumpAmmSdk, PUMP_AMM_SDK } from '@pump-fun/pump-swap-sdk';
import { createManagedSolanaRpcPool, parseRpcUrlList } from './lib/solana/rpcPool.js';

const ROOT_DIR = path.resolve('.');
const DEFAULT_DATA_DIR = path.join(ROOT_DIR, 'data');
const RENDER_PERSISTENT_ROOT = '/var/data';
const configuredDataDir = process.env.BOT_DATA_DIR?.trim() || process.env.DATA_DIR?.trim() || null;
const autoPersistentDataDir = fsSync.existsSync(RENDER_PERSISTENT_ROOT)
  ? path.join(RENDER_PERSISTENT_ROOT, 'wizard-toolz')
  : null;
const DATA_DIR = configuredDataDir || autoPersistentDataDir || DEFAULT_DATA_DIR;
const STORE_PATH = process.env.TELEGRAM_STORE_PATH?.trim()
  || path.join(DATA_DIR, 'telegram-store.json');

if (process.env.RENDER && !process.env.TELEGRAM_STORE_PATH?.trim() && !process.env.BOT_DATA_DIR?.trim() && DATA_DIR === DEFAULT_DATA_DIR) {
  console.warn('[storage] Persistent bot state is not configured. Wallet-backed features can desync after restart/redeploy on Render. Set BOT_DATA_DIR to your persistent disk mount, such as /var/data/wizard-toolz.');
}

const WORKER_POLL_INTERVAL_MS = parsePositiveInt(process.env.WORKER_BOT_POLL_MS, 2_000);
const WORKER_ORDER_SCAN_INTERVAL_MS = parsePositiveInt(process.env.WORKER_ORDER_SCAN_INTERVAL_MS, 4_000);
const WORKER_SNIPER_SCAN_INTERVAL_MS = parsePositiveInt(process.env.WORKER_SNIPER_SCAN_INTERVAL_MS, WORKER_POLL_INTERVAL_MS);
const WORKER_TRADING_SCAN_INTERVAL_MS = parsePositiveInt(process.env.WORKER_TRADING_SCAN_INTERVAL_MS, 4_000);
const WORKER_LAUNCH_SCAN_INTERVAL_MS = parsePositiveInt(process.env.WORKER_LAUNCH_SCAN_INTERVAL_MS, WORKER_POLL_INTERVAL_MS);
const WORKER_AUTOMATION_SCAN_INTERVAL_MS = parsePositiveInt(process.env.WORKER_AUTOMATION_SCAN_INTERVAL_MS, 8_000);
const WORKER_STAKING_SCAN_INTERVAL_MS = parsePositiveInt(process.env.WORKER_STAKING_SCAN_INTERVAL_MS, 10_000);
const WORKER_DEV_SWAP_SCAN_INTERVAL_MS = parsePositiveInt(process.env.WORKER_DEV_SWAP_SCAN_INTERVAL_MS, 30_000);
const SPLIT_FEE_BUFFER_LAMPORTS = 20_000;
const PROCESSING_RETRY_AFTER_MS = 5 * 60_000;
const PLATFORM_SPLIT_BPS_DENOMINATOR = 10_000;
const DEV_WALLET_SWAP_RETRY_AFTER_MS = parsePositiveInt(
  process.env.DEV_WALLET_SWAP_RETRY_AFTER_MS,
  2 * 60_000,
);
const BURN_AGENT_RETRY_AFTER_MS = parsePositiveInt(
  process.env.BURN_AGENT_RETRY_AFTER_MS,
  2 * 60_000,
);
const PUMP_CREATOR_REWARD_INTERVAL_MS = parsePositiveInt(
  process.env.PUMP_CREATOR_REWARD_INTERVAL_MS,
  61_000,
);
const BURN_AGENT_INTERVAL_MS = parsePositiveInt(
  process.env.BURN_AGENT_INTERVAL_MS,
  PUMP_CREATOR_REWARD_INTERVAL_MS,
);
const BURN_AGENT_FEE_RESERVE_LAMPORTS = parseSolToLamports(
  process.env.BURN_AGENT_FEE_RESERVE_SOL || '0.01',
);
const BURN_AGENT_MINIMUM_CLAIM_LAMPORTS = parseSolToLamports(
  process.env.BURN_AGENT_MIN_SOL || process.env.PUMP_CREATOR_REWARD_MIN_SOL || '0',
);
const BURN_AGENT_BUY_SLIPPAGE_PERCENT = parsePositiveInt(
  process.env.BURN_AGENT_BUY_SLIPPAGE || process.env.PUMP_CREATOR_REWARD_BUY_SLIPPAGE,
  5,
);
const APPLE_BOOSTER_FEE_RESERVE_LAMPORTS = parseSolToLamports(
  process.env.APPLE_BOOSTER_FEE_RESERVE_SOL || '0.01',
);
const APPLE_BOOSTER_SWEEP_RESERVE_LAMPORTS = parsePositiveInt(
  process.env.APPLE_BOOSTER_SWEEP_RESERVE_LAMPORTS,
  5_000,
);
const MAGIC_SELL_MINIMUM_BUY_LAMPORTS = parseSolToLamports(
  process.env.MAGIC_SELL_MIN_BUY_SOL || '0.1',
);
const MAGIC_SELL_SELL_PERCENT = parsePositiveInt(process.env.MAGIC_SELL_SELL_PERCENT, 25);
const MAGIC_SELL_WORKER_GAS_RESERVE_LAMPORTS = parseSolToLamports(
  process.env.MAGIC_SELL_WORKER_GAS_RESERVE_SOL || '0.0025',
);
const MAGIC_SELL_SCAN_LIMIT = parsePositiveInt(process.env.MAGIC_SELL_SCAN_LIMIT, 20);
const FOMO_WORKER_GAS_RESERVE_LAMPORTS = parseSolToLamports(
  process.env.FOMO_WORKER_GAS_RESERVE_SOL || '0.003',
);
const FOMO_JITO_TIP_LAMPORTS = parsePositiveInt(
  process.env.FOMO_JITO_TIP_LAMPORTS,
  10_000,
);
const SNIPER_JITO_TIP_LAMPORTS = parsePositiveInt(
  process.env.SNIPER_JITO_TIP_LAMPORTS,
  10_000,
);
const SNIPER_GAS_RESERVE_LAMPORTS = parseSolToLamports(
  process.env.SNIPER_GAS_RESERVE_SOL || '0.01',
);
const SNIPER_FUNDING_TOLERANCE_LAMPORTS = parsePositiveInt(
  process.env.SNIPER_FUNDING_TOLERANCE_LAMPORTS,
  500_000,
);
const SNIPER_BUY_SLIPPAGE_PERCENT = parsePositiveInt(
  process.env.SNIPER_BUY_SLIPPAGE_PERCENT,
  10,
);
const SNIPER_COMPUTE_UNIT_LIMIT = parsePositiveInt(
  process.env.SNIPER_COMPUTE_UNIT_LIMIT,
  350_000,
);
const SNIPER_PRIORITY_FEE_MICROLAMPORTS = parsePositiveInt(
  process.env.SNIPER_PRIORITY_FEE_MICROLAMPORTS,
  300_000,
);
const SNIPER_LAUNCH_FAST_FETCH_RETRIES = parsePositiveInt(
  process.env.SNIPER_LAUNCH_FAST_FETCH_RETRIES,
  8,
);
const SNIPER_LAUNCH_FAST_FETCH_DELAY_MS = parsePositiveInt(
  process.env.SNIPER_LAUNCH_FAST_FETCH_DELAY_MS,
  40,
);
const SNIPER_LAUNCH_FETCH_RETRIES = parsePositiveInt(
  process.env.SNIPER_LAUNCH_FETCH_RETRIES,
  25,
);
const SNIPER_LAUNCH_FETCH_DELAY_MS = parsePositiveInt(
  process.env.SNIPER_LAUNCH_FETCH_DELAY_MS,
  120,
);
const SNIPER_WATCHER_HEARTBEAT_MS = parsePositiveInt(
  process.env.SNIPER_WATCHER_HEARTBEAT_MS,
  4_000,
);
const SNIPER_WATCHER_STALE_MS = parsePositiveInt(
  process.env.SNIPER_WATCHER_STALE_MS,
  12_000,
);
const SNIPER_BALANCE_REFRESH_MS = parsePositiveInt(
  process.env.SNIPER_BALANCE_REFRESH_MS,
  15_000,
);
const COMMUNITY_VISION_SCAN_INTERVAL_MS = parsePositiveInt(
  process.env.COMMUNITY_VISION_SCAN_INTERVAL_MS,
  300_000,
);
const WALLET_TRACKER_SCAN_INTERVAL_MS = parsePositiveInt(
  process.env.WALLET_TRACKER_SCAN_INTERVAL_MS,
  20_000,
);
const WALLET_TRACKER_SCAN_LIMIT = parsePositiveInt(
  process.env.WALLET_TRACKER_SCAN_LIMIT,
  20,
);
const FOMO_DEPOSIT_RESERVE_LAMPORTS = parsePositiveInt(
  process.env.FOMO_DEPOSIT_RESERVE_LAMPORTS,
  10_000,
);
const APPLE_BOOSTER_NETWORK_FEE_LAMPORTS = parsePositiveInt(
  process.env.APPLE_BOOSTER_NETWORK_FEE_LAMPORTS,
  60_000,
);
const SOL_PRICE_CACHE_MS = 60_000;
const JUPITER_API_KEY = process.env.JUPITER_API_KEY?.trim() || null;
const JUPITER_SWAP_API_BASE_URL = process.env.JUPITER_SWAP_API_BASE_URL?.trim() || 'https://api.jup.ag/swap/v2';
const HELIUS_SENDER_URL = process.env.HELIUS_SENDER_URL?.trim() || 'https://sender.helius-rpc.com/fast';
const HELIUS_SENDER_MIN_TIP_LAMPORTS = 200_000;
const HELIUS_SENDER_TIP_LAMPORTS = parsePositiveInt(process.env.HELIUS_SENDER_TIP_LAMPORTS, 0);
const HELIUS_SENDER_ENABLED = HELIUS_SENDER_TIP_LAMPORTS >= HELIUS_SENDER_MIN_TIP_LAMPORTS
  && String(process.env.HELIUS_SENDER_ENABLED || 'false').trim().toLowerCase() !== 'false';
const JITO_BLOCK_ENGINE_URL = process.env.JITO_BLOCK_ENGINE_URL?.trim() || 'https://mainnet.block-engine.jito.wtf';
const JITO_AUTH_KEY = process.env.JITO_AUTH_KEY?.trim() || null;
const BUNDLED_JITO_TIP_LAMPORTS = parsePositiveInt(process.env.BUNDLED_JITO_TIP_LAMPORTS, 10_000);
const BUNDLED_JITO_STATUS_TIMEOUT_MS = parsePositiveInt(process.env.BUNDLED_JITO_STATUS_TIMEOUT_MS, 45_000);
const BUNDLED_JITO_POLL_MS = parsePositiveInt(process.env.BUNDLED_JITO_POLL_MS, 2_000);
const JITO_TIP_ACCOUNTS_CACHE_MS = parsePositiveInt(process.env.JITO_TIP_ACCOUNTS_CACHE_MS, 60_000);
const JITO_TIP_PREFETCH_INTERVAL_MS = parsePositiveInt(
  process.env.JITO_TIP_PREFETCH_INTERVAL_MS,
  Math.max(15_000, Math.floor(JITO_TIP_ACCOUNTS_CACHE_MS / 2)),
);
const JITO_MAX_BUNDLE_TRANSACTIONS = 5;
const SPLITNOW_API_KEY = process.env.SPLITNOW_API_KEY?.trim() || null;
const SPLITNOW_API_BASE_URL = process.env.SPLITNOW_API_BASE_URL?.trim() || 'https://splitnow.io/api';
const MAGIC_BUNDLE_PLATFORM_FEE_BPS = parsePositiveInt(process.env.MAGIC_BUNDLE_PLATFORM_FEE_BPS, 0);
const MAGIC_BUNDLE_SPLITNOW_FEE_ESTIMATE_BPS = parsePositiveInt(process.env.MAGIC_BUNDLE_SPLITNOW_FEE_ESTIMATE_BPS, 100);
const MAGIC_BUNDLE_STEALTH_SETUP_FEE_LAMPORTS = parseSolToLamports(
  process.env.MAGIC_BUNDLE_STEALTH_SETUP_FEE_SOL || '0.05',
);
const TRADING_HANDLING_FEE_BPS = parsePositiveInt(process.env.TRADING_HANDLING_FEE_BPS, 50);
const MAGIC_BUNDLE_FEE_RESERVE_LAMPORTS = parseSolToLamports(process.env.MAGIC_BUNDLE_FEE_RESERVE_SOL || '0.005');
const STAKING_MIN_CLAIM_LAMPORTS = parseSolToLamports(process.env.STAKING_MIN_CLAIM_SOL || '0.01');
const STAKING_REWARDS_VAULT_RESERVE_LAMPORTS = parseSolToLamports(
  process.env.STAKING_REWARDS_VAULT_RESERVE_SOL || '0.001',
);
const STAKING_EARLY_WEIGHT_DAYS = parsePositiveInt(process.env.STAKING_EARLY_WEIGHT_DAYS, 7);
const STAKING_WEIGHT_TIERS = [
  { minDays: 0, bps: 2500, label: 'Starting' },
  { minDays: STAKING_EARLY_WEIGHT_DAYS, bps: 10000, label: 'Standard' },
  { minDays: 30, bps: 12500, label: 'Committed' },
  { minDays: 90, bps: 15000, label: 'Core' },
  { minDays: 180, bps: 20000, label: 'Diamond' },
];
const MAGIC_BUNDLE_POSITION_GAS_RESERVE_LAMPORTS = parseSolToLamports(
  process.env.MAGIC_BUNDLE_POSITION_GAS_RESERVE_SOL || '0.003',
);
const MAGIC_BUNDLE_DIP_BUY_SPEND_BPS = Math.min(
  10_000,
  Math.max(1, parsePositiveInt(process.env.MAGIC_BUNDLE_DIP_BUY_SPEND_BPS, 5000)),
);
const MAGIC_BUNDLE_DIP_COOLDOWN_MS = parsePositiveInt(
  process.env.MAGIC_BUNDLE_DIP_COOLDOWN_MS,
  60_000,
);
const TRADING_DESK_GAS_RESERVE_LAMPORTS = parseSolToLamports(
  process.env.TRADING_DESK_GAS_RESERVE_SOL || '0.005',
);
const TRADING_COPY_SCAN_LIMIT = parsePositiveInt(
  process.env.TRADING_COPY_SCAN_LIMIT,
  20,
);
const LAUNCH_BUY_NORMAL_SETUP_FEE_LAMPORTS = parseSolToLamports(
  process.env.LAUNCH_BUY_NORMAL_SETUP_FEE_SOL || '0.35',
);
const LAUNCH_BUY_MAGIC_SETUP_FEE_LAMPORTS = parseSolToLamports(
  process.env.LAUNCH_BUY_MAGIC_SETUP_FEE_SOL || '0.5',
);
const SNIPER_MAGIC_SETUP_FEE_LAMPORTS = parseSolToLamports(
  process.env.SNIPER_MAGIC_SETUP_FEE_SOL || '0.10',
);
const SNIPER_DEFAULT_WALLET_COUNT = parsePositiveInt(
  process.env.SNIPER_DEFAULT_WALLET_COUNT,
  1,
);
const SNIPER_MAX_WALLET_COUNT = parsePositiveInt(
  process.env.SNIPER_MAX_WALLET_COUNT,
  20,
);
const LAUNCH_BUY_DEFAULT_WALLET_COUNT = parsePositiveInt(
  process.env.LAUNCH_BUY_DEFAULT_WALLET_COUNT,
  5,
);
const LAUNCH_BUY_MAX_WALLET_COUNT = parsePositiveInt(
  process.env.LAUNCH_BUY_MAX_WALLET_COUNT,
  20,
);
const LAUNCH_BUY_DEFAULT_JITO_TIP_LAMPORTS = parseSolToLamports(
  process.env.LAUNCH_BUY_DEFAULT_JITO_TIP_SOL || '0.01',
);
const LAUNCH_BUY_LAUNCH_OVERHEAD_LAMPORTS = parseSolToLamports(
  process.env.LAUNCH_BUY_LAUNCH_OVERHEAD_SOL || '0.012',
);
const LAUNCH_BUY_BUYER_RESERVE_LAMPORTS = parseSolToLamports(
  process.env.LAUNCH_BUY_BUYER_RESERVE_SOL || '0.006',
);
const LAUNCH_BUY_FUNDING_TOLERANCE_LAMPORTS = parsePositiveInt(
  process.env.LAUNCH_BUY_FUNDING_TOLERANCE_LAMPORTS,
  500_000,
);
const LAUNCH_BUY_MAX_ATOMIC_BUYERS_PER_TX = parsePositiveInt(
  process.env.LAUNCH_BUY_MAX_ATOMIC_BUYERS_PER_TX,
  1,
);
const LAUNCH_BUY_ASSETS_DIR = path.join(DATA_DIR, 'launch-assets');
const MAGIC_BUNDLE_DEV_SELL_SCAN_LIMIT = parsePositiveInt(
  process.env.MAGIC_BUNDLE_DEV_SELL_SCAN_LIMIT,
  20,
);
const SOL_MINT_ADDRESS = 'So11111111111111111111111111111111111111112';
const BASE58_ALPHABET = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';
const PUMP_CREATE_DISCRIMINATOR = Uint8Array.from([24, 30, 200, 40, 5, 28, 7, 119]);
const PUMP_CREATE_V2_DISCRIMINATOR = Uint8Array.from([214, 144, 76, 236, 95, 139, 49, 180]);
let solUsdRateCache = null;
let solUsdRateCachedAt = 0;
const sniperWizardSubscriptions = new Map();
let sniperWizardHeartbeatTimer = null;
const knownLaunchMintCache = new Map();
let jitoTipAccountsCache = {
  accounts: null,
  cachedAt: 0,
};
let jitoTipAccountsInFlight = null;
const workerScanLastRunAt = new Map();

const ORGANIC_VOLUME_PACKAGES = [
  { key: '3k', label: '3K', treasuryCutSol: '0.05' },
  { key: '5k', label: '5K', treasuryCutSol: '0.20' },
  { key: '10k', label: '10K', treasuryCutSol: '0.30' },
  { key: '20k', label: '20K', treasuryCutSol: '0.40' },
  { key: '30k', label: '30K', treasuryCutSol: '0.60' },
  { key: '50k', label: '50K', treasuryCutSol: '1.00' },
  { key: '75k', label: '75K', treasuryCutSol: '1.25' },
  { key: '100k', label: '100K', treasuryCutSol: '2.00' },
  { key: '200k', label: '200K', treasuryCutSol: '4.00' },
  { key: '500k', label: '500K', treasuryCutSol: '10.00' },
];
const BUNDLED_VOLUME_PACKAGES = [
  { key: '20k', label: '20K', treasuryCutSol: '0.40' },
  { key: '30k', label: '30K', treasuryCutSol: '0.60' },
  { key: '50k', label: '50K', treasuryCutSol: '1.00' },
  { key: '100k', label: '100K', treasuryCutSol: '2.00' },
  { key: '200k', label: '200K', treasuryCutSol: '4.00' },
  { key: '500k', label: '500K', treasuryCutSol: '10.00' },
];

function parsePositiveInt(value, fallback) {
  const parsed = Number.parseInt(String(value || '').trim(), 10);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function parseSolToLamports(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return 0;
  }

  return Math.round(parsed * LAMPORTS_PER_SOL);
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
      throw new Error('Base58 value contains invalid characters.');
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

function formatSolAmountFromLamports(lamports) {
  if (!Number.isInteger(lamports)) {
    return '0';
  }

  return (lamports / LAMPORTS_PER_SOL).toFixed(9);
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

async function fetchSolUsdSpotRate() {
  const apiBaseUrl = (process.env.SOLANA_PRICE_API_BASE_URL?.trim() || 'https://api.coinbase.com').replace(/\/+$/, '');
  const response = await fetch(`${apiBaseUrl}/v2/prices/SOL-USD/spot`, {
    headers: {
      'User-Agent': 'steel-tester-worker/1.0',
    },
  });

  if (!response.ok) {
    throw new Error(`SOL/USD price lookup failed with status ${response.status}.`);
  }

  const payload = await response.json();
  const amount = Number.parseFloat(payload?.data?.amount);
  if (!Number.isFinite(amount) || amount <= 0) {
    throw new Error('SOL/USD price lookup returned an invalid amount.');
  }

  return amount;
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
    lastError: null,
    stopRequested: false,
  };
}

function normalizeAppleBoosterWorkerWallet(worker = {}) {
  return {
    address: typeof worker.address === 'string' ? worker.address : null,
    secretKeyB64: typeof worker.secretKeyB64 === 'string' ? worker.secretKeyB64 : null,
    secretKeyBase58: typeof worker.secretKeyBase58 === 'string' ? worker.secretKeyBase58 : null,
    currentLamports: Number.isInteger(worker.currentLamports) ? worker.currentLamports : 0,
    currentSol: typeof worker.currentSol === 'string' ? worker.currentSol : '0',
    status: typeof worker.status === 'string' ? worker.status : 'idle',
    pendingSellAmount: typeof worker.pendingSellAmount === 'string' ? worker.pendingSellAmount : null,
    nextActionAt: typeof worker.nextActionAt === 'string' ? worker.nextActionAt : null,
    lastActionAt: typeof worker.lastActionAt === 'string' ? worker.lastActionAt : null,
    lastBuyInputLamports: Number.isInteger(worker.lastBuyInputLamports) ? worker.lastBuyInputLamports : null,
    lastBuyOutputAmount: typeof worker.lastBuyOutputAmount === 'string' ? worker.lastBuyOutputAmount : null,
    lastBuySignature: typeof worker.lastBuySignature === 'string' ? worker.lastBuySignature : null,
    lastSellInputAmount: typeof worker.lastSellInputAmount === 'string' ? worker.lastSellInputAmount : null,
    lastSellOutputLamports: typeof worker.lastSellOutputLamports === 'string' ? worker.lastSellOutputLamports : null,
    lastSellSignature: typeof worker.lastSellSignature === 'string' ? worker.lastSellSignature : null,
    lastError: typeof worker.lastError === 'string' ? worker.lastError : null,
  };
}

function createAppleBoosterId() {
  return `ab_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

function normalizeOrganicOrder(order = {}) {
  return {
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
    treasurySplitStatus: typeof order.treasurySplitStatus === 'string' ? order.treasurySplitStatus : null,
    treasurySplitAttemptedAt: typeof order.treasurySplitAttemptedAt === 'string' ? order.treasurySplitAttemptedAt : null,
    treasurySplitProcessedAt: typeof order.treasurySplitProcessedAt === 'string' ? order.treasurySplitProcessedAt : null,
    treasurySplitSignature: typeof order.treasurySplitSignature === 'string' ? order.treasurySplitSignature : null,
    treasurySplitError: typeof order.treasurySplitError === 'string' ? order.treasurySplitError : null,
    treasurySplitFailedAt: typeof order.treasurySplitFailedAt === 'string' ? order.treasurySplitFailedAt : null,
    treasurySplitLamports: Number.isInteger(order.treasurySplitLamports) ? order.treasurySplitLamports : null,
    treasurySplitTreasuryLamports: Number.isInteger(order.treasurySplitTreasuryLamports)
      ? order.treasurySplitTreasuryLamports
      : null,
    treasurySplitDevLamports: Number.isInteger(order.treasurySplitDevLamports) ? order.treasurySplitDevLamports : null,
    treasurySplitBalanceBeforeLamports: Number.isInteger(order.treasurySplitBalanceBeforeLamports)
      ? order.treasurySplitBalanceBeforeLamports
      : null,
    treasurySplitBalanceAfterLamports: Number.isInteger(order.treasurySplitBalanceAfterLamports)
      ? order.treasurySplitBalanceAfterLamports
      : null,
    appleBooster: {
      ...createDefaultAppleBoosterState(),
      ...(order.appleBooster ?? {}),
      walletCount: Number.isInteger(order.appleBooster?.walletCount)
        ? order.appleBooster.walletCount
        : null,
      workerWallets: Array.isArray(order.appleBooster?.workerWallets)
        ? order.appleBooster.workerWallets.map((worker) => normalizeAppleBoosterWorkerWallet(worker))
        : [],
      totalManagedLamports: Number.isInteger(order.appleBooster?.totalManagedLamports)
        ? order.appleBooster.totalManagedLamports
        : null,
      marketPhase: typeof order.appleBooster?.marketPhase === 'string' ? order.appleBooster.marketPhase : null,
      marketCapLamports: typeof order.appleBooster?.marketCapLamports === 'string'
        ? order.appleBooster.marketCapLamports
        : null,
      marketCapSol: typeof order.appleBooster?.marketCapSol === 'string' ? order.appleBooster.marketCapSol : null,
      lpFeeBps: Number.isInteger(order.appleBooster?.lpFeeBps) ? order.appleBooster.lpFeeBps : null,
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
      totalBuyCount: Number.isInteger(order.appleBooster?.totalBuyCount) ? order.appleBooster.totalBuyCount : 0,
      totalSellCount: Number.isInteger(order.appleBooster?.totalSellCount) ? order.appleBooster.totalSellCount : 0,
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
      cycleCount: Number.isInteger(order.appleBooster?.cycleCount) ? order.appleBooster.cycleCount : 0,
      stopRequested: Boolean(order.appleBooster?.stopRequested),
    },
  };
}

function decodeOrderWallet(secretKeyB64) {
  const bytes = Buffer.from(secretKeyB64, 'base64');
  return Keypair.fromSecretKey(Uint8Array.from(bytes));
}

function matchesDiscriminator(data, discriminator) {
  if (typeof data !== 'string' || !data) {
    return false;
  }

  try {
    const decoded = base58Decode(data);
    if (decoded.length < discriminator.length) {
      return false;
    }

    return discriminator.every((value, index) => decoded[index] === value);
  } catch {
    return false;
  }
}

function isPumpCreateInstruction(instruction) {
  const programId = instruction?.programId?.toBase58?.() ?? instruction?.programId ?? null;
  if (programId !== PUMP_PROGRAM_ID.toBase58()) {
    return false;
  }

  return matchesDiscriminator(instruction?.data, PUMP_CREATE_DISCRIMINATOR)
    || matchesDiscriminator(instruction?.data, PUMP_CREATE_V2_DISCRIMINATOR);
}

function getInstructionAccountStrings(instruction) {
  if (!Array.isArray(instruction?.accounts)) {
    return [];
  }

  return instruction.accounts.map((account) => (
    typeof account === 'string'
      ? account
      : (account?.pubkey?.toBase58?.() ?? account?.pubkey ?? account?.toBase58?.() ?? String(account))
  ));
}

function extractPumpLaunchMintAddress(transaction) {
  const instructions = transaction?.transaction?.message?.instructions ?? [];
  for (const instruction of instructions) {
    if (!isPumpCreateInstruction(instruction)) {
      continue;
    }

    const accounts = getInstructionAccountStrings(instruction);
    const mintAddress = accounts[0];
    if (mintAddress) {
      return mintAddress;
    }
  }

  return null;
}

function extractPumpLaunchMintAddressFromCompiledTransaction(transaction) {
  const message = transaction?.transaction?.message;
  const staticAccountKeys = Array.isArray(message?.staticAccountKeys)
    ? message.staticAccountKeys.map((key) => publicKeyishToBase58(key)).filter(Boolean)
    : [];
  const instructions = Array.isArray(message?.compiledInstructions)
    ? message.compiledInstructions
    : [];

  for (const instruction of instructions) {
    const programId = staticAccountKeys[instruction?.programIdIndex];
    if (programId !== PUMP_PROGRAM_ID.toBase58()) {
      continue;
    }
    if (!matchesDiscriminator(instruction?.data, PUMP_CREATE_DISCRIMINATOR)
      && !matchesDiscriminator(instruction?.data, PUMP_CREATE_V2_DISCRIMINATOR)) {
      continue;
    }
    const mintIndex = Array.isArray(instruction?.accountKeyIndexes) ? instruction.accountKeyIndexes[0] : null;
    const mintAddress = Number.isInteger(mintIndex) ? staticAccountKeys[mintIndex] : null;
    if (mintAddress) {
      return mintAddress;
    }
  }

  return null;
}

function rememberKnownLaunchMint(signature, mintAddress) {
  if (!signature || !mintAddress) {
    return;
  }
  knownLaunchMintCache.set(signature, mintAddress);
  if (knownLaunchMintCache.size > 200) {
    const firstKey = knownLaunchMintCache.keys().next().value;
    if (firstKey) {
      knownLaunchMintCache.delete(firstKey);
    }
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function parseCsvList(...sources) {
  const values = sources
    .flatMap((value) => (typeof value === 'string' ? value.split(',') : Array.isArray(value) ? value : []))
    .map((value) => String(value || '').trim())
    .filter(Boolean);

  return values.filter((value, index, array) => array.indexOf(value) === index);
}

function toWebSocketUrl(url) {
  if (!url) {
    return null;
  }

  try {
    const parsed = new URL(url);
    if (parsed.protocol === 'https:') {
      parsed.protocol = 'wss:';
    } else if (parsed.protocol === 'http:') {
      parsed.protocol = 'ws:';
    }
    return parsed.toString();
  } catch {
    return null;
  }
}

function buildWatcherEndpointPairs(rpcUrls, explicitWsUrls = []) {
  const httpUrls = parseRpcUrlList(rpcUrls);
  const wsUrls = parseCsvList(explicitWsUrls).map((value) => toWebSocketUrl(value)).filter(Boolean);

  if (wsUrls.length === httpUrls.length && wsUrls.length > 0) {
    return httpUrls.map((httpUrl, index) => ({
      httpUrl,
      wsUrl: wsUrls[index],
    }));
  }

  return httpUrls.map((httpUrl) => ({
    httpUrl,
    wsUrl: toWebSocketUrl(httpUrl),
  })).filter((item) => item.wsUrl);
}

function createTimingTracker() {
  const startedAt = Date.now();
  const marks = {};
  return {
    mark(label) {
      marks[label] = Date.now() - startedAt;
    },
    get elapsedMs() {
      return Date.now() - startedAt;
    },
    snapshot(extra = {}) {
      return {
        ...marks,
        totalMs: Date.now() - startedAt,
        ...extra,
      };
    },
  };
}

function decodeWalletFromEnv(secretKeyB64, secretKeyJson) {
  if (secretKeyB64) {
    const bytes = Buffer.from(secretKeyB64, 'base64');
    return Keypair.fromSecretKey(Uint8Array.from(bytes));
  }

  if (!secretKeyJson) {
    throw new Error('A wallet secret key is required.');
  }

  const parsed = JSON.parse(secretKeyJson);
  if (!Array.isArray(parsed) || parsed.length !== 64) {
    throw new Error('Wallet secret key JSON must be a 64-byte array.');
  }

  return Keypair.fromSecretKey(Uint8Array.from(parsed));
}

function buildDevWalletSigner(devWalletAddress) {
  const secretKeyB64 = process.env.DEV_WALLET_SECRET_KEY_B64?.trim() || null;
  const secretKeyJson = process.env.DEV_WALLET_SECRET_KEY?.trim() || null;

  if (!devWalletAddress || devWalletAddress.includes('PLACEHOLDER')) {
    return {
      signer: null,
      reason: 'missing DEV_WALLET_ADDRESS',
    };
  }

  if (!secretKeyB64 && !secretKeyJson) {
    return {
      signer: null,
      reason: 'missing DEV_WALLET_SECRET_KEY_B64 or DEV_WALLET_SECRET_KEY',
    };
  }

  try {
    const signer = decodeWalletFromEnv(secretKeyB64, secretKeyJson);
    if (signer.publicKey.toBase58() !== devWalletAddress) {
      throw new Error('Dev wallet secret key does not match DEV_WALLET_ADDRESS.');
    }

    return {
      signer,
      reason: null,
    };
  } catch (error) {
    return {
      signer: null,
      reason: String(error.message || error),
    };
  }
}

function buildVolumeTrialConfig() {
  const walletAddress = process.env.VOLUME_TRIAL_WALLET_ADDRESS?.trim() || null;
  const secretKeyB64 = process.env.VOLUME_TRIAL_WALLET_SECRET_KEY_B64?.trim() || null;
  const secretKeyJson = process.env.VOLUME_TRIAL_WALLET_SECRET_KEY?.trim() || null;
  const minLamports = parseSolToLamports(process.env.VOLUME_TRIAL_MIN_SOL || '0.001');
  const maxLamports = parseSolToLamports(process.env.VOLUME_TRIAL_MAX_SOL || '0.003');
  const minIntervalSeconds = parsePositiveInt(process.env.VOLUME_TRIAL_MIN_INTERVAL_SECONDS, 8);
  const maxIntervalSeconds = parsePositiveInt(process.env.VOLUME_TRIAL_MAX_INTERVAL_SECONDS, 18);
  const tradeGoal = parsePositiveInt(process.env.VOLUME_TRIAL_TRADE_GOAL, 5);
  const sourceReserveLamports = parseSolToLamports(process.env.VOLUME_TRIAL_SOURCE_RESERVE_SOL || '0.02');

  if (!walletAddress || !secretKeyB64 && !secretKeyJson) {
    return {
      enabled: false,
      reason: 'missing VOLUME_TRIAL_WALLET_ADDRESS or VOLUME_TRIAL_WALLET_SECRET_KEY_B64/VOLUME_TRIAL_WALLET_SECRET_KEY',
      signer: null,
      walletAddress,
      minLamports: Math.min(minLamports, maxLamports),
      maxLamports: Math.max(minLamports, maxLamports),
      minIntervalSeconds: Math.min(minIntervalSeconds, maxIntervalSeconds),
      maxIntervalSeconds: Math.max(minIntervalSeconds, maxIntervalSeconds),
      tradeGoal,
      sourceReserveLamports,
    };
  }

  try {
    const signer = decodeWalletFromEnv(secretKeyB64, secretKeyJson);
    if (signer.publicKey.toBase58() !== walletAddress) {
      throw new Error('Volume trial secret key does not match VOLUME_TRIAL_WALLET_ADDRESS.');
    }

    return {
      enabled: true,
      reason: null,
      signer,
      walletAddress,
      minLamports: Math.min(minLamports, maxLamports),
      maxLamports: Math.max(minLamports, maxLamports),
      minIntervalSeconds: Math.min(minIntervalSeconds, maxIntervalSeconds),
      maxIntervalSeconds: Math.max(minIntervalSeconds, maxIntervalSeconds),
      tradeGoal,
      sourceReserveLamports,
    };
  } catch (error) {
    return {
      enabled: false,
      reason: String(error.message || error),
      signer: null,
      walletAddress,
      minLamports: Math.min(minLamports, maxLamports),
      maxLamports: Math.max(minLamports, maxLamports),
      minIntervalSeconds: Math.min(minIntervalSeconds, maxIntervalSeconds),
      maxIntervalSeconds: Math.max(minIntervalSeconds, maxIntervalSeconds),
      tradeGoal,
      sourceReserveLamports,
    };
  }
}

function buildDevWalletSwapConfig(devWalletAddress, devWalletSigner) {
  const targetMint = process.env.DEV_WALLET_SWAP_TARGET_MINT?.trim() || null;
  const apiKey = process.env.JUPITER_API_KEY?.trim() || null;
  const apiBaseUrl = process.env.JUPITER_SWAP_API_BASE_URL?.trim() || 'https://api.jup.ag/swap/v2';
  const reserveLamports = parseSolToLamports(process.env.DEV_WALLET_SWAP_RESERVE_SOL || '0.02');
  const minimumLamports = parseSolToLamports(process.env.DEV_WALLET_SWAP_MIN_SOL || '0.01');

  const missing = [];
  if (!targetMint) {
    missing.push('DEV_WALLET_SWAP_TARGET_MINT');
  }
  if (!apiKey) {
    missing.push('JUPITER_API_KEY');
  }
  if (!devWalletAddress || devWalletAddress.includes('PLACEHOLDER')) {
    missing.push('DEV_WALLET_ADDRESS');
  }
  if (!devWalletSigner) {
    missing.push('DEV_WALLET_SECRET_KEY_B64 or DEV_WALLET_SECRET_KEY');
  }

  if (missing.length > 0) {
    return {
      enabled: false,
      reason: `missing ${missing.join(', ')}`,
      targetMint,
      apiKey,
      apiBaseUrl,
      reserveLamports,
      minimumLamports,
      signer: null,
    };
  }

  try {
    const mint = new PublicKey(targetMint);
    return {
      enabled: true,
      reason: null,
      targetMint: mint.toBase58(),
      apiKey,
      apiBaseUrl: apiBaseUrl.replace(/\/+$/, ''),
      reserveLamports,
      minimumLamports,
      signer: devWalletSigner,
    };
  } catch (error) {
    return {
      enabled: false,
      reason: String(error.message || error),
      targetMint,
      apiKey,
      apiBaseUrl,
      reserveLamports,
      minimumLamports,
      signer: devWalletSigner,
    };
  }
}

function isConfiguredAddress(value) {
  return Boolean(value && !String(value).includes('PLACEHOLDER'));
}

const LIVE_WIZARD_TOKEN_MINT = 'HqvX73Yi99DbUr4NTR5a4HycrCT7waMDePruW5XQpump';

function buildPlatformRevenueConfig(devWalletSigner) {
  const tokenMint = process.env.WIZARD_TOKEN_MINT?.trim()
    || LIVE_WIZARD_TOKEN_MINT
    || process.env.PUMP_CREATOR_REWARD_MINT?.trim()
    || process.env.DEV_WALLET_SWAP_TARGET_MINT?.trim()
    || null;
  const rewardsVaultAddress = process.env.WIZARD_REWARDS_VAULT_ADDRESS?.trim() || null;
  const treasuryBps = parsePositiveInt(process.env.PLATFORM_TREASURY_BPS, 5000);
  const burnBps = parsePositiveInt(process.env.PLATFORM_BURN_BPS, 2500);
  const rewardsBps = parsePositiveInt(process.env.PLATFORM_REWARDS_BPS, 2500);
  const buySlippagePercent = parsePositiveInt(
    process.env.PLATFORM_BUYBACK_SLIPPAGE_PERCENT || process.env.PUMP_CREATOR_REWARD_BUY_SLIPPAGE,
    5,
  );
  const missing = [];
  if (!tokenMint) {
    missing.push('WIZARD_TOKEN_MINT');
  }
  if (!isConfiguredAddress(rewardsVaultAddress)) {
    missing.push('WIZARD_REWARDS_VAULT_ADDRESS');
  }
  if (!devWalletSigner) {
    missing.push('DEV_WALLET_SECRET_KEY_B64 or DEV_WALLET_SECRET_KEY');
  }

  if (treasuryBps + burnBps + rewardsBps !== PLATFORM_SPLIT_BPS_DENOMINATOR) {
    return {
      enabled: false,
      reason: 'PLATFORM_TREASURY_BPS + PLATFORM_BURN_BPS + PLATFORM_REWARDS_BPS must equal 10000',
      tokenMint,
      rewardsVaultAddress,
      treasuryBps,
      burnBps,
      rewardsBps,
      buySlippagePercent,
      signer: devWalletSigner,
    };
  }

  if (missing.length > 0) {
    return {
      enabled: false,
      reason: `missing ${missing.join(', ')}`,
      tokenMint,
      rewardsVaultAddress,
      treasuryBps,
      burnBps,
      rewardsBps,
      buySlippagePercent,
      signer: devWalletSigner,
    };
  }

  try {
    return {
      enabled: true,
      reason: null,
      tokenMint: new PublicKey(tokenMint).toBase58(),
      rewardsVaultAddress: new PublicKey(rewardsVaultAddress).toBase58(),
      treasuryBps,
      burnBps,
      rewardsBps,
      buySlippagePercent,
      signer: devWalletSigner,
    };
  } catch (error) {
    return {
      enabled: false,
      reason: String(error.message || error),
      tokenMint,
      rewardsVaultAddress,
      treasuryBps,
      burnBps,
      rewardsBps,
      buySlippagePercent,
      signer: devWalletSigner,
    };
  }
}

function buildPumpCreatorRewardsConfig(devWalletSigner, platformRevenue) {
  const targetMint = platformRevenue?.tokenMint
    || process.env.PUMP_CREATOR_REWARD_MINT?.trim()
    || process.env.DEV_WALLET_SWAP_TARGET_MINT?.trim()
    || null;
  const minimumClaimLamports = parseSolToLamports(process.env.PUMP_CREATOR_REWARD_MIN_SOL || '0');
  const buySlippagePercent = parsePositiveInt(
    process.env.PUMP_CREATOR_REWARD_BUY_SLIPPAGE,
    platformRevenue?.buySlippagePercent ?? 5,
  );

  const missing = [];
  if (!targetMint) {
    missing.push('PUMP_CREATOR_REWARD_MINT or DEV_WALLET_SWAP_TARGET_MINT');
  }
  if (!devWalletSigner) {
    missing.push('DEV_WALLET_SECRET_KEY_B64 or DEV_WALLET_SECRET_KEY');
  }
  if (!platformRevenue?.enabled) {
    missing.push(platformRevenue?.reason || 'platform revenue routing config');
  }

  if (missing.length > 0) {
    return {
      enabled: false,
      reason: `missing ${missing.join(', ')}`,
      mint: targetMint,
      intervalMs: PUMP_CREATOR_REWARD_INTERVAL_MS,
      minimumClaimLamports,
      buySlippagePercent,
      signer: devWalletSigner,
    };
  }

  try {
    const mint = new PublicKey(targetMint);
    return {
      enabled: true,
      reason: null,
      mint: mint.toBase58(),
      intervalMs: PUMP_CREATOR_REWARD_INTERVAL_MS,
      minimumClaimLamports,
      buySlippagePercent,
      signer: devWalletSigner,
    };
  } catch (error) {
    return {
      enabled: false,
      reason: String(error.message || error),
      mint: targetMint,
      intervalMs: PUMP_CREATOR_REWARD_INTERVAL_MS,
      minimumClaimLamports,
      buySlippagePercent,
      signer: devWalletSigner,
    };
  }
}

function getConfig() {
  const missing = [];
  const telegramBotToken = process.env.TELEGRAM_BOT_TOKEN?.trim() || null;
  const rpcUrls = parseRpcUrlList(
    process.env.SOLANA_RPC_URLS?.trim() || '',
    process.env.SOLANA_RPC_URL?.trim() || '',
  );
  const wsUrls = buildWatcherEndpointPairs(
    rpcUrls,
    process.env.SOLANA_WS_URLS?.trim() || '',
  );
  const treasuryWalletAddress = process.env.TREASURY_WALLET_ADDRESS?.trim() || null;
  const devWalletAddress = process.env.DEV_WALLET_ADDRESS?.trim() || null;
  const communityVisionApiUrl = process.env.COMMUNITY_VISION_API_URL?.trim() || null;
  const communityVisionBearerToken = process.env.COMMUNITY_VISION_API_BEARER_TOKEN?.trim() || null;

  if (missing.length > 0) {
    throw new Error(`Missing required environment variables: ${missing.join(', ')}`);
  }

  const { signer: devWalletSigner, reason: devWalletSignerReason } = buildDevWalletSigner(devWalletAddress);
  const platformRevenue = buildPlatformRevenueConfig(devWalletSigner);

  return {
    telegramBotToken,
    rpcUrl: rpcUrls[0],
    rpcUrls,
    wsEndpoints: wsUrls,
    rpcRotationCooldownMs: parsePositiveInt(process.env.SOLANA_RPC_ROTATION_COOLDOWN_MS, 8_000),
    rpcTimeoutMs: parsePositiveInt(process.env.SOLANA_RPC_TIMEOUT_MS, 4_000),
    rpcMaxRetries: parsePositiveInt(process.env.SOLANA_RPC_MAX_RETRIES, 4),
    treasuryWalletAddress,
    devWalletAddress,
    devWalletSigner,
    devWalletSignerReason,
    platformRevenue,
    devWalletSwap: buildDevWalletSwapConfig(devWalletAddress, devWalletSigner),
    pumpCreatorRewards: buildPumpCreatorRewardsConfig(devWalletSigner, platformRevenue),
    volumeTrial: buildVolumeTrialConfig(),
    communityVision: {
      enabled: Boolean(communityVisionApiUrl),
      apiUrl: communityVisionApiUrl,
      bearerToken: communityVisionBearerToken,
      intervalMs: COMMUNITY_VISION_SCAN_INTERVAL_MS,
    },
  };
}

const cfg = getConfig();
const solanaRpcPool = createManagedSolanaRpcPool({
  urls: cfg.rpcUrls,
  commitment: 'confirmed',
  label: 'worker-rpc',
  cooldownMs: cfg.rpcRotationCooldownMs,
  timeoutMs: cfg.rpcTimeoutMs,
  maxRetries: cfg.rpcMaxRetries,
});
const connection = solanaRpcPool.connection;
const pumpOnlineSdk = new OnlinePumpSdk(connection);
const pumpAmmOnlineSdk = new OnlinePumpAmmSdk(connection);
let cycleInFlight = false;

function createDefaultWorkerState() {
  return {
    devWalletSwap: {
      enabled: cfg.devWalletSwap.enabled,
      status: cfg.devWalletSwap.enabled ? 'idle' : 'disabled',
      targetMint: cfg.devWalletSwap.targetMint ?? null,
      reserveLamports: cfg.devWalletSwap.reserveLamports,
      minimumLamports: cfg.devWalletSwap.minimumLamports,
      lastCheckedAt: null,
      lastBalanceLamports: null,
      lastBalanceSol: null,
      lastSwappableLamports: null,
      lastAttemptedAt: null,
      lastProcessedAt: null,
      lastInputLamports: null,
      lastQuotedOutputAmount: null,
      lastOutputAmount: null,
      pendingBurnAmount: null,
      lastBurnAttemptedAt: null,
      lastBurnProcessedAt: null,
      lastBurnAmount: null,
      lastBurnSignature: null,
      lastBurnError: null,
      lastRouter: null,
      lastMode: null,
      lastSignature: null,
      lastError: cfg.devWalletSwap.enabled ? null : cfg.devWalletSwap.reason,
    },
    pumpCreatorRewards: {
      enabled: cfg.pumpCreatorRewards.enabled,
      status: cfg.pumpCreatorRewards.enabled ? 'idle' : 'disabled',
      mint: cfg.pumpCreatorRewards.mint ?? null,
      intervalMs: cfg.pumpCreatorRewards.intervalMs,
      minimumClaimLamports: cfg.pumpCreatorRewards.minimumClaimLamports,
      buySlippagePercent: cfg.pumpCreatorRewards.buySlippagePercent,
      lastCheckedAt: null,
      lastVaultLamports: null,
      lastClaimAttemptedAt: null,
      lastClaimedAt: null,
      lastClaimedLamports: null,
      lastClaimSignature: null,
      pendingTreasuryLamports: null,
      pendingBurnBuybackLamports: null,
      pendingRewardsVaultLamports: null,
      pendingBurnAmount: null,
      lastTreasuryAttemptedAt: null,
      lastTreasuryProcessedAt: null,
      lastTreasurySignature: null,
      lastBuybackAttemptedAt: null,
      lastBuybackProcessedAt: null,
      lastBuybackSignature: null,
      lastBuybackMode: null,
      lastBuybackTokenProgram: null,
      lastBuybackRawAmount: null,
      lastRewardsVaultAttemptedAt: null,
      lastRewardsVaultProcessedAt: null,
      lastRewardsVaultSignature: null,
      lastRewardsVaultRawAmount: null,
      lastBurnAttemptedAt: null,
      lastBurnProcessedAt: null,
      lastBurnSignature: null,
      lastBurnAmount: null,
      lastBurnError: null,
      lastError: cfg.pumpCreatorRewards.enabled ? null : cfg.pumpCreatorRewards.reason,
    },
    stakingRewards: {
      enabled: cfg.platformRevenue.enabled,
      status: cfg.platformRevenue.enabled ? 'idle' : 'disabled',
      mint: cfg.platformRevenue.tokenMint ?? null,
      rewardsVaultAddress: cfg.platformRevenue.rewardsVaultAddress ?? null,
      reserveLamports: STAKING_REWARDS_VAULT_RESERVE_LAMPORTS,
      claimThresholdLamports: STAKING_MIN_CLAIM_LAMPORTS,
      earlyWeightDays: STAKING_EARLY_WEIGHT_DAYS,
      pendingUndistributedLamports: 0,
      lastCheckedAt: null,
      lastObservedVaultLamports: null,
      lastObservedVaultSol: null,
      lastDistributedAt: null,
      lastDistributedLamports: null,
      totalDistributedLamports: 0,
      totalTrackedRaw: '0',
      totalTrackedWallets: 0,
      lastError: cfg.platformRevenue.enabled ? null : cfg.platformRevenue.reason,
    },
  };
}

function createDefaultBurnAgentRuntime() {
  return {
    status: 'idle',
    intervalMs: BURN_AGENT_INTERVAL_MS,
    minimumClaimLamports: BURN_AGENT_MINIMUM_CLAIM_LAMPORTS,
    buySlippagePercent: BURN_AGENT_BUY_SLIPPAGE_PERCENT,
    feeReserveLamports: BURN_AGENT_FEE_RESERVE_LAMPORTS,
    lastCheckedAt: null,
    lastVaultLamports: null,
    lastClaimAttemptedAt: null,
    lastClaimedAt: null,
    lastClaimedLamports: null,
    lastClaimSignature: null,
    pendingTreasuryLamports: null,
    pendingBuybackLamports: null,
    pendingBurnAmount: null,
    lastTreasuryAttemptedAt: null,
    lastTreasuryProcessedAt: null,
    lastTreasurySignature: null,
    lastBuybackAttemptedAt: null,
    lastBuybackProcessedAt: null,
    lastBuybackSignature: null,
    lastBuybackMode: null,
    lastBuybackTokenProgram: null,
    lastBuybackRawAmount: null,
    lastBurnAttemptedAt: null,
    lastBurnProcessedAt: null,
    lastBurnSignature: null,
    lastBurnAmount: null,
    lastBurnError: null,
    totalClaimChecks: 0,
    totalClaimCount: 0,
    totalClaimedLamports: 0,
    totalTreasuryTransferCount: 0,
    totalTreasuryLamportsSent: 0,
    totalBuybackCount: 0,
    totalBuybackLamports: 0,
    totalBurnCount: 0,
    totalBurnedRawAmount: '0',
    lastError: null,
  };
}

function normalizeBurnAgentRuntime(runtime = {}) {
  return {
    ...createDefaultBurnAgentRuntime(),
    ...(runtime ?? {}),
    intervalMs: BURN_AGENT_INTERVAL_MS,
    minimumClaimLamports: BURN_AGENT_MINIMUM_CLAIM_LAMPORTS,
    buySlippagePercent: BURN_AGENT_BUY_SLIPPAGE_PERCENT,
    feeReserveLamports: BURN_AGENT_FEE_RESERVE_LAMPORTS,
    totalClaimChecks: Number.isInteger(runtime?.totalClaimChecks) ? runtime.totalClaimChecks : 0,
    totalClaimCount: Number.isInteger(runtime?.totalClaimCount) ? runtime.totalClaimCount : 0,
    totalClaimedLamports: Number.isInteger(runtime?.totalClaimedLamports) ? runtime.totalClaimedLamports : 0,
    totalTreasuryTransferCount: Number.isInteger(runtime?.totalTreasuryTransferCount)
      ? runtime.totalTreasuryTransferCount
      : 0,
    totalTreasuryLamportsSent: Number.isInteger(runtime?.totalTreasuryLamportsSent)
      ? runtime.totalTreasuryLamportsSent
      : 0,
    totalBuybackCount: Number.isInteger(runtime?.totalBuybackCount) ? runtime.totalBuybackCount : 0,
    totalBuybackLamports: Number.isInteger(runtime?.totalBuybackLamports) ? runtime.totalBuybackLamports : 0,
    totalBurnCount: Number.isInteger(runtime?.totalBurnCount) ? runtime.totalBurnCount : 0,
    totalBurnedRawAmount: typeof runtime?.totalBurnedRawAmount === 'string' ? runtime.totalBurnedRawAmount : '0',
  };
}

function normalizeBurnAgentRecord(agent = {}) {
  return {
    ...(agent ?? {}),
    id: typeof agent?.id === 'string' ? agent.id : null,
    speed: typeof agent?.speed === 'string' ? agent.speed : null,
    walletMode: typeof agent?.walletMode === 'string' ? agent.walletMode : null,
    walletAddress: typeof agent?.walletAddress === 'string' ? agent.walletAddress : null,
    walletSecretKeyB64: typeof agent?.walletSecretKeyB64 === 'string' ? agent.walletSecretKeyB64 : null,
    mintAddress: typeof agent?.mintAddress === 'string' ? agent.mintAddress : null,
    treasuryAddress: typeof agent?.treasuryAddress === 'string' ? agent.treasuryAddress : null,
    burnPercent: Number.isInteger(agent?.burnPercent) ? agent.burnPercent : null,
    treasuryPercent: Number.isInteger(agent?.treasuryPercent) ? agent.treasuryPercent : null,
    automationEnabled: Boolean(agent?.automationEnabled),
    archivedAt: typeof agent?.archivedAt === 'string' ? agent.archivedAt : null,
    runtime: normalizeBurnAgentRuntime(agent?.runtime),
  };
}

function hasMeaningfulBurnAgentRecord(agent = {}) {
  return Boolean(
    agent
    && (
      agent.speed
      || agent.walletAddress
      || agent.walletSecretKeyB64
      || agent.mintAddress
      || agent.treasuryAddress
      || Object.keys(agent.runtime || {}).length > 0
    )
  );
}

function hasMeaningfulOrganicOrderRecord(order = {}) {
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
        || (Array.isArray(order.appleBooster.workerWallets) && order.appleBooster.workerWallets.length > 0)
      ))
    )
  );
}

function createDefaultHolderBoosterRecord() {
  return {
    id: `hb_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
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

function normalizeHolderBoosterRecord(order = {}) {
  return {
    ...createDefaultHolderBoosterRecord(),
    ...(order ?? {}),
    id: typeof order.id === 'string' && order.id ? order.id : createDefaultHolderBoosterRecord().id,
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

function hasMeaningfulHolderBoosterRecord(order = {}) {
  return Boolean(
    order
    && (
      order.mintAddress
      || order.holderCount
      || order.walletAddress
      || order.walletSecretKeyB64
      || order.createdAt
      || order.completedAt
      || (Array.isArray(order.childWallets) && order.childWallets.length > 0)
    )
  );
}

function createDefaultMagicSellRecord() {
  return {
    id: `ms_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    tokenName: null,
    mintAddress: null,
    targetMarketCapUsd: null,
    whitelistWallets: [],
    walletAddress: null,
    walletSecretKeyB64: null,
    walletSecretKeyBase58: null,
    tokenDecimals: null,
    tokenProgram: null,
    sellerWalletCount: 0,
    sellerWallets: [],
    currentLamports: 0,
    currentSol: '0',
    currentTokenAmountRaw: '0',
    currentTokenAmountDisplay: '0',
    totalManagedLamports: 0,
    currentMarketCapUsd: null,
    currentMarketCapSol: null,
    marketPhase: null,
    sellPercent: MAGIC_SELL_SELL_PERCENT,
    minimumBuyLamports: MAGIC_SELL_MINIMUM_BUY_LAMPORTS,
    recommendedGasLamports: 0,
    automationEnabled: false,
    status: 'setup',
    stats: {},
    lastSeenSignature: null,
    lastProcessedBuyAt: null,
    lastBalanceCheckAt: null,
    lastError: null,
    archivedAt: null,
  };
}

function normalizeMagicSellSellerWalletRecord(wallet = {}) {
  return {
    address: typeof wallet?.address === 'string' ? wallet.address : null,
    secretKeyB64: typeof wallet?.secretKeyB64 === 'string' ? wallet.secretKeyB64 : null,
    secretKeyBase58: typeof wallet?.secretKeyBase58 === 'string' ? wallet.secretKeyBase58 : null,
    currentLamports: Number.isInteger(wallet?.currentLamports) ? wallet.currentLamports : 0,
    currentSol: typeof wallet?.currentSol === 'string' ? wallet.currentSol : '0',
    currentTokenAmountRaw: typeof wallet?.currentTokenAmountRaw === 'string' ? wallet.currentTokenAmountRaw : '0',
    currentTokenAmountDisplay: typeof wallet?.currentTokenAmountDisplay === 'string'
      ? wallet.currentTokenAmountDisplay
      : '0',
    sellCount: Number.isInteger(wallet?.sellCount) ? wallet.sellCount : 0,
    lastUsedAt: typeof wallet?.lastUsedAt === 'string' ? wallet.lastUsedAt : null,
    status: typeof wallet?.status === 'string' ? wallet.status : 'idle',
    lastSellSignature: typeof wallet?.lastSellSignature === 'string' ? wallet.lastSellSignature : null,
    lastError: typeof wallet?.lastError === 'string' ? wallet.lastError : null,
  };
}

function normalizeMagicSellStats(stats = {}) {
  return {
    triggerCount: Number.isInteger(stats?.triggerCount) ? stats.triggerCount : 0,
    sellCount: Number.isInteger(stats?.sellCount) ? stats.sellCount : 0,
    totalObservedBuyLamports: Number.isInteger(stats?.totalObservedBuyLamports) ? stats.totalObservedBuyLamports : 0,
    totalTargetSellLamports: Number.isInteger(stats?.totalTargetSellLamports) ? stats.totalTargetSellLamports : 0,
    totalSoldTokenRaw: typeof stats?.totalSoldTokenRaw === 'string' ? stats.totalSoldTokenRaw : '0',
    lastTriggerSignature: typeof stats?.lastTriggerSignature === 'string' ? stats.lastTriggerSignature : null,
    lastSellSignature: typeof stats?.lastSellSignature === 'string' ? stats.lastSellSignature : null,
  };
}

function normalizeMagicSellRecord(order = {}) {
  const defaults = createDefaultMagicSellRecord();
  return {
    ...defaults,
    ...(order ?? {}),
    id: typeof order?.id === 'string' && order.id ? order.id : defaults.id,
    tokenName: typeof order?.tokenName === 'string' ? order.tokenName : null,
    mintAddress: typeof order?.mintAddress === 'string' ? order.mintAddress : null,
    targetMarketCapUsd: Number.isFinite(order?.targetMarketCapUsd) ? Number(order.targetMarketCapUsd) : null,
    whitelistWallets: Array.isArray(order?.whitelistWallets)
      ? order.whitelistWallets.filter((item) => typeof item === 'string' && item.trim())
      : [],
    walletAddress: typeof order?.walletAddress === 'string' ? order.walletAddress : null,
    walletSecretKeyB64: typeof order?.walletSecretKeyB64 === 'string' ? order.walletSecretKeyB64 : null,
    walletSecretKeyBase58: typeof order?.walletSecretKeyBase58 === 'string' ? order.walletSecretKeyBase58 : null,
    tokenDecimals: Number.isInteger(order?.tokenDecimals) ? order.tokenDecimals : null,
    tokenProgram: typeof order?.tokenProgram === 'string' ? order.tokenProgram : null,
    sellerWalletCount: Number.isInteger(order?.sellerWalletCount) ? order.sellerWalletCount : 0,
    sellerWallets: Array.isArray(order?.sellerWallets)
      ? order.sellerWallets.map((wallet) => normalizeMagicSellSellerWalletRecord(wallet))
      : [],
    currentLamports: Number.isInteger(order?.currentLamports) ? order.currentLamports : 0,
    currentSol: typeof order?.currentSol === 'string' ? order.currentSol : '0',
    currentTokenAmountRaw: typeof order?.currentTokenAmountRaw === 'string' ? order.currentTokenAmountRaw : '0',
    currentTokenAmountDisplay: typeof order?.currentTokenAmountDisplay === 'string' ? order.currentTokenAmountDisplay : '0',
    totalManagedLamports: Number.isInteger(order?.totalManagedLamports) ? order.totalManagedLamports : 0,
    currentMarketCapUsd: Number.isFinite(order?.currentMarketCapUsd) ? Number(order.currentMarketCapUsd) : null,
    currentMarketCapSol: typeof order?.currentMarketCapSol === 'string' ? order.currentMarketCapSol : null,
    marketPhase: typeof order?.marketPhase === 'string' ? order.marketPhase : null,
    sellPercent: Number.isInteger(order?.sellPercent) ? order.sellPercent : MAGIC_SELL_SELL_PERCENT,
    minimumBuyLamports: Number.isInteger(order?.minimumBuyLamports) ? order.minimumBuyLamports : MAGIC_SELL_MINIMUM_BUY_LAMPORTS,
    recommendedGasLamports: Number.isInteger(order?.recommendedGasLamports) ? order.recommendedGasLamports : 0,
    automationEnabled: Boolean(order?.automationEnabled),
    status: typeof order?.status === 'string' ? order.status : 'setup',
    stats: normalizeMagicSellStats(order?.stats),
    lastSeenSignature: typeof order?.lastSeenSignature === 'string' ? order.lastSeenSignature : null,
    lastProcessedBuyAt: typeof order?.lastProcessedBuyAt === 'string' ? order.lastProcessedBuyAt : null,
    lastBalanceCheckAt: typeof order?.lastBalanceCheckAt === 'string' ? order.lastBalanceCheckAt : null,
    lastError: typeof order?.lastError === 'string' ? order.lastError : null,
    archivedAt: typeof order?.archivedAt === 'string' ? order.archivedAt : null,
  };
}

function hasMeaningfulMagicSellRecord(order = {}) {
  return Boolean(
    order
    && (
      order.mintAddress
      || order.walletAddress
      || order.walletSecretKeyB64
      || order.targetMarketCapUsd
      || (Array.isArray(order.sellerWallets) && order.sellerWallets.length > 0)
    )
  );
}

function createDefaultMagicBundleRecord() {
  return {
    id: `mb_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    bundleMode: 'stealth',
    tokenName: null,
    mintAddress: null,
    tokenDecimals: null,
    tokenProgram: null,
    walletCount: 0,
    splitWallets: [],
    walletAddress: null,
    walletSecretKeyB64: null,
    walletSecretKeyBase58: null,
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
    platformFeeBps: parsePositiveInt(process.env.MAGIC_BUNDLE_PLATFORM_FEE_BPS, 0),
    splitNowFeeEstimateBps: parsePositiveInt(process.env.MAGIC_BUNDLE_SPLITNOW_FEE_ESTIMATE_BPS, 100),
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
    archivedAt: null,
  };
}

function normalizeMagicBundleWalletRecord(wallet = {}) {
  return {
    address: typeof wallet?.address === 'string' ? wallet.address : null,
    secretKeyB64: typeof wallet?.secretKeyB64 === 'string' ? wallet.secretKeyB64 : null,
    secretKeyBase58: typeof wallet?.secretKeyBase58 === 'string' ? wallet.secretKeyBase58 : null,
    currentLamports: Number.isInteger(wallet?.currentLamports) ? wallet.currentLamports : 0,
    currentSol: typeof wallet?.currentSol === 'string' ? wallet.currentSol : '0',
    currentTokenAmountRaw: typeof wallet?.currentTokenAmountRaw === 'string' ? wallet.currentTokenAmountRaw : '0',
    currentTokenAmountDisplay: typeof wallet?.currentTokenAmountDisplay === 'string'
      ? wallet.currentTokenAmountDisplay
      : '0',
    currentPositionValueLamports: Number.isInteger(wallet?.currentPositionValueLamports)
      ? wallet.currentPositionValueLamports
      : 0,
    costBasisLamports: Number.isInteger(wallet?.costBasisLamports) ? wallet.costBasisLamports : null,
    highestValueLamports: Number.isInteger(wallet?.highestValueLamports) ? wallet.highestValueLamports : null,
    buyDipCount: Number.isInteger(wallet?.buyDipCount) ? wallet.buyDipCount : 0,
    lastActionAt: typeof wallet?.lastActionAt === 'string' ? wallet.lastActionAt : null,
    lastBuySignature: typeof wallet?.lastBuySignature === 'string' ? wallet.lastBuySignature : null,
    lastSellSignature: typeof wallet?.lastSellSignature === 'string' ? wallet.lastSellSignature : null,
    lastTriggerReason: typeof wallet?.lastTriggerReason === 'string' ? wallet.lastTriggerReason : null,
    status: typeof wallet?.status === 'string' ? wallet.status : 'idle',
    lastError: typeof wallet?.lastError === 'string' ? wallet.lastError : null,
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

function normalizeMagicBundleRecord(order = {}) {
  const defaults = createDefaultMagicBundleRecord();
  const bundleMode = order?.bundleMode === 'standard' ? 'standard' : 'stealth';
  const platformFeeBps = bundleMode === 'standard'
    ? 0
    : 0;
  const splitNowFeeEstimateBps = bundleMode === 'standard'
    ? 0
    : (Number.isInteger(order?.splitNowFeeEstimateBps) ? order.splitNowFeeEstimateBps : defaults.splitNowFeeEstimateBps);
  return {
    ...defaults,
    ...(order ?? {}),
    id: typeof order?.id === 'string' && order.id ? order.id : defaults.id,
    bundleMode,
    tokenName: typeof order?.tokenName === 'string' ? order.tokenName : null,
    mintAddress: typeof order?.mintAddress === 'string' ? order.mintAddress : null,
    tokenDecimals: Number.isInteger(order?.tokenDecimals) ? order.tokenDecimals : null,
    tokenProgram: typeof order?.tokenProgram === 'string' ? order.tokenProgram : null,
    walletCount: Number.isInteger(order?.walletCount) ? order.walletCount : 0,
    splitWallets: Array.isArray(order?.splitWallets)
      ? order.splitWallets.map((wallet) => normalizeMagicBundleWalletRecord(wallet))
      : [],
    walletAddress: typeof order?.walletAddress === 'string' ? order.walletAddress : null,
    walletSecretKeyB64: typeof order?.walletSecretKeyB64 === 'string' ? order.walletSecretKeyB64 : null,
    walletSecretKeyBase58: typeof order?.walletSecretKeyBase58 === 'string' ? order.walletSecretKeyBase58 : null,
    currentLamports: Number.isInteger(order?.currentLamports) ? order.currentLamports : 0,
    currentSol: typeof order?.currentSol === 'string' ? order.currentSol : '0',
    totalManagedLamports: Number.isInteger(order?.totalManagedLamports) ? order.totalManagedLamports : 0,
    currentTokenAmountRaw: typeof order?.currentTokenAmountRaw === 'string' ? order.currentTokenAmountRaw : '0',
    currentTokenAmountDisplay: typeof order?.currentTokenAmountDisplay === 'string'
      ? order.currentTokenAmountDisplay
      : '0',
    currentPositionValueLamports: Number.isInteger(order?.currentPositionValueLamports)
      ? order.currentPositionValueLamports
      : 0,
    creatorAddress: typeof order?.creatorAddress === 'string' ? order.creatorAddress : null,
    stopLossPercent: Number.isFinite(order?.stopLossPercent) ? Number(order.stopLossPercent) : null,
    takeProfitPercent: Number.isFinite(order?.takeProfitPercent) ? Number(order.takeProfitPercent) : null,
    trailingStopLossPercent: Number.isFinite(order?.trailingStopLossPercent) ? Number(order.trailingStopLossPercent) : null,
    buyDipPercent: Number.isFinite(order?.buyDipPercent) ? Number(order.buyDipPercent) : null,
    sellOnDevSell: Boolean(order?.sellOnDevSell),
    automationEnabled: Boolean(order?.automationEnabled),
    platformFeeBps,
    splitNowFeeEstimateBps,
    estimatedPlatformFeeLamports: Number.isInteger(order?.estimatedPlatformFeeLamports) ? order.estimatedPlatformFeeLamports : 0,
    estimatedSplitNowFeeLamports: Number.isInteger(order?.estimatedSplitNowFeeLamports) ? order.estimatedSplitNowFeeLamports : 0,
    estimatedNetSplitLamports: Number.isInteger(order?.estimatedNetSplitLamports) ? order.estimatedNetSplitLamports : 0,
    splitnowOrderId: typeof order?.splitnowOrderId === 'string' ? order.splitnowOrderId : null,
    splitnowQuoteId: typeof order?.splitnowQuoteId === 'string' ? order.splitnowQuoteId : null,
    splitnowDepositAddress: typeof order?.splitnowDepositAddress === 'string' ? order.splitnowDepositAddress : null,
    splitnowDepositAmountSol: typeof order?.splitnowDepositAmountSol === 'string' ? order.splitnowDepositAmountSol : null,
    splitnowStatus: typeof order?.splitnowStatus === 'string' ? order.splitnowStatus : null,
    splitCompletedAt: typeof order?.splitCompletedAt === 'string' ? order.splitCompletedAt : null,
    stats: normalizeMagicBundleStats(order?.stats),
    lastCreatorSeenSignature: typeof order?.lastCreatorSeenSignature === 'string'
      ? order.lastCreatorSeenSignature
      : null,
    lastActionAt: typeof order?.lastActionAt === 'string' ? order.lastActionAt : null,
    lastTriggerReason: typeof order?.lastTriggerReason === 'string' ? order.lastTriggerReason : null,
    lastBalanceCheckAt: typeof order?.lastBalanceCheckAt === 'string' ? order.lastBalanceCheckAt : null,
    lastError: typeof order?.lastError === 'string' ? order.lastError : null,
    archivedAt: typeof order?.archivedAt === 'string' ? order.archivedAt : null,
  };
}

function hasMeaningfulMagicBundleRecord(order = {}) {
  return Boolean(
    order
    && (
      order.mintAddress
      || order.walletAddress
      || order.walletSecretKeyB64
      || Number.isInteger(order.walletCount) && order.walletCount > 0
      || Array.isArray(order.splitWallets) && order.splitWallets.length > 0
      || typeof order.splitnowOrderId === 'string' && order.splitnowOrderId
      || order.automationEnabled
    )
  );
}

function createDefaultFomoBoosterRecord() {
  return {
    id: `fomo_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    tokenName: null,
    mintAddress: null,
    walletAddress: null,
    walletSecretKeyB64: null,
    walletSecretKeyBase58: null,
    tokenDecimals: null,
    tokenProgram: null,
    walletCount: 0,
    workerWallets: [],
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
    recommendedGasLamports: 0,
    automationEnabled: false,
    status: 'setup',
    stats: {},
    lastBundleId: null,
    lastBundleAt: null,
    nextActionAt: null,
    lastBalanceCheckAt: null,
    lastError: null,
    createdAt: null,
    updatedAt: null,
  };
}

function normalizeFomoWorkerWalletRecord(wallet = {}) {
  return {
    address: typeof wallet?.address === 'string' ? wallet.address : null,
    secretKeyB64: typeof wallet?.secretKeyB64 === 'string' ? wallet.secretKeyB64 : null,
    secretKeyBase58: typeof wallet?.secretKeyBase58 === 'string' ? wallet.secretKeyBase58 : null,
    currentLamports: Number.isInteger(wallet?.currentLamports) ? wallet.currentLamports : 0,
    currentSol: typeof wallet?.currentSol === 'string' ? wallet.currentSol : '0',
    currentTokenAmountRaw: typeof wallet?.currentTokenAmountRaw === 'string' ? wallet.currentTokenAmountRaw : '0',
    currentTokenAmountDisplay: typeof wallet?.currentTokenAmountDisplay === 'string'
      ? wallet.currentTokenAmountDisplay
      : '0',
    buyCount: Number.isInteger(wallet?.buyCount) ? wallet.buyCount : 0,
    sellCount: Number.isInteger(wallet?.sellCount) ? wallet.sellCount : 0,
    lastUsedAt: typeof wallet?.lastUsedAt === 'string' ? wallet.lastUsedAt : null,
    status: typeof wallet?.status === 'string' ? wallet.status : 'idle',
    lastBuySignature: typeof wallet?.lastBuySignature === 'string' ? wallet.lastBuySignature : null,
    lastSellSignature: typeof wallet?.lastSellSignature === 'string' ? wallet.lastSellSignature : null,
    lastError: typeof wallet?.lastError === 'string' ? wallet.lastError : null,
  };
}

function normalizeFomoBoosterStats(stats = {}) {
  return {
    bundleCount: Number.isInteger(stats?.bundleCount) ? stats.bundleCount : 0,
    buyCount: Number.isInteger(stats?.buyCount) ? stats.buyCount : 0,
    sellCount: Number.isInteger(stats?.sellCount) ? stats.sellCount : 0,
    totalBuyLamports: Number.isInteger(stats?.totalBuyLamports) ? stats.totalBuyLamports : 0,
    totalSellLamports: Number.isInteger(stats?.totalSellLamports) ? stats.totalSellLamports : 0,
    lastBuySignature: typeof stats?.lastBuySignature === 'string' ? stats.lastBuySignature : null,
    lastSellSignature: typeof stats?.lastSellSignature === 'string' ? stats.lastSellSignature : null,
    lastSeedSignature: typeof stats?.lastSeedSignature === 'string' ? stats.lastSeedSignature : null,
  };
}

function normalizeFomoBoosterRecord(order = {}) {
  const defaults = createDefaultFomoBoosterRecord();
  return {
    ...defaults,
    ...(order ?? {}),
    id: typeof order?.id === 'string' && order.id ? order.id : defaults.id,
    tokenName: typeof order?.tokenName === 'string' ? order.tokenName : null,
    mintAddress: typeof order?.mintAddress === 'string' ? order.mintAddress : null,
    walletAddress: typeof order?.walletAddress === 'string' ? order.walletAddress : null,
    walletSecretKeyB64: typeof order?.walletSecretKeyB64 === 'string' ? order.walletSecretKeyB64 : null,
    walletSecretKeyBase58: typeof order?.walletSecretKeyBase58 === 'string' ? order.walletSecretKeyBase58 : null,
    tokenDecimals: Number.isInteger(order?.tokenDecimals) ? order.tokenDecimals : null,
    tokenProgram: typeof order?.tokenProgram === 'string' ? order.tokenProgram : null,
    walletCount: Number.isInteger(order?.walletCount) ? order.walletCount : 0,
    workerWallets: Array.isArray(order?.workerWallets)
      ? order.workerWallets.map((wallet) => normalizeFomoWorkerWalletRecord(wallet))
      : [],
    minBuySol: typeof order?.minBuySol === 'string' ? order.minBuySol : null,
    maxBuySol: typeof order?.maxBuySol === 'string' ? order.maxBuySol : null,
    minBuyLamports: Number.isInteger(order?.minBuyLamports) ? order.minBuyLamports : null,
    maxBuyLamports: Number.isInteger(order?.maxBuyLamports) ? order.maxBuyLamports : null,
    minIntervalSeconds: Number.isInteger(order?.minIntervalSeconds) ? order.minIntervalSeconds : null,
    maxIntervalSeconds: Number.isInteger(order?.maxIntervalSeconds) ? order.maxIntervalSeconds : null,
    currentLamports: Number.isInteger(order?.currentLamports) ? order.currentLamports : 0,
    currentSol: typeof order?.currentSol === 'string' ? order.currentSol : '0',
    totalManagedLamports: Number.isInteger(order?.totalManagedLamports) ? order.totalManagedLamports : 0,
    currentTokenAmountRaw: typeof order?.currentTokenAmountRaw === 'string' ? order.currentTokenAmountRaw : '0',
    currentTokenAmountDisplay: typeof order?.currentTokenAmountDisplay === 'string'
      ? order.currentTokenAmountDisplay
      : '0',
    currentMarketCapUsd: Number.isFinite(order?.currentMarketCapUsd) ? Number(order.currentMarketCapUsd) : null,
    currentMarketCapSol: typeof order?.currentMarketCapSol === 'string' ? order.currentMarketCapSol : null,
    marketPhase: typeof order?.marketPhase === 'string' ? order.marketPhase : null,
    recommendedGasLamports: Number.isInteger(order?.recommendedGasLamports) ? order.recommendedGasLamports : 0,
    automationEnabled: Boolean(order?.automationEnabled),
    status: typeof order?.status === 'string' ? order.status : 'setup',
    stats: normalizeFomoBoosterStats(order?.stats),
    lastBundleId: typeof order?.lastBundleId === 'string' ? order.lastBundleId : null,
    lastBundleAt: typeof order?.lastBundleAt === 'string' ? order.lastBundleAt : null,
    nextActionAt: typeof order?.nextActionAt === 'string' ? order.nextActionAt : null,
    lastBalanceCheckAt: typeof order?.lastBalanceCheckAt === 'string' ? order.lastBalanceCheckAt : null,
    lastError: typeof order?.lastError === 'string' ? order.lastError : null,
    createdAt: typeof order?.createdAt === 'string' ? order.createdAt : null,
    updatedAt: typeof order?.updatedAt === 'string' ? order.updatedAt : null,
  };
}

function hasMeaningfulFomoBoosterRecord(order = {}) {
  return Boolean(
    order
    && (
      order.mintAddress
      || order.automationEnabled
      || Number.isInteger(order.minBuyLamports)
      || Number.isInteger(order.maxBuyLamports)
      || Number.isInteger(order.currentLamports) && order.currentLamports > 0
      || typeof order.currentTokenAmountRaw === 'string' && order.currentTokenAmountRaw !== '0'
      || normalizeFomoBoosterStats(order.stats).bundleCount > 0
    )
  );
}

function createDefaultSniperWizardRecord() {
  return {
    id: `sw_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    sniperMode: null,
    targetWalletAddress: null,
    walletCount: SNIPER_DEFAULT_WALLET_COUNT,
    workerWallets: [],
    walletAddress: null,
    walletSecretKeyB64: null,
    walletSecretKeyBase58: null,
    snipePercent: 50,
    currentLamports: 0,
    currentSol: '0',
    totalManagedLamports: 0,
    estimatedPlatformFeeLamports: 0,
    estimatedSplitNowFeeLamports: 0,
    estimatedNetSplitLamports: 0,
    automationEnabled: false,
    status: 'setup',
    stats: {},
    routingOrderId: null,
    routingQuoteId: null,
    routingDepositAddress: null,
    routingStatus: null,
    routingCompletedAt: null,
    setupFeePaidAt: null,
    lastDetectedLaunchSignature: null,
    lastDetectedMintAddress: null,
    lastSnipeSignature: null,
    lastBalanceCheckAt: null,
    lastError: null,
    createdAt: null,
    updatedAt: null,
  };
}

function normalizeSniperWizardStats(stats = {}) {
  return {
    launchCount: Number.isInteger(stats?.launchCount) ? stats.launchCount : 0,
    snipeAttemptCount: Number.isInteger(stats?.snipeAttemptCount) ? stats.snipeAttemptCount : 0,
    snipeSuccessCount: Number.isInteger(stats?.snipeSuccessCount) ? stats.snipeSuccessCount : 0,
    totalSpentLamports: Number.isInteger(stats?.totalSpentLamports) ? stats.totalSpentLamports : 0,
    totalFeeLamports: Number.isInteger(stats?.totalFeeLamports) ? stats.totalFeeLamports : 0,
    avgTotalLatencyMs: Number.isInteger(stats?.avgTotalLatencyMs) ? stats.avgTotalLatencyMs : 0,
    bestTotalLatencyMs: Number.isInteger(stats?.bestTotalLatencyMs) ? stats.bestTotalLatencyMs : 0,
    lastDetectToBuildMs: Number.isInteger(stats?.lastDetectToBuildMs) ? stats.lastDetectToBuildMs : 0,
    lastSubmitMs: Number.isInteger(stats?.lastSubmitMs) ? stats.lastSubmitMs : 0,
    lastConfirmMs: Number.isInteger(stats?.lastConfirmMs) ? stats.lastConfirmMs : 0,
    lastTotalLatencyMs: Number.isInteger(stats?.lastTotalLatencyMs) ? stats.lastTotalLatencyMs : 0,
    lastRoute: typeof stats?.lastRoute === 'string' ? stats.lastRoute : null,
    lastLaunchSignature: typeof stats?.lastLaunchSignature === 'string' ? stats.lastLaunchSignature : null,
    lastMintAddress: typeof stats?.lastMintAddress === 'string' ? stats.lastMintAddress : null,
    lastSnipeSignature: typeof stats?.lastSnipeSignature === 'string' ? stats.lastSnipeSignature : null,
  };
}

function normalizeSniperWizardRecord(order = {}) {
  const defaults = createDefaultSniperWizardRecord();
  const walletCount = Number.isInteger(order?.walletCount)
    ? Math.min(SNIPER_MAX_WALLET_COUNT, Math.max(1, order.walletCount))
    : defaults.walletCount;
  const workerWallets = Array.isArray(order?.workerWallets)
    ? order.workerWallets.map((wallet) => normalizeLaunchBuyBuyerWalletRecord(wallet)).slice(0, walletCount)
    : [];
  return {
    ...defaults,
    ...(order ?? {}),
    id: typeof order?.id === 'string' && order.id ? order.id : defaults.id,
    sniperMode: order?.sniperMode === 'magic' ? 'magic' : (order?.sniperMode === 'standard' ? 'standard' : null),
    targetWalletAddress: typeof order?.targetWalletAddress === 'string' ? order.targetWalletAddress : null,
    walletCount,
    workerWallets,
    walletAddress: typeof order?.walletAddress === 'string' ? order.walletAddress : null,
    walletSecretKeyB64: typeof order?.walletSecretKeyB64 === 'string' ? order.walletSecretKeyB64 : null,
    walletSecretKeyBase58: typeof order?.walletSecretKeyBase58 === 'string' ? order.walletSecretKeyBase58 : null,
    snipePercent: Number.isInteger(order?.snipePercent) ? order.snipePercent : defaults.snipePercent,
    currentLamports: Number.isInteger(order?.currentLamports) ? order.currentLamports : 0,
    currentSol: typeof order?.currentSol === 'string' ? order.currentSol : '0',
    totalManagedLamports: Number.isInteger(order?.totalManagedLamports) ? order.totalManagedLamports : 0,
    estimatedPlatformFeeLamports: Number.isInteger(order?.estimatedPlatformFeeLamports)
      ? order.estimatedPlatformFeeLamports
      : 0,
    estimatedSplitNowFeeLamports: Number.isInteger(order?.estimatedSplitNowFeeLamports)
      ? order.estimatedSplitNowFeeLamports
      : 0,
    estimatedNetSplitLamports: Number.isInteger(order?.estimatedNetSplitLamports)
      ? order.estimatedNetSplitLamports
      : 0,
    automationEnabled: Boolean(order?.automationEnabled),
    status: typeof order?.status === 'string' ? order.status : 'setup',
    stats: normalizeSniperWizardStats(order?.stats),
    routingOrderId: typeof order?.routingOrderId === 'string' ? order.routingOrderId : null,
    routingQuoteId: typeof order?.routingQuoteId === 'string' ? order.routingQuoteId : null,
    routingDepositAddress: typeof order?.routingDepositAddress === 'string' ? order.routingDepositAddress : null,
    routingStatus: typeof order?.routingStatus === 'string' ? order.routingStatus : null,
    routingCompletedAt: typeof order?.routingCompletedAt === 'string' ? order.routingCompletedAt : null,
    setupFeePaidAt: typeof order?.setupFeePaidAt === 'string' ? order.setupFeePaidAt : null,
    lastDetectedLaunchSignature: typeof order?.lastDetectedLaunchSignature === 'string'
      ? order.lastDetectedLaunchSignature
      : null,
    lastDetectedMintAddress: typeof order?.lastDetectedMintAddress === 'string'
      ? order.lastDetectedMintAddress
      : null,
    lastSnipeSignature: typeof order?.lastSnipeSignature === 'string' ? order.lastSnipeSignature : null,
    lastBalanceCheckAt: typeof order?.lastBalanceCheckAt === 'string' ? order.lastBalanceCheckAt : null,
    lastError: typeof order?.lastError === 'string' ? order.lastError : null,
    createdAt: typeof order?.createdAt === 'string' ? order.createdAt : null,
    updatedAt: typeof order?.updatedAt === 'string' ? order.updatedAt : null,
  };
}

function hasMeaningfulSniperWizardRecord(order = {}) {
  return Boolean(
    order
    && (
      order.sniperMode
      || order.targetWalletAddress
      || order.automationEnabled
      || Number.isInteger(order.currentLamports) && order.currentLamports > 0
      || normalizeSniperWizardStats(order.stats).snipeAttemptCount > 0
      || typeof order.lastDetectedMintAddress === 'string' && order.lastDetectedMintAddress
    )
  );
}

function createDefaultCommunityVisionRecord() {
  return {
    id: `cv_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    profileUrl: null,
    handle: null,
    trackedCommunities: [],
    automationEnabled: false,
    status: 'setup',
    stats: {},
    lastCheckedAt: null,
    lastAlertAt: null,
    lastChangeAt: null,
    lastError: null,
    archivedAt: null,
  };
}

function normalizeCommunityVisionStats(stats = {}) {
  return {
    renameCount: Number.isInteger(stats?.renameCount) ? stats.renameCount : 0,
    alertCount: Number.isInteger(stats?.alertCount) ? stats.alertCount : 0,
  };
}

function normalizeCommunityVisionRecord(order = {}) {
  const defaults = createDefaultCommunityVisionRecord();
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
    automationEnabled: Boolean(order?.automationEnabled),
    status: typeof order?.status === 'string' ? order.status : defaults.status,
    stats: normalizeCommunityVisionStats(order?.stats),
    lastCheckedAt: typeof order?.lastCheckedAt === 'string' ? order.lastCheckedAt : null,
    lastAlertAt: typeof order?.lastAlertAt === 'string' ? order.lastAlertAt : null,
    lastChangeAt: typeof order?.lastChangeAt === 'string' ? order.lastChangeAt : null,
    lastError: typeof order?.lastError === 'string' ? order.lastError : null,
    archivedAt: typeof order?.archivedAt === 'string' ? order.archivedAt : null,
  };
}

function hasMeaningfulCommunityVisionRecord(order = {}) {
  return Boolean(order && (order.handle || order.profileUrl || order.automationEnabled));
}

function createDefaultWalletTrackerRecord() {
  return {
    id: `wt_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    walletAddress: null,
    buyMode: 'first',
    notifySells: true,
    notifyLaunches: true,
    automationEnabled: false,
    status: 'setup',
    stats: {},
    notifiedBuyMints: [],
    lastSeenSignature: null,
    lastCheckedAt: null,
    lastAlertAt: null,
    lastEventAt: null,
    lastError: null,
    archivedAt: null,
  };
}

function normalizeWalletTrackerStats(stats = {}) {
  return {
    launchCount: Number.isInteger(stats?.launchCount) ? stats.launchCount : 0,
    buyAlertCount: Number.isInteger(stats?.buyAlertCount) ? stats.buyAlertCount : 0,
    sellAlertCount: Number.isInteger(stats?.sellAlertCount) ? stats.sellAlertCount : 0,
  };
}

function normalizeWalletTrackerRecord(order = {}) {
  const defaults = createDefaultWalletTrackerRecord();
  const buyMode = ['off', 'first', 'every'].includes(order?.buyMode) ? order.buyMode : defaults.buyMode;
  return {
    ...defaults,
    ...(order ?? {}),
    id: typeof order?.id === 'string' && order.id ? order.id : defaults.id,
    walletAddress: typeof order?.walletAddress === 'string' ? order.walletAddress : null,
    buyMode,
    notifySells: typeof order?.notifySells === 'boolean' ? order.notifySells : defaults.notifySells,
    notifyLaunches: typeof order?.notifyLaunches === 'boolean' ? order.notifyLaunches : defaults.notifyLaunches,
    automationEnabled: Boolean(order?.automationEnabled),
    status: typeof order?.status === 'string' ? order.status : defaults.status,
    stats: normalizeWalletTrackerStats(order?.stats),
    notifiedBuyMints: Array.isArray(order?.notifiedBuyMints)
      ? order.notifiedBuyMints.filter((item) => typeof item === 'string').slice(0, 100)
      : [],
    lastSeenSignature: typeof order?.lastSeenSignature === 'string' ? order.lastSeenSignature : null,
    lastCheckedAt: typeof order?.lastCheckedAt === 'string' ? order.lastCheckedAt : null,
    lastAlertAt: typeof order?.lastAlertAt === 'string' ? order.lastAlertAt : null,
    lastEventAt: typeof order?.lastEventAt === 'string' ? order.lastEventAt : null,
    lastError: typeof order?.lastError === 'string' ? order.lastError : null,
    archivedAt: typeof order?.archivedAt === 'string' ? order.archivedAt : null,
  };
}

function hasMeaningfulWalletTrackerRecord(order = {}) {
  return Boolean(order && (order.walletAddress || order.automationEnabled));
}

function normalizeUserAppleBoosters(user = {}) {
  const appleBoosters = Array.isArray(user?.appleBoosters)
    ? user.appleBoosters.map((order) => normalizeOrganicOrder(order))
    : [];

  if (appleBoosters.length === 0 && user?.organicVolumeOrder && typeof user.organicVolumeOrder === 'object') {
    const legacyOrder = normalizeOrganicOrder(user.organicVolumeOrder);
    if (hasMeaningfulOrganicOrderRecord(legacyOrder)) {
      appleBoosters.push(legacyOrder);
    }
  }

  return appleBoosters;
}

function normalizeUserBurnAgents(user = {}) {
  const burnAgents = Array.isArray(user?.burnAgents)
    ? user.burnAgents.map((agent) => normalizeBurnAgentRecord(agent))
    : [];

  if (burnAgents.length === 0 && user?.burnAgent && typeof user.burnAgent === 'object') {
    const legacyAgent = normalizeBurnAgentRecord(user.burnAgent);
    if (hasMeaningfulBurnAgentRecord(legacyAgent)) {
      burnAgents.push(legacyAgent);
    }
  }

  return burnAgents;
}

function normalizeUserHolderBooster(user = {}) {
  const holderBooster = normalizeHolderBoosterRecord(user?.holderBooster ?? {});
  return hasMeaningfulHolderBoosterRecord(holderBooster)
    ? holderBooster
    : normalizeHolderBoosterRecord({});
}

function normalizeUserMagicSells(user = {}) {
  return Array.isArray(user?.magicSells)
    ? user.magicSells.map((order) => normalizeMagicSellRecord(order))
    : [];
}

function normalizeUserMagicBundles(user = {}) {
  return Array.isArray(user?.magicBundles)
    ? user.magicBundles.map((order) => normalizeMagicBundleRecord(order))
    : [];
}

function normalizeUserFomoBooster(user = {}) {
  const fomoBooster = normalizeFomoBoosterRecord(user?.fomoBooster ?? {});
  return hasMeaningfulFomoBoosterRecord(fomoBooster)
    ? fomoBooster
    : normalizeFomoBoosterRecord({});
}

function normalizeUserSniperWizard(user = {}) {
  const sniperWizard = normalizeSniperWizardRecord(user?.sniperWizard ?? {});
  return hasMeaningfulSniperWizardRecord(sniperWizard)
    ? sniperWizard
    : normalizeSniperWizardRecord({});
}

function collectRunnableSniperWizards(user = {}) {
  const candidates = [];
  const primary = normalizeUserSniperWizard(user);
  if (primary?.id && sniperWizardCanRun(primary) && primary.automationEnabled) {
    candidates.push(primary);
  }
  const fromArray = Array.isArray(user?.sniperWizards)
    ? user.sniperWizards.map((order) => normalizeSniperWizardRecord(order))
    : [];
  for (const order of fromArray) {
    if (!order?.id || !sniperWizardCanRun(order) || !order.automationEnabled) {
      continue;
    }
    if (!candidates.some((item) => item.id === order.id)) {
      candidates.push(order);
    }
  }
  return candidates;
}

function normalizeUserCommunityVisions(user = {}) {
  return Array.isArray(user?.communityVisions)
    ? user.communityVisions.map((order) => normalizeCommunityVisionRecord(order))
    : [];
}

function normalizeUserWalletTrackers(user = {}) {
  return Array.isArray(user?.walletTrackers)
    ? user.walletTrackers.map((order) => normalizeWalletTrackerRecord(order))
    : [];
}

function createDefaultTradingWalletRecord() {
  return {
    id: `tw_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    label: 'Generated Wallet',
    address: null,
    secretKeyB64: null,
    secretKeyBase58: null,
    currentLamports: 0,
    currentSol: '0',
    imported: false,
    privateKeyVisible: false,
    createdAt: new Date().toISOString(),
  };
}

function normalizeTradingWalletRecord(wallet = {}) {
  const defaults = createDefaultTradingWalletRecord();
  return {
    ...defaults,
    ...(wallet ?? {}),
    id: typeof wallet?.id === 'string' && wallet.id ? wallet.id : defaults.id,
    label: typeof wallet?.label === 'string' && wallet.label.trim()
      ? wallet.label.trim().slice(0, 24)
      : defaults.label,
    address: typeof wallet?.address === 'string' ? wallet.address : null,
    secretKeyB64: typeof wallet?.secretKeyB64 === 'string' ? wallet.secretKeyB64 : null,
    secretKeyBase58: typeof wallet?.secretKeyBase58 === 'string' ? wallet.secretKeyBase58 : null,
    currentLamports: Number.isInteger(wallet?.currentLamports) ? wallet.currentLamports : 0,
    currentSol: typeof wallet?.currentSol === 'string' ? wallet.currentSol : '0',
    imported: Boolean(wallet?.imported),
    privateKeyVisible: Boolean(wallet?.privateKeyVisible),
    createdAt: typeof wallet?.createdAt === 'string' ? wallet.createdAt : defaults.createdAt,
  };
}

function createDefaultTradingDeskRecord() {
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
    handlingFeeBps: TRADING_HANDLING_FEE_BPS,
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

function normalizeTradingDeskRecord(tradingDesk = {}) {
  const defaults = createDefaultTradingDeskRecord();
  const wallets = Array.isArray(tradingDesk?.wallets)
    ? tradingDesk.wallets.map((wallet) => normalizeTradingWalletRecord(wallet))
    : [];
  const activeWalletId = typeof tradingDesk?.activeWalletId === 'string'
    && wallets.some((wallet) => wallet.id === tradingDesk.activeWalletId)
    ? tradingDesk.activeWalletId
    : (wallets[0]?.id ?? null);
  const copyStats = tradingDesk?.copyTrade?.stats ?? {};
  const quickBuyLamports = Number.isInteger(tradingDesk?.quickBuyLamports)
    ? tradingDesk.quickBuyLamports
    : (typeof tradingDesk?.quickBuySol === 'string' ? parseSolToLamports(tradingDesk.quickBuySol) : null);
  const limitBuyLamports = Number.isInteger(tradingDesk?.limitOrder?.buyLamports)
    ? tradingDesk.limitOrder.buyLamports
    : (typeof tradingDesk?.limitOrder?.buySol === 'string' ? parseSolToLamports(tradingDesk.limitOrder.buySol) : null);
  const copyFixedBuyLamports = Number.isInteger(tradingDesk?.copyTrade?.fixedBuyLamports)
    ? tradingDesk.copyTrade.fixedBuyLamports
    : (typeof tradingDesk?.copyTrade?.fixedBuySol === 'string' ? parseSolToLamports(tradingDesk.copyTrade.fixedBuySol) : null);

  return {
    ...defaults,
    ...(tradingDesk ?? {}),
    wallets,
    activeWalletId,
    quickTradeMintAddress: typeof tradingDesk?.quickTradeMintAddress === 'string'
      ? tradingDesk.quickTradeMintAddress
      : null,
    selectedMagicBundleId: typeof tradingDesk?.selectedMagicBundleId === 'string'
      ? tradingDesk.selectedMagicBundleId
      : null,
    quickBuyLamports,
    quickBuySol: quickBuyLamports ? formatSolAmountFromLamports(quickBuyLamports) : null,
    quickSellPercent: Number.isInteger(tradingDesk?.quickSellPercent)
      ? Math.min(100, Math.max(1, tradingDesk.quickSellPercent))
      : defaults.quickSellPercent,
    pendingAction: tradingDesk?.pendingAction && typeof tradingDesk.pendingAction === 'object'
      ? {
        type: ['buy', 'sell'].includes(tradingDesk.pendingAction.type) ? tradingDesk.pendingAction.type : null,
        requestedAt: typeof tradingDesk.pendingAction.requestedAt === 'string'
          ? tradingDesk.pendingAction.requestedAt
          : null,
      }
      : null,
    lastTradeSignature: typeof tradingDesk?.lastTradeSignature === 'string' ? tradingDesk.lastTradeSignature : null,
    lastTradeSide: typeof tradingDesk?.lastTradeSide === 'string' ? tradingDesk.lastTradeSide : null,
    lastTradeAt: typeof tradingDesk?.lastTradeAt === 'string' ? tradingDesk.lastTradeAt : null,
    handlingFeeBps: Number.isInteger(tradingDesk?.handlingFeeBps)
      ? tradingDesk.handlingFeeBps
      : defaults.handlingFeeBps,
    limitOrder: {
      ...defaults.limitOrder,
      ...(tradingDesk?.limitOrder ?? {}),
      side: tradingDesk?.limitOrder?.side === 'sell' ? 'sell' : 'buy',
      triggerMarketCapUsd: Number.isFinite(tradingDesk?.limitOrder?.triggerMarketCapUsd)
        ? tradingDesk.limitOrder.triggerMarketCapUsd
        : null,
      buyLamports: limitBuyLamports,
      buySol: limitBuyLamports ? formatSolAmountFromLamports(limitBuyLamports) : null,
      sellPercent: Number.isInteger(tradingDesk?.limitOrder?.sellPercent)
        ? Math.min(100, Math.max(1, tradingDesk.limitOrder.sellPercent))
        : defaults.limitOrder.sellPercent,
      enabled: Boolean(tradingDesk?.limitOrder?.enabled),
      lastTriggeredAt: typeof tradingDesk?.limitOrder?.lastTriggeredAt === 'string'
        ? tradingDesk.limitOrder.lastTriggeredAt
        : null,
      lastTriggerSignature: typeof tradingDesk?.limitOrder?.lastTriggerSignature === 'string'
        ? tradingDesk.limitOrder.lastTriggerSignature
        : null,
      lastError: typeof tradingDesk?.limitOrder?.lastError === 'string'
        ? tradingDesk.limitOrder.lastError
        : null,
    },
    copyTrade: {
      ...defaults.copyTrade,
      ...(tradingDesk?.copyTrade ?? {}),
      followWalletAddress: typeof tradingDesk?.copyTrade?.followWalletAddress === 'string'
        ? tradingDesk.copyTrade.followWalletAddress
        : null,
      fixedBuyLamports: copyFixedBuyLamports,
      fixedBuySol: copyFixedBuyLamports ? formatSolAmountFromLamports(copyFixedBuyLamports) : null,
      copySells: typeof tradingDesk?.copyTrade?.copySells === 'boolean'
        ? tradingDesk.copyTrade.copySells
        : defaults.copyTrade.copySells,
      enabled: Boolean(tradingDesk?.copyTrade?.enabled),
      lastSeenSignature: typeof tradingDesk?.copyTrade?.lastSeenSignature === 'string'
        ? tradingDesk.copyTrade.lastSeenSignature
        : null,
      lastCopiedAt: typeof tradingDesk?.copyTrade?.lastCopiedAt === 'string'
        ? tradingDesk.copyTrade.lastCopiedAt
        : null,
      lastError: typeof tradingDesk?.copyTrade?.lastError === 'string'
        ? tradingDesk.copyTrade.lastError
        : null,
      stats: {
        buyCount: Number.isInteger(copyStats?.buyCount) ? copyStats.buyCount : 0,
        sellCount: Number.isInteger(copyStats?.sellCount) ? copyStats.sellCount : 0,
      },
    },
    awaitingField: typeof tradingDesk?.awaitingField === 'string' ? tradingDesk.awaitingField : null,
    status: typeof tradingDesk?.status === 'string' ? tradingDesk.status : defaults.status,
    lastBalanceCheckAt: typeof tradingDesk?.lastBalanceCheckAt === 'string'
      ? tradingDesk.lastBalanceCheckAt
      : null,
    lastError: typeof tradingDesk?.lastError === 'string' ? tradingDesk.lastError : null,
  };
}

function normalizeUserTradingDesk(user = {}) {
  return normalizeTradingDeskRecord(user?.tradingDesk ?? {});
}

function getActiveTradingWalletRecord(tradingDesk) {
  const desk = normalizeTradingDeskRecord(tradingDesk);
  return desk.wallets.find((wallet) => wallet.id === desk.activeWalletId) ?? null;
}

function createLaunchBuyBuyerWalletRecord() {
  return {
    label: 'Buyer Wallet',
    address: null,
    secretKeyB64: null,
    secretKeyBase58: null,
    imported: false,
    currentLamports: 0,
    currentSol: '0',
  };
}

function normalizeLaunchBuyBuyerWalletRecord(wallet = {}) {
  const defaults = createLaunchBuyBuyerWalletRecord();
  return {
    ...defaults,
    ...(wallet ?? {}),
    label: typeof wallet?.label === 'string' ? wallet.label : defaults.label,
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

function createDefaultLaunchBuyRecord() {
  return {
    id: `lb_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
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
    buyerWallets: [],
    walletAddress: null,
    walletSecretKeyB64: null,
    walletSecretKeyBase58: null,
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
    routingOrderId: null,
    routingQuoteId: null,
    routingDepositAddress: null,
    routingStatus: null,
    routingCompletedAt: null,
    setupFeePaidAt: null,
    launchedMintAddress: null,
    launchBundleId: null,
    launchSignatures: [],
    launchedAt: null,
    stats: {},
  };
}

function normalizeLaunchBuyStats(stats = {}) {
  return {
    launchCount: Number.isInteger(stats?.launchCount) ? stats.launchCount : 0,
    successCount: Number.isInteger(stats?.successCount) ? stats.successCount : 0,
    atomicWaveCount: Number.isInteger(stats?.atomicWaveCount) ? stats.atomicWaveCount : 0,
    overflowWaveCount: Number.isInteger(stats?.overflowWaveCount) ? stats.overflowWaveCount : 0,
    atomicBuyerWalletCount: Number.isInteger(stats?.atomicBuyerWalletCount) ? stats.atomicBuyerWalletCount : 0,
    overflowBuyerWalletCount: Number.isInteger(stats?.overflowBuyerWalletCount) ? stats.overflowBuyerWalletCount : 0,
    lastBuildMs: Number.isInteger(stats?.lastBuildMs) ? stats.lastBuildMs : 0,
    lastBundleSubmitMs: Number.isInteger(stats?.lastBundleSubmitMs) ? stats.lastBundleSubmitMs : 0,
    lastBundleWaitMs: Number.isInteger(stats?.lastBundleWaitMs) ? stats.lastBundleWaitMs : 0,
    lastOverflowMs: Number.isInteger(stats?.lastOverflowMs) ? stats.lastOverflowMs : 0,
    lastTotalLatencyMs: Number.isInteger(stats?.lastTotalLatencyMs) ? stats.lastTotalLatencyMs : 0,
    bestTotalLatencyMs: Number.isInteger(stats?.bestTotalLatencyMs) ? stats.bestTotalLatencyMs : 0,
    avgTotalLatencyMs: Number.isInteger(stats?.avgTotalLatencyMs) ? stats.avgTotalLatencyMs : 0,
    lastBundleTransactionCount: Number.isInteger(stats?.lastBundleTransactionCount) ? stats.lastBundleTransactionCount : 0,
    lastMintAddress: typeof stats?.lastMintAddress === 'string' ? stats.lastMintAddress : null,
    lastBundleId: typeof stats?.lastBundleId === 'string' ? stats.lastBundleId : null,
  };
}

function normalizeLaunchBuyRecord(order = {}) {
  const defaults = createDefaultLaunchBuyRecord();
  const buyerWalletCount = Number.isInteger(order?.buyerWalletCount)
    ? Math.min(LAUNCH_BUY_MAX_WALLET_COUNT, Math.max(1, order.buyerWalletCount))
    : defaults.buyerWalletCount;
  const buyerWallets = Array.isArray(order?.buyerWallets)
    ? order.buyerWallets.map((wallet) => normalizeLaunchBuyBuyerWalletRecord(wallet)).slice(0, buyerWalletCount)
    : [];
  const totalBuyLamports = Number.isInteger(order?.totalBuyLamports)
    ? order.totalBuyLamports
    : (typeof order?.totalBuySol === 'string' ? parseSolToLamports(order.totalBuySol) : null);
  const jitoTipLamports = Number.isInteger(order?.jitoTipLamports)
    ? order.jitoTipLamports
    : (typeof order?.jitoTipSol === 'string' ? parseSolToLamports(order.jitoTipSol) : defaults.jitoTipLamports);
  const launchMode = order?.launchMode === 'magic' ? 'magic' : 'normal';
  const estimatedSetupFeeLamports = launchMode === 'magic'
    ? LAUNCH_BUY_MAGIC_SETUP_FEE_LAMPORTS
    : LAUNCH_BUY_NORMAL_SETUP_FEE_LAMPORTS;
  const estimatedRoutingFeeLamports = launchMode === 'magic' && totalBuyLamports
    ? Math.floor(totalBuyLamports * (MAGIC_BUNDLE_SPLITNOW_FEE_ESTIMATE_BPS / 10_000))
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
    walletAddress: typeof order?.walletAddress === 'string' ? order.walletAddress : null,
    walletSecretKeyB64: typeof order?.walletSecretKeyB64 === 'string' ? order.walletSecretKeyB64 : null,
    walletSecretKeyBase58: typeof order?.walletSecretKeyBase58 === 'string' ? order.walletSecretKeyBase58 : null,
    privateKeyVisible: Boolean(order?.privateKeyVisible),
    currentLamports: Number.isInteger(order?.currentLamports) ? order.currentLamports : 0,
    currentSol: typeof order?.currentSol === 'string' ? order.currentSol : '0',
    totalBuyLamports,
    totalBuySol: Number.isInteger(totalBuyLamports) ? formatSolAmountFromLamports(totalBuyLamports) : null,
    jitoTipLamports,
    jitoTipSol: formatSolAmountFromLamports(jitoTipLamports),
    estimatedSetupFeeLamports,
    estimatedRoutingFeeLamports,
    estimatedTotalNeededLamports: Math.max(
      0,
      estimatedSetupFeeLamports
        + estimatedRoutingFeeLamports
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
    routingOrderId: typeof order?.routingOrderId === 'string' ? order.routingOrderId : null,
    routingQuoteId: typeof order?.routingQuoteId === 'string' ? order.routingQuoteId : null,
    routingDepositAddress: typeof order?.routingDepositAddress === 'string' ? order.routingDepositAddress : null,
    routingStatus: typeof order?.routingStatus === 'string' ? order.routingStatus : null,
    routingCompletedAt: typeof order?.routingCompletedAt === 'string' ? order.routingCompletedAt : null,
    setupFeePaidAt: typeof order?.setupFeePaidAt === 'string' ? order.setupFeePaidAt : null,
    launchedMintAddress: typeof order?.launchedMintAddress === 'string' ? order.launchedMintAddress : null,
    launchBundleId: typeof order?.launchBundleId === 'string' ? order.launchBundleId : null,
    launchSignatures: Array.isArray(order?.launchSignatures)
      ? order.launchSignatures.filter((item) => typeof item === 'string')
      : [],
    launchedAt: typeof order?.launchedAt === 'string' ? order.launchedAt : null,
    stats: normalizeLaunchBuyStats(order?.stats),
  };
}

function normalizeUserLaunchBuys(user = {}) {
  return Array.isArray(user?.launchBuys)
    ? user.launchBuys.map((order) => normalizeLaunchBuyRecord(order))
    : [];
}

async function getLatestLaunchBuyOrder(userId, launchBuyId) {
  if (!userId || !launchBuyId) {
    return null;
  }
  const store = await readStore();
  const user = store.users?.[userId];
  if (!user) {
    return null;
  }
  const orders = normalizeUserLaunchBuys(user);
  return orders.find((order) => order.id === launchBuyId) ?? null;
}

async function ensureStore() {
  await fs.mkdir(DATA_DIR, { recursive: true });
  try {
    await fs.access(STORE_PATH);
  } catch {
    await fs.writeFile(
      STORE_PATH,
      JSON.stringify(
        {
          users: {},
          jobs: [],
          processedPaymentSignatures: [],
          worker: createDefaultWorkerState(),
        },
        null,
        2,
      ),
      'utf8',
    );
  }
}

async function readStore() {
  await ensureStore();
  const raw = await fs.readFile(STORE_PATH, 'utf8');
  const parsed = JSON.parse(raw);
  const defaultWorkerState = createDefaultWorkerState();

  return {
    users: parsed.users ?? {},
    jobs: Array.isArray(parsed.jobs) ? parsed.jobs : [],
    processedPaymentSignatures: Array.isArray(parsed.processedPaymentSignatures)
      ? parsed.processedPaymentSignatures
      : [],
    worker: {
      ...defaultWorkerState,
      ...(parsed.worker ?? {}),
      devWalletSwap: {
        ...defaultWorkerState.devWalletSwap,
        ...(parsed.worker?.devWalletSwap ?? {}),
        enabled: cfg.devWalletSwap.enabled,
        status: cfg.devWalletSwap.enabled
          ? (parsed.worker?.devWalletSwap?.status === 'disabled'
            ? 'idle'
            : (parsed.worker?.devWalletSwap?.status ?? 'idle'))
          : 'disabled',
        targetMint: cfg.devWalletSwap.targetMint ?? null,
        reserveLamports: cfg.devWalletSwap.reserveLamports,
        minimumLamports: cfg.devWalletSwap.minimumLamports,
        lastError: cfg.devWalletSwap.enabled
          ? (parsed.worker?.devWalletSwap?.lastError ?? null)
          : cfg.devWalletSwap.reason,
      },
      pumpCreatorRewards: {
        ...defaultWorkerState.pumpCreatorRewards,
        ...(parsed.worker?.pumpCreatorRewards ?? {}),
        enabled: cfg.pumpCreatorRewards.enabled,
        status: cfg.pumpCreatorRewards.enabled
          ? (parsed.worker?.pumpCreatorRewards?.status === 'disabled'
            ? 'idle'
            : (parsed.worker?.pumpCreatorRewards?.status ?? 'idle'))
          : 'disabled',
        mint: cfg.pumpCreatorRewards.mint ?? null,
        intervalMs: cfg.pumpCreatorRewards.intervalMs,
        minimumClaimLamports: cfg.pumpCreatorRewards.minimumClaimLamports,
        buySlippagePercent: cfg.pumpCreatorRewards.buySlippagePercent,
        lastError: cfg.pumpCreatorRewards.enabled
          ? (parsed.worker?.pumpCreatorRewards?.lastError ?? null)
          : cfg.pumpCreatorRewards.reason,
      },
      stakingRewards: {
        ...defaultWorkerState.stakingRewards,
        ...(parsed.worker?.stakingRewards ?? {}),
        enabled: cfg.platformRevenue.enabled,
        status: cfg.platformRevenue.enabled
          ? (parsed.worker?.stakingRewards?.status === 'disabled'
            ? 'idle'
            : (parsed.worker?.stakingRewards?.status ?? 'idle'))
          : 'disabled',
        mint: cfg.platformRevenue.tokenMint ?? null,
        rewardsVaultAddress: cfg.platformRevenue.rewardsVaultAddress ?? null,
        reserveLamports: STAKING_REWARDS_VAULT_RESERVE_LAMPORTS,
        claimThresholdLamports: STAKING_MIN_CLAIM_LAMPORTS,
        earlyWeightDays: STAKING_EARLY_WEIGHT_DAYS,
        lastError: cfg.platformRevenue.enabled
          ? (parsed.worker?.stakingRewards?.lastError ?? null)
          : cfg.platformRevenue.reason,
      },
    },
  };
}

async function writeStore(store) {
  await ensureStore();
  await fs.writeFile(STORE_PATH, JSON.stringify(store, null, 2), 'utf8');
}

async function appendUserActivityLog(userId, entry) {
  const store = await readStore();
  const user = store.users?.[userId];
  if (!user) {
    return;
  }

  const activityLogs = Array.isArray(user.activityLogs) ? user.activityLogs : [];
  activityLogs.unshift({
    at: new Date().toISOString(),
    level: 'info',
    scope: 'general',
    ...entry,
  });
  user.activityLogs = activityLogs.slice(0, 80);
  store.users[userId] = user;
  await writeStore(store);
}

async function sendTelegramText(userId, text) {
  if (!cfg.telegramBotToken || !userId || !text) {
    return false;
  }

  const response = await fetch(`https://api.telegram.org/bot${cfg.telegramBotToken}/sendMessage`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'User-Agent': 'steel-tester-worker/1.0',
    },
    body: JSON.stringify({
      chat_id: String(userId),
      text,
      parse_mode: 'Markdown',
      disable_web_page_preview: true,
    }),
  });

  return response.ok;
}

async function updateOrder(userId, updater) {
  const store = await readStore();
  const user = store.users?.[userId];
  if (!user) {
    return null;
  }

  const currentOrder = normalizeOrganicOrder(user.organicVolumeOrder ?? {});
  user.organicVolumeOrder = normalizeOrganicOrder(
    updater(structuredClone(currentOrder)),
  );
  store.users[userId] = user;
  await writeStore(store);
  return user.organicVolumeOrder;
}

function syncUserAppleBoosterSelection(user, appleBoosters) {
  const activeAppleBoosterId = typeof user.activeAppleBoosterId === 'string'
    && appleBoosters.some((order) => order.id === user.activeAppleBoosterId)
    ? user.activeAppleBoosterId
    : (appleBoosters.find((order) => !order.archivedAt)?.id
      ?? appleBoosters[0]?.id
      ?? null);
  user.appleBoosters = appleBoosters;
  user.activeAppleBoosterId = activeAppleBoosterId;
  user.organicVolumeOrder = appleBoosters.find((order) => order.id === activeAppleBoosterId)
    ?? normalizeOrganicOrder({});
  user.organicVolumePackage = user.organicVolumeOrder.packageKey ?? null;
}

function organicOrderScope(orderId) {
  return `organic_order:${orderId}`;
}

async function updateAppleBooster(userId, orderId, updater) {
  const store = await readStore();
  const user = store.users?.[userId];
  if (!user) {
    return null;
  }

  const appleBoosters = normalizeUserAppleBoosters(user);
  const orderIndex = appleBoosters.findIndex((order) => order.id === orderId);
  if (orderIndex < 0) {
    return null;
  }

  appleBoosters[orderIndex] = normalizeOrganicOrder(
    updater(structuredClone(appleBoosters[orderIndex])),
  );
  syncUserAppleBoosterSelection(user, appleBoosters);
  store.users[userId] = user;
  await writeStore(store);
  return appleBoosters[orderIndex];
}

async function updateHolderBooster(userId, updater) {
  const store = await readStore();
  const user = store.users?.[userId];
  if (!user) {
    return null;
  }

  user.holderBooster = normalizeHolderBoosterRecord(
    updater(structuredClone(normalizeUserHolderBooster(user))),
  );
  store.users[userId] = user;
  await writeStore(store);
  return user.holderBooster;
}

async function updateMagicSell(userId, magicSellId, updater) {
  const store = await readStore();
  const user = store.users?.[userId];
  if (!user) {
    return null;
  }

  const magicSells = normalizeUserMagicSells(user);
  const orderIndex = magicSells.findIndex((order) => order.id === magicSellId);
  if (orderIndex < 0) {
    return null;
  }

  magicSells[orderIndex] = normalizeMagicSellRecord(
    updater(structuredClone(magicSells[orderIndex])),
  );
  user.magicSells = magicSells;
  user.magicSell = magicSells.find((order) => order.id === user.activeMagicSellId)
    ?? magicSells.find((order) => !order.archivedAt)
    ?? magicSells[0]
    ?? null;
  store.users[userId] = user;
  await writeStore(store);
  return magicSells[orderIndex];
}

async function updateMagicBundle(userId, magicBundleId, updater) {
  const store = await readStore();
  const user = store.users?.[userId];
  if (!user) {
    return null;
  }

  const magicBundles = normalizeUserMagicBundles(user);
  const orderIndex = magicBundles.findIndex((order) => order.id === magicBundleId);
  if (orderIndex < 0) {
    return null;
  }

  magicBundles[orderIndex] = normalizeMagicBundleRecord(
    updater(structuredClone(magicBundles[orderIndex])),
  );
  user.magicBundles = magicBundles;
  user.magicBundle = magicBundles.find((order) => order.id === user.activeMagicBundleId)
    ?? magicBundles.find((order) => !order.archivedAt)
    ?? magicBundles[0]
    ?? null;
  store.users[userId] = user;
  await writeStore(store);
  return magicBundles[orderIndex];
}

async function updateFomoBooster(userId, updater) {
  const store = await readStore();
  const user = store.users?.[userId];
  if (!user) {
    return null;
  }

  user.fomoBooster = normalizeFomoBoosterRecord(
    updater(structuredClone(normalizeUserFomoBooster(user))),
  );
  store.users[userId] = user;
  await writeStore(store);
  return user.fomoBooster;
}

async function updateSniperWizard(userId, updater) {
  const store = await readStore();
  const user = store.users?.[userId];
  if (!user) {
    return null;
  }

  user.sniperWizard = normalizeSniperWizardRecord(
    updater(structuredClone(normalizeUserSniperWizard(user))),
  );
  store.users[userId] = user;
  await writeStore(store);
  return user.sniperWizard;
}

async function updateCommunityVision(userId, communityVisionId, updater) {
  const store = await readStore();
  const user = store.users?.[userId];
  if (!user) {
    return null;
  }

  const orders = normalizeUserCommunityVisions(user);
  const orderIndex = orders.findIndex((order) => order.id === communityVisionId);
  if (orderIndex < 0) {
    return null;
  }

  orders[orderIndex] = normalizeCommunityVisionRecord(
    updater(structuredClone(orders[orderIndex])),
  );
  user.communityVisions = orders;
  user.communityVision = orders.find((order) => order.id === user.activeCommunityVisionId)
    ?? orders.find((order) => !order.archivedAt)
    ?? orders[0]
    ?? null;
  store.users[userId] = user;
  await writeStore(store);
  return orders[orderIndex];
}

async function updateWalletTracker(userId, walletTrackerId, updater) {
  const store = await readStore();
  const user = store.users?.[userId];
  if (!user) {
    return null;
  }

  const orders = normalizeUserWalletTrackers(user);
  const orderIndex = orders.findIndex((order) => order.id === walletTrackerId);
  if (orderIndex < 0) {
    return null;
  }

  orders[orderIndex] = normalizeWalletTrackerRecord(
    updater(structuredClone(orders[orderIndex])),
  );
  user.walletTrackers = orders;
  user.walletTracker = orders.find((order) => order.id === user.activeWalletTrackerId)
    ?? orders.find((order) => !order.archivedAt)
    ?? orders[0]
    ?? null;
  store.users[userId] = user;
  await writeStore(store);
  return orders[orderIndex];
}

async function updateTradingDesk(userId, updater) {
  const store = await readStore();
  const user = store.users?.[userId];
  if (!user) {
    return null;
  }

  user.tradingDesk = normalizeTradingDeskRecord(
    updater(structuredClone(normalizeUserTradingDesk(user))),
  );
  store.users[userId] = user;
  await writeStore(store);
  return user.tradingDesk;
}

async function updateLaunchBuy(userId, launchBuyId, updater) {
  const store = await readStore();
  const user = store.users?.[userId];
  if (!user) {
    return null;
  }

  const launchBuys = normalizeUserLaunchBuys(user);
  const orderIndex = launchBuys.findIndex((order) => order.id === launchBuyId);
  if (orderIndex < 0) {
    return null;
  }

  launchBuys[orderIndex] = normalizeLaunchBuyRecord(
    updater(structuredClone(launchBuys[orderIndex])),
  );
  user.launchBuys = launchBuys;
  user.launchBuy = launchBuys.find((order) => order.id === user.activeLaunchBuyId)
    ?? launchBuys.find((order) => !order.archivedAt)
    ?? launchBuys[0]
    ?? null;
  store.users[userId] = user;
  await writeStore(store);
  return launchBuys[orderIndex];
}

async function updateUserBurnAgent(userId, agentId, updater) {
  const store = await readStore();
  const user = store.users?.[userId];
  if (!user) {
    return null;
  }

  const burnAgents = normalizeUserBurnAgents(user);
  const agentIndex = burnAgents.findIndex((agent) => agent.id === agentId);
  if (agentIndex < 0) {
    return null;
  }

  burnAgents[agentIndex] = normalizeBurnAgentRecord(
    updater(structuredClone(burnAgents[agentIndex])),
  );
  user.burnAgents = burnAgents;
  user.burnAgent = burnAgents.find((agent) => agent.id === user.activeBurnAgentId)
    ?? burnAgents.find((agent) => !agent.archivedAt)
    ?? burnAgents[0]
    ?? null;
  store.users[userId] = user;
  await writeStore(store);
  return burnAgents[agentIndex];
}

async function updateBurnAgentRuntime(userId, agentId, updater) {
  const updatedAgent = await updateUserBurnAgent(userId, agentId, (draft) => {
    draft.runtime = normalizeBurnAgentRuntime(
      updater(structuredClone(normalizeBurnAgentRuntime(draft.runtime))),
    );
    return draft;
  });

  return updatedAgent?.runtime ?? null;
}

async function updateWorkerState(updater) {
  const store = await readStore();
  store.worker = updater(structuredClone(store.worker ?? createDefaultWorkerState()));
  await writeStore(store);
  return store.worker;
}

async function sendLegacyTransaction(instructions, signer) {
  if (!Array.isArray(instructions) || instructions.length === 0) {
    throw new Error('Cannot send an empty transaction.');
  }

  const latestBlockhash = await connection.getLatestBlockhash('confirmed');
  const transaction = new Transaction({
    feePayer: signer.publicKey,
    lastValidBlockHeight: latestBlockhash.lastValidBlockHeight,
    recentBlockhash: latestBlockhash.blockhash,
  });

  for (const instruction of instructions) {
    transaction.add(instruction);
  }

  const signature = await connection.sendTransaction(transaction, [signer], {
    skipPreflight: true,
    preflightCommitment: 'confirmed',
    maxRetries: 3,
  });

  await confirmTransactionByMetadata(
    signature,
    latestBlockhash.blockhash,
    latestBlockhash.lastValidBlockHeight,
    'confirmed',
  );

  return signature;
}

function getReservedDevWalletLamports(workerState) {
  const pendingTreasuryLamports = Number.parseInt(
    String(workerState?.pumpCreatorRewards?.pendingTreasuryLamports || '0'),
    10,
  );
  const pendingBurnBuybackLamports = Number.parseInt(
    String(workerState?.pumpCreatorRewards?.pendingBurnBuybackLamports || '0'),
    10,
  );
  const pendingRewardsVaultLamports = Number.parseInt(
    String(workerState?.pumpCreatorRewards?.pendingRewardsVaultLamports || '0'),
    10,
  );

  return Math.max(0, pendingTreasuryLamports)
    + Math.max(0, pendingBurnBuybackLamports)
    + Math.max(0, pendingRewardsVaultLamports);
}

async function getMintTokenProgram(mintAddress, commitment = 'confirmed') {
  const mintPubkey = new PublicKey(mintAddress);
  const mintAccountInfo = await connection.getAccountInfo(mintPubkey, commitment);
  if (!mintAccountInfo) {
    throw new Error(`Mint account not found for ${mintPubkey.toBase58()}.`);
  }

  if (
    !mintAccountInfo.owner.equals(TOKEN_PROGRAM_ID)
    && !mintAccountInfo.owner.equals(TOKEN_2022_PROGRAM_ID)
  ) {
    throw new Error(
      `Unsupported token program ${mintAccountInfo.owner.toBase58()} for mint ${mintPubkey.toBase58()}.`,
    );
  }

  return mintAccountInfo.owner;
}

async function getMintMetadata(mintAddress) {
  const mintPubkey = new PublicKey(mintAddress);
  const parsedAccount = await connection.getParsedAccountInfo(mintPubkey, 'confirmed');
  const parsedInfo = parsedAccount.value?.data?.parsed?.info;
  const decimals = parsedInfo?.decimals;
  if (!Number.isInteger(decimals) || decimals < 0) {
    throw new Error(`Unable to read mint decimals for ${mintPubkey.toBase58()}.`);
  }

  return {
    decimals,
    tokenProgram: await getMintTokenProgram(mintAddress),
  };
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
    lastRewardsAllocatedAt: typeof state.lastRewardsAllocatedAt === 'string'
      ? state.lastRewardsAllocatedAt
      : null,
    currentWeightLabel: typeof state.currentWeightLabel === 'string'
      ? state.currentWeightLabel
      : defaults.currentWeightLabel,
    lastError: typeof state.lastError === 'string' ? state.lastError : null,
  };
}

function getStakingWeightProfile(trackingStartedAt) {
  if (!trackingStartedAt) {
    return STAKING_WEIGHT_TIERS[0];
  }

  const startedAtMs = new Date(trackingStartedAt).getTime();
  if (!Number.isFinite(startedAtMs) || startedAtMs <= 0) {
    return STAKING_WEIGHT_TIERS[0];
  }

  const elapsedDays = Math.max(0, Math.floor((Date.now() - startedAtMs) / 86_400_000));
  let selected = STAKING_WEIGHT_TIERS[0];
  for (const tier of STAKING_WEIGHT_TIERS) {
    if (elapsedDays >= tier.minDays) {
      selected = tier;
    }
  }
  return selected;
}

function shouldTrackOrder(order) {
  return Boolean(
    order &&
    !order.archivedAt &&
    !order.freeTrial &&
    order.walletAddress &&
    order.walletSecretKeyB64 &&
    order.packageKey &&
    Number.isInteger(order.requiredLamports),
  );
}

function getSplitStatus(order) {
  return order.treasurySplitStatus || 'pending';
}

function shouldAttemptSplit(order) {
  const status = getSplitStatus(order);
  if (status === 'completed') {
    return false;
  }

  if (status !== 'processing') {
    return true;
  }

  const attemptedAt = order.treasurySplitAttemptedAt
    ? new Date(order.treasurySplitAttemptedAt).getTime()
    : 0;
  return !attemptedAt || Date.now() - attemptedAt >= PROCESSING_RETRY_AFTER_MS;
}

async function refreshOrderFunding(userId, orderId, order) {
  const balanceLamports = await connection.getBalance(new PublicKey(order.walletAddress), 'confirmed');
  const funded = Boolean(
    order.funded ||
    order.fundedAt ||
    balanceLamports >= (order.requiredLamports ?? Number.MAX_SAFE_INTEGER),
  );
  const now = new Date().toISOString();

  return updateAppleBooster(userId, orderId, (draft) => ({
    ...draft,
    currentLamports: balanceLamports,
    currentSol: formatSolAmountFromLamports(balanceLamports),
    funded,
    fundedAt: funded ? (draft.fundedAt || now) : null,
    lastBalanceCheckAt: now,
    lastError: null,
  }));
}

function buildSplitPlan(order) {
  const pkg = getAppleBoosterPackage(order.strategy, order.packageKey);
  const treasuryCutLamports = parseSolToLamports(order.treasuryCutSol || pkg?.treasuryCutSol || '0');
  if (treasuryCutLamports <= 0) {
    throw new Error('Treasury cut is not configured for this package.');
  }

  const explicitTreasuryLamports = parseSolToLamports(order.treasuryShareSol || '0');
  const explicitDevLamports = parseSolToLamports(order.devShareSol || '0');

  if (
    explicitTreasuryLamports > 0 &&
    explicitDevLamports > 0 &&
    explicitTreasuryLamports + explicitDevLamports === treasuryCutLamports
  ) {
    return {
      treasuryCutLamports,
      treasuryLamports: explicitTreasuryLamports,
      devLamports: explicitDevLamports,
    };
  }

  const treasuryLamports = Math.floor(treasuryCutLamports / 2);
  return {
    treasuryCutLamports,
    treasuryLamports,
    devLamports: treasuryCutLamports - treasuryLamports,
  };
}

async function processTreasurySplit(userId, orderId, order) {
  if (!isConfiguredAddress(cfg.treasuryWalletAddress)) {
    throw new Error('Treasury split requires TREASURY_WALLET_ADDRESS.');
  }

  const sender = decodeOrderWallet(order.walletSecretKeyB64);
  if (sender.publicKey.toBase58() !== order.walletAddress) {
    throw new Error('Order wallet secret does not match the stored wallet address.');
  }

  const splitPlan = buildSplitPlan(order);
  const balanceBeforeLamports = await connection.getBalance(sender.publicKey, 'confirmed');
  if (balanceBeforeLamports < splitPlan.treasuryCutLamports + SPLIT_FEE_BUFFER_LAMPORTS) {
    throw new Error('Order wallet balance is too low to cover the treasury split and network fee.');
  }

  await updateAppleBooster(userId, orderId, (draft) => ({
    ...draft,
    treasurySplitStatus: 'processing',
    treasurySplitError: null,
    treasurySplitAttemptedAt: new Date().toISOString(),
  }));

  const routeSummary = await routePlatformProfitFromSigner(
    sender,
    splitPlan.treasuryCutLamports,
    'Apple Booster',
  );

  const balanceAfterLamports = await connection.getBalance(sender.publicKey, 'confirmed');
  await updateAppleBooster(userId, orderId, (draft) => ({
    ...draft,
    currentLamports: balanceAfterLamports,
    currentSol: formatSolAmountFromLamports(balanceAfterLamports),
    treasurySplitStatus: 'completed',
    treasurySplitSignature: routeSummary?.treasurySignature
      || routeSummary?.burnSignature
      || routeSummary?.burnBuybackSignature
      || routeSummary?.rewardsVaultSignature
      || routeSummary?.signature
      || null,
    treasurySplitProcessedAt: new Date().toISOString(),
    treasurySplitError: null,
    treasurySplitLamports: splitPlan.treasuryCutLamports,
    treasurySplitTreasuryLamports: routeSummary?.treasuryLamports ?? splitPlan.treasuryLamports,
    treasurySplitDevLamports: 0,
    treasurySplitBalanceBeforeLamports: balanceBeforeLamports,
    treasurySplitBalanceAfterLamports: balanceAfterLamports,
  }));

  console.log(
    `[worker] Split completed for user ${userId}: ${formatSolAmountFromLamports(splitPlan.treasuryCutLamports)} SOL routed through the platform router.`,
  );
  await appendUserActivityLog(userId, {
    scope: organicOrderScope(orderId),
    level: 'info',
    message: `Platform profit routing completed for ${formatSolAmountFromLamports(splitPlan.treasuryCutLamports)} SOL.`,
  });
}

async function markSplitFailure(userId, orderId, error) {
  await updateAppleBooster(userId, orderId, (draft) => ({
    ...draft,
    treasurySplitStatus: 'failed',
    treasurySplitError: String(error.message || error),
    treasurySplitFailedAt: new Date().toISOString(),
  }));
  await appendUserActivityLog(userId, {
    scope: organicOrderScope(orderId),
    level: 'error',
    message: `Treasury split failed: ${String(error.message || error)}`,
  });
}

async function scanOrders() {
  const store = await readStore();
  for (const [userId, user] of Object.entries(store.users ?? {})) {
    for (const order of normalizeUserAppleBoosters(user)) {
      if (!shouldTrackOrder(order)) {
        continue;
      }

      let refreshedOrder = order;
      try {
        refreshedOrder = await refreshOrderFunding(userId, order.id, order);
      } catch (error) {
        console.error(`[worker] Funding refresh failed for user ${userId}:`, error.message || error);
        await updateAppleBooster(userId, order.id, (draft) => ({
          ...draft,
          lastBalanceCheckAt: new Date().toISOString(),
          lastError: String(error.message || error),
        }));
        continue;
      }

      if (!refreshedOrder?.funded) {
        continue;
      }

      if (!shouldAttemptSplit(refreshedOrder)) {
        continue;
      }

      try {
        await processTreasurySplit(userId, order.id, refreshedOrder);
      } catch (error) {
        console.error(`[worker] Treasury split failed for user ${userId}:`, error.message || error);
        await markSplitFailure(userId, order.id, error);
      }
    }
  }
}

function organicBoosterIsConfigured(order) {
  if (order?.strategy === 'bundled') {
    return Boolean(
      order?.walletAddress
      && order?.walletSecretKeyB64
      && order?.funded
      && order?.appleBooster?.mintAddress
      && Number.isInteger(order.appleBooster.minSwapLamports)
      && Number.isInteger(order.appleBooster.maxSwapLamports)
      && order.appleBooster.minSwapLamports > 0
      && order.appleBooster.maxSwapLamports >= order.appleBooster.minSwapLamports
      && Number.isInteger(order.appleBooster.minIntervalSeconds)
      && Number.isInteger(order.appleBooster.maxIntervalSeconds)
      && order.appleBooster.minIntervalSeconds > 0
      && order.appleBooster.maxIntervalSeconds >= order.appleBooster.minIntervalSeconds
    );
  }

  return Boolean(
    order?.walletAddress
    && order?.walletSecretKeyB64
    && order?.funded
    && Number.isInteger(order?.appleBooster?.walletCount)
    && order.appleBooster.walletCount >= 1
    && order.appleBooster.walletCount <= 5
    && Array.isArray(order?.appleBooster?.workerWallets)
    && order.appleBooster.workerWallets.length === order.appleBooster.walletCount
    && order.appleBooster.workerWallets.every((worker) => worker.address && worker.secretKeyB64)
    && order?.appleBooster?.mintAddress
    && Number.isInteger(order.appleBooster.minSwapLamports)
    && Number.isInteger(order.appleBooster.maxSwapLamports)
    && order.appleBooster.minSwapLamports > 0
    && order.appleBooster.maxSwapLamports >= order.appleBooster.minSwapLamports
    && Number.isInteger(order.appleBooster.minIntervalSeconds)
    && Number.isInteger(order.appleBooster.maxIntervalSeconds)
    && order.appleBooster.minIntervalSeconds > 0
    && order.appleBooster.maxIntervalSeconds >= order.appleBooster.minIntervalSeconds
  );
}

function shouldRunOrganicBooster(order) {
  return Boolean(!order?.archivedAt && (order?.running || order?.appleBooster?.stopRequested) && organicBoosterIsConfigured(order));
}

function isVolumeTrialOrder(order) {
  return Boolean(order?.freeTrial);
}

function getVolumeTrialTradeGoal(order) {
  return Number.isInteger(order?.trialTradeGoal) && order.trialTradeGoal > 0
    ? order.trialTradeGoal
    : cfg.volumeTrial.tradeGoal;
}

function getOrganicTradeLegCount(order) {
  return (order?.appleBooster?.totalBuyCount || 0) + (order?.appleBooster?.totalSellCount || 0);
}

async function fundVolumeTrialDepositIfNeeded(userId, order, snapshot, requiredAvailableLamports) {
  if (!isVolumeTrialOrder(order)) {
    return snapshot;
  }

  if (!cfg.volumeTrial.enabled || !cfg.volumeTrial.signer) {
    throw new Error(`Volume free trial is unavailable: ${cfg.volumeTrial.reason || 'missing trial wallet config'}.`);
  }

  const availableLamports = Math.max(0, snapshot.depositLamports - APPLE_BOOSTER_FEE_RESERVE_LAMPORTS);
  if (availableLamports >= requiredAvailableLamports) {
    return snapshot;
  }

  const fundingLamports = requiredAvailableLamports - availableLamports;
  const sponsorBalanceLamports = await connection.getBalance(cfg.volumeTrial.signer.publicKey, 'confirmed');
  if (sponsorBalanceLamports <= fundingLamports + cfg.volumeTrial.sourceReserveLamports) {
    throw new Error('Volume free-trial wallet is out of SOL for demo funding.');
  }

  await transferLamportsBetweenWallets(
    cfg.volumeTrial.signer,
    order.walletAddress,
    fundingLamports,
  );

  await appendUserActivityLog(userId, {
    scope: organicOrderScope(order.id),
    level: 'info',
    message: `Platform free-trial wallet funded the demo with ${formatSolAmountFromLamports(fundingLamports)} SOL.`,
  });

  return refreshOrganicWalletSnapshot(order);
}

function randomIntegerBetween(min, max) {
  if (!Number.isInteger(min) || !Number.isInteger(max)) {
    return 0;
  }

  if (max <= min) {
    return min;
  }

  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function getNextOrganicBoosterActionAt(booster) {
  const intervalSeconds = randomIntegerBetween(
    booster.minIntervalSeconds,
    booster.maxIntervalSeconds,
  );
  return new Date(Date.now() + (intervalSeconds * 1000)).toISOString();
}

function organicBoosterCanAct(booster) {
  const nextActionAt = booster?.nextActionAt ? new Date(booster.nextActionAt).getTime() : 0;
  return !nextActionAt || Date.now() >= nextActionAt;
}

function formatBnLamports(bn) {
  const whole = bn.div(new BN(LAMPORTS_PER_SOL));
  const fractional = bn.mod(new BN(LAMPORTS_PER_SOL)).toString(10).padStart(9, '0').replace(/0+$/, '');
  return fractional ? `${whole.toString()}.${fractional}` : whole.toString();
}

function selectPumpFeeTier(feeConfig, marketCap) {
  if (!feeConfig?.feeTiers?.length) {
    return feeConfig?.flatFees ?? null;
  }

  const firstTier = feeConfig.feeTiers[0];
  if (marketCap.lt(firstTier.marketCapLamportsThreshold)) {
    return firstTier.fees;
  }

  for (const tier of feeConfig.feeTiers.slice().reverse()) {
    if (marketCap.gte(tier.marketCapLamportsThreshold)) {
      return tier.fees;
    }
  }

  return firstTier.fees;
}

function estimateAppleBoosterRuntimeFromSnapshot(booster, totalManagedLamports) {
  const effectiveWalletCount = Number.isInteger(booster?.walletCount)
    ? booster.walletCount
    : 1;
  if (
    !Number.isInteger(totalManagedLamports)
    || !Number.isInteger(effectiveWalletCount)
    || !Number.isInteger(booster?.estimatedCycleFeeLamports)
    || booster.estimatedCycleFeeLamports <= 0
  ) {
    return {
      estimatedCyclesRemaining: null,
      estimatedRuntimeSeconds: null,
    };
  }

  const reserveLamports = APPLE_BOOSTER_FEE_RESERVE_LAMPORTS * (effectiveWalletCount + 1);
  const spendableLamports = Math.max(0, totalManagedLamports - reserveLamports);
  const estimatedCyclesRemaining = Math.floor(spendableLamports / booster.estimatedCycleFeeLamports);
  const averageIntervalSeconds = Number.isInteger(booster.minIntervalSeconds) && Number.isInteger(booster.maxIntervalSeconds)
    ? Math.round((booster.minIntervalSeconds + booster.maxIntervalSeconds) / 2)
    : null;

  return {
    estimatedCyclesRemaining,
    estimatedRuntimeSeconds: Number.isInteger(averageIntervalSeconds)
      ? estimatedCyclesRemaining * averageIntervalSeconds
      : null,
  };
}

async function inspectPumpMintMarket(mintAddress, userPublicKey) {
  try {
    const mint = new PublicKey(mintAddress);
    const {
      bondingCurve,
    } = await pumpOnlineSdk.fetchBuyState(mint, userPublicKey);

    if (!bondingCurve.complete) {
      const global = await pumpOnlineSdk.fetchGlobal();
      const feeConfig = await pumpOnlineSdk.fetchFeeConfig();
      const marketCap = bondingCurveMarketCap({
        mintSupply: global.tokenTotalSupply,
        virtualSolReserves: bondingCurve.virtualSolReserves,
        virtualTokenReserves: bondingCurve.virtualTokenReserves,
      });
      const fees = selectPumpFeeTier(feeConfig, marketCap);

      return {
        marketPhase: 'bonding_curve',
        marketCapLamports: marketCap.toString(),
        marketCapSol: formatBnLamports(marketCap),
        lpFeeBps: Number(fees?.lpFeeBps?.toString() || '0'),
        protocolFeeBps: Number(fees?.protocolFeeBps?.toString() || global.feeBasisPoints.toString()),
        creatorFeeBps: Number(fees?.creatorFeeBps?.toString() || global.creatorFeeBasisPoints.toString()),
        lastMarketCheckedAt: new Date().toISOString(),
      };
    }

    const poolKey = canonicalPumpPoolPda(mint);
    const swapState = await pumpAmmOnlineSdk.swapSolanaState(poolKey, userPublicKey);

    return {
      marketPhase: 'bonded_pool',
      marketCapLamports: null,
      marketCapSol: null,
      lpFeeBps: Number(swapState.globalConfig.lpFeeBasisPoints.toString()),
      protocolFeeBps: Number(swapState.globalConfig.protocolFeeBasisPoints.toString()),
      creatorFeeBps: Number(swapState.globalConfig.coinCreatorFeeBasisPoints.toString()),
      lastMarketCheckedAt: new Date().toISOString(),
    };
  } catch {
    return {
      marketPhase: 'unknown',
      marketCapLamports: null,
      marketCapSol: null,
      lpFeeBps: null,
      protocolFeeBps: null,
      creatorFeeBps: null,
      lastMarketCheckedAt: new Date().toISOString(),
    };
  }
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

function calculateMagicSellCurrentMarketCapUsd(marketInfo, solUsdRate) {
  const marketCapSol = Number.parseFloat(marketInfo?.marketCapSol || '');
  if (!Number.isFinite(marketCapSol) || marketCapSol <= 0 || !Number.isFinite(solUsdRate) || solUsdRate <= 0) {
    return null;
  }

  return marketCapSol * solUsdRate;
}

async function getRecentMintSignatures(mintAddress, limit = MAGIC_SELL_SCAN_LIMIT) {
  return connection.getSignaturesForAddress(new PublicKey(mintAddress), {
    limit,
    commitment: 'confirmed',
  });
}

async function getParsedTransactionBySignature(signature) {
  return connection.getParsedTransaction(signature, {
    commitment: 'confirmed',
    maxSupportedTransactionVersion: 0,
  });
}

function buildMagicSellBuyerEvent(transaction, mintAddress, ignoredWallets = new Set()) {
  if (!transaction || transaction.meta?.err) {
    return null;
  }

  const accountKeys = transaction.transaction?.message?.accountKeys ?? [];
  const positiveTokenDeltas = new Map();
  for (const entry of transaction.meta?.preTokenBalances ?? []) {
    if (entry?.mint !== mintAddress || !entry?.owner) {
      continue;
    }
    const key = entry.owner;
    const current = positiveTokenDeltas.get(key) || 0n;
    positiveTokenDeltas.set(key, current - BigInt(entry.uiTokenAmount?.amount || '0'));
  }
  for (const entry of transaction.meta?.postTokenBalances ?? []) {
    if (entry?.mint !== mintAddress || !entry?.owner) {
      continue;
    }
    const key = entry.owner;
    const current = positiveTokenDeltas.get(key) || 0n;
    positiveTokenDeltas.set(key, current + BigInt(entry.uiTokenAmount?.amount || '0'));
  }

  const candidates = [...positiveTokenDeltas.entries()]
    .filter(([owner, delta]) => delta > 0n && !ignoredWallets.has(owner));
  if (candidates.length === 0) {
    return null;
  }

  candidates.sort((left, right) => (left[1] === right[1] ? 0 : (left[1] > right[1] ? -1 : 1)));
  const [buyerWallet, tokenDelta] = candidates[0];
  const accountIndex = accountKeys.findIndex((accountKey) => getAccountKeyValue(accountKey) === buyerWallet);
  if (accountIndex < 0) {
    return null;
  }

  const preBalance = transaction.meta?.preBalances?.[accountIndex];
  const postBalance = transaction.meta?.postBalances?.[accountIndex];
  if (!Number.isInteger(preBalance) || !Number.isInteger(postBalance)) {
    return null;
  }

  const lamportsSpent = Math.max(0, preBalance - postBalance);
  if (lamportsSpent <= 0) {
    return null;
  }

  return {
    buyerWallet,
    lamportsSpent,
    tokenAmountRaw: tokenDelta.toString(),
  };
}

function publicKeyishToBase58(value) {
  if (!value) {
    return null;
  }

  if (typeof value === 'string') {
    try {
      return new PublicKey(value).toBase58();
    } catch {
      return null;
    }
  }

  if (typeof value?.toBase58 === 'function') {
    return value.toBase58();
  }

  return null;
}

async function resolveMagicBundleCreatorAddress(mintAddress, userPublicKey) {
  try {
    const mint = new PublicKey(mintAddress);
    const { bondingCurve } = await pumpOnlineSdk.fetchBuyState(mint, userPublicKey);
    const directCreator = publicKeyishToBase58(bondingCurve?.creator);
    if (directCreator) {
      return directCreator;
    }

    if (bondingCurve?.complete) {
      const poolKey = canonicalPumpPoolPda(mint);
      const swapState = await pumpAmmOnlineSdk.swapSolanaState(poolKey, userPublicKey);
      return publicKeyishToBase58(swapState?.pool?.coinCreator)
        || publicKeyishToBase58(swapState?.pool?.creator)
        || publicKeyishToBase58(swapState?.coinCreator)
        || publicKeyishToBase58(swapState?.creator);
    }
  } catch {
    return null;
  }

  return null;
}

function buildMagicBundleCreatorSellEvent(transaction, mintAddress, creatorAddress) {
  if (!transaction || transaction.meta?.err || !creatorAddress) {
    return null;
  }

  let delta = 0n;
  for (const entry of transaction.meta?.preTokenBalances ?? []) {
    if (entry?.mint === mintAddress && entry?.owner === creatorAddress) {
      delta -= BigInt(entry.uiTokenAmount?.amount || '0');
    }
  }
  for (const entry of transaction.meta?.postTokenBalances ?? []) {
    if (entry?.mint === mintAddress && entry?.owner === creatorAddress) {
      delta += BigInt(entry.uiTokenAmount?.amount || '0');
    }
  }

  if (delta >= 0n) {
    return null;
  }

  return {
    soldAmountRaw: (-delta).toString(),
  };
}

async function detectRecentMagicBundleCreatorSell(order, creatorAddress) {
  const signatures = await getRecentMintSignatures(order.mintAddress, MAGIC_BUNDLE_DEV_SELL_SCAN_LIMIT);
  if (!Array.isArray(signatures) || signatures.length === 0) {
    return {
      lastSeenSignature: order.lastCreatorSeenSignature ?? null,
      event: null,
    };
  }

  if (!order.lastCreatorSeenSignature) {
    return {
      lastSeenSignature: signatures[0]?.signature ?? null,
      event: null,
    };
  }

  const toProcess = [];
  for (const info of signatures) {
    if (!info?.signature || info.err) {
      continue;
    }
    if (info.signature === order.lastCreatorSeenSignature) {
      break;
    }
    toProcess.push(info);
  }

  if (toProcess.length === 0) {
    return {
      lastSeenSignature: order.lastCreatorSeenSignature,
      event: null,
    };
  }

  let detectedEvent = null;
  for (const info of toProcess.reverse()) {
    const transaction = await getParsedTransactionBySignature(info.signature);
    const event = buildMagicBundleCreatorSellEvent(transaction, order.mintAddress, creatorAddress);
    if (event && !detectedEvent) {
      detectedEvent = {
        ...event,
        signature: info.signature,
      };
    }
  }

  return {
    lastSeenSignature: toProcess[0]?.signature ?? order.lastCreatorSeenSignature,
    event: detectedEvent,
  };
}

function hasMagicBundleAutomationRule(order) {
  return Boolean(
    Number.isFinite(order?.stopLossPercent)
    || Number.isFinite(order?.takeProfitPercent)
    || Number.isFinite(order?.trailingStopLossPercent)
    || Number.isFinite(order?.buyDipPercent)
    || order?.sellOnDevSell,
  );
}

function isMagicBundleAutomationConfigured(order) {
  return Boolean(
    order?.mintAddress
    && order?.walletAddress
    && order?.walletSecretKeyB64
    && order?.splitCompletedAt
    && Array.isArray(order?.splitWallets)
    && order.splitWallets.length > 0
    && hasMagicBundleAutomationRule(order),
  );
}

async function refreshMagicBundleExecutionSnapshot(order) {
  const signer = decodeOrderWallet(order.walletSecretKeyB64);
  if (signer.publicKey.toBase58() !== order.walletAddress) {
    throw new Error('Magic Bundle deposit wallet secret does not match the stored wallet address.');
  }

  const depositLamports = await connection.getBalance(signer.publicKey, 'confirmed');
  const mintMetadata = order.mintAddress ? await getMintMetadata(order.mintAddress) : null;
  const creatorAddress = order.mintAddress
    ? (await resolveMagicBundleCreatorAddress(order.mintAddress, signer.publicKey)) || order.creatorAddress || null
    : null;

  let totalTokenRaw = 0n;
  let currentPositionValueLamports = 0;

  const splitWallets = await Promise.all(
    (Array.isArray(order.splitWallets) ? order.splitWallets : []).map(async (wallet) => {
      const normalizedWallet = normalizeMagicBundleWalletRecord(wallet);
      if (!normalizedWallet.address) {
        return normalizedWallet;
      }

      const currentLamports = await connection.getBalance(new PublicKey(normalizedWallet.address), 'confirmed');
      let currentTokenRaw = 0n;
      let currentTokenAmountDisplay = '0';
      let quotedValueLamports = 0;

      if (mintMetadata && order.mintAddress) {
        currentTokenRaw = await getOwnedMintRawBalance(new PublicKey(normalizedWallet.address), order.mintAddress);
        currentTokenAmountDisplay = formatTokenAmountFromRaw(currentTokenRaw.toString(), mintMetadata.decimals);
        if (currentTokenRaw > 0n) {
          try {
            const liquidationOrder = await fetchJupiterSwapOrderFor({
              inputMint: order.mintAddress,
              outputMint: SOL_MINT_ADDRESS,
              amount: currentTokenRaw.toString(),
              taker: normalizedWallet.address,
            });
            quotedValueLamports = Number.parseInt(String(liquidationOrder.outAmount || '0'), 10) || 0;
          } catch {
            quotedValueLamports = 0;
          }
        }
      }

      totalTokenRaw += currentTokenRaw;
      currentPositionValueLamports += quotedValueLamports;

      const costBasisLamports = currentTokenRaw > 0n
        ? (
          Number.isInteger(normalizedWallet.costBasisLamports) && normalizedWallet.costBasisLamports > 0
            ? normalizedWallet.costBasisLamports
            : quotedValueLamports
        )
        : null;
      const highestValueLamports = currentTokenRaw > 0n
        ? Math.max(
          Number.isInteger(normalizedWallet.highestValueLamports) ? normalizedWallet.highestValueLamports : 0,
          quotedValueLamports,
        )
        : null;

      return normalizeMagicBundleWalletRecord({
        ...normalizedWallet,
        currentLamports,
        currentSol: formatSolAmountFromLamports(currentLamports),
        currentTokenAmountRaw: currentTokenRaw.toString(),
        currentTokenAmountDisplay,
        currentPositionValueLamports: quotedValueLamports,
        costBasisLamports,
        highestValueLamports,
        status: currentTokenRaw > 0n ? 'monitoring' : 'idle',
      });
    }),
  );

  const totalManagedLamports = depositLamports + splitWallets.reduce(
    (sum, wallet) => sum + (Number.isInteger(wallet.currentLamports) ? wallet.currentLamports : 0),
    0,
  );

  return {
    signer,
    depositLamports,
    splitWallets,
    totalManagedLamports,
    mintMetadata,
    totalTokenRaw,
    currentPositionValueLamports,
    creatorAddress,
  };
}

async function persistMagicBundleSnapshot(userId, orderId, snapshot, patchOrder = () => ({})) {
  return updateMagicBundle(userId, orderId, (draft) => ({
    ...draft,
    tokenDecimals: snapshot.mintMetadata?.decimals ?? draft.tokenDecimals ?? null,
    tokenProgram: snapshot.mintMetadata?.tokenProgram
      ? snapshot.mintMetadata.tokenProgram.toBase58()
      : draft.tokenProgram,
    currentLamports: snapshot.depositLamports,
    currentSol: formatSolAmountFromLamports(snapshot.depositLamports),
    totalManagedLamports: snapshot.totalManagedLamports,
    currentTokenAmountRaw: snapshot.totalTokenRaw.toString(),
    currentTokenAmountDisplay: snapshot.mintMetadata
      ? formatTokenAmountFromRaw(snapshot.totalTokenRaw.toString(), snapshot.mintMetadata.decimals)
      : draft.currentTokenAmountDisplay,
    currentPositionValueLamports: snapshot.currentPositionValueLamports,
    creatorAddress: snapshot.creatorAddress ?? draft.creatorAddress ?? null,
    splitWallets: snapshot.splitWallets,
    lastBalanceCheckAt: new Date().toISOString(),
    ...patchOrder(draft),
  }));
}

function getMagicBundleBuyDipSpendLamports(wallet) {
  const spendableLamports = Math.max(
    0,
    (wallet.currentLamports || 0) - MAGIC_BUNDLE_POSITION_GAS_RESERVE_LAMPORTS,
  );
  if (spendableLamports <= 0) {
    return 0;
  }

  return Math.max(1, Math.floor((spendableLamports * MAGIC_BUNDLE_DIP_BUY_SPEND_BPS) / 10_000));
}

function magicBundleDipCooldownElapsed(wallet) {
  const lastActionTime = wallet?.lastActionAt ? new Date(wallet.lastActionAt).getTime() : 0;
  return !lastActionTime || Date.now() - lastActionTime >= MAGIC_BUNDLE_DIP_COOLDOWN_MS;
}

async function executeMagicBundleSell(userId, order, snapshot, walletIndex, reason) {
  const targetWallet = normalizeMagicBundleWalletRecord(snapshot.splitWallets[walletIndex]);
  const sellAmountRaw = BigInt(targetWallet.currentTokenAmountRaw || '0');
  if (!targetWallet.address || !targetWallet.secretKeyB64 || sellAmountRaw <= 0n) {
    return snapshot;
  }

  const signer = decodeOrderWallet(targetWallet.secretKeyB64);
  if (signer.publicKey.toBase58() !== targetWallet.address) {
    throw new Error(`Magic Bundle wallet #${walletIndex + 1} secret does not match its wallet address.`);
  }

  const preppedWallets = snapshot.splitWallets.map((wallet, index) => (
    index === walletIndex
      ? normalizeMagicBundleWalletRecord({
        ...wallet,
        status: 'selling',
        lastError: null,
      })
      : wallet
  ));

  await persistMagicBundleSnapshot(userId, order.id, {
    ...snapshot,
    splitWallets: preppedWallets,
  }, () => ({
    status: 'selling',
    lastTriggerReason: reason,
    lastError: null,
  }));

  const sellOrder = await fetchJupiterSwapOrderFor({
    inputMint: order.mintAddress,
    outputMint: SOL_MINT_ADDRESS,
    amount: sellAmountRaw.toString(),
    taker: targetWallet.address,
  });
  const sellResult = await executeJupiterSwapFor(sellOrder, signer);
  const grossSellLamports = Number(sellResult.outputAmountResult ?? sellOrder.outAmount ?? 0);
  const handlingFeeLamports = calculateHandlingFeeLamports(grossSellLamports);
  if (handlingFeeLamports > 0) {
    await sendTradingHandlingFee(signer, handlingFeeLamports);
  }
  const refreshedSnapshot = await refreshMagicBundleExecutionSnapshot({
    ...order,
    creatorAddress: snapshot.creatorAddress ?? order.creatorAddress ?? null,
  });

  await persistMagicBundleSnapshot(userId, order.id, {
    ...refreshedSnapshot,
    splitWallets: refreshedSnapshot.splitWallets.map((wallet, index) => (
      index === walletIndex
        ? normalizeMagicBundleWalletRecord({
          ...wallet,
          costBasisLamports: null,
          highestValueLamports: null,
          lastActionAt: new Date().toISOString(),
          lastSellSignature: sellResult.signature ?? null,
          lastTriggerReason: reason,
          status: BigInt(wallet.currentTokenAmountRaw || '0') > 0n ? 'monitoring' : 'idle',
          lastError: null,
        })
        : wallet
    )),
  }, (draft) => {
    const stats = normalizeMagicBundleStats(draft.stats);
    const sellLamports = grossSellLamports;
    return {
      status: refreshedSnapshot.totalTokenRaw > 0n ? 'running' : 'waiting_inventory',
      stats: {
        ...stats,
        triggerCount: stats.triggerCount + 1,
        sellCount: stats.sellCount + 1,
        stopLossCount: reason === 'stop_loss' ? stats.stopLossCount + 1 : stats.stopLossCount,
        takeProfitCount: reason === 'take_profit' ? stats.takeProfitCount + 1 : stats.takeProfitCount,
        trailingStopCount: reason === 'trailing_stop' ? stats.trailingStopCount + 1 : stats.trailingStopCount,
        devSellCount: reason === 'dev_sell' ? stats.devSellCount + 1 : stats.devSellCount,
        totalSellLamports: stats.totalSellLamports + sellLamports,
        totalFeeLamports: stats.totalFeeLamports + handlingFeeLamports,
        lastSellSignature: sellResult.signature ?? null,
        lastTriggerReason: reason,
      },
      lastActionAt: new Date().toISOString(),
      lastTriggerReason: reason,
      lastError: null,
    };
  });

  await appendUserActivityLog(userId, {
    scope: `magic_bundle:${order.id}`,
    level: 'info',
    message: `Magic Bundle wallet #${walletIndex + 1} sold its position because ${reason.replace(/_/g, ' ')} was triggered. Handling fee applied: ${formatSolAmountFromLamports(handlingFeeLamports)} SOL.`,
  });

  return refreshedSnapshot;
}

async function executeMagicBundleDipBuy(userId, order, snapshot, walletIndex, spendLamports) {
  const targetWallet = normalizeMagicBundleWalletRecord(snapshot.splitWallets[walletIndex]);
  if (!targetWallet.address || !targetWallet.secretKeyB64 || spendLamports <= 0) {
    return snapshot;
  }

  const signer = decodeOrderWallet(targetWallet.secretKeyB64);
  if (signer.publicKey.toBase58() !== targetWallet.address) {
    throw new Error(`Magic Bundle wallet #${walletIndex + 1} secret does not match its wallet address.`);
  }

  const existingCostBasis = Number.isInteger(targetWallet.costBasisLamports)
    ? targetWallet.costBasisLamports
    : (targetWallet.currentPositionValueLamports || 0);
  const existingHighWater = Number.isInteger(targetWallet.highestValueLamports)
    ? targetWallet.highestValueLamports
    : (targetWallet.currentPositionValueLamports || 0);

  const preppedWallets = snapshot.splitWallets.map((wallet, index) => (
    index === walletIndex
      ? normalizeMagicBundleWalletRecord({
        ...wallet,
        status: 'buying_dip',
        lastError: null,
      })
      : wallet
  ));

  await persistMagicBundleSnapshot(userId, order.id, {
    ...snapshot,
    splitWallets: preppedWallets,
  }, () => ({
    status: 'buying_dip',
    lastTriggerReason: 'buy_dip',
    lastError: null,
  }));

  const handlingFeeLamports = calculateHandlingFeeLamports(spendLamports);
  const netSpendLamports = spendLamports - handlingFeeLamports;
  if (netSpendLamports <= 0) {
    throw new Error('Bundle buy amount is too small after the handling fee.');
  }
  if (handlingFeeLamports > 0) {
    await sendTradingHandlingFee(signer, handlingFeeLamports);
  }

  const buyOrder = await fetchJupiterSwapOrderFor({
    inputMint: SOL_MINT_ADDRESS,
    outputMint: order.mintAddress,
    amount: netSpendLamports,
    taker: targetWallet.address,
  });
  const buyResult = await executeJupiterSwapFor(buyOrder, signer);
  const refreshedSnapshot = await refreshMagicBundleExecutionSnapshot({
    ...order,
    creatorAddress: snapshot.creatorAddress ?? order.creatorAddress ?? null,
  });

  await persistMagicBundleSnapshot(userId, order.id, {
    ...refreshedSnapshot,
    splitWallets: refreshedSnapshot.splitWallets.map((wallet, index) => (
      index === walletIndex
        ? normalizeMagicBundleWalletRecord({
          ...wallet,
          costBasisLamports: existingCostBasis + spendLamports,
          highestValueLamports: Math.max(existingHighWater, wallet.currentPositionValueLamports || 0),
          buyDipCount: (targetWallet.buyDipCount || 0) + 1,
          lastActionAt: new Date().toISOString(),
          lastBuySignature: buyResult.signature ?? null,
          lastTriggerReason: 'buy_dip',
          status: BigInt(wallet.currentTokenAmountRaw || '0') > 0n ? 'monitoring' : 'idle',
          lastError: null,
        })
        : wallet
    )),
  }, (draft) => {
    const stats = normalizeMagicBundleStats(draft.stats);
    return {
      status: refreshedSnapshot.totalTokenRaw > 0n ? 'running' : 'waiting_inventory',
      stats: {
        ...stats,
        triggerCount: stats.triggerCount + 1,
        buyCount: stats.buyCount + 1,
        dipBuyCount: stats.dipBuyCount + 1,
        totalBuyLamports: stats.totalBuyLamports + netSpendLamports,
        totalFeeLamports: stats.totalFeeLamports + handlingFeeLamports,
        lastBuySignature: buyResult.signature ?? null,
        lastTriggerReason: 'buy_dip',
      },
      lastActionAt: new Date().toISOString(),
      lastTriggerReason: 'buy_dip',
      lastError: null,
    };
  });

  await appendUserActivityLog(userId, {
    scope: `magic_bundle:${order.id}`,
    level: 'info',
    message: `Magic Bundle wallet #${walletIndex + 1} bought the dip with ${formatSolAmountFromLamports(netSpendLamports)} SOL after a ${formatSolAmountFromLamports(handlingFeeLamports)} SOL handling fee.`,
  });

  return refreshedSnapshot;
}

function chooseMagicSellWalletIndex(wallets = []) {
  const candidates = wallets
    .map((wallet, index) => ({ wallet, index }))
    .filter(({ wallet }) => wallet?.address && wallet?.secretKeyB64);
  if (candidates.length === 0) {
    return null;
  }

  candidates.sort((left, right) => {
    const leftCount = left.wallet.sellCount || 0;
    const rightCount = right.wallet.sellCount || 0;
    if (leftCount !== rightCount) {
      return leftCount - rightCount;
    }

    const leftTime = left.wallet.lastUsedAt ? new Date(left.wallet.lastUsedAt).getTime() : 0;
    const rightTime = right.wallet.lastUsedAt ? new Date(right.wallet.lastUsedAt).getTime() : 0;
    return leftTime - rightTime;
  });

  const lowestCount = candidates[0].wallet.sellCount || 0;
  const lowestTier = candidates.filter(({ wallet }) => (wallet.sellCount || 0) === lowestCount);
  return lowestTier[Math.floor(Math.random() * lowestTier.length)].index;
}

function buildAppleBoosterMarketEstimatePatch(booster, totalManagedLamports, marketInfo) {
  const averageSwapLamports = Number.isInteger(booster.minSwapLamports) && Number.isInteger(booster.maxSwapLamports)
    ? Math.round((booster.minSwapLamports + booster.maxSwapLamports) / 2)
    : null;
  const totalFeeBps = [marketInfo.lpFeeBps, marketInfo.protocolFeeBps, marketInfo.creatorFeeBps]
    .filter((value) => Number.isInteger(value))
    .reduce((sum, value) => sum + value, 0);
  const estimatedTradeFeeLamports = Number.isInteger(averageSwapLamports)
    ? Math.ceil((averageSwapLamports * totalFeeBps * 2) / 10_000)
    : null;
  const estimatedNetworkFeeLamports = APPLE_BOOSTER_NETWORK_FEE_LAMPORTS;
  const estimatedCycleFeeLamports = Number.isInteger(estimatedTradeFeeLamports)
    ? estimatedTradeFeeLamports + estimatedNetworkFeeLamports
    : null;
  const estimateBase = {
    ...booster,
    estimatedCycleFeeLamports,
  };
  const runtimeEstimate = estimateAppleBoosterRuntimeFromSnapshot(estimateBase, totalManagedLamports);

  return {
    ...marketInfo,
    estimatedTradeFeeLamports,
    estimatedNetworkFeeLamports,
    estimatedCycleFeeLamports,
    estimatedCyclesRemaining: runtimeEstimate.estimatedCyclesRemaining,
    estimatedRuntimeSeconds: runtimeEstimate.estimatedRuntimeSeconds,
  };
}

function calculateAppleBoosterVolumeProgressUsd(order, solUsdRate) {
  const pkg = getOrganicVolumePackage(order?.packageKey);
  const packageTargetUsd = parseOrganicPackageTargetUsd(pkg?.label);
  const approximateVolumeLamports = (order?.appleBooster?.totalBuyInputLamports || 0)
    + (order?.appleBooster?.totalSellOutputLamports || 0);
  const approximateVolumeUsd = Number.isFinite(solUsdRate) && solUsdRate > 0
    ? (approximateVolumeLamports / LAMPORTS_PER_SOL) * solUsdRate
    : null;

  return {
    packageTargetUsd,
    approximateVolumeLamports,
    approximateVolumeUsd,
  };
}

function shouldAutoCompleteAppleBooster(order, solUsdRate) {
  if (!order?.running || order?.appleBooster?.stopRequested) {
    return false;
  }

  const { packageTargetUsd, approximateVolumeUsd } = calculateAppleBoosterVolumeProgressUsd(order, solUsdRate);
  return Boolean(
    Number.isFinite(packageTargetUsd)
    && packageTargetUsd > 0
    && Number.isFinite(approximateVolumeUsd)
    && approximateVolumeUsd >= packageTargetUsd,
  );
}

function organicWorkerCanAct(worker) {
  const nextActionAt = worker?.nextActionAt ? new Date(worker.nextActionAt).getTime() : 0;
  return !nextActionAt || Date.now() >= nextActionAt;
}

function organicBoosterWorkerPatch(workers, workerIndex, updater) {
  return workers.map((worker, index) => (
    index === workerIndex ? normalizeAppleBoosterWorkerWallet(updater(structuredClone(worker))) : worker
  ));
}

async function refreshOrganicWalletSnapshot(order) {
  const depositSigner = decodeOrderWallet(order.walletSecretKeyB64);
  if (depositSigner.publicKey.toBase58() !== order.walletAddress) {
    throw new Error('Apple Booster deposit wallet secret does not match the stored wallet address.');
  }

  const depositLamports = await connection.getBalance(depositSigner.publicKey, 'confirmed');
  const workerWallets = await Promise.all(
    order.appleBooster.workerWallets.map(async (worker) => {
      const lamports = worker.address
        ? await connection.getBalance(new PublicKey(worker.address), 'confirmed')
        : 0;
      return normalizeAppleBoosterWorkerWallet({
        ...worker,
        currentLamports: lamports,
        currentSol: formatSolAmountFromLamports(lamports),
      });
    }),
  );

  const totalManagedLamports = depositLamports + workerWallets.reduce(
    (sum, worker) => sum + worker.currentLamports,
    0,
  );

  return {
    depositSigner,
    depositLamports,
    workerWallets,
    totalManagedLamports,
  };
}

async function persistOrganicWalletSnapshot(userId, orderId, snapshot, patchBooster = () => ({})) {
  return updateAppleBooster(userId, orderId, (draft) => ({
    ...draft,
    currentLamports: snapshot.depositLamports,
    currentSol: formatSolAmountFromLamports(snapshot.depositLamports),
    appleBooster: (() => {
      const nextBooster = {
        ...draft.appleBooster,
        workerWallets: snapshot.workerWallets,
        totalManagedLamports: snapshot.totalManagedLamports,
        ...patchBooster(draft.appleBooster),
      };
      const runtimeEstimate = estimateAppleBoosterRuntimeFromSnapshot(
        nextBooster,
        snapshot.totalManagedLamports,
      );
      return {
        ...nextBooster,
        estimatedCyclesRemaining: runtimeEstimate.estimatedCyclesRemaining,
        estimatedRuntimeSeconds: runtimeEstimate.estimatedRuntimeSeconds,
      };
    })(),
  }));
}

async function transferLamportsBetweenWallets(signer, destinationAddress, lamports) {
  if (!Number.isInteger(lamports) || lamports <= 0) {
    return null;
  }

  return sendLegacyTransaction([
    SystemProgram.transfer({
      fromPubkey: signer.publicKey,
      toPubkey: new PublicKey(destinationAddress),
      lamports,
    }),
  ], signer);
}

async function finalizeBundledBoosterStop(userId, order, snapshot) {
  const shouldSendRemainderToTreasury = order.appleBooster.status === 'target_reached';

  if (shouldSendRemainderToTreasury) {
    const treasuryDestination = order.treasuryWalletAddress || cfg.treasuryWalletAddress;
    if (!treasuryDestination) {
      throw new Error('Bundled Apple Booster completion requires a treasury wallet address.');
    }

    const treasurySweepLamports = Math.max(0, snapshot.depositLamports - APPLE_BOOSTER_SWEEP_RESERVE_LAMPORTS);
    if (treasurySweepLamports > 0) {
      await transferLamportsBetweenWallets(
        snapshot.depositSigner,
        treasuryDestination,
        treasurySweepLamports,
      );
      snapshot = await refreshOrganicWalletSnapshot(order);
      await appendUserActivityLog(userId, {
        scope: organicOrderScope(order.id),
        level: 'info',
        message: `Bundled Apple Booster sent the remaining ${formatSolAmountFromLamports(treasurySweepLamports)} SOL to treasury after hitting the package target.`,
      });
    }
  }

  await persistOrganicWalletSnapshot(userId, order.id, snapshot, () => ({
    status: shouldSendRemainderToTreasury ? 'archived' : 'stopped',
    stopRequested: false,
  }));
  await updateAppleBooster(userId, order.id, (draft) => ({
    ...draft,
    running: false,
    archivedAt: shouldSendRemainderToTreasury ? new Date().toISOString() : null,
    appleBooster: {
      ...draft.appleBooster,
      status: shouldSendRemainderToTreasury ? 'archived' : 'stopped',
      stopRequested: false,
      nextActionAt: null,
      lastError: null,
    },
  }));
  await appendUserActivityLog(userId, {
    scope: organicOrderScope(order.id),
    level: 'info',
    message: shouldSendRemainderToTreasury
      ? 'Bundled Apple Booster completion finished. Remaining deposit-wallet SOL was sent to treasury and the booster was auto-archived.'
      : 'Bundled Apple Booster stop completed.',
  });
}

async function processBundledBoosterCycle(userId, order, snapshot, marketEstimatePatch, solUsdRate) {
  const booster = normalizeOrganicOrder({
    ...order,
    currentLamports: snapshot.depositLamports,
    currentSol: formatSolAmountFromLamports(snapshot.depositLamports),
    appleBooster: {
      ...order.appleBooster,
      ...marketEstimatePatch,
      workerWallets: [],
      totalManagedLamports: snapshot.totalManagedLamports,
    },
  }).appleBooster;

  if (shouldAutoCompleteAppleBooster({ ...order, appleBooster: booster }, solUsdRate)) {
    const { packageTargetUsd, approximateVolumeUsd } = calculateAppleBoosterVolumeProgressUsd({
      ...order,
      appleBooster: booster,
    }, solUsdRate);
    await updateAppleBooster(userId, order.id, (draft) => ({
      ...draft,
      running: true,
      appleBooster: {
        ...draft.appleBooster,
        stopRequested: true,
        status: 'target_reached',
        nextActionAt: null,
        lastError: null,
      },
    }));
    await appendUserActivityLog(userId, {
      scope: organicOrderScope(order.id),
      level: 'info',
      message: `Bundled Apple Booster reached its ${Math.round(packageTargetUsd).toLocaleString('en-US')} USD package target at about ${Math.round(approximateVolumeUsd || 0).toLocaleString('en-US')} USD. Auto-stopping now.`,
    });
    return true;
  }

  if (order.appleBooster.stopRequested) {
    await finalizeBundledBoosterStop(userId, order, snapshot);
    return true;
  }

  const availableLamports = Math.max(
    0,
    snapshot.depositLamports - APPLE_BOOSTER_FEE_RESERVE_LAMPORTS - BUNDLED_JITO_TIP_LAMPORTS,
  );
  const desiredLamports = randomIntegerBetween(
    booster.minSwapLamports,
    booster.maxSwapLamports,
  );

  if (availableLamports < desiredLamports) {
    await persistOrganicWalletSnapshot(userId, order.id, snapshot, (draftBooster) => ({
      status: 'waiting_funds',
      nextActionAt: getNextOrganicBoosterActionAt(draftBooster),
      lastError: null,
    }));
    return true;
  }

  const buyOrder = await fetchJupiterSwapOrderFor({
    inputMint: SOL_MINT_ADDRESS,
    outputMint: booster.mintAddress,
    amount: desiredLamports,
    taker: snapshot.depositSigner.publicKey.toBase58(),
  });
  const buyOutAmount = BigInt(buyOrder.outAmount || '0');
  if (buyOutAmount <= 0n) {
    throw new Error('Bundled Apple Booster buy quote returned zero output.');
  }

  const sellOrder = await fetchJupiterSwapOrderFor({
    inputMint: booster.mintAddress,
    outputMint: SOL_MINT_ADDRESS,
    amount: buyOutAmount.toString(),
    taker: snapshot.depositSigner.publicKey.toBase58(),
  });

  const buyTransaction = signJupiterTransaction(buyOrder, snapshot.depositSigner);
  const sellTransaction = signJupiterTransaction(sellOrder, snapshot.depositSigner);
  const tipAccounts = await getJitoTipAccounts();
  const tipTransaction = await createTipTransaction(
    snapshot.depositSigner,
    chooseRandomTipAccount(tipAccounts),
    BUNDLED_JITO_TIP_LAMPORTS,
  );

  await persistOrganicWalletSnapshot(userId, order.id, snapshot, () => ({
    status: 'bundling',
    nextActionAt: null,
    lastError: null,
  }));

  const bundleId = await sendJitoBundle([buyTransaction, sellTransaction, tipTransaction]);
  await waitForJitoBundleLanded(bundleId);

  const refreshedSnapshot = await refreshOrganicWalletSnapshot(order);
  const nextActionAt = getNextOrganicBoosterActionAt(booster);
  await persistOrganicWalletSnapshot(userId, order.id, refreshedSnapshot, (draftBooster) => ({
    status: 'running',
    nextActionAt,
    lastActionAt: new Date().toISOString(),
    lastBuyInputLamports: desiredLamports,
    lastBuyOutputAmount: buyOutAmount.toString(),
    lastSellInputAmount: buyOutAmount.toString(),
    lastSellOutputLamports: String(sellOrder.outAmount ?? '0'),
    lastBuySignature: bundleId,
    lastSellSignature: bundleId,
    totalBuyCount: (draftBooster.totalBuyCount || 0) + 1,
    totalSellCount: (draftBooster.totalSellCount || 0) + 1,
    totalBuyInputLamports: (draftBooster.totalBuyInputLamports || 0) + desiredLamports,
    totalSellOutputLamports: (draftBooster.totalSellOutputLamports || 0)
      + Number(sellOrder.outAmount ?? 0),
    cycleCount: (draftBooster.cycleCount || 0) + 1,
    lastError: null,
  }));
  await appendUserActivityLog(userId, {
    scope: organicOrderScope(order.id),
    level: 'info',
    message: `Bundled Apple Booster landed Jito bundle ${bundleId} for ${formatSolAmountFromLamports(desiredLamports)} SOL.`,
  });
  return true;
}

function isFomoBoosterConfigured(order) {
  return Boolean(
    order?.walletAddress
    && order?.walletSecretKeyB64
    && order?.mintAddress
    && Number.isInteger(order?.walletCount)
    && order.walletCount >= 3
    && Array.isArray(order?.workerWallets)
    && order.workerWallets.length === order.walletCount
    && order.workerWallets.every((wallet) => wallet?.address && wallet?.secretKeyB64)
    && Number.isInteger(order?.minBuyLamports)
    && Number.isInteger(order?.maxBuyLamports)
    && order.maxBuyLamports >= order.minBuyLamports
    && order.minBuyLamports > 0
    && Number.isInteger(order?.minIntervalSeconds)
    && Number.isInteger(order?.maxIntervalSeconds)
    && order.maxIntervalSeconds >= order.minIntervalSeconds
    && order.minIntervalSeconds > 0
  );
}

function fomoBoosterCanAct(order) {
  const nextActionAt = order?.nextActionAt ? new Date(order.nextActionAt).getTime() : 0;
  return !nextActionAt || Date.now() >= nextActionAt;
}

function getNextFomoBoosterActionAt(order) {
  const intervalSeconds = randomIntegerBetween(
    order.minIntervalSeconds,
    order.maxIntervalSeconds,
  );
  return new Date(Date.now() + (intervalSeconds * 1000)).toISOString();
}

function calculateFomoCurrentMarketCapUsd(marketInfo, solUsdRate) {
  return calculateMagicSellCurrentMarketCapUsd(marketInfo, solUsdRate);
}

function fomoBoosterScope(orderId) {
  return `fomo_booster:${orderId}`;
}

function chooseFomoBuyerWalletIndices(wallets = [], excludedIndices = new Set(), count = 2) {
  const available = wallets
    .map((wallet, index) => ({ wallet: normalizeFomoWorkerWalletRecord(wallet), index }))
    .filter(({ wallet, index }) => wallet.address && wallet.secretKeyB64 && !excludedIndices.has(index));
  const chosen = [];

  while (chosen.length < count && available.length > 0) {
    available.sort((left, right) => {
      const leftCount = left.wallet.buyCount || 0;
      const rightCount = right.wallet.buyCount || 0;
      if (leftCount !== rightCount) {
        return leftCount - rightCount;
      }

      const leftTime = left.wallet.lastUsedAt ? new Date(left.wallet.lastUsedAt).getTime() : 0;
      const rightTime = right.wallet.lastUsedAt ? new Date(right.wallet.lastUsedAt).getTime() : 0;
      return leftTime - rightTime;
    });

    const lowestCount = available[0].wallet.buyCount || 0;
    const tier = available.filter(({ wallet }) => (wallet.buyCount || 0) === lowestCount);
    const selected = tier[Math.floor(Math.random() * tier.length)];
    chosen.push(selected.index);
    available.splice(available.findIndex((item) => item.index === selected.index), 1);
  }

  return chosen.length === count ? chosen : [];
}

function chooseFomoSellerWalletIndex(wallets = [], requiredTokenRaw = 0n, excludedIndices = new Set()) {
  const candidates = wallets
    .map((wallet, index) => ({ wallet: normalizeFomoWorkerWalletRecord(wallet), index }))
    .filter(({ wallet, index }) => (
      wallet.address
      && wallet.secretKeyB64
      && !excludedIndices.has(index)
      && BigInt(wallet.currentTokenAmountRaw || '0') >= requiredTokenRaw
    ));

  if (candidates.length === 0) {
    return null;
  }

  candidates.sort((left, right) => {
    const leftCount = left.wallet.sellCount || 0;
    const rightCount = right.wallet.sellCount || 0;
    if (leftCount !== rightCount) {
      return leftCount - rightCount;
    }

    const leftTime = left.wallet.lastUsedAt ? new Date(left.wallet.lastUsedAt).getTime() : 0;
    const rightTime = right.wallet.lastUsedAt ? new Date(right.wallet.lastUsedAt).getTime() : 0;
    return leftTime - rightTime;
  });

  const lowestCount = candidates[0].wallet.sellCount || 0;
  const tier = candidates.filter(({ wallet }) => (wallet.sellCount || 0) === lowestCount);
  return tier[Math.floor(Math.random() * tier.length)].index;
}

function chooseFomoSeedWalletIndex(wallets = []) {
  const candidates = wallets
    .map((wallet, index) => ({ wallet: normalizeFomoWorkerWalletRecord(wallet), index }))
    .filter(({ wallet }) => wallet.address && wallet.secretKeyB64);
  if (candidates.length === 0) {
    return null;
  }

  candidates.sort((left, right) => {
    const leftBuyCount = left.wallet.buyCount || 0;
    const rightBuyCount = right.wallet.buyCount || 0;
    if (leftBuyCount !== rightBuyCount) {
      return leftBuyCount - rightBuyCount;
    }

    const leftToken = BigInt(left.wallet.currentTokenAmountRaw || '0');
    const rightToken = BigInt(right.wallet.currentTokenAmountRaw || '0');
    if (leftToken !== rightToken) {
      return leftToken < rightToken ? -1 : 1;
    }

    return Math.random() < 0.5 ? -1 : 1;
  });

  return candidates[0].index;
}

async function refreshFomoBoosterSnapshot(order, solUsdRate) {
  const depositSigner = decodeOrderWallet(order.walletSecretKeyB64);
  if (depositSigner.publicKey.toBase58() !== order.walletAddress) {
    throw new Error('FOMO Booster deposit wallet secret does not match the stored wallet address.');
  }

  const mintMetadata = await getMintMetadata(order.mintAddress);
  const depositLamports = await connection.getBalance(depositSigner.publicKey, 'confirmed');
  const depositTokenRaw = await getOwnedMintRawBalance(depositSigner.publicKey, order.mintAddress);
  const workerWallets = await Promise.all(
    order.workerWallets.map(async (wallet) => {
      if (!wallet?.address) {
        return normalizeFomoWorkerWalletRecord(wallet);
      }

      const lamports = await connection.getBalance(new PublicKey(wallet.address), 'confirmed');
      const tokenRaw = await getOwnedMintRawBalance(new PublicKey(wallet.address), order.mintAddress);
      return normalizeFomoWorkerWalletRecord({
        ...wallet,
        currentLamports: lamports,
        currentSol: formatSolAmountFromLamports(lamports),
        currentTokenAmountRaw: tokenRaw.toString(),
        currentTokenAmountDisplay: formatTokenAmountFromRaw(tokenRaw.toString(), mintMetadata.decimals),
      });
    }),
  );
  const totalManagedLamports = depositLamports + workerWallets.reduce(
    (sum, wallet) => sum + (Number.isInteger(wallet.currentLamports) ? wallet.currentLamports : 0),
    0,
  );
  const marketInfo = await inspectPumpMintMarket(order.mintAddress, depositSigner.publicKey);
  const currentMarketCapUsd = calculateFomoCurrentMarketCapUsd(marketInfo, solUsdRate);

  return {
    depositSigner,
    mintMetadata,
    depositLamports,
    depositTokenRaw,
    workerWallets,
    totalManagedLamports,
    marketInfo,
    currentMarketCapUsd,
  };
}

async function persistFomoBoosterSnapshot(userId, snapshot, patchOrder = () => ({})) {
  return updateFomoBooster(userId, (draft) => ({
    ...draft,
    tokenDecimals: snapshot.mintMetadata.decimals,
    tokenProgram: snapshot.mintMetadata.tokenProgram.toBase58(),
    currentLamports: snapshot.depositLamports,
    currentSol: formatSolAmountFromLamports(snapshot.depositLamports),
    currentTokenAmountRaw: snapshot.depositTokenRaw.toString(),
    currentTokenAmountDisplay: formatTokenAmountFromRaw(
      snapshot.depositTokenRaw.toString(),
      snapshot.mintMetadata.decimals,
    ),
    totalManagedLamports: snapshot.totalManagedLamports,
    workerWallets: snapshot.workerWallets,
    currentMarketCapUsd: snapshot.currentMarketCapUsd,
    currentMarketCapSol: snapshot.marketInfo.marketCapSol,
    marketPhase: snapshot.marketInfo.marketPhase,
    lastBalanceCheckAt: new Date().toISOString(),
    ...patchOrder(draft),
  }));
}

async function seedFomoSellerInventory(userId, order, snapshot, desiredLamports, walletIndex) {
  const selectedWallet = normalizeFomoWorkerWalletRecord(snapshot.workerWallets[walletIndex]);
  const signer = decodeOrderWallet(selectedWallet.secretKeyB64);
  if (signer.publicKey.toBase58() !== selectedWallet.address) {
    throw new Error(`FOMO Booster worker wallet #${walletIndex + 1} secret does not match its wallet address.`);
  }

  const topUpTargetLamports = desiredLamports + FOMO_WORKER_GAS_RESERVE_LAMPORTS;
  const topUpLamports = Math.max(0, topUpTargetLamports - selectedWallet.currentLamports);
  const requiredLamports = topUpLamports + FOMO_DEPOSIT_RESERVE_LAMPORTS;
  if (snapshot.depositLamports < requiredLamports) {
    await persistFomoBoosterSnapshot(userId, snapshot, (draft) => ({
      status: 'waiting_funds',
      nextActionAt: getNextFomoBoosterActionAt(draft),
      lastError: null,
    }));
    return false;
  }

  if (topUpLamports > 0) {
    await transferLamportsBetweenWallets(snapshot.depositSigner, selectedWallet.address, topUpLamports);
  }

  await updateFomoBooster(userId, (draft) => ({
    ...draft,
    status: 'bootstrapping',
    nextActionAt: null,
    lastError: null,
  }));

  const buyOrder = await fetchJupiterSwapOrderFor({
    inputMint: SOL_MINT_ADDRESS,
    outputMint: order.mintAddress,
    amount: desiredLamports,
    taker: selectedWallet.address,
  });
  const buyResult = await executeJupiterSwapFor(buyOrder, signer);
  const refreshedSnapshot = await refreshFomoBoosterSnapshot(order, await ensureSolPriceCache());

  await persistFomoBoosterSnapshot(userId, refreshedSnapshot, (draft) => {
    const workerWallets = refreshedSnapshot.workerWallets.map((wallet, index) => (
      index === walletIndex
        ? normalizeFomoWorkerWalletRecord({
          ...wallet,
          buyCount: (wallet.buyCount || 0) + 1,
          lastUsedAt: new Date().toISOString(),
          status: 'idle',
          lastBuySignature: buyResult.signature ?? null,
          lastError: null,
        })
        : wallet
    ));

    return {
      workerWallets,
      status: 'bootstrapped',
      nextActionAt: getNextFomoBoosterActionAt(draft),
      stats: {
        ...normalizeFomoBoosterStats(draft.stats),
        lastSeedSignature: buyResult.signature ?? null,
      },
      lastError: null,
    };
  });

  await appendUserActivityLog(userId, {
    scope: fomoBoosterScope(order.id),
    level: 'info',
    message: `FOMO Booster bootstrapped worker #${walletIndex + 1} with an initial ${formatSolAmountFromLamports(desiredLamports)} SOL buy.`,
  });
  return true;
}

async function scanFomoBoosters() {
  const store = await readStore();
  const solUsdRate = await ensureSolPriceCache();

  for (const [userId, user] of Object.entries(store.users ?? {})) {
    const order = normalizeUserFomoBooster(user);
    if (!hasMeaningfulFomoBoosterRecord(order) || !order.walletAddress || !order.walletSecretKeyB64 || !order.mintAddress) {
      continue;
    }

    try {
      const snapshot = await refreshFomoBoosterSnapshot(order, solUsdRate);
      const configured = isFomoBoosterConfigured(order);

      await persistFomoBoosterSnapshot(userId, snapshot, (draft) => ({
        status: draft.automationEnabled
          ? (configured ? (draft.status === 'failed' ? 'running' : draft.status) : 'setup')
          : (configured ? 'stopped' : 'setup'),
        lastError: draft.automationEnabled ? draft.lastError : null,
      }));

      if (!order.automationEnabled || !configured) {
        continue;
      }

      if (!fomoBoosterCanAct(order)) {
        continue;
      }

      const desiredLamports = randomIntegerBetween(order.minBuyLamports, order.maxBuyLamports);
      if (!Number.isInteger(desiredLamports) || desiredLamports <= 0) {
        continue;
      }

      const inventoryQuote = await fetchJupiterSwapOrderFor({
        inputMint: SOL_MINT_ADDRESS,
        outputMint: order.mintAddress,
        amount: desiredLamports,
        taker: snapshot.depositSigner.publicKey.toBase58(),
      });
      const sellTokenAmountRaw = BigInt(inventoryQuote.outAmount || '0');
      if (sellTokenAmountRaw <= 0n) {
        throw new Error('FOMO Booster inventory quote returned zero output.');
      }

      let sellerIndex = chooseFomoSellerWalletIndex(snapshot.workerWallets, sellTokenAmountRaw);
      if (!Number.isInteger(sellerIndex)) {
        const seedIndex = chooseFomoSeedWalletIndex(snapshot.workerWallets);
        if (!Number.isInteger(seedIndex)) {
          throw new Error('FOMO Booster does not have any valid worker wallets.');
        }

        const seeded = await seedFomoSellerInventory(userId, order, snapshot, desiredLamports, seedIndex);
        if (seeded) {
          continue;
        }
        continue;
      }

      const excludedBuyerIndices = new Set([sellerIndex]);
      const buyerIndices = chooseFomoBuyerWalletIndices(snapshot.workerWallets, excludedBuyerIndices, 2);
      if (buyerIndices.length !== 2) {
        throw new Error('FOMO Booster needs at least two buyer wallets and one seller wallet.');
      }

      const buyerWallets = buyerIndices.map((index) => normalizeFomoWorkerWalletRecord(snapshot.workerWallets[index]));
      const sellerWallet = normalizeFomoWorkerWalletRecord(snapshot.workerWallets[sellerIndex]);
      const buyerSigners = buyerWallets.map((wallet, position) => {
        const signer = decodeOrderWallet(wallet.secretKeyB64);
        if (signer.publicKey.toBase58() !== wallet.address) {
          throw new Error(`FOMO Booster buyer wallet #${buyerIndices[position] + 1} secret does not match its wallet address.`);
        }
        return signer;
      });
      const sellerSigner = decodeOrderWallet(sellerWallet.secretKeyB64);
      if (sellerSigner.publicKey.toBase58() !== sellerWallet.address) {
        throw new Error(`FOMO Booster seller wallet #${sellerIndex + 1} secret does not match its wallet address.`);
      }

      const requiredBuyerTopUps = buyerWallets.map((wallet) => Math.max(
        0,
        desiredLamports + FOMO_WORKER_GAS_RESERVE_LAMPORTS - wallet.currentLamports,
      ));
      const requiredSellerTopUp = Math.max(
        0,
        FOMO_WORKER_GAS_RESERVE_LAMPORTS - sellerWallet.currentLamports,
      );
      const totalPreparationLamports = requiredBuyerTopUps.reduce((sum, value) => sum + value, 0)
        + requiredSellerTopUp
        + FOMO_JITO_TIP_LAMPORTS
        + FOMO_DEPOSIT_RESERVE_LAMPORTS;
      if (snapshot.depositLamports < totalPreparationLamports) {
        await persistFomoBoosterSnapshot(userId, snapshot, (draft) => ({
          status: 'waiting_funds',
          nextActionAt: getNextFomoBoosterActionAt(draft),
          lastError: null,
        }));
        continue;
      }

      for (let index = 0; index < buyerWallets.length; index += 1) {
        if (requiredBuyerTopUps[index] > 0) {
          await transferLamportsBetweenWallets(
            snapshot.depositSigner,
            buyerWallets[index].address,
            requiredBuyerTopUps[index],
          );
        }
      }
      if (requiredSellerTopUp > 0) {
        await transferLamportsBetweenWallets(
          snapshot.depositSigner,
          sellerWallet.address,
          requiredSellerTopUp,
        );
      }

      const buyOrderA = await fetchJupiterSwapOrderFor({
        inputMint: SOL_MINT_ADDRESS,
        outputMint: order.mintAddress,
        amount: desiredLamports,
        taker: buyerWallets[0].address,
      });
      const buyOrderB = await fetchJupiterSwapOrderFor({
        inputMint: SOL_MINT_ADDRESS,
        outputMint: order.mintAddress,
        amount: desiredLamports,
        taker: buyerWallets[1].address,
      });
      const sellOrder = await fetchJupiterSwapOrderFor({
        inputMint: order.mintAddress,
        outputMint: SOL_MINT_ADDRESS,
        amount: sellTokenAmountRaw.toString(),
        taker: sellerWallet.address,
      });

      await updateFomoBooster(userId, (draft) => ({
        ...draft,
        status: 'bundling',
        nextActionAt: null,
        lastError: null,
      }));

      const buyTransactionA = signJupiterTransaction(buyOrderA, buyerSigners[0]);
      const buyTransactionB = signJupiterTransaction(buyOrderB, buyerSigners[1]);
      const sellTransaction = signJupiterTransaction(sellOrder, sellerSigner);
      const tipAccounts = await getJitoTipAccounts();
      const tipTransaction = await createTipTransaction(
        snapshot.depositSigner,
        chooseRandomTipAccount(tipAccounts),
        FOMO_JITO_TIP_LAMPORTS,
      );

      const bundleId = await sendJitoBundle([
        buyTransactionA,
        buyTransactionB,
        sellTransaction,
        tipTransaction,
      ]);
      await waitForJitoBundleLanded(bundleId);

      const refreshedSnapshot = await refreshFomoBoosterSnapshot(order, solUsdRate);
      await persistFomoBoosterSnapshot(userId, refreshedSnapshot, (draft) => {
        const stats = normalizeFomoBoosterStats(draft.stats);
        const workerWallets = refreshedSnapshot.workerWallets.map((wallet, index) => {
          if (index === buyerIndices[0] || index === buyerIndices[1]) {
            return normalizeFomoWorkerWalletRecord({
              ...wallet,
              buyCount: (wallet.buyCount || 0) + 1,
              lastUsedAt: new Date().toISOString(),
              status: 'idle',
              lastBuySignature: bundleId,
              lastError: null,
            });
          }

          if (index === sellerIndex) {
            return normalizeFomoWorkerWalletRecord({
              ...wallet,
              sellCount: (wallet.sellCount || 0) + 1,
              lastUsedAt: new Date().toISOString(),
              status: 'idle',
              lastSellSignature: bundleId,
              lastError: null,
            });
          }

          return wallet;
        });

        return {
          workerWallets,
          status: 'running',
          stats: {
            ...stats,
            bundleCount: stats.bundleCount + 1,
            buyCount: stats.buyCount + 2,
            sellCount: stats.sellCount + 1,
            totalBuyLamports: stats.totalBuyLamports + (desiredLamports * 2),
            totalSellLamports: stats.totalSellLamports + Number(sellOrder.outAmount ?? 0),
            lastBuySignature: bundleId,
            lastSellSignature: bundleId,
          },
          lastBundleId: bundleId,
          lastBundleAt: new Date().toISOString(),
          nextActionAt: getNextFomoBoosterActionAt(draft),
          lastError: null,
        };
      });

      await appendUserActivityLog(userId, {
        scope: fomoBoosterScope(order.id),
        level: 'info',
        message: `FOMO Booster landed bundle ${bundleId}: 2 buys of ${formatSolAmountFromLamports(desiredLamports)} SOL and 1 sell from worker #${sellerIndex + 1}.`,
      });
    } catch (error) {
      await updateFomoBooster(userId, (draft) => ({
        ...draft,
        status: 'failed',
        lastBalanceCheckAt: new Date().toISOString(),
        lastError: String(error.message || error),
      }));
      await appendUserActivityLog(userId, {
        scope: fomoBoosterScope(order.id),
        level: 'error',
        message: `FOMO Booster error: ${String(error.message || error)}`,
      });
      console.error(`[worker] FOMO Booster failed for user ${userId}:`, error.message || error);
    }
  }
}

function formatLamportsAsSolNumber(lamports) {
  return Number((Math.max(0, lamports) / LAMPORTS_PER_SOL).toFixed(9));
}

function estimateMagicBundleFees(balanceLamports, walletCount, bundleMode = 'stealth') {
  const safeBalanceLamports = Number.isInteger(balanceLamports) ? Math.max(0, balanceLamports) : 0;
  const safeWalletCount = Number.isInteger(walletCount) ? Math.max(1, walletCount) : 1;
  const platformFeeLamports = bundleMode === 'standard'
    ? 0
    : MAGIC_BUNDLE_STEALTH_SETUP_FEE_LAMPORTS;
  const splitNowFeeLamports = bundleMode === 'standard'
    ? 0
    : Math.floor(safeBalanceLamports * (MAGIC_BUNDLE_SPLITNOW_FEE_ESTIMATE_BPS / 10_000));
  const reserveLamports = MAGIC_BUNDLE_FEE_RESERVE_LAMPORTS + (safeWalletCount * 5_000);
  const netSplitLamports = Math.max(0, safeBalanceLamports - platformFeeLamports - splitNowFeeLamports - reserveLamports);
  return {
    platformFeeLamports,
    splitNowFeeLamports,
    netSplitLamports,
    reserveLamports,
  };
}

function estimateSniperWizardFees(balanceLamports, walletCount, sniperMode = 'standard', setupFeePaidAt = null) {
  const safeBalanceLamports = Number.isInteger(balanceLamports) ? Math.max(0, balanceLamports) : 0;
  const safeWalletCount = Number.isInteger(walletCount)
    ? Math.max(1, Math.min(SNIPER_MAX_WALLET_COUNT, walletCount))
    : SNIPER_DEFAULT_WALLET_COUNT;
  const platformFeeLamports = sniperMode === 'magic' && !setupFeePaidAt
    ? SNIPER_MAGIC_SETUP_FEE_LAMPORTS
    : 0;
  const splitNowFeeLamports = sniperMode === 'magic'
    ? Math.floor(safeBalanceLamports * (MAGIC_BUNDLE_SPLITNOW_FEE_ESTIMATE_BPS / 10_000))
    : 0;
  const reserveLamports = SNIPER_GAS_RESERVE_LAMPORTS + (safeWalletCount * 5_000);
  const netSplitLamports = Math.max(
    0,
    safeBalanceLamports - platformFeeLamports - splitNowFeeLamports - reserveLamports,
  );
  return {
    platformFeeLamports,
    splitNowFeeLamports,
    netSplitLamports,
    reserveLamports,
  };
}

function calculateHandlingFeeLamports(lamports) {
  if (!Number.isInteger(lamports) || lamports <= 0) {
    return 0;
  }
  return Math.floor(lamports * (TRADING_HANDLING_FEE_BPS / 10_000));
}

async function callSplitNowApi(pathname, { method = 'GET', body = null } = {}) {
  if (!SPLITNOW_API_KEY) {
    throw new Error('Stealth routing is not configured right now.');
  }

  const response = await fetch(`${SPLITNOW_API_BASE_URL.replace(/\/+$/, '')}${pathname}`, {
    method,
    headers: {
      'x-api-key': SPLITNOW_API_KEY,
      'Content-Type': 'application/json',
      Accept: 'application/json',
      'User-Agent': 'steel-tester-worker/1.0',
    },
    body: body ? JSON.stringify(body) : undefined,
  });

  let payload = null;
  try {
    payload = await response.json();
  } catch {
    payload = null;
  }

  if (!response.ok) {
    throw new Error(payload?.message || payload?.error || `Stealth routing failed with status ${response.status}.`);
  }

  return payload?.data ?? payload;
}

async function createSplitNowQuoteData(fromAmountLamports) {
  const fromAmount = formatLamportsAsSolNumber(fromAmountLamports);
  const quoteId = await callSplitNowApi('/quotes/', {
    method: 'POST',
    body: {
      type: 'floating_rate',
      quoteInput: {
        fromAmount: fromAmount,
        fromAssetId: 'sol',
        fromNetworkId: 'solana',
      },
      quoteOutputs: [
        {
          toPctBips: 10000,
          toAssetId: 'sol',
          toNetworkId: 'solana',
        },
      ],
    },
  });

  const quote = await callSplitNowApi(`/quotes/${quoteId}`);
  return {
    quoteId: typeof quoteId === 'string' ? quoteId : quote?._id,
    quote,
  };
}

function pickSplitNowExchangerId(quote) {
  const quoteLegs = Array.isArray(quote?.quoteLegs) ? quote.quoteLegs : [];
  if (quoteLegs.length === 0) {
    throw new Error('Stealth routing did not return an available route.');
  }

  const bestLeg = quoteLegs.reduce((best, leg) => {
    if (!best) {
      return leg;
    }
    const currentAmount = Number(leg?.quoteLegOutput?.toAmount || 0);
    const bestAmount = Number(best?.quoteLegOutput?.toAmount || 0);
    return currentAmount > bestAmount ? leg : best;
  }, null);

  const exchangerId = bestLeg?.quoteLegOutput?.toExchangerId;
  if (!exchangerId) {
    throw new Error('Stealth routing did not return a valid route id.');
  }

  return exchangerId;
}

function buildMagicBundleOrderOutputs(order, exchangerId) {
  const wallets = Array.isArray(order.splitWallets) ? order.splitWallets.filter((wallet) => wallet?.address) : [];
  if (wallets.length === 0) {
    throw new Error('Magic Bundle does not have any recipient wallets configured.');
  }

  const baseBips = Math.floor(10_000 / wallets.length);
  let remainingBips = 10_000 - (baseBips * wallets.length);
  return wallets.map((wallet) => {
    const extra = remainingBips > 0 ? 1 : 0;
    remainingBips = Math.max(0, remainingBips - extra);
    return {
      toAddress: wallet.address,
      toPctBips: baseBips + extra,
      toAssetId: 'sol',
      toNetworkId: 'solana',
      toExchangerId: exchangerId,
    };
  });
}

function buildWalletRoutingOutputs(wallets, exchangerId, emptyMessage) {
  const readyWallets = Array.isArray(wallets) ? wallets.filter((wallet) => wallet?.address) : [];
  if (readyWallets.length === 0) {
    throw new Error(emptyMessage);
  }

  const baseBips = Math.floor(10_000 / readyWallets.length);
  let remainingBips = 10_000 - (baseBips * readyWallets.length);
  return readyWallets.map((wallet) => {
    const extra = remainingBips > 0 ? 1 : 0;
    remainingBips = Math.max(0, remainingBips - extra);
    return {
      toAddress: wallet.address,
      toPctBips: baseBips + extra,
      toAssetId: 'sol',
      toNetworkId: 'solana',
      toExchangerId: exchangerId,
    };
  });
}

function buildLaunchBuyRoutingOutputs(order, exchangerId) {
  const wallets = Array.isArray(order.buyerWallets) ? order.buyerWallets.filter((wallet) => wallet?.address) : [];
  return buildWalletRoutingOutputs(wallets, exchangerId, 'Launch + Buy does not have any buyer wallets configured.');
}

function buildSniperWizardRoutingOutputs(order, exchangerId) {
  const wallets = Array.isArray(order.workerWallets) ? order.workerWallets.filter((wallet) => wallet?.address) : [];
  return buildWalletRoutingOutputs(wallets, exchangerId, 'Sniper Wizard does not have any sniper wallets configured.');
}

async function createSplitNowOrderData(order, amountLamports) {
  const { quoteId, quote } = await createSplitNowQuoteData(amountLamports);
  const exchangerId = pickSplitNowExchangerId(quote);
  const outputs = buildMagicBundleOrderOutputs(order, exchangerId);
  const response = await callSplitNowApi('/orders/', {
    method: 'POST',
    body: {
      type: 'floating_rate',
      quoteId,
      orderInput: {
        fromAmount: formatLamportsAsSolNumber(amountLamports),
        fromAssetId: 'sol',
        fromNetworkId: 'solana',
      },
      orderOutputs: outputs,
    },
  });

  const orderLookupId = response?.shortId || response?.orderId;
  if (!orderLookupId) {
    throw new Error('Stealth routing did not return a tracking id.');
  }

  const orderDetails = await callSplitNowApi(`/orders/${orderLookupId}`);
  return {
    quoteId,
    orderId: response?.shortId || response?.orderId,
    depositAddress: orderDetails?.depositWalletAddress || null,
    depositAmount: orderDetails?.depositAmount || formatLamportsAsSolNumber(amountLamports),
    status: orderDetails?.statusText || orderDetails?.statusShort || orderDetails?.status || 'Pending',
  };
}

async function createLaunchBuyRoutingOrderData(order, amountLamports) {
  const { quoteId, quote } = await createSplitNowQuoteData(amountLamports);
  const exchangerId = pickSplitNowExchangerId(quote);
  const outputs = buildLaunchBuyRoutingOutputs(order, exchangerId);
  const response = await callSplitNowApi('/orders/', {
    method: 'POST',
    body: {
      type: 'floating_rate',
      quoteId,
      orderInput: {
        fromAmount: formatLamportsAsSolNumber(amountLamports),
        fromAssetId: 'sol',
        fromNetworkId: 'solana',
      },
      orderOutputs: outputs,
    },
  });

  const orderLookupId = response?.shortId || response?.orderId;
  if (!orderLookupId) {
    throw new Error('Stealth routing did not return a tracking id.');
  }

  const orderDetails = await callSplitNowApi(`/orders/${orderLookupId}`);
  return {
    quoteId,
    orderId: response?.shortId || response?.orderId,
    depositAddress: orderDetails?.depositWalletAddress || null,
    depositAmount: orderDetails?.depositAmount || formatLamportsAsSolNumber(amountLamports),
    status: orderDetails?.statusText || orderDetails?.statusShort || orderDetails?.status || 'Pending',
  };
}

async function getSplitNowOrderStatus(orderId) {
  const order = await callSplitNowApi(`/orders/${orderId}`);
  return {
    statusShort: order?.statusShort || order?.status || 'pending',
    statusText: order?.statusText || order?.status || 'Pending',
    raw: order,
  };
}

async function sendMagicBundlePlatformFee(signer, lamports) {
  return routePlatformProfitFromSigner(signer, lamports, 'Magic Bundle');
}

async function sendTradingHandlingFee(signer, lamports) {
  return routePlatformProfitFromSigner(signer, lamports, 'Trading');
}

function scheduleTradingHandlingFeeTransfer(signer, lamports, sourceLabel = 'Trading') {
  if (!Number.isInteger(lamports) || lamports <= 0) {
    return;
  }

  void sendTradingHandlingFee(signer, lamports).catch((error) => {
    console.warn(
      `[worker] ${sourceLabel} handling fee transfer failed for ${signer.publicKey.toBase58()}:`,
      error?.message || error,
    );
  });
}

function calculatePlatformRevenueRoute(lamports) {
  if (lamports <= 0) {
    return {
      totalLamports: 0,
      treasuryLamports: 0,
      burnLamports: 0,
      rewardsLamports: 0,
    };
  }

  const treasuryLamports = Math.floor((lamports * cfg.platformRevenue.treasuryBps) / PLATFORM_SPLIT_BPS_DENOMINATOR);
  const burnLamports = Math.floor((lamports * cfg.platformRevenue.burnBps) / PLATFORM_SPLIT_BPS_DENOMINATOR);
  const rewardsLamports = Math.max(0, lamports - treasuryLamports - burnLamports);
  return {
    totalLamports: lamports,
    treasuryLamports,
    burnLamports,
    rewardsLamports,
  };
}

async function sendLegacyTreasuryDevSplit(signer, lamports, sourceLabel) {
  if (lamports <= 0) {
    return null;
  }
  if (!isConfiguredAddress(cfg.treasuryWalletAddress) || !isConfiguredAddress(cfg.devWalletAddress)) {
    throw new Error(`${sourceLabel} fees require TREASURY_WALLET_ADDRESS and DEV_WALLET_ADDRESS.`);
  }

  const treasuryLamports = Math.floor(lamports / 2);
  const devLamports = lamports - treasuryLamports;
  const instructions = [];
  if (treasuryLamports > 0) {
    instructions.push(SystemProgram.transfer({
      fromPubkey: signer.publicKey,
      toPubkey: new PublicKey(cfg.treasuryWalletAddress),
      lamports: treasuryLamports,
    }));
  }
  if (devLamports > 0) {
    instructions.push(SystemProgram.transfer({
      fromPubkey: signer.publicKey,
      toPubkey: new PublicKey(cfg.devWalletAddress),
      lamports: devLamports,
    }));
  }

  const signature = await sendLegacyTransaction(instructions, signer);
  console.warn(
    `[worker] ${sourceLabel} used legacy treasury/dev fallback because platform buyback routing is not fully configured.`,
  );
  return {
    mode: 'legacy_treasury_dev_fallback',
    signature,
    treasuryLamports,
    devLamports,
  };
}

async function transferSolFromSigner(signer, destinationAddress, lamports) {
  if (!Number.isInteger(lamports) || lamports <= 0) {
    return null;
  }

  return sendLegacyTransaction([
    SystemProgram.transfer({
      fromPubkey: signer.publicKey,
      toPubkey: new PublicKey(destinationAddress),
      lamports,
    }),
  ], signer);
}

async function buyPlatformTokenWithSigner(signer, lamportsRaw, options = {}) {
  const mintAddress = options.mintAddress || cfg.platformRevenue.tokenMint;
  const slippagePercent = options.slippagePercent ?? cfg.platformRevenue.buySlippagePercent;
  const mint = new PublicKey(mintAddress);
  const mintTokenProgram = await getMintTokenProgram(mintAddress);
  const tokenBalanceBefore = await getOwnedMintRawBalance(signer.publicKey, mintAddress);

  let signature = null;
  let mode = 'bonding_curve';

  const {
    bondingCurveAccountInfo,
    bondingCurve,
    associatedUserAccountInfo,
  } = await pumpOnlineSdk.fetchBuyState(
    mint,
    signer.publicKey,
    mintTokenProgram,
  );

  if (bondingCurve.complete) {
    const poolKey = canonicalPumpPoolPda(mint);
    const swapState = await pumpAmmOnlineSdk.swapSolanaState(poolKey, signer.publicKey);
    const instructions = await PUMP_AMM_SDK.buyQuoteInput(
      swapState,
      new BN(lamportsRaw),
      slippagePercent,
    );
    signature = await sendLegacyTransaction(instructions, signer);
    mode = 'pumpswap';
  } else {
    const global = await pumpOnlineSdk.fetchGlobal();
    const feeConfig = await pumpOnlineSdk.fetchFeeConfig();
    const amount = getBuyTokenAmountFromSolAmount({
      global,
      feeConfig,
      mintSupply: bondingCurve.tokenTotalSupply,
      bondingCurve,
      amount: new BN(lamportsRaw),
    });

    if (amount.lte(new BN(0))) {
      throw new Error('Platform buyback quote returned zero tokens.');
    }

    const instructions = await PUMP_SDK.buyInstructions({
      global,
      bondingCurveAccountInfo,
      bondingCurve,
      associatedUserAccountInfo,
      mint,
      user: signer.publicKey,
      amount,
      solAmount: new BN(lamportsRaw),
      slippage: slippagePercent,
      tokenProgram: mintTokenProgram,
    });
    signature = await sendLegacyTransaction(instructions, signer);
  }

  const tokenBalanceAfter = await getOwnedMintRawBalance(signer.publicKey, mintAddress);
  const purchasedRawAmount = tokenBalanceAfter > tokenBalanceBefore
    ? tokenBalanceAfter - tokenBalanceBefore
    : 0n;

  if (purchasedRawAmount <= 0n) {
    throw new Error('Platform buyback completed but no token balance increase was detected.');
  }

  return {
    signature,
    mode,
    mintAddress,
    mintTokenProgram,
    purchasedRawAmount,
  };
}

async function routePlatformProfitFromSigner(signer, lamports, sourceLabel) {
  if (!Number.isInteger(lamports) || lamports <= 0) {
    return null;
  }

  if (!isConfiguredAddress(cfg.treasuryWalletAddress)) {
    throw new Error(`${sourceLabel} routing requires TREASURY_WALLET_ADDRESS.`);
  }

  if (!cfg.platformRevenue.enabled) {
    return sendLegacyTreasuryDevSplit(signer, lamports, sourceLabel);
  }

  const route = calculatePlatformRevenueRoute(lamports);
  const summary = {
    mode: 'platform_50_25_25',
    totalLamports: route.totalLamports,
    treasuryLamports: route.treasuryLamports,
    burnLamports: route.burnLamports,
    rewardsLamports: route.rewardsLamports,
    treasurySignature: null,
    burnBuybackSignature: null,
    burnSignature: null,
    rewardsVaultSignature: null,
    burnRawAmount: '0',
  };

  if (route.treasuryLamports > 0) {
    summary.treasurySignature = await transferSolFromSigner(
      signer,
      cfg.treasuryWalletAddress,
      route.treasuryLamports,
    );
  }

  if (route.burnLamports > 0) {
    const buyback = await buyPlatformTokenWithSigner(signer, route.burnLamports);
    summary.burnBuybackSignature = buyback.signature;
    summary.burnRawAmount = buyback.purchasedRawAmount.toString();
    const burnResult = await burnMintTokens(
      signer,
      buyback.mintAddress,
      buyback.purchasedRawAmount.toString(),
    );
    summary.burnSignature = burnResult.signatures[burnResult.signatures.length - 1] ?? null;
  }

  if (route.rewardsLamports > 0) {
    summary.rewardsVaultSignature = await transferSolFromSigner(
      signer,
      cfg.platformRevenue.rewardsVaultAddress,
      route.rewardsLamports,
    );
  }

  console.log(
    `[worker] ${sourceLabel} routed ${formatSolAmountFromLamports(route.totalLamports)} SOL -> treasury ${formatSolAmountFromLamports(route.treasuryLamports)}, burn ${formatSolAmountFromLamports(route.burnLamports)}, rewards vault ${formatSolAmountFromLamports(route.rewardsLamports)}.`,
  );
  return summary;
}

function buildDirectBundleTransfers(order, totalLamports) {
  const wallets = Array.isArray(order.splitWallets) ? order.splitWallets.filter((wallet) => wallet?.address) : [];
  if (wallets.length === 0) {
    throw new Error('Bundle does not have any recipient wallets configured.');
  }

  const baseLamports = Math.floor(totalLamports / wallets.length);
  let remainder = totalLamports - (baseLamports * wallets.length);
  return wallets.map((wallet) => {
    const extra = remainder > 0 ? 1 : 0;
    remainder = Math.max(0, remainder - extra);
    return {
      address: wallet.address,
      lamports: baseLamports + extra,
    };
  }).filter((item) => item.lamports > 0);
}

async function executeDirectMagicBundleSpread(signer, order, totalLamports) {
  const transfers = buildDirectBundleTransfers(order, totalLamports);
  const instructions = transfers.map((item) => SystemProgram.transfer({
    fromPubkey: signer.publicKey,
    toPubkey: new PublicKey(item.address),
    lamports: item.lamports,
  }));
  const signature = await sendLegacyTransaction(instructions, signer);
  return {
    signature,
    transferCount: transfers.length,
  };
}

async function scanMagicBundles() {
  const store = await readStore();

  for (const [userId, user] of Object.entries(store.users ?? {})) {
    for (const order of normalizeUserMagicBundles(user)) {
      if (
        !hasMeaningfulMagicBundleRecord(order)
        || order.archivedAt
        || !order.walletAddress
        || !order.walletSecretKeyB64
        || !Number.isInteger(order.walletCount)
        || order.walletCount <= 0
      ) {
        continue;
      }

      try {
        let snapshot = await refreshMagicBundleExecutionSnapshot(order);
        const estimates = estimateMagicBundleFees(snapshot.depositLamports, order.walletCount, order.bundleMode);
        const configured = isMagicBundleAutomationConfigured(order);
        const isStealthBundle = order.bundleMode !== 'standard';
        const baseStatus = order.splitCompletedAt
          ? (
            order.automationEnabled
              ? (configured
                ? (snapshot.totalTokenRaw > 0n ? 'running' : 'waiting_inventory')
                : 'setup')
              : (configured ? 'stopped' : 'ready')
          )
          : (snapshot.depositLamports > 0 ? ((isStealthBundle && order.splitnowOrderId) ? 'splitting' : 'awaiting_deposit') : 'setup');

        await persistMagicBundleSnapshot(userId, order.id, snapshot, (draft) => ({
          estimatedPlatformFeeLamports: estimates.platformFeeLamports,
          estimatedSplitNowFeeLamports: estimates.splitNowFeeLamports,
          estimatedNetSplitLamports: estimates.netSplitLamports,
          status: draft.status === 'failed' && draft.lastError ? draft.status : baseStatus,
          lastError: draft.status === 'failed' ? draft.lastError : null,
        }));

        if (!order.splitCompletedAt) {
          if (isStealthBundle && !SPLITNOW_API_KEY) {
            continue;
          }

          if (isStealthBundle && order.splitnowOrderId) {
            const orderStatus = await getSplitNowOrderStatus(order.splitnowOrderId);
            const normalizedStatus = String(orderStatus.statusShort || '').toLowerCase();
            await updateMagicBundle(userId, order.id, (draft) => ({
              ...draft,
              splitnowStatus: orderStatus.statusText,
              status: normalizedStatus === 'completed'
                ? (draft.automationEnabled && isMagicBundleAutomationConfigured(draft) ? 'waiting_inventory' : 'ready')
                : 'splitting',
              splitCompletedAt: normalizedStatus === 'completed'
                ? (draft.splitCompletedAt || new Date().toISOString())
                : draft.splitCompletedAt,
              lastBalanceCheckAt: new Date().toISOString(),
              lastError: null,
            }));

            if (normalizedStatus === 'completed') {
              await appendUserActivityLog(userId, {
                scope: `magic_bundle:${order.id}`,
                level: 'info',
                message: `Magic Bundle split completed across ${order.walletCount} wallets.`,
              });
            }
            continue;
          }

          const sendToSplitLamports = estimates.netSplitLamports;
          if (
            sendToSplitLamports <= 0
            || snapshot.depositLamports < (estimates.platformFeeLamports + MAGIC_BUNDLE_FEE_RESERVE_LAMPORTS)
          ) {
            continue;
          }

          if (estimates.platformFeeLamports > 0) {
            await sendMagicBundlePlatformFee(snapshot.signer, estimates.platformFeeLamports);
          }

          if (isStealthBundle) {
            const splitOrder = await createSplitNowOrderData(order, sendToSplitLamports);
            if (!splitOrder.depositAddress) {
              throw new Error('Stealth routing did not return a deposit address for this bundle.');
            }
            await transferLamportsBetweenWallets(snapshot.signer, splitOrder.depositAddress, sendToSplitLamports);

            await updateMagicBundle(userId, order.id, (draft) => ({
              ...draft,
              splitnowQuoteId: splitOrder.quoteId,
              splitnowOrderId: splitOrder.orderId,
              splitnowDepositAddress: splitOrder.depositAddress,
              splitnowDepositAmountSol: String(splitOrder.depositAmount),
              splitnowStatus: splitOrder.status,
              status: 'splitting',
              lastBalanceCheckAt: new Date().toISOString(),
              lastError: null,
            }));
            await appendUserActivityLog(userId, {
              scope: `magic_bundle:${order.id}`,
              level: 'info',
              message: `Magic Bundle sent ${formatSolAmountFromLamports(sendToSplitLamports)} SOL into the stealth spread for ${order.walletCount} bundle wallets.`,
            });
          } else {
            const spreadResult = await executeDirectMagicBundleSpread(snapshot.signer, order, sendToSplitLamports);
            await updateMagicBundle(userId, order.id, (draft) => ({
              ...draft,
              splitnowQuoteId: null,
              splitnowOrderId: null,
              splitnowDepositAddress: null,
              splitnowDepositAmountSol: null,
              splitnowStatus: 'Direct spread completed',
              splitCompletedAt: draft.splitCompletedAt || new Date().toISOString(),
              status: draft.automationEnabled && isMagicBundleAutomationConfigured(draft) ? 'waiting_inventory' : 'ready',
              lastBalanceCheckAt: new Date().toISOString(),
              lastError: null,
            }));
            await appendUserActivityLog(userId, {
              scope: `magic_bundle:${order.id}`,
              level: 'info',
              message: `Regular Bundle spread ${formatSolAmountFromLamports(sendToSplitLamports)} SOL across ${spreadResult.transferCount} bundle wallets.`,
            });
          }
          continue;
        }

        if (!order.automationEnabled || !configured) {
          continue;
        }
        if (!JUPITER_API_KEY) {
          throw new Error('JUPITER_API_KEY is required for Magic Bundle automation.');
        }

        let creatorSellEvent = null;
        let lastCreatorSeenSignature = order.lastCreatorSeenSignature ?? null;
        if (order.sellOnDevSell && snapshot.creatorAddress) {
          const creatorScan = await detectRecentMagicBundleCreatorSell(order, snapshot.creatorAddress);
          creatorSellEvent = creatorScan.event;
          lastCreatorSeenSignature = creatorScan.lastSeenSignature;
          if (creatorScan.lastSeenSignature !== order.lastCreatorSeenSignature || creatorScan.event) {
            await updateMagicBundle(userId, order.id, (draft) => ({
              ...draft,
              lastCreatorSeenSignature: creatorScan.lastSeenSignature,
            }));
          }
          if (creatorSellEvent) {
            await appendUserActivityLog(userId, {
              scope: `magic_bundle:${order.id}`,
              level: 'warn',
              message: `Magic Bundle detected a creator sell and is protecting active bundle wallets.`,
            });
          }
        }

        let acted = false;
        if (creatorSellEvent) {
          for (let walletIndex = 0; walletIndex < snapshot.splitWallets.length; walletIndex += 1) {
            const wallet = normalizeMagicBundleWalletRecord(snapshot.splitWallets[walletIndex]);
            if (BigInt(wallet.currentTokenAmountRaw || '0') <= 0n) {
              continue;
            }

            snapshot = await executeMagicBundleSell(userId, order, snapshot, walletIndex, 'dev_sell');
            acted = true;
          }
        } else {
          for (let walletIndex = 0; walletIndex < snapshot.splitWallets.length; walletIndex += 1) {
            const wallet = normalizeMagicBundleWalletRecord(snapshot.splitWallets[walletIndex]);
            const tokenRaw = BigInt(wallet.currentTokenAmountRaw || '0');
            if (tokenRaw <= 0n) {
              continue;
            }

            const currentValueLamports = wallet.currentPositionValueLamports || 0;
            const costBasisLamports = Number.isInteger(wallet.costBasisLamports)
              ? wallet.costBasisLamports
              : currentValueLamports;
            const highestValueLamports = Math.max(
              Number.isInteger(wallet.highestValueLamports) ? wallet.highestValueLamports : 0,
              currentValueLamports,
            );

            if (
              Number.isFinite(order.takeProfitPercent)
              && costBasisLamports > 0
              && currentValueLamports >= Math.ceil(costBasisLamports * (1 + (order.takeProfitPercent / 100)))
            ) {
              snapshot = await executeMagicBundleSell(userId, order, snapshot, walletIndex, 'take_profit');
              acted = true;
              break;
            }

            if (
              Number.isFinite(order.trailingStopLossPercent)
              && highestValueLamports > 0
              && currentValueLamports <= Math.floor(highestValueLamports * (1 - (order.trailingStopLossPercent / 100)))
            ) {
              snapshot = await executeMagicBundleSell(userId, order, snapshot, walletIndex, 'trailing_stop');
              acted = true;
              break;
            }

            if (
              Number.isFinite(order.stopLossPercent)
              && costBasisLamports > 0
              && currentValueLamports <= Math.floor(costBasisLamports * (1 - (order.stopLossPercent / 100)))
            ) {
              snapshot = await executeMagicBundleSell(userId, order, snapshot, walletIndex, 'stop_loss');
              acted = true;
              break;
            }

            const dipBuySpendLamports = getMagicBundleBuyDipSpendLamports(wallet);
            if (
              Number.isFinite(order.buyDipPercent)
              && costBasisLamports > 0
              && dipBuySpendLamports > 0
              && magicBundleDipCooldownElapsed(wallet)
              && currentValueLamports <= Math.floor(costBasisLamports * (1 - (order.buyDipPercent / 100)))
            ) {
              snapshot = await executeMagicBundleDipBuy(userId, order, snapshot, walletIndex, dipBuySpendLamports);
              acted = true;
              break;
            }
          }
        }

        if (!acted) {
          await persistMagicBundleSnapshot(userId, order.id, snapshot, () => ({
            status: snapshot.totalTokenRaw > 0n ? 'running' : 'waiting_inventory',
            lastCreatorSeenSignature,
            lastError: null,
          }));
        }
      } catch (error) {
        await updateMagicBundle(userId, order.id, (draft) => ({
          ...draft,
          status: 'failed',
          lastBalanceCheckAt: new Date().toISOString(),
          lastError: String(error.message || error),
        }));
        await appendUserActivityLog(userId, {
          scope: `magic_bundle:${order.id}`,
          level: 'error',
          message: `Magic Bundle error: ${String(error.message || error)}`,
        });
        console.error(`[worker] Magic Bundle failed for user ${userId}:`, error.message || error);
      }
    }
  }
}

async function scanMagicSells() {
  const store = await readStore();
  const solUsdRate = await ensureSolPriceCache();

  for (const [userId, user] of Object.entries(store.users ?? {})) {
    for (const order of normalizeUserMagicSells(user)) {
      if (
        !hasMeaningfulMagicSellRecord(order)
        || order.archivedAt
        || !order.automationEnabled
        || !order.walletAddress
        || !order.walletSecretKeyB64
        || !order.mintAddress
        || !Number.isFinite(order.targetMarketCapUsd)
      ) {
        continue;
      }

      try {
        const signer = decodeOrderWallet(order.walletSecretKeyB64);
        if (signer.publicKey.toBase58() !== order.walletAddress) {
          throw new Error('Magic Sell deposit wallet secret does not match the stored wallet address.');
        }

        const mintMetadata = await getMintMetadata(order.mintAddress);
        const depositLamports = await connection.getBalance(signer.publicKey, 'confirmed');
        const depositTokenRaw = await getOwnedMintRawBalance(signer.publicKey, order.mintAddress);
        const sellerWallets = await Promise.all(
          order.sellerWallets.map(async (wallet) => {
            if (!wallet?.address) {
              return normalizeMagicSellSellerWalletRecord(wallet);
            }

            const lamports = await connection.getBalance(new PublicKey(wallet.address), 'confirmed');
            const tokenRaw = await getOwnedMintRawBalance(new PublicKey(wallet.address), order.mintAddress);
            return normalizeMagicSellSellerWalletRecord({
              ...wallet,
              currentLamports: lamports,
              currentSol: formatSolAmountFromLamports(lamports),
              currentTokenAmountRaw: tokenRaw.toString(),
              currentTokenAmountDisplay: formatTokenAmountFromRaw(tokenRaw.toString(), mintMetadata.decimals),
            });
          }),
        );
        const totalManagedLamports = depositLamports + sellerWallets.reduce(
          (sum, wallet) => sum + (Number.isInteger(wallet.currentLamports) ? wallet.currentLamports : 0),
          0,
        );
        const marketInfo = await inspectPumpMintMarket(order.mintAddress, signer.publicKey);
        const currentMarketCapUsd = calculateMagicSellCurrentMarketCapUsd(marketInfo, solUsdRate);

        await updateMagicSell(userId, order.id, (draft) => ({
          ...draft,
          tokenDecimals: mintMetadata.decimals,
          tokenProgram: mintMetadata.tokenProgram.toBase58(),
          currentLamports: depositLamports,
          currentSol: formatSolAmountFromLamports(depositLamports),
          currentTokenAmountRaw: depositTokenRaw.toString(),
          currentTokenAmountDisplay: formatTokenAmountFromRaw(depositTokenRaw.toString(), mintMetadata.decimals),
          totalManagedLamports,
          sellerWallets,
          currentMarketCapUsd,
          currentMarketCapSol: marketInfo.marketCapSol,
          marketPhase: marketInfo.marketPhase,
          status: Number.isFinite(currentMarketCapUsd) && currentMarketCapUsd >= order.targetMarketCapUsd
            ? 'running'
            : 'waiting_target',
          lastBalanceCheckAt: new Date().toISOString(),
          lastError: null,
        }));

        if (!Number.isFinite(currentMarketCapUsd) || currentMarketCapUsd < order.targetMarketCapUsd) {
          continue;
        }

        const signatures = await getRecentMintSignatures(order.mintAddress, MAGIC_SELL_SCAN_LIMIT);
        if (!Array.isArray(signatures) || signatures.length === 0) {
          continue;
        }

        if (!order.lastSeenSignature) {
          await updateMagicSell(userId, order.id, (draft) => ({
            ...draft,
            lastSeenSignature: signatures[0]?.signature ?? draft.lastSeenSignature,
            status: 'running',
            lastError: null,
          }));
          continue;
        }

        const toProcess = [];
        for (const info of signatures) {
          if (!info?.signature || info.err) {
            continue;
          }
          if (info.signature === order.lastSeenSignature) {
            break;
          }
          toProcess.push(info);
        }

        if (toProcess.length === 0) {
          continue;
        }

        const ignoredWallets = new Set([
          signer.publicKey.toBase58(),
          ...(Array.isArray(order.whitelistWallets) ? order.whitelistWallets : []),
          ...sellerWallets.map((wallet) => wallet.address).filter(Boolean),
        ]);

        let handled = false;
        for (const info of toProcess.reverse()) {
          const transaction = await getParsedTransactionBySignature(info.signature);
          const buyEvent = buildMagicSellBuyerEvent(transaction, order.mintAddress, ignoredWallets);
          if (!buyEvent || buyEvent.lamportsSpent < (order.minimumBuyLamports || MAGIC_SELL_MINIMUM_BUY_LAMPORTS)) {
            await updateMagicSell(userId, order.id, (draft) => ({
              ...draft,
              lastSeenSignature: info.signature,
            }));
            continue;
          }

          const targetSellLamports = Math.max(1, Math.floor(buyEvent.lamportsSpent * (order.sellPercent || MAGIC_SELL_SELL_PERCENT) / 100));
          const tokenQuote = await fetchJupiterSwapOrderFor({
            inputMint: SOL_MINT_ADDRESS,
            outputMint: order.mintAddress,
            amount: targetSellLamports,
            taker: signer.publicKey.toBase58(),
          });
          const sellTokenAmountRaw = BigInt(tokenQuote.outAmount || '0');
          if (sellTokenAmountRaw <= 0n) {
            await updateMagicSell(userId, order.id, (draft) => ({
              ...draft,
              lastSeenSignature: info.signature,
            }));
            continue;
          }

          const refreshedDepositTokenRaw = await getOwnedMintRawBalance(signer.publicKey, order.mintAddress);
          const refreshedDepositLamports = await connection.getBalance(signer.publicKey, 'confirmed');
          if (refreshedDepositTokenRaw < sellTokenAmountRaw || refreshedDepositLamports < MAGIC_SELL_WORKER_GAS_RESERVE_LAMPORTS) {
            await updateMagicSell(userId, order.id, (draft) => ({
              ...draft,
              lastSeenSignature: info.signature,
              currentLamports: refreshedDepositLamports,
              currentSol: formatSolAmountFromLamports(refreshedDepositLamports),
              currentTokenAmountRaw: refreshedDepositTokenRaw.toString(),
              currentTokenAmountDisplay: formatTokenAmountFromRaw(refreshedDepositTokenRaw.toString(), mintMetadata.decimals),
              status: 'waiting_inventory',
              lastError: null,
            }));
            break;
          }

          const sellerIndex = chooseMagicSellWalletIndex(sellerWallets);
          if (!Number.isInteger(sellerIndex)) {
            throw new Error('Magic Sell does not have any valid seller wallets.');
          }

          const selectedSeller = sellerWallets[sellerIndex];
          const sellerSigner = decodeOrderWallet(selectedSeller.secretKeyB64);
          if (sellerSigner.publicKey.toBase58() !== selectedSeller.address) {
            throw new Error(`Magic Sell seller wallet #${sellerIndex + 1} secret does not match its wallet address.`);
          }

          const sellerLamportsBefore = await connection.getBalance(sellerSigner.publicKey, 'confirmed');
          const gasTopUpLamports = Math.max(0, MAGIC_SELL_WORKER_GAS_RESERVE_LAMPORTS - sellerLamportsBefore);
          if (gasTopUpLamports > 0) {
            await transferLamportsBetweenWallets(signer, selectedSeller.address, gasTopUpLamports);
          }
          await transferMintAmountToWallet(
            signer,
            order.mintAddress,
            selectedSeller.address,
            sellTokenAmountRaw.toString(),
            mintMetadata.decimals,
            mintMetadata.tokenProgram,
          );

          const sellOrder = await fetchJupiterSwapOrderFor({
            inputMint: order.mintAddress,
            outputMint: SOL_MINT_ADDRESS,
            amount: sellTokenAmountRaw.toString(),
            taker: selectedSeller.address,
          });
          const sellResult = await executeJupiterSwapFor(sellOrder, sellerSigner);

          const updatedDepositLamports = await connection.getBalance(signer.publicKey, 'confirmed');
          const updatedDepositTokenRaw = await getOwnedMintRawBalance(signer.publicKey, order.mintAddress);
          const updatedSellerLamports = await connection.getBalance(sellerSigner.publicKey, 'confirmed');
          const updatedSellerTokenRaw = await getOwnedMintRawBalance(sellerSigner.publicKey, order.mintAddress);
          sellerWallets[sellerIndex] = normalizeMagicSellSellerWalletRecord({
            ...selectedSeller,
            currentLamports: updatedSellerLamports,
            currentSol: formatSolAmountFromLamports(updatedSellerLamports),
            currentTokenAmountRaw: updatedSellerTokenRaw.toString(),
            currentTokenAmountDisplay: formatTokenAmountFromRaw(updatedSellerTokenRaw.toString(), mintMetadata.decimals),
            sellCount: (selectedSeller.sellCount || 0) + 1,
            lastUsedAt: new Date().toISOString(),
            status: 'idle',
            lastSellSignature: sellResult.signature ?? null,
            lastError: null,
          });

          await updateMagicSell(userId, order.id, (draft) => ({
            ...draft,
            currentLamports: updatedDepositLamports,
            currentSol: formatSolAmountFromLamports(updatedDepositLamports),
            currentTokenAmountRaw: updatedDepositTokenRaw.toString(),
            currentTokenAmountDisplay: formatTokenAmountFromRaw(updatedDepositTokenRaw.toString(), mintMetadata.decimals),
            totalManagedLamports: updatedDepositLamports + sellerWallets.reduce(
              (sum, wallet) => sum + (Number.isInteger(wallet.currentLamports) ? wallet.currentLamports : 0),
              0,
            ),
            sellerWallets,
            status: 'running',
            stats: {
              ...normalizeMagicSellStats(draft.stats),
              triggerCount: normalizeMagicSellStats(draft.stats).triggerCount + 1,
              sellCount: normalizeMagicSellStats(draft.stats).sellCount + 1,
              totalObservedBuyLamports: normalizeMagicSellStats(draft.stats).totalObservedBuyLamports + buyEvent.lamportsSpent,
              totalTargetSellLamports: normalizeMagicSellStats(draft.stats).totalTargetSellLamports + targetSellLamports,
              totalSoldTokenRaw: (BigInt(normalizeMagicSellStats(draft.stats).totalSoldTokenRaw) + sellTokenAmountRaw).toString(),
              lastTriggerSignature: info.signature,
              lastSellSignature: sellResult.signature ?? null,
            },
            lastSeenSignature: info.signature,
            lastProcessedBuyAt: new Date().toISOString(),
            lastBalanceCheckAt: new Date().toISOString(),
            lastError: null,
          }));
          await appendUserActivityLog(userId, {
            scope: `magic_sell:${order.id}`,
            level: 'info',
            message: `Magic Sell reacted to a ${formatSolAmountFromLamports(buyEvent.lamportsSpent)} SOL buy from ${buyEvent.buyerWallet} and sold ${formatTokenAmountFromRaw(sellTokenAmountRaw.toString(), mintMetadata.decimals)} tokens from seller #${sellerIndex + 1}.`,
          });
          handled = true;
          break;
        }

        if (!handled) {
          const newestSeenSignature = toProcess[0]?.signature ?? order.lastSeenSignature;
          await updateMagicSell(userId, order.id, (draft) => ({
            ...draft,
            lastSeenSignature: newestSeenSignature,
            status: 'running',
            lastError: null,
          }));
        }
      } catch (error) {
        await updateMagicSell(userId, order.id, (draft) => ({
          ...draft,
          status: 'failed',
          lastBalanceCheckAt: new Date().toISOString(),
          lastError: String(error.message || error),
        }));
        await appendUserActivityLog(userId, {
          scope: `magic_sell:${order.id}`,
          level: 'error',
          message: `Magic Sell error: ${String(error.message || error)}`,
        });
        console.error(`[worker] Magic Sell failed for user ${userId}:`, error.message || error);
      }
    }
  }
}

async function scanHolderBoosters() {
  const store = await readStore();

  for (const [userId, user] of Object.entries(store.users ?? {})) {
    const order = normalizeUserHolderBooster(user);
    if (!hasMeaningfulHolderBoosterRecord(order)) {
      continue;
    }

    if (
      !order.walletAddress
      || !order.walletSecretKeyB64
      || !order.mintAddress
      || !Number.isInteger(order.holderCount)
      || order.holderCount <= 0
      || order.awaitingField
      || order.status === 'completed'
    ) {
      continue;
    }

    try {
      const signer = decodeOrderWallet(order.walletSecretKeyB64);
      if (signer.publicKey.toBase58() !== order.walletAddress) {
        throw new Error('Holder Booster wallet secret does not match the stored wallet address.');
      }

      const mintMetadata = await getMintMetadata(order.mintAddress);
      const balanceLamports = await connection.getBalance(signer.publicKey, 'confirmed');
      const tokenBalanceRaw = await getOwnedMintRawBalance(signer.publicKey, order.mintAddress);
      const oneWholeTokenRaw = 10n ** BigInt(mintMetadata.decimals);
      const requiredTokenRaw = oneWholeTokenRaw * BigInt(order.holderCount);
      const requiredLamports = Number.isInteger(order.requiredLamports)
        ? order.requiredLamports
        : Math.round(order.holderCount * 0.10 * LAMPORTS_PER_SOL);
      const hasRequiredFunding = balanceLamports >= requiredLamports && tokenBalanceRaw >= requiredTokenRaw;

      await updateHolderBooster(userId, (draft) => ({
        ...draft,
        tokenDecimals: mintMetadata.decimals,
        tokenProgram: mintMetadata.tokenProgram.toBase58(),
        requiredLamports,
        requiredSol: (order.holderCount * 0.10).toFixed(2),
        requiredTokenAmountRaw: requiredTokenRaw.toString(),
        currentLamports: balanceLamports,
        currentSol: formatSolAmountFromLamports(balanceLamports),
        currentTokenAmountRaw: tokenBalanceRaw.toString(),
        currentTokenAmountDisplay: formatTokenAmountFromRaw(tokenBalanceRaw.toString(), mintMetadata.decimals),
        status: draft.status === 'processing'
          ? 'processing'
          : (hasRequiredFunding ? 'ready' : 'awaiting_funding'),
        fundedAt: hasRequiredFunding ? (draft.fundedAt || new Date().toISOString()) : draft.fundedAt,
        lastBalanceCheckAt: new Date().toISOString(),
        lastError: null,
      }));

      if (!hasRequiredFunding) {
        continue;
      }

      if (!cfg.treasuryWalletAddress || !cfg.devWalletAddress) {
        throw new Error('Holder Booster requires TREASURY_WALLET_ADDRESS and DEV_WALLET_ADDRESS.');
      }

      let workingOrder = normalizeHolderBoosterRecord((await readStore()).users?.[userId]?.holderBooster ?? order);
      await updateHolderBooster(userId, (draft) => ({
        ...draft,
        status: 'processing',
        lastError: null,
      }));

      for (let index = workingOrder.processedWalletCount; index < workingOrder.childWallets.length; index += 1) {
        const recipient = workingOrder.childWallets[index];
        if (!recipient?.address) {
          continue;
        }

        await transferOneWholeTokenToWallet(
          signer,
          workingOrder.mintAddress,
          recipient.address,
          mintMetadata.decimals,
          mintMetadata.tokenProgram,
        );

        workingOrder = await updateHolderBooster(userId, (draft) => ({
          ...draft,
          processedWalletCount: index + 1,
          childWallets: draft.childWallets.map((wallet, walletIndex) => (
            walletIndex === index ? { ...wallet, status: 'funded' } : wallet
          )),
          status: 'processing',
          lastError: null,
        }));
      }

      const remainingLamports = await connection.getBalance(signer.publicKey, 'confirmed');
      const distributableLamports = Math.max(0, remainingLamports - SPLIT_FEE_BUFFER_LAMPORTS);
      let payoutSignature = null;
      if (distributableLamports > 0) {
        const treasuryLamports = Math.floor(distributableLamports / 2);
        const devLamports = distributableLamports - treasuryLamports;
        const latestBlockhash = await connection.getLatestBlockhash('confirmed');
        const transaction = new Transaction({
          feePayer: signer.publicKey,
          recentBlockhash: latestBlockhash.blockhash,
          lastValidBlockHeight: latestBlockhash.lastValidBlockHeight,
        }).add(
          SystemProgram.transfer({
            fromPubkey: signer.publicKey,
            toPubkey: new PublicKey(cfg.treasuryWalletAddress),
            lamports: treasuryLamports,
          }),
          SystemProgram.transfer({
            fromPubkey: signer.publicKey,
            toPubkey: new PublicKey(cfg.devWalletAddress),
            lamports: devLamports,
          }),
        );

        payoutSignature = await connection.sendTransaction(transaction, [signer], {
          skipPreflight: true,
          preflightCommitment: 'confirmed',
          maxRetries: 3,
        });
        await confirmTransactionByMetadata(
          payoutSignature,
          latestBlockhash.blockhash,
          latestBlockhash.lastValidBlockHeight,
          'confirmed',
        );
      }

      const finalLamports = await connection.getBalance(signer.publicKey, 'confirmed');
      const finalTokenBalanceRaw = await getOwnedMintRawBalance(signer.publicKey, workingOrder.mintAddress);
      await updateHolderBooster(userId, (draft) => ({
        ...draft,
        currentLamports: finalLamports,
        currentSol: formatSolAmountFromLamports(finalLamports),
        currentTokenAmountRaw: finalTokenBalanceRaw.toString(),
        currentTokenAmountDisplay: formatTokenAmountFromRaw(finalTokenBalanceRaw.toString(), mintMetadata.decimals),
        processedWalletCount: draft.childWallets.length,
        status: 'completed',
        completedAt: new Date().toISOString(),
        treasurySignature: payoutSignature,
        devSignature: payoutSignature,
        lastBalanceCheckAt: new Date().toISOString(),
        lastError: null,
      }));
      await appendUserActivityLog(userId, {
        scope: `holder_booster:${workingOrder.id}`,
        level: 'info',
        message: `Holder Booster completed: ${workingOrder.holderCount} holder wallets funded and remaining SOL split to treasury/dev.`,
      });
    } catch (error) {
      await updateHolderBooster(userId, (draft) => ({
        ...draft,
        status: 'failed',
        lastBalanceCheckAt: new Date().toISOString(),
        lastError: String(error.message || error),
      }));
      await appendUserActivityLog(userId, {
        scope: `holder_booster:${order.id}`,
        level: 'error',
        message: `Holder Booster error: ${String(error.message || error)}`,
      });
      console.error(`[worker] Holder Booster failed for user ${userId}:`, error.message || error);
    }
  }
}

async function scanOrganicBoosters() {
  const store = await readStore();
  const solUsdRate = await ensureSolPriceCache();

  for (const [userId, user] of Object.entries(store.users ?? {})) {
    for (const order of normalizeUserAppleBoosters(user)) {
      if (!shouldRunOrganicBooster(order)) {
        continue;
      }

      if (order.treasuryCutSol && order.treasurySplitStatus !== 'completed') {
        continue;
      }

      if (!order.appleBooster.stopRequested && !organicBoosterCanAct(order.appleBooster)) {
        continue;
      }

      try {
      if (isVolumeTrialOrder(order) && !cfg.volumeTrial.enabled) {
        throw new Error(`Volume free trial is unavailable: ${cfg.volumeTrial.reason || 'missing trial wallet config'}.`);
      }

      let snapshot = await refreshOrganicWalletSnapshot(order);
      const marketInfo = await inspectPumpMintMarket(order.appleBooster.mintAddress, snapshot.depositSigner.publicKey);
      const marketEstimatePatch = buildAppleBoosterMarketEstimatePatch(
        order.appleBooster,
        snapshot.totalManagedLamports,
        marketInfo,
      );
      await persistOrganicWalletSnapshot(userId, order.id, snapshot, (booster) => ({
        status: order.appleBooster.stopRequested ? 'stopping' : booster.status,
        ...marketEstimatePatch,
        lastError: null,
      }));

      if (order.strategy === 'bundled') {
        await processBundledBoosterCycle(userId, order, snapshot, marketEstimatePatch, solUsdRate);
        continue;
      }

      const booster = normalizeOrganicOrder({
        ...order,
        currentLamports: snapshot.depositLamports,
        currentSol: formatSolAmountFromLamports(snapshot.depositLamports),
        appleBooster: {
          ...order.appleBooster,
          ...marketEstimatePatch,
          workerWallets: snapshot.workerWallets,
          totalManagedLamports: snapshot.totalManagedLamports,
        },
      }).appleBooster;

      if (shouldAutoCompleteAppleBooster({
        ...order,
        appleBooster: booster,
      }, solUsdRate)) {
        const { packageTargetUsd, approximateVolumeUsd } = calculateAppleBoosterVolumeProgressUsd({
          ...order,
          appleBooster: booster,
        }, solUsdRate);
        await updateAppleBooster(userId, order.id, (draft) => ({
          ...draft,
          running: true,
          appleBooster: {
            ...draft.appleBooster,
            stopRequested: true,
            status: 'target_reached',
            nextActionAt: null,
            lastError: null,
          },
        }));
        await appendUserActivityLog(userId, {
          scope: organicOrderScope(order.id),
          level: 'info',
          message: `Apple Booster reached its ${Math.round(packageTargetUsd).toLocaleString('en-US')} USD package target at about ${Math.round(approximateVolumeUsd || 0).toLocaleString('en-US')} USD. Auto-stopping now.`,
        });
        continue;
      }

      if (
        isVolumeTrialOrder(order)
        && order.running
        && !order.appleBooster.stopRequested
        && getOrganicTradeLegCount({ ...order, appleBooster: booster }) >= getVolumeTrialTradeGoal(order)
      ) {
        await updateAppleBooster(userId, order.id, (draft) => ({
          ...draft,
          running: true,
          appleBooster: {
            ...draft.appleBooster,
            stopRequested: true,
            status: 'trial_complete',
            nextActionAt: null,
            lastError: null,
          },
        }));
        await appendUserActivityLog(userId, {
          scope: organicOrderScope(order.id),
          level: 'info',
          message: `Organic Volume Free Trial reached its ${getVolumeTrialTradeGoal(order)}-trade demo goal. Auto-stopping now.`,
        });
        continue;
      }

      let acted = false;

      for (let workerIndex = 0; workerIndex < snapshot.workerWallets.length; workerIndex += 1) {
        const worker = snapshot.workerWallets[workerIndex];
        const workerSigner = decodeOrderWallet(worker.secretKeyB64);
        if (workerSigner.publicKey.toBase58() !== worker.address) {
          throw new Error(`Apple Booster worker ${workerIndex + 1} secret does not match its wallet address.`);
        }

        const pendingSellAmount = BigInt(worker.pendingSellAmount || '0');
        const strayMintBalance = await getOwnedMintRawBalance(workerSigner.publicKey, booster.mintAddress);
        const sellAmount = pendingSellAmount > 0n
          ? pendingSellAmount
          : (strayMintBalance > 0n ? strayMintBalance : 0n);

        if (sellAmount > 0n) {
          const sellingWorkers = organicBoosterWorkerPatch(snapshot.workerWallets, workerIndex, (draftWorker) => ({
            ...draftWorker,
            status: 'selling',
            lastError: null,
          }));
          await persistOrganicWalletSnapshot(userId, order.id, {
            ...snapshot,
            workerWallets: sellingWorkers,
          }, () => ({
            status: order.appleBooster.stopRequested ? 'stopping' : 'selling',
            lastError: null,
          }));

          const sellOrder = await fetchJupiterSwapOrderFor({
            inputMint: booster.mintAddress,
            outputMint: SOL_MINT_ADDRESS,
            amount: sellAmount.toString(),
            taker: workerSigner.publicKey.toBase58(),
          });
          const sellResult = await executeJupiterSwapFor(sellOrder, workerSigner);
          const workerBalanceAfterSell = await connection.getBalance(workerSigner.publicKey, 'confirmed');
          const nextActionAt = order.appleBooster.stopRequested ? null : getNextOrganicBoosterActionAt(booster);

          snapshot = {
            ...snapshot,
            workerWallets: organicBoosterWorkerPatch(snapshot.workerWallets, workerIndex, (draftWorker) => ({
              ...draftWorker,
              currentLamports: workerBalanceAfterSell,
              currentSol: formatSolAmountFromLamports(workerBalanceAfterSell),
              status: order.appleBooster.stopRequested ? 'ready_to_sweep' : 'idle',
              pendingSellAmount: null,
              nextActionAt,
              lastActionAt: new Date().toISOString(),
              lastSellInputAmount: sellAmount.toString(),
              lastSellOutputLamports: String(sellResult.outputAmountResult ?? sellOrder.outAmount ?? '0'),
              lastSellSignature: sellResult.signature ?? null,
              lastError: null,
            })),
          };
          snapshot.totalManagedLamports = snapshot.depositLamports + snapshot.workerWallets.reduce(
            (sum, item) => sum + item.currentLamports,
            0,
          );
          await persistOrganicWalletSnapshot(userId, order.id, snapshot, (draftBooster) => ({
            status: order.appleBooster.stopRequested ? 'stopping' : 'running',
            nextActionAt,
            lastActionAt: new Date().toISOString(),
            lastSellInputAmount: sellAmount.toString(),
            lastSellOutputLamports: String(sellResult.outputAmountResult ?? sellOrder.outAmount ?? '0'),
            lastSellSignature: sellResult.signature ?? null,
            totalSellCount: (draftBooster.totalSellCount || 0) + 1,
            totalSellOutputLamports: (draftBooster.totalSellOutputLamports || 0)
              + Number(sellResult.outputAmountResult ?? sellOrder.outAmount ?? 0),
            cycleCount: order.appleBooster.stopRequested
              ? draftBooster.cycleCount
              : ((draftBooster.cycleCount || 0) + 1),
            lastError: null,
          }));
      await appendUserActivityLog(userId, {
            scope: organicOrderScope(order.id),
            level: 'info',
            message: order.appleBooster.stopRequested
              ? `Worker #${workerIndex + 1} sold its token balance back to SOL for stop-and-sweep.`
              : `Worker #${workerIndex + 1} completed a sell leg back to SOL.`,
          });
          acted = true;
          break;
        }

        if (order.appleBooster.stopRequested) {
          const sweepLamports = Math.max(0, worker.currentLamports - APPLE_BOOSTER_SWEEP_RESERVE_LAMPORTS);
          if (sweepLamports > 0) {
            const signature = await transferLamportsBetweenWallets(
              workerSigner,
              order.walletAddress,
              sweepLamports,
            );
            const refreshedDepositLamports = await connection.getBalance(snapshot.depositSigner.publicKey, 'confirmed');
            const refreshedWorkerLamports = await connection.getBalance(workerSigner.publicKey, 'confirmed');
            snapshot = {
              ...snapshot,
              depositLamports: refreshedDepositLamports,
              workerWallets: organicBoosterWorkerPatch(snapshot.workerWallets, workerIndex, (draftWorker) => ({
                ...draftWorker,
                currentLamports: refreshedWorkerLamports,
                currentSol: formatSolAmountFromLamports(refreshedWorkerLamports),
                status: 'swept',
                nextActionAt: null,
                lastActionAt: new Date().toISOString(),
                lastSellSignature: draftWorker.lastSellSignature || signature,
                lastError: null,
              })),
            };
            snapshot.totalManagedLamports = snapshot.depositLamports + snapshot.workerWallets.reduce(
              (sum, item) => sum + item.currentLamports,
              0,
            );
            await persistOrganicWalletSnapshot(userId, order.id, snapshot, (draftBooster) => ({
              status: 'stopping',
              nextActionAt: null,
              lastActionAt: new Date().toISOString(),
              totalSweptLamports: (draftBooster.totalSweptLamports || 0) + sweepLamports,
              lastError: null,
            }));
            await appendUserActivityLog(userId, {
              scope: organicOrderScope(order.id),
              level: 'info',
              message: `Worker #${workerIndex + 1} swept ${formatSolAmountFromLamports(sweepLamports)} SOL back to the deposit wallet.`,
            });
            acted = true;
            break;
          }
        }
      }

      if (acted) {
        continue;
      }

      if (order.appleBooster.stopRequested) {
        const shouldSendRemainderToTreasury = order.appleBooster.status === 'target_reached';
        const shouldReturnTrialFunds = isVolumeTrialOrder(order);

        if (shouldSendRemainderToTreasury) {
          const treasuryDestination = order.treasuryWalletAddress || cfg.treasuryWalletAddress;
          if (!treasuryDestination) {
            throw new Error('Apple Booster completion requires a treasury wallet address.');
          }

          const treasurySweepLamports = Math.max(0, snapshot.depositLamports - APPLE_BOOSTER_SWEEP_RESERVE_LAMPORTS);
          if (treasurySweepLamports > 0) {
            await transferLamportsBetweenWallets(
              snapshot.depositSigner,
              treasuryDestination,
              treasurySweepLamports,
            );
            snapshot = await refreshOrganicWalletSnapshot(order);
            await persistOrganicWalletSnapshot(userId, order.id, snapshot, (draftBooster) => ({
              status: 'target_reached',
              nextActionAt: null,
              lastActionAt: new Date().toISOString(),
              lastError: null,
            }));
            await appendUserActivityLog(userId, {
              scope: organicOrderScope(order.id),
              level: 'info',
              message: `Apple Booster sent the remaining ${formatSolAmountFromLamports(treasurySweepLamports)} SOL to treasury after hitting the package target.`,
            });
          }
        }

        if (shouldReturnTrialFunds) {
          const reclaimLamports = Math.max(0, snapshot.depositLamports - APPLE_BOOSTER_SWEEP_RESERVE_LAMPORTS);
          if (reclaimLamports > 0) {
            await transferLamportsBetweenWallets(
              snapshot.depositSigner,
              cfg.volumeTrial.walletAddress,
              reclaimLamports,
            );
            snapshot = await refreshOrganicWalletSnapshot(order);
            await persistOrganicWalletSnapshot(userId, order.id, snapshot, (draftBooster) => ({
              status: 'trial_complete',
              nextActionAt: null,
              lastActionAt: new Date().toISOString(),
              lastError: null,
            }));
            await appendUserActivityLog(userId, {
              scope: organicOrderScope(order.id),
              level: 'info',
              message: `Organic Volume Free Trial returned ${formatSolAmountFromLamports(reclaimLamports)} SOL to the platform trial wallet after the demo finished.`,
            });
          }
        }

        await persistOrganicWalletSnapshot(userId, order.id, snapshot, () => ({
          status: shouldSendRemainderToTreasury || shouldReturnTrialFunds ? 'archived' : 'stopped',
          stopRequested: false,
        }));
        await updateAppleBooster(userId, order.id, (draft) => ({
          ...draft,
          running: false,
          archivedAt: shouldSendRemainderToTreasury || shouldReturnTrialFunds ? new Date().toISOString() : null,
          appleBooster: {
            ...draft.appleBooster,
            status: shouldSendRemainderToTreasury || shouldReturnTrialFunds ? 'archived' : 'stopped',
            stopRequested: false,
            nextActionAt: null,
            lastError: null,
          },
        }));
        await appendUserActivityLog(userId, {
          scope: organicOrderScope(order.id),
          level: 'info',
          message: shouldSendRemainderToTreasury
            ? 'Apple Booster completion finished. Worker wallets were sold out, the remaining deposit-wallet SOL was sent to treasury, and the booster was auto-archived.'
            : (shouldReturnTrialFunds
              ? 'Organic Volume Free Trial finished. Worker wallets were sold out, leftovers were returned to the platform trial wallet, and the demo was auto-archived.'
              : 'Apple Booster stop completed. Worker wallets were sold out and swept back into the deposit wallet.'),
        });
        continue;
      }

      const readyWorkerIndexes = snapshot.workerWallets
        .map((worker, index) => ({ worker, index }))
        .filter(({ worker }) => organicWorkerCanAct(worker))
        .map(({ index }) => index);

      if (readyWorkerIndexes.length === 0) {
        await persistOrganicWalletSnapshot(userId, order.id, snapshot, (draftBooster) => ({
          status: draftBooster.status || 'running',
          lastError: null,
        }));
        continue;
      }

      const selectedWorkerIndex = readyWorkerIndexes[
        randomIntegerBetween(0, readyWorkerIndexes.length - 1)
      ];
      const selectedWorker = snapshot.workerWallets[selectedWorkerIndex];
      const selectedWorkerSigner = decodeOrderWallet(selectedWorker.secretKeyB64);
      if (selectedWorkerSigner.publicKey.toBase58() !== selectedWorker.address) {
        throw new Error(`Apple Booster worker ${selectedWorkerIndex + 1} secret does not match its wallet address.`);
      }

      const desiredLamports = randomIntegerBetween(
        booster.minSwapLamports,
        booster.maxSwapLamports,
      );
      const targetWorkerLamports = desiredLamports + APPLE_BOOSTER_FEE_RESERVE_LAMPORTS;
      const topUpLamports = Math.max(0, targetWorkerLamports - selectedWorker.currentLamports);
      if (topUpLamports > 0 && isVolumeTrialOrder(order)) {
        snapshot = await fundVolumeTrialDepositIfNeeded(userId, order, snapshot, topUpLamports);
      }
      const availableDepositLamports = Math.max(0, snapshot.depositLamports - APPLE_BOOSTER_FEE_RESERVE_LAMPORTS);

      if (topUpLamports > 0) {
        if (availableDepositLamports < topUpLamports) {
          await persistOrganicWalletSnapshot(userId, order.id, snapshot, (draftBooster) => ({
            status: 'waiting_funds',
            nextActionAt: getNextOrganicBoosterActionAt(draftBooster),
            lastError: null,
          }));
          continue;
        }

        const fundingWorkers = organicBoosterWorkerPatch(snapshot.workerWallets, selectedWorkerIndex, (draftWorker) => ({
          ...draftWorker,
          status: 'funding',
          lastError: null,
        }));
        await persistOrganicWalletSnapshot(userId, order.id, {
          ...snapshot,
          workerWallets: fundingWorkers,
        }, () => ({
          status: 'funding_worker',
          lastError: null,
        }));

        await transferLamportsBetweenWallets(
          snapshot.depositSigner,
          selectedWorker.address,
          topUpLamports,
        );
        await appendUserActivityLog(userId, {
          scope: organicOrderScope(order.id),
          level: 'info',
          message: `Deposit wallet topped up worker #${selectedWorkerIndex + 1} with ${formatSolAmountFromLamports(topUpLamports)} SOL.`,
        });

        snapshot = await refreshOrganicWalletSnapshot({
          ...order,
          appleBooster: {
            ...order.appleBooster,
            workerWallets: fundingWorkers,
          },
        });
      }

      const refreshedSelectedWorker = snapshot.workerWallets[selectedWorkerIndex];
      const spendableLamports = Math.max(
        0,
        refreshedSelectedWorker.currentLamports - APPLE_BOOSTER_FEE_RESERVE_LAMPORTS,
      );

      if (spendableLamports < desiredLamports) {
        await persistOrganicWalletSnapshot(userId, order.id, snapshot, (draftBooster) => ({
          status: 'waiting_funds',
          nextActionAt: getNextOrganicBoosterActionAt(draftBooster),
          lastError: null,
        }));
        continue;
      }

      const buyingWorkers = organicBoosterWorkerPatch(snapshot.workerWallets, selectedWorkerIndex, (draftWorker) => ({
        ...draftWorker,
        status: 'buying',
        lastError: null,
      }));
      await persistOrganicWalletSnapshot(userId, order.id, {
        ...snapshot,
        workerWallets: buyingWorkers,
      }, () => ({
        status: 'buying',
        lastError: null,
      }));

      const buyOrder = await fetchJupiterSwapOrderFor({
        inputMint: SOL_MINT_ADDRESS,
        outputMint: booster.mintAddress,
        amount: desiredLamports,
        taker: selectedWorkerSigner.publicKey.toBase58(),
      });
      const buyResult = await executeJupiterSwapFor(buyOrder, selectedWorkerSigner);
      const workerBalanceAfterBuy = await connection.getBalance(selectedWorkerSigner.publicKey, 'confirmed');
      const boughtAmount = buyResult.outputAmountResult ?? buyOrder.outAmount ?? null;

      if (!boughtAmount) {
        throw new Error('Apple Booster buy completed without a token output amount.');
      }

      const nextActionAt = getNextOrganicBoosterActionAt(booster);
      snapshot = {
        ...snapshot,
        workerWallets: organicBoosterWorkerPatch(snapshot.workerWallets, selectedWorkerIndex, (draftWorker) => ({
          ...draftWorker,
          currentLamports: workerBalanceAfterBuy,
          currentSol: formatSolAmountFromLamports(workerBalanceAfterBuy),
          status: 'waiting_to_sell',
          pendingSellAmount: String(boughtAmount),
          nextActionAt,
          lastActionAt: new Date().toISOString(),
          lastBuyInputLamports: desiredLamports,
          lastBuyOutputAmount: String(boughtAmount),
          lastBuySignature: buyResult.signature ?? null,
          lastError: null,
        })),
      };
      snapshot.totalManagedLamports = snapshot.depositLamports + snapshot.workerWallets.reduce(
        (sum, item) => sum + item.currentLamports,
        0,
      );
      await persistOrganicWalletSnapshot(userId, order.id, snapshot, (draftBooster) => ({
        status: 'running',
        nextActionAt,
        lastActionAt: new Date().toISOString(),
        lastBuyInputLamports: desiredLamports,
        lastBuyOutputAmount: String(boughtAmount),
        lastBuySignature: buyResult.signature ?? null,
        totalBuyCount: (draftBooster.totalBuyCount || 0) + 1,
        totalTopUpLamports: (draftBooster.totalTopUpLamports || 0) + topUpLamports,
        totalBuyInputLamports: (draftBooster.totalBuyInputLamports || 0) + desiredLamports,
        lastError: null,
      }));
      await appendUserActivityLog(userId, {
        scope: organicOrderScope(order.id),
        level: 'info',
        message: `Worker #${selectedWorkerIndex + 1} bought ${booster.mintAddress} using ${formatSolAmountFromLamports(desiredLamports)} SOL.`,
      });
    } catch (error) {
      await updateAppleBooster(userId, order.id, (draft) => ({
        ...draft,
        appleBooster: {
          ...draft.appleBooster,
          status: draft.appleBooster.stopRequested ? 'stopping_failed' : 'failed',
          nextActionAt: getNextOrganicBoosterActionAt(draft.appleBooster),
          lastError: String(error.message || error),
        },
      }));
      await appendUserActivityLog(userId, {
        scope: organicOrderScope(order.id),
        level: 'error',
        message: `${order.strategy === 'bundled' ? 'Bundled Apple Booster' : 'Apple Booster'} error: ${String(error.message || error)}`,
      });
      console.error(
        `[worker] ${order.strategy === 'bundled' ? 'Bundled Apple Booster' : 'Apple Booster'} failed for user ${userId}:`,
        error.message || error,
      );
      }
    }
  }
}

function shouldAttemptDevWalletSwap(state, swappableLamports) {
  if (!cfg.devWalletSwap.enabled) {
    return false;
  }

  if (swappableLamports < cfg.devWalletSwap.minimumLamports) {
    return false;
  }

  if (state?.status !== 'processing') {
    return true;
  }

  const attemptedAt = state?.lastAttemptedAt ? new Date(state.lastAttemptedAt).getTime() : 0;
  return !attemptedAt || Date.now() - attemptedAt >= DEV_WALLET_SWAP_RETRY_AFTER_MS;
}

function shouldDevWalletSwapYieldToCreatorRewards(workerState) {
  if (!cfg.devWalletSwap.enabled || !cfg.pumpCreatorRewards.enabled) {
    return false;
  }

  if (cfg.devWalletSwap.targetMint !== cfg.pumpCreatorRewards.mint) {
    return false;
  }

  return BigInt(workerState?.pumpCreatorRewards?.pendingTreasuryLamports || '0') > 0n
    || BigInt(workerState?.pumpCreatorRewards?.pendingBurnBuybackLamports || '0') > 0n
    || BigInt(workerState?.pumpCreatorRewards?.pendingRewardsVaultLamports || '0') > 0n
    || BigInt(workerState?.pumpCreatorRewards?.pendingBurnAmount || '0') > 0n;
}

function shouldAttemptPendingBurn(state) {
  const pendingBurnAmount = BigInt(state?.pendingBurnAmount || '0');
  if (pendingBurnAmount <= 0n) {
    return false;
  }

  if (state?.status !== 'burning') {
    return true;
  }

  const attemptedAt = state?.lastBurnAttemptedAt ? new Date(state.lastBurnAttemptedAt).getTime() : 0;
  return !attemptedAt || Date.now() - attemptedAt >= DEV_WALLET_SWAP_RETRY_AFTER_MS;
}

async function getOwnedTokenAccountsForMint(ownerPublicKey, mintAddress) {
  const mintPubkey = new PublicKey(mintAddress);
  const tokenProgram = await getMintTokenProgram(mintAddress);
  const response = await connection.getParsedTokenAccountsByOwner(
    ownerPublicKey,
    { programId: tokenProgram },
    'confirmed',
  );

  return response.value
    .map(({ pubkey, account }) => {
      const parsedInfo = account.data?.parsed?.info;
      const tokenAmount = parsedInfo?.tokenAmount;
      return {
        pubkey,
        programId: account.owner,
        mint: parsedInfo?.mint || null,
        amount: tokenAmount?.amount || '0',
        decimals: tokenAmount?.decimals ?? 0,
      };
    })
    .filter((item) => item.mint === mintPubkey.toBase58())
    .filter((item) => BigInt(item.amount) > 0n)
    .sort((left, right) => {
      const leftAmount = BigInt(left.amount);
      const rightAmount = BigInt(right.amount);
      if (leftAmount === rightAmount) {
        return 0;
      }

      return leftAmount > rightAmount ? -1 : 1;
    });
}

async function getOwnedMintRawBalance(ownerPublicKey, mintAddress) {
  const tokenAccounts = await getOwnedTokenAccountsForMint(ownerPublicKey, mintAddress);
  return tokenAccounts.reduce((sum, account) => sum + BigInt(account.amount), 0n);
}

async function transferOneWholeTokenToWallet(sourceSigner, mintAddress, recipientAddress, decimals, tokenProgram) {
  const tokenAccounts = await getOwnedTokenAccountsForMint(sourceSigner.publicKey, mintAddress);
  const oneWholeTokenRaw = 10n ** BigInt(decimals);
  const sourceAccount = tokenAccounts.find((account) => BigInt(account.amount) >= oneWholeTokenRaw);
  if (!sourceAccount) {
    throw new Error('Not enough token balance is available to fan out one full token.');
  }

  const mint = new PublicKey(mintAddress);
  const recipientOwner = new PublicKey(recipientAddress);
  const recipientAta = getAssociatedTokenAddressSync(
    mint,
    recipientOwner,
    false,
    tokenProgram,
    ASSOCIATED_TOKEN_PROGRAM_ID,
  );
  const instructions = [];
  const ataInfo = await connection.getAccountInfo(recipientAta, 'confirmed');
  if (!ataInfo) {
    instructions.push(
      createAssociatedTokenAccountInstruction(
        sourceSigner.publicKey,
        recipientAta,
        recipientOwner,
        mint,
        tokenProgram,
        ASSOCIATED_TOKEN_PROGRAM_ID,
      ),
    );
  }

  instructions.push(
    createTransferCheckedInstruction(
      sourceAccount.pubkey,
      mint,
      recipientAta,
      sourceSigner.publicKey,
      oneWholeTokenRaw,
      decimals,
      [],
      tokenProgram,
    ),
  );

  return sendLegacyTransaction(instructions, sourceSigner);
}

async function transferMintAmountToWallet(sourceSigner, mintAddress, recipientAddress, amountRaw, decimals, tokenProgram) {
  const targetAmount = BigInt(String(amountRaw || '0'));
  if (targetAmount <= 0n) {
    throw new Error('Token transfer amount must be greater than zero.');
  }

  const tokenAccounts = await getOwnedTokenAccountsForMint(sourceSigner.publicKey, mintAddress);
  const sourceAccount = tokenAccounts.find((account) => BigInt(account.amount) >= targetAmount);
  if (!sourceAccount) {
    throw new Error('Not enough token balance is available for the requested Magic Sell transfer.');
  }

  const mint = new PublicKey(mintAddress);
  const recipientOwner = new PublicKey(recipientAddress);
  const recipientAta = getAssociatedTokenAddressSync(
    mint,
    recipientOwner,
    false,
    tokenProgram,
    ASSOCIATED_TOKEN_PROGRAM_ID,
  );
  const instructions = [];
  const ataInfo = await connection.getAccountInfo(recipientAta, 'confirmed');
  if (!ataInfo) {
    instructions.push(
      createAssociatedTokenAccountInstruction(
        sourceSigner.publicKey,
        recipientAta,
        recipientOwner,
        mint,
        tokenProgram,
        ASSOCIATED_TOKEN_PROGRAM_ID,
      ),
    );
  }

  instructions.push(
    createTransferCheckedInstruction(
      sourceAccount.pubkey,
      mint,
      recipientAta,
      sourceSigner.publicKey,
      targetAmount,
      decimals,
      [],
      tokenProgram,
    ),
  );

  return sendLegacyTransaction(instructions, sourceSigner);
}

async function burnMintTokens(signer, mintAddress, requestedAmountRaw = null) {
  const tokenAccounts = await getOwnedTokenAccountsForMint(signer.publicKey, mintAddress);
  const availableAmount = tokenAccounts.reduce((sum, account) => sum + BigInt(account.amount), 0n);
  const targetAmount = requestedAmountRaw ? BigInt(requestedAmountRaw) : availableAmount;

  if (availableAmount <= 0n) {
    throw new Error('No target tokens are available to burn.');
  }

  if (targetAmount <= 0n) {
    return {
      burnedAmount: '0',
      signatures: [],
      remainingAmount: availableAmount.toString(),
    };
  }

  if (availableAmount < targetAmount) {
    throw new Error(`Only ${availableAmount} raw units are available to burn, below requested ${targetAmount}.`);
  }

  let remaining = targetAmount;
  let burned = 0n;
  const signatures = [];

  try {
    for (const account of tokenAccounts) {
      if (remaining <= 0n) {
        break;
      }

      const accountAmount = BigInt(account.amount);
      const burnAmount = accountAmount < remaining ? accountAmount : remaining;
      if (burnAmount <= 0n) {
        continue;
      }

      const latestBlockhash = await connection.getLatestBlockhash('confirmed');
      const transaction = new Transaction({
        feePayer: signer.publicKey,
        lastValidBlockHeight: latestBlockhash.lastValidBlockHeight,
        recentBlockhash: latestBlockhash.blockhash,
      }).add(
        createBurnCheckedInstruction(
          account.pubkey,
          new PublicKey(mintAddress),
          signer.publicKey,
          burnAmount,
          account.decimals,
          [],
          account.programId,
        ),
      );

      const signature = await connection.sendTransaction(transaction, [signer], {
        skipPreflight: true,
        preflightCommitment: 'confirmed',
        maxRetries: 3,
      });

      await confirmTransactionByMetadata(
        signature,
        latestBlockhash.blockhash,
        latestBlockhash.lastValidBlockHeight,
        'confirmed',
      );

      signatures.push(signature);
      burned += burnAmount;
      remaining -= burnAmount;
    }
  } catch (error) {
    error.burnedAmount = burned.toString();
    error.remainingAmount = remaining.toString();
    error.signatures = signatures;
    throw error;
  }

  if (remaining > 0n) {
    const error = new Error(`Burn completed partially; ${remaining} raw units are still pending.`);
    error.burnedAmount = burned.toString();
    error.remainingAmount = remaining.toString();
    error.signatures = signatures;
    throw error;
  }

  return {
    burnedAmount: burned.toString(),
    signatures,
    remainingAmount: remaining.toString(),
  };
}

function isBurnAgentConfigured(agent) {
  const hasWalletCore = Boolean(
    agent
    && agent.speed
    && agent.walletAddress
    && agent.walletSecretKeyB64
    && agent.mintAddress,
  );

  if (!hasWalletCore) {
    return false;
  }

  if (agent.speed === 'normal') {
    return true;
  }

  return Boolean(
    agent.treasuryAddress
    && Number.isInteger(agent.burnPercent)
    && Number.isInteger(agent.treasuryPercent)
    && agent.burnPercent >= 0
    && agent.treasuryPercent >= 0
    && agent.burnPercent + agent.treasuryPercent === 100,
  );
}

function buildBurnAgentContext(userId, agent) {
  const normalizedAgent = normalizeBurnAgentRecord(agent);
  const runtime = normalizeBurnAgentRuntime(normalizedAgent.runtime);

  if (!isBurnAgentConfigured(normalizedAgent)) {
    return {
      enabled: false,
      userId,
      agentId: normalizedAgent.id,
      agent: normalizedAgent,
      runtime,
      reason: 'Burn Agent is not fully configured yet.',
    };
  }

  try {
    const signer = decodeOrderWallet(normalizedAgent.walletSecretKeyB64);
    if (signer.publicKey.toBase58() !== normalizedAgent.walletAddress) {
      throw new Error('Burn Agent wallet secret does not match the stored wallet address.');
    }

    const burnPercent = normalizedAgent.speed === 'normal' ? 100 : normalizedAgent.burnPercent;
    const treasuryPercent = normalizedAgent.speed === 'normal' ? 0 : normalizedAgent.treasuryPercent;

    return {
      enabled: true,
      userId,
      agentId: normalizedAgent.id,
      agent: normalizedAgent,
      runtime,
      signer,
      walletAddress: signer.publicKey.toBase58(),
      mintAddress: new PublicKey(normalizedAgent.mintAddress).toBase58(),
      treasuryAddress: normalizedAgent.treasuryAddress
        ? new PublicKey(normalizedAgent.treasuryAddress).toBase58()
        : null,
      burnPercent,
      treasuryPercent,
    };
  } catch (error) {
    return {
      enabled: false,
      userId,
      agentId: normalizedAgent.id,
      agent: normalizedAgent,
      runtime,
      reason: String(error.message || error),
    };
  }
}

function describeBurnAgentContext(context) {
  return `user ${context.userId}${context.agentId ? ` / agent ${context.agentId}` : ''}`;
}

function shouldRunBurnAgent(agent) {
  return Boolean(agent && agent.automationEnabled && !agent.archivedAt);
}

function shouldAttemptBurnAgentScan(runtime) {
  const lastCheckedAt = runtime?.lastCheckedAt ? new Date(runtime.lastCheckedAt).getTime() : 0;
  const intervalMs = runtime?.intervalMs ?? BURN_AGENT_INTERVAL_MS;
  return !lastCheckedAt || Date.now() - lastCheckedAt >= intervalMs;
}

function shouldAttemptBurnAgentPendingBurn(runtime) {
  const pendingBurnAmount = BigInt(runtime?.pendingBurnAmount || '0');
  if (pendingBurnAmount <= 0n) {
    return false;
  }

  if (runtime?.status !== 'burning') {
    return true;
  }

  const attemptedAt = runtime?.lastBurnAttemptedAt ? new Date(runtime.lastBurnAttemptedAt).getTime() : 0;
  return !attemptedAt || Date.now() - attemptedAt >= BURN_AGENT_RETRY_AFTER_MS;
}

function calculateBurnAgentDistributableLamports(
  balanceBeforeLamports,
  balanceAfterLamports,
  feeReserveLamports = BURN_AGENT_FEE_RESERVE_LAMPORTS,
) {
  const retainedBalanceTarget = Math.max(balanceBeforeLamports, feeReserveLamports);
  return Math.max(0, balanceAfterLamports - retainedBalanceTarget);
}

async function processUserBurnAgentPendingBurn(context, requestedAmountRaw = null) {
  const requestedAmount = requestedAmountRaw ? BigInt(requestedAmountRaw) : 0n;

  await updateBurnAgentRuntime(context.userId, context.agentId, (draft) => ({
    ...draft,
    status: 'burning',
    lastBurnAttemptedAt: new Date().toISOString(),
    lastBurnAmount: requestedAmount > 0n ? requestedAmount.toString() : draft.lastBurnAmount,
    lastBurnSignature: null,
    lastBurnError: null,
    lastError: null,
  }));

  try {
    const result = await burnMintTokens(
      context.signer,
      context.mintAddress,
      requestedAmount > 0n ? requestedAmount.toString() : null,
    );

    await updateBurnAgentRuntime(context.userId, context.agentId, (draft) => ({
      ...draft,
      status: 'completed',
      pendingBurnAmount: null,
      lastBurnProcessedAt: new Date().toISOString(),
      lastBurnAmount: result.burnedAmount,
      lastBurnSignature: result.signatures[result.signatures.length - 1] ?? null,
      totalBurnCount: (draft.totalBurnCount || 0) + 1,
      totalBurnedRawAmount: (BigInt(draft.totalBurnedRawAmount || '0') + BigInt(result.burnedAmount)).toString(),
      lastBurnError: null,
      lastError: null,
    }));
    await appendUserActivityLog(context.userId, {
      scope: `burn_agent:${context.agentId}`,
      level: 'info',
      message: `Burn Agent burned ${result.burnedAmount} raw units of ${context.mintAddress}.`,
    });

    console.log(
      `[worker] Burn Agent burn completed for ${describeBurnAgentContext(context)}: ${result.burnedAmount} raw units of ${context.mintAddress} burned (${result.signatures.join(', ')})`,
    );
  } catch (error) {
    const burnedAmount = BigInt(error?.burnedAmount || '0');
    const remainingAmount = error?.remainingAmount
      ? BigInt(error.remainingAmount)
      : (requestedAmount > burnedAmount ? requestedAmount - burnedAmount : 0n);

    await updateBurnAgentRuntime(context.userId, context.agentId, (draft) => ({
      ...draft,
      status: 'burn_failed',
      pendingBurnAmount: remainingAmount > 0n ? remainingAmount.toString() : draft.pendingBurnAmount,
      lastBurnAmount: burnedAmount > 0n ? burnedAmount.toString() : draft.lastBurnAmount,
      lastBurnSignature: error?.signatures?.[error.signatures.length - 1] ?? draft.lastBurnSignature,
      lastBurnError: String(error.message || error),
      lastError: String(error.message || error),
    }));
    await appendUserActivityLog(context.userId, {
      scope: `burn_agent:${context.agentId}`,
      level: 'error',
      message: `Burn Agent burn failed: ${String(error.message || error)}`,
    });

    console.error(`[worker] Burn Agent burn failed for ${describeBurnAgentContext(context)}:`, error.message || error);
  }
}

async function claimUserBurnAgentFees(context, vaultBalanceBefore = null) {
  const claimableBefore = vaultBalanceBefore ?? await pumpOnlineSdk.getCreatorVaultBalanceBothPrograms(
    context.signer.publicKey,
  );
  const balanceBeforeLamports = await connection.getBalance(context.signer.publicKey, 'confirmed');
  const instructions = await pumpOnlineSdk.collectCoinCreatorFeeInstructions(
    context.signer.publicKey,
    context.signer.publicKey,
  );

  await updateBurnAgentRuntime(context.userId, context.agentId, (draft) => ({
    ...draft,
    status: 'claiming',
    lastClaimAttemptedAt: new Date().toISOString(),
    lastClaimSignature: null,
    lastError: null,
  }));

  const signature = await sendLegacyTransaction(instructions, context.signer);
  const [claimableAfter, balanceAfterLamports] = await Promise.all([
    pumpOnlineSdk.getCreatorVaultBalanceBothPrograms(context.signer.publicKey),
    connection.getBalance(context.signer.publicKey, 'confirmed'),
  ]);

  const grossClaimedLamportsBigInt = BigInt(claimableBefore.toString()) - BigInt(claimableAfter.toString());
  const grossClaimedLamports = grossClaimedLamportsBigInt > 0n ? Number(grossClaimedLamportsBigInt) : 0;
  const distributableLamports = calculateBurnAgentDistributableLamports(
    balanceBeforeLamports,
    balanceAfterLamports,
    context.runtime.feeReserveLamports,
  );
  const treasuryLamports = Math.floor((distributableLamports * context.treasuryPercent) / 100);
  const buybackLamports = distributableLamports - treasuryLamports;

  const updatedRuntime = await updateBurnAgentRuntime(context.userId, context.agentId, (draft) => ({
    ...draft,
    status: treasuryLamports > 0 || buybackLamports > 0 ? 'payout_pending' : 'completed',
    lastClaimedAt: new Date().toISOString(),
    lastClaimedLamports: String(distributableLamports),
    lastClaimSignature: signature,
    lastVaultLamports: claimableAfter.toString(),
    pendingTreasuryLamports: treasuryLamports > 0 ? String(treasuryLamports) : null,
    pendingBuybackLamports: buybackLamports > 0 ? String(buybackLamports) : null,
    totalClaimCount: (draft.totalClaimCount || 0) + 1,
    totalClaimedLamports: (draft.totalClaimedLamports || 0) + distributableLamports,
    lastError: null,
  }));
  await appendUserActivityLog(context.userId, {
    scope: `burn_agent:${context.agentId}`,
    level: 'info',
    message: `Burn Agent claimed ${formatSolAmountFromLamports(distributableLamports)} SOL in creator rewards.`,
  });

  console.log(
    `[worker] Burn Agent claimed creator fees for ${describeBurnAgentContext(context)}: gross ${formatSolAmountFromLamports(grossClaimedLamports)} SOL, distributable ${formatSolAmountFromLamports(distributableLamports)} SOL (${signature})`,
  );

  if (treasuryLamports > 0 || buybackLamports > 0) {
    await processUserBurnAgentPendingPayouts(context, updatedRuntime);
  }
}

async function transferUserBurnAgentTreasuryShare(context, lamports) {
  if (!Number.isInteger(lamports) || lamports <= 0) {
    return null;
  }

  await updateBurnAgentRuntime(context.userId, context.agentId, (draft) => ({
    ...draft,
    status: 'treasury_transfer',
    lastTreasuryAttemptedAt: new Date().toISOString(),
    lastTreasurySignature: null,
    lastError: null,
  }));

  const signature = await sendLegacyTransaction([
    SystemProgram.transfer({
      fromPubkey: context.signer.publicKey,
      toPubkey: new PublicKey(context.treasuryAddress),
      lamports,
    }),
  ], context.signer);

  await updateBurnAgentRuntime(context.userId, context.agentId, (draft) => ({
    ...draft,
    status: draft.pendingBuybackLamports ? 'payout_pending' : 'completed',
    pendingTreasuryLamports: null,
    lastTreasuryProcessedAt: new Date().toISOString(),
    lastTreasurySignature: signature,
    totalTreasuryTransferCount: (draft.totalTreasuryTransferCount || 0) + 1,
    totalTreasuryLamportsSent: (draft.totalTreasuryLamportsSent || 0) + lamports,
    lastError: null,
  }));
  await appendUserActivityLog(context.userId, {
    scope: `burn_agent:${context.agentId}`,
    level: 'info',
    message: `Burn Agent sent ${formatSolAmountFromLamports(lamports)} SOL to treasury.`,
  });

  console.log(
    `[worker] Burn Agent treasury transfer completed for ${describeBurnAgentContext(context)}: ${formatSolAmountFromLamports(lamports)} SOL -> ${context.treasuryAddress} (${signature})`,
  );

  return signature;
}

async function executeUserBurnAgentBuyback(context, lamportsRaw) {
  const mint = new PublicKey(context.mintAddress);
  const mintTokenProgram = await getMintTokenProgram(context.mintAddress);
  const tokenBalanceBefore = await getOwnedMintRawBalance(context.signer.publicKey, context.mintAddress);

  let signature = null;
  let mode = 'bonding_curve';

  const {
    bondingCurveAccountInfo,
    bondingCurve,
    associatedUserAccountInfo,
  } = await pumpOnlineSdk.fetchBuyState(
    mint,
    context.signer.publicKey,
    mintTokenProgram,
  );

  if (bondingCurve.complete) {
    const poolKey = canonicalPumpPoolPda(mint);
    const swapState = await pumpAmmOnlineSdk.swapSolanaState(poolKey, context.signer.publicKey);
    const instructions = await PUMP_AMM_SDK.buyQuoteInput(
      swapState,
      new BN(lamportsRaw),
      context.runtime.buySlippagePercent,
    );
    signature = await sendLegacyTransaction(instructions, context.signer);
    mode = 'pumpswap';
  } else {
    const global = await pumpOnlineSdk.fetchGlobal();
    const feeConfig = await pumpOnlineSdk.fetchFeeConfig();
    const amount = getBuyTokenAmountFromSolAmount({
      global,
      feeConfig,
      mintSupply: bondingCurve.tokenTotalSupply,
      bondingCurve,
      amount: new BN(lamportsRaw),
    });

    if (amount.lte(new BN(0))) {
      throw new Error('Burn Agent buyback quote returned zero tokens.');
    }

    const instructions = await PUMP_SDK.buyInstructions({
      global,
      bondingCurveAccountInfo,
      bondingCurve,
      associatedUserAccountInfo,
      mint,
      user: context.signer.publicKey,
      amount,
      solAmount: new BN(lamportsRaw),
      slippage: context.runtime.buySlippagePercent,
      tokenProgram: mintTokenProgram,
    });
    signature = await sendLegacyTransaction(instructions, context.signer);
  }

  const tokenBalanceAfter = await getOwnedMintRawBalance(context.signer.publicKey, context.mintAddress);
  const purchasedRawAmount = tokenBalanceAfter > tokenBalanceBefore
    ? tokenBalanceAfter - tokenBalanceBefore
    : 0n;

  if (purchasedRawAmount <= 0n) {
    throw new Error('Burn Agent buyback completed but no token balance increase was detected.');
  }

  await updateBurnAgentRuntime(context.userId, context.agentId, (draft) => ({
    ...draft,
    status: 'burn_pending',
    pendingBuybackLamports: null,
    pendingBurnAmount: purchasedRawAmount.toString(),
    lastBuybackProcessedAt: new Date().toISOString(),
    lastBuybackSignature: signature,
    lastBuybackMode: mode,
    lastBuybackTokenProgram: mintTokenProgram.toBase58(),
    lastBuybackRawAmount: purchasedRawAmount.toString(),
    totalBuybackCount: (draft.totalBuybackCount || 0) + 1,
    totalBuybackLamports: (draft.totalBuybackLamports || 0) + lamportsRaw,
    lastError: null,
  }));
  await appendUserActivityLog(context.userId, {
    scope: `burn_agent:${context.agentId}`,
    level: 'info',
    message: `Burn Agent used ${formatSolAmountFromLamports(lamportsRaw)} SOL for a buyback via ${mode}.`,
  });

  console.log(
    `[worker] Burn Agent buyback completed for ${describeBurnAgentContext(context)}: ${formatSolAmountFromLamports(lamportsRaw)} SOL -> ${context.mintAddress} via ${mode} (${signature})`,
  );

  await processUserBurnAgentPendingBurn(context, purchasedRawAmount.toString());
}

async function processUserBurnAgentPendingPayouts(context, runtime) {
  const pendingTreasuryLamports = Number.parseInt(
    String(runtime?.pendingTreasuryLamports || '0'),
    10,
  );
  const pendingBuybackLamports = Number.parseInt(
    String(runtime?.pendingBuybackLamports || '0'),
    10,
  );

  if (pendingTreasuryLamports > 0) {
    await transferUserBurnAgentTreasuryShare(context, pendingTreasuryLamports);
  }

  if (pendingBuybackLamports > 0) {
    await updateBurnAgentRuntime(context.userId, context.agentId, (draft) => ({
      ...draft,
      status: 'buying_back',
      lastBuybackAttemptedAt: new Date().toISOString(),
      lastBuybackSignature: null,
      lastError: null,
    }));

    await executeUserBurnAgentBuyback(context, pendingBuybackLamports);
  }
}

async function scanUserBurnAgents() {
  const store = await readStore();

  for (const [userId, user] of Object.entries(store.users ?? {})) {
    for (const agent of normalizeUserBurnAgents(user)) {
      if (!shouldRunBurnAgent(agent)) {
        continue;
      }

      const context = buildBurnAgentContext(userId, agent);
      if (!context.enabled) {
        continue;
      }

      const currentRuntime = normalizeBurnAgentRuntime(context.runtime);
      const hasPendingActions = BigInt(currentRuntime.pendingTreasuryLamports || '0') > 0n
        || BigInt(currentRuntime.pendingBuybackLamports || '0') > 0n
        || BigInt(currentRuntime.pendingBurnAmount || '0') > 0n;

      if (!hasPendingActions && !shouldAttemptBurnAgentScan(currentRuntime)) {
        continue;
      }

      try {
        const vaultLamports = await pumpOnlineSdk.getCreatorVaultBalanceBothPrograms(context.signer.publicKey);
        await updateBurnAgentRuntime(userId, context.agentId, (draft) => ({
          ...draft,
          lastCheckedAt: new Date().toISOString(),
          lastVaultLamports: vaultLamports.toString(),
          totalClaimChecks: (draft.totalClaimChecks || 0) + 1,
          lastError: hasPendingActions ? draft.lastError : null,
        }));

        if (shouldAttemptBurnAgentPendingBurn(currentRuntime)) {
          await processUserBurnAgentPendingBurn(context, currentRuntime.pendingBurnAmount);
          continue;
        }

        if (
          BigInt(currentRuntime.pendingTreasuryLamports || '0') > 0n
          || BigInt(currentRuntime.pendingBuybackLamports || '0') > 0n
        ) {
          await processUserBurnAgentPendingPayouts(context, currentRuntime);
          continue;
        }

        const strayMintBalance = await getOwnedMintRawBalance(
          context.signer.publicKey,
          context.mintAddress,
        );
        if (strayMintBalance > 0n) {
          await updateBurnAgentRuntime(userId, context.agentId, (draft) => ({
            ...draft,
            pendingBurnAmount: strayMintBalance.toString(),
          }));
          await processUserBurnAgentPendingBurn(context, strayMintBalance.toString());
          continue;
        }

        if (vaultLamports.lte(new BN(currentRuntime.minimumClaimLamports))) {
          continue;
        }

        await claimUserBurnAgentFees(context, vaultLamports);
      } catch (error) {
        await updateBurnAgentRuntime(userId, context.agentId, (draft) => ({
          ...draft,
          status: 'failed',
          lastError: String(error.message || error),
        }));
        await appendUserActivityLog(userId, {
          scope: `burn_agent:${context.agentId}`,
          level: 'error',
          message: `Burn Agent error: ${String(error.message || error)}`,
        });
        console.error(`[worker] Burn Agent failed for ${describeBurnAgentContext(context)}:`, error.message || error);
      }
    }
  }
}

async function processPendingBurn(requestedAmountRaw = null) {
  const requestedAmount = requestedAmountRaw
    ? BigInt(requestedAmountRaw)
    : 0n;

  await updateWorkerState((draft) => ({
    ...draft,
    devWalletSwap: {
      ...draft.devWalletSwap,
      status: 'burning',
      lastBurnAttemptedAt: new Date().toISOString(),
      lastBurnAmount: requestedAmount > 0n ? requestedAmount.toString() : draft.devWalletSwap.lastBurnAmount,
      lastBurnSignature: null,
      lastBurnError: null,
    },
  }));

  try {
    const result = await burnMintTokens(
      cfg.devWalletSwap.signer,
      cfg.devWalletSwap.targetMint,
      requestedAmount > 0n ? requestedAmount.toString() : null,
    );
    await updateWorkerState((draft) => ({
      ...draft,
      devWalletSwap: {
        ...draft.devWalletSwap,
        status: 'completed',
        pendingBurnAmount: null,
        lastBurnProcessedAt: new Date().toISOString(),
        lastBurnAmount: result.burnedAmount,
        lastBurnSignature: result.signatures[result.signatures.length - 1] ?? null,
        lastBurnError: null,
      },
    }));

    console.log(
      `[worker] Dev wallet burn completed: ${result.burnedAmount} raw units of ${cfg.devWalletSwap.targetMint} burned (${result.signatures.join(', ')})`,
    );
  } catch (error) {
    const burnedAmount = BigInt(error?.burnedAmount || '0');
    const remainingAmount = error?.remainingAmount
      ? BigInt(error.remainingAmount)
      : (requestedAmount > burnedAmount ? requestedAmount - burnedAmount : 0n);

    await updateWorkerState((draft) => ({
      ...draft,
      devWalletSwap: {
        ...draft.devWalletSwap,
        status: 'burn_failed',
        pendingBurnAmount: remainingAmount > 0n
          ? remainingAmount.toString()
          : draft.devWalletSwap.pendingBurnAmount,
        lastBurnAmount: burnedAmount > 0n ? burnedAmount.toString() : draft.devWalletSwap.lastBurnAmount,
        lastBurnSignature: error?.signatures?.[error.signatures.length - 1] ?? draft.devWalletSwap.lastBurnSignature,
        lastBurnError: String(error.message || error),
      },
    }));

    console.error('[worker] Dev wallet burn failed:', error.message || error);
  }
}

function shouldAttemptPumpCreatorScan(state) {
  if (!cfg.pumpCreatorRewards.enabled) {
    return false;
  }

  const lastCheckedAt = state?.lastCheckedAt ? new Date(state.lastCheckedAt).getTime() : 0;
  return !lastCheckedAt || Date.now() - lastCheckedAt >= cfg.pumpCreatorRewards.intervalMs;
}

async function processPumpCreatorPendingBurn(requestedAmountRaw = null) {
  const requestedAmount = requestedAmountRaw
    ? BigInt(requestedAmountRaw)
    : 0n;

  await updateWorkerState((draft) => ({
    ...draft,
    pumpCreatorRewards: {
      ...draft.pumpCreatorRewards,
      status: 'burning',
      lastBurnAttemptedAt: new Date().toISOString(),
      lastBurnAmount: requestedAmount > 0n ? requestedAmount.toString() : draft.pumpCreatorRewards.lastBurnAmount,
      lastBurnSignature: null,
      lastBurnError: null,
      lastError: null,
    },
  }));

  try {
    const result = await burnMintTokens(
      cfg.devWalletSigner,
      cfg.pumpCreatorRewards.mint,
      requestedAmount > 0n ? requestedAmount.toString() : null,
    );

    await updateWorkerState((draft) => ({
      ...draft,
      pumpCreatorRewards: {
        ...draft.pumpCreatorRewards,
        status: 'completed',
        pendingBurnAmount: null,
        lastBurnProcessedAt: new Date().toISOString(),
        lastBurnAmount: result.burnedAmount,
        lastBurnSignature: result.signatures[result.signatures.length - 1] ?? null,
        lastBurnError: null,
      },
    }));

    console.log(
      `[worker] Pump creator burn completed: ${result.burnedAmount} raw units of ${cfg.pumpCreatorRewards.mint} burned (${result.signatures.join(', ')})`,
    );
  } catch (error) {
    const burnedAmount = BigInt(error?.burnedAmount || '0');
    const remainingAmount = error?.remainingAmount
      ? BigInt(error.remainingAmount)
      : (requestedAmount > burnedAmount ? requestedAmount - burnedAmount : 0n);

    await updateWorkerState((draft) => ({
      ...draft,
      pumpCreatorRewards: {
        ...draft.pumpCreatorRewards,
        status: 'burn_failed',
        pendingBurnAmount: remainingAmount > 0n
          ? remainingAmount.toString()
          : draft.pumpCreatorRewards.pendingBurnAmount,
        lastBurnAmount: burnedAmount > 0n
          ? burnedAmount.toString()
          : draft.pumpCreatorRewards.lastBurnAmount,
        lastBurnSignature: error?.signatures?.[error.signatures.length - 1]
          ?? draft.pumpCreatorRewards.lastBurnSignature,
        lastBurnError: String(error.message || error),
        lastError: String(error.message || error),
      },
    }));

    console.error('[worker] Pump creator burn failed:', error.message || error);
  }
}

async function claimPumpCreatorFees(vaultBalanceBefore = null) {
  const signer = cfg.devWalletSigner;
  const claimableBefore = vaultBalanceBefore ?? await pumpOnlineSdk.getCreatorVaultBalanceBothPrograms(
    signer.publicKey,
  );
  const instructions = await pumpOnlineSdk.collectCoinCreatorFeeInstructions(
    signer.publicKey,
    signer.publicKey,
  );

  await updateWorkerState((draft) => ({
    ...draft,
    pumpCreatorRewards: {
      ...draft.pumpCreatorRewards,
      status: 'claiming',
      lastClaimAttemptedAt: new Date().toISOString(),
      lastClaimSignature: null,
      lastError: null,
    },
  }));

  const signature = await sendLegacyTransaction(instructions, signer);
  const claimableAfter = await pumpOnlineSdk.getCreatorVaultBalanceBothPrograms(signer.publicKey);
  const claimedLamportsBigInt = BigInt(claimableBefore.toString()) - BigInt(claimableAfter.toString());
  const claimedLamports = claimedLamportsBigInt > 0n ? Number(claimedLamportsBigInt) : 0;
  const route = calculatePlatformRevenueRoute(claimedLamports);

  await updateWorkerState((draft) => ({
    ...draft,
    pumpCreatorRewards: {
      ...draft.pumpCreatorRewards,
      status: route.treasuryLamports > 0 || route.burnLamports > 0 || route.rewardsLamports > 0
        ? 'payout_pending'
        : 'completed',
      lastClaimedAt: new Date().toISOString(),
      lastClaimedLamports: String(claimedLamports),
      lastClaimSignature: signature,
      lastVaultLamports: claimableAfter.toString(),
      pendingTreasuryLamports: route.treasuryLamports > 0 ? String(route.treasuryLamports) : null,
      pendingBurnBuybackLamports: route.burnLamports > 0 ? String(route.burnLamports) : null,
      pendingRewardsVaultLamports: route.rewardsLamports > 0 ? String(route.rewardsLamports) : null,
      lastError: null,
    },
  }));

  console.log(
    `[worker] Pump creator fees claimed: ${formatSolAmountFromLamports(claimedLamports)} SOL (${signature})`,
  );

  if (route.treasuryLamports > 0 || route.burnLamports > 0 || route.rewardsLamports > 0) {
    const refreshedState = (await readStore()).worker.pumpCreatorRewards;
    await processPumpCreatorPendingPayouts(refreshedState);
  }
}

async function transferPumpCreatorTreasuryShare(lamports) {
  if (!Number.isInteger(lamports) || lamports <= 0) {
    return null;
  }

  if (!cfg.treasuryWalletAddress) {
    throw new Error('Pump creator treasury transfer requires TREASURY_WALLET_ADDRESS.');
  }

  const signer = cfg.devWalletSigner;
  await updateWorkerState((draft) => ({
    ...draft,
    pumpCreatorRewards: {
      ...draft.pumpCreatorRewards,
      status: 'treasury_transfer',
      lastTreasuryAttemptedAt: new Date().toISOString(),
      lastTreasurySignature: null,
      lastError: null,
    },
  }));

  const signature = await sendLegacyTransaction([
    SystemProgram.transfer({
      fromPubkey: signer.publicKey,
      toPubkey: new PublicKey(cfg.treasuryWalletAddress),
      lamports,
    }),
  ], signer);

  await updateWorkerState((draft) => ({
    ...draft,
    pumpCreatorRewards: {
      ...draft.pumpCreatorRewards,
      status: draft.pumpCreatorRewards.pendingBurnBuybackLamports || draft.pumpCreatorRewards.pendingRewardsVaultLamports
        ? 'payout_pending'
        : 'completed',
      pendingTreasuryLamports: null,
      lastTreasuryProcessedAt: new Date().toISOString(),
      lastTreasurySignature: signature,
      lastError: null,
    },
  }));

  console.log(
    `[worker] Pump creator treasury transfer completed: ${formatSolAmountFromLamports(lamports)} SOL -> treasury (${signature})`,
  );

  return signature;
}

async function executePumpCreatorBurnBuyback(lamportsRaw) {
  const signer = cfg.devWalletSigner;
  const buyback = await buyPlatformTokenWithSigner(signer, lamportsRaw, {
    mintAddress: cfg.pumpCreatorRewards.mint,
    slippagePercent: cfg.pumpCreatorRewards.buySlippagePercent,
  });
  await updateWorkerState((draft) => ({
    ...draft,
    pumpCreatorRewards: {
      ...draft.pumpCreatorRewards,
      status: 'burn_pending',
      pendingBurnBuybackLamports: null,
      pendingBurnAmount: buyback.purchasedRawAmount.toString(),
      lastBuybackProcessedAt: new Date().toISOString(),
      lastBuybackSignature: buyback.signature,
      lastBuybackMode: buyback.mode,
      lastBuybackTokenProgram: buyback.mintTokenProgram.toBase58(),
      lastBuybackRawAmount: buyback.purchasedRawAmount.toString(),
      lastError: null,
    },
  }));

  console.log(
    `[worker] Pump creator burn-side buyback completed: ${formatSolAmountFromLamports(lamportsRaw)} SOL -> ${cfg.pumpCreatorRewards.mint} via ${buyback.mode} (${buyback.signature})`,
  );

  await processPumpCreatorPendingBurn(buyback.purchasedRawAmount.toString());
}

async function executePumpCreatorRewardsVaultTransfer(lamportsRaw) {
  const signer = cfg.devWalletSigner;
  await updateWorkerState((draft) => ({
    ...draft,
    pumpCreatorRewards: {
      ...draft.pumpCreatorRewards,
      status: 'rewards_vault_transfer',
      pendingRewardsVaultLamports: null,
      lastRewardsVaultAttemptedAt: new Date().toISOString(),
      lastError: null,
    },
  }));

  const signature = await transferSolFromSigner(
    signer,
    cfg.platformRevenue.rewardsVaultAddress,
    lamportsRaw,
  );

  await updateWorkerState((draft) => ({
    ...draft,
    pumpCreatorRewards: {
      ...draft.pumpCreatorRewards,
      status: draft.pumpCreatorRewards.pendingBurnAmount ? 'burn_pending' : 'completed',
      lastRewardsVaultProcessedAt: new Date().toISOString(),
      lastRewardsVaultSignature: signature,
      lastRewardsVaultRawAmount: String(lamportsRaw),
      lastError: null,
    },
  }));

  console.log(
    `[worker] Pump creator rewards-vault transfer completed: ${formatSolAmountFromLamports(lamportsRaw)} SOL -> vault ${cfg.platformRevenue.rewardsVaultAddress} (${signature})`,
  );
}

async function processPumpCreatorPendingPayouts(state) {
  const pendingTreasuryLamports = Number.parseInt(
    String(state?.pendingTreasuryLamports || '0'),
    10,
  );
  const pendingBurnBuybackLamports = Number.parseInt(
    String(state?.pendingBurnBuybackLamports || '0'),
    10,
  );
  const pendingRewardsVaultLamports = Number.parseInt(
    String(state?.pendingRewardsVaultLamports || '0'),
    10,
  );

  if (pendingTreasuryLamports > 0) {
    await transferPumpCreatorTreasuryShare(pendingTreasuryLamports);
  }

  if (pendingBurnBuybackLamports > 0) {
    await updateWorkerState((draft) => ({
      ...draft,
      pumpCreatorRewards: {
        ...draft.pumpCreatorRewards,
        status: 'buying_back',
        lastBuybackAttemptedAt: new Date().toISOString(),
        lastBuybackSignature: null,
        lastError: null,
      },
    }));

    await executePumpCreatorBurnBuyback(pendingBurnBuybackLamports);
  }

  if (pendingRewardsVaultLamports > 0) {
    await executePumpCreatorRewardsVaultTransfer(pendingRewardsVaultLamports);
  }
}

async function scanPumpCreatorRewards() {
  if (!cfg.pumpCreatorRewards.enabled) {
    return;
  }

  const store = await readStore();
  const currentState = store.worker?.pumpCreatorRewards ?? createDefaultWorkerState().pumpCreatorRewards;
  const hasPendingActions = BigInt(currentState.pendingTreasuryLamports || '0') > 0n
    || BigInt(currentState.pendingBurnBuybackLamports || '0') > 0n
    || BigInt(currentState.pendingRewardsVaultLamports || '0') > 0n
    || BigInt(currentState.pendingBurnAmount || '0') > 0n;

  if (!hasPendingActions && !shouldAttemptPumpCreatorScan(currentState)) {
    return;
  }

  try {
    let vaultLamports = new BN(0);
    try {
      vaultLamports = await pumpOnlineSdk.getCreatorVaultBalanceBothPrograms(cfg.devWalletSigner.publicKey);
    } catch (error) {
      const name = String(error?.name || '');
      const message = String(error?.message || error || '');
      if (!name.includes('TokenAccountNotFoundError') && !message.includes('TokenAccountNotFoundError')) {
        throw error;
      }
    }

    await updateWorkerState((draft) => ({
      ...draft,
      pumpCreatorRewards: {
        ...draft.pumpCreatorRewards,
        lastCheckedAt: new Date().toISOString(),
        lastVaultLamports: vaultLamports.toString(),
        lastError: hasPendingActions ? draft.pumpCreatorRewards.lastError : null,
      },
    }));

    if (BigInt(currentState.pendingBurnAmount || '0') > 0n) {
      await processPumpCreatorPendingBurn(currentState.pendingBurnAmount);
      return;
    }

    if (BigInt(currentState.pendingTreasuryLamports || '0') > 0n
      || BigInt(currentState.pendingBurnBuybackLamports || '0') > 0n
      || BigInt(currentState.pendingRewardsVaultLamports || '0') > 0n) {
      await processPumpCreatorPendingPayouts(currentState);
      return;
    }

    const strayCreatorMintBalance = await getOwnedMintRawBalance(
      cfg.devWalletSigner.publicKey,
      cfg.pumpCreatorRewards.mint,
    );
    if (strayCreatorMintBalance > 0n) {
      await updateWorkerState((draft) => ({
        ...draft,
        pumpCreatorRewards: {
          ...draft.pumpCreatorRewards,
          pendingBurnAmount: strayCreatorMintBalance.toString(),
        },
      }));
      await processPumpCreatorPendingBurn(strayCreatorMintBalance.toString());
      return;
    }

    if (vaultLamports.lte(new BN(cfg.pumpCreatorRewards.minimumClaimLamports))) {
      return;
    }

    await claimPumpCreatorFees(vaultLamports);
  } catch (error) {
    await updateWorkerState((draft) => ({
      ...draft,
      pumpCreatorRewards: {
        ...draft.pumpCreatorRewards,
        status: 'failed',
        lastError: String(error.message || error),
      },
    }));
    console.error('[worker] Pump creator rewards failed:', error.message || error);
  }
}

async function fetchJupiterSwapOrderFor({
  inputMint,
  outputMint,
  amount,
  taker,
  apiBaseUrl = JUPITER_SWAP_API_BASE_URL,
  apiKey = JUPITER_API_KEY,
}) {
  if (!apiKey) {
    throw new Error('JUPITER_API_KEY is required for Apple Booster and swap automation.');
  }

  const params = new URLSearchParams({
    inputMint,
    outputMint,
    amount: String(amount),
    taker,
  });

  const response = await fetch(`${apiBaseUrl}/order?${params}`, {
    headers: {
      'x-api-key': apiKey,
    },
  });

  if (!response.ok) {
    throw new Error(`Jupiter /order failed (${response.status}): ${await response.text()}`);
  }

  const order = await response.json();
  if (!order.transaction || !order.requestId) {
    const orderError = order.errorMessage || order.error || `code ${order.errorCode ?? 'unknown'}`;
    throw new Error(`Jupiter order did not return a signable transaction: ${orderError}`);
  }

  return order;
}

async function fetchJupiterSwapOrder(inputLamports) {
  return fetchJupiterSwapOrderFor({
    inputMint: SOL_MINT_ADDRESS,
    outputMint: cfg.devWalletSwap.targetMint,
    amount: inputLamports,
    taker: cfg.devWalletAddress,
    apiBaseUrl: cfg.devWalletSwap.apiBaseUrl,
    apiKey: cfg.devWalletSwap.apiKey,
  });
}

async function executeJupiterSwapFor(order, signer, { apiBaseUrl = JUPITER_SWAP_API_BASE_URL, apiKey = JUPITER_API_KEY } = {}) {
  if (!apiKey) {
    throw new Error('JUPITER_API_KEY is required for Apple Booster and swap automation.');
  }

  const transactionBytes = Buffer.from(order.transaction, 'base64');
  const transaction = VersionedTransaction.deserialize(Uint8Array.from(transactionBytes));
  transaction.sign([signer]);

  const response = await fetch(`${apiBaseUrl}/execute`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': apiKey,
    },
    body: JSON.stringify({
      signedTransaction: Buffer.from(transaction.serialize()).toString('base64'),
      requestId: order.requestId,
    }),
  });

  if (!response.ok) {
    throw new Error(`Jupiter /execute failed (${response.status}): ${await response.text()}`);
  }

  const result = await response.json();
  if (result.status !== 'Success' || result.code !== 0) {
    throw new Error(
      result.error || `Jupiter execute returned status ${result.status || 'unknown'} with code ${result.code}`,
    );
  }

  return result;
}

async function executeJupiterSwap(order) {
  return executeJupiterSwapFor(order, cfg.devWalletSwap.signer, {
    apiBaseUrl: cfg.devWalletSwap.apiBaseUrl,
    apiKey: cfg.devWalletSwap.apiKey,
  });
}

function getJitoHeaders() {
  return {
    'Content-Type': 'application/json',
    ...(JITO_AUTH_KEY ? { 'x-jito-auth': JITO_AUTH_KEY } : {}),
  };
}

async function callJitoRpc(path, method, params = []) {
  const response = await fetch(`${JITO_BLOCK_ENGINE_URL.replace(/\/+$/, '')}${path}`, {
    method: 'POST',
    headers: getJitoHeaders(),
    body: JSON.stringify({
      jsonrpc: '2.0',
      id: 1,
      method,
      params,
    }),
  });

  if (!response.ok) {
    throw new Error(`Jito ${method} failed (${response.status}): ${await response.text()}`);
  }

  const payload = await response.json();
  if (payload.error) {
    throw new Error(payload.error.message || `Jito ${method} returned an error.`);
  }

  return payload.result;
}

async function getJitoTipAccounts() {
  if (
    Array.isArray(jitoTipAccountsCache.accounts)
    && jitoTipAccountsCache.accounts.length > 0
    && (Date.now() - jitoTipAccountsCache.cachedAt) < JITO_TIP_ACCOUNTS_CACHE_MS
  ) {
    return jitoTipAccountsCache.accounts;
  }

  if (jitoTipAccountsInFlight) {
    return jitoTipAccountsInFlight;
  }

  jitoTipAccountsInFlight = callJitoRpc('/api/v1/getTipAccounts', 'getTipAccounts', [])
    .then((result) => {
      if (!Array.isArray(result) || result.length === 0) {
        throw new Error('Jito did not return any tip accounts.');
      }

      jitoTipAccountsCache = {
        accounts: result,
        cachedAt: Date.now(),
      };
      return result;
    })
    .finally(() => {
      jitoTipAccountsInFlight = null;
    });

  return jitoTipAccountsInFlight;
}

async function warmJitoTipAccountsCache() {
  try {
    await getJitoTipAccounts();
  } catch (error) {
    console.warn('[worker] Jito tip-account cache warm failed:', error?.message || error);
  }
}

function chooseRandomTipAccount(accounts) {
  return accounts[Math.floor(Math.random() * accounts.length)];
}

async function createTipTransaction(signer, destinationAddress, lamports) {
  const latestBlockhash = await connection.getLatestBlockhash('processed');
  const message = new TransactionMessage({
    payerKey: signer.publicKey,
    recentBlockhash: latestBlockhash.blockhash,
    instructions: [
    SystemProgram.transfer({
      fromPubkey: signer.publicKey,
      toPubkey: new PublicKey(destinationAddress),
      lamports,
    }),
    ],
  }).compileToV0Message();

  const transaction = new VersionedTransaction(message);
  transaction.sign([signer]);
  return transaction;
}

async function createVersionedTransactionFromInstructions(signer, instructions) {
  const latestBlockhash = await connection.getLatestBlockhash('processed');
  const message = new TransactionMessage({
    payerKey: signer.publicKey,
    recentBlockhash: latestBlockhash.blockhash,
    instructions,
  }).compileToV0Message();

  const transaction = new VersionedTransaction(message);
  transaction.sign([signer]);
  return transaction;
}

async function createVersionedTransactionWithMetadata(
  signer,
  instructions,
  { commitment = 'processed', latestBlockhash = null } = {},
) {
  const blockhashMeta = latestBlockhash || await connection.getLatestBlockhash(commitment);
  const message = new TransactionMessage({
    payerKey: signer.publicKey,
    recentBlockhash: blockhashMeta.blockhash,
    instructions,
  }).compileToV0Message();

  const transaction = new VersionedTransaction(message);
  transaction.sign([signer]);

  return {
    transaction,
    blockhash: blockhashMeta.blockhash,
    lastValidBlockHeight: blockhashMeta.lastValidBlockHeight,
  };
}

async function createVersionedTransactionForSigners(
  payerSigner,
  instructions,
  signers = [],
  { commitment = 'processed', latestBlockhash = null } = {},
) {
  const blockhashMeta = latestBlockhash || await connection.getLatestBlockhash(commitment);
  const message = new TransactionMessage({
    payerKey: payerSigner.publicKey,
    recentBlockhash: blockhashMeta.blockhash,
    instructions,
  }).compileToV0Message();

  const transaction = new VersionedTransaction(message);
  const uniqueSigners = [payerSigner, ...signers].filter(
    (signer, index, array) => signer && array.findIndex((candidate) => candidate.publicKey.equals(signer.publicKey)) === index,
  );
  transaction.sign(uniqueSigners);

  return {
    transaction,
    blockhash: blockhashMeta.blockhash,
    lastValidBlockHeight: blockhashMeta.lastValidBlockHeight,
  };
}

async function sendJitoBundle(transactions) {
  const serializedTransactions = transactions.map((transaction) =>
    Buffer.from(transaction.serialize()).toString('base64'),
  );
  return callJitoRpc('/api/v1/bundles', 'sendBundle', [
    serializedTransactions,
    {
      encoding: 'base64',
    },
  ]);
}

async function sendJitoTransaction(transaction, { bundleOnly = false } = {}) {
  const query = bundleOnly ? '?bundleOnly=true' : '';
  const response = await fetch(`${JITO_BLOCK_ENGINE_URL.replace(/\/+$/, '')}/api/v1/transactions${query}`, {
    method: 'POST',
    headers: getJitoHeaders(),
    body: JSON.stringify({
      jsonrpc: '2.0',
      id: 1,
      method: 'sendTransaction',
      params: [
        Buffer.from(transaction.serialize()).toString('base64'),
        {
          encoding: 'base64',
        },
      ],
    }),
  });

  if (!response.ok) {
    throw new Error(`Jito sendTransaction failed (${response.status}): ${await response.text()}`);
  }

  const payload = await response.json();
  if (payload.error) {
    throw new Error(payload.error.message || 'Jito sendTransaction returned an error.');
  }

  return {
    signature: payload.result,
    bundleId: response.headers.get('x-bundle-id') || null,
  };
}

async function sendHeliusSenderTransaction(transaction) {
  if (!HELIUS_SENDER_ENABLED) {
    throw new Error('Helius Sender is disabled.');
  }

  const response = await fetch(HELIUS_SENDER_URL, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      jsonrpc: '2.0',
      id: 1,
      method: 'sendTransaction',
      params: [
        Buffer.from(transaction.serialize()).toString('base64'),
        {
          encoding: 'base64',
          skipPreflight: true,
          maxRetries: 0,
        },
      ],
    }),
  });

  if (!response.ok) {
    throw new Error(`Helius Sender failed (${response.status}): ${await response.text()}`);
  }

  const payload = await response.json();
  if (payload.error) {
    throw new Error(payload.error.message || 'Helius Sender returned an error.');
  }

  return payload.result;
}

async function confirmTransactionByMetadata(signature, blockhash, lastValidBlockHeight, commitment = 'confirmed') {
  const commitmentRank = {
    processed: 0,
    confirmed: 1,
    finalized: 2,
  };
  const requiredRank = commitmentRank[commitment] ?? commitmentRank.confirmed;
  const startedAt = Date.now();
  let lastStatus = null;

  while (Date.now() - startedAt < 30_000) {
    const response = await connection.getSignatureStatuses([signature], {
      searchTransactionHistory: false,
    });
    const status = response?.value?.[0] ?? null;
    if (status) {
      lastStatus = status;
      if (status.err) {
        throw new Error(`Transaction ${signature} confirmed with error ${JSON.stringify(status.err)}`);
      }
      const currentRank = commitmentRank[status.confirmationStatus] ?? -1;
      if (status.confirmationStatus === 'finalized' || currentRank >= requiredRank) {
        return { value: status };
      }
    }

    const currentBlockHeight = await connection.getBlockHeight(commitment);
    if (currentBlockHeight > lastValidBlockHeight) {
      throw new Error(`Transaction ${signature} expired before reaching ${commitment}.`);
    }

    await sleep(200);
  }

  if (lastStatus?.err) {
    throw new Error(`Transaction ${signature} confirmed with error ${JSON.stringify(lastStatus.err)}`);
  }
  throw new Error(`Timed out polling confirmation for ${signature}.`);
}

async function confirmTransactionByMetadataWithTimeout(
  signature,
  blockhash,
  lastValidBlockHeight,
  commitment = 'confirmed',
  timeoutMs = 4_000,
) {
  let timeoutHandle = null;
  try {
    return await Promise.race([
      confirmTransactionByMetadata(signature, blockhash, lastValidBlockHeight, commitment),
      new Promise((_, reject) => {
        timeoutHandle = setTimeout(() => {
          reject(new Error(`Confirmation soft timeout after ${timeoutMs}ms for ${signature}`));
        }, timeoutMs);
        timeoutHandle.unref?.();
      }),
    ]);
  } finally {
    if (timeoutHandle) {
      clearTimeout(timeoutHandle);
    }
  }
}

async function sendRawTransactionAcrossRpcPool(transaction) {
  const serialized = Buffer.from(transaction.serialize()).toString('base64');
  const rpcPool = connection?.__rpcPool ?? null;
  const endpointUrls = Array.isArray(rpcPool?.getRpcPoolStatus?.().endpoints)
    ? rpcPool.getRpcPoolStatus().endpoints.map((endpoint) => endpoint.url).filter(Boolean)
    : [connection.rpcEndpoint].filter(Boolean);
  const attempts = endpointUrls.map(async (url) => {
    const response = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: Date.now(),
        method: 'sendTransaction',
        params: [
          serialized,
          {
            encoding: 'base64',
            skipPreflight: true,
            maxRetries: 3,
          },
        ],
      }),
    });
    if (!response.ok) {
      throw new Error(`sendTransaction failed on ${url} with status ${response.status}.`);
    }
    const payload = await response.json();
    if (payload?.error) {
      throw new Error(payload.error.message || `sendTransaction failed on ${url}.`);
    }
    if (typeof payload?.result === 'string' && payload.result) {
      return payload.result;
    }
    throw new Error(`sendTransaction on ${url} did not return a signature.`);
  });

  const results = await Promise.allSettled(attempts);
  const success = results.find((item) => item.status === 'fulfilled');
  if (success?.status === 'fulfilled') {
    return success.value;
  }

  const failure = results.find((item) => item.status === 'rejected');
  throw failure?.reason ?? new Error('sendTransaction failed on every configured RPC endpoint.');
}

function getRpcPoolEndpointUrls() {
  const rpcPool = connection?.__rpcPool ?? null;
  const endpointUrls = Array.isArray(rpcPool?.getRpcPoolStatus?.().endpoints)
    ? rpcPool.getRpcPoolStatus().endpoints.map((endpoint) => endpoint.url).filter(Boolean)
    : [connection.rpcEndpoint].filter(Boolean);
  return endpointUrls.length > 0 ? endpointUrls : [connection.rpcEndpoint].filter(Boolean);
}

async function rpcJsonRequest(url, method, params = [], timeoutMs = 1_000) {
  const controller = new AbortController();
  const timeoutHandle = setTimeout(() => controller.abort(), timeoutMs);
  timeoutHandle.unref?.();
  try {
    const response = await fetch(url, {
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
    if (!response.ok) {
      throw new Error(`RPC ${method} failed on ${url} with status ${response.status}.`);
    }
    const payload = await response.json();
    if (payload?.error) {
      throw new Error(payload.error.message || `RPC ${method} failed on ${url}.`);
    }
    return payload?.result ?? null;
  } finally {
    clearTimeout(timeoutHandle);
  }
}

async function fetchTransactionAcrossRpcPool(signature, commitment = 'confirmed', preferredConnection = null) {
  const preferredUrl = preferredConnection?.rpcEndpoint || null;
  const endpointUrls = [
    ...new Set([
      preferredUrl,
      ...getRpcPoolEndpointUrls(),
    ].filter(Boolean)),
  ];

  const requestParams = [
    signature,
    {
      commitment,
      maxSupportedTransactionVersion: 0,
      encoding: 'json',
    },
  ];

  const attempts = endpointUrls.map(async (url) => {
    const transaction = await rpcJsonRequest(url, 'getTransaction', requestParams, 900);
    if (!transaction) {
      throw new Error(`Transaction ${signature} not yet visible on ${url}.`);
    }
    return transaction;
  });

  const results = await Promise.allSettled(attempts);
  const success = results.find((item) => item.status === 'fulfilled');
  if (success?.status === 'fulfilled') {
    return success.value;
  }
  return null;
}

async function sendRawVersionedTransaction(transaction, { blockhash, lastValidBlockHeight }) {
  const signature = await sendRawTransactionAcrossRpcPool(transaction);
  await confirmTransactionByMetadata(signature, blockhash, lastValidBlockHeight, 'confirmed');
  return signature;
}

async function rebroadcastVersionedTransactionUntilSettled(
  transaction,
  {
    blockhash,
    lastValidBlockHeight,
    commitment = 'processed',
    timeoutMs = 10_000,
    rebroadcastIntervalMs = 450,
    successCheck = null,
  } = {},
) {
  const startedAt = Date.now();
  let signature = null;
  let lastError = null;
  const commitmentRank = {
    processed: 0,
    confirmed: 1,
    finalized: 2,
  };
  const requiredRank = commitmentRank[commitment] ?? commitmentRank.processed;

  while (Date.now() - startedAt < timeoutMs) {
    try {
      const nextSignature = await sendRawTransactionAcrossRpcPool(transaction);
      if (!signature && nextSignature) {
        signature = nextSignature;
      }
    } catch (error) {
      lastError = error;
    }

    if (signature) {
      try {
        const response = await connection.getSignatureStatuses([signature], {
          searchTransactionHistory: false,
        });
        const status = response?.value?.[0] ?? null;
        if (status?.err) {
          throw new Error(`Transaction ${signature} confirmed with error ${JSON.stringify(status.err)}`);
        }
        if (status) {
          const currentRank = commitmentRank[status.confirmationStatus] ?? -1;
          if (status.confirmationStatus === 'finalized' || currentRank >= requiredRank) {
            return signature;
          }
        }
      } catch (error) {
        lastError = error;
      }
    }

    if (typeof successCheck === 'function') {
      try {
        const success = await successCheck();
        if (success) {
          return signature;
        }
      } catch (error) {
        lastError = error;
      }
    }

    try {
      const currentBlockHeight = await connection.getBlockHeight(commitment);
      if (currentBlockHeight > lastValidBlockHeight) {
        throw new Error(`Transaction ${signature || '[pending signature]'} expired before reaching ${commitment}.`);
      }
    } catch (error) {
      lastError = error;
    }

    await sleep(Math.min(rebroadcastIntervalMs, Math.max(100, timeoutMs - (Date.now() - startedAt))));
  }

  if (signature && typeof successCheck === 'function') {
    const success = await successCheck().catch(() => false);
    if (success) {
      return signature;
    }
  }

  throw lastError ?? new Error('Timed out rebroadcasting transaction before it settled.');
}

async function sendFastVersionedTransactionWithConfirmation(
  transaction,
  { blockhash, lastValidBlockHeight, preferSender = true, preferBundleOnlyJito = true } = {},
) {
  const errors = {};

  if (preferSender && HELIUS_SENDER_ENABLED) {
    try {
      const sendStartedAt = Date.now();
      const signature = await sendHeliusSenderTransaction(transaction);
      const sentAt = Date.now();
      await confirmTransactionByMetadataWithTimeout(signature, blockhash, lastValidBlockHeight, 'confirmed');
      return {
        signature,
        route: 'helius_sender',
        submitMs: sentAt - sendStartedAt,
        confirmMs: Date.now() - sentAt,
        errors,
      };
    } catch (error) {
      errors.heliusSender = String(error.message || error);
    }
  }

  if (preferBundleOnlyJito) {
    try {
      const sendStartedAt = Date.now();
      const submitResult = await sendJitoTransaction(transaction, { bundleOnly: true });
      const sentAt = Date.now();
      await confirmTransactionByMetadataWithTimeout(
        submitResult.signature,
        blockhash,
        lastValidBlockHeight,
        'confirmed',
      );
      return {
        signature: submitResult.signature,
        route: submitResult.bundleId ? 'jito_send_bundle_only' : 'jito_send_transaction',
        bundleId: submitResult.bundleId || null,
        submitMs: sentAt - sendStartedAt,
        confirmMs: Date.now() - sentAt,
        errors,
      };
    } catch (error) {
      errors.jito = String(error.message || error);
    }
  }

  try {
    const sendStartedAt = Date.now();
    const signature = await sendRawVersionedTransaction(transaction, { blockhash, lastValidBlockHeight });
    const totalMs = Date.now() - sendStartedAt;
    return {
      signature,
      route: 'direct_raw',
      submitMs: totalMs,
      confirmMs: 0,
      errors,
    };
  } catch (error) {
    errors.directRaw = String(error.message || error);
    const summary = Object.entries(errors).map(([key, value]) => `${key}: ${value}`).join(' | ');
    throw new Error(summary || String(error.message || error));
  }
}

function signJupiterTransaction(order, signer) {
  const transactionBytes = Buffer.from(order.transaction, 'base64');
  const transaction = VersionedTransaction.deserialize(Uint8Array.from(transactionBytes));
  transaction.sign([signer]);
  return transaction;
}

async function getInflightBundleStatuses(bundleId) {
  const result = await callJitoRpc('/api/v1/getInflightBundleStatuses', 'getInflightBundleStatuses', [[bundleId]]);
  return result?.value ?? null;
}

async function getBundleStatuses(bundleId) {
  const result = await callJitoRpc('/api/v1/bundles', 'getBundleStatuses', [[bundleId]]);
  return result?.value ?? null;
}

async function waitForJitoBundleLanded(bundleId, timeoutMs = BUNDLED_JITO_STATUS_TIMEOUT_MS) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    const statuses = await getInflightBundleStatuses(bundleId);
    const current = Array.isArray(statuses) ? statuses.find((item) => item.bundle_id === bundleId) : null;
    if (current?.status === 'Landed') {
      return current;
    }

    if (current?.status === 'Failed' || current?.status === 'Invalid') {
      throw new Error(`Jito bundle ${bundleId} returned status ${current.status}.`);
    }

    const landedStatuses = await getBundleStatuses(bundleId);
    const landedCurrent = Array.isArray(landedStatuses)
      ? landedStatuses.find((item) => item.bundle_id === bundleId)
      : null;
    if (landedCurrent && !landedCurrent.err) {
      return landedCurrent;
    }

    await new Promise((resolve) => setTimeout(resolve, BUNDLED_JITO_POLL_MS));
  }

  throw new Error(`Timed out waiting for Jito bundle ${bundleId} to land.`);
}

async function createSniperWizardRoutingOrderData(order, amountLamports) {
  const { quoteId, quote } = await createSplitNowQuoteData(amountLamports);
  const exchangerId = pickSplitNowExchangerId(quote);
  const outputs = buildSniperWizardRoutingOutputs(order, exchangerId);
  const response = await callSplitNowApi('/orders/', {
    method: 'POST',
    body: {
      type: 'floating_rate',
      quoteId,
      orderInput: {
        fromAmount: formatLamportsAsSolNumber(amountLamports),
        fromAssetId: 'sol',
        fromNetworkId: 'solana',
      },
      orderOutputs: outputs,
    },
  });

  const orderLookupId = response?.shortId || response?.orderId;
  if (!orderLookupId) {
    throw new Error('Stealth routing did not return a tracking id.');
  }

  const orderDetails = await callSplitNowApi(`/orders/${orderLookupId}`);
  return {
    quoteId,
    orderId: response?.shortId || response?.orderId,
    depositAddress: orderDetails?.depositWalletAddress || null,
    depositAmount: orderDetails?.depositAmount || formatLamportsAsSolNumber(amountLamports),
    status: orderDetails?.statusText || orderDetails?.statusShort || orderDetails?.status || 'Pending',
  };
}

function sniperWizardCanRun(order) {
  return Boolean(
    (order?.sniperMode === 'standard' || order?.sniperMode === 'magic')
    && order?.walletAddress
    && order?.walletSecretKeyB64
    && order?.targetWalletAddress
    && Number.isInteger(order?.walletCount)
    && order.walletCount >= 1
    && order.walletCount <= SNIPER_MAX_WALLET_COUNT
    && Array.isArray(order?.workerWallets)
    && order.workerWallets.length >= order.walletCount
    && Number.isInteger(order?.snipePercent)
    && order.snipePercent >= 1
    && order.snipePercent <= 100,
  );
}

function getSniperSpendableLamports(balanceLamports) {
  const reserveLamports = Math.max(
    SNIPER_GAS_RESERVE_LAMPORTS,
    SNIPER_JITO_TIP_LAMPORTS + 50_000,
  );
  return Math.max(0, balanceLamports - reserveLamports);
}

function buildStoredSniperWizardSnapshot(order) {
  const signer = decodeOrderWallet(order.walletSecretKeyB64);
  if (signer.publicKey.toBase58() !== order.walletAddress) {
    throw new Error('Sniper Wizard deposit wallet secret does not match the stored wallet address.');
  }

  const workerWallets = (order.workerWallets || []).map((wallet) => normalizeLaunchBuyBuyerWalletRecord(wallet));
  const currentLamports = Number.isInteger(order.currentLamports) ? order.currentLamports : 0;
  const totalManagedLamports = Number.isInteger(order.totalManagedLamports)
    ? order.totalManagedLamports
    : currentLamports + workerWallets.reduce((sum, wallet) => sum + (wallet.currentLamports || 0), 0);

  return {
    signer,
    currentLamports,
    workerWallets,
    totalManagedLamports,
  };
}

async function refreshSniperWizardSnapshot(order) {
  const signer = decodeOrderWallet(order.walletSecretKeyB64);
  if (signer.publicKey.toBase58() !== order.walletAddress) {
    throw new Error('Sniper Wizard deposit wallet secret does not match the stored wallet address.');
  }

  const currentLamports = await connection.getBalance(signer.publicKey, 'confirmed');
  const workerWallets = await Promise.all(
    (order.workerWallets || []).map(async (wallet) => {
      if (!wallet.address) {
        return normalizeLaunchBuyBuyerWalletRecord(wallet);
      }
      const lamports = await connection.getBalance(new PublicKey(wallet.address), 'confirmed')
        .catch(() => wallet.currentLamports || 0);
      return normalizeLaunchBuyBuyerWalletRecord({
        ...wallet,
        currentLamports: lamports,
        currentSol: formatSolAmountFromLamports(lamports),
      });
    }),
  );
  const totalManagedLamports = currentLamports + workerWallets.reduce(
    (sum, wallet) => sum + (wallet.currentLamports || 0),
    0,
  );

  return {
    signer,
    currentLamports,
    workerWallets,
    totalManagedLamports,
  };
}

async function persistSniperWizardSnapshot(userId, snapshot, patchOrder = () => ({})) {
  return updateSniperWizard(userId, (draft) => {
    const estimates = estimateSniperWizardFees(
      snapshot.currentLamports,
      draft.walletCount,
      draft.sniperMode || 'standard',
      draft.setupFeePaidAt,
    );
    return {
      ...draft,
      currentLamports: snapshot.currentLamports,
      currentSol: formatSolAmountFromLamports(snapshot.currentLamports),
      totalManagedLamports: snapshot.totalManagedLamports,
      workerWallets: snapshot.workerWallets,
      estimatedPlatformFeeLamports: estimates.platformFeeLamports,
      estimatedSplitNowFeeLamports: estimates.splitNowFeeLamports,
      estimatedNetSplitLamports: estimates.netSplitLamports,
      lastBalanceCheckAt: new Date().toISOString(),
      ...patchOrder(draft),
    };
  });
}

function buildSniperWizardFundingPlan(order, snapshot) {
  const walletCount = Number.isInteger(order.walletCount)
    ? Math.max(1, Math.min(SNIPER_MAX_WALLET_COUNT, order.walletCount))
    : Math.max(1, snapshot.workerWallets.length || 1);
  const estimates = estimateSniperWizardFees(
    snapshot.currentLamports,
    walletCount,
    order.sniperMode || 'standard',
    order.setupFeePaidAt,
  );
  const distributableLamports = estimates.netSplitLamports
    + snapshot.workerWallets.reduce((sum, wallet) => sum + (wallet.currentLamports || 0), 0);
  const splitSeed = [
    order.id,
    walletCount,
    ...snapshot.workerWallets.slice(0, walletCount).map((wallet, index) => wallet.address || wallet.label || String(index)),
  ].join(':');
  const splitPlan = splitLamportsRandomized(distributableLamports, walletCount, {
    varianceBps: 2_200,
    seed: splitSeed,
  });
  const workerPlans = snapshot.workerWallets.slice(0, walletCount).map((wallet, index) => ({
    ...wallet,
    targetLamports: splitPlan[index] || 0,
    surplusLamports: Math.max(0, (wallet.currentLamports || 0) - (splitPlan[index] || 0)),
    topUpLamports: Math.max(0, (splitPlan[index] || 0) - (wallet.currentLamports || 0)),
  }));

  return {
    walletCount,
    estimates,
    workerPlans,
    routeLamports: estimates.netSplitLamports,
  };
}

async function ensureSniperWizardFunding(userId, order, snapshot, fundingPlan) {
  if (order.sniperMode !== 'magic') {
    for (const worker of shuffleArray(fundingPlan.workerPlans)) {
      if (!worker.address || !worker.secretKeyB64 || worker.surplusLamports <= 0) {
        continue;
      }
      const workerSigner = decodeOrderWallet(worker.secretKeyB64);
      if (workerSigner.publicKey.toBase58() !== worker.address) {
        throw new Error(`Sniper Wizard wallet secret does not match ${worker.label || worker.address}.`);
      }
      await transferLamportsBetweenWallets(workerSigner, snapshot.signer.publicKey.toBase58(), worker.surplusLamports);
    }
    snapshot = await refreshSniperWizardSnapshot(order);
    fundingPlan = buildSniperWizardFundingPlan(order, snapshot);
    for (const worker of shuffleArray(fundingPlan.workerPlans)) {
      if (!worker.address || worker.topUpLamports <= 0) {
        continue;
      }
      await transferLamportsBetweenWallets(snapshot.signer, worker.address, worker.topUpLamports);
    }
    const verifiedSnapshot = await refreshSniperWizardSnapshot(order);
    await persistSniperWizardSnapshot(userId, verifiedSnapshot);
    const verifiedPlan = buildSniperWizardFundingPlan(order, verifiedSnapshot);
    const underfundedWorker = verifiedPlan.workerPlans.find((worker) => (
      worker.address
      && worker.currentLamports + SNIPER_FUNDING_TOLERANCE_LAMPORTS < worker.targetLamports
    ));
    if (underfundedWorker) {
      throw new Error(
        `Sniper wallet ${underfundedWorker.label || underfundedWorker.address} is underfunded `
        + `(${formatSolAmountFromLamports(underfundedWorker.currentLamports)} SOL / `
        + `${formatSolAmountFromLamports(underfundedWorker.targetLamports)} SOL target).`,
      );
    }
    return {
      order,
      routed: true,
    };
  }

  if (!order.setupFeePaidAt && fundingPlan.estimates.platformFeeLamports > 0) {
    await sendMagicBundlePlatformFee(snapshot.signer, fundingPlan.estimates.platformFeeLamports);
    order = normalizeSniperWizardRecord({
      ...order,
      setupFeePaidAt: new Date().toISOString(),
    });
    await updateSniperWizard(userId, (draft) => ({
      ...draft,
      setupFeePaidAt: order.setupFeePaidAt,
      lastError: null,
    }));
    await appendUserActivityLog(userId, {
      scope: `sniper_wizard:${order.id}`,
      level: 'info',
      message: `Sniper Wizard magic setup fee collected: ${formatSolAmountFromLamports(fundingPlan.estimates.platformFeeLamports)} SOL.`,
    });
    snapshot = await refreshSniperWizardSnapshot(order);
    fundingPlan = buildSniperWizardFundingPlan(order, snapshot);
  }

  if (order.routingOrderId) {
    const routingStatus = await getSplitNowOrderStatus(order.routingOrderId);
    const isComplete = ['completed', 'complete', 'finished', 'done'].includes(String(routingStatus.statusShort || '').toLowerCase());
    await updateSniperWizard(userId, (draft) => ({
      ...draft,
      routingOrderId: isComplete ? null : draft.routingOrderId,
      routingStatus: routingStatus.statusText,
      routingCompletedAt: isComplete ? new Date().toISOString() : draft.routingCompletedAt,
      status: isComplete ? 'watching' : 'routing',
      lastError: null,
    }));
    return {
      order: normalizeSniperWizardRecord({
        ...order,
        routingOrderId: isComplete ? null : order.routingOrderId,
        routingStatus: routingStatus.statusText,
        routingCompletedAt: isComplete ? new Date().toISOString() : order.routingCompletedAt,
        status: isComplete ? 'watching' : 'routing',
      }),
      routed: isComplete,
    };
  }

  if (fundingPlan.routeLamports <= 0) {
    return {
      order,
      routed: true,
    };
  }

  const routingOrder = await createSniperWizardRoutingOrderData(order, fundingPlan.routeLamports);
  await updateSniperWizard(userId, (draft) => ({
    ...draft,
    routingQuoteId: routingOrder.quoteId,
    routingOrderId: routingOrder.orderId,
    routingDepositAddress: routingOrder.depositAddress,
    routingStatus: routingOrder.status,
    status: 'routing',
    lastError: null,
  }));
  await transferLamportsBetweenWallets(snapshot.signer, routingOrder.depositAddress, fundingPlan.routeLamports);
  await appendUserActivityLog(userId, {
    scope: `sniper_wizard:${order.id}`,
    level: 'info',
    message: `Sniper Wizard magic routing started for ${formatSolAmountFromLamports(fundingPlan.routeLamports)} SOL across ${fundingPlan.workerPlans.length} sniper wallets.`,
  });
  return {
    order: normalizeSniperWizardRecord({
      ...order,
      routingQuoteId: routingOrder.quoteId,
      routingOrderId: routingOrder.orderId,
      routingDepositAddress: routingOrder.depositAddress,
      routingStatus: routingOrder.status,
      status: 'routing',
    }),
    routed: false,
  };
}

async function fetchLaunchMintAddressWithRetry(signature, preferredConnection = null) {
  const cachedMintAddress = knownLaunchMintCache.get(signature);
  if (cachedMintAddress) {
    return cachedMintAddress;
  }
  let lastError = null;

  for (let attempt = 0; attempt < SNIPER_LAUNCH_FAST_FETCH_RETRIES; attempt += 1) {
    try {
      const transaction = await fetchTransactionAcrossRpcPool(signature, 'confirmed', preferredConnection);
      const mintAddress = extractPumpLaunchMintAddressFromCompiledTransaction(transaction);
      if (mintAddress) {
        rememberKnownLaunchMint(signature, mintAddress);
        return mintAddress;
      }
    } catch (error) {
      lastError = error;
    }

    await sleep(SNIPER_LAUNCH_FAST_FETCH_DELAY_MS);
  }

  for (let attempt = 0; attempt < SNIPER_LAUNCH_FETCH_RETRIES; attempt += 1) {
    try {
      const transaction = await connection.getTransaction(signature, {
        commitment: 'confirmed',
        maxSupportedTransactionVersion: 0,
      });
      const mintAddress = extractPumpLaunchMintAddressFromCompiledTransaction(transaction);
      if (mintAddress) {
        rememberKnownLaunchMint(signature, mintAddress);
        return mintAddress;
      }
    } catch (error) {
      lastError = error;
    }

    await sleep(Math.min(SNIPER_LAUNCH_FETCH_DELAY_MS, 120));
  }

  if (lastError) {
    throw lastError;
  }

  throw new Error(`Timed out waiting for launch mint extraction from ${signature}.`);
}

async function buildPumpSniperBuyInstructions(signer, mintAddress, lamports, sharedLaunchState = null) {
  const mint = new PublicKey(mintAddress);
  let lastError = null;
  let tokenProgram = sharedLaunchState?.tokenProgram ?? null;
  let global = sharedLaunchState?.global ?? null;
  let feeConfig = sharedLaunchState?.feeConfig ?? null;
  let bondingCurveAccountInfo = sharedLaunchState?.bondingCurveAccountInfo ?? null;
  let bondingCurve = sharedLaunchState?.bondingCurve ?? null;
  const maxAttempts = sharedLaunchState ? Math.min(SNIPER_LAUNCH_FETCH_RETRIES, 4) : SNIPER_LAUNCH_FETCH_RETRIES;
  const retryDelayMs = sharedLaunchState ? Math.min(SNIPER_LAUNCH_FETCH_DELAY_MS, 35) : SNIPER_LAUNCH_FETCH_DELAY_MS;

  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    try {
      if (!tokenProgram) {
        tokenProgram = await getMintTokenProgram(mintAddress, 'processed');
      }
      let associatedUserAccountInfo = null;
      if (!bondingCurveAccountInfo || !bondingCurve) {
        ({
          bondingCurveAccountInfo,
          bondingCurve,
          associatedUserAccountInfo,
        } = await pumpOnlineSdk.fetchBuyState(
          mint,
          signer.publicKey,
          tokenProgram,
        ));
      } else {
        const ataInfoByOwner = sharedLaunchState?.associatedUserAccountInfoByOwner ?? null;
        const ownerAddress = signer.publicKey.toBase58();
        if (ataInfoByOwner && Object.prototype.hasOwnProperty.call(ataInfoByOwner, ownerAddress)) {
          associatedUserAccountInfo = ataInfoByOwner[ownerAddress];
        } else {
          const associatedUser = getAssociatedTokenAddressSync(
            mint,
            signer.publicKey,
            true,
            tokenProgram,
          );
          associatedUserAccountInfo = await connection.getAccountInfo(associatedUser, 'processed').catch(() => null);
        }
      }

      if (bondingCurve.complete) {
        throw new Error('Launch already moved beyond the bonding curve before the snipe could execute.');
      }

      if (!global) {
        global = await pumpOnlineSdk.fetchGlobal();
      }
      if (!feeConfig) {
        feeConfig = await pumpOnlineSdk.fetchFeeConfig();
      }
      const amount = getBuyTokenAmountFromSolAmount({
        global,
        feeConfig,
        mintSupply: bondingCurve.tokenTotalSupply,
        bondingCurve,
        amount: new BN(lamports),
      });

      if (amount.lte(new BN(0))) {
        throw new Error('Sniper buy quote returned zero tokens.');
      }

      const instructions = await PUMP_SDK.buyInstructions({
        global,
        bondingCurveAccountInfo,
        bondingCurve,
        associatedUserAccountInfo,
        mint,
        user: signer.publicKey,
        amount,
        solAmount: new BN(lamports),
        slippage: SNIPER_BUY_SLIPPAGE_PERCENT,
        tokenProgram,
      });

      return {
        tokenProgram,
        instructions: [
          ComputeBudgetProgram.setComputeUnitLimit({ units: SNIPER_COMPUTE_UNIT_LIMIT }),
          ComputeBudgetProgram.setComputeUnitPrice({ microLamports: SNIPER_PRIORITY_FEE_MICROLAMPORTS }),
          ...instructions,
        ],
      };
    } catch (error) {
      lastError = error;
      if (attempt + 1 < maxAttempts) {
        await sleep(retryDelayMs);
      }
    }
  }

  throw lastError ?? new Error('Unable to build sniper buy instructions.');
}

async function executeSniperWizardBuy(order, mintAddress, grossLamports, sharedLaunchState = null) {
  const timing = createTimingTracker();
  const signer = decodeOrderWallet(order.secretKeyB64 || order.walletSecretKeyB64);
  const expectedAddress = order.address || order.walletAddress;
  if (signer.publicKey.toBase58() !== expectedAddress) {
    throw new Error('Sniper Wizard wallet secret does not match the stored wallet address.');
  }

  const handlingFeeLamports = calculateHandlingFeeLamports(grossLamports);
  const netLamports = grossLamports - handlingFeeLamports;
  if (netLamports <= 0) {
    throw new Error('Sniper Wizard buy amount is too small after the handling fee.');
  }

  const { instructions } = await buildPumpSniperBuyInstructions(
    signer,
    mintAddress,
    netLamports,
    sharedLaunchState,
  );
  timing.mark('buildMs');

  try {
    const submitStartedAt = Date.now();
    let finalInstructions = instructions;
    try {
      const tipAccounts = await getJitoTipAccounts();
      const tipInstruction = SystemProgram.transfer({
        fromPubkey: signer.publicKey,
        toPubkey: new PublicKey(chooseRandomTipAccount(tipAccounts)),
        lamports: SNIPER_JITO_TIP_LAMPORTS,
      });
      finalInstructions = [...instructions, tipInstruction];
    } catch (tipError) {
      console.warn(`[worker] Sniper Wizard tip-account fetch failed for ${signer.publicKey.toBase58()}:`, tipError?.message || tipError);
    }

    const {
      transaction,
      blockhash,
      lastValidBlockHeight,
    } = await createVersionedTransactionWithMetadata(
      signer,
      finalInstructions,
      {
        commitment: 'processed',
        latestBlockhash: sharedLaunchState?.latestBlockhashMeta ?? null,
      },
    );
    const signature = await rebroadcastVersionedTransactionUntilSettled(
      transaction,
      {
        blockhash,
        lastValidBlockHeight,
        commitment: 'processed',
        timeoutMs: 4_000,
        rebroadcastIntervalMs: 175,
      },
    );
    const submitMs = Date.now() - submitStartedAt;
    const buildMs = timing.elapsedMs;
    if (handlingFeeLamports > 0) {
      scheduleTradingHandlingFeeTransfer(signer, handlingFeeLamports, 'Sniper Wizard');
    }
    return {
      signature,
      route: 'rebroadcast_direct',
      grossLamports,
      netLamports,
      handlingFeeLamports,
      timingMs: timing.snapshot({
        buildMs,
        submitMs,
        confirmMs: 0,
      }),
    };
  } catch (error) {
    const signature = await sendLegacyTransaction(instructions, signer);
    return {
      signature,
      route: 'legacy_instruction_fallback',
      bundleError: String(error.message || error),
      grossLamports,
      netLamports,
      handlingFeeLamports,
      timingMs: timing.snapshot(),
    };
  }
}

async function handleSniperWizardLaunch(
  userId,
  signature,
  preferredConnection = null,
  preResolvedMintAddress = null,
  preResolvedLaunchState = null,
) {
  const launchDetectedAt = Date.now();
  const store = await readStore();
  const user = store.users?.[userId];
  if (!user) {
    return;
  }

  const order = normalizeUserSniperWizard(user);
  if (!order.automationEnabled || !sniperWizardCanRun(order)) {
    return;
  }

  const mintAddress = preResolvedMintAddress || await fetchLaunchMintAddressWithRetry(signature, preferredConnection);
  rememberKnownLaunchMint(signature, mintAddress);
  let snapshot = buildStoredSniperWizardSnapshot(order);
  const workingOrder = order;
  let fundingPlan = buildSniperWizardFundingPlan(workingOrder, snapshot);

  let workerAttempts = fundingPlan.workerPlans
    .map((worker, index) => {
      const spendableLamports = getSniperSpendableLamports(worker.currentLamports || 0);
      const grossLamports = Math.floor((spendableLamports * workingOrder.snipePercent) / 100);
      return {
        index,
        worker,
        grossLamports,
      };
    })
    .filter((item) => item.worker?.address && item.worker?.secretKeyB64 && item.grossLamports > 0);

  if (workerAttempts.length === 0) {
    snapshot = await refreshSniperWizardSnapshot(order);
    fundingPlan = buildSniperWizardFundingPlan(workingOrder, snapshot);
    workerAttempts = fundingPlan.workerPlans
      .map((worker, index) => {
        const spendableLamports = getSniperSpendableLamports(worker.currentLamports || 0);
        const grossLamports = Math.floor((spendableLamports * workingOrder.snipePercent) / 100);
        return {
          index,
          worker,
          grossLamports,
        };
      })
      .filter((item) => item.worker?.address && item.worker?.secretKeyB64 && item.grossLamports > 0);
  }

  if (workerAttempts.length === 0) {
    const routingState = await ensureSniperWizardFunding(userId, order, snapshot, fundingPlan);
    const reroutedOrder = normalizeSniperWizardRecord(routingState.order || order);
    if (!routingState.routed) {
      snapshot = await refreshSniperWizardSnapshot(reroutedOrder);
      await persistSniperWizardSnapshot(userId, snapshot, () => ({
        status: 'routing',
        lastError: null,
      }));
      await appendUserActivityLog(userId, {
        scope: `sniper_wizard:${order.id}`,
        level: 'warn',
        message: 'Sniper Wizard saw a launch before stealth routing finished, so it is arming the sniper wallets now.',
      });
      return;
    }

    snapshot = routingState.snapshot ?? await refreshSniperWizardSnapshot(reroutedOrder);
    await persistSniperWizardSnapshot(userId, snapshot, () => ({
      status: 'waiting_funds',
      lastError: 'The sniper wallets do not have enough SOL available after keeping gas in reserve.',
    }));
    await appendUserActivityLog(userId, {
      scope: `sniper_wizard:${order.id}`,
      level: 'warn',
      message: 'Sniper Wizard saw a launch but the sniper wallets did not have enough SOL available after gas reserve.',
    });
    return;
  }

  let sharedLaunchState = preResolvedLaunchState
    ? {
      tokenProgram: preResolvedLaunchState.tokenProgram ?? null,
      global: preResolvedLaunchState.global ?? null,
      feeConfig: preResolvedLaunchState.feeConfig ?? null,
      bondingCurveAccountInfo: preResolvedLaunchState.bondingCurveAccountInfo ?? null,
      bondingCurve: preResolvedLaunchState.bondingCurve ?? null,
      associatedUserAccountInfoByOwner: preResolvedLaunchState.associatedUserAccountInfoByOwner ?? null,
      latestBlockhashMeta: preResolvedLaunchState.latestBlockhashMeta ?? null,
    }
    : null;
  try {
    const primaryAttempt = workerAttempts[0] || null;
    const primarySigner = primaryAttempt?.worker?.secretKeyB64
      ? decodeOrderWallet(primaryAttempt.worker.secretKeyB64)
      : null;
    const [tokenProgram, global, feeConfig, latestBlockhashMeta] = await Promise.all([
      sharedLaunchState?.tokenProgram
        ? sharedLaunchState.tokenProgram
        : getMintTokenProgram(mintAddress, 'processed'),
      sharedLaunchState?.global
        ? sharedLaunchState.global
        : pumpOnlineSdk.fetchGlobal(),
      sharedLaunchState?.feeConfig
        ? sharedLaunchState.feeConfig
        : pumpOnlineSdk.fetchFeeConfig(),
      sharedLaunchState?.latestBlockhashMeta
        ? sharedLaunchState.latestBlockhashMeta
        : connection.getLatestBlockhash('processed'),
    ]);
    let prefetchedBuyState = null;
    if (
      primarySigner
      && (!sharedLaunchState?.bondingCurveAccountInfo || !sharedLaunchState?.bondingCurve)
    ) {
      prefetchedBuyState = await pumpOnlineSdk.fetchBuyState(
        new PublicKey(mintAddress),
        primarySigner.publicKey,
        tokenProgram,
      );
    }
    let associatedUserAccountInfoByOwner = sharedLaunchState?.associatedUserAccountInfoByOwner ?? null;
    if (!associatedUserAccountInfoByOwner && workerAttempts.length > 0) {
      const associatedOwners = workerAttempts
        .map((item) => item.worker?.secretKeyB64 ? decodeOrderWallet(item.worker.secretKeyB64).publicKey : null)
        .filter(Boolean);
      if (associatedOwners.length > 0) {
        const associatedAccounts = associatedOwners.map((owner) => getAssociatedTokenAddressSync(
          new PublicKey(mintAddress),
          owner,
          true,
          tokenProgram,
        ));
        const accountInfos = await connection.getMultipleAccountsInfo(associatedAccounts, 'processed');
        associatedUserAccountInfoByOwner = {};
        associatedOwners.forEach((owner, index) => {
          associatedUserAccountInfoByOwner[owner.toBase58()] = accountInfos[index] ?? null;
        });
      }
    }
    sharedLaunchState = {
      tokenProgram,
      global,
      feeConfig,
      bondingCurveAccountInfo: prefetchedBuyState?.bondingCurveAccountInfo ?? null,
      bondingCurve: prefetchedBuyState?.bondingCurve ?? null,
      associatedUserAccountInfoByOwner,
      latestBlockhashMeta,
    };
  } catch (error) {
    console.warn(`[worker] Sniper Wizard shared launch state prefetch failed for ${mintAddress}:`, error?.message || error);
  }

  const attemptResults = await Promise.allSettled(workerAttempts.map((item) => executeSniperWizardBuy({
    address: item.worker.address,
    secretKeyB64: item.worker.secretKeyB64,
  }, mintAddress, item.grossLamports, sharedLaunchState)));
  const successes = attemptResults
    .map((result, index) => ({ result, worker: workerAttempts[index] }))
    .filter((item) => item.result.status === 'fulfilled');
  const failures = attemptResults
    .map((result, index) => ({ result, worker: workerAttempts[index] }))
    .filter((item) => item.result.status === 'rejected');

  await persistSniperWizardSnapshot(userId, snapshot, (draft) => {
    const stats = normalizeSniperWizardStats(draft.stats);
    const totalNetLamports = successes.reduce(
      (sum, item) => sum + (item.result.status === 'fulfilled' ? item.result.value.netLamports : 0),
      0,
    );
    const totalFeeLamports = successes.reduce(
      (sum, item) => sum + (item.result.status === 'fulfilled' ? item.result.value.handlingFeeLamports : 0),
      0,
    );
    const firstTiming = successes.find((item) => item.result.status === 'fulfilled')?.result.value?.timingMs ?? null;
    const lastTotalLatencyMs = firstTiming?.totalMs || (Date.now() - launchDetectedAt);
    const successCount = stats.snipeSuccessCount + successes.length;
    const firstSignature = successes[0]?.result.status === 'fulfilled' ? successes[0].result.value.signature : null;
    const failureMessage = failures[0]?.result.status === 'rejected'
      ? String(failures[0].result.reason?.message || failures[0].result.reason || 'Unknown sniper failure.')
      : null;
    const alreadyTracked = draft.lastDetectedLaunchSignature === signature;
    return {
      ...draft,
      status: successes.length > 0
        ? (draft.automationEnabled ? 'watching' : 'stopped')
        : 'failed',
      lastDetectedLaunchSignature: signature,
      lastDetectedMintAddress: mintAddress,
      lastSnipeSignature: firstSignature,
      lastBalanceCheckAt: new Date().toISOString(),
      lastError: failureMessage,
      stats: {
        ...stats,
        launchCount: alreadyTracked ? stats.launchCount : (stats.launchCount + 1),
        snipeAttemptCount: stats.snipeAttemptCount + workerAttempts.length,
        snipeSuccessCount: successCount,
        totalSpentLamports: stats.totalSpentLamports + totalNetLamports,
        totalFeeLamports: stats.totalFeeLamports + totalFeeLamports,
        avgTotalLatencyMs: successCount > 0
          ? Math.round((((stats.avgTotalLatencyMs || 0) * stats.snipeSuccessCount) + lastTotalLatencyMs) / successCount)
          : 0,
        bestTotalLatencyMs: stats.bestTotalLatencyMs > 0
          ? Math.min(stats.bestTotalLatencyMs, lastTotalLatencyMs)
          : lastTotalLatencyMs,
        lastDetectToBuildMs: firstTiming?.buildMs || 0,
        lastSubmitMs: firstTiming?.submitMs || 0,
        lastConfirmMs: firstTiming?.confirmMs || 0,
        lastTotalLatencyMs,
        lastRoute: successes[0]?.result.status === 'fulfilled' ? successes[0].result.value.route : stats.lastRoute,
        lastLaunchSignature: signature,
        lastMintAddress: mintAddress,
        lastSnipeSignature: firstSignature,
      },
    };
  });

  if (successes.length > 0) {
    const totalNetLamports = successes.reduce(
      (sum, item) => sum + (item.result.status === 'fulfilled' ? item.result.value.netLamports : 0),
      0,
    );
    const totalFeeLamports = successes.reduce(
      (sum, item) => sum + (item.result.status === 'fulfilled' ? item.result.value.handlingFeeLamports : 0),
      0,
    );
    const firstTiming = successes.find((item) => item.result.status === 'fulfilled')?.result.value?.timingMs ?? null;
    await appendUserActivityLog(userId, {
      scope: `sniper_wizard:${order.id}`,
      level: 'info',
      message: `Sniper Wizard fired on ${mintAddress} across ${successes.length} sniper wallet(s), using ${formatSolAmountFromLamports(totalNetLamports)} SOL after ${formatSolAmountFromLamports(totalFeeLamports)} SOL in handling fees. Fastest path: ${successes[0]?.result.value.route || 'unknown'} in about ${firstTiming?.totalMs || Date.now() - launchDetectedAt}ms.`,
    });
  }
  if (failures.length > 0) {
    await appendUserActivityLog(userId, {
      scope: `sniper_wizard:${order.id}`,
      level: 'warn',
      message: `Sniper Wizard had ${failures.length} wallet(s) fail during the snipe. The first error was: ${String(failures[0].result.reason?.message || failures[0].result.reason || 'Unknown error')}`,
    });
  }

  void refreshSniperWizardSnapshot(workingOrder)
    .then((freshSnapshot) => persistSniperWizardSnapshot(userId, freshSnapshot))
    .catch(() => null);
}

async function stopSniperWizardSubscription(userId) {
  const existing = sniperWizardSubscriptions.get(userId);
  if (!existing) {
    return;
  }

  sniperWizardSubscriptions.delete(userId);
  try {
    if (existing.connection && existing.subscriptionId) {
      await existing.connection.removeOnLogsListener(existing.subscriptionId);
    }
  } catch {
    // Ignore cleanup errors.
  }
  try {
    if (existing.connection && existing.slotSubscriptionId) {
      await existing.connection.removeSlotChangeListener(existing.slotSubscriptionId);
    }
  } catch {
    // Ignore cleanup errors.
  }
  try {
    existing.connection?._rpcWebSocket?.close?.(1000, 'sniper watcher rotate');
  } catch {
    // Ignore websocket shutdown errors.
  }
}

function rememberSniperSignature(watcher, signature) {
  watcher.seenSignatures.add(signature);
  if (watcher.seenSignatures.size > 100) {
    const [first] = watcher.seenSignatures;
    watcher.seenSignatures.delete(first);
  }
}

function createSniperWatcherConnection(endpoint) {
  return new Connection(endpoint.httpUrl, {
    commitment: 'processed',
    wsEndpoint: endpoint.wsUrl,
  });
}

async function doesLaunchMintExist(mintAddress) {
  try {
    const info = await connection.getAccountInfo(mintAddress, 'confirmed');
    return Boolean(info);
  } catch {
    return false;
  }
}

async function waitForLaunchMintExist(mintAddress, timeoutMs = 4_000, pollMs = 250) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    if (await doesLaunchMintExist(mintAddress)) {
      return true;
    }
    await sleep(pollMs);
  }
  return false;
}

function getNextSniperWatcherEndpointIndex(currentIndex = -1) {
  if (!Array.isArray(cfg.wsEndpoints) || cfg.wsEndpoints.length === 0) {
    return 0;
  }
  return (currentIndex + 1) % cfg.wsEndpoints.length;
}

function ensureSniperWatcherHeartbeatLoop() {
  if (sniperWizardHeartbeatTimer) {
    return;
  }

  sniperWizardHeartbeatTimer = setInterval(() => {
    void healSniperWizardSubscriptions().catch((error) => {
      console.error('[worker] Sniper watcher heartbeat failed:', error.message || error);
    });
  }, SNIPER_WATCHER_HEARTBEAT_MS);
  sniperWizardHeartbeatTimer.unref?.();
}

async function rotateSniperWizardSubscription(userId, reason = 'failover') {
  const store = await readStore();
  const user = store.users?.[userId];
  if (!user) {
    await stopSniperWizardSubscription(userId);
    return;
  }

  const order = normalizeUserSniperWizard(user);
  if (!order.automationEnabled || !sniperWizardCanRun(order)) {
    await stopSniperWizardSubscription(userId);
    return;
  }

  const existing = sniperWizardSubscriptions.get(userId);
  const nextIndex = getNextSniperWatcherEndpointIndex(existing?.endpointIndex ?? -1);
  await startSniperWizardSubscription(userId, order, {
    endpointIndex: nextIndex,
    seenSignatures: existing?.seenSignatures,
    rotationCount: (existing?.rotationCount || 0) + 1,
  });
  await appendUserActivityLog(userId, {
    scope: `sniper_wizard:${order.id}`,
    level: 'warn',
    message: `Sniper Wizard watcher rotated to backup websocket (${cfg.wsEndpoints[nextIndex]?.wsUrl || 'unknown'}) after ${reason}.`,
  }).catch(() => null);
}

async function healSniperWizardSubscriptions() {
  const now = Date.now();
  const rotations = [];

  for (const [userId, watcher] of sniperWizardSubscriptions.entries()) {
    if (!watcher || !watcher.targetWalletAddress) {
      continue;
    }
    if (now - watcher.lastSlotAt > SNIPER_WATCHER_STALE_MS) {
      rotations.push(rotateSniperWizardSubscription(userId, 'stale websocket heartbeat'));
    }
  }

  if (rotations.length > 0) {
    await Promise.allSettled(rotations);
  }
}

async function startSniperWizardSubscription(userId, order, seed = {}) {
  await stopSniperWizardSubscription(userId);
  const endpointIndex = Number.isInteger(seed?.endpointIndex)
    ? Math.min(Math.max(seed.endpointIndex, 0), Math.max(0, cfg.wsEndpoints.length - 1))
    : getNextSniperWatcherEndpointIndex(-1);
  const endpoint = cfg.wsEndpoints[endpointIndex] || {
    httpUrl: cfg.rpcUrl,
    wsUrl: toWebSocketUrl(cfg.rpcUrl),
  };
  const watcherConnection = createSniperWatcherConnection(endpoint);
  const watcher = {
    targetWalletAddress: order.targetWalletAddress,
    processingSignatures: new Set(),
    seenSignatures: new Set(seed?.seenSignatures ? [...seed.seenSignatures] : []),
    subscriptionId: 0,
    slotSubscriptionId: 0,
    endpointIndex,
    endpoint,
    connection: watcherConnection,
    startedAt: Date.now(),
    lastSlotAt: Date.now(),
    rotationCount: Number.isInteger(seed?.rotationCount) ? seed.rotationCount : 0,
  };

  watcher.slotSubscriptionId = watcherConnection.onSlotChange(() => {
    watcher.lastSlotAt = Date.now();
  });

  const subscriptionId = watcherConnection.onLogs(new PublicKey(order.targetWalletAddress), (logInfo) => {
    const signature = logInfo?.signature;
    if (!signature || logInfo?.err) {
      return;
    }

    const activeWatcher = sniperWizardSubscriptions.get(userId);
    if (!activeWatcher || activeWatcher.targetWalletAddress !== order.targetWalletAddress) {
      return;
    }

    if (activeWatcher.processingSignatures.has(signature) || activeWatcher.seenSignatures.has(signature)) {
      return;
    }

    activeWatcher.processingSignatures.add(signature);
    rememberSniperSignature(activeWatcher, signature);

    void handleSniperWizardLaunch(userId, signature, watcherConnection)
      .catch(async (error) => {
        await updateSniperWizard(userId, (draft) => ({
          ...draft,
          status: draft.automationEnabled ? 'watching' : 'stopped',
          lastError: String(error.message || error),
        })).catch(() => null);
        await appendUserActivityLog(userId, {
          scope: `sniper_wizard:${order.id}`,
          level: 'error',
          message: `Sniper Wizard error: ${String(error.message || error)}`,
        }).catch(() => null);
        console.error(`[worker] Sniper Wizard failed for user ${userId}:`, error.message || error);
      })
      .finally(() => {
        const latestWatcher = sniperWizardSubscriptions.get(userId);
        latestWatcher?.processingSignatures.delete(signature);
      });
  }, 'processed');

  watcher.subscriptionId = subscriptionId;
  sniperWizardSubscriptions.set(userId, watcher);
  ensureSniperWatcherHeartbeatLoop();
}

async function waitForSniperWatchersReady(targetWalletAddress, timeoutMs = 4_000, minWarmMs = 750) {
  if (!targetWalletAddress) {
    return;
  }

  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const matchingWatchers = [...sniperWizardSubscriptions.values()].filter(
      (watcher) => watcher?.targetWalletAddress === targetWalletAddress,
    );
    if (matchingWatchers.length === 0) {
      return;
    }

    const watchersReady = matchingWatchers.every((watcher) => (
      watcher.subscriptionId
      && (Date.now() - (watcher.startedAt || 0)) >= minWarmMs
      && (Date.now() - (watcher.lastSlotAt || 0)) <= SNIPER_WATCHER_STALE_MS
    ));
    if (watchersReady) {
      return;
    }

    await sleep(100);
  }
}

async function syncSniperWizardSubscriptions(store) {
  const desired = new Map();

  for (const [userId, user] of Object.entries(store.users ?? {})) {
    const order = normalizeUserSniperWizard(user);
    if (order.automationEnabled && sniperWizardCanRun(order)) {
      desired.set(userId, order);
    }
  }

  for (const [userId, watcher] of sniperWizardSubscriptions.entries()) {
    const desiredOrder = desired.get(userId);
    if (!desiredOrder || desiredOrder.targetWalletAddress !== watcher.targetWalletAddress) {
      await stopSniperWizardSubscription(userId);
    }
  }

  for (const [userId, order] of desired.entries()) {
    const watcher = sniperWizardSubscriptions.get(userId);
    if (!watcher || watcher.targetWalletAddress !== order.targetWalletAddress) {
      try {
        await startSniperWizardSubscription(userId, order);
      } catch (error) {
        await updateSniperWizard(userId, (draft) => ({
          ...draft,
          status: 'failed',
          lastError: String(error.message || error),
        })).catch(() => null);
        console.error(`[worker] Failed to start Sniper Wizard subscription for user ${userId}:`, error.message || error);
      }
    }
  }
}

async function scanSniperWizards() {
  const store = await readStore();
  await syncSniperWizardSubscriptions(store);

  for (const [userId, user] of Object.entries(store.users ?? {})) {
    const order = normalizeUserSniperWizard(user);
    if (!hasMeaningfulSniperWizardRecord(order) || !order.walletAddress) {
      continue;
    }

    try {
      const lastBalanceCheckAtMs = order.lastBalanceCheckAt ? Date.parse(order.lastBalanceCheckAt) : 0;
      const canUseStoredSnapshot = Boolean(
        lastBalanceCheckAtMs
        && Number.isFinite(lastBalanceCheckAtMs)
        && (Date.now() - lastBalanceCheckAtMs) <= SNIPER_BALANCE_REFRESH_MS,
      );
      let snapshot = canUseStoredSnapshot
        ? buildStoredSniperWizardSnapshot(order)
        : await refreshSniperWizardSnapshot(order);
      let workingOrder = order;

      if (workingOrder.automationEnabled && sniperWizardCanRun(workingOrder)) {
        const fundingPlan = buildSniperWizardFundingPlan(workingOrder, snapshot);
        const walletsReady = fundingPlan.workerPlans.every((worker) => (
          !worker.address
          || worker.currentLamports + SNIPER_FUNDING_TOLERANCE_LAMPORTS >= worker.targetLamports
        ));
        if (!canUseStoredSnapshot || !walletsReady) {
          const fundingState = await ensureSniperWizardFunding(userId, workingOrder, snapshot, fundingPlan);
          workingOrder = normalizeSniperWizardRecord(fundingState.order || workingOrder);
          snapshot = await refreshSniperWizardSnapshot(workingOrder);
        }
      }

      await persistSniperWizardSnapshot(userId, snapshot, (draft) => {
        const hasFundedWorkers = snapshot.workerWallets.some(
          (wallet) => getSniperSpendableLamports(wallet.currentLamports || 0) > 0,
        );
        const nextStatus = draft.automationEnabled
          ? (sniperWizardCanRun(draft)
            ? (
              draft.status === 'routing'
                ? 'routing'
                : (['launch_detected', 'sniping'].includes(draft.status)
                  ? draft.status
                  : (hasFundedWorkers ? 'watching' : 'waiting_funds'))
            )
            : 'setup')
          : (sniperWizardCanRun(draft) ? 'stopped' : 'setup');

        return {
          status: nextStatus,
          lastError: (draft.automationEnabled && ['routing', 'launch_detected', 'sniping', 'failed'].includes(nextStatus))
            ? draft.lastError
            : null,
        };
      });
    } catch (error) {
      await updateSniperWizard(userId, (draft) => ({
        ...draft,
        lastBalanceCheckAt: new Date().toISOString(),
        lastError: String(error.message || error),
        status: draft.automationEnabled ? 'failed' : draft.status,
      }));
    }
  }
}

function buildCommunityVisionRequestUrl(handle) {
  const base = String(cfg.communityVision.apiUrl || '').trim();
  if (!base) {
    throw new Error('Community Vision feed is not configured.');
  }

  if (base.includes('{handle}')) {
    return base.replaceAll('{handle}', encodeURIComponent(handle));
  }

  const url = new URL(base);
  url.searchParams.set('handle', handle);
  return url.toString();
}

async function fetchCommunityVisionCommunities(handle) {
  const response = await fetch(buildCommunityVisionRequestUrl(handle), {
    headers: {
      'User-Agent': 'steel-tester-worker/1.0',
      ...(cfg.communityVision.bearerToken
        ? { Authorization: `Bearer ${cfg.communityVision.bearerToken}` }
        : {}),
    },
  }, 'confirmed');

  if (!response.ok) {
    throw new Error(`Community Vision lookup failed with status ${response.status}.`);
  }

  const payload = await response.json();
  const communities = Array.isArray(payload)
    ? payload
    : (Array.isArray(payload?.communities) ? payload.communities : []);

  return communities
    .filter((item) => item && typeof item === 'object')
    .map((item) => ({
      id: String(item.id || item.community_id || '').trim() || null,
      name: String(item.name || item.title || '').trim() || null,
      url: String(item.url || item.link || '').trim() || null,
    }))
    .filter((item) => item.id && item.name);
}

function formatCommunityVisionAlert(handle, community, previousName, nextName) {
  return [
    '*Community Vision Alert*',
    '',
    `X account: *@${handle}*`,
    `Community: *${previousName}*`,
    `New name: *${nextName}*`,
    ...(community?.url ? ['', community.url] : []),
  ].join('\n');
}

function getParsedAccountKeys(transaction) {
  return (transaction?.transaction?.message?.accountKeys ?? []).map((key) => {
    if (typeof key === 'string') {
      return key;
    }
    if (typeof key?.pubkey?.toBase58 === 'function') {
      return key.pubkey.toBase58();
    }
    if (typeof key?.pubkey === 'string') {
      return key.pubkey;
    }
    return null;
  });
}

function getWalletSolDeltaFromTransaction(transaction, walletAddress) {
  const accountKeys = getParsedAccountKeys(transaction);
  const walletIndex = accountKeys.findIndex((key) => key === walletAddress);
  if (walletIndex < 0) {
    return 0;
  }

  const preBalance = Number.isInteger(transaction?.meta?.preBalances?.[walletIndex])
    ? transaction.meta.preBalances[walletIndex]
    : 0;
  const postBalance = Number.isInteger(transaction?.meta?.postBalances?.[walletIndex])
    ? transaction.meta.postBalances[walletIndex]
    : 0;
  return postBalance - preBalance;
}

function buildWalletTrackerTokenEvents(transaction, walletAddress) {
  const solDeltaLamports = getWalletSolDeltaFromTransaction(transaction, walletAddress);
  const preMap = new Map();
  const postMap = new Map();

  for (const entry of transaction?.meta?.preTokenBalances ?? []) {
    if (entry?.owner !== walletAddress || !entry?.mint) {
      continue;
    }
    preMap.set(entry.mint, {
      amount: BigInt(entry?.uiTokenAmount?.amount || '0'),
      decimals: Number.isInteger(entry?.uiTokenAmount?.decimals) ? entry.uiTokenAmount.decimals : 0,
    });
  }

  for (const entry of transaction?.meta?.postTokenBalances ?? []) {
    if (entry?.owner !== walletAddress || !entry?.mint) {
      continue;
    }
    postMap.set(entry.mint, {
      amount: BigInt(entry?.uiTokenAmount?.amount || '0'),
      decimals: Number.isInteger(entry?.uiTokenAmount?.decimals) ? entry.uiTokenAmount.decimals : 0,
    });
  }

  const mints = new Set([...preMap.keys(), ...postMap.keys()]);
  return [...mints]
    .map((mint) => {
      const pre = preMap.get(mint) ?? { amount: 0n, decimals: postMap.get(mint)?.decimals ?? 0 };
      const post = postMap.get(mint) ?? { amount: 0n, decimals: pre.decimals };
      const deltaRaw = post.amount - pre.amount;
      if (deltaRaw === 0n || mint === SOL_MINT_ADDRESS) {
        return null;
      }

      const direction = deltaRaw > 0n
        ? (solDeltaLamports < 0 ? 'buy' : 'receive')
        : (solDeltaLamports > 0 ? 'sell' : 'send');
      return {
        mint,
        deltaRaw,
        decimals: post.decimals ?? pre.decimals ?? 0,
        direction,
      };
    })
    .filter(Boolean);
}

function formatWalletTrackerAlert(walletAddress, type, details) {
  const walletLabel = `${walletAddress.slice(0, 4)}...${walletAddress.slice(-4)}`;
  switch (type) {
    case 'launch':
      return [
        '*Wallet Tracker Alert*',
        '',
        `Wallet: \`${walletLabel}\``,
        'Activity: *Token launch detected*',
        `Mint: \`${details.mint}\``,
      ].join('\n');
    case 'buy':
      return [
        '*Wallet Tracker Alert*',
        '',
        `Wallet: \`${walletLabel}\``,
        'Activity: *Buy detected*',
        `Mint: \`${details.mint}\``,
        `Amount: *${details.amountDisplay}*`,
      ].join('\n');
    case 'sell':
      return [
        '*Wallet Tracker Alert*',
        '',
        `Wallet: \`${walletLabel}\``,
        'Activity: *Sell detected*',
        `Mint: \`${details.mint}\``,
        `Amount: *${details.amountDisplay}*`,
      ].join('\n');
    default:
      return '*Wallet Tracker Alert*';
  }
}

function splitLamportsEvenly(totalLamports, parts) {
  if (!Number.isInteger(totalLamports) || totalLamports <= 0 || !Number.isInteger(parts) || parts <= 0) {
    return [];
  }

  const base = Math.floor(totalLamports / parts);
  let remainder = totalLamports - (base * parts);
  return Array.from({ length: parts }, () => {
    const value = base + (remainder > 0 ? 1 : 0);
    if (remainder > 0) {
      remainder -= 1;
    }
    return value;
  });
}

function shuffleArray(values = []) {
  const copy = Array.isArray(values) ? [...values] : [];
  for (let index = copy.length - 1; index > 0; index -= 1) {
    const swapIndex = Math.floor(Math.random() * (index + 1));
    [copy[index], copy[swapIndex]] = [copy[swapIndex], copy[index]];
  }
  return copy;
}

function hashStringToUint32(value) {
  const input = String(value || '');
  let hash = 2166136261;
  for (let index = 0; index < input.length; index += 1) {
    hash ^= input.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

function createSeededRandom(seedValue) {
  let state = hashStringToUint32(seedValue) || 1;
  return () => {
    state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
    return state / 0x100000000;
  };
}

function splitLamportsRandomized(totalLamports, parts, { varianceBps = 2_500, seed = 'default' } = {}) {
  if (!Number.isInteger(totalLamports) || totalLamports <= 0 || !Number.isInteger(parts) || parts <= 0) {
    return [];
  }

  if (parts === 1) {
    return [totalLamports];
  }

  const safeVarianceBps = Math.max(0, Math.min(4_500, varianceBps));
  const baseWeight = 10_000;
  const random = createSeededRandom(seed);
  const weights = Array.from({ length: parts }, () => (
    baseWeight + Math.floor((random() * ((safeVarianceBps * 2) + 1)) - safeVarianceBps)
  ));
  const totalWeight = weights.reduce((sum, weight) => sum + weight, 0);
  const randomized = weights.map((weight) => Math.max(1, Math.floor((totalLamports * weight) / totalWeight)));

  let remainder = totalLamports - randomized.reduce((sum, value) => sum + value, 0);
  const randomIndexes = shuffleArray(
    Array.from({ length: parts }, (_, index) => ({ index, sortKey: random() })),
  ).sort((left, right) => left.sortKey - right.sortKey)
    .map((item) => item.index);

  while (remainder > 0) {
    for (const index of randomIndexes) {
      randomized[index] += 1;
      remainder -= 1;
      if (remainder <= 0) {
        break;
      }
    }
  }

  while (remainder < 0) {
    for (const index of randomIndexes) {
      if (randomized[index] <= 1) {
        continue;
      }
      randomized[index] -= 1;
      remainder += 1;
      if (remainder >= 0) {
        break;
      }
    }
  }

  return randomized;
}

function getWalletTokenBalanceChangeDetails(transaction, walletAddress, mint) {
  let preAmount = 0n;
  let postAmount = 0n;
  let decimals = 0;

  for (const entry of transaction?.meta?.preTokenBalances ?? []) {
    if (entry?.owner === walletAddress && entry?.mint === mint) {
      preAmount = BigInt(entry?.uiTokenAmount?.amount || '0');
      decimals = Number.isInteger(entry?.uiTokenAmount?.decimals) ? entry.uiTokenAmount.decimals : decimals;
      break;
    }
  }

  for (const entry of transaction?.meta?.postTokenBalances ?? []) {
    if (entry?.owner === walletAddress && entry?.mint === mint) {
      postAmount = BigInt(entry?.uiTokenAmount?.amount || '0');
      decimals = Number.isInteger(entry?.uiTokenAmount?.decimals) ? entry.uiTokenAmount.decimals : decimals;
      break;
    }
  }

  return {
    preAmount,
    postAmount,
    deltaRaw: postAmount - preAmount,
    decimals,
  };
}

async function refreshTradingDeskWalletBalances(userId) {
  const store = await readStore();
  const user = store.users?.[userId];
  if (!user) {
    return null;
  }
  const desk = normalizeUserTradingDesk(user);
  if (desk.wallets.length === 0) {
    return desk;
  }

  const refreshedWallets = await Promise.all(
    desk.wallets.map(async (wallet) => {
      if (!wallet.address) {
        return wallet;
      }
      const lamports = await connection.getBalance(new PublicKey(wallet.address), 'confirmed')
        .catch(() => wallet.currentLamports || 0);
      return normalizeTradingWalletRecord({
        ...wallet,
        currentLamports: lamports,
        currentSol: formatSolAmountFromLamports(lamports),
      });
    }),
  );

  return updateTradingDesk(userId, (draft) => ({
    ...draft,
    wallets: refreshedWallets,
    lastBalanceCheckAt: new Date().toISOString(),
  }));
}

function getTradingBundleTargets(user, selectedMagicBundleId) {
  if (!selectedMagicBundleId) {
    return null;
  }

  const bundle = normalizeUserMagicBundles(user).find((order) =>
    order.id === selectedMagicBundleId
    && !order.archivedAt
    && order.splitCompletedAt
    && Array.isArray(order.splitWallets)
    && order.splitWallets.length > 0
  );
  if (!bundle) {
    return null;
  }

  const targets = bundle.splitWallets
    .map((wallet, index) => ({ wallet: normalizeMagicBundleWalletRecord(wallet), index }))
    .filter(({ wallet }) => wallet.address && wallet.secretKeyB64)
    .map(({ wallet, index }) => {
      const signer = decodeOrderWallet(wallet.secretKeyB64);
      if (signer.publicKey.toBase58() !== wallet.address) {
        return null;
      }
      return {
        kind: 'bundle_wallet',
        label: wallet.label || `Bundle Wallet #${index + 1}`,
        address: wallet.address,
        signer,
      };
    })
    .filter(Boolean);

  if (targets.length === 0) {
    return null;
  }

  return {
    mode: 'bundle',
    bundle,
    targets,
  };
}

function getTradingActiveWalletTarget(desk) {
  const activeWallet = getActiveTradingWalletRecord(desk);
  if (!activeWallet?.address || !activeWallet?.secretKeyB64) {
    return null;
  }

  const signer = decodeOrderWallet(activeWallet.secretKeyB64);
  if (signer.publicKey.toBase58() !== activeWallet.address) {
    throw new Error('Active trading wallet secret does not match its address.');
  }

  return {
    mode: 'wallet',
    bundle: null,
    targets: [{
      kind: 'wallet',
      label: activeWallet.label || 'Trading Wallet',
      address: activeWallet.address,
      signer,
    }],
  };
}

function resolveTradingTargets(user, desk) {
  const bundleTargets = getTradingBundleTargets(user, desk.selectedMagicBundleId);
  if (bundleTargets) {
    return bundleTargets;
  }
  return getTradingActiveWalletTarget(desk);
}

async function executeTradingBuyAcrossTargets(userId, mintAddress, grossLamports, {
  reason = 'manual_buy',
  source = 'quick_trade',
} = {}) {
  const store = await readStore();
  const user = store.users?.[userId];
  if (!user) {
    throw new Error('Trading user record is missing.');
  }

  const desk = normalizeUserTradingDesk(user);
  const execution = resolveTradingTargets(user, desk);
  if (!execution?.targets?.length) {
    throw new Error('No active trading wallet or bundle is ready to trade.');
  }

  const spendPlan = splitLamportsEvenly(grossLamports, execution.targets.length);
  const signatures = [];
  let totalFeeLamports = 0;
  let successCount = 0;
  const errors = [];

  for (const [index, target] of execution.targets.entries()) {
    const desiredGrossLamports = spendPlan[index] || 0;
    if (desiredGrossLamports <= 0) {
      continue;
    }

    try {
      const balanceLamports = await connection.getBalance(target.signer.publicKey, 'confirmed');
      const maxGrossLamports = Math.max(0, balanceLamports - TRADING_DESK_GAS_RESERVE_LAMPORTS);
      const actualGrossLamports = Math.min(desiredGrossLamports, maxGrossLamports);
      const handlingFeeLamports = calculateHandlingFeeLamports(actualGrossLamports);
      const netSpendLamports = actualGrossLamports - handlingFeeLamports;
      if (netSpendLamports <= 0) {
        continue;
      }

      if (handlingFeeLamports > 0) {
        await sendTradingHandlingFee(target.signer, handlingFeeLamports);
        totalFeeLamports += handlingFeeLamports;
      }

      const order = await fetchJupiterSwapOrderFor({
        inputMint: SOL_MINT_ADDRESS,
        outputMint: mintAddress,
        amount: netSpendLamports,
        taker: target.address,
      });
      const result = await executeJupiterSwapFor(order, target.signer);
      if (result?.signature) {
        signatures.push(result.signature);
      }
      successCount += 1;
    } catch (error) {
      errors.push(`${target.label}: ${String(error.message || error)}`);
    }
  }

  if (successCount === 0) {
    throw new Error(errors[0] || 'No trading wallet had enough SOL to complete the buy.');
  }

  const now = new Date().toISOString();
  await updateTradingDesk(userId, (draft) => {
    const nextDesk = normalizeTradingDeskRecord(draft);
    nextDesk.quickTradeMintAddress = mintAddress;
    nextDesk.pendingAction = null;
    nextDesk.lastTradeSide = 'buy';
    nextDesk.lastTradeAt = now;
    nextDesk.lastTradeSignature = signatures[0] ?? null;
    nextDesk.lastError = errors.length > 0 ? errors.join(' | ') : null;
    return nextDesk;
  });

  await appendUserActivityLog(userId, {
    scope: 'buy_sell',
    level: errors.length > 0 ? 'warning' : 'info',
    message: `${source === 'copy_trade' ? 'Copy trading' : 'Buy / Sell'} bought ${mintAddress} across ${successCount} wallet${successCount === 1 ? '' : 's'} for ${formatSolAmountFromLamports(grossLamports)} SOL. Handling fees: ${formatSolAmountFromLamports(totalFeeLamports)} SOL.${errors.length > 0 ? ` Partial errors: ${errors.join(' | ')}` : ''}`,
  });

  await refreshTradingDeskWalletBalances(userId).catch(() => null);
  return {
    signature: signatures[0] ?? null,
    signatures,
    errors,
  };
}

async function executeTradingSellAcrossTargets(userId, mintAddress, sellPercent, {
  reason = 'manual_sell',
  source = 'quick_trade',
} = {}) {
  const store = await readStore();
  const user = store.users?.[userId];
  if (!user) {
    throw new Error('Trading user record is missing.');
  }

  const desk = normalizeUserTradingDesk(user);
  const execution = resolveTradingTargets(user, desk);
  if (!execution?.targets?.length) {
    throw new Error('No active trading wallet or bundle is ready to trade.');
  }

  const signatures = [];
  let totalFeeLamports = 0;
  let successCount = 0;
  const errors = [];

  for (const target of execution.targets) {
    try {
      const totalRaw = await getOwnedMintRawBalance(target.signer.publicKey, mintAddress);
      const amountRaw = (totalRaw * BigInt(sellPercent)) / 100n;
      if (amountRaw <= 0n) {
        continue;
      }

      const order = await fetchJupiterSwapOrderFor({
        inputMint: mintAddress,
        outputMint: SOL_MINT_ADDRESS,
        amount: amountRaw.toString(),
        taker: target.address,
      });
      const result = await executeJupiterSwapFor(order, target.signer);
      const grossSellLamports = Number(result.outputAmountResult ?? order.outAmount ?? 0);
      const handlingFeeLamports = calculateHandlingFeeLamports(grossSellLamports);
      if (handlingFeeLamports > 0) {
        await sendTradingHandlingFee(target.signer, handlingFeeLamports);
        totalFeeLamports += handlingFeeLamports;
      }

      if (result?.signature) {
        signatures.push(result.signature);
      }
      successCount += 1;
    } catch (error) {
      errors.push(`${target.label}: ${String(error.message || error)}`);
    }
  }

  if (successCount === 0) {
    throw new Error(errors[0] || 'No tracked tokens were available to sell.');
  }

  const now = new Date().toISOString();
  await updateTradingDesk(userId, (draft) => {
    const nextDesk = normalizeTradingDeskRecord(draft);
    nextDesk.quickTradeMintAddress = mintAddress;
    nextDesk.pendingAction = null;
    nextDesk.lastTradeSide = 'sell';
    nextDesk.lastTradeAt = now;
    nextDesk.lastTradeSignature = signatures[0] ?? null;
    nextDesk.lastError = errors.length > 0 ? errors.join(' | ') : null;
    return nextDesk;
  });

  await appendUserActivityLog(userId, {
    scope: 'buy_sell',
    level: errors.length > 0 ? 'warning' : 'info',
    message: `${source === 'copy_trade' ? 'Copy trading' : 'Buy / Sell'} sold ${sellPercent}% of ${mintAddress} across ${successCount} wallet${successCount === 1 ? '' : 's'}. Handling fees: ${formatSolAmountFromLamports(totalFeeLamports)} SOL.${errors.length > 0 ? ` Partial errors: ${errors.join(' | ')}` : ''}`,
  });

  await refreshTradingDeskWalletBalances(userId).catch(() => null);
  return {
    signature: signatures[0] ?? null,
    signatures,
    errors,
  };
}

async function processTradingPendingAction(userId, user, desk) {
  if (!desk.pendingAction?.type || !desk.quickTradeMintAddress) {
    return;
  }

  if (desk.pendingAction.type === 'buy') {
    await executeTradingBuyAcrossTargets(
      userId,
      desk.quickTradeMintAddress,
      desk.quickBuyLamports || 0,
      { reason: 'manual_buy', source: 'quick_trade' },
    );
    return;
  }

  if (desk.pendingAction.type === 'sell') {
    await executeTradingSellAcrossTargets(
      userId,
      desk.quickTradeMintAddress,
      desk.quickSellPercent || 100,
      { reason: 'manual_sell', source: 'quick_trade' },
    );
  }
}

async function processTradingLimitOrder(userId, user, desk) {
  if (!desk.limitOrder.enabled || !desk.quickTradeMintAddress || !Number.isFinite(desk.limitOrder.triggerMarketCapUsd)) {
    return;
  }

  const execution = resolveTradingTargets(user, desk);
  if (!execution?.targets?.length) {
    await updateTradingDesk(userId, (draft) => ({
      ...normalizeTradingDeskRecord(draft),
      lastError: 'No trading wallet or bundle is ready for the limit order.',
    }));
    return;
  }

  const solUsdRate = await ensureSolPriceCache();
  const marketInfo = await inspectPumpMintMarket(desk.quickTradeMintAddress, execution.targets[0].signer.publicKey);
  const marketCapUsd = calculateMagicSellCurrentMarketCapUsd(marketInfo, solUsdRate);

  if (!Number.isFinite(marketCapUsd) || marketCapUsd <= 0) {
    await updateTradingDesk(userId, (draft) => {
      const nextDesk = normalizeTradingDeskRecord(draft);
      nextDesk.limitOrder.lastError = 'Market cap data is not available for this token yet.';
      nextDesk.lastError = nextDesk.limitOrder.lastError;
      return nextDesk;
    });
    return;
  }

  const shouldTrigger = desk.limitOrder.side === 'buy'
    ? marketCapUsd <= desk.limitOrder.triggerMarketCapUsd
    : marketCapUsd >= desk.limitOrder.triggerMarketCapUsd;

  if (!shouldTrigger) {
    await updateTradingDesk(userId, (draft) => {
      const nextDesk = normalizeTradingDeskRecord(draft);
      nextDesk.limitOrder.lastError = null;
      nextDesk.lastError = null;
      return nextDesk;
    });
    return;
  }

  const result = desk.limitOrder.side === 'buy'
    ? await executeTradingBuyAcrossTargets(
      userId,
      desk.quickTradeMintAddress,
      desk.limitOrder.buyLamports || 0,
      { reason: 'limit_buy', source: 'limit_order' },
    )
    : await executeTradingSellAcrossTargets(
      userId,
      desk.quickTradeMintAddress,
      desk.limitOrder.sellPercent || 100,
      { reason: 'limit_sell', source: 'limit_order' },
    );

  await updateTradingDesk(userId, (draft) => {
    const nextDesk = normalizeTradingDeskRecord(draft);
    nextDesk.limitOrder.enabled = false;
    nextDesk.limitOrder.lastTriggeredAt = new Date().toISOString();
    nextDesk.limitOrder.lastTriggerSignature = result.signature ?? null;
    nextDesk.limitOrder.lastError = null;
    nextDesk.lastError = null;
    return nextDesk;
  });
}

async function processTradingCopyTrade(userId, user, desk) {
  const copyTrade = desk.copyTrade;
  if (!copyTrade.enabled || !copyTrade.followWalletAddress || !copyTrade.fixedBuyLamports) {
    return;
  }

  const signatures = await connection.getSignaturesForAddress(new PublicKey(copyTrade.followWalletAddress), {
    limit: TRADING_COPY_SCAN_LIMIT,
  });
  if (!Array.isArray(signatures) || signatures.length === 0) {
    return;
  }

  if (!copyTrade.lastSeenSignature) {
    await updateTradingDesk(userId, (draft) => {
      const nextDesk = normalizeTradingDeskRecord(draft);
      nextDesk.copyTrade.lastSeenSignature = signatures[0]?.signature ?? nextDesk.copyTrade.lastSeenSignature;
      nextDesk.lastError = null;
      nextDesk.copyTrade.lastError = null;
      return nextDesk;
    });
    return;
  }

  const toProcess = [];
  for (const info of signatures) {
    if (!info?.signature || info.err) {
      continue;
    }
    if (info.signature === copyTrade.lastSeenSignature) {
      break;
    }
    toProcess.push(info.signature);
  }

  if (toProcess.length === 0) {
    return;
  }

  let buyCount = 0;
  let sellCount = 0;
  let lastCopiedAt = copyTrade.lastCopiedAt;

  for (const signature of toProcess.reverse()) {
    const transaction = await getParsedTransactionBySignature(signature);
    if (!transaction) {
      continue;
    }

    const events = buildWalletTrackerTokenEvents(transaction, copyTrade.followWalletAddress);
    for (const event of events) {
      if (event.direction === 'buy') {
        const result = await executeTradingBuyAcrossTargets(
          userId,
          event.mint,
          copyTrade.fixedBuyLamports,
          { reason: 'copy_buy', source: 'copy_trade' },
        );
        if (result.signature) {
          buyCount += 1;
          lastCopiedAt = new Date().toISOString();
        }
      }

      if (event.direction === 'sell' && copyTrade.copySells) {
        const details = getWalletTokenBalanceChangeDetails(transaction, copyTrade.followWalletAddress, event.mint);
        if (details.preAmount <= 0n || details.deltaRaw >= 0n) {
          continue;
        }
        const sellPercent = Math.max(
          1,
          Math.min(100, Math.round(Number((details.deltaRaw * -100n) / details.preAmount))),
        );
        const result = await executeTradingSellAcrossTargets(
          userId,
          event.mint,
          sellPercent,
          { reason: 'copy_sell', source: 'copy_trade' },
        );
        if (result.signature) {
          sellCount += 1;
          lastCopiedAt = new Date().toISOString();
        }
      }
    }
  }

  await updateTradingDesk(userId, (draft) => {
    const nextDesk = normalizeTradingDeskRecord(draft);
    nextDesk.copyTrade.lastSeenSignature = toProcess[0] ?? nextDesk.copyTrade.lastSeenSignature;
    nextDesk.copyTrade.lastCopiedAt = lastCopiedAt;
    nextDesk.copyTrade.lastError = null;
    nextDesk.copyTrade.stats = {
      buyCount: nextDesk.copyTrade.stats.buyCount + buyCount,
      sellCount: nextDesk.copyTrade.stats.sellCount + sellCount,
    };
    nextDesk.lastError = null;
    return nextDesk;
  });
}

async function scanTradingDesks() {
  const store = await readStore();

  for (const [userId, user] of Object.entries(store.users ?? {})) {
    const desk = normalizeUserTradingDesk(user);
    if (desk.wallets.length === 0 && !desk.selectedMagicBundleId) {
      continue;
    }

    try {
      await refreshTradingDeskWalletBalances(userId).catch(() => null);
      if (desk.pendingAction?.type) {
        await processTradingPendingAction(userId, user, desk);
      }

      const refreshedStore = await readStore();
      const refreshedUser = refreshedStore.users?.[userId];
      if (!refreshedUser) {
        continue;
      }
      const refreshedDesk = normalizeUserTradingDesk(refreshedUser);

      if (refreshedDesk.limitOrder.enabled) {
        await processTradingLimitOrder(userId, refreshedUser, refreshedDesk);
      }

      const latestStore = await readStore();
      const latestUser = latestStore.users?.[userId];
      if (!latestUser) {
        continue;
      }
      const latestDesk = normalizeUserTradingDesk(latestUser);
      if (latestDesk.copyTrade.enabled) {
        await processTradingCopyTrade(userId, latestUser, latestDesk);
      }
    } catch (error) {
      await updateTradingDesk(userId, (draft) => {
        const nextDesk = normalizeTradingDeskRecord(draft);
        nextDesk.pendingAction = null;
        nextDesk.lastError = String(error.message || error);
        return nextDesk;
      }).catch(() => null);

      await appendUserActivityLog(userId, {
        scope: 'buy_sell',
        level: 'error',
        message: `Buy / Sell error: ${String(error.message || error)}`,
      });
    }
  }
}

function getLaunchBuyPublicBaseUrl() {
  const raw = process.env.PUBLIC_APP_BASE_URL?.trim()
    || process.env.TELEGRAM_WEBHOOK_BASE_URL?.trim()
    || process.env.RENDER_EXTERNAL_URL?.trim()
    || null;
  return raw ? raw.replace(/\/+$/, '') : null;
}

function buildLaunchBuyPublicUrl(relativePath) {
  const baseUrl = getLaunchBuyPublicBaseUrl();
  if (!baseUrl) {
    throw new Error('Launch + Buy needs a public app URL before it can publish token metadata.');
  }
  return `${baseUrl}/${String(relativePath || '').replace(/^\/+/, '')}`;
}

async function ensureLaunchBuyMetadata(order) {
  if (!order.logoPath) {
    throw new Error('Launch + Buy is missing its logo asset.');
  }

  await fs.mkdir(LAUNCH_BUY_ASSETS_DIR, { recursive: true });
  const logoFileName = path.basename(order.logoPath);
  const metadataFileName = `${order.id}.json`;
  const imageUrl = buildLaunchBuyPublicUrl(`a/${logoFileName}`);
  const metadataUrl = buildLaunchBuyPublicUrl(`a/${metadataFileName}`);
  const metadata = {
    name: order.tokenName,
    symbol: order.symbol,
    description: order.description,
    image: imageUrl,
    showName: true,
    createdOn: 'Wizard Toolz',
    website: order.website || undefined,
    telegram: order.telegram || undefined,
    twitter: order.twitter || undefined,
  };

  await fs.writeFile(
    path.join(LAUNCH_BUY_ASSETS_DIR, metadataFileName),
    JSON.stringify(metadata, null, 2),
    'utf8',
  );

  return {
    metadataUrl,
    imageUrl,
  };
}

async function refreshLaunchBuySnapshot(order) {
  const signer = decodeOrderWallet(order.walletSecretKeyB64);
  if (signer.publicKey.toBase58() !== order.walletAddress) {
    throw new Error('Launch + Buy wallet secret does not match the stored wallet address.');
  }

  const currentLamports = await connection.getBalance(signer.publicKey, 'confirmed');
  const buyerWallets = await Promise.all(
    (order.buyerWallets || []).map(async (wallet) => {
      if (!wallet.address) {
        return normalizeLaunchBuyBuyerWalletRecord(wallet);
      }
      const lamports = await connection.getBalance(new PublicKey(wallet.address), 'confirmed')
        .catch(() => wallet.currentLamports || 0);
      return normalizeLaunchBuyBuyerWalletRecord({
        ...wallet,
        currentLamports: lamports,
        currentSol: formatSolAmountFromLamports(lamports),
      });
    }),
  );

  return {
    signer,
    currentLamports,
    buyerWallets,
  };
}

async function persistLaunchBuySnapshot(userId, orderId, snapshot, patchOrder = () => ({})) {
  return updateLaunchBuy(userId, orderId, (draft) => ({
    ...draft,
    currentLamports: snapshot.currentLamports,
    currentSol: formatSolAmountFromLamports(snapshot.currentLamports),
    buyerWallets: snapshot.buyerWallets,
    fundedReady: snapshot.currentLamports >= (draft.estimatedTotalNeededLamports || 0),
    updatedAt: new Date().toISOString(),
    ...patchOrder(draft),
  }));
}

function buildLaunchBuyParticipantPlan(order, snapshot) {
  const totalBuyLamports = order.totalBuyLamports || 0;
  const participantCount = 1 + (order.buyerWallets?.length || 0);
  const splitSeed = [
    order.id,
    order.walletAddress || 'launch',
    ...snapshot.buyerWallets.map((wallet, index) => wallet.address || wallet.label || String(index)),
  ].join(':');
  const splitPlan = splitLamportsRandomized(totalBuyLamports, participantCount, {
    varianceBps: 2_800,
    seed: splitSeed,
  });
  const launcherBudgetLamports = splitPlan[0] || 0;

  const buyerPlans = (snapshot.buyerWallets || []).map((wallet, index) => ({
    ...wallet,
    plannedLamports: splitPlan[index + 1] || 0,
    fundingLamports: (splitPlan[index + 1] || 0) + LAUNCH_BUY_BUYER_RESERVE_LAMPORTS,
    fundedLamports: wallet.currentLamports || 0,
  }));

  return {
    launcherBudgetLamports,
    buyerPlans,
  };
}

function advanceSyntheticBondingCurve(curve, grossLamports, tokenAmountRaw) {
  curve.virtualSolReserves = curve.virtualSolReserves.add(new BN(grossLamports));
  curve.realSolReserves = curve.realSolReserves.add(new BN(grossLamports));
  curve.virtualTokenReserves = curve.virtualTokenReserves.sub(tokenAmountRaw);
  curve.realTokenReserves = curve.realTokenReserves.sub(tokenAmountRaw);
  return curve;
}

function chunkLaunchBuyPlansBySize(plans, maxChunkSize = LAUNCH_BUY_MAX_ATOMIC_BUYERS_PER_TX) {
  const size = Math.max(1, maxChunkSize);
  const chunks = [];
  for (let index = 0; index < plans.length; index += size) {
    chunks.push(plans.slice(index, index + size));
  }
  return chunks;
}

function splitLaunchBuyBuyerWaves(plans) {
  const maxAtomicBuyerTxCount = Math.max(1, JITO_MAX_BUNDLE_TRANSACTIONS - 1);
  const maxBuyersPerAtomicTx = 1;
  const atomicBuyerCapacity = maxAtomicBuyerTxCount * maxBuyersPerAtomicTx;
  const atomicBuyers = plans.slice(0, atomicBuyerCapacity);
  const overflowBuyers = plans.slice(atomicBuyerCapacity);
  return {
    atomicGroups: chunkLaunchBuyPlansBySize(atomicBuyers, maxBuyersPerAtomicTx)
      .slice(0, maxAtomicBuyerTxCount),
    overflowBuyers,
  };
}

async function ensureLaunchBuyBuyerFunding(userId, order, snapshot, participantPlan) {
  if (order.launchMode !== 'magic') {
    for (const buyer of shuffleArray(participantPlan.buyerPlans)) {
      if (!buyer.address || buyer.currentLamports >= buyer.fundingLamports || buyer.plannedLamports <= 0) {
        continue;
      }
      const topUpLamports = buyer.fundingLamports - buyer.currentLamports;
      await transferLamportsBetweenWallets(snapshot.signer, buyer.address, topUpLamports);
    }
    const verifiedSnapshot = await refreshLaunchBuySnapshot(order);
    await persistLaunchBuySnapshot(userId, order.id, verifiedSnapshot);
    const verifiedPlan = buildLaunchBuyParticipantPlan(order, verifiedSnapshot);
    const underfundedBuyer = verifiedPlan.buyerPlans.find((buyer) => (
      buyer.address
      && buyer.plannedLamports > 0
      && buyer.currentLamports + LAUNCH_BUY_FUNDING_TOLERANCE_LAMPORTS < buyer.fundingLamports
    ));
    if (underfundedBuyer) {
      throw new Error(
        `Buyer wallet ${underfundedBuyer.label || underfundedBuyer.address} is underfunded `
        + `(${formatSolAmountFromLamports(underfundedBuyer.currentLamports)} SOL / `
        + `${formatSolAmountFromLamports(underfundedBuyer.fundingLamports)} SOL target).`,
      );
    }
    return {
      order,
      routed: true,
      snapshot: verifiedSnapshot,
    };
  }

  if (!order.routingOrderId) {
    const amountLamports = participantPlan.buyerPlans.reduce((sum, wallet) => sum + (wallet.fundingLamports || 0), 0);
    const routingOrder = await createLaunchBuyRoutingOrderData(order, amountLamports);
    await updateLaunchBuy(userId, order.id, (draft) => ({
      ...draft,
      routingQuoteId: routingOrder.quoteId,
      routingOrderId: routingOrder.orderId,
      routingDepositAddress: routingOrder.depositAddress,
      routingStatus: routingOrder.status,
      status: 'routing',
      lastError: null,
    }));
    await transferLamportsBetweenWallets(
      snapshot.signer,
      routingOrder.depositAddress,
      amountLamports,
    );
    await appendUserActivityLog(userId, {
      scope: `launch_buy:${order.id}`,
      level: 'info',
      message: `Launch + Buy magic routing started for ${formatSolAmountFromLamports(amountLamports)} SOL across ${participantPlan.buyerPlans.length} buyer wallets.`,
    });
    return {
      order: normalizeLaunchBuyRecord({
        ...order,
        routingOrderId: routingOrder.orderId,
        routingQuoteId: routingOrder.quoteId,
        routingDepositAddress: routingOrder.depositAddress,
        routingStatus: routingOrder.status,
        status: 'routing',
      }),
      routed: false,
    };
  }

  const routingStatus = await getSplitNowOrderStatus(order.routingOrderId);
  const isComplete = ['completed', 'complete', 'finished', 'done'].includes(String(routingStatus.statusShort || '').toLowerCase());
  await updateLaunchBuy(userId, order.id, (draft) => ({
    ...draft,
    routingStatus: routingStatus.statusText,
    routingCompletedAt: isComplete ? new Date().toISOString() : draft.routingCompletedAt,
    status: isComplete ? 'queued' : 'routing',
    lastError: null,
  }));

  return {
    order: normalizeLaunchBuyRecord({
      ...order,
      routingStatus: routingStatus.statusText,
      routingCompletedAt: isComplete ? new Date().toISOString() : order.routingCompletedAt,
      status: isComplete ? 'queued' : 'routing',
    }),
    routed: isComplete,
    snapshot: isComplete ? await refreshLaunchBuySnapshot(order) : null,
  };
}

async function createLaunchBuyBuyerVersionedTransaction({
  payerSigner,
  mintAddress,
  creator,
  global,
  feeConfig,
  syntheticCurve,
  budgetLamports,
  latestBlockhash = null,
}) {
  const associatedUser = getAssociatedTokenAddressSync(
    mintAddress,
    payerSigner.publicKey,
    true,
    TOKEN_2022_PROGRAM_ID,
  );
  const amount = getBuyTokenAmountFromSolAmount({
    global,
    feeConfig,
    mintSupply: syntheticCurve.tokenTotalSupply,
    bondingCurve: syntheticCurve,
    amount: new BN(budgetLamports),
  });

  const instructions = [
    ComputeBudgetProgram.setComputeUnitLimit({ units: SNIPER_COMPUTE_UNIT_LIMIT }),
    ComputeBudgetProgram.setComputeUnitPrice({ microLamports: SNIPER_PRIORITY_FEE_MICROLAMPORTS }),
    createAssociatedTokenAccountIdempotentInstruction(
      payerSigner.publicKey,
      associatedUser,
      payerSigner.publicKey,
      mintAddress,
      TOKEN_2022_PROGRAM_ID,
      ASSOCIATED_TOKEN_PROGRAM_ID,
    ),
    await PUMP_SDK.buyInstruction({
      global,
      mint: mintAddress,
      creator,
      user: payerSigner.publicKey,
      associatedUser,
      amount,
      solAmount: new BN(budgetLamports),
      slippage: 35,
      tokenProgram: TOKEN_2022_PROGRAM_ID,
      mayhemMode: false,
    }),
  ];

  const transactionMeta = await createVersionedTransactionWithMetadata(
    payerSigner,
    instructions,
    {
      commitment: 'processed',
      latestBlockhash,
    },
  );
  advanceSyntheticBondingCurve(syntheticCurve, budgetLamports, amount);
  return transactionMeta;
}

async function executeLaunchBuyBundle(userId, order, snapshot) {
  order = await getLatestLaunchBuyOrder(userId, order.id) ?? normalizeLaunchBuyRecord(order);
  if (!Array.isArray(order.buyerWallets) || order.buyerWallets.length < order.buyerWalletCount) {
    throw new Error(
      `Launch + Buy requires ${order.buyerWalletCount} buyer wallets, but only `
      + `${Array.isArray(order.buyerWallets) ? order.buyerWallets.length : 0} are configured.`,
    );
  }
  const timing = createTimingTracker();
  const [metadataState, feeConfig, global] = await Promise.all([
    ensureLaunchBuyMetadata(order),
    pumpOnlineSdk.fetchFeeConfig(),
    pumpOnlineSdk.fetchGlobal(),
  ]);
  const { metadataUrl } = metadataState;
  const mintKeypair = Keypair.generate();
  const creator = snapshot.signer.publicKey;

  if (!order.setupFeePaidAt) {
    await sendMagicBundlePlatformFee(snapshot.signer, order.estimatedSetupFeeLamports || 0);
    await updateLaunchBuy(userId, order.id, (draft) => ({
      ...draft,
      setupFeePaidAt: new Date().toISOString(),
      lastError: null,
    }));
    await appendUserActivityLog(userId, {
      scope: `launch_buy:${order.id}`,
      level: 'info',
      message: `Launch + Buy setup fee collected: ${formatSolAmountFromLamports(order.estimatedSetupFeeLamports || 0)} SOL.`,
    });
    snapshot = await refreshLaunchBuySnapshot(order);
  }

  const participantPlan = buildLaunchBuyParticipantPlan(order, snapshot);
  if (participantPlan.buyerPlans.length !== order.buyerWalletCount) {
    throw new Error(
      `Launch + Buy expected ${order.buyerWalletCount} buyer plans, but built ${participantPlan.buyerPlans.length}.`,
    );
  }
  const routingState = await ensureLaunchBuyBuyerFunding(userId, order, snapshot, participantPlan);
  if (!routingState.routed) {
    return false;
  }

  const refreshedSnapshot = routingState.snapshot ?? await refreshLaunchBuySnapshot(order);
  const refreshedPlan = buildLaunchBuyParticipantPlan(order, refreshedSnapshot);
  const syntheticCurve = newBondingCurve(global);
  const sharedLaunchBlockhash = await connection.getLatestBlockhash('processed');

  const launcherAmount = getBuyTokenAmountFromSolAmount({
    global,
    feeConfig,
    mintSupply: syntheticCurve.tokenTotalSupply,
    bondingCurve: syntheticCurve,
    amount: new BN(refreshedPlan.launcherBudgetLamports),
  });

  const launchInstructions = await PUMP_SDK.createV2AndBuyInstructions({
    global,
    mint: mintKeypair.publicKey,
    name: order.tokenName,
    symbol: order.symbol,
    uri: metadataUrl,
    creator,
    user: refreshedSnapshot.signer.publicKey,
    amount: launcherAmount,
    solAmount: new BN(refreshedPlan.launcherBudgetLamports),
    mayhemMode: false,
  });
  const launchInstructionsWithPriority = [
    ComputeBudgetProgram.setComputeUnitLimit({ units: SNIPER_COMPUTE_UNIT_LIMIT }),
    ComputeBudgetProgram.setComputeUnitPrice({ microLamports: SNIPER_PRIORITY_FEE_MICROLAMPORTS }),
    ...launchInstructions,
  ];
  advanceSyntheticBondingCurve(syntheticCurve, refreshedPlan.launcherBudgetLamports, launcherAmount);

  const buyerPlans = refreshedSnapshot.buyerWallets
    .map((wallet, index) => ({
      wallet,
      budgetLamports: refreshedPlan.buyerPlans[index]?.plannedLamports || 0,
    }))
    .filter((item) => item.wallet?.address && item.wallet?.secretKeyB64 && item.budgetLamports > 0);

  const { atomicGroups, overflowBuyers } = splitLaunchBuyBuyerWaves(buyerPlans);
  const buyerTransactionPlans = [];

  for (const group of atomicGroups) {
    for (const item of group) {
      const buyerSigner = decodeOrderWallet(item.wallet.secretKeyB64);
      const meta = await createLaunchBuyBuyerVersionedTransaction({
        payerSigner: buyerSigner,
        mintAddress: mintKeypair.publicKey,
        creator,
        global,
        feeConfig,
        syntheticCurve,
        budgetLamports: item.budgetLamports,
        latestBlockhash: sharedLaunchBlockhash,
      });
      buyerTransactionPlans.push({
        walletAddress: item.wallet.address,
        budgetLamports: item.budgetLamports,
        signers: [buyerSigner],
        meta,
      });
    }
  }
  timing.mark('buildMs');

  const launchTransactionMeta = await createVersionedTransactionForSigners(
    refreshedSnapshot.signer,
    launchInstructionsWithPriority,
    [mintKeypair],
    {
      commitment: 'processed',
      latestBlockhash: sharedLaunchBlockhash,
    },
  );
  const launchTransactionPlan = {
    instructions: launchInstructionsWithPriority,
    signers: [mintKeypair],
    meta: launchTransactionMeta,
  };
  const bundleSubmitStartedAt = Date.now();
  let bundleId = null;
  let bundleSentAt = bundleSubmitStartedAt;
  let bundleWaitMs = 0;
  let primarySignatures = [];
  const launchSignature = await rebroadcastVersionedTransactionUntilSettled(
    launchTransactionPlan.meta.transaction,
    {
      blockhash: launchTransactionPlan.meta.blockhash,
      lastValidBlockHeight: launchTransactionPlan.meta.lastValidBlockHeight,
      commitment: 'processed',
      timeoutMs: 9_000,
      rebroadcastIntervalMs: 350,
      successCheck: () => doesLaunchMintExist(mintKeypair.publicKey),
    },
  );
  bundleSentAt = Date.now();
  primarySignatures.push(launchSignature);
  const launchConfirmed = await doesLaunchMintExist(mintKeypair.publicKey);
  if (!launchConfirmed) {
    throw new Error(`Launch + Buy launch transaction did not land for mint ${mintKeypair.publicKey.toBase58()}.`);
  }
  bundleWaitMs = Date.now() - bundleSentAt;

  const latestStore = await readStore();
  const latestUser = latestStore.users?.[userId] ?? {};
  const sniperCandidates = collectRunnableSniperWizards(latestUser)
    .filter((candidate) => candidate.targetWalletAddress === order.walletAddress);
  const primaryLaunchSignature = primarySignatures.find(Boolean) || null;
  const sniperOrder = sniperCandidates[0] ?? null;
  if (primaryLaunchSignature && sniperOrder) {
    const watcher = sniperWizardSubscriptions.get(userId);
    watcher?.processingSignatures.add(primaryLaunchSignature);
    if (watcher) {
      rememberSniperSignature(watcher, primaryLaunchSignature);
    }
    rememberKnownLaunchMint(primaryLaunchSignature, mintKeypair.publicKey.toBase58());
    void updateSniperWizard(userId, (draft) => ({
      ...draft,
      status: 'launch_detected',
      lastDetectedLaunchSignature: primaryLaunchSignature,
      lastDetectedMintAddress: mintKeypair.publicKey.toBase58(),
      lastError: null,
      stats: {
        ...normalizeSniperWizardStats(draft.stats),
        launchCount: normalizeSniperWizardStats(draft.stats).launchCount + 1,
        lastLaunchSignature: primaryLaunchSignature,
        lastMintAddress: mintKeypair.publicKey.toBase58(),
      },
    })).catch(() => null);
    console.info(
      `[worker] Launch + Buy handoff armed Sniper Wizard ${sniperOrder.id} for mint `
      + `${mintKeypair.publicKey.toBase58()} (${primaryLaunchSignature}).`,
    );

    const sniperSharedLaunchState = {
      tokenProgram: TOKEN_2022_PROGRAM_ID,
      global,
      feeConfig,
    };

    void handleSniperWizardLaunch(
      userId,
      primaryLaunchSignature,
      connection,
      mintKeypair.publicKey.toBase58(),
      sniperSharedLaunchState,
    )
      .catch(async (error) => {
        await updateSniperWizard(userId, (draft) => ({
          ...draft,
          status: draft.automationEnabled ? 'watching' : 'stopped',
          lastError: String(error.message || error),
        })).catch(() => null);
        await appendUserActivityLog(userId, {
          scope: `sniper_wizard:${sniperOrder.id}`,
          level: 'error',
          message: `Sniper Wizard error: ${String(error.message || error)}`,
        }).catch(() => null);
      })
      .finally(() => {
        const latestWatcher = sniperWizardSubscriptions.get(userId);
        latestWatcher?.processingSignatures.delete(primaryLaunchSignature);
      });
  } else {
    console.info(
      `[worker] Launch + Buy skipped sniper handoff for ${order.id}: `
      + `${primaryLaunchSignature ? 'no matching runnable sniper order' : 'missing launch signature'}.`,
    );
  }

  const atomicStartedAt = Date.now();
  const atomicResults = await Promise.allSettled(
    buyerTransactionPlans.map((item) => rebroadcastVersionedTransactionUntilSettled(
      item.meta.transaction,
      {
        blockhash: item.meta.blockhash,
        lastValidBlockHeight: item.meta.lastValidBlockHeight,
        commitment: 'processed',
        timeoutMs: 7_000,
        rebroadcastIntervalMs: 300,
      },
    )),
  );
  for (const result of atomicResults) {
    if (result.status === 'fulfilled' && result.value) {
      primarySignatures.push(result.value);
    }
  }
  console.info(`[worker] Launch + Buy direct atomic wave for ${order.id} sent ${buyerTransactionPlans.length} buyer tx(s) in ${Date.now() - atomicStartedAt}ms.`);

  let overflowResults = [];
  if (overflowBuyers.length > 0) {
    const overflowTransactions = [];
    for (const item of overflowBuyers) {
      const buyerSigner = decodeOrderWallet(item.wallet.secretKeyB64);
      overflowTransactions.push({
        ...item,
        meta: await createLaunchBuyBuyerVersionedTransaction({
          payerSigner: buyerSigner,
          mintAddress: mintKeypair.publicKey,
          creator,
          global,
          feeConfig,
          syntheticCurve,
          budgetLamports: item.budgetLamports,
          latestBlockhash: sharedLaunchBlockhash,
        }),
      });
    }

    const overflowStartedAt = Date.now();
    overflowResults = await Promise.allSettled(
      overflowTransactions.map((item) => rebroadcastVersionedTransactionUntilSettled(
        item.meta.transaction,
        {
          blockhash: item.meta.blockhash,
          lastValidBlockHeight: item.meta.lastValidBlockHeight,
          commitment: 'processed',
          timeoutMs: 7_000,
          rebroadcastIntervalMs: 300,
        },
      )),
    );
    timing.mark('overflowMs');
    console.info(`[worker] Launch + Buy overflow wave for ${order.id} sent ${overflowTransactions.length} buyer tx(s) in ${Date.now() - overflowStartedAt}ms.`);
  }

  await updateLaunchBuy(userId, order.id, (draft) => ({
    ...draft,
    stats: (() => {
      const existing = normalizeLaunchBuyStats(draft.stats);
      const launchCount = existing.launchCount + 1;
      const totalMs = timing.elapsedMs;
      const timingSnapshot = timing.snapshot();
      return {
        ...existing,
        launchCount,
        successCount: existing.successCount + 1,
        atomicWaveCount: existing.atomicWaveCount + 1,
        overflowWaveCount: existing.overflowWaveCount + (overflowBuyers.length > 0 ? 1 : 0),
        atomicBuyerWalletCount: atomicGroups.flat().length,
        overflowBuyerWalletCount: overflowBuyers.length,
        lastBuildMs: timingSnapshot.buildMs || 0,
        lastBundleSubmitMs: bundleSentAt - bundleSubmitStartedAt,
        lastBundleWaitMs: bundleWaitMs,
        lastOverflowMs: timingSnapshot.overflowMs || 0,
        lastTotalLatencyMs: totalMs,
        bestTotalLatencyMs: existing.bestTotalLatencyMs > 0 ? Math.min(existing.bestTotalLatencyMs, totalMs) : totalMs,
        avgTotalLatencyMs: launchCount > 0
          ? Math.round((((existing.avgTotalLatencyMs || 0) * existing.launchCount) + totalMs) / launchCount)
          : totalMs,
        lastBundleTransactionCount: 1 + buyerTransactionPlans.length,
        lastMintAddress: mintKeypair.publicKey.toBase58(),
        lastBundleId: bundleId,
      };
    })(),
    status: 'completed',
    launchedMintAddress: mintKeypair.publicKey.toBase58(),
    launchBundleId: bundleId,
    launchSignatures: [
      ...primarySignatures,
      ...overflowResults
        .filter((item) => item.status === 'fulfilled')
        .map((item) => item.value)
        .filter(Boolean),
    ],
    launchedAt: new Date().toISOString(),
    lastError: null,
  }));

  await appendUserActivityLog(userId, {
    scope: `launch_buy:${order.id}`,
    level: 'info',
    message: `Launch + Buy completed for ${order.tokenName} (${mintKeypair.publicKey.toBase58()}) through direct launch + warmed buyer execution. Atomic wave used ${1 + buyerTransactionPlans.length} transaction(s) for ${atomicGroups.flat().length} buyer wallet(s) in about ${timing.elapsedMs}ms${overflowBuyers.length > 0 ? `, then pushed ${overflowBuyers.length} overflow buyer wallet(s) immediately after landing.` : '.'}`,
  });

  return true;
}

async function scanLaunchBuys() {
  const store = await readStore();

  for (const [userId, user] of Object.entries(store.users ?? {})) {
    for (const order of normalizeUserLaunchBuys(user)) {
      if (order.archivedAt || !['queued', 'routing', 'launching', 'ready', 'awaiting_funds', 'setup'].includes(order.status)) {
        continue;
      }

      try {
        const snapshot = await refreshLaunchBuySnapshot(order);
        const buyerReserveLamports = estimateLaunchBuyBuyerReserveLamports(order.buyerWalletCount);
        const requiredLamports = order.setupFeePaidAt
          ? (
            (order.totalBuyLamports || 0)
            + (order.jitoTipLamports || 0)
            + (order.estimatedRoutingFeeLamports || 0)
            + buyerReserveLamports
          )
          : (order.estimatedTotalNeededLamports || 0);
        const fundedReady = snapshot.currentLamports >= requiredLamports;
        await persistLaunchBuySnapshot(userId, order.id, snapshot, () => ({
          status: order.status === 'completed'
            ? 'completed'
            : (fundedReady ? (order.status === 'setup' ? 'ready' : order.status) : 'awaiting_funds'),
          lastError: null,
        }));

        if (
          fundedReady
          && order.launchMode !== 'magic'
          && ['ready', 'setup', 'awaiting_funds'].includes(order.status)
        ) {
          const participantPlan = buildLaunchBuyParticipantPlan(order, snapshot);
          const warmState = await ensureLaunchBuyBuyerFunding(userId, order, snapshot, participantPlan);
          snapshot = warmState.snapshot ?? await refreshLaunchBuySnapshot(order);
        }

        if (order.status === 'queued' || order.status === 'routing') {
          await waitForSniperWatchersReady(order.walletAddress);
          await updateLaunchBuy(userId, order.id, (draft) => ({
            ...draft,
            status: draft.status === 'routing' ? 'routing' : 'launching',
            lastError: null,
          }));
          await executeLaunchBuyBundle(userId, normalizeLaunchBuyRecord(order), snapshot);
        }
      } catch (error) {
        await updateLaunchBuy(userId, order.id, (draft) => ({
          ...draft,
          status: 'failed',
          lastError: String(error.message || error),
        })).catch(() => null);

        await appendUserActivityLog(userId, {
          scope: `launch_buy:${order.id}`,
          level: 'error',
          message: `Launch + Buy error: ${String(error.message || error)}`,
        });
      }
    }
  }
}

async function scanCommunityVisions() {
  const store = await readStore();

  for (const [userId, user] of Object.entries(store.users ?? {})) {
    for (const order of normalizeUserCommunityVisions(user)) {
      if (!hasMeaningfulCommunityVisionRecord(order) || order.archivedAt || !order.handle || !order.automationEnabled) {
        continue;
      }

      const lastCheckedMs = order.lastCheckedAt ? Date.parse(order.lastCheckedAt) : 0;
      if (Number.isFinite(lastCheckedMs) && lastCheckedMs > 0 && Date.now() - lastCheckedMs < cfg.communityVision.intervalMs) {
        continue;
      }

      try {
        if (!cfg.communityVision.enabled) {
          await updateCommunityVision(userId, order.id, (draft) => ({
            ...draft,
            status: 'offline',
            lastCheckedAt: new Date().toISOString(),
            lastError: 'Community Vision feed is not configured yet.',
          }));
          continue;
        }

        const communities = await fetchCommunityVisionCommunities(order.handle);
        const existing = Array.isArray(order.trackedCommunities) ? order.trackedCommunities : [];
        const existingMap = new Map(existing.map((item) => [item.id, item]));
        const currentMap = new Map(communities.map((item) => [item.id, item]));
        let renameCount = 0;

        if (existing.length > 0) {
          for (const [id, previousCommunity] of existingMap.entries()) {
            const currentCommunity = currentMap.get(id);
            if (!currentCommunity || currentCommunity.name === previousCommunity.name) {
              continue;
            }

            renameCount += 1;
            await sendTelegramText(
              userId,
              formatCommunityVisionAlert(order.handle, currentCommunity, previousCommunity.name, currentCommunity.name),
            );
            await appendUserActivityLog(userId, {
              scope: `community_vision:${order.id}`,
              level: 'info',
              message: `Community Vision saw a rename for @${order.handle}: ${previousCommunity.name} -> ${currentCommunity.name}.`,
            });
          }
        }

        await updateCommunityVision(userId, order.id, (draft) => ({
          ...draft,
          trackedCommunities: existing.length > 0
            ? existing.map((item) => currentMap.get(item.id) ?? item)
            : communities,
          status: 'watching',
          stats: {
            ...normalizeCommunityVisionStats(draft.stats),
            renameCount: normalizeCommunityVisionStats(draft.stats).renameCount + renameCount,
            alertCount: normalizeCommunityVisionStats(draft.stats).alertCount + renameCount,
          },
          lastCheckedAt: new Date().toISOString(),
          lastAlertAt: renameCount > 0 ? new Date().toISOString() : draft.lastAlertAt,
          lastChangeAt: renameCount > 0 ? new Date().toISOString() : draft.lastChangeAt,
          lastError: null,
        }));
      } catch (error) {
        await updateCommunityVision(userId, order.id, (draft) => ({
          ...draft,
          status: 'failed',
          lastCheckedAt: new Date().toISOString(),
          lastError: String(error.message || error),
        }));
        await appendUserActivityLog(userId, {
          scope: `community_vision:${order.id}`,
          level: 'error',
          message: `Community Vision error: ${String(error.message || error)}`,
        });
      }
    }
  }
}

async function scanWalletTrackers() {
  const store = await readStore();

  for (const [userId, user] of Object.entries(store.users ?? {})) {
    for (const order of normalizeUserWalletTrackers(user)) {
      if (!hasMeaningfulWalletTrackerRecord(order) || order.archivedAt || !order.walletAddress || !order.automationEnabled) {
        continue;
      }

      const lastCheckedMs = order.lastCheckedAt ? Date.parse(order.lastCheckedAt) : 0;
      if (Number.isFinite(lastCheckedMs) && lastCheckedMs > 0 && Date.now() - lastCheckedMs < WALLET_TRACKER_SCAN_INTERVAL_MS) {
        continue;
      }

      try {
        const signatures = await connection.getSignaturesForAddress(new PublicKey(order.walletAddress), {
          limit: WALLET_TRACKER_SCAN_LIMIT,
        });

        if (!Array.isArray(signatures) || signatures.length === 0) {
          await updateWalletTracker(userId, order.id, (draft) => ({
            ...draft,
            status: 'watching',
            lastCheckedAt: new Date().toISOString(),
            lastError: null,
          }));
          continue;
        }

        if (!order.lastSeenSignature) {
          await updateWalletTracker(userId, order.id, (draft) => ({
            ...draft,
            status: 'watching',
            lastSeenSignature: signatures[0]?.signature ?? draft.lastSeenSignature,
            lastCheckedAt: new Date().toISOString(),
            lastError: null,
          }));
          continue;
        }

        const toProcess = [];
        for (const info of signatures) {
          if (!info?.signature || info.err) {
            continue;
          }
          if (info.signature === order.lastSeenSignature) {
            break;
          }
          toProcess.push(info);
        }

        let stats = normalizeWalletTrackerStats(order.stats);
        let notifiedBuyMints = Array.isArray(order.notifiedBuyMints) ? [...order.notifiedBuyMints] : [];
        let lastEventAt = order.lastEventAt;
        let lastAlertAt = order.lastAlertAt;

        for (const info of toProcess.reverse()) {
          const transaction = await getParsedTransactionBySignature(info.signature);
          if (!transaction) {
            continue;
          }

          if (order.notifyLaunches) {
            const launchMint = extractPumpLaunchMintAddress(transaction);
            if (launchMint) {
              await sendTelegramText(userId, formatWalletTrackerAlert(order.walletAddress, 'launch', { mint: launchMint }));
              stats.launchCount += 1;
              lastEventAt = new Date().toISOString();
              lastAlertAt = lastEventAt;
              await appendUserActivityLog(userId, {
                scope: `wallet_tracker:${order.id}`,
                level: 'info',
                message: `Wallet Tracker saw a token launch from ${order.walletAddress}: ${launchMint}.`,
              });
            }
          }

          const tokenEvents = buildWalletTrackerTokenEvents(transaction, order.walletAddress);
          for (const event of tokenEvents) {
            if (event.direction === 'buy' && order.buyMode !== 'off') {
              const firstOnlyBlocked = order.buyMode === 'first' && notifiedBuyMints.includes(event.mint);
              if (!firstOnlyBlocked) {
                await sendTelegramText(userId, formatWalletTrackerAlert(order.walletAddress, 'buy', {
                  mint: event.mint,
                  amountDisplay: formatTokenAmountFromRaw(event.deltaRaw.toString(), event.decimals),
                }));
                stats.buyAlertCount += 1;
                if (order.buyMode === 'first') {
                  notifiedBuyMints.push(event.mint);
                  notifiedBuyMints = [...new Set(notifiedBuyMints)].slice(-100);
                }
                lastEventAt = new Date().toISOString();
                lastAlertAt = lastEventAt;
              }
            }

            if (event.direction === 'sell' && order.notifySells) {
              await sendTelegramText(userId, formatWalletTrackerAlert(order.walletAddress, 'sell', {
                mint: event.mint,
                amountDisplay: formatTokenAmountFromRaw((event.deltaRaw * -1n).toString(), event.decimals),
              }));
              stats.sellAlertCount += 1;
              lastEventAt = new Date().toISOString();
              lastAlertAt = lastEventAt;
            }
          }
        }

        const newestSeenSignature = toProcess[0]?.signature ?? order.lastSeenSignature;
        await updateWalletTracker(userId, order.id, (draft) => ({
          ...draft,
          status: 'watching',
          stats,
          notifiedBuyMints,
          lastSeenSignature: newestSeenSignature,
          lastCheckedAt: new Date().toISOString(),
          lastAlertAt,
          lastEventAt,
          lastError: null,
        }));
      } catch (error) {
        await updateWalletTracker(userId, order.id, (draft) => ({
          ...draft,
          status: 'failed',
          lastCheckedAt: new Date().toISOString(),
          lastError: String(error.message || error),
        }));
        await appendUserActivityLog(userId, {
          scope: `wallet_tracker:${order.id}`,
          level: 'error',
          message: `Wallet Tracker error: ${String(error.message || error)}`,
        });
      }
    }
  }
}

async function scanDevWalletSwap() {
  if (!cfg.devWalletSwap.enabled) {
    return;
  }

  const signer = cfg.devWalletSwap.signer;
  const balanceLamports = await connection.getBalance(signer.publicKey, 'confirmed');
  const store = await readStore();
  const reservedCreatorLamports = getReservedDevWalletLamports(store.worker);
  const swappableLamports = Math.max(
    0,
    balanceLamports - cfg.devWalletSwap.reserveLamports - reservedCreatorLamports,
  );
  const currentState = store.worker?.devWalletSwap ?? createDefaultWorkerState().devWalletSwap;
  const now = new Date().toISOString();

  await updateWorkerState((draft) => ({
    ...draft,
    devWalletSwap: {
      ...draft.devWalletSwap,
      status: swappableLamports >= cfg.devWalletSwap.minimumLamports ? draft.devWalletSwap.status : 'idle',
      lastCheckedAt: now,
      lastBalanceLamports: balanceLamports,
      lastBalanceSol: formatSolAmountFromLamports(balanceLamports),
      lastSwappableLamports: swappableLamports,
      lastError: swappableLamports >= cfg.devWalletSwap.minimumLamports
        ? draft.devWalletSwap.lastError
        : null,
    },
  }));

  if (shouldDevWalletSwapYieldToCreatorRewards(store.worker)) {
    return;
  }

  if (BigInt(currentState.pendingBurnAmount || '0') <= 0n) {
    const targetTokenAccounts = await getOwnedTokenAccountsForMint(signer.publicKey, cfg.devWalletSwap.targetMint);
    const strayTargetBalance = targetTokenAccounts.reduce((sum, account) => sum + BigInt(account.amount), 0n);
    if (strayTargetBalance > 0n) {
      await updateWorkerState((draft) => ({
        ...draft,
        devWalletSwap: {
          ...draft.devWalletSwap,
          pendingBurnAmount: strayTargetBalance.toString(),
        },
      }));
      await processPendingBurn(strayTargetBalance.toString());
      return;
    }
  }

  if (shouldAttemptPendingBurn(currentState)) {
    await processPendingBurn(currentState.pendingBurnAmount);
    return;
  }

  if (!shouldAttemptDevWalletSwap(currentState, swappableLamports)) {
    return;
  }

  try {
    await updateWorkerState((draft) => ({
      ...draft,
      devWalletSwap: {
        ...draft.devWalletSwap,
        status: 'processing',
        lastAttemptedAt: new Date().toISOString(),
        lastInputLamports: swappableLamports,
        lastQuotedOutputAmount: null,
        lastOutputAmount: null,
        pendingBurnAmount: null,
        lastRouter: null,
        lastMode: null,
        lastSignature: null,
        lastError: null,
      },
    }));

    const order = await fetchJupiterSwapOrder(swappableLamports);
    await updateWorkerState((draft) => ({
      ...draft,
      devWalletSwap: {
        ...draft.devWalletSwap,
        lastQuotedOutputAmount: order.outAmount ?? null,
        lastRouter: order.router ?? null,
        lastMode: order.mode ?? null,
      },
    }));

    const result = await executeJupiterSwap(order);
    const burnAmount = result.outputAmountResult ?? order.outAmount ?? null;
    const balanceAfterLamports = await connection.getBalance(signer.publicKey, 'confirmed');
    await updateWorkerState((draft) => ({
      ...draft,
      devWalletSwap: {
        ...draft.devWalletSwap,
        status: burnAmount ? 'burn_pending' : 'completed',
        lastCheckedAt: new Date().toISOString(),
        lastProcessedAt: new Date().toISOString(),
        lastBalanceLamports: balanceAfterLamports,
        lastBalanceSol: formatSolAmountFromLamports(balanceAfterLamports),
        lastSwappableLamports: Math.max(0, balanceAfterLamports - cfg.devWalletSwap.reserveLamports),
        lastOutputAmount: result.outputAmountResult ?? null,
        pendingBurnAmount: burnAmount,
        lastSignature: result.signature ?? null,
        lastError: null,
      },
    }));

    console.log(
      `[worker] Dev wallet swap completed: ${formatSolAmountFromLamports(swappableLamports)} SOL -> ${cfg.devWalletSwap.targetMint} (${result.signature})`,
    );

    if (burnAmount) {
      await processPendingBurn(burnAmount);
    }
  } catch (error) {
    const balanceAfterFailureLamports = await connection.getBalance(signer.publicKey, 'confirmed')
      .catch(() => null);

    await updateWorkerState((draft) => ({
      ...draft,
      devWalletSwap: {
        ...draft.devWalletSwap,
        status: 'failed',
        lastCheckedAt: new Date().toISOString(),
        lastBalanceLamports: Number.isInteger(balanceAfterFailureLamports)
          ? balanceAfterFailureLamports
          : draft.devWalletSwap.lastBalanceLamports,
        lastBalanceSol: Number.isInteger(balanceAfterFailureLamports)
          ? formatSolAmountFromLamports(balanceAfterFailureLamports)
          : draft.devWalletSwap.lastBalanceSol,
        lastSwappableLamports: Number.isInteger(balanceAfterFailureLamports)
          ? Math.max(0, balanceAfterFailureLamports - cfg.devWalletSwap.reserveLamports)
          : draft.devWalletSwap.lastSwappableLamports,
        lastError: String(error.message || error),
      },
    }));

    console.error('[worker] Dev wallet swap failed:', error.message || error);
  }
}

async function scanStakingRewards() {
  const store = await readStore();
  const currentState = store.worker?.stakingRewards ?? createDefaultWorkerState().stakingRewards;
  const nowIso = new Date().toISOString();

  if (!cfg.platformRevenue.enabled) {
    if (currentState.status !== 'disabled') {
      store.worker = {
        ...store.worker,
        stakingRewards: {
          ...currentState,
          enabled: false,
          status: 'disabled',
          lastCheckedAt: nowIso,
          lastError: cfg.platformRevenue.reason,
        },
      };
      await writeStore(store);
    }
    return;
  }

  let rewardsVaultBalanceLamports = 0;
  try {
    rewardsVaultBalanceLamports = await connection.getBalance(
      new PublicKey(cfg.platformRevenue.rewardsVaultAddress),
      'confirmed',
    );
  } catch (error) {
    store.worker = {
      ...store.worker,
      stakingRewards: {
        ...currentState,
        enabled: true,
        status: 'error',
        lastCheckedAt: nowIso,
        lastError: String(error.message || error),
      },
    };
    await writeStore(store);
    console.error('[worker] Staking rewards scan failed:', error.message || error);
    return;
  }

  const availableVaultLamports = Math.max(
    0,
    rewardsVaultBalanceLamports - STAKING_REWARDS_VAULT_RESERVE_LAMPORTS,
  );
  const previousObservedVaultLamports = Number.isInteger(currentState.lastObservedVaultLamports)
    ? currentState.lastObservedVaultLamports
    : null;
  const incomingLamports = previousObservedVaultLamports === null
    ? availableVaultLamports
    : Math.max(0, availableVaultLamports - previousObservedVaultLamports);

  let pendingUndistributedLamports = Number.parseInt(
    String(currentState.pendingUndistributedLamports || '0'),
    10,
  );
  if (!Number.isInteger(pendingUndistributedLamports) || pendingUndistributedLamports < 0) {
    pendingUndistributedLamports = 0;
  }
  pendingUndistributedLamports += incomingLamports;

  let mintMetadata;
  try {
    mintMetadata = await getMintMetadata(cfg.platformRevenue.tokenMint);
  } catch (error) {
    store.worker = {
      ...store.worker,
      stakingRewards: {
        ...currentState,
        enabled: true,
        status: 'error',
        lastCheckedAt: nowIso,
        lastObservedVaultLamports: availableVaultLamports,
        lastObservedVaultSol: formatSolAmountFromLamports(availableVaultLamports),
        lastError: String(error.message || error),
      },
    };
    await writeStore(store);
    console.error('[worker] Staking mint metadata read failed:', error.message || error);
    return;
  }

  const trackedEntries = [];
  let totalTrackedRaw = 0n;
  let totalWeightedRaw = 0n;
  let totalTrackedWallets = 0;

  for (const [userId, user] of Object.entries(store.users ?? {})) {
    const staking = normalizeStakingState(user?.staking);
    if (!staking.walletAddress) {
      continue;
    }

    let currentRaw = BigInt(staking.totalStakedRaw || '0');
    let stakingError = null;
    try {
      currentRaw = await getOwnedMintRawBalance(new PublicKey(staking.walletAddress), cfg.platformRevenue.tokenMint);
    } catch (error) {
      stakingError = String(error.message || error);
    }

    const previousRaw = BigInt(staking.totalStakedRaw || '0');
    let trackingStartedAt = staking.trackingStartedAt;
    if (currentRaw <= 0n) {
      trackingStartedAt = null;
    } else if (!trackingStartedAt || currentRaw !== previousRaw) {
      trackingStartedAt = nowIso;
    }

    const weightProfile = currentRaw > 0n
      ? getStakingWeightProfile(trackingStartedAt)
      : { bps: 0, label: 'Not Staking' };
    const weightedRaw = currentRaw > 0n ? currentRaw * BigInt(weightProfile.bps) : 0n;
    if (currentRaw > 0n) {
      totalTrackedWallets += 1;
      totalTrackedRaw += currentRaw;
      totalWeightedRaw += weightedRaw;
    }

    user.staking = normalizeStakingState({
      ...staking,
      status: currentRaw > 0n
        ? ((staking.claimableLamports || 0) >= staking.claimThresholdLamports ? 'claim-ready' : 'tracking')
        : 'linked',
      manualClaimOnly: true,
      rewardsAsset: 'SOL',
      claimThresholdLamports: STAKING_MIN_CLAIM_LAMPORTS,
      totalStakedRaw: currentRaw.toString(),
      totalStakedDisplay: formatTokenAmountFromRaw(currentRaw, mintMetadata.decimals),
      trackingStartedAt,
      lastBalanceSyncedAt: nowIso,
      currentWeightLabel: weightProfile.label,
      lastError: stakingError,
    });

    trackedEntries.push({ userId, user, weightedRaw });
  }

  let distributedLamports = 0;
  if (pendingUndistributedLamports > 0 && totalWeightedRaw > 0n) {
    const eligibleEntries = trackedEntries.filter((entry) => entry.weightedRaw > 0n);
    let remainingLamports = pendingUndistributedLamports;
    for (let index = 0; index < eligibleEntries.length; index += 1) {
      const entry = eligibleEntries[index];
      const shareLamports = index === eligibleEntries.length - 1
        ? remainingLamports
        : Number((BigInt(pendingUndistributedLamports) * entry.weightedRaw) / totalWeightedRaw);
      const clampedShareLamports = Math.max(0, Math.min(remainingLamports, shareLamports));
      if (clampedShareLamports > 0) {
        const staking = normalizeStakingState(entry.user.staking);
        const nextClaimableLamports = staking.claimableLamports + clampedShareLamports;
        entry.user.staking = normalizeStakingState({
          ...staking,
          claimableLamports: nextClaimableLamports,
          lastRewardsAllocatedAt: nowIso,
          status: nextClaimableLamports >= staking.claimThresholdLamports ? 'claim-ready' : 'tracking',
          lastError: null,
        });
        distributedLamports += clampedShareLamports;
        remainingLamports -= clampedShareLamports;
      }
    }
    pendingUndistributedLamports = remainingLamports;
  }

  store.worker = {
    ...store.worker,
    stakingRewards: {
      ...currentState,
      enabled: true,
      status: totalTrackedWallets > 0 ? 'tracking' : 'idle',
      mint: cfg.platformRevenue.tokenMint,
      rewardsVaultAddress: cfg.platformRevenue.rewardsVaultAddress,
      reserveLamports: STAKING_REWARDS_VAULT_RESERVE_LAMPORTS,
      claimThresholdLamports: STAKING_MIN_CLAIM_LAMPORTS,
      earlyWeightDays: STAKING_EARLY_WEIGHT_DAYS,
      pendingUndistributedLamports,
      lastCheckedAt: nowIso,
      lastObservedVaultLamports: availableVaultLamports,
      lastObservedVaultSol: formatSolAmountFromLamports(availableVaultLamports),
      lastDistributedAt: distributedLamports > 0 ? nowIso : currentState.lastDistributedAt,
      lastDistributedLamports: distributedLamports > 0
        ? distributedLamports
        : currentState.lastDistributedLamports,
      totalDistributedLamports: (Number.isInteger(currentState.totalDistributedLamports)
        ? currentState.totalDistributedLamports
        : 0) + distributedLamports,
      totalTrackedRaw: totalTrackedRaw.toString(),
      totalTrackedWallets,
      lastError: null,
    },
  };

  await writeStore(store);

  if (distributedLamports > 0) {
    console.log(
      `[worker] Staking rewards allocated: ${formatSolAmountFromLamports(distributedLamports)} SOL across ${totalTrackedWallets} tracked wallet(s).`,
    );
  }
}

function shouldRunWorkerScan(scanKey, intervalMs, now = Date.now()) {
  const previousRunAt = workerScanLastRunAt.get(scanKey) || 0;
  if (now - previousRunAt < intervalMs) {
    return false;
  }
  workerScanLastRunAt.set(scanKey, now);
  return true;
}

async function runScheduledWorkerScan(scanKey, intervalMs, now, fn) {
  if (!shouldRunWorkerScan(scanKey, intervalMs, now)) {
    return;
  }

  try {
    await fn();
  } catch (error) {
    console.warn(`[worker] ${scanKey} scan failed:`, error?.message || error);
  }
}

async function runWorkerCycle() {
  if (cycleInFlight) {
    return;
  }

  cycleInFlight = true;
  try {
    const now = Date.now();

    await runScheduledWorkerScan('launch', WORKER_LAUNCH_SCAN_INTERVAL_MS, now, scanLaunchBuys);
    await runScheduledWorkerScan('sniper', WORKER_SNIPER_SCAN_INTERVAL_MS, now, scanSniperWizards);
    await runScheduledWorkerScan('trading', WORKER_TRADING_SCAN_INTERVAL_MS, now, scanTradingDesks);
    await runScheduledWorkerScan('orders', WORKER_ORDER_SCAN_INTERVAL_MS, now, scanOrders);
    await runScheduledWorkerScan('wallet_tracker', WALLET_TRACKER_SCAN_INTERVAL_MS, now, scanWalletTrackers);
    await runScheduledWorkerScan('community_vision', COMMUNITY_VISION_SCAN_INTERVAL_MS, now, scanCommunityVisions);
    await runScheduledWorkerScan('fomo', WORKER_AUTOMATION_SCAN_INTERVAL_MS, now, scanFomoBoosters);
    await runScheduledWorkerScan('magic_bundle', WORKER_AUTOMATION_SCAN_INTERVAL_MS, now, scanMagicBundles);
    await runScheduledWorkerScan('magic_sell', WORKER_AUTOMATION_SCAN_INTERVAL_MS, now, scanMagicSells);
    await runScheduledWorkerScan('holder_booster', WORKER_AUTOMATION_SCAN_INTERVAL_MS, now, scanHolderBoosters);
    await runScheduledWorkerScan('organic_booster', WORKER_AUTOMATION_SCAN_INTERVAL_MS, now, scanOrganicBoosters);
    await runScheduledWorkerScan('burn_agents', BURN_AGENT_INTERVAL_MS, now, scanUserBurnAgents);
    await runScheduledWorkerScan('pump_creator_rewards', PUMP_CREATOR_REWARD_INTERVAL_MS, now, scanPumpCreatorRewards);
    await runScheduledWorkerScan('jito_tip_cache', JITO_TIP_PREFETCH_INTERVAL_MS, now, warmJitoTipAccountsCache);
    await runScheduledWorkerScan('staking', WORKER_STAKING_SCAN_INTERVAL_MS, now, scanStakingRewards);
    await runScheduledWorkerScan('dev_swap', WORKER_DEV_SWAP_SCAN_INTERVAL_MS, now, scanDevWalletSwap);
  } finally {
    cycleInFlight = false;
  }
}

await ensureStore();
console.log(`[worker] Treasury split worker watching ${STORE_PATH}`);
console.log(`[worker] Poll interval: ${WORKER_POLL_INTERVAL_MS}ms`);
console.log(`[worker] RPC pool: ${cfg.rpcUrls.length} endpoint(s). Active: ${cfg.rpcUrl}`);
console.log(
  `[worker] Multi-user Burn Agent worker enabled: interval ${BURN_AGENT_INTERVAL_MS}ms, min claim ${formatSolAmountFromLamports(BURN_AGENT_MINIMUM_CLAIM_LAMPORTS)} SOL, buy slippage ${BURN_AGENT_BUY_SLIPPAGE_PERCENT}%, fee reserve ${formatSolAmountFromLamports(BURN_AGENT_FEE_RESERVE_LAMPORTS)} SOL`,
);
if (cfg.devWalletSwap.enabled) {
  console.log(
    `[worker] Dev wallet auto-swap enabled: SOL -> ${cfg.devWalletSwap.targetMint}, reserve ${formatSolAmountFromLamports(cfg.devWalletSwap.reserveLamports)} SOL, minimum ${formatSolAmountFromLamports(cfg.devWalletSwap.minimumLamports)} SOL`,
  );
} else {
  console.log(`[worker] Dev wallet auto-swap disabled: ${cfg.devWalletSwap.reason}`);
}
if (cfg.pumpCreatorRewards.enabled) {
  console.log(
    `[worker] Pump creator rewards enabled: claim every ${cfg.pumpCreatorRewards.intervalMs}ms, mint ${cfg.pumpCreatorRewards.mint}, min claim ${formatSolAmountFromLamports(cfg.pumpCreatorRewards.minimumClaimLamports)} SOL, buy slippage ${cfg.pumpCreatorRewards.buySlippagePercent}%`,
  );
} else {
  console.log(`[worker] Pump creator rewards disabled: ${cfg.pumpCreatorRewards.reason}`);
}
if (cfg.platformRevenue.enabled) {
  console.log(
    `[worker] Staking rewards tracker enabled: mint ${cfg.platformRevenue.tokenMint}, rewards vault ${cfg.platformRevenue.rewardsVaultAddress}, min claim ${formatSolAmountFromLamports(STAKING_MIN_CLAIM_LAMPORTS)} SOL`,
  );
} else {
  console.log(`[worker] Staking rewards tracker disabled: ${cfg.platformRevenue.reason}`);
}
await runWorkerCycle();
setInterval(() => {
  void runWorkerCycle();
}, WORKER_POLL_INTERVAL_MS);
