import { createHash } from 'node:crypto';

export function normalizeWebhookPath(value) {
  if (!value) {
    return '/telegram/webhook';
  }

  return value.startsWith('/') ? value : `/${value}`;
}

export function buildWebhookUrl(cfg) {
  if (!cfg.telegramWebhookBaseUrl) {
    return null;
  }

  return `${cfg.telegramWebhookBaseUrl.replace(/\/+$/, '')}${cfg.telegramWebhookPath}`;
}

export function createBotConfig({
  env,
  bundlePricing,
  getBundlePricing,
  parsePositiveInts,
  parseSolToLamports,
  parsePositiveInt,
  parseOptionalInt,
  parseCsv,
  formatSolAmountFromLamports,
}) {
  const missing = [];
  const telegramToken = env.TELEGRAM_BOT_TOKEN;
  if (!telegramToken) missing.push('TELEGRAM_BOT_TOKEN');

  const defaultTarget = env.TARGET_URL;
  if (!defaultTarget) missing.push('TARGET_URL');

  if (missing.length) {
    throw new Error(`Missing required environment variables: ${missing.join(', ')}`);
  }

  const defaultBundleAmounts = Object.keys(bundlePricing).map((amount) => Number(amount));
  const configuredBundleAmounts = parsePositiveInts(env.PACKAGE_AMOUNTS, defaultBundleAmounts)
    .filter((amount) => getBundlePricing(amount));
  const solanaRpcUrls = [
    ...parseCsv(env.SOLANA_RPC_URLS),
    ...(env.SOLANA_RPC_URL?.trim() ? [env.SOLANA_RPC_URL.trim()] : []),
  ].filter(Boolean).filter((url, index, array) => array.indexOf(url) === index);
  const volumeTrialMinLamports = parseSolToLamports(env.VOLUME_TRIAL_MIN_SOL || '0.001');
  const volumeTrialMaxLamports = parseSolToLamports(env.VOLUME_TRIAL_MAX_SOL || '0.003');
  const volumeTrialMinIntervalSeconds = parsePositiveInt(env.VOLUME_TRIAL_MIN_INTERVAL_SECONDS, 8);
  const volumeTrialMaxIntervalSeconds = parsePositiveInt(env.VOLUME_TRIAL_MAX_INTERVAL_SECONDS, 18);
  const volumeTrialWalletAddress = env.VOLUME_TRIAL_WALLET_ADDRESS?.trim() || null;
  const volumeTrialSecretKeyB64 = env.VOLUME_TRIAL_WALLET_SECRET_KEY_B64?.trim() || null;
  const volumeTrialSecretKeyJson = env.VOLUME_TRIAL_WALLET_SECRET_KEY?.trim() || null;
  const magicBundleSplitNowFeeEstimateBps = parsePositiveInt(env.MAGIC_BUNDLE_SPLITNOW_FEE_ESTIMATE_BPS, 100);
  const magicBundleStealthSetupFeeLamports = parseSolToLamports(env.MAGIC_BUNDLE_STEALTH_SETUP_FEE_SOL || '0.05');
  const tradingHandlingFeeBps = parsePositiveInt(env.TRADING_HANDLING_FEE_BPS, 50);
  const telegramTransport = (env.TELEGRAM_TRANSPORT?.trim().toLowerCase()
    || (env.RENDER_EXTERNAL_URL?.trim() ? 'webhook' : 'polling'));
  const telegramWebhookBaseUrl = env.TELEGRAM_WEBHOOK_BASE_URL?.trim()
    || env.RENDER_EXTERNAL_URL?.trim()
    || null;
  const telegramWebhookPath = normalizeWebhookPath(
    env.TELEGRAM_WEBHOOK_PATH?.trim()
    || `/telegram/${createHash('sha256').update(telegramToken).digest('hex').slice(0, 24)}`,
  );
  const telegramWebhookSecret = env.TELEGRAM_WEBHOOK_SECRET?.trim()
    || createHash('sha256').update(`${telegramToken}:wizardtoolz`).digest('hex');
  const telegramWebhookPort = Number.parseInt(env.PORT || '10000', 10);

  return {
    telegramToken,
    telegramTransport: telegramTransport === 'webhook' ? 'webhook' : 'polling',
    telegramWebhookBaseUrl,
    telegramWebhookPath,
    telegramWebhookSecret,
    telegramWebhookPort: Number.isFinite(telegramWebhookPort) && telegramWebhookPort > 0
      ? telegramWebhookPort
      : 10000,
    defaultTarget,
    bannerImagePath: env.BANNER_IMAGE_PATH?.trim() || null,
    adminIds: new Set(parseCsv(env.TELEGRAM_ADMIN_IDS).map((item) => String(item))),
    freeTrialAmount: parsePositiveInt(env.FREE_TRIAL_AMOUNT, 5),
    packageAmounts: configuredBundleAmounts.length > 0 ? configuredBundleAmounts : defaultBundleAmounts,
    runnerMode: (env.RUNNER_MODE || 'command').toLowerCase(),
    runnerCommand: env.RUNNER_COMMAND?.trim() || 'node safe-steel-runner.js',
    solanaReceiveAddress: env.SOLANA_RECEIVE_ADDRESS?.trim() || null,
    solanaRpcUrl: solanaRpcUrls[0] || 'https://api.mainnet-beta.solana.com',
    solanaRpcUrls: solanaRpcUrls.length > 0 ? solanaRpcUrls : ['https://api.mainnet-beta.solana.com'],
    solanaRpcRotationCooldownMs: parsePositiveInt(env.SOLANA_RPC_ROTATION_COOLDOWN_MS, 8_000),
    solanaRpcTimeoutMs: parsePositiveInt(env.SOLANA_RPC_TIMEOUT_MS, 4_000),
    solanaRpcMaxRetries: parsePositiveInt(env.SOLANA_RPC_MAX_RETRIES, 4),
    solanaQuoteTtlMinutes: parsePositiveInt(env.SOLANA_QUOTE_TTL_MINUTES, 15),
    solanaPaymentToleranceLamports: parsePositiveInt(env.SOLANA_PAYMENT_TOLERANCE_LAMPORTS, 5_000),
    solanaPaymentPollMs: parsePositiveInt(env.SOLANA_PAYMENT_POLL_MS, 20_000),
    solanaTxScanLimit: parsePositiveInt(env.SOLANA_TX_SCAN_LIMIT, 25),
    solanaPriceApiBaseUrl: env.SOLANA_PRICE_API_BASE_URL?.trim() || 'https://api.coinbase.com',
    salesChannelId: env.TELEGRAM_SALES_CHANNEL_ID?.trim() || null,
    salesThreadId: parseOptionalInt(env.TELEGRAM_SALES_THREAD_ID, null),
    alertsChannelId: env.TELEGRAM_ALERTS_CHANNEL_ID?.trim() || '@wizardtoolz_alerts',
    alertsThreadId: parseOptionalInt(env.TELEGRAM_ALERTS_THREAD_ID, null),
    treasuryWalletAddress: env.TREASURY_WALLET_ADDRESS?.trim() || 'TREASURY_WALLET_PLACEHOLDER',
    devWalletAddress: env.DEV_WALLET_ADDRESS?.trim() || 'DEV_WALLET_PLACEHOLDER',
    volumeTrialEnabled: Boolean(
      volumeTrialWalletAddress
      && (volumeTrialSecretKeyB64 || volumeTrialSecretKeyJson)
    ),
    volumeTrialMinSol: formatSolAmountFromLamports(Math.min(volumeTrialMinLamports, volumeTrialMaxLamports)),
    volumeTrialMaxSol: formatSolAmountFromLamports(Math.max(volumeTrialMinLamports, volumeTrialMaxLamports)),
    volumeTrialMinLamports: Math.min(volumeTrialMinLamports, volumeTrialMaxLamports),
    volumeTrialMaxLamports: Math.max(volumeTrialMinLamports, volumeTrialMaxLamports),
    volumeTrialMinIntervalSeconds: Math.min(volumeTrialMinIntervalSeconds, volumeTrialMaxIntervalSeconds),
    volumeTrialMaxIntervalSeconds: Math.max(volumeTrialMinIntervalSeconds, volumeTrialMaxIntervalSeconds),
    volumeTrialTradeGoal: parsePositiveInt(env.VOLUME_TRIAL_TRADE_GOAL, 5),
    splitnowEnabled: Boolean(env.SPLITNOW_API_KEY?.trim()),
    magicBundlePlatformFeeBps: 0,
    magicBundleSplitNowFeeEstimateBps,
    magicBundleStealthSetupFeeLamports,
    magicBundleStealthSetupFeeSol: formatSolAmountFromLamports(magicBundleStealthSetupFeeLamports),
    tradingHandlingFeeBps,
  };
}
