export function buildBuySellText({
  user,
  cfg,
  menuDivider,
  normalizeTradingDesk,
  getActiveTradingWallet,
}) {
  const tradingDesk = normalizeTradingDesk(user.tradingDesk);
  const activeWallet = getActiveTradingWallet(user);
  const selectedBundle = user.magicBundles?.find((bundle) => bundle.id === tradingDesk.selectedMagicBundleId) ?? null;

  return [
    '\u{1F4B1} *Buy / Sell Desk*',
    '',
    'Your central trading workspace for wallets, bundle-linked wallets, and trade-ready setup.',
    '',
    menuDivider,
    '\u2728 *Desk Overview*',
    `- Active wallet: *${activeWallet ? activeWallet.label : 'Not set'}*`,
    `- Wallet address: ${activeWallet ? `\`${activeWallet.address}\`` : 'Add or generate a wallet first'}`,
    `- Wallet count: *${tradingDesk.wallets.length}*`,
    `- Selected bundle: *${selectedBundle ? (selectedBundle.tokenName || selectedBundle.id) : 'None selected'}*`,
    `- Token CA: ${tradingDesk.quickTradeMintAddress ? `\`${tradingDesk.quickTradeMintAddress}\`` : '*Not set*'}`,
    '',
    '\u{1F4CA} *What You Can Do Here*',
    '- Set the token mint / CA you want ready for trading',
    '- Import an existing wallet or generate a fresh one',
    '- Choose which wallet should stay active on the desk',
    '- Link supported source wallets into one place for easier trading',
    `- Built-in trading routes use a *${formatBpsPercent(cfg.tradingHandlingFeeBps)}* handling fee per executed trade`,
    '',
    '\u26A0\uFE0F *Hot-Wallet Warning*',
    'Any wallet imported here should be treated like a live trading wallet with real funds.',
    'Generated and imported private keys are hidden by default inside the bot. Never share them with support.',
    ...(tradingDesk.awaitingField ? ['', promptForBuySellField(tradingDesk.awaitingField)] : []),
    ...(tradingDesk.lastError ? ['', `Last error: \`${tradingDesk.lastError}\``] : []),
  ].join('\n');

  function formatBpsPercent(bps) {
    return `${(bps / 100).toFixed(Number.isInteger(bps / 100) ? 0 : 2)}%`;
  }

  function promptForBuySellField(field) {
    switch (field) {
      case 'quick_trade_mint':
        return 'Send the token CA / mint you want ready on this desk.';
      case 'import_wallet':
        return 'Paste the wallet private key you want to import.';
      default:
        return 'Send the next trading-desk value to continue.';
    }
  }
}

export function buildHomeText({ cfg, menuDivider, supportUsername }) {
  return [
    '\u{1F44B} *Welcome to WIZARD TOOLZ*',
    '',
    'A premium Telegram platform for Solana growth, trading, launches, automation, and utility tools.',
    '',
    menuDivider,
    '\u2728 *How It Works*',
    '1. Choose the tool you want from the home menu.',
    '2. Follow the setup steps shown on that screen.',
    '3. Fund the generated wallet only if that feature asks for funding.',
    '4. For launch and sniper flows, funding early gives the worker time to warm wallets before the live action.',
    '5. Start, refresh, monitor, and manage everything from Telegram.',
    '',
    menuDivider,
    '\u{1F680} *What The Platform Covers*',
    'Volume, visibility, burns, holder distribution, bundles, launch tools, smart selling, wallet tracking, social services, subscriptions, accounts, and branded utility tools.',
    '',
    '\u{1F4CA} Professional execution | Free trial available on supported routes | Built to be simple on the surface and powerful underneath',
    `\u{1F4B8} Built-in handled trading routes use *${formatBpsPercent(cfg.tradingHandlingFeeBps)}* per trade, lower than every competitor we track, and platform profit is routed *50% to treasury, 25% to buyback + burn, and 25% to the rewards vault*`,
    `\u{1F91D} Need help? Message support: \`@${supportUsername}\``,
    '\u{1F4AC} Community chat: `@wizardtoolz`',
    '\u{1F514} Alerts channel: `@wizardtoolz_alerts`',
    `\u{1F6E0}\uFE0F Want custom tools built for you? Message support: \`@${supportUsername}\``,
    '',
    'Choose a service below to get started.',
  ].join('\n');

  function formatBpsPercent(bps) {
    return `${(bps / 100).toFixed(Number.isInteger(bps / 100) ? 0 : 2)}%`;
  }
}

export function buildHelpText({ cfg, menuDivider, supportUsername }) {
  return [
    '\u2139\uFE0F *Help & Info*',
    '',
    'Welcome to *WIZARD TOOLZ* - a premium Telegram control panel for Solana growth, trading, launch, automation, and utility tools.',
    '',
    menuDivider,
    '\u2728 *How To Use The Bot*',
    '1. Open the feature you want from the home menu.',
    '2. Read the screen and complete the setup fields it asks for.',
    '3. Fund the generated wallet only if that feature requires funding.',
    '4. For launch and sniper flows, funding early gives the worker time to warm wallets before the live action.',
    '5. Start the automation and use Refresh to see the latest status, balances, and stats.',
    '',
    menuDivider,
    '\u{1F6E1}\uFE0F *Safety Notes*',
    `- Free trial is limited to one run per Telegram account at x${cfg.freeTrialAmount}.`,
    '- Paid quotes are tied to the selected package and expire automatically.',
    '- Wallet-based tools control real on-chain funds, so treat every private key as a hot wallet.',
    '- Generated and imported keys are hidden by default in the bot and only revealed back to your own chat when you choose.',
    '- Always double-check mint addresses, treasury addresses, and deposit amounts before funding.',
    '',
    menuDivider,
    '\u{1F91D} *Support*',
    `Questions or issues: \`@${supportUsername}\``,
    'Community chat: `@wizardtoolz`',
    'Alerts channel: `@wizardtoolz_alerts`',
    `Custom tools: message \`@${supportUsername}\``,
    '',
    'Tap a feature button below for a deeper plain-English walkthrough.',
  ].join('\n');
}

