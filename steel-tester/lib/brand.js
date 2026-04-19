import path from 'node:path';

export const ROOT_DIR = path.resolve('.');
export const DATA_DIR = path.join(ROOT_DIR, 'data');
export const STORE_PATH = path.join(DATA_DIR, 'telegram-store.json');
export const ASSETS_DIR = path.join(ROOT_DIR, 'assets');

export const MENU_LOGO_IMAGE_PATH = path.join(ASSETS_DIR, 'trendingtoolz logo.png');
export const REACTION_MENU_IMAGE_PATH = path.join(ASSETS_DIR, 'trendingtoolz reaction image.png');
export const VOLUME_MENU_IMAGE_PATH = path.join(ASSETS_DIR, 'trendingtoolz volume image.png');
export const BURN_AGENT_MENU_IMAGE_PATH = path.join(ASSETS_DIR, 'trendingtoolz burn agent image.png');
export const HOLDER_BOOSTER_MENU_IMAGE_PATH = path.join(ASSETS_DIR, 'trendingtoolz holder booster.png');
export const FOMO_MENU_IMAGE_PATH = path.join(ASSETS_DIR, 'trendingtoolz fomo.png');
export const MAGIC_SELL_MENU_IMAGE_PATH = path.join(ASSETS_DIR, 'trendingtoolz magic sell.png');
export const SNIPER_MENU_IMAGE_PATH = path.join(ASSETS_DIR, 'trendingtoolz sniper.png');
export const MENU_HOME_IMAGE_PATH = MENU_LOGO_IMAGE_PATH;
export const MENU_EMOJI_IMAGE_PATH = REACTION_MENU_IMAGE_PATH;
export const SALES_BROADCAST_IMAGE_PATH = REACTION_MENU_IMAGE_PATH;
export const BURN_AGENT_ALERT_IMAGE_PATH = BURN_AGENT_MENU_IMAGE_PATH;

export const BRAND_NAME = 'Wizard Toolz';
export const BRAND_TAGLINE = 'Solana growth, trading, launch, and utility tooling';
export const SUPPORT_USERNAME = 'wizard_toolz';
export const TG_DIVIDER = '\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501';

export const RESIZER_PRESETS = {
  logo: {
    key: 'logo',
    label: 'Logo',
    emoji: '\u{1F5BC}\uFE0F',
    ratioLabel: '1:1',
    width: 1024,
    height: 1024,
    filename: 'logo-square.png',
  },
  banner: {
    key: 'banner',
    label: 'Banner',
    emoji: '\u{1F304}',
    ratioLabel: '1:3',
    width: 1500,
    height: 500,
    filename: 'banner-wide.png',
  },
};
