# WIZARD TOOLZ

Professional Telegram automation stack for Solana growth, trading, launch flows, tracking, and utility tools.

## Project Layout

- `steel-tester/` - main application code
- `steel-tester/bot.js` - Telegram bot UI and webhook server
- `steel-tester/worker-bot.js` - background automation worker
- `steel-tester/assets/` - branded Telegram menu artwork and website files
- `steel-tester/data/telegram-store.json` - runtime state store

## Local Development

From `steel-tester/`:

```powershell
npm install
powershell -ExecutionPolicy Bypass -File .\start-all.ps1
```

That startup flow:

- rotates old log files into `steel-tester/logs/archive/`
- starts the Telegram bot
- starts the worker process

## Render Deployment

This project is set up to run as a single Render web service.

- Root directory: `steel-tester`
- Start command: `npm start`
- The bot runs in webhook mode when `TELEGRAM_TRANSPORT=webhook`
- The bot serves:
  - `GET /` and `GET /healthz` for health checks
  - `POST <TELEGRAM_WEBHOOK_PATH>` for Telegram updates

Recommended environment variables:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_TRANSPORT=webhook`
- `TELEGRAM_WEBHOOK_BASE_URL=https://your-service.onrender.com`
- `TELEGRAM_WEBHOOK_PATH=/telegram/webhook`
- `TELEGRAM_WEBHOOK_SECRET=<long-random-secret>`
- `SOLANA_RPC_URL`
- `SOLANA_RPC_URLS`
- `SOLANA_WS_URLS`
- `JUPITER_API_KEY`
- `SOLANA_RECEIVE_ADDRESS`
- `TREASURY_WALLET_ADDRESS`
- `DEV_WALLET_ADDRESS`
- feature-specific envs you actually plan to enable on launch day:
  - `DEV_WALLET_SECRET_KEY_B64`
  - `VOLUME_TRIAL_WALLET_ADDRESS`
  - `VOLUME_TRIAL_WALLET_SECRET_KEY_B64`
  - `SPLITNOW_API_KEY`
  - `COMMUNITY_VISION_API_URL`
  - `COMMUNITY_VISION_API_BEARER_TOKEN`

## Launch-Day Render Checklist

- `render.yaml` points to `steel-tester/` and uses `npm start`
- health check is served at `/healthz`
- bot runs in `webhook` mode on Render
- `TELEGRAM_WEBHOOK_BASE_URL` or `RENDER_EXTERNAL_URL` is set
- `TELEGRAM_WEBHOOK_SECRET` is set
- `SOLANA_RPC_URLS` is set with multiple mainnet HTTP endpoints
- `SOLANA_WS_URLS` is set with matching mainnet WSS endpoints for watcher failover
- `SOLANA_RECEIVE_ADDRESS` is real
- `TREASURY_WALLET_ADDRESS` is real
- `DEV_WALLET_ADDRESS` is real
- `JUPITER_API_KEY` is real
- `SPLITNOW_API_KEY` is set only if stealth features are meant to be live
- `COMMUNITY_VISION_API_URL` is set only if Vision is meant to be live
- `VOLUME_TRIAL_WALLET_*` is set only if the trial is meant to be live

## Notes

- Runtime secrets and state are ignored by `.gitignore`
- Old root `.log` files are rotated before local startup
- The bot artwork is mapped to the matching Telegram menus
- The website pages live inside `steel-tester/assets/`
