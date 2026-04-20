# WIZARD TOOLZ Staking

Anchor workspace for the WIZARD TOOLZ staking system.

## V1 Model

- stake the WIZARD TOOLZ token into a program vault
- rewards accrue immediately, but early positions have low weight
- standard unstake uses a cooldown
- no instant unstake path
- rewards are paid in `SOL`
- claims are manual from the Telegram bot
- one `SOL` rewards vault is funded by:
  - `25%` creator rewards
  - `25%` net platform profit

## Current Scope

This first program pass implements:

- config initialization
- admin config updates
- per-user staking positions
- cooldown unstake flow
- funding instructions for the staking vault / rewards vault model

This pass does **not** yet implement the final claim-distribution path. The intended next layer is:

- continuous off-chain reward allocation from new rewards-vault inflows
- on-chain Merkle or direct claim distribution
- Telegram / website integration

## Recommended V1 Defaults

- unstake cooldown: `7 days`
- no minimum stake requirement
- reward accrual: `continuous`
- claim threshold: `0.01 SOL`
- revenue split on net platform profit:
  - `50% treasury`
  - `25% buyback + burn`
  - `25% SOL -> rewards vault`
