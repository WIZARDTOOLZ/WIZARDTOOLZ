# WIZARD TOOLZ Staking Plan

## Core Goal

Make people want to hold WIZARD TOOLZ for real time, not just jump in for a snapshot and leave.

## Locked Direction So Far

- staking starts immediately from launch
- rewards accrue immediately
- early accrual is intentionally weak
- loyalty gets better over time through multiplier tiers
- standard unstake uses a cooldown
- no instant unstake path
- claims are manual inside the Telegram bot
- rewards are paid in `SOL`, not in extra WIZARD TOOLZ

## Recommended V1 Defaults

- standard unstake cooldown: `7 days`
- reward accrual: `continuous`
- minimum claim threshold: `0.01 SOL`
- reward style: `SOL` rewards from real platform revenue, not fake APR

## Revenue Routing

For all net platform profit:

- `50%` treasury
- `25%` buyback + burn
- `25%` `SOL` to the rewards vault

This includes creator rewards because creator rewards are pure platform profit in this model.

## Reward Vault Model

### SOL Rewards Vault

- one `SOL` rewards vault
- funded by:
  - `25%` of creator rewards
  - `25%` of all net platform profit
- claims are manual from the bot once the user crosses the minimum claim threshold

### Staking Vault

- one program-owned token vault for staked WIZARD TOOLZ
- staked tokens should not sit in an owner wallet
- no owner-drain path in the final contract

## User Story

Users stake WIZARD TOOLZ and build reward weight over time.

They are rewarded for:

- size of stake
- time in stake
- staying held and tracked over time

They are not rewarded for:

- fast in / fast out behavior
- pure snapshot games

## Fairness Model

The intended reward system is:

- time-weighted staking
- age multipliers
- continuous off-chain accrual from new rewards-vault inflows
- no minimum stake requirement
- minimum claim threshold instead of a minimum stake threshold
- configurable rates and splits

### Suggested Weight Curve

- `0-7 days`: low accrual weight
- `8-30 days`: base weight
- `31-90 days`: boosted weight
- `91-180 days`: stronger boost
- `180+ days`: strongest boost

Exact multiplier values remain configurable in the staking config account.

## Manual Claims

Recommended v1 behavior:

- rewards build immediately in the background
- users manually claim from the Telegram bot
- claims only go out once claimable rewards are at least `0.01 SOL`

Why:

- cleaner accounting
- avoids dust payouts
- easier for users to understand
- easier to audit against the live rewards vault

## Still To Wire After Model Approval

- on-chain staking vault + SOL rewards vault authority model
- hard-staking deposit / unstake transaction flow
