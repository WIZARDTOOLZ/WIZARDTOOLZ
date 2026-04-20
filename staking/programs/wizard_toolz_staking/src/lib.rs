use anchor_lang::prelude::*;
use anchor_spl::token_interface::{
    self, Mint, TokenAccount, TokenInterface, TransferChecked,
};

declare_id!("Fg6PaFpoGXkYsidMpWxTWqkZgfXrRzPf7hhF5TzS2dQ");

const BPS_DENOMINATOR: u64 = 10_000;

#[program]
pub mod wizard_toolz_staking {
    use super::*;

    pub fn initialize_config(
        ctx: Context<InitializeConfig>,
        params: InitializeConfigParams,
    ) -> Result<()> {
        params.validate()?;

        let config = &mut ctx.accounts.config;
        config.authority = ctx.accounts.authority.key();
        config.mint = ctx.accounts.mint.key();
        config.token_program = ctx.accounts.token_program.key();
        config.stake_vault = ctx.accounts.stake_vault.key();
        config.bump = ctx.bumps.config;
        config.stake_vault_bump = ctx.bumps.stake_vault;
        config.unstake_cooldown_seconds = params.unstake_cooldown_seconds;
        config.epoch_duration_seconds = params.epoch_duration_seconds;
        config.early_accrual_bps = params.early_accrual_bps;
        config.tier_1_age_seconds = params.tier_1_age_seconds;
        config.tier_1_multiplier_bps = params.tier_1_multiplier_bps;
        config.tier_2_age_seconds = params.tier_2_age_seconds;
        config.tier_2_multiplier_bps = params.tier_2_multiplier_bps;
        config.tier_3_age_seconds = params.tier_3_age_seconds;
        config.tier_3_multiplier_bps = params.tier_3_multiplier_bps;
        config.tier_4_age_seconds = params.tier_4_age_seconds;
        config.tier_4_multiplier_bps = params.tier_4_multiplier_bps;
        config.total_staked = 0;
        config.total_positions = 0;

        emit!(ConfigInitialized {
            authority: config.authority,
            mint: config.mint,
            unstake_cooldown_seconds: config.unstake_cooldown_seconds,
            epoch_duration_seconds: config.epoch_duration_seconds,
        });

        Ok(())
    }

    pub fn update_config(
        ctx: Context<UpdateConfig>,
        params: UpdateConfigParams,
    ) -> Result<()> {
        params.validate()?;

        let config = &mut ctx.accounts.config;
        config.unstake_cooldown_seconds = params.unstake_cooldown_seconds;
        config.epoch_duration_seconds = params.epoch_duration_seconds;
        config.early_accrual_bps = params.early_accrual_bps;
        config.tier_1_age_seconds = params.tier_1_age_seconds;
        config.tier_1_multiplier_bps = params.tier_1_multiplier_bps;
        config.tier_2_age_seconds = params.tier_2_age_seconds;
        config.tier_2_multiplier_bps = params.tier_2_multiplier_bps;
        config.tier_3_age_seconds = params.tier_3_age_seconds;
        config.tier_3_multiplier_bps = params.tier_3_multiplier_bps;
        config.tier_4_age_seconds = params.tier_4_age_seconds;
        config.tier_4_multiplier_bps = params.tier_4_multiplier_bps;

        emit!(ConfigUpdated {
            authority: ctx.accounts.authority.key(),
            mint: config.mint,
            unstake_cooldown_seconds: config.unstake_cooldown_seconds,
            epoch_duration_seconds: config.epoch_duration_seconds,
        });

        Ok(())
    }

    pub fn stake(ctx: Context<Stake>, amount: u64) -> Result<()> {
        require!(amount > 0, StakingError::NothingToTransfer);

        transfer_from_user(
            &ctx.accounts.owner,
            &ctx.accounts.owner_token_account,
            &ctx.accounts.stake_vault,
            &ctx.accounts.mint,
            &ctx.accounts.token_program,
            amount,
        )?;

        let now = Clock::get()?.unix_timestamp;
        let user_state = &mut ctx.accounts.user_state;
        if user_state.owner == Pubkey::default() {
            user_state.owner = ctx.accounts.owner.key();
            user_state.config = ctx.accounts.config.key();
            user_state.next_position_id = 0;
            user_state.total_active_staked = 0;
            user_state.total_positions_open = 0;
        }

        let position_id = user_state.next_position_id;
        let position = &mut ctx.accounts.position;
        position.config = ctx.accounts.config.key();
        position.user_state = user_state.key();
        position.owner = ctx.accounts.owner.key();
        position.position_id = position_id;
        position.amount = amount;
        position.staked_at = now;
        position.cooldown_requested_at = 0;
        position.status = PositionStatus::Active as u8;

        user_state.next_position_id = user_state
            .next_position_id
            .checked_add(1)
            .ok_or(StakingError::MathOverflow)?;
        user_state.total_active_staked = user_state
            .total_active_staked
            .checked_add(amount)
            .ok_or(StakingError::MathOverflow)?;
        user_state.total_positions_open = user_state
            .total_positions_open
            .checked_add(1)
            .ok_or(StakingError::MathOverflow)?;

        let config = &mut ctx.accounts.config;
        config.total_staked = config
            .total_staked
            .checked_add(amount)
            .ok_or(StakingError::MathOverflow)?;
        config.total_positions = config
            .total_positions
            .checked_add(1)
            .ok_or(StakingError::MathOverflow)?;

        emit!(PositionStaked {
            owner: ctx.accounts.owner.key(),
            config: config.key(),
            position_id,
            amount,
            staked_at: now,
        });

        Ok(())
    }

    pub fn request_unstake(ctx: Context<ModifyPosition>) -> Result<()> {
        let position = &mut ctx.accounts.position;
        require!(
            position.status == PositionStatus::Active as u8,
            StakingError::InvalidPositionState
        );

        let now = Clock::get()?.unix_timestamp;
        position.status = PositionStatus::Cooldown as u8;
        position.cooldown_requested_at = now;

        emit!(UnstakeRequested {
            owner: ctx.accounts.owner.key(),
            config: ctx.accounts.config.key(),
            position_id: position.position_id,
            amount: position.amount,
            cooldown_requested_at: now,
        });

        Ok(())
    }

    pub fn cancel_unstake(ctx: Context<ModifyPosition>) -> Result<()> {
        let position = &mut ctx.accounts.position;
        require!(
            position.status == PositionStatus::Cooldown as u8,
            StakingError::InvalidPositionState
        );

        position.status = PositionStatus::Active as u8;
        position.cooldown_requested_at = 0;

        emit!(UnstakeCancelled {
            owner: ctx.accounts.owner.key(),
            config: ctx.accounts.config.key(),
            position_id: position.position_id,
        });

        Ok(())
    }

    pub fn claim_unstake(ctx: Context<ClaimUnstake>) -> Result<()> {
        let position = &ctx.accounts.position;

        require!(
            position.status == PositionStatus::Cooldown as u8,
            StakingError::InvalidPositionState
        );

        let now = Clock::get()?.unix_timestamp;
        let cooldown_ends_at = position
            .cooldown_requested_at
            .checked_add(ctx.accounts.config.unstake_cooldown_seconds as i64)
            .ok_or(StakingError::MathOverflow)?;
        require!(now >= cooldown_ends_at, StakingError::CooldownStillActive);

        transfer_from_vault(
            &ctx.accounts.config,
            &ctx.accounts.stake_vault,
            &ctx.accounts.owner_token_account,
            &ctx.accounts.mint,
            &ctx.accounts.token_program,
            position.amount,
        )?;

        let config = &mut ctx.accounts.config;
        let user_state = &mut ctx.accounts.user_state;
        user_state.total_active_staked = user_state
            .total_active_staked
            .checked_sub(position.amount)
            .ok_or(StakingError::MathOverflow)?;
        user_state.total_positions_open = user_state
            .total_positions_open
            .checked_sub(1)
            .ok_or(StakingError::MathOverflow)?;
        config.total_staked = config
            .total_staked
            .checked_sub(position.amount)
            .ok_or(StakingError::MathOverflow)?;
        config.total_positions = config
            .total_positions
            .checked_sub(1)
            .ok_or(StakingError::MathOverflow)?;

        emit!(UnstakeClaimed {
            owner: ctx.accounts.owner.key(),
            config: config.key(),
            position_id: position.position_id,
            amount: position.amount,
            claimed_at: now,
        });

        Ok(())
    }
}

#[derive(Accounts)]
pub struct InitializeConfig<'info> {
    #[account(mut)]
    pub authority: Signer<'info>,
    pub mint: InterfaceAccount<'info, Mint>,
    #[account(
        init,
        payer = authority,
        space = StakingConfig::INIT_SPACE,
        seeds = [b"config", mint.key().as_ref()],
        bump
    )]
    pub config: Account<'info, StakingConfig>,
    #[account(
        init,
        payer = authority,
        token::mint = mint,
        token::authority = config,
        token::token_program = token_program,
        seeds = [b"stake-vault", config.key().as_ref()],
        bump
    )]
    pub stake_vault: InterfaceAccount<'info, TokenAccount>,
    pub token_program: Interface<'info, TokenInterface>,
    pub system_program: Program<'info, System>,
}

#[derive(Accounts)]
pub struct UpdateConfig<'info> {
    #[account(mut, address = config.authority)]
    pub authority: Signer<'info>,
    #[account(
        mut,
        seeds = [b"config", config.mint.as_ref()],
        bump = config.bump
    )]
    pub config: Account<'info, StakingConfig>,
}

#[derive(Accounts)]
pub struct Stake<'info> {
    #[account(mut)]
    pub owner: Signer<'info>,
    pub mint: InterfaceAccount<'info, Mint>,
    #[account(
        mut,
        seeds = [b"config", mint.key().as_ref()],
        bump = config.bump,
        has_one = mint,
        has_one = token_program,
        constraint = stake_vault.key() == config.stake_vault @ StakingError::VaultMismatch
    )]
    pub config: Account<'info, StakingConfig>,
    #[account(
        init_if_needed,
        payer = owner,
        space = UserStakeState::INIT_SPACE,
        seeds = [b"user-state", config.key().as_ref(), owner.key().as_ref()],
        bump
    )]
    pub user_state: Account<'info, UserStakeState>,
    #[account(
        init,
        payer = owner,
        space = StakePosition::INIT_SPACE,
        seeds = [b"position", user_state.key().as_ref(), &user_state.next_position_id.to_le_bytes()],
        bump
    )]
    pub position: Account<'info, StakePosition>,
    #[account(
        mut,
        token::mint = mint,
        token::authority = owner,
        token::token_program = token_program
    )]
    pub owner_token_account: InterfaceAccount<'info, TokenAccount>,
    #[account(
        mut,
        seeds = [b"stake-vault", config.key().as_ref()],
        bump = config.stake_vault_bump
    )]
    pub stake_vault: InterfaceAccount<'info, TokenAccount>,
    pub token_program: Interface<'info, TokenInterface>,
    pub system_program: Program<'info, System>,
}

#[derive(Accounts)]
pub struct ModifyPosition<'info> {
    #[account(mut)]
    pub owner: Signer<'info>,
    #[account(
        seeds = [b"config", config.mint.as_ref()],
        bump = config.bump
    )]
    pub config: Account<'info, StakingConfig>,
    #[account(
        mut,
        seeds = [b"user-state", config.key().as_ref(), owner.key().as_ref()],
        bump
    )]
    pub user_state: Account<'info, UserStakeState>,
    #[account(
        mut,
        seeds = [b"position", user_state.key().as_ref(), &position.position_id.to_le_bytes()],
        bump,
        has_one = owner,
        has_one = config,
        has_one = user_state
    )]
    pub position: Account<'info, StakePosition>,
}

#[derive(Accounts)]
pub struct ClaimUnstake<'info> {
    #[account(mut)]
    pub owner: Signer<'info>,
    pub mint: InterfaceAccount<'info, Mint>,
    #[account(
        mut,
        seeds = [b"config", mint.key().as_ref()],
        bump = config.bump,
        has_one = mint,
        has_one = token_program,
        constraint = stake_vault.key() == config.stake_vault @ StakingError::VaultMismatch
    )]
    pub config: Account<'info, StakingConfig>,
    #[account(
        mut,
        seeds = [b"user-state", config.key().as_ref(), owner.key().as_ref()],
        bump
    )]
    pub user_state: Account<'info, UserStakeState>,
    #[account(
        mut,
        close = owner,
        seeds = [b"position", user_state.key().as_ref(), &position.position_id.to_le_bytes()],
        bump,
        has_one = owner,
        has_one = config,
        has_one = user_state
    )]
    pub position: Account<'info, StakePosition>,
    #[account(
        mut,
        token::mint = mint,
        token::authority = owner,
        token::token_program = token_program
    )]
    pub owner_token_account: InterfaceAccount<'info, TokenAccount>,
    #[account(
        mut,
        seeds = [b"stake-vault", config.key().as_ref()],
        bump = config.stake_vault_bump
    )]
    pub stake_vault: InterfaceAccount<'info, TokenAccount>,
    pub token_program: Interface<'info, TokenInterface>,
}

#[account]
pub struct StakingConfig {
    pub authority: Pubkey,
    pub mint: Pubkey,
    pub token_program: Pubkey,
    pub stake_vault: Pubkey,
    pub bump: u8,
    pub stake_vault_bump: u8,
    pub unstake_cooldown_seconds: u64,
    pub epoch_duration_seconds: u64,
    pub early_accrual_bps: u16,
    pub tier_1_age_seconds: u64,
    pub tier_1_multiplier_bps: u16,
    pub tier_2_age_seconds: u64,
    pub tier_2_multiplier_bps: u16,
    pub tier_3_age_seconds: u64,
    pub tier_3_multiplier_bps: u16,
    pub tier_4_age_seconds: u64,
    pub tier_4_multiplier_bps: u16,
    pub total_staked: u64,
    pub total_positions: u64,
}

impl StakingConfig {
    pub const INIT_SPACE: usize = 8 + 256;
}

#[account]
pub struct UserStakeState {
    pub owner: Pubkey,
    pub config: Pubkey,
    pub next_position_id: u64,
    pub total_active_staked: u64,
    pub total_positions_open: u64,
}

impl UserStakeState {
    pub const INIT_SPACE: usize = 8 + 128;
}

#[account]
pub struct StakePosition {
    pub config: Pubkey,
    pub user_state: Pubkey,
    pub owner: Pubkey,
    pub position_id: u64,
    pub amount: u64,
    pub staked_at: i64,
    pub cooldown_requested_at: i64,
    pub status: u8,
}

impl StakePosition {
    pub const INIT_SPACE: usize = 8 + 160;
}

#[repr(u8)]
pub enum PositionStatus {
    Active = 1,
    Cooldown = 2,
}

#[derive(AnchorSerialize, AnchorDeserialize, Clone)]
pub struct InitializeConfigParams {
    pub unstake_cooldown_seconds: u64,
    pub epoch_duration_seconds: u64,
    pub early_accrual_bps: u16,
    pub tier_1_age_seconds: u64,
    pub tier_1_multiplier_bps: u16,
    pub tier_2_age_seconds: u64,
    pub tier_2_multiplier_bps: u16,
    pub tier_3_age_seconds: u64,
    pub tier_3_multiplier_bps: u16,
    pub tier_4_age_seconds: u64,
    pub tier_4_multiplier_bps: u16,
}

impl InitializeConfigParams {
    pub fn validate(&self) -> Result<()> {
        validate_config_inputs(
            self.unstake_cooldown_seconds,
            self.epoch_duration_seconds,
            self.early_accrual_bps,
            self.tier_1_age_seconds,
            self.tier_1_multiplier_bps,
            self.tier_2_age_seconds,
            self.tier_2_multiplier_bps,
            self.tier_3_age_seconds,
            self.tier_3_multiplier_bps,
            self.tier_4_age_seconds,
            self.tier_4_multiplier_bps,
        )
    }
}

#[derive(AnchorSerialize, AnchorDeserialize, Clone)]
pub struct UpdateConfigParams {
    pub unstake_cooldown_seconds: u64,
    pub epoch_duration_seconds: u64,
    pub early_accrual_bps: u16,
    pub tier_1_age_seconds: u64,
    pub tier_1_multiplier_bps: u16,
    pub tier_2_age_seconds: u64,
    pub tier_2_multiplier_bps: u16,
    pub tier_3_age_seconds: u64,
    pub tier_3_multiplier_bps: u16,
    pub tier_4_age_seconds: u64,
    pub tier_4_multiplier_bps: u16,
}

impl UpdateConfigParams {
    pub fn validate(&self) -> Result<()> {
        validate_config_inputs(
            self.unstake_cooldown_seconds,
            self.epoch_duration_seconds,
            self.early_accrual_bps,
            self.tier_1_age_seconds,
            self.tier_1_multiplier_bps,
            self.tier_2_age_seconds,
            self.tier_2_multiplier_bps,
            self.tier_3_age_seconds,
            self.tier_3_multiplier_bps,
            self.tier_4_age_seconds,
            self.tier_4_multiplier_bps,
        )
    }
}

fn validate_config_inputs(
    unstake_cooldown_seconds: u64,
    epoch_duration_seconds: u64,
    early_accrual_bps: u16,
    tier_1_age_seconds: u64,
    tier_1_multiplier_bps: u16,
    tier_2_age_seconds: u64,
    tier_2_multiplier_bps: u16,
    tier_3_age_seconds: u64,
    tier_3_multiplier_bps: u16,
    tier_4_age_seconds: u64,
    tier_4_multiplier_bps: u16,
) -> Result<()> {
    require!(epoch_duration_seconds > 0, StakingError::InvalidEpochDuration);
    require!(unstake_cooldown_seconds > 0, StakingError::InvalidCooldown);
    require!(
        early_accrual_bps as u64 <= BPS_DENOMINATOR,
        StakingError::InvalidBps
    );
    require!(
        tier_1_age_seconds <= tier_2_age_seconds
            && tier_2_age_seconds <= tier_3_age_seconds
            && tier_3_age_seconds <= tier_4_age_seconds,
        StakingError::InvalidTierOrdering
    );
    require!(
        tier_1_multiplier_bps as u64 <= 50_000
            && tier_2_multiplier_bps as u64 <= 50_000
            && tier_3_multiplier_bps as u64 <= 50_000
            && tier_4_multiplier_bps as u64 <= 50_000,
        StakingError::InvalidBps
    );
    Ok(())
}

fn transfer_from_user<'info>(
    authority: &Signer<'info>,
    source: &InterfaceAccount<'info, TokenAccount>,
    destination: &InterfaceAccount<'info, TokenAccount>,
    mint: &InterfaceAccount<'info, Mint>,
    token_program: &Interface<'info, TokenInterface>,
    amount: u64,
) -> Result<()> {
    let cpi_accounts = TransferChecked {
        from: source.to_account_info(),
        mint: mint.to_account_info(),
        to: destination.to_account_info(),
        authority: authority.to_account_info(),
    };
    let cpi_ctx = CpiContext::new(token_program.to_account_info(), cpi_accounts);
    token_interface::transfer_checked(cpi_ctx, amount, mint.decimals)
}

fn transfer_from_vault<'info>(
    config: &Account<'info, StakingConfig>,
    source: &InterfaceAccount<'info, TokenAccount>,
    destination: &InterfaceAccount<'info, TokenAccount>,
    mint: &InterfaceAccount<'info, Mint>,
    token_program: &Interface<'info, TokenInterface>,
    amount: u64,
) -> Result<()> {
    let mint_key = config.mint;
    let signer_seeds: &[&[u8]] = &[b"config", mint_key.as_ref(), &[config.bump]];
    let signer_binding = [signer_seeds];
    let cpi_accounts = TransferChecked {
        from: source.to_account_info(),
        mint: mint.to_account_info(),
        to: destination.to_account_info(),
        authority: config.to_account_info(),
    };
    let cpi_ctx = CpiContext::new_with_signer(
        token_program.to_account_info(),
        cpi_accounts,
        &signer_binding,
    );
    token_interface::transfer_checked(cpi_ctx, amount, mint.decimals)
}

#[event]
pub struct ConfigInitialized {
    pub authority: Pubkey,
    pub mint: Pubkey,
    pub unstake_cooldown_seconds: u64,
    pub epoch_duration_seconds: u64,
}

#[event]
pub struct ConfigUpdated {
    pub authority: Pubkey,
    pub mint: Pubkey,
    pub unstake_cooldown_seconds: u64,
    pub epoch_duration_seconds: u64,
}

#[event]
pub struct PositionStaked {
    pub owner: Pubkey,
    pub config: Pubkey,
    pub position_id: u64,
    pub amount: u64,
    pub staked_at: i64,
}

#[event]
pub struct UnstakeRequested {
    pub owner: Pubkey,
    pub config: Pubkey,
    pub position_id: u64,
    pub amount: u64,
    pub cooldown_requested_at: i64,
}

#[event]
pub struct UnstakeCancelled {
    pub owner: Pubkey,
    pub config: Pubkey,
    pub position_id: u64,
}

#[event]
pub struct UnstakeClaimed {
    pub owner: Pubkey,
    pub config: Pubkey,
    pub position_id: u64,
    pub amount: u64,
    pub claimed_at: i64,
}

#[error_code]
pub enum StakingError {
    #[msg("Math overflow.")]
    MathOverflow,
    #[msg("The position is not in a valid state for this action.")]
    InvalidPositionState,
    #[msg("The unstake cooldown is still active.")]
    CooldownStillActive,
    #[msg("The configured basis points are invalid.")]
    InvalidBps,
    #[msg("The tier ages must be in ascending order.")]
    InvalidTierOrdering,
    #[msg("The epoch duration must be greater than zero.")]
    InvalidEpochDuration,
    #[msg("The unstake cooldown must be greater than zero.")]
    InvalidCooldown,
    #[msg("Nothing to transfer.")]
    NothingToTransfer,
    #[msg("The provided vault account does not match the config.")]
    VaultMismatch,
}
