const parameters = {
    TIER_LARGE_CAP_MIN_MARKET_CAP: 5_000_000_000, // 5 Billion USD
    TIER_MID_CAP_MIN_MARKET_CAP:   500_000_000, // 500 Million USD
    TIME_CACHE_DURATION_MS: 60000,
    CHECK_SIGNAL_INTERVAL_MS: 250, // How often to run the `checkSignal` logic for each symbol.
    PRICE_BUCKET_DURATION_MS: 100, // The time resolution for bucketing historical prices for momentum checks.
    AGG_TRADE_BUFFER_SIZE: 250, // Number of recent trades to keep in memory for each symbol.
    PRICE_LOOKBACK_WINDOW_MS: 2500, // How far back to look for the price momentum calculation (2.5 seconds).
    PRICE_SLOPE_ALPHA: 0.4, // EWMA slope smoother ~2 s
    PRICE_SLOPE_ZSCORE: 1.9,
    MIN_TRADES_IN_1S: 5, // A sanity check to ensure the volume spike is from broad participation, not one huge trade.
    MAX_BID_ASK_SPREAD_PCT: 0.003, // Max 0.3% spread to avoid high slippage and illiquid markets.
    EWMA_ALPHA_VOL_FAST: 0.1175, // T=2s, dt=0.25s (1 - exp(-0.25 / 2))
    EWMA_ALPHA_VOL_SLOW: 0.000833, // T=300s, dt=0.25s (1 - exp(-0.25 / 300))
    EWMA_ALPHA_VOL_MED: 0.00416, // Baseline time-constant 60 s  ➜  α = 1-exp(-0.25/60)
    MIN_VOLUME_SPIKE_RATIO_1M5M: 1.5,   // 1-min / 5-min
    VOLUME_ACCEL_ZSCORE: 2.0, // k·σ threshold
    ATR_PERIOD_SECONDS: 60, // The lookback period (in seconds) for calculating ATR
    ATR_ALPHA: 2 / (30 + 1), // Alpha = 1/N. For EWMA: Alpha = 2/(N+1). We use EWMA.
    ATR_ALPHA_SLOW: 2 / (300 + 1),
    SIGNAL_COOLDOWN_MS: 6000 // 6 seconds cooldown per pair after a signal to prevent rapid re-triggering.
};

export default parameters;