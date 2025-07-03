const EXCHANGE = "gate";

import { Queue } from "bullmq";
import mongo from "./mongo.js";
import params from "./parameters.js";
import CircularBuffer from "./circularBuffer.js";

// ---------------------------------------------------------------------
// Constants for the new logic – tweak in parameters.js if desired
// ---------------------------------------------------------------------
const PRICE_SLOPE_LOOKBACK_MS = 2_000;   // look‑back horizon (2 s)
const ALPHA_TAKER_RATIO       = 0.20;    // EWMA smoothing for flow ratio
const MAX_TAKER_RATIO         = 100;     // hard cap to avoid infinities

// Queues --------------------------------------------------------------
const priceQueue = new Queue(`${EXCHANGE}_price`);
const orderQueue = new Queue(`${EXCHANGE}_order`);

// Expected exec parameters (unchanged) --------------------------------
const EXPECTED_TRADE_SIZE_USD = 500;
const MIN_EXECUTION_MULTIPLIER = 5;
const MIN_24H_VOLUME_USD = 1_000_000;

class SymbolMonitor {
    constructor(symbol, marketCapTier = "mid") {
        this.symbol = symbol;
        this.exchange = EXCHANGE;
        this.marketCapTier = marketCapTier;

        /* ----------------------------------------------------------
         *   VOLUME / ACCELERATION TRACKING
         * --------------------------------------------------------*/
        this.accelHistory = new CircularBuffer(60);   // 15 s (60 × 250 ms)
        this.accelSigma   = 0;

        /* ----------------------------------------------------------
         *   PRICE MOMENTUM TRACKING
         * --------------------------------------------------------*/
        this.priceSlope     = 0;                      // pct / sec  (FIX‑1)
        this.priceSlopeHist = new CircularBuffer(40); // ~10 s
        this.priceSlopeSigma = 0;

        /* ----------------------------------------------------------
         *   TIME CACHING
         * --------------------------------------------------------*/
        this.cachedDayOfWeek = 0;
        this.cachedHourOfDay = 0;
        this.cachedIsWeekend = false;
        this.lastTimeCacheUpdate = 0;

        /* ----------------------------------------------------------
         *   TRADE / ORDER BOOK DATA
         * --------------------------------------------------------*/
        this.aggTrades        = new CircularBuffer(params.AGG_TRADE_BUFFER_SIZE);
        this.bestBid          = 0;
        this.bestAsk          = 0;
        this.bidAskMidpoint   = 0;

        /* ----------------------------------------------------------
         *   VOLUME TRACKING
         * --------------------------------------------------------*/
        this.current1sVolumeSum  = 0;
        this.current1sTradeCount = 0;
        this.ewma1sVolumeFast    = 0;
        this.ewma5mVolumeBaseline = 0;
        this.ewma1mVolumeBaseline = 0;
        this.prevEwma1s          = 0;
        this.volumeAccel         = 0;

        /* ----------------------------------------------------------
         *   TAKER‑FLOW TRACKING
         * --------------------------------------------------------*/
        this.current1sTakerBuyVolume  = 0;
        this.current1sTakerSellVolume = 0;
        this.takerFlowImbalance       = 0;
        this.takerFlowMagnitude       = 0;
        this.takerFlowRatio           = 0;
        this.takerRatioEwma           = 0;   // FIX‑2: smoothed ratio

        /* ----------------------------------------------------------
         *   PRICE TRACKING
         * --------------------------------------------------------*/
        this.lastPrice          = 0;
        this.priceBuckets       = new CircularBuffer(Math.ceil(params.PRICE_LOOKBACK_WINDOW_MS / params.PRICE_BUCKET_DURATION_MS) + 5);
        this.lastPriceBucketTime = 0;

        /* ----------------------------------------------------------
         *   VOLATILITY TRACKING (returns)
         * --------------------------------------------------------*/
        this.returnHistory         = new CircularBuffer(300); // 5 minutes of 1‑s returns
        this.lastReturnCalcTime    = 0;
        this.lastPriceForReturn    = 0;
        this.volatility30s         = 0;  // annualised
        this.volatility5m          = 0;
        this.volatilityRatio       = 1.0;

        /* ----------------------------------------------------------
         *   MICROSTRUCTURE QUALITY
         * --------------------------------------------------------*/
        this.effectiveSpreadHistory = new CircularBuffer(60);
        this.avgEffectiveSpread     = 0;
        this.tradeImbalanceHistory  = new CircularBuffer(60);
        this.avgTradeImbalance      = 0;

        /* ----------------------------------------------------------
         *   TECHNICAL INDICATORS
         * --------------------------------------------------------*/
        this.rsiPriceHistory = new CircularBuffer(20);
        this.rsiValue        = 50;
        this.avgGain = 0;
        this.avgLoss = 0;

        // EMA / PPO ---------------------------------------------------
        this.emaFast = 0;
        this.emaSlow = 0;
        this.ppoLine = 0;
        this.signalLine = 0;
        this.ppoHistogram = 0;
        this.ema9  = 0;
        this.ema21 = 0;
        this.ema50 = 0;
        this.emaAlignment9Over21 = false;
        this.emaAlignment21Over50 = false;
        this.emaStackedBullish = false;
        this.emaStackedBearish = false;
        this.emaStackedNeutral = false;
        this.ema9_21_spread   = 0;
        this.ema21_50_spread  = 0;
        this.emaAlignmentStrength = 0;
        this.priceAboveEma9  = false;

        /* ----------------------------------------------------------
         *   ORDER BOOK IMBALANCE
         * --------------------------------------------------------*/
        this.depth5ObImbalance = 0;
        this.depth5BidVolume   = 0;
        this.depth5AskVolume   = 0;
        this.depth5TotalVolume = 0;
        this.depth5VolumeRatio = 0;
        this.imbalanceHistory  = new CircularBuffer(20);
        this.imbalanceMA5      = 0;
        this.imbalanceMA20     = 0;
        this.previousImbalance = 0;
        this.imbalanceVelocity   = 0;
        this.imbalanceVolatility = 0;

        /* ----------------------------------------------------------
         *   24‑H TICKER DATA
         * --------------------------------------------------------*/
        this.ticker24hrVolumeUsdt    = 0;
        this.ticker24hrPriceChangePct = 0;
        this.ticker24hrHigh          = 0;
        this.ticker24hrLow           = 0;

        /* ----------------------------------------------------------
         *   SIGNAL CONTROL & DB refs
         * --------------------------------------------------------*/
        this.lastSignalTriggerTime = 0;
        this.db         = null;
        this.collection = null;
    }

    /* -----------------------------------------------------------------
     *                     TIME CACHE HELPERS
     * ----------------------------------------------------------------*/
    updateTimeCache(now = Date.now()) {
        if (now - this.lastTimeCacheUpdate >= params.TIME_CACHE_DURATION_MS) {
            const date = new Date(now);
            this.cachedDayOfWeek = date.getUTCDay();
            this.cachedHourOfDay = date.getUTCHours();
            this.cachedIsWeekend = [0, 6].includes(date.getUTCDay());
            this.lastTimeCacheUpdate = now;
        }
    }

    /* -----------------------------------------------------------------
     *                     VOLATILITY UPDATE
     * ----------------------------------------------------------------*/
    updateVolatility(currentPrice, now) {
        if (this.lastPriceForReturn > 0 && now - this.lastReturnCalcTime >= 1000) {
            const logReturn = Math.log(currentPrice / this.lastPriceForReturn);
            this.returnHistory.add({ time: now, return: logReturn });

            const allReturns = this.returnHistory.toArray();
            const cutoff30s = now - 30000;
            const cutoff5m  = now - 300000;

            const returns30s = allReturns.filter(r => r.time > cutoff30s).map(r => r.return);
            const returns5m  = allReturns.filter(r => r.time > cutoff5m ).map(r => r.return);

            if (returns30s.length >= 10) {
                this.volatility30s = this.calculateAnnualizedVolatility(returns30s);
            }
            if (returns5m.length >= 30) {
                this.volatility5m = this.calculateAnnualizedVolatility(returns5m);
                this.volatilityRatio = this.volatility5m > 0 ? this.volatility30s / this.volatility5m : 1.0;
            }

            this.lastPriceForReturn = currentPrice;
            this.lastReturnCalcTime = now;
        } else if (this.lastPriceForReturn === 0) {
            this.lastPriceForReturn = currentPrice;
            this.lastReturnCalcTime = now;
        }
    }

    calculateAnnualizedVolatility(returns) {
        if (returns.length < 2) return 0;
        const mean = returns.reduce((a, b) => a + b, 0) / returns.length;
        const variance = returns.reduce((s, r) => s + (r - mean) ** 2, 0) / (returns.length - 1);
        return Math.sqrt(variance) * Math.sqrt(365 * 24 * 60 * 60);
    }

    /* -----------------------------------------------------------------
     *                 DYNAMIC VOLUME THRESHOLD (unchanged)
     * ----------------------------------------------------------------*/
    getDynamicVolumeThreshold(timeContext = {}) {
        const baseMultiplier = 4.0;
        if (this.lastPrice === 0 || this.volatility5m === 0) return baseMultiplier;

        const normalizedVol = this.volatility30s / Math.sqrt(365 * 24 * 60 * 60);
        let regimeModifier = 1.0;
        if (this.volatilityRatio > 1.5) regimeModifier = 1.25;
        else if (this.volatilityRatio < 0.8) regimeModifier = 0.75;

        const volatilityFactor = 1 + (normalizedVol * 50 * regimeModifier);

        const { hourOfDay = this.cachedHourOfDay, isWeekend = this.cachedIsWeekend } = timeContext;
        let sessionFactor = 1.0;
        if (isWeekend) sessionFactor = 0.8;
        else if (hourOfDay >= 13 && hourOfDay <= 17) sessionFactor = 1.5;
        else if (hourOfDay >= 0 && hourOfDay < 7) sessionFactor = 0.75;

        const dynamicThreshold = baseMultiplier * volatilityFactor * sessionFactor;
        return Math.max(2.5, Math.min(20.0, dynamicThreshold));
    }

    /* -----------------------------------------------------------------
     *                    EMA ALIGNMENT / MACD / RSI
     * ----------------------------------------------------------------*/
    updateEMAAlignment(currentPrice) {
        const alpha9 = 2 / (9 + 1);
        const alpha21 = 2 / (21 + 1);
        const alpha50 = 2 / (50 + 1);

        if (this.ema9 === 0) this.ema9 = currentPrice;
        if (this.ema21 === 0) this.ema21 = currentPrice;
        if (this.ema50 === 0) this.ema50 = currentPrice;

        this.ema9  = alpha9  * currentPrice + (1 - alpha9)  * this.ema9;
        this.ema21 = alpha21 * currentPrice + (1 - alpha21) * this.ema21;
        this.ema50 = alpha50 * currentPrice + (1 - alpha50) * this.ema50;

        this.emaAlignment9Over21 = this.ema9 > this.ema21;
        this.emaAlignment21Over50 = this.ema21 > this.ema50;
        this.emaStackedBullish = this.emaAlignment9Over21 && this.emaAlignment21Over50;
        this.emaStackedBearish = !this.emaAlignment9Over21 && !this.emaAlignment21Over50;
        this.emaStackedNeutral = !(this.emaStackedBullish || this.emaStackedBearish);
        this.ema9_21_spread  = (this.ema9 - this.ema21) / currentPrice;
        this.ema21_50_spread = (this.ema21 - this.ema50) / currentPrice;
        this.emaAlignmentStrength = this.ema9_21_spread + this.ema21_50_spread;
        this.priceAboveEma9 = currentPrice > this.ema9;
    }

    updateMACD(currentPrice) {
        const fastPeriod = 3;
        const slowPeriod = 10;
        const signalPeriod = 16;

        const alphaFast = 2 / (fastPeriod + 1);
        const alphaSlow = 2 / (slowPeriod + 1);
        const alphaSignal = 2 / (signalPeriod + 1);

        if (this.emaFast === 0) this.emaFast = currentPrice;
        if (this.emaSlow === 0) this.emaSlow = currentPrice;

        this.emaFast = alphaFast * currentPrice + (1 - alphaFast) * this.emaFast;
        this.emaSlow = alphaSlow * currentPrice + (1 - alphaSlow) * this.emaSlow;

        this.ppoLine = this.emaSlow !== 0 ? ((this.emaFast - this.emaSlow) / this.emaSlow) * 100 : 0;
        if (this.signalLine === 0 && this.ppoLine !== 0) this.signalLine = this.ppoLine;
        this.signalLine = alphaSignal * this.ppoLine + (1 - alphaSignal) * this.signalLine;
        this.ppoHistogram = this.ppoLine - this.signalLine;
    }

    updateRSI(currentPrice) {
        this.rsiPriceHistory.add(currentPrice);
        const period = 9;
        if (this.rsiPriceHistory.size < 2) return;

        const prices = this.rsiPriceHistory.toArray();
        const priceChange = currentPrice - prices[prices.length - 2];
        const gain = Math.max(0, priceChange);
        const loss = Math.max(0, -priceChange);

        if (this.rsiPriceHistory.size === period + 1) {
            let totalGains = 0, totalLosses = 0;
            for (let i = 1; i < prices.length; i++) {
                const delta = prices[i] - prices[i - 1];
                totalGains += Math.max(0, delta);
                totalLosses += Math.max(0, -delta);
            }
            this.avgGain = totalGains / period;
            this.avgLoss = totalLosses / period;
        } else if (this.rsiPriceHistory.size > period + 1) {
            this.avgGain = ((period - 1) * this.avgGain + gain) / period;
            this.avgLoss = ((period - 1) * this.avgLoss + loss) / period;
        } else return;

        if (this.avgLoss === 0 && this.avgGain === 0)      this.rsiValue = 50;
        else if (this.avgLoss === 0)                        this.rsiValue = 100;
        else {
            const rs = this.avgGain / this.avgLoss;
            this.rsiValue = 100 - 100 / (1 + rs);
        }
        this.rsiValue = Math.max(0, Math.min(100, this.rsiValue));
    }

    /* -----------------------------------------------------------------
     *                    ORDER BOOK SNAPSHOT / IMBALANCE
     * ----------------------------------------------------------------*/
    updateDepthSnapshot(data) {
        if (!data.bids?.length || !data.asks?.length) return;
        let bidSum = 0, askSum = 0;
        for (let i = 0; i < Math.min(5, data.bids.length); i++) bidSum += Number(data.bids[i][1]);
        for (let i = 0; i < Math.min(5, data.asks.length); i++) askSum += Number(data.asks[i][1]);

        const imbalance = (bidSum - askSum) / (bidSum + askSum + 1e-8);
        this.depth5ObImbalance = imbalance;
        this.depth5BidVolume   = bidSum;
        this.depth5AskVolume   = askSum;
        this.depth5TotalVolume = bidSum + askSum;
        this.depth5VolumeRatio = bidSum / (askSum + 1e-8);
        this.updateImbalanceFeatures(imbalance);
    }

    updateImbalanceFeatures(currentImbalance) {
        this.imbalanceHistory.add(currentImbalance);
        this.imbalanceVelocity = currentImbalance - this.previousImbalance;
        this.previousImbalance = currentImbalance;

        const arr = this.imbalanceHistory.toArray();
        if (arr.length >= 5) this.imbalanceMA5  = arr.slice(-5).reduce((a, b) => a + b, 0) / 5;
        if (arr.length >= 20) this.imbalanceMA20 = arr.reduce((a, b) => a + b, 0) / arr.length;
        this.imbalanceVolatility = this.calculateImbalanceVolatility();
    }

    calculateImbalanceVolatility() {
        const windowSize = 10;
        if (this.imbalanceHistory.size < windowSize) return 0;
        const vals = this.imbalanceHistory.toArray().slice(-windowSize);
        const mean = vals.reduce((a, b) => a + b, 0) / vals.length;
        const varr = vals.reduce((s, x) => s + (x - mean) ** 2, 0) / vals.length;
        return Math.sqrt(varr);
    }

    /* -----------------------------------------------------------------
     *                       BOOK TICKER UPDATE
     * ----------------------------------------------------------------*/
    applyBookTickerUpdate(data) {
        const bid = parseFloat(data.b), ask = parseFloat(data.a);
        if (Number.isFinite(bid) && bid > 0) this.bestBid = bid;
        if (Number.isFinite(ask) && ask > 0) this.bestAsk = ask;
        if (this.bestBid > 0 && this.bestAsk > 0) this.bidAskMidpoint = (this.bestBid + this.bestAsk) / 2;
    }

    /* -----------------------------------------------------------------
     *                          AGG TRADE ADD
     * ----------------------------------------------------------------*/
    addAggTrade(tradeData) {
        const trade = {
            price: parseFloat(tradeData.p),
            quantity: parseFloat(tradeData.q),
            eventTime: parseInt(tradeData.E, 10),
            isBuyerMaker: tradeData.m,
        };
        this.aggTrades.add(trade);
        this.lastPrice = trade.price;

        if (this.bidAskMidpoint > 0) {
            const effSpreadBps = Math.abs(trade.price - this.bidAskMidpoint) / this.bidAskMidpoint * 1e4;
            this.effectiveSpreadHistory.add(effSpreadBps);
            const spreads = this.effectiveSpreadHistory.toArray();
            this.avgEffectiveSpread = spreads.reduce((a, b) => a + b, 0) / spreads.length;
        }

        const imb = trade.isBuyerMaker ? -trade.quantity : trade.quantity;
        this.tradeImbalanceHistory.add(imb);
    }

    applyTickerUpdate(ticker) {
        this.ticker24hrVolumeUsdt    = parseFloat(ticker.q);
        this.ticker24hrPriceChangePct = parseFloat(ticker.P);
        this.ticker24hrHigh          = parseFloat(ticker.h);
        this.ticker24hrLow           = parseFloat(ticker.l);
        this.tickerLastPrice         = parseFloat(ticker.c);
    }

    getHistoricalPrice(targetTimeMs) {
        for (let i = this.priceBuckets.size - 1; i >= 0; i--) {
            const bucket = this.priceBuckets.get(i);
            if (bucket && bucket.time <= targetTimeMs) return bucket.price;
        }
        return null;
    }

    /* -----------------------------------------------------------------
     *                 PERIODIC CALCULATIONS  (RUN EVERY 250 ms)
     * ----------------------------------------------------------------*/
    performPeriodicCalculations() {
        const now = Date.now();
        const oneSecAgo = now - 1000;

        // Update volatility using lastPrice
        if (this.lastPrice > 0) this.updateVolatility(this.lastPrice, now);

        /* -- 1‑SECOND VOLUME ---------------------------------------*/
        let vol1s = 0, trades1s = 0, buyUSDT = 0, sellUSDT = 0;
        const trades = this.aggTrades.toArray();
        for (let i = trades.length - 1; i >= 0; i--) {
            const t = trades[i];
            if (t.eventTime < oneSecAgo) break;
            const notional = t.price * t.quantity;
            vol1s   += notional;
            trades1s++;
            if (!t.isBuyerMaker) buyUSDT += notional; else sellUSDT += notional;
        }
        this.current1sVolumeSum   = vol1s;
        this.current1sTradeCount  = trades1s;
        this.current1sTakerBuyVolume  = buyUSDT;
        this.current1sTakerSellVolume = sellUSDT;

        /* -- EWMA VOLUME & ACCEL ----------------------------------*/
        if (this.ewma5mVolumeBaseline === 0 && vol1s > 0) this.ewma5mVolumeBaseline = vol1s;
        this.ewma1sVolumeFast = params.EWMA_ALPHA_VOL_FAST * vol1s + (1 - params.EWMA_ALPHA_VOL_FAST) * this.ewma1sVolumeFast;
        this.ewma5mVolumeBaseline = params.EWMA_ALPHA_VOL_SLOW * vol1s + (1 - params.EWMA_ALPHA_VOL_SLOW) * this.ewma5mVolumeBaseline;
        this.ewma1mVolumeBaseline = params.EWMA_ALPHA_VOL_MED * vol1s + (1 - params.EWMA_ALPHA_VOL_MED) * this.ewma1mVolumeBaseline;

        this.volumeAccel = this.ewma1sVolumeFast - this.prevEwma1s;
        this.prevEwma1s  = this.ewma1sVolumeFast;

        /* -- PRICE BUCKET (lastPrice may be 0 early on) -------------*/
        if (this.lastPrice > 0) {
            const bucketFloor = Math.floor(now / params.PRICE_BUCKET_DURATION_MS) * params.PRICE_BUCKET_DURATION_MS;
            if (this.lastPriceBucketTime === 0 || bucketFloor > this.lastPriceBucketTime) {
                this.priceBuckets.add({ time: bucketFloor, price: this.lastPrice });
                this.lastPriceBucketTime = bucketFloor;
            } else {
                const last = this.priceBuckets.getNewest();
                if (last && last.time === bucketFloor) last.price = this.lastPrice;
            }
        }

        /* -- EMA / RSI / MACD -------------------------------------*/
        this.updateEMAAlignment(this.lastPrice);
        this.updateRSI(this.lastPrice);
        this.updateMACD(this.lastPrice);

        /* -- TAKER FLOW METRICS  (FIX‑2) ---------------------------*/
        const rawRatio = sellUSDT > 0 ? buyUSDT / sellUSDT : (buyUSDT > 0 ? MAX_TAKER_RATIO : 0);
        const clippedRatio = Math.min(rawRatio, MAX_TAKER_RATIO);
        this.takerRatioEwma = ALPHA_TAKER_RATIO * clippedRatio + (1 - ALPHA_TAKER_RATIO) * this.takerRatioEwma; // EWMA smooth

        this.takerFlowImbalance = (buyUSDT - sellUSDT) / (buyUSDT + sellUSDT + 1e-8);
        this.takerFlowMagnitude = buyUSDT + sellUSDT;
        this.takerFlowRatio     = clippedRatio; // still export raw ratio (clipped)

        /* -- VOLUME ACCEL SIGMA -----------------------------------*/
        this.accelHistory.add(this.volumeAccel);
        if (this.accelHistory.size >= 20) {
            const arr = this.accelHistory.toArray();
            const mean = arr.reduce((s, x) => s + x, 0) / arr.length;
            const varr = arr.reduce((s, x) => s + (x - mean) ** 2, 0) / arr.length;
            this.accelSigma = Math.sqrt(varr);
        }

        /* -- PRICE SLOPE  (FIX‑1) ---------------------------------*/
        if (this.lastPrice > 0) {
            const priceT = this.getHistoricalPrice(now - PRICE_SLOPE_LOOKBACK_MS) ?? this.lastPrice;
            if (priceT > 0) {
                const pctChange = (this.lastPrice - priceT) / priceT;
                const slopePerS = pctChange / (PRICE_SLOPE_LOOKBACK_MS / 1000);
                this.priceSlope = params.PRICE_SLOPE_ALPHA * slopePerS + (1 - params.PRICE_SLOPE_ALPHA) * this.priceSlope;
            }
        }
        this.priceSlopeHist.add(this.priceSlope);
        if (this.priceSlopeHist.size >= 20) {
            const v = this.priceSlopeHist.toArray();
            const m = v.reduce((s, x) => s + x, 0) / v.length;
            const varr = v.reduce((s, x) => s + (x - m) ** 2, 0) / v.length;
            this.priceSlopeSigma = Math.sqrt(varr);
        }
    }

    /* -----------------------------------------------------------------
     *                 LIQUIDITY & VOLUME FLOOR HELPERS
     * ----------------------------------------------------------------*/
    getAbsoluteVolumeFloor() {
        const secInDay = 86_400;
        const quarterShare = this.ticker24hrVolumeUsdt / secInDay * 0.25;
        const tierFloorMap = { mega: 1000, large: 600, mid: 500, small: 400, micro: 300 };
        return Math.max(tierFloorMap[this.marketCapTier] ?? 400, quarterShare);
    }

    hasSufficientLiquidity() {
        if (this.bidAskMidpoint <= 0) return false;
        const notionalBid = this.depth5BidVolume * this.bidAskMidpoint;
        const notionalAsk = this.depth5AskVolume * this.bidAskMidpoint;
        const weakestDepth = Math.min(notionalBid, notionalAsk);
        const minDepthNeeded = EXPECTED_TRADE_SIZE_USD * MIN_EXECUTION_MULTIPLIER;
        if (weakestDepth < minDepthNeeded) return false;
        if (this.current1sVolumeSum < EXPECTED_TRADE_SIZE_USD) return false;
        return true;
    }

    /* -----------------------------------------------------------------
     *                               SIGNAL
     * ----------------------------------------------------------------*/
    async checkSignal() {
        const now = Date.now();
        this.updateTimeCache(now);

        if (this.lastPrice === 0 || this.ewma5mVolumeBaseline === 0) return null;
        if (this.returnHistory.size < 30 || this.volatility30s === 0) return null;
        if (this.ticker24hrVolumeUsdt < MIN_24H_VOLUME_USD) return null;
        if (!this.hasSufficientLiquidity()) return null;
        if (now - this.lastSignalTriggerTime < params.SIGNAL_COOLDOWN_MS) return null;

        const maxVolByTier = { mega: 0.50, large: 0.80, mid: 1.20, small: 2.00, micro: 3.00 };
        if (this.volatility5m > (maxVolByTier[this.marketCapTier] ?? 1.50) || this.volatility5m < 0.05) return null;

        if (!Number.isFinite(this.bestBid) || !Number.isFinite(this.bestAsk) || this.bestBid <= 0 || this.bestAsk <= this.bestBid) return null;

        const spreadPct = (this.bestAsk - this.bestBid) / this.bestAsk;
        const spreadBps = spreadPct * 1e4;
        if (spreadPct > params.MAX_BID_ASK_SPREAD_PCT) return null;
        const instantVol = this.volatility30s / Math.sqrt(365 * 24 * 60 * 60);
        const normalizedSpread = spreadPct / (instantVol + 1e-4);
        if (normalizedSpread > 3.0) return null;

        /* -------- volume spike detection -------------------------*/
        const ratioFast1m = this.ewma1mVolumeBaseline > 0 ? this.ewma1sVolumeFast / this.ewma1mVolumeBaseline : 0;
        const ratio1m5m   = this.ewma5mVolumeBaseline > 0 ? this.ewma1mVolumeBaseline / this.ewma5mVolumeBaseline : 0;
        const accelZ      = this.accelSigma > 0 ? this.volumeAccel / this.accelSigma : 0;
        const dynThresh   = this.getDynamicVolumeThreshold();

        const isVolumeSpike = ratioFast1m >= dynThresh && ratio1m5m >= params.MIN_VOLUME_SPIKE_RATIO_1M5M && accelZ >= params.VOLUME_ACCEL_ZSCORE &&
                               this.current1sVolumeSum >= this.getAbsoluteVolumeFloor() && this.current1sTradeCount >= params.MIN_TRADES_IN_1S;
        if (!isVolumeSpike) return null;

        /* -------- price momentum check ---------------------------*/
        const priceLookback = this.getHistoricalPrice(now - params.PRICE_LOOKBACK_WINDOW_MS);
        if (priceLookback === null || this.lastPrice <= priceLookback) return null;

        const priceChangePct = (this.lastPrice - priceLookback) / priceLookback;
        const slopeZ = this.priceSlopeSigma > 0 ? this.priceSlope / this.priceSlopeSigma : 0;
        if (slopeZ < params.PRICE_SLOPE_ZSCORE) return null;

        const priceZScore = instantVol > 0 ? priceChangePct / instantVol : 0;
        if (priceZScore < 1.5) return null;

        /* -------- all checks passed – trigger --------------------*/
        this.lastSignalTriggerTime = now;

        // FIX‑2: use smoothed & clipped ratio
        const takerRatioInstant = this.takerRatioEwma;

        console.log(new Date(), `[SIGNAL] ${this.symbol} | Px: ${this.lastPrice.toFixed(4)} | Vol30s: ${(this.volatility30s * 100).toFixed(1)}% | VolRatio: ${this.volatilityRatio.toFixed(2)} | TakerR: ${takerRatioInstant.toFixed(2)} | Spread: ${spreadBps.toFixed(1)}bps`);

        const vector = {
            exchange: this.exchange,
            createdAt: new Date(now),
            symbol: this.symbol.replace(/[^A-Za-z0-9]/g, "").toUpperCase(),
            signalTimestampMs: now,
            triggerPrice: this.lastPrice,

            // price
            priceChangePct,
            priceSlope: this.priceSlope,               // pct / sec  (FIX‑1)
            slopeZ,
            priceZScore,

            // volume
            volumeRatioFast1m: ratioFast1m,
            volumeRatio1m5m: ratio1m5m,
            volumeAccelZ: accelZ,
            current1sVolumeUsdt: this.current1sVolumeSum,
            volumePerDollar: this.current1sVolumeSum / (this.ticker24hrVolumeUsdt + 1),
            dynVolumeThresh: dynThresh,

            // volatility
            volatility30s: this.volatility30s,
            volatility5m: this.volatility5m,
            volatilityRatio: this.volatilityRatio,

            // microstructure
            spreadPct,
            spreadBps,
            normalizedSpread,
            effectiveSpreadBps: this.avgEffectiveSpread,

            // order book
            depth5ObImbalance: this.depth5ObImbalance,
            depth5BidVolume: this.depth5BidVolume,
            depth5AskVolume: this.depth5AskVolume,
            depth5TotalVolume: this.depth5TotalVolume,
            depth5VolumeRatio: this.depth5VolumeRatio,
            imbalanceMA5: this.imbalanceMA5,
            imbalanceMA20: this.imbalanceMA20,
            imbalanceVelocity: this.imbalanceVelocity,
            imbalanceVolatility: this.imbalanceVolatility,

            // taker flow
            takerRatioSmoothed: takerRatioInstant,     // FIX‑2: bounded & EWMA
            takerBuyVolumeAbs: this.current1sTakerBuyVolume,
            takerFlowImbalance: this.takerFlowImbalance,
            takerFlowMagnitude: this.takerFlowMagnitude,
            takerFlowRatio: this.takerFlowRatio,

            // technical
            ppoHistogram: this.ppoHistogram,
            ppoLine: this.ppoLine,
            signalLine: this.signalLine,
            rsi9: this.rsiValue,
            ema9Over21: this.emaAlignment9Over21,
            ema21Over50: this.emaAlignment21Over50,
            emaAlignmentStrength: this.emaAlignmentStrength,
            emaStackedBullish: this.emaStackedBullish,
            emaStackedBearish: this.emaStackedBearish,
            emaStackedNeutral: this.emaStackedNeutral,
            priceAboveEma9: this.priceAboveEma9,

            // market data
            ticker24hrVolumeUsdt: this.ticker24hrVolumeUsdt,
            ticker24hrPriceChangePct: this.ticker24hrPriceChangePct,
            ticker24hrHigh: this.ticker24hrHigh,
            ticker24hrLow: this.ticker24hrLow,

            // time
            hourOfDay: this.cachedHourOfDay,
            dayOfWeek: this.cachedDayOfWeek,
            isWeekend: this.cachedIsWeekend,

            // deprecated but kept for compatibility
            realisedVolFast: this.volatility30s,
            realisedVolMedium: this.volatility5m,
            explosiveRatio: this.volatilityRatio,
            volatilityExpansionRatio: this.volatilityRatio,
        };

        // insert to DB -------------------------------------------------
        const insert = await mongo.signals.insertOne(vector);

        // queue -------------------------------------------------------
        await priceQueue.add(`${EXCHANGE}_price`, { id: insert.insertedId.toString(), symbol: this.symbol, timestamp: vector.signalTimestampMs }, { removeOnComplete: true, removeOnFail: true, delay: 31 * 60 * 1000 });

        for (const tOffset of [3, 10, 30]) {
            await orderQueue.add(`${EXCHANGE}_orderbook`, { id: insert.insertedId.toString(), symbol: this.symbol, tOffset }, { removeOnComplete: true, removeOnFail: true, delay: tOffset * 1000 });
        }

        return vector;
    }
}

export default SymbolMonitor;