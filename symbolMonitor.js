import { Queue } from "bullmq";
import mongo from "./mongo.js";
import params from "./parameters.js";
import CircularBuffer from "./circularBuffer.js";

const priceQueue = new Queue("gate_price");
const orderQueue = new Queue("gate_order");

class SymbolMonitor {
    constructor(symbol, marketCapTier = "mid") {
        this.symbol = symbol;

        this.accelHistory = new CircularBuffer(60);   // last 15 s (60×250 ms)
        this.accelSigma = 0;

        this.priceSlope = 0;
        this.priceSlopeHist = new CircularBuffer(40);  // ~10 s
        this.priceSlopeSigma = 0;

        this.marketCapTier = marketCapTier;

        this.cachedDayOfWeek = 0;
        this.cachedHourOfDay = 0;
        this.cachedIsWeekend = false;
        this.lastTimeCacheUpdate = 0;

        this.aggTrades = new CircularBuffer(params.AGG_TRADE_BUFFER_SIZE);
        this.bestBid = 0;
        this.bestAsk = 0;

        this.current1sVolumeSum = 0;
        this.current1sTradeCount = 0;

        this.ewma1sVolumeFast = 0;
        this.ewma5mVolumeBaseline = 0;
        this.ewma1mVolumeBaseline = 0;
        this.prevEwma1s = 0;
        this.volumeAccel = 0;

        this.current1sTakerBuyVolume = 0;
        this.current1sTakerSellVolume = 0;

        this.lastPrice = 0;
        this.priceBuckets = new CircularBuffer(Math.ceil(params.PRICE_LOOKBACK_WINDOW_MS / params.PRICE_BUCKET_DURATION_MS) + 5);
        this.lastPriceBucketTime = 0;

        this.priceHistoryForATR = new CircularBuffer(params.ATR_PERIOD_SECONDS + 5);
        this.lastAtrCalculationTime = 0;

        this.rsiPriceHistory = new CircularBuffer(20);
        this.rsiValue = 50;

        this.takerFlowImbalance = 0;
        this.takerFlowMagnitude = 0;
        this.takerFlowRatio = 0;

        this.depth5ObImbalance = 0;
        this.depth5BidVolume = 0;
        this.depth5AskVolume = 0;
        this.depth5TotalVolume = 0;
        this.depth5VolumeRatio = 0;

        this.atrFast = 0;
        this.atrSlow = 0;

        this.ticker24hrVolumeUsdt = 0;
        this.ticker24hrPriceChangePct = 0;
        this.ticker24hrHigh = 0;
        this.ticker24hrLow = 0;

        this.emaFast = 0;
        this.emaSlow = 0;
        this.ppoLine = 0;
        this.signalLine = 0;
        this.ppoHistogram = 0;

        this.ema9 = 0;
        this.ema21 = 0;
        this.ema50 = 0;

        this.emaAlignment9Over21 = false;
        this.emaAlignment21Over50 = false;
        this.emaStackedBullish = false;
        this.emaStackedBearish = false;
        this.emaStackedNeutral = false;
        this.ema9_21_spread = 0;
        this.ema21_50_spread = 0;
        this.emaAlignmentStrength = 0;
        this.priceAboveEma9 = false;
        this.priceAboveEma21 = false;
        this.priceAboveEma50 = false;

        this.imbalanceHistory = new CircularBuffer(20);
        this.imbalanceMA5 = 0;
        this.imbalanceMA20 = 0;
        this.previousImbalance = 0;
        this.imbalanceVelocity = 0;
        this.imbalanceVolatility = 0;

        this.lastSignalTriggerTime = 0;

        this.db = null;
        this.collection = null;
    }

    updateTimeCache(now = Date.now()) {
        if (now - this.lastTimeCacheUpdate >= params.TIME_CACHE_DURATION_MS) {
            const date = new Date(now);

            this.cachedDayOfWeek = date.getUTCDay();
            this.cachedHourOfDay = date.getUTCHours();
            this.cachedIsWeekend = [0, 6].includes(date.getUTCDay());
            this.lastTimeCacheUpdate = now;
        }
    }

    getATRVolatilityScaling() {
        // Research formula: adjust threshold based on current vs historical ATR
        if (this.atrSlow === 0 || this.atrFast === 0) {
            return 1.0; // No scaling if ATR not ready
        }

        const atrRatio = this.atrFast / this.atrSlow;
        const atrSensitivity = 0.6; // Research suggests 0.6-0.8

        if (atrRatio > 1.5) {
            // High volatility: increase threshold by up to 40%
            return 1.0 + (atrSensitivity * (atrRatio - 1.5));
        } else if (atrRatio < 0.8) {
            // Low volatility: decrease threshold by up to 20%
            return Math.max(0.8, 1.0 - (atrSensitivity * (0.8 - atrRatio) * 0.5));
        } else {
            // Medium volatility: minor adjustments around baseline
            return 1.0 + (atrSensitivity * (atrRatio - 1.0) * 0.3);
        }
    }

    getDynamicVolumeThreshold(timeContext = {}) {
        const baseMultiplier = 4.0; // The baseline volume ratio we are looking for.

        if (this.lastPrice === 0 || this.atrSlow === 0) {
            return baseMultiplier; // Not enough data, return baseline.
        }

        // --- 1. Unified Volatility Factor ---
        // Start with the normalized absolute volatility.
        // This asks: "How volatile is this asset right now, as a percentage?"
        const normalizedAtr = this.atrFast / this.lastPrice;

        // Now, determine the volatility regime based on the fast/slow ATR ratio.
        // This asks: "Is the market heating up or cooling down?"
        const atrRatio = this.atrFast / this.atrSlow;
        let regimeModifier;

        // Research-backed regime definitions:
        if (atrRatio > 1.5) {
            // High Volatility Expansion: Be more aggressive with the adjustment.
            regimeModifier = 1.25;
        } else if (atrRatio < 0.8) {
            // Volatility Contraction: Dampen the adjustment. The asset is quieting down.
            regimeModifier = 0.75;
        } else {
            // Normal/Trending Volatility: Use a baseline modifier.
            regimeModifier = 1.0;
        }

        // The unified factor: base volatility, scaled by the current regime.
        // We add 1 so it's a multiplier starting from 1.0x.
        // The 50 is a sensitivity constant; e.g., a 2% ATR (0.02) becomes 0.02 * 50 = 1.
        const unifiedVolatilityFactor = 1 + (normalizedAtr * 50 * regimeModifier);

        // --- 2. Session Factor (using the corrected logic from below) ---
        const { hourOfDay = this.cachedHourOfDay, isWeekend = this.cachedIsWeekend } = timeContext;

        let sessionFactor = 1.0;

        if (isWeekend) {
            // Weekends have their own distinct, often lower, volume profile.
            sessionFactor = 0.8; // e.g., require less volume on weekends
        } else {
            // Weekday session logic
            if (hourOfDay >= 13 && hourOfDay <= 17) {
                sessionFactor = 1.5; // EU-US peak overlap
            } else if (hourOfDay >= 0 && hourOfDay < 7) {
                sessionFactor = 0.75; // Asian low-liquidity period
            }
        }

        // --- 3. Final Calculation ---
        const dynamicThreshold = baseMultiplier * unifiedVolatilityFactor * sessionFactor;

        // Apply sane caps to prevent extreme values.
        return Math.max(2.5, Math.min(20.0, dynamicThreshold));
    }

    updateEMAAlignment(currentPrice) {
        const alpha9 = 2 / (9 + 1);
        const alpha21 = 2 / (21 + 1);
        const alpha50 = 2 / (50 + 1);

        if (this.ema9 === 0) this.ema9 = currentPrice;
        if (this.ema21 === 0) this.ema21 = currentPrice;
        if (this.ema50 === 0) this.ema50 = currentPrice;

        this.ema9 = alpha9 * currentPrice + (1 - alpha9) * this.ema9;
        this.ema21 = alpha21 * currentPrice + (1 - alpha21) * this.ema21;
        this.ema50 = alpha50 * currentPrice + (1 - alpha50) * this.ema50;

        this.emaAlignment9Over21 = this.ema9 > this.ema21;
        this.emaAlignment21Over50 = this.ema21 > this.ema50;
        this.emaStackedBullish = this.emaAlignment9Over21 && this.emaAlignment21Over50;
        this.emaStackedBearish = !this.emaAlignment9Over21 && !this.emaAlignment21Over50;
        this.emaStackedNeutral = !(this.emaStackedBullish || this.emaStackedBearish);
        this.ema9_21_spread = (this.ema9 - this.ema21) / currentPrice;
        this.ema21_50_spread = (this.ema21 - this.ema50) / currentPrice;
        this.emaAlignmentStrength = this.ema9_21_spread + this.ema21_50_spread;
        this.priceAboveEma9 = currentPrice > this.ema9;
        this.priceAboveEma21 = currentPrice > this.ema21;
        this.priceAboveEma50 = currentPrice > this.ema50;
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

        this.emaFast = (alphaFast * currentPrice) + ((1 - alphaFast) * this.emaFast);
        this.emaSlow = (alphaSlow * currentPrice) + ((1 - alphaSlow) * this.emaSlow);

        if (this.emaSlow !== 0) {
            this.ppoLine = ((this.emaFast - this.emaSlow) / this.emaSlow) * 100;
        } else {
            this.ppoLine = 0;
        }

        if (this.signalLine === 0 && this.ppoLine !== 0) {
             this.signalLine = this.ppoLine;
        }

        this.signalLine = (alphaSignal * this.ppoLine) + ((1 - alphaSignal) * this.signalLine);
        this.ppoHistogram = this.ppoLine - this.signalLine;
    }

    updateRSI(currentPrice) {
        this.rsiPriceHistory.add(currentPrice);

        const period = 9;

        if (this.rsiPriceHistory.size < 2) {
            return;
        }

        const prices = this.rsiPriceHistory.toArray();
        const priceChange = currentPrice - prices[prices.length - 2];
        const gain = Math.max(0, priceChange);
        const loss = Math.max(0, -priceChange);

        if (this.rsiPriceHistory.size === period + 1) {
            let totalGains = 0;
            let totalLosses = 0;

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
        } else {
            return;
        }

        if (this.avgLoss === 0 && this.avgGain === 0) {
            this.rsiValue = 50;
        } else if (this.avgLoss === 0) {
            this.rsiValue = 100;
        } else {
            const rs = this.avgGain / this.avgLoss;
            this.rsiValue = 100 - (100 / (1 + rs));
        }

        this.rsiValue = Math.max(0, Math.min(100, this.rsiValue));
    }

    updateDepthSnapshot(data) {
        if (!data.bids?.length || !data.asks?.length) {
            return;
        }

        let bidSum = 0;
        let askSum = 0;

        for (let i = 0; i < Math.min(5, data.bids.length); i++) {
            bidSum += Number(data.bids[i][1]);
        }

        for (let i = 0; i < Math.min(5, data.asks.length); i++) {
            askSum += Number(data.asks[i][1]);
        }

        const imbalance = (bidSum - askSum) / (bidSum + askSum + 1e-8);

        this.depth5ObImbalance = imbalance;
        this.depth5BidVolume = bidSum;
        this.depth5AskVolume = askSum;
        this.depth5TotalVolume = bidSum + askSum;
        this.depth5VolumeRatio = bidSum / (askSum + 1e-8);

        this.updateImbalanceFeatures(imbalance);
    }

    updateImbalanceFeatures(currentImbalance) {
        this.imbalanceHistory.add(currentImbalance);

        this.imbalanceVelocity = currentImbalance - this.previousImbalance;
        this.previousImbalance = currentImbalance;

        const historyArray = this.imbalanceHistory.toArray();
        const size = historyArray.length;

        if (size >= 5) {
            this.imbalanceMA5 = historyArray.slice(-5).reduce((a, b) => a + b, 0) / 5;
        }

        if (size >= 20) {
            this.imbalanceMA20 = historyArray.reduce((a, b) => a + b, 0) / size;
        }

        this.imbalanceVolatility = this.calculateImbalanceVolatility();
    }

    calculateImbalanceVolatility() {
        const windowSize = 10;
        if (this.imbalanceHistory.size < windowSize) return 0;

        const values = this.imbalanceHistory.toArray().slice(-windowSize);
        const mean = values.reduce((a, b) => a + b, 0) / values.length;
        const variance = values.reduce((sum, val) => sum + Math.pow(val - mean, 2), 0) / values.length;
        return Math.sqrt(variance);
    }

    applyBookTickerUpdate(data) {
        const bid = Number.parseFloat(data.b);
        const ask = Number.parseFloat(data.a);

        if (Number.isFinite(bid) && bid > 0) this.bestBid = bid;
        if (Number.isFinite(ask) && ask > 0) this.bestAsk = ask;
    }

    addAggTrade(tradeData) {
        this.aggTrades.add({
            price: parseFloat(tradeData.p),
            quantity: parseFloat(tradeData.q),
            eventTime: parseInt(tradeData.E, 10),
            isBuyerMaker: tradeData.m,
        });

        this.lastPrice = parseFloat(tradeData.p);
    }

    applyTickerUpdate(tickerData) {
        this.ticker24hrVolumeUsdt = parseFloat(tickerData.q);
        this.ticker24hrPriceChangePct = parseFloat(tickerData.P);
        this.ticker24hrHigh = parseFloat(tickerData.h);
        this.ticker24hrLow = parseFloat(tickerData.l);
        this.tickerLastPrice = parseFloat(tickerData.c);
    }

    getHistoricalPrice(targetTimeMs) {
        for (let i = this.priceBuckets.size - 1; i >= 0; i--) {
            const bucket = this.priceBuckets.get(i);

            if (bucket && bucket.time <= targetTimeMs) {
                return bucket.price;
            }
        }

        return null;
    }

    performPeriodicCalculations() {
        const now = Date.now();
        const oneSecondAgo = now - 1000;

        let volume1s = 0;
        let tradeCount1s = 0;
        let takerBuyVolume1s = 0;
        let takerSellVolume1s = 0;

        const tradesInLastSecond = [];
        const recentTrades = this.aggTrades.toArray();

        for (let i = recentTrades.length - 1; i >= 0; i--) {
            const trade = recentTrades[i];

            if (trade.eventTime >= oneSecondAgo) {
                const tradeVolume = trade.price * trade.quantity;

                volume1s += tradeVolume;
                tradeCount1s++;
                tradesInLastSecond.push(trade);

                if (!trade.isBuyerMaker) {
                    takerBuyVolume1s += tradeVolume;
                } else {
                    takerSellVolume1s += tradeVolume;
                }
            } else {
                break;
            }
        }

        this.current1sVolumeSum = volume1s;
        this.current1sTradeCount = tradeCount1s;
        this.current1sTakerBuyVolume = takerBuyVolume1s;
        this.current1sTakerSellVolume = takerSellVolume1s;

        if (this.ewma5mVolumeBaseline === 0 && volume1s > 0) {
            this.ewma5mVolumeBaseline = volume1s;
        }

        this.ewma1sVolumeFast = (params.EWMA_ALPHA_VOL_FAST * volume1s) + ((1 - params.EWMA_ALPHA_VOL_FAST) * this.ewma1sVolumeFast);
        this.ewma5mVolumeBaseline = (params.EWMA_ALPHA_VOL_SLOW * volume1s) + ((1 - params.EWMA_ALPHA_VOL_SLOW) * this.ewma5mVolumeBaseline);

        this.ewma1mVolumeBaseline = params.EWMA_ALPHA_VOL_MED * volume1s +
                            (1 - params.EWMA_ALPHA_VOL_MED) * this.ewma1mVolumeBaseline;
        this.volumeAccel = this.ewma1sVolumeFast - this.prevEwma1s;
        this.prevEwma1s  = this.ewma1sVolumeFast;

        if (now - this.lastAtrCalculationTime >= 1000) {
            if (tradesInLastSecond.length > 0) {
                const prices = tradesInLastSecond.map(t => t.price);
                const high = Math.max(...prices);
                const low = Math.min(...prices);
                const close = prices[0]; // Most recent trade in the 1s window
                const prevClose = this.priceHistoryForATR.size > 0 ? this.priceHistoryForATR.getNewest().close : close;
                const trueRange = Math.max(high - low, Math.abs(high - prevClose), Math.abs(low - prevClose));

                // Initialize both ATRs if they are zero
                if (this.atrFast === 0) this.atrFast = trueRange;
                if (this.atrSlow === 0) this.atrSlow = trueRange;

                // Update both fast and slow ATRs using their respective alphas
                this.atrFast = (params.ATR_ALPHA * trueRange) + ((1 - params.ATR_ALPHA) * this.atrFast);
                this.atrSlow = (params.ATR_ALPHA_SLOW * trueRange) + ((1 - params.ATR_ALPHA_SLOW) * this.atrSlow);

                // This history buffer is for getting the previous close for the next TR calc
                this.priceHistoryForATR.add({ high, low, close });
            } else if (this.priceHistoryForATR.size > 0) {
                // Carry forward last candle if no trades
                const lastCandle = this.priceHistoryForATR.getNewest();
                this.priceHistoryForATR.add(lastCandle);
            }

            this.lastAtrCalculationTime = now;
        }

        if (this.lastPrice > 0) {
            const currentBucketFloor = Math.floor(now / params.PRICE_BUCKET_DURATION_MS) * params.PRICE_BUCKET_DURATION_MS;

            if (this.lastPriceBucketTime === 0) {
                this.lastPriceBucketTime = currentBucketFloor;
            }

            if (currentBucketFloor > this.lastPriceBucketTime) {
                this.priceBuckets.add({ time: currentBucketFloor, price: this.lastPrice });
                this.lastPriceBucketTime = currentBucketFloor;
            } else {
                const lastEntry = this.priceBuckets.getNewest();

                if (lastEntry && lastEntry.time === currentBucketFloor) {
                    lastEntry.price = this.lastPrice;
                } else if (!lastEntry || lastEntry.time < currentBucketFloor) {
                    this.priceBuckets.add({ time: currentBucketFloor, price: this.lastPrice });
                }
            }
        }

        this.updateEMAAlignment(this.lastPrice);
        this.updateRSI(this.lastPrice);
        this.updateMACD(this.lastPrice);

        const buy = this.current1sTakerBuyVolume;
        const sell = this.current1sTakerSellVolume;

        this.takerFlowImbalance = (buy - sell) / (buy + sell + 1e-8);
        this.takerFlowMagnitude = buy + sell;
        this.takerFlowRatio = buy / (sell + 1e-8);

        this.accelHistory.add(this.volumeAccel);

        if (this.accelHistory.size >= 20) {
            const a = this.accelHistory.toArray();
            const mean = a.reduce((s,x)=>s+x,0) / a.length;
            const var_ = a.reduce((s,x)=>s+(x-mean)**2,0) / a.length;
            this.accelSigma = Math.sqrt(var_);
        }

        const priceAtLookback2s = this.getHistoricalPrice(now - 2000) ?? this.lastPrice;

        const priceDelta = this.lastPrice - priceAtLookback2s;

        this.priceSlope = params.PRICE_SLOPE_ALPHA * priceDelta +
                          (1 - params.PRICE_SLOPE_ALPHA) * this.priceSlope;

        this.priceSlopeHist.add(this.priceSlope);

        if (this.priceSlopeHist.size >= 20) {
            const v = this.priceSlopeHist.toArray();
            const m = v.reduce((s,x)=>s+x,0)/v.length;
            const var_ = v.reduce((s,x)=>s+(x-m)**2,0)/v.length;
            this.priceSlopeSigma = Math.sqrt(var_);
        }
    }

    getAbsoluteVolumeFloor() {
        const secondsInDay = 24 * 60 * 60;
        const quarterSecondShare = this.ticker24hrVolumeUsdt / secondsInDay * 0.25;

        const tierFloor = this.marketCapTier === "large" ? 600
                         : this.marketCapTier === "mid"   ? 500
                         : 400;

        return Math.max(tierFloor, quarterSecondShare);
    }

    async checkSignal() {
        const now = Date.now();

        this.updateTimeCache(now);

        const maxAtr = this.ticker24hrVolumeUsdt < 2e8 ? 0.10 : 0.05;

        if (this.atrSlow === 0) {
            return null;
        }

        if (this.atrSlow / this.lastPrice > maxAtr) {
            // console.log("atr slow");
            return null;
        }

        if (this.ticker24hrVolumeUsdt < 750000) {
            return null;
        }

        if (now - this.lastSignalTriggerTime < params.SIGNAL_COOLDOWN_MS) {
            return null;
        }

        if (!Number.isFinite(this.bestBid) || !Number.isFinite(this.bestAsk) ||
            this.bestBid <= 0 || this.bestAsk <= 0 ||
            this.bestAsk <= this.bestBid) {
            return null;
        }

        const spreadPct = (this.bestAsk - this.bestBid) / this.bestAsk;

        if (spreadPct > params.MAX_BID_ASK_SPREAD_PCT) {
            return null;
        }

        if (this.lastPrice === 0 || this.ewma5mVolumeBaseline === 0) {
            return null;
        }

        const ratioFast1m = this.ewma1mVolumeBaseline > 0
            ? this.ewma1sVolumeFast / this.ewma1mVolumeBaseline : 0;

        const ratio1m5m   = this.ewma5mVolumeBaseline > 0
            ? this.ewma1mVolumeBaseline / this.ewma5mVolumeBaseline : 0;

        const accelZ = this.accelSigma > 0 ? this.volumeAccel / this.accelSigma : 0;

        const dynThresh = this.getDynamicVolumeThreshold();

        const isVolumeSpike =
              ratioFast1m >= dynThresh &&
              ratio1m5m   >= params.MIN_VOLUME_SPIKE_RATIO_1M5M &&
              accelZ      >= params.VOLUME_ACCEL_ZSCORE &&
              this.current1sVolumeSum >= this.getAbsoluteVolumeFloor() &&
              this.current1sTradeCount >= params.MIN_TRADES_IN_1S;

        if (!isVolumeSpike) {
            return null;
        }

        const priceAtLookback = this.getHistoricalPrice(now - params.PRICE_LOOKBACK_WINDOW_MS);

        if (priceAtLookback === null || this.lastPrice <= priceAtLookback) {
            return null;
        }

        const priceChangePct = (this.lastPrice - priceAtLookback) / priceAtLookback;
        const atrFastAsPercentage = this.atrFast > 0 ? this.atrFast / this.lastPrice : 0;
        const slopeZ = this.priceSlopeSigma > 0 ? this.priceSlope / this.priceSlopeSigma : 0;

        if (slopeZ < params.PRICE_SLOPE_ZSCORE) {
            return null;
        }

        this.lastSignalTriggerTime = now;

        const takerRatioSmoothed = this.current1sTakerBuyVolume / (this.current1sTakerSellVolume + 1);

        const volatilityExpansionRatio = this.atrSlow > 0
            ? this.atrFast / this.atrSlow
            : 1.0;

        const atrScaling = this.getATRVolatilityScaling();

        console.log(new Date(), `[SIGNAL] ${this.symbol} | Px: ${this.lastPrice.toFixed(4)} | ATR: ${atrScaling.toFixed(2)}x | TakerR(s): ${takerRatioSmoothed.toFixed(2)}x | VolExp: ${volatilityExpansionRatio.toFixed(2)}x`);

        const vector = {
            createdAt: new Date(),
            symbol: this.symbol,
            signalTimestampMs: now,
            triggerPrice: this.lastPrice,
            priceChangePct: priceChangePct,
            volumeRatioFast1m: ratioFast1m,
            volumeRatio1m5m: ratio1m5m,
            volumeAccelZ: accelZ,
            takerRatioSmoothed: takerRatioSmoothed,
            takerBuyVolumeAbs: this.current1sTakerBuyVolume,
            volatilityExpansionRatio: volatilityExpansionRatio,
            current1sVolumeUsdt: this.current1sVolumeSum,
            spreadPct: spreadPct,
            atrFastPct: atrFastAsPercentage,
            depth5ObImbalance: this.depth5ObImbalance,
            depth5BidVolume: this.depth5BidVolume,
            depth5AskVolume: this.depth5AskVolume,
            depth5TotalVolume: this.depth5TotalVolume,
            depth5VolumeRatio: this.depth5VolumeRatio,
            ticker24hrVolumeUsdt: this.ticker24hrVolumeUsdt,
            ticker24hrPriceChangePct: this.ticker24hrPriceChangePct,
            ticker24hrHigh: this.ticker24hrHigh,
            ticker24hrLow: this.ticker24hrLow,
            takerFlowImbalance: this.takerFlowImbalance,
            takerFlowMagnitude: this.takerFlowMagnitude,
            takerFlowRatio: this.takerFlowRatio,
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
            imbalanceMA5: this.imbalanceMA5,
            imbalanceMA20: this.imbalanceMA20,
            imbalanceVelocity: this.imbalanceVelocity,
            imbalanceVolatility: this.imbalanceVolatility,
            priceSlope: this.priceSlope,
            slopeZ: slopeZ,
            realisedVolFast: this.atrFast,
            realisedVolMedium: this.atrSlow,
            dynVolumeThresh: dynThresh,
            explosiveRatio: this.atrSlow > 0 ? this.atrFast/this.atrSlow : 1,
            hourOfDay: this.cachedHourOfDay,
            dayOfWeek: this.cachedDayOfWeek,
            isWeekend: this.cachedIsWeekend
        };

        const insert = await mongo.signals.insertOne(vector);

        await priceQueue.add("gate_price", {
            id: insert.insertedId.toString(),
            symbol: this.symbol,
            timestamp: vector.signalTimestampMs
        }, {
            removeOnComplete: true,
            removeOnFail: true,
            delay: 31 * 60 * 1000
        });

        await orderQueue.add("gate_orderbook", {
            id: insert.insertedId.toString(),
            symbol: this.symbol,
            tOffset: 3
        }, {
            removeOnComplete: true,
            removeOnFail: true,
            delay: 3000
        });

        await orderQueue.add("gate_orderbook", {
            id: insert.insertedId.toString(),
            symbol: this.symbol,
            tOffset: 10
        }, {
            removeOnComplete: true,
            removeOnFail: true,
            delay: 10000
        });

        await orderQueue.add("gate_ongorderbook", {
            id: insert.insertedId.toString(),
            symbol: this.symbol,
            tOffset: 30
        }, {
            removeOnComplete: true,
            removeOnFail: true,
            delay: 30000
        });
    }
}

export default SymbolMonitor;