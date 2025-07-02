import { Queue } from "bullmq";
import mongo from "./mongo.js";
import params from "./parameters.js";
import CircularBuffer from "./circularBuffer.js";

const priceQueue = new Queue("gate_price");
const orderQueue = new Queue("gate_order");

class SymbolMonitor {
    constructor(symbol, marketCapTier = "mid") {
        this.symbol = symbol;
        this.exchange = "gate";
        this.marketCapTier = marketCapTier;

        // Volume acceleration tracking
        this.accelHistory = new CircularBuffer(60);   // last 15 s (60×250 ms)
        this.accelSigma = 0;

        // Price momentum tracking
        this.priceSlope = 0;
        this.priceSlopeHist = new CircularBuffer(40);  // ~10 s
        this.priceSlopeSigma = 0;

        // Time caching for efficiency
        this.cachedDayOfWeek = 0;
        this.cachedHourOfDay = 0;
        this.cachedIsWeekend = false;
        this.lastTimeCacheUpdate = 0;

        // Trade and order book data
        this.aggTrades = new CircularBuffer(params.AGG_TRADE_BUFFER_SIZE);
        this.bestBid = 0;
        this.bestAsk = 0;
        this.bidAskMidpoint = 0;

        // Volume tracking
        this.current1sVolumeSum = 0;
        this.current1sTradeCount = 0;
        this.ewma1sVolumeFast = 0;
        this.ewma5mVolumeBaseline = 0;
        this.ewma1mVolumeBaseline = 0;
        this.prevEwma1s = 0;
        this.volumeAccel = 0;

        // Taker flow tracking
        this.current1sTakerBuyVolume = 0;
        this.current1sTakerSellVolume = 0;
        this.takerFlowImbalance = 0;
        this.takerFlowMagnitude = 0;
        this.takerFlowRatio = 0;

        // Price tracking
        this.lastPrice = 0;
        this.priceBuckets = new CircularBuffer(Math.ceil(params.PRICE_LOOKBACK_WINDOW_MS / params.PRICE_BUCKET_DURATION_MS) + 5);
        this.lastPriceBucketTime = 0;

        // NEW: Volatility tracking using returns
        this.returnHistory = new CircularBuffer(300); // 5 minutes of 1-second returns
        this.lastReturnCalcTime = 0;
        this.lastPriceForReturn = 0;
        this.volatility30s = 0;  // 30-second realized volatility (annualized)
        this.volatility5m = 0;   // 5-minute realized volatility (annualized)
        this.volatilityRatio = 1.0;

        // Microstructure quality tracking
        this.effectiveSpreadHistory = new CircularBuffer(60); // 1 minute
        this.avgEffectiveSpread = 0;
        this.tradeImbalanceHistory = new CircularBuffer(60);
        this.avgTradeImbalance = 0;

        // Technical indicators
        this.rsiPriceHistory = new CircularBuffer(20);
        this.rsiValue = 50;
        this.avgGain = 0;
        this.avgLoss = 0;

        // EMA tracking
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

        // Order book imbalance tracking
        this.depth5ObImbalance = 0;
        this.depth5BidVolume = 0;
        this.depth5AskVolume = 0;
        this.depth5TotalVolume = 0;
        this.depth5VolumeRatio = 0;
        this.imbalanceHistory = new CircularBuffer(20);
        this.imbalanceMA5 = 0;
        this.imbalanceMA20 = 0;
        this.previousImbalance = 0;
        this.imbalanceVelocity = 0;
        this.imbalanceVolatility = 0;

        // Ticker data
        this.ticker24hrVolumeUsdt = 0;
        this.ticker24hrPriceChangePct = 0;
        this.ticker24hrHigh = 0;
        this.ticker24hrLow = 0;

        // Signal control
        this.lastSignalTriggerTime = 0;

        // Database references
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

    // NEW: Calculate volatility from returns
    updateVolatility(currentPrice, now) {
        // Only calculate returns at 1-second intervals
        if (this.lastPriceForReturn > 0 && now - this.lastReturnCalcTime >= 1000) {
            const logReturn = Math.log(currentPrice / this.lastPriceForReturn);
            this.returnHistory.add({ time: now, return: logReturn });

            // Get returns for different windows
            const allReturns = this.returnHistory.toArray();
            const cutoff30s = now - 30000;
            const cutoff5m = now - 300000;

            const returns30s = allReturns.filter(r => r.time > cutoff30s).map(r => r.return);
            const returns5m = allReturns.filter(r => r.time > cutoff5m).map(r => r.return);

            // Calculate annualized volatilities
            if (returns30s.length >= 10) { // Need at least 10 returns
                this.volatility30s = this.calculateAnnualizedVolatility(returns30s);
            }

            if (returns5m.length >= 30) { // Need at least 30 returns for 5m
                this.volatility5m = this.calculateAnnualizedVolatility(returns5m);
                this.volatilityRatio = this.volatility5m > 0 ? this.volatility30s / this.volatility5m : 1.0;
            }

            this.lastPriceForReturn = currentPrice;
            this.lastReturnCalcTime = now;
        } else if (this.lastPriceForReturn === 0) {
            // Initialize
            this.lastPriceForReturn = currentPrice;
            this.lastReturnCalcTime = now;
        }
    }

    calculateAnnualizedVolatility(returns) {
        if (returns.length < 2) return 0;

        const mean = returns.reduce((a, b) => a + b, 0) / returns.length;
        const variance = returns.reduce((sum, r) => sum + Math.pow(r - mean, 2), 0) / (returns.length - 1);
        const stdDev = Math.sqrt(variance);

        // Annualize: multiply by sqrt(seconds per year)
        return stdDev * Math.sqrt(365 * 24 * 60 * 60);
    }

    // Updated dynamic threshold calculation
    getDynamicVolumeThreshold(timeContext = {}) {
        const baseMultiplier = 4.0;

        if (this.lastPrice === 0 || this.volatility5m === 0) {
            return baseMultiplier;
        }

        // Use actual volatility instead of ATR
        const normalizedVol = this.volatility30s / Math.sqrt(365 * 24 * 60 * 60); // Convert back to 1-second vol

        // Volatility regime adjustment
        let regimeModifier;
        if (this.volatilityRatio > 1.5) {
            regimeModifier = 1.25; // Volatility expansion
        } else if (this.volatilityRatio < 0.8) {
            regimeModifier = 0.75; // Volatility contraction
        } else {
            regimeModifier = 1.0;  // Normal regime
        }

        const volatilityFactor = 1 + (normalizedVol * 50 * regimeModifier);

        // Session factor
        const { hourOfDay = this.cachedHourOfDay, isWeekend = this.cachedIsWeekend } = timeContext;
        let sessionFactor = 1.0;

        if (isWeekend) {
            sessionFactor = 0.8;
        } else {
            if (hourOfDay >= 13 && hourOfDay <= 17) {
                sessionFactor = 1.5; // EU-US overlap
            } else if (hourOfDay >= 0 && hourOfDay < 7) {
                sessionFactor = 0.75; // Asian session
            }
        }

        const dynamicThreshold = baseMultiplier * volatilityFactor * sessionFactor;
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

        if (this.rsiPriceHistory.size < 2) return;

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
        if (!data.bids?.length || !data.asks?.length) return;

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

        if (this.bestBid > 0 && this.bestAsk > 0) {
            this.bidAskMidpoint = (this.bestBid + this.bestAsk) / 2;
        }
    }

    addAggTrade(tradeData) {
        const trade = {
            price: parseFloat(tradeData.p),
            quantity: parseFloat(tradeData.q),
            eventTime: parseInt(tradeData.E, 10),
            isBuyerMaker: tradeData.m,
        };

        this.aggTrades.add(trade);
        this.lastPrice = trade.price;

        // Calculate effective spread if we have quotes
        if (this.bidAskMidpoint > 0) {
            const effectiveSpreadBps = Math.abs(trade.price - this.bidAskMidpoint) / this.bidAskMidpoint * 10000;
            this.effectiveSpreadHistory.add(effectiveSpreadBps);

            // Update average
            const spreads = this.effectiveSpreadHistory.toArray();
            this.avgEffectiveSpread = spreads.reduce((a, b) => a + b, 0) / spreads.length;
        }

        // Track trade imbalance
        const imbalance = trade.isBuyerMaker ? -trade.quantity : trade.quantity;
        this.tradeImbalanceHistory.add(imbalance);
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

        // Update volatility with current price
        if (this.lastPrice > 0) {
            this.updateVolatility(this.lastPrice, now);
        }

        // Calculate 1-second volume metrics
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

        // Update volume EMAs
        if (this.ewma5mVolumeBaseline === 0 && volume1s > 0) {
            this.ewma5mVolumeBaseline = volume1s;
        }

        this.ewma1sVolumeFast = (params.EWMA_ALPHA_VOL_FAST * volume1s) + ((1 - params.EWMA_ALPHA_VOL_FAST) * this.ewma1sVolumeFast);
        this.ewma5mVolumeBaseline = (params.EWMA_ALPHA_VOL_SLOW * volume1s) + ((1 - params.EWMA_ALPHA_VOL_SLOW) * this.ewma5mVolumeBaseline);
        this.ewma1mVolumeBaseline = params.EWMA_ALPHA_VOL_MED * volume1s + (1 - params.EWMA_ALPHA_VOL_MED) * this.ewma1mVolumeBaseline;

        this.volumeAccel = this.ewma1sVolumeFast - this.prevEwma1s;
        this.prevEwma1s = this.ewma1sVolumeFast;

        // Update price buckets
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

        // Update technical indicators
        this.updateEMAAlignment(this.lastPrice);
        this.updateRSI(this.lastPrice);
        this.updateMACD(this.lastPrice);

        // Update taker flow metrics
        const buy = this.current1sTakerBuyVolume;
        const sell = this.current1sTakerSellVolume;
        this.takerFlowImbalance = (buy - sell) / (buy + sell + 1e-8);
        this.takerFlowMagnitude = buy + sell;
        this.takerFlowRatio = buy / (sell + 1e-8);

        // Update acceleration history
        this.accelHistory.add(this.volumeAccel);
        if (this.accelHistory.size >= 20) {
            const a = this.accelHistory.toArray();
            const mean = a.reduce((s, x) => s + x, 0) / a.length;
            const variance = a.reduce((s, x) => s + (x - mean) ** 2, 0) / a.length;
            this.accelSigma = Math.sqrt(variance);
        }

        // Update price slope
        const priceAtLookback2s = this.getHistoricalPrice(now - 2000) ?? this.lastPrice;
        const priceDelta = this.lastPrice - priceAtLookback2s;
        this.priceSlope = params.PRICE_SLOPE_ALPHA * priceDelta + (1 - params.PRICE_SLOPE_ALPHA) * this.priceSlope;

        this.priceSlopeHist.add(this.priceSlope);
        if (this.priceSlopeHist.size >= 20) {
            const v = this.priceSlopeHist.toArray();
            const m = v.reduce((s, x) => s + x, 0) / v.length;
            const variance = v.reduce((s, x) => s + (x - m) ** 2, 0) / v.length;
            this.priceSlopeSigma = Math.sqrt(variance);
        }
    }

    getAbsoluteVolumeFloor() {
        const secondsInDay = 24 * 60 * 60;
        const quarterSecondShare = this.ticker24hrVolumeUsdt / secondsInDay * 0.25;

        const tierFloor = {
            'mega': 1000,
            'large': 600,
            'mid': 500,
            'small': 400,
            'micro': 300
        }[this.marketCapTier] || 400;

        return Math.max(tierFloor, quarterSecondShare);
    }

    async checkSignal() {
        const now = Date.now();
        this.updateTimeCache(now);

        // Basic data availability checks
        if (this.lastPrice === 0 || this.ewma5mVolumeBaseline === 0) {
            return null;
        }

        // NEW: Ensure we have enough return history for reliable volatility
        const returnCount = this.returnHistory.size;

        if (returnCount < 30) {
            // Need at least 30 seconds of returns for volatility30s
            return null;
        }

        // Check if we have volatility data
        if (this.volatility30s === 0) {
            // Still warming up volatility calculations
            return null;
        }

        // For 5-minute volatility, we can work with partial data
        // but should have at least 60 returns (1 minute)
        // const hasReliableVolatility = returnCount >= 60;

        // Log startup status periodically
        // if (!hasReliableVolatility && now % 10000 < 250) {
            // console.log(`[${this.symbol}] Warming up: ${returnCount} returns collected, need 60+ for signals`);
        // }

        if (this.ticker24hrVolumeUsdt < 1_000_000) { // $1M daily volume minimum
            return null;
        }

        // Cooldown check
        if (now - this.lastSignalTriggerTime < params.SIGNAL_COOLDOWN_MS) {
            return null;
        }

        // Volatility checks based on tier
        const maxVolatility = {
            'mega': 0.50,   // 50% annualized
            'large': 0.80,  // 80% annualized
            'mid': 1.20,    // 120% annualized
            'small': 2.00,  // 200% annualized
            'micro': 3.00   // 300% annualized
        };

        const tierMaxVol = maxVolatility[this.marketCapTier] || 1.50;
        if (this.volatility5m > tierMaxVol) {
            return null; // Too volatile
        }

        if (this.volatility5m < 0.05) {
            return null; // Too stable (< 5% annual vol)
        }

        // Spread checks
        if (!Number.isFinite(this.bestBid) || !Number.isFinite(this.bestAsk) ||
            this.bestBid <= 0 || this.bestAsk <= 0 || this.bestAsk <= this.bestBid) {
            return null;
        }

        const spreadPct = (this.bestAsk - this.bestBid) / this.bestAsk;
        const spreadBps = spreadPct * 10000;

        if (spreadPct > params.MAX_BID_ASK_SPREAD_PCT) {
            return null;
        }

        // Spread relative to volatility check
        const instantVol = this.volatility30s / Math.sqrt(365 * 24 * 60 * 60); // Convert to per-second
        const normalizedSpread = spreadPct / (instantVol + 0.0001);
        if (normalizedSpread > 3.0) {
            return null; // Spread too wide relative to volatility
        }

        // Volume spike detection
        const ratioFast1m = this.ewma1mVolumeBaseline > 0 ? this.ewma1sVolumeFast / this.ewma1mVolumeBaseline : 0;
        const ratio1m5m = this.ewma5mVolumeBaseline > 0 ? this.ewma1mVolumeBaseline / this.ewma5mVolumeBaseline : 0;
        const accelZ = this.accelSigma > 0 ? this.volumeAccel / this.accelSigma : 0;
        const dynThresh = this.getDynamicVolumeThreshold();

        const isVolumeSpike =
            ratioFast1m >= dynThresh &&
            ratio1m5m >= params.MIN_VOLUME_SPIKE_RATIO_1M5M &&
            accelZ >= params.VOLUME_ACCEL_ZSCORE &&
            this.current1sVolumeSum >= this.getAbsoluteVolumeFloor() &&
            this.current1sTradeCount >= params.MIN_TRADES_IN_1S;

        if (!isVolumeSpike) {
            return null;
        }

        // Price momentum check
        const priceAtLookback = this.getHistoricalPrice(now - params.PRICE_LOOKBACK_WINDOW_MS);
        if (priceAtLookback === null || this.lastPrice <= priceAtLookback) {
            return null;
        }

        const priceChangePct = (this.lastPrice - priceAtLookback) / priceAtLookback;
        const slopeZ = this.priceSlopeSigma > 0 ? this.priceSlope / this.priceSlopeSigma : 0;

        if (slopeZ < params.PRICE_SLOPE_ZSCORE) {
            return null;
        }

        // Price move relative to volatility
        const priceZScore = instantVol > 0 ? (priceChangePct / instantVol) : 0;
        if (priceZScore < 1.5) {
            return null; // Price move not significant relative to volatility
        }

        // All checks passed - trigger signal
        this.lastSignalTriggerTime = now;

        const takerRatioInstant = this.current1sTakerBuyVolume / (this.current1sTakerSellVolume + 1);

        console.log(new Date(), `[SIGNAL] ${this.symbol} | Px: ${this.lastPrice.toFixed(4)} | Vol30s: ${(this.volatility30s * 100).toFixed(1)}% | VolRatio: ${this.volatilityRatio.toFixed(2)} | TakerR: ${takerRatioInstant.toFixed(2)} | Spread: ${spreadBps.toFixed(1)}bps`);

        const vector = {
            exchange: this.exchange,
            createdAt: new Date(now),
            symbol: this.symbol.replace(/[^A-Za-z0-9]/g, "").toUpperCase(),
            signalTimestampMs: now,
            triggerPrice: this.lastPrice,

            // Price metrics
            priceChangePct: priceChangePct,
            priceSlope: this.priceSlope,
            slopeZ: slopeZ,
            priceZScore: priceZScore,

            // Volume metrics
            volumeRatioFast1m: ratioFast1m,
            volumeRatio1m5m: ratio1m5m,
            volumeAccelZ: accelZ,
            current1sVolumeUsdt: this.current1sVolumeSum,
            volumePerDollar: this.current1sVolumeSum / (this.ticker24hrVolumeUsdt + 1),
            dynVolumeThresh: dynThresh,

            // Volatility metrics (NEW)
            volatility30s: this.volatility30s,
            volatility5m: this.volatility5m,
            volatilityRatio: this.volatilityRatio,

            // Microstructure metrics
            spreadPct: spreadPct,
            spreadBps: spreadBps,
            normalizedSpread: normalizedSpread,
            effectiveSpreadBps: this.avgEffectiveSpread,

            // Order book metrics
            depth5ObImbalance: this.depth5ObImbalance,
            depth5BidVolume: this.depth5BidVolume,
            depth5AskVolume: this.depth5AskVolume,
            depth5TotalVolume: this.depth5TotalVolume,
            depth5VolumeRatio: this.depth5VolumeRatio,
            imbalanceMA5: this.imbalanceMA5,
            imbalanceMA20: this.imbalanceMA20,
            imbalanceVelocity: this.imbalanceVelocity,
            imbalanceVolatility: this.imbalanceVolatility,

            // Taker flow metrics
            takerRatioSmoothed: takerRatioInstant,
            takerBuyVolumeAbs: this.current1sTakerBuyVolume,
            takerFlowImbalance: this.takerFlowImbalance,
            takerFlowMagnitude: this.takerFlowMagnitude,
            takerFlowRatio: this.takerFlowRatio,

            // Technical indicators
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

            // Market data
            ticker24hrVolumeUsdt: this.ticker24hrVolumeUsdt,
            ticker24hrPriceChangePct: this.ticker24hrPriceChangePct,
            ticker24hrHigh: this.ticker24hrHigh,
            ticker24hrLow: this.ticker24hrLow,

            // Time features
            hourOfDay: this.cachedHourOfDay,
            dayOfWeek: this.cachedDayOfWeek,
            isWeekend: this.cachedIsWeekend,

            // DEPRECATED - for backwards compatibility only
            realisedVolFast: this.volatility30s,
            realisedVolMedium: this.volatility5m,
            explosiveRatio: this.volatilityRatio,
            volatilityExpansionRatio: this.volatilityRatio
        };

        // Save to database
        const insert = await mongo.signals.insertOne(vector);

        // Queue follow-up tasks
        await priceQueue.add("gate_price", {
            id: insert.insertedId.toString(),
            symbol: this.symbol,
            timestamp: vector.signalTimestampMs
        }, {
            removeOnComplete: true,
            removeOnFail: true,
            delay: 31 * 60 * 1000 // 31 minutes
        });

        // Queue orderbook snapshots
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

        await orderQueue.add("gate_orderbook", {
            id: insert.insertedId.toString(),
            symbol: this.symbol,
            tOffset: 30
        }, {
            removeOnComplete: true,
            removeOnFail: true,
            delay: 30000
        });

        return vector;
    }
}

export default SymbolMonitor;