import IORedis from "ioredis";

const RETENTION_DAYS = 45;
const RETENTION_SECONDS = RETENTION_DAYS * 24 * 60 * 60;
const REDIS_KEY_PREFIX = "gate:secbar:";

class BarBuilder {
    constructor(pair, redis) {
        this.pair = pair;
        this.redis = redis;
        this._secBucketTS = null;
        this._open = this._high = this._low = this._close = 0;
        this._volume = 0;
    }

    onTrade(price, volumeUSDT, tsMs) {
        const secBucket = Math.floor(tsMs / 1000);

        if (this._secBucketTS === null) {
            this._startNewBar(secBucket, price, volumeUSDT);
            return;
        }

        if (secBucket === this._secBucketTS) {
            this._high= Math.max(this._high, price);
            this._low= Math.min(this._low,  price);
            this._close = price;
            this._volume += volumeUSDT;
        } else if (secBucket > this._secBucketTS) {
            this._flushCurrentBar();

            for (let s = this._secBucketTS + 1; s < secBucket; s++) {
                this._pushBarToRedis(s, this._close, this._close, this._close, this._close, 0);
            }

            this._startNewBar(secBucket, price, volumeUSDT);
        }
    }

    flush() {
        if (this._secBucketTS !== null) {
            this._flushCurrentBar();
        }
    }

    static async getSecBars(redis, pair, startMs, endMs) {
        const key = REDIS_KEY_PREFIX + pair.toUpperCase();
        const startScore = Math.floor(startMs / 1000);
        const endScore = Math.floor(endMs   / 1000);
        const raw = await redis.zrangebyscore(key, startScore, endScore, "WITHSCORES");
        const bars = [];

        for (let i = 0; i < raw.length; i += 2) {
            const [o, h, l, c, v] = raw[i].split(",").map(Number);

            bars.push({
                t: Number(raw[i + 1]) * 1000, // back to ms
                open: o, high: h, low: l, close: c, volume: v
            });
        }

        return bars;
    }

    _startNewBar(secBucket, price, volumeUSDT) {
        this._secBucketTS = secBucket;
        this._open = this._high = this._low = this._close = price;
        this._volume = volumeUSDT;
    }

    _flushCurrentBar() {
        this._pushBarToRedis(
            this._secBucketTS,
            this._open,
            this._high,
            this._low,
            this._close,
            this._volume
        );
    }

    _pushBarToRedis(secBucket, open, high, low, close, volume) {
        const key = REDIS_KEY_PREFIX + this.pair;
        const member = `${open},${high},${low},${close},${volume}`;
        this.redis.zadd(key, secBucket, member);
        this.redis.expire(key, RETENTION_SECONDS);
    }
}

const _builders = new Map();
let _redis = null;

export function initPriceTape(redisInstance) {
    if (!redisInstance || !(redisInstance instanceof IORedis)) {
        throw new Error("initPriceTape() requires an ioredis instance");
    }

    _redis = redisInstance;
}

export function ensureTapeForPair(pair) {
    if (!_redis) {
        throw new Error("PriceTape not initialised – call initPriceTape()");
    }

    if (!_builders.has(pair)) {
        _builders.set(pair, new BarBuilder(pair, _redis));
    }

    return _builders.get(pair);
}

export function handleTradeTick(pair, price, volumeUSDT, tsMs) {
    ensureTapeForPair(pair).onTrade(price, volumeUSDT, tsMs);
}

export async function getSecBars(pair, startMs, endMs) {
    return BarBuilder.getSecBars(_redis, pair, startMs, endMs);
}

export function shutdownPriceTape() {
    for (const builder of _builders.values()) {
        builder.flush();
    }
}