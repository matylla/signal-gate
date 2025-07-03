import { ObjectId } from "mongodb";
import mongo from "./mongo.js";
import { Worker } from "bullmq";
import IORedis from "ioredis";
import { getSecBars } from "./priceTape.js";

const redis = new IORedis({
    maxRetriesPerRequest: null
});

const WINDOW_MS = 30 * 60 * 1000;
const offsets = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,45,60,90,120,150,180,210,240,270,300,330,360,390,420,450,480,510,540,570,600,660,720,780,840,900,960,1020,1080,1140,1200,1260,1320,1380,1440,1500,1560,1620,1680,1740,1800];

function realisedSigma(bars) {
    if (bars.length < 2) {
        return null;
    }

    const rets = [];

    for (let i = 1; i < bars.length; i++) {
        const p0 = bars[i - 1].close;
        const p1 = bars[i].close;

        if (p0 > 0) {
            rets.push(Math.log(p1 / p0));
        }
    }

    if (!rets.length) {
        return null;
    }

    const μ = rets.reduce((s, x) => s + x, 0) / rets.length;
    const variance = rets.reduce((s, x) => s + (x - μ) ** 2, 0) / rets.length;
    return Math.sqrt(variance);
}

new Worker("gate_price", async (job) => {
    try {
        await processJob(job);
    } catch (err) {
        console.error("[workerPrice] error:", err);
    }
}, {
    connection: new IORedis({
        maxRetriesPerRequest: null
    })
});

async function processJob(job) {
    const { id, symbol, timestamp } = job.data;

    const startMs = timestamp;
    const endMs = startMs + WINDOW_MS;
    const bars = await getSecBars(symbol, startMs, endMs - 1000);

    if (!bars.length) {
        console.warn(`[workerPrice] No bars for ${symbol} ${new Date(startMs).toISOString()}`);
    }

    const sigma30m = realisedSigma(bars);

    const priceRows = offsets.map(sec => {
        const targetTime = startMs + sec * 1000;
        let nearestBar = null;

        for (let i = 0; i < bars.length; i++) {
            if (bars[i].t >= targetTime) {
                nearestBar = bars[i];
                break;
            }
        }

        if (!nearestBar && bars.length) {
            nearestBar = bars[bars.length - 1];
        }

        return {
            t_offset_s: sec,
            price: nearestBar ? nearestBar.close : null,
            volume: nearestBar ? nearestBar.volume : 0
        };
    });

    await mongo.prices.insertOne({
        signal_id: new ObjectId(id),
        symbol: symbol.replace(/[^A-Za-z0-9]/g, "").toUpperCase(),
        exchange: "gate",
        sigma30m: sigma30m,
        prices: priceRows
    });
}

process.on("SIGINT", shut);
process.on("SIGTERM", shut);

async function shut() {
    try {
        await redis.quit();
    } finally {
        process.exit(0);
    }
}