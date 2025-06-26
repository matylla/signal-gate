// ORDERBOOK IN MONGO. MOST LIKELY ACROSS ALL THE EXCHANGES
// One detail to remember
// bid_sum / ask_sum are token quantities, not USDT notionals.
// That mirrors the original Gate logic, but if you ever compare across symbols you’ll want to normalise:

// const bidVolUSDT = bid_sum * triggerPrice;  // optional post-process step





// 1 Yes — queue depth is per symbol, per side, and varies second-to-second
// BTC-USDT on Binance may show $50 k resting at the best bid right now,
// while a small-cap like JOE-USDT might only have $2 k.
// Depth thins out during Asia night or after big prints and thickens again at
// overlap hours.
// So any “max safe size” must be re-estimated continuously.
// 2 What you actually want to stay below
// Notation    Definition
// Q₁  Quantity (base or quote) already resting at the best price on the side you’ll hit (ask if you buy, bid if you sell).
// tickSize    Smallest price step (e.g. 0.01 USDT on BTC-USDT).
// Safe taker size 0.8 × Q₁ (80 % of visible best-level volume) — gives room for hidden/refresh orders and small queue shifts in the 20 ms between snapshot and order hit.
// If you’re willing to eat one extra tick of slippage, take
// Q₁ + Q₂ (sum of level-1 & level-2).

// 3 How to measure it in code (you already capture books5)
// // running inside SymbolMonitor.updateDepthSnapshot()
// this.bestBidSize  = Number(data.bids[0][1]);  // base size
// this.bestAskSize  = Number(data.asks[0][1]);

// // keep a rolling 1-minute window of level-1 depth
// this.depth1Hist.add(this.isBuyer ? this.bestAskSize : this.bestBidSize);
// if (this.depth1Hist.size > 240) this.depth1Hist.popOldest(); // 1 min @250 ms updates

// // conservative max-notional in USDT
// const depthP25 = percentile(this.depth1Hist.toArray(), 25);   // lower quartile
// this.maxNotional = depthP25 * this.lastPrice * 0.8;           // 80 % guard
// Use this.maxNotional when sizing the order for that symbol.
// Update it every time you call performPeriodicCalculations().

// 4 Quick rule-of-thumb table (spot, Binance depth-1 during EU-US overlap)
// Symbol  Median depth-1  25-th percentile    Safe taker size (80 % × P25)
// BTC-USDT    $80 k   $55 k   $44 k
// ETH-USDT    $35 k   $22 k   $18 k
// Large-cap alt (SOL) $8 k    $5 k    $4 k
// Mid-cap (UNI)   $3 k    $1.8 k  $1.4 k
// Small-cap (JOE) $0.9 k  $0.5 k  $0.4 k
// (These are illustrative 2025 figures; harvest your own percentiles.)

// 5 Terminology recap
// Jargon  Plain talk
// Queue depth Visible size sitting ahead of you in the limit-order queue at a given price.
// Print   One trade report that hits the public tape. “$50 k/print” ≈ there’s usually $50 k available at the best price before a new print occurs at a worse price.
// 6 Take-aways for your sizing logic
// Derive depth stats per symbol in real time.
// Keep a rolling P25 or P10 of level-1 depth and size at ~80 % of that.
// Re-compute after each fill (the act of filling often thins the queue).
// For block-time venues (Phoenix/OpenBook) use per-slot snapshots—the depth you see in one slot is exactly what you’ll hit in that slot.
// Stay inside those dynamic limits and your realised slippage remains linear; exceed them and your edge decays sharply because you start walking the book.

// You said:
// so maxNotional is is the maximum order in USDT I can put per symbol, right?
// ChatGPT said:
// **Yes. maxNotional is meant as a per-symbol, per-side cap—expressed in quote currency (USDT for -USDT books)—on how large a single taker order you send.

// What it represents
// maxNotional  ≈  0.8 × 25th-percentile(level-1 queue depth) × lastPrice
// Per symbol – computed from that market’s bid or ask queue.
// Per side – if you’re about to buy, you look at ask depth; if you’ll sell, you look at bid depth.
// In USDT – because we multiply the depth size (base-asset units) by lastPrice.
// So on BTC-USDT:

// Snapshot field  Example Explanation
// data.asks[0][1] 0.420 BTC   Size resting at best ask
// lastPrice   104 000 USDT    Mid/last trade price
// 25-th pct depth¹    0.35 BTC    From rolling window
// maxNotional 0.8 × 0.35 × 104 000 ≈ 29 k USDT    Safe single order size
// How you use it
// const desiredNotional = 45_000;            // what your model wants
// const sizeToSend = Math.min(desiredNotional, this.maxNotional) / this.lastPrice;
// sendMarketBuy(sizeToSend);
// Keeps each fill inside liquidity that is very likely to exist at the top of the book, thus preserving your expected bps edge.

// ¹ You can pick a different percentile (P10 or median) or a different multiplier (60 %, 100 %) based on your risk tolerance.






import mongo from "./mongo.js";

(async () => {
    await mongo.connect();
})();

import IORedis from "ioredis";
import Websockets from "./websockets.js";
import SymbolMonitor from "./symbolMonitor.js";
import params from "./parameters.js";
import fetchBestPairs from "./pairSelector.js";
import { initPriceTape, handleTradeTick, shutdownPriceTape } from "./priceTape.js";

import "./workerBook.js";
import "./workerPrice.js";

const redis = new IORedis({ maxRetriesPerRequest: null });

initPriceTape(redis);

const symbolMonitors = new Map();

let gateStream;
let signalCheckIntervalId;

function processCombinedStreamData(rawMessage) {
    try {
        const message = JSON.parse(rawMessage);

        if (!message.stream || !message.data) {
            return;
        }

        const streamName = message.stream;
        const data = message.data;

        const firstAt = streamName.indexOf("@");

        if (firstAt === -1) {
            return;
        }

        const symbol = streamName.substring(0, firstAt).toUpperCase();
        const streamType = streamName.substring(firstAt + 1);
        const monitor = symbolMonitors.get(symbol);

        if (!monitor) {
            return;
        }

        switch (streamType) {
            case "aggTrade": {
                monitor.addAggTrade(data);

                const price = parseFloat(data.p);
                const volumeU = parseFloat(data.q) * price;
                const tsMs = data.E;

                handleTradeTick(symbol, price, volumeU, tsMs);
                break;
            }

            case "ticker":
                monitor.applyTickerUpdate(data);
                break;

            case "bookTicker":
                monitor.applyBookTickerUpdate(data);
                break;

            case "depth5@100ms":
                monitor.updateDepthSnapshot(data);
                break;
        }
    } catch { }
}

async function start() {
    console.log("Starting Cryptana Gate");

    const initialPairs = await fetchBestPairs();

    if (!initialPairs.length) {
        console.error("FATAL: Pair selection returned no symbols. Cannot start.");
        process.exit(1);
    }

    console.log(`Found ${initialPairs.length} pairs to monitor for this session.`);

    initialPairs.forEach(pairData => {
        symbolMonitors.set(pairData.symbol, new SymbolMonitor(pairData.symbol, pairData.tier));
    });

    const initialStreams = initialPairs.flatMap(p => [
        `${p.symbol.toLowerCase()}@aggTrade`,
        `${p.symbol.toLowerCase()}@ticker`,
        `${p.symbol.toLowerCase()}@bookTicker`,
        `${p.symbol.toLowerCase()}@depth5@100ms`
    ]);

    gateStream = new Websockets();
    gateStream.on("message", processCombinedStreamData);
    gateStream.on("error", (err) => console.error("WebSocket Error:", err));
    gateStream.on("close", ()  => console.warn("WebSocket closed. Process restart required."));
    gateStream.connect(initialStreams);

    signalCheckIntervalId = setInterval(() => {
        for (const m of symbolMonitors.values()) {
            m.performPeriodicCalculations();
            m.checkSignal();
        }
    }, params.CHECK_SIGNAL_INTERVAL_MS);

    console.log(`System started and is now monitoring ${symbolMonitors.size} pairs.`);
}

async function stop() {
    console.log("Shutting down…");
    clearInterval(signalCheckIntervalId);

    if (gateStream) {
        gateStream.disconnect();
    }

    shutdownPriceTape();

    try {
        await redis.quit();
    } catch {}

    console.log("Shutdown complete.");
    process.exit(0);
}

process.on("SIGINT", stop);
process.on("SIGTERM", stop);

start();