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