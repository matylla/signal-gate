import mongo from "./mongo.js";
import restRequest from "./restHelper.js";
import { Worker } from "bullmq";
import IORedis from "ioredis";
import { ObjectId } from "mongodb";

const REST_ENDPOINT = "https://api.gateio.ws/api/v4/spot/order_book";

async function fetchDepth5(pair) {
    const { data } = await restRequest({
        method: "get",
        url: REST_ENDPOINT,
        params: {
            currency_pair: pair,
            limit: 5,
            with_id: false
        }
    });

    return data;
}

new Worker("gate_order", async (job) => {
    const { symbol, id, tOffset } = job.data;

    let snapshot;

    try {
        snapshot = await fetchDepth5(symbol);
    } catch (err) {
        console.error("[workerBook] REST error", err?.response?.status || "", err?.message);
        return;
    }

    // Calculate sums in token amounts
    const bidSum = snapshot.bids.slice(0, 5).reduce((s, [, q]) => s + Number(q), 0);
    const askSum = snapshot.asks.slice(0, 5).reduce((s, [, q]) => s + Number(q), 0);

    // Get best bid and ask prices from the orderbook
    const bestBid = snapshot.bids[0] ? Number(snapshot.bids[0][0]) : 0;
    const bestAsk = snapshot.asks[0] ? Number(snapshot.asks[0][0]) : 0;

    // Calculate midpoint price for normalization
    const midPrice = (bestBid + bestAsk) / 2;

    // Calculate USDT normalized values
    const bidSumUsdt = bidSum * midPrice;
    const askSumUsdt = askSum * midPrice;
    const totalUsdt = bidSumUsdt + askSumUsdt;

    // Calculate both raw and normalized imbalances
    const imbalance = (bidSum - askSum) / (bidSum + askSum + 1e-8);
    const imbalanceUsdt = (bidSumUsdt - askSumUsdt) / (totalUsdt + 1e-8);

    // Update MongoDB with both raw and normalized values
    await mongo.orderbooks.updateOne({
        signal_id: new ObjectId(id)
    }, {
        $set: {
            symbol
        },
        $push: {
            snapshots: {
                t_offset_s: tOffset,
                ts: Date.now(),
                // Raw token amounts (backward compatibility)
                bid_sum: bidSum,
                ask_sum: askSum,
                imbalance: imbalance,
                // NEW: USDT normalized values
                bid_sum_usdt: bidSumUsdt,
                ask_sum_usdt: askSumUsdt,
                total_liquidity_usdt: totalUsdt,
                imbalance_usdt: imbalanceUsdt,
                // Price reference
                mid_price: midPrice,
                best_bid: bestBid,
                best_ask: bestAsk,
                spread_bps: ((bestAsk - bestBid) / bestAsk) * 10000
            }
        }
    }, {
        upsert: true
    });
}, {
    connection: new IORedis({
        maxRetriesPerRequest: null
    })
});