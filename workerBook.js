import mongo from "./mongo.js";
import restRequest from "./restHelper.js";
import { Worker } from "bullmq";
import IORedis from "ioredis";
import { ObjectId } from "mongodb";

const REST_ENDPOINT = "https://api.gateio.ws/api/v4/spot/order_book";

async function fetchDepth5(pair) {
    const { data } = await restRequest({
        methos: "get",
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

    const bidSum = snapshot.bids.slice(0, 5).reduce((s, [ , q]) => s + Number(q), 0);
    const askSum = snapshot.asks.slice(0, 5).reduce((s, [ , q]) => s + Number(q), 0);
    const imbalance = (bidSum - askSum) / (bidSum + askSum + 1e-8);

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
                bid_sum: bidSum,
                ask_sum: askSum,
                imbalance
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