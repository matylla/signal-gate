import WebSocket from "ws";
import EventEmitter from "events";

const WS_URL = "wss://api.gateio.ws/ws/v4/";
const MAX_TOPICS_PER_WS = 180;
const PING_INTERVAL_MS = 20_000;
const RECONNECT_MS = 2_000;

function mapChannelToTopic(ch) {
    const at = ch.indexOf("@");

    if (at === -1) {
        return null;
    }

    const symbol = ch.slice(0, at).toUpperCase();
    const stream = ch.slice(at + 1).toLowerCase();

    switch (stream) {
        case "aggtrade":
            return {
                channel: "spot.trades",
                payload: [symbol]
            };

        case "ticker":
            return {
                channel: "spot.tickers",
                payload: [symbol]
            };

        case "bookticker":
            return {
                channel: "spot.book_ticker",
                payload: [symbol]
            };

        case "depth5@100ms":
            return {
                channel: "spot.order_book",
                payload: [symbol, "5", "100ms"]
            };

        default: return null;
    }
}

function translateGate(msg) {
    const out = [];

    if (!msg || msg.event !== "update" || !msg.channel) {
        return out;
    }

    if (msg.channel === "spot.trades") {
        const payload = msg.result;

        if (!payload) {
            return out;
        }

        const trades = Array.isArray(payload) ? payload : [payload];

        for (const t of trades) {
            const pair = t.currency_pair || t.s;

            if (!pair) {
                continue;
            }

            const symbolLc = pair.toLowerCase();

            out.push({
                stream: `${symbolLc}@aggTrade`,
                data: {
                    p: t.price,
                    q: t.amount,
                    E: Number(t.create_time_ms || t.create_time * 1000),
                    m: t.side === "sell"
                }
            });
        }
    }

    else if (msg.channel === "spot.tickers") {
        const d = msg.result;

        if (!d) {
            return out;
        }

        const pair = d.currency_pair || d.s;

        if (!pair) {
            return out;
        }

        const symbolLc = pair.toLowerCase();

        out.push({
            stream: `${symbolLc}@ticker`,
            data: {
                q: d.quote_volume,
                P: (parseFloat(d.change_percentage) || 0).toFixed(4),
                h: d.high_24h,
                l: d.low_24h,
                c: d.last
            }
        });
    }

    else if (msg.channel === "spot.book_ticker") {
        const d = msg.result;

        if (!d) {
            return out;
        }

        const pair = d.currency_pair || d.s;

        if (!pair) {
            return out;
        }

        const symbolLc = pair.toLowerCase();

        out.push({
            stream: `${symbolLc}@bookTicker`,
            data: {
                b: d.highest_bid,
                a: d.lowest_ask
            }
        });
    }

    else if (msg.channel === "spot.order_book") {
        const d = msg.result;

        if (!d) {
            return out;
        }

        const pair = d.currency_pair || d.s;

        if (!pair) {
            return out;
        }

        const symbolLc = pair.toLowerCase();

        out.push({
            stream: `${symbolLc}@depth5@100ms`,
            data: {
                bids: (d.bids || []).slice(0, 5),
                asks: (d.asks || []).slice(0, 5)
            }
        });

        if (d.bids?.length && d.asks?.length) {
            out.push({
                stream: `${symbolLc}@bookTicker`,
                data: {
                    b: d.bids[0][0],
                    a: d.asks[0][0]
                }
            });
        }
    }

    return out;
}

export default class Websockets extends EventEmitter {
    constructor(opts = {}) {
        super();
        this.reconnectMs = opts.reconnectIntervalMs ?? RECONNECT_MS;
        this.wsArr = [];
        this.pingTimers = new Map();
    }

    connect(binanceStyleChannels = []) {
        this.disconnect();

        const topics = binanceStyleChannels.map(mapChannelToTopic).filter(Boolean);

        if (!topics.length) {
            console.warn("[GateWS] No valid topics – nothing to subscribe.");
            return;
        }

        for (let i = 0; i < topics.length; i += MAX_TOPICS_PER_WS) {
            const chunk = topics.slice(i, i + MAX_TOPICS_PER_WS);
            this._spawnSocket(chunk);
        }

        console.log(`[GateWS] Spawned ${Math.ceil(topics.length / MAX_TOPICS_PER_WS)} connection(s) for ${topics.length} topics.`);
    }

    _spawnSocket(topicChunk) {
        const ws = new WebSocket(WS_URL);
        this.wsArr.push(ws);

        ws.on("open", () => {
            console.log("[GateWS] open; subscribing", topicChunk.length, "topics");

            for (let i = 0; i < topicChunk.length; i += 10) {
                const slice = topicChunk.slice(i, i + 10);

                for (const t of slice) {
                    ws.send(JSON.stringify({
                        time: Math.floor(Date.now() / 1000),
                        channel: t.channel,
                        event: "subscribe",
                        payload: t.payload
                    }));
                }
            }

            const pingId = setInterval(() => {
                if (ws.readyState === WebSocket.OPEN) {
                    ws.send(JSON.stringify({ time: Math.floor(Date.now() / 1000), channel: "spot.ping" }));
                }
            }, PING_INTERVAL_MS);

            this.pingTimers.set(ws, pingId);
                this.emit("open");
            });

            ws.on("message", raw => {
                let msg;

                try {
                    msg = JSON.parse(raw.toString());
                } catch (err) {
                    this.emit("error", err);
                    return;
                }

                if (msg.error) {
                    console.error("[GateWS] error", msg.error);
                }

                for (const m of translateGate(msg)) {
                    this.emit("message", JSON.stringify(m));
                }
            });

            ws.on("error", (err) => {
                this.emit("error", err);
            });

            ws.on("close", () => {
                clearInterval(this.pingTimers.get(ws));
                this.pingTimers.delete(ws);
                this.emit("close");

                setTimeout(() => {
                    this._spawnSocket(topicChunk);
                }, this.reconnectMs);
        });
    }

    disconnect() {
        for (const ws of this.wsArr) {
            clearInterval(this.pingTimers.get(ws));
            ws.removeAllListeners();

            if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING) {
                ws.close();
            }
        }

        this.wsArr = [];
        this.pingTimers.clear();
        this.emit("disconnected");
    }
}