import axios from "axios";
import params from "./parameters.js";

const COINGECKO_API_URL = "https://api.coingecko.com/api/v3/coins/markets";
const GATE_PAIRS_URL = "https://api.gateio.ws/api/v4/spot/currency_pairs";

const PAIRS_TO_SELECT = 150;
const STABLECOIN_SYMBOLS = new Set(["USDC","USDT","DAI","TUSD","USDP","FDUSD","GUSD","FRAX","USDD","PYUSD","PAXG","USDE","EURS","USDX","OUSD","BUSD"]);

const MIN_24H_VOLUME = 1_000_000;
const MIN_MARKET_CAP = 50_000_000;

function classifyTier(marketCap) {
    if (marketCap >= params.TIER_LARGE_CAP_MIN_MARKET_CAP) return "large";
    if (marketCap >= params.TIER_MID_CAP_MIN_MARKET_CAP)   return "mid";
    return "small";
}

export default async function fetchBestPairs() {
    console.log("[pairSelector] Fetching Gate listings + CoinGecko stats");

    let gatePairs;

    try {
        const res = await axios.get(GATE_PAIRS_URL, {
            timeout: 10_000
        });

        gatePairs = res.data;
    } catch (err) {
        console.error("[pairSelector] Cannot fetch Gate pairs", err.message);
        return [];
    }

    const tradableUsdtPairs = new Set(
        gatePairs
            .filter(p => p.quote === "USDT" && p.trade_status === "tradable")
            .map(p => p.id)
    );

    let gecko;

    try {
        const res = await axios.get(COINGECKO_API_URL, {
            params: {
                vs_currency: "usd",
                per_page: 250,
                page: 1
            },
            timeout: 10_000
        });

        gecko = res.data;
    } catch (err) {
        console.error("[pairSelector] CoinGecko fetch failed", err.message);
        return [];
    }

    const selected = gecko.map(coin => ({
        ...coin,
        gateSymbol: `${coin.symbol.toUpperCase()}_USDT`
    }))
    // Listed on Gate & tradable
    .filter(c => tradableUsdtPairs.has(c.gateSymbol))
    // Exclude stablecoins
    .filter(c => !STABLECOIN_SYMBOLS.has(c.symbol.toUpperCase()))
    // Liquidity floor
    .filter(c => c.total_volume > MIN_24H_VOLUME)
    // Market‑cap floor
    .filter(c => c.market_cap >= MIN_MARKET_CAP)
    // Rank by absolute 24 h % move (volatility proxy)
    .sort((a, b) => Math.abs(b.price_change_percentage_24h) - Math.abs(a.price_change_percentage_24h))
    .slice(0, PAIRS_TO_SELECT)
    .map(c => ({
        symbol: c.gateSymbol,
        tier: classifyTier(c.market_cap)
    }));

    console.log(`[pairSelector] Selected ${selected.length} pairs for session.`);
    return selected;
}