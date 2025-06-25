import axios from "axios";

export default async function restRequest(config, maxRetries = 5, backoffBaseMs = 500, jitter = true) {
    let attempt = 0;

    while (attempt <= maxRetries) {
        try {
            const response = await axios(config);

            if (response.status === 200) {
                return response;
            }

            console.warn(`Request failed with status ${response.status}. Retrying...`);
        } catch (err) {
            console.warn(`Request error: ${err.message}. Retrying...`);
        }

        attempt++;

        if (attempt > maxRetries) {
            throw new Error(`Failed after ${maxRetries} retries`);
        }

        const delay = jitter
            ? Math.random() * backoffBaseMs * Math.pow(2, attempt)
            : backoffBaseMs * Math.pow(2, attempt);

        await new Promise(resolve => setTimeout(resolve, delay));
    }
}