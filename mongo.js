import { MongoClient } from "mongodb";

const mongo = {
    isConnected: false
};

async function connect() {
    if (mongo.isConnected) {
        return;
    }

    const client = await MongoClient.connect("mongodb://127.0.0.1:27017/cryptana", {
        maxPoolSize: 10
    });

    const db = client.db();

    mongo.client = client;
    mongo.db = db;

    const requiredCollections = ["signals", "prices", "orderbooks"];
    const existingColls = await db.listCollections().toArray();
    const existingNames = existingColls.map(coll => coll.name);

    for (const collName of requiredCollections) {
        if (!existingNames.includes(collName)) {
            await db.createCollection(collName);
        }
    }

    const colls = await db.listCollections().toArray();

    for (const coll of colls) {
        mongo[coll.name] = db.collection(coll.name);
    }

    mongo.isConnected = true;
}

mongo.connect = connect;

mongo.getCollection = (name) => {
    if (!mongo.isConnected) {
        throw new Error("MongoDB not connected. Call mongo.connect() first.");
    }

    if (mongo[name]) {
        return mongo[name];
    }

    mongo[name] = mongo.db.collection(name);

    return mongo[name];
};

export default mongo;