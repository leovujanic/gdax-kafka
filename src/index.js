const Gdax = require("gdax");
const Kafka = require("node-rdkafka");
const products = [
    "BCH-BTC",
    "BCH-USD",
    "BTC-EUR",
    "BTC-GBP",
    "BTC-USD",
    "ETH-BTC",
    "ETH-EUR",
    "ETH-USD",
    "LTC-BTC",
    "LTC-EUR",
    "LTC-USD",
    "BCH-EUR",
];


const stream = Kafka.Producer.createWriteStream({
    "metadata.broker.list": "localhost:9092",
    "request.required.acks": 0,
    "queue.buffering.max.messages": 100000,
    "compression.codec": "gzip",
    "retry.backoff.ms": 100,
    "message.send.max.retries": 10,
    "delivery.report.only.error": true,
    "socket.keepalive.enable": true,
    "queue.buffering.max.ms": 500,
    "batch.num.messages": 10000,
    "dr_cb": true
}, {}, {
    topic: "gdax"
});


const websocket = new Gdax.WebsocketClient(products);

websocket.on("message", data => {
    if (data.type && data.type !== "subscriptions") {
        stream.write(new Buffer(JSON.stringify(data)));
    }
});


websocket.on("error", err => {
    throw new Error("GDAX stream error");
});

websocket.on("close", () => {
    throw new Error("GDAX stream closed");
});

stream.on("error", function (err) {
    console.error(err);
    throw new Error("Error in our kafka stream");
});


process.on("SIGINT", function () {
    stream.close();

    if (i_should_exit)
        process.exit();
});
