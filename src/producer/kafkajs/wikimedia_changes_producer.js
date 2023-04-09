const { Kafka } = require("kafkajs");
const EventSource = require("eventsource");
const config = require('../../config');

async function run() {
  const kafka = new Kafka({
    clientId: 'demo-node-wiki-producer',
    brokers: ['cluster.playground.cdkt.io:9092'],
    ssl: true,
    sasl: {
      mechanism: 'plain',
      username: config.kafka.username,
      password: config.kafka.password,
    },
  });

  const producer = kafka.producer({
    idempotent: true,
    acks: -1,
    retries: Number.MAX_SAFE_INTEGER,
    compression: 'snappy',
    linger: 20,
    batchSize: 32 * 1024,
  });

  const topic = "wikimedia.recentchange";
  const url = "https://stream.wikimedia.org/v2/stream/recentchange";

  const eventHandler = async (event) => {
    console.log(event.data);
    await producer.send({
      topic,
      messages: [{ value: event.data }],
    });
  };

  await producer.connect();

  const eventSource = new EventSource(url);
  eventSource.onmessage = eventHandler;
  eventSource.onerror = (error) => {
    console.error("Error in Stream Reading", error);
  };

  // Graceful shutdown
  const shutdown = async () => {
    console.log("Shutting down gracefully...");

    eventSource.close();
    await producer.disconnect();

    console.log("Producer finished.");
    process.exit(0);
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);

  // Run the producer for 1 second before closing
  await new Promise((resolve) => {
    setTimeout(async () => {
      eventSource.close();
      await producer.disconnect();
      resolve();
    }, 2 * 1000);
  });
}

run()
.then(() => {
  console.log("Producer finished.");
})
.catch(console.error);
