// Failed to send message
// Error: Unknown

const { KafkaClient, Producer } = require("kafka-node");
const EventSource = require("eventsource");
const config = require("../../config");

async function run() {
  const client = new KafkaClient({
    kafkaHost: "cluster.playground.cdkt.io:9092",
    clientId: "demo-node-consumer",
    sslOptions: {
      rejectUnauthorized: false,
    },
    sasl: {
      mechanism: "plain",
      username: config.kafka.username,
      password: config.kafka.password,
    },
  });

  const producer = new Producer(client);

  const topic = "wikimedia.recentchange";
  const url = "https://stream.wikimedia.org/v2/stream/recentchange";

  const eventHandler = async (event) => {
    const payload = {
      topic,
      messages: "event.data",
    };

    producer.send([payload], (err) => {
      if (err) {
        // Failed to send message
        // Error: Unknown
        console.error("\n\nFailed to send message");
        console.error(err);
      }
    });
  };

  producer.on("ready", () => {
    console.log('Producer Ready !!')

    const eventSource = new EventSource(url);
    eventSource.onmessage = eventHandler;
    eventSource.onerror = (error) => {
      console.error("\n\nError in Stream Reading");
      console.error("Error in Stream Reading", error);
    };

    // Run the producer for 10 minutes before closing
    setTimeout(() => {
      eventSource.close();
      producer.close();
    }, 5 * 1000);
  });

  producer.on("error", (err) => {
    console.error("\n\nError with Kafka producer");
    console.error(err);
  });
}

run().catch(console.error);
