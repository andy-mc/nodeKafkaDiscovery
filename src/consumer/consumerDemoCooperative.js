const { Kafka } = require("kafkajs");
const connections = require('../connections');


const groupId = "my-nodejs-application";
const topic = "demo_node";

// get kafka client
const kafkaClient = connections.kafka.getKafkaClient();

// Create a consumer
const consumer = kafkaClient.consumer({ groupId });

const run = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: false });

  consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        key: message.key ? message.key.toString() : null,
        value: message.value ? message.value.toString() : null,
        partition,
        offset: message.offset,
      });
    },
  });
  
};

const shutdown = async () => {
  console.log("Detected a shutdown, gracefully closing the consumer...");
  await consumer.disconnect();
};

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

run().catch((error) => console.error("Unexpected exception in the consumer", error));
