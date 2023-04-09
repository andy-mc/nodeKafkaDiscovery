// npm run dev --  src/consumer/wikimedia_consumer.js 

const { Kafka } = require('kafkajs');
const { Client } = require('@opensearch-project/opensearch');
const config = require('../config');

const createOpenSearchClient = () => {
  const connString = config.bonsai.url;
  const openSearchClient = new Client({ node: connString });
  return openSearchClient;
};

const createKafkaConsumer = async () => {
  const kafka = new Kafka({
    clientId: 'demo-node-wiki-consumer-opensearch',
    brokers: ['cluster.playground.cdkt.io:9092'],
    ssl: true,
    sasl: {
      mechanism: 'plain',
      username: config.kafka.username,
      password: config.kafka.password,
    },
  });

  const consumer = kafka.consumer({ groupId: 'consumer-opensearch-demo' });
  await consumer.connect();
  return consumer;
};

const extractId = (json) => {
  const parsed = JSON.parse(json);
  return parsed.meta.id;
};

const gracefullyShutdown = async (consumer, openSearchClient) => {
  console.log('Gracefully shutting down...');
  try {
    // Close the Kafka consumer
    await consumer.disconnect();
    console.log('Kafka consumer disconnected');

    // Close the OpenSearch client
    await openSearchClient.close();
    console.log('OpenSearch client closed');
  } catch (error) {
    console.error('Error during shutdown:', error);
  } finally {
    process.exit(0);
  }
};

const main = async () => {
  const openSearchClient = createOpenSearchClient();
  const consumer = await createKafkaConsumer();
  // Add SIGTERM handler
  process.on('SIGTERM', async () => {
    await gracefullyShutdown(consumer, openSearchClient);
  });
  // Existing SIGINT handler
  process.on('SIGINT', async () => {
    await gracefullyShutdown(consumer, openSearchClient);
  });

  const indexExists = await openSearchClient.indices.exists({ index: 'wikimedia' });

  if (!indexExists.body) {
    await openSearchClient.indices.create({ index: 'wikimedia' });
    console.log('The Wikimedia Index has been created!');
  } else {
    console.log('The Wikimedia Index already exists');
  }

  await consumer.subscribe({ topic: 'wikimedia.recentchange', fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const id = extractId(message.value.toString());
        const indexRequest = {
          index: 'wikimedia',
          id,
          body: JSON.parse(message.value.toString()),
        };
        const response = await openSearchClient.index(indexRequest);
        console.log(`Indexed document with ID: ${response.body._id}`);
      } catch (error) {
        console.error('Failed to index document:', error);
      }
    },
  });
};

main().catch((error) => console.error('Unexpected exception in the consumer', error));
