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

  const consumer = kafka.consumer({ 
    groupId: 'consumer-opensearch-demo',
    autoOffsetReset: 'earliest',
   });
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

const sendBulkRequest = async (openSearchClient, docsToIndex) => {
  try {
    const bulkResponse = await openSearchClient.bulk({ body: docsToIndex });
    bulkResponse.body.items.forEach((item) => {
      console.error(`document: ${item.index._id} - ${item.index.result} - ${item.index.status}`);
    });

    console.log(`Indexed ${bulkResponse.body.items.length} documents in a bulk request`);
    return bulkResponse;
  } catch (error) {
    console.error('Failed to send bulk request:', error);
    return error
  }
};

const main = async () => {
  const openSearchClient = createOpenSearchClient();
  const consumer = await createKafkaConsumer();

  const indexExists = await openSearchClient.indices.exists({ index: 'wikimedia' });

  if (!indexExists.body) {
    await openSearchClient.indices.create({ index: 'wikimedia' });
    console.log('The Wikimedia Index has been created!');
  } else {
    console.log('The Wikimedia Index already exists');
  }

  await consumer.subscribe({ topic: 'wikimedia.recentchange', fromBeginning: true });

  let docsToIndex = [];
  const bulkThreshold = 2 * 10; // Adjust this value based on your requirements

  let timeoutId;
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      clearTimeout(timeoutId); // Clear any existing timeout

      try {
        const id = extractId(message.value.toString());
        docsToIndex.push({ index: { _index: 'wikimedia', _id: id } });
        docsToIndex.push(JSON.parse(message.value.toString()));

        if (docsToIndex.length >= bulkThreshold) {
          await sendBulkRequest(openSearchClient, docsToIndex);
          docsToIndex = [];
        } else {
          console.log('timeoutBulk')
          timeoutId = setTimeout(async () => {
            await sendBulkRequest(openSearchClient, docsToIndex);
            docsToIndex = [];
          }, 1000);
        }
      } catch (error) {
        console.error('Failed to index document:', error);
      }
    },
  });

  // Add SIGTERM handler
  process.on('SIGTERM', async () => {
    await gracefullyShutdown(consumer, openSearchClient);
  });
  // Existing SIGINT handler
  process.on('SIGINT', async () => {
    await gracefullyShutdown(consumer, openSearchClient);
  });
};

main().catch((error) => console.error('Unexpected exception in the consumer', error));
