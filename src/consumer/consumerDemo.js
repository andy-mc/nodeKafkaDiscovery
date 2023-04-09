const log = require('loglevel');
const connections = require('../connections');

log.setLevel('info');

async function main() {
  log.info('I am a Kafka Consumer!');

  const groupId = 'billing';
  const topic = 'demo_node';

  // get kafka client
  const kafkaClient = connections.kafka.getKafkaClient();

  // create a consumer
  const consumer = kafkaClient.consumer({ groupId });

  // subscribe to a topic
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: false });

  log.info('Polling');

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      log.info(`Key: ${message.key?.toString()}, Value: ${message.value.toString()}`);
      log.info(`Partition: ${partition}, Offset: ${message.offset}`);
    },
  }, {
    // fetch: {
    //   maxWaitTimeInMs: 500, // Adjust this value to control the frequency of fetching new messages
    //   minBytes: 1,
    // },
  });
}

main().catch((error) => {
  log.error('Error in the consumer', error);
  process.exit(1);
});
