const log = require('loglevel');
const connections = require('../connections');

log.setLevel('info');

const groupId = 'my-node-application';
const topic = 'wikimedia.recentchange';

const kafka = connections.kafka.getKafkaClient();

const consumer = kafka.consumer({ groupId });

const run = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });

  consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      log.info(`Key: ${message.key?.toString()}, Value: ${message.value.toString()}`);
      log.info(`Partition: ${partition}, Offset: ${message.offset}`);
    },
  });

  process.on('SIGINT', async () => {
    log.info("Detected shutdown, gracefully closing consumer...");
    await consumer.disconnect();
    process.exit(0);
  });

  process.on('SIGTERM', async () => {
    log.info("Detected shutdown, gracefully closing consumer...");
    await consumer.disconnect();
    process.exit(0);
  });
};

run().catch((error) => {
  log.error('Error in the consumer', error);
  process.exit(1);
});
