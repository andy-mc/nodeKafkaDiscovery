const log = require('loglevel');
const connections = require('../connections');

log.setLevel('info');

async function sendMessages(producer, messages) {
  try {
    const [metadata] = await producer.send(messages);
    log.info(`Received new metadata 
    topicName: ${metadata.topicName} 
    partition: ${metadata.partition} 
    errorCode: ${metadata.errorCode} 
    baseOffset: ${metadata.baseOffset} 
    logAppendTime: ${metadata.logAppendTime} 
    logStartOffset: ${metadata.logStartOffset}`);
  } catch (e) {
    log.error('Error while producing', e);
  }
}

async function main() {
  log.info('I am a Kafka Producer!');

  const kafkaClient = connections.kafka.getKafkaClient();
  const producer = kafkaClient.producer();
  await producer.connect();

  for (let i = 0; i < 1; i++) {
    const messages = Array.from({ length: 10 }, (_, idx) => ({
      topic: 'demo_node',
      messages: [{ key: `id_${idx}`, value: `I like pizza ${idx} 111` }],
    }));

    await Promise.all(messages.map(msg => sendMessages(producer, msg)));
    console.log('--------------------------------');
  }

  await producer.disconnect();
}

main();
