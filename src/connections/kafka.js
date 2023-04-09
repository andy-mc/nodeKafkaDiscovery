const { Kafka } = require('kafkajs');
const kafkaNode = require('kafka-node');
const config = require('../config');
const CooperativeStickyAssignor = require("./cooperativeStickyAssignor");

let kafkaInstance;
let kafkaClient;

const getKafkaInstance = (configs) => {
  if (!kafkaInstance) {
    kafkaInstance = new Kafka({
      clientId: 'demo-node-consumer',
      brokers: ['cluster.playground.cdkt.io:9092'],
      ssl: true,
      sasl: {
        mechanism: 'plain',
        username: config.kafka.username,
        password: config.kafka.password,
      },
      customPartitionAssigners: [CooperativeStickyAssignor],
      ...configs,
    });
  }
  return kafkaInstance;
};

const getKafkaClient = (configs) => {
  if (!kafkaClient) {
    console.log('kafkaClient:', kafkaClient)
    kafkaClient = new kafkaNode.KafkaClient({
      clientId: 'demo-node-consumer',
      kafkaHost: 'cluster.playground.cdkt.io:9092',
      ssl: true,
      sasl: {
        mechanism: 'plain',
        username: config.kafka.username,
        password: config.kafka.password,
      },
    });
  }
  console.log('kafkaClient:', kafkaClient)
  return kafkaClient;
};

module.exports = {
  getKafkaInstance,
  getKafkaClient,
};
