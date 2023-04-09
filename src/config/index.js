const dotenv = require('dotenv');
const env = process.env.NODE_ENV || 'dev';

dotenv.config({ path: `.env.${env}` });

module.exports = {
  kafka: {
    username: process.env.KAFKA_USERNAME,
    password: process.env.KAFKA_PASSWORD,
  },
  bonsai: {
    url: process.env.BONSAI_URL,
  }
}