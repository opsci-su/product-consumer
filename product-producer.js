const { Kafka } = require('kafkajs');
const fs = require('fs');
const csv = require('csv-parser');

const BROKER_1 = process.env.BROKER_1 || 'localhost:9092';
const BROKER_2 = process.env.BROKER_2 || 'localhost:9092';
const BROKER_3 = process.env.BROKER_3 || 'localhost:9092';
const TOPIC = process.env.TOPIC || 'product';
const FILE_NAME = process.env.FILE_NAME | "products.txt";
const ERROR_TOPIC = process.env.ERROR_TOPIC || 'errors';

const log = (...str) => console.log(`${new Date().toUTCString()}: `, ...str);

const kafka = new Kafka({
    clientId: 'product-consumer',
    brokers: [BROKER_1, BROKER_2, BROKER_3],
});

const producer = kafka.producer();

async function sendToKafka(topic, key, value) {
  return new Promise((resolve, reject) => {
    producer.send({
      topic,
      messages: [{ key, value }],
    }).then((result) => {
      log(`Message sent successfully: ${key}`);
      resolve(result);
    }).catch((err) => {
      log(`Error sending message: ${err}`);
      reject(err);
    });
  });
}

async function main() {
    if (!FILE_NAME) {
      console.error('File name is missing.');
      return;
    }
  
    await producer.connect();
  
    const sendPromises = [];
  
    const fileStream = fs.createReadStream(FILE_NAME)
      .pipe(csv())
      .on('data', async (row) => {
        const product = {
          name: row['name'],
          description: row['description'],
          stock_available: parseInt(row['stock_available']),
          barcode: parseInt(row['barcode']),
          status: parseInt(row['nb_stock']),
        };
  
        const message = JSON.stringify(product);
        const key = '1';
        log('Sending: ', message);
        const sendPromise = sendToKafka(TOPIC, key, message);
        sendPromises.push(sendPromise);
      })
      .on('end', async () => {
        try {
          log('Waiting for all messages to be sent...');
          await Promise.all(sendPromises);
          log('All messages have been sent');
        } catch (err) {
          console.error('Error sending messages:', err);
        } finally {
          log('Disconnecting producer ...');
          await producer.disconnect();
        }
      });
  
    fileStream.on('error', (err) => {
      console.error('Error reading file:', err);
    });
}

main().catch((err) => {
  console.error(`Error executing the script : ${err}`);
  process.exit(1);
});