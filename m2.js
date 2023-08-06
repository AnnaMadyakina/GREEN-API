const amqp = require('amqplib/callback_api');
require("dotenv").config();

const connectionString = process.env.AMQP_CREDENTIALS;
const queueName = process.env.AMQP_TOPIC_FIRST;
const responseQueue = process.env.AMQP_TOPIC_SECOND;

const logger = require('./logger');

amqp.connect(connectionString, (error, connection) => {
  if (error) {
    console.error(error);
    logger.info(`Error: ${error}`);
    return;
  }

  connection.createChannel((error, channel) => {
    if (error) {
      console.error(error);
      logger.info(`Close connection: ${error}`);
      connection.close();
      return;
    }

    channel.assertQueue(queueName, { durable: false });
    channel.assertQueue(responseQueue, { durable: false });

    channel.consume(queueName, (message) => {
      const requestData = JSON.parse(message.content.toString());

      console.log(`Result sent to response queue for request ID: ${requestData.requestId}`);
      logger.info(`queueName: ${queueName}, Result sent to response queue for request ID: ${requestData.requestId}`);
      // Обработка задачи и получение результата
      const result = {...requestData, message: 'Processed successfully'};

      channel.sendToQueue(responseQueue, Buffer.from(JSON.stringify(result)));
      console.log(`Task sent to RabbitMQ with request ID: ${JSON.stringify(result)}`);
      logger.info(`queueName: ${responseQueue}, Task sent to RabbitMQ with request ID: ${JSON.stringify(result)}`);
      channel.ack(message); // Подтверждение успешной обработки задачи
    });
  });
});

