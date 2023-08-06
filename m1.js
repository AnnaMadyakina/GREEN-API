const express = require('express');
const amqp = require('amqplib/callback_api');
const app = express();
require("dotenv").config();

const connectionString = process.env.AMQP_CREDENTIALS;
const queueNameFirst = process.env.AMQP_TOPIC_FIRST;
const queueNameSecond = process.env.AMQP_TOPIC_SECOND;

const logger = require('./logger');

app.use(express.json());

app.post('/process', (req, res) => {
  try {
    const data = req.body;
  
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
    
        channel.assertQueue(queueNameFirst, { durable: false });
    
        const requestId = Date.now().toString();
        const message = JSON.stringify({ ...data, requestId });
    
        channel.sendToQueue(queueNameFirst, Buffer.from(message));
        console.log(`Task sent to RabbitMQ with request ID: ${requestId}`);
        logger.info(`queueName: ${queueNameFirst}, Task sent to RabbitMQ with request ID: ${requestId}`);

        channel.consume(queueNameSecond, (message) => {
          const requestData = JSON.parse(message.content.toString());
          console.log(`Result sent to response queue for request ID: ${requestData.requestId}`);
          logger.info(`queueName: ${queueNameSecond}, Result sent to response queue for request ID: ${requestData.requestId}`);
          if(requestId === requestData.requestId) {
            res.json(requestData);
          }
          channel.ack(message); // Подтверждение успешной обработки задачи
        });
      });
    });
  } catch (error) {
    console.log(error)
    res.sendStatus(401);
  }
})

app.listen(process.env.PORT, () => {
  console.log(`M1 microservice listening on port ${process.env.PORT}`);
});


