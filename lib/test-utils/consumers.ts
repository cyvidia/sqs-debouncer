import { Consumer } from 'sqs-consumer';
import { eventEmitterToGenerator } from './eventEmitterToGenerator.js';
import { SQSClient } from '@aws-sdk/client-sqs';

export const getOutputConsumer = (sqs: SQSClient, outputQueueUrl: string) => {
  const outputConsumer = Consumer.create({
    sqs: sqs,
    queueUrl: outputQueueUrl,
    handleMessage: async () => {}
  });
  const { generator: outputMessages, stop: stopOutputConsumer } =
    consumerToGenerator(outputConsumer);

  return {
    outputMessages,
    startOutputConsumer: () => outputConsumer.start(),
    stopOutputConsumer
  };
};

export const consumerToGenerator = (consumer: Consumer) => {
  const generator = eventEmitterToGenerator(consumer, 'message_processed');

  const stop = () => {
    generator.return();
    consumer.stop();
  };
  consumer.on('error', () => stop());
  return { generator, stop };
};
