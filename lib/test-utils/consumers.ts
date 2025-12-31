import { Consumer } from 'sqs-consumer';
import { eventEmitterToGenerator } from './eventEmitterToGenerator.js';
import { Debouncer } from '../debouncer.js';

export const givenIOConsumers = (debouncer: Debouncer) => {
  const inputConsumer = debouncer.createInputConsumer();
  const { generator: inputMessages, stop: stopInputConsumer } =
    consumerToGenerator(inputConsumer);

  const outputConsumer = Consumer.create({
    sqs: debouncer.sqs,
    queueUrl: debouncer.outputQueueUrl,
    handleMessage: async () => {}
  });
  const { generator: outputMessages, stop: stopOutputConsumer } =
    consumerToGenerator(outputConsumer);

  return {
    inputMessages,
    startInputConsumer: () => inputConsumer.start(),
    stopInputConsumer,
    outputMessages,
    startOutputConsumer: () => outputConsumer.start(),
    stopOutputConsumer
  };
};

export const consumerToGenerator = (consumer) => {
  const generator = eventEmitterToGenerator(consumer, 'message_processed');

  const stop = () => {
    generator.return();
    consumer.stop();
  };
  consumer.on('error', () => stop());
  return { generator, stop };
};
