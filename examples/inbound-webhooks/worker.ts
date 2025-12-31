import { Consumer } from 'sqs-consumer';
import { SQS_DEBOUNCED_QUEUE_URL } from './debouncedSqs';

const app = Consumer.create({
  queueUrl: SQS_DEBOUNCED_QUEUE_URL,
  handleMessage: async (message) => {
    const { tenantId, webhookId, data } = JSON.parse(message.Body!);

    // Do something with this debounced data
  }
});

app.on('error', (err) => {
  console.error(err.message);
});

app.start();
