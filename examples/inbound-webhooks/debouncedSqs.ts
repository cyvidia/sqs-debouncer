import { S3Client } from '@aws-sdk/client-s3';
import { SQSClient } from '@aws-sdk/client-sqs';

import { Debouncer } from '../../lib/debouncer.js';
import { S3Storage } from '../../lib/unique-index/connectors/s3.js';
import { MessageMapper } from '../../lib/types.js';
import { DirectInputMessageHandler } from '../../lib/input-message-handler/direct.js';

const s3Client = new S3Client({
  region: 'your-region'
});
const S3_BUCKET_NAME = 'your-s3-bucket-name';

const sqsClient = new SQSClient({
  region: 'your-region'
});
export const SQS_QUEUE_URL = 'your-sqs-queue-url';
export const SQS_DEBOUNCED_QUEUE_URL = 'your-sqs-debounced-queue-url';

const index = new S3Storage(s3Client, S3_BUCKET_NAME);
const messageMapper: MessageMapper = {
  mapInputMessage: async ({ webhookId, data }) => {
    return {
      key: String(webhookId),
      payload: data
    };
  },
  mapOutputMessages: async (entries) => {
    return entries.map((entry) => {
      return {
        webhookId: Number(entry.key),
        data: entry.payload
      };
    });
  }
};

const debouncer = new Debouncer({
  index: new S3Storage(s3Client, S3_BUCKET_NAME),
  inputMessageHandler: new DirectInputMessageHandler(index),
  outputQueueUrl: SQS_DEBOUNCED_QUEUE_URL,
  messageMapper,
  sqs: sqsClient
});

export default debouncer;
