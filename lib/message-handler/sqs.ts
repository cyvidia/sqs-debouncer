import { SendMessageCommand, SQSClient } from '@aws-sdk/client-sqs';
import { Consumer } from 'sqs-consumer';
import {
  IndexedStorage,
  IndexEntry,
  InputMessageHandler,
  MessageMapper
} from '../types.js';

export class SQSInputMessageHandler implements InputMessageHandler {
  constructor(
    private readonly sqs: SQSClient,
    private inputQueueUrl: string,
    private readonly messageMapper: MessageMapper,
    private readonly index: IndexedStorage
  ) {}

  async createInputConsumer(): Promise<Consumer> {
    const handleMessage = async (message: any) => {
      const { groupId, entryId, payload } =
        await this.messageMapper.mapInputMessage(JSON.parse(message.Body));

      await this.index.add(groupId, entryId, payload);
    };

    return Consumer.create({
      sqs: this.sqs,
      queueUrl: this.inputQueueUrl,
      batchSize: 10,
      handleMessageBatch: async (messages) => {
        const results = await Promise.all(
          messages.map(async (message) => {
            try {
              await handleMessage(message);
              return message;
            } catch (error) {
              console.error(error);
            }
          })
        );

        const successful = results.filter(Boolean);
        return successful;
      }
    });
  }

  async handleMessage(input: IndexEntry): Promise<void> {
    await this.sqs.send(
      new SendMessageCommand({
        QueueUrl: this.inputQueueUrl,
        MessageBody: JSON.stringify(input)
      })
    );
  }
}
