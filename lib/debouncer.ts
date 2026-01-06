import {
  SQSClient,
  SendMessageBatchCommand,
  SendMessageBatchRequestEntry
} from '@aws-sdk/client-sqs';
import { nanoid } from 'nanoid';
import pLimit from 'p-limit';

import {
  MessageMapper,
  DebouncerOptions,
  MessagePayload,
  InputMessageHandler,
  IndexedStorage
} from './types.js';

export class Debouncer {
  public sqs: SQSClient;
  public outputQueueUrl: string;
  private messageMapper: MessageMapper;
  private index: IndexedStorage;
  private inputMessageHandler: InputMessageHandler;

  /**
   * Creates debouncer. See {@link createInputConsumer} on how to listen for input messages.
   * See {@link dispatchStoredMessages} on how to send messages to the output queue (e.g. from a cron trigger).
   */
  constructor(options: DebouncerOptions) {
    this.sqs = options.sqs;
    this.outputQueueUrl = options.outputQueueUrl;
    this.messageMapper = options.messageMapper;
    this.index = options.index;
    this.inputMessageHandler = options.inputMessageHandler;
  }

  async ingest(rawEvent: unknown) {
    const { key, payload } = await this.messageMapper.mapInputMessage(rawEvent);

    await this.inputMessageHandler.handleMessage({
      key,
      payload
    });
  }

  /**
   * Call this method at the rate you wish to debounce events. Example:
   *
   * ```
   * // Runs every hour at the start of the hour
   * cron.schedule("0 * * * *", async () => {
   *     // Push out all deduped messages received so far
   *     debouncer.dispatchStoredMessages();
   * });
   * ```
   */
  async dispatchStoredMessages() {
    for await (const entries of this.index.list()) {
      const messages = await this.messageMapper.mapOutputMessages(entries);
      await this.enqueue(messages, this.outputQueueUrl);
    }
    await this.index.clear();
  }

  async enqueue(messages: MessagePayload[], queueUrl: string) {
    const chunkMessages = (
      messages: SendMessageBatchRequestEntry[]
    ): Array<SendMessageBatchRequestEntry[]> => {
      const chunks: Array<SendMessageBatchRequestEntry[]> = [];
      for (let i = 0; i < messages.length; i += 10) {
        chunks.push(messages.slice(i, i + 10));
      }
      return chunks;
    };

    const sqsMessages = messages.map<SendMessageBatchRequestEntry>(
      (message) => ({
        Id: nanoid(),
        MessageBody: JSON.stringify(message)
      })
    );

    const limit = pLimit(5);
    await Promise.all(
      chunkMessages(sqsMessages).map((chunk) =>
        limit(async () => {
          const params = {
            QueueUrl: queueUrl,
            Entries: chunk
          };
          await this.sqs.send(new SendMessageBatchCommand(params));
        })
      )
    );
  }
}
