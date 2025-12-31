import * as chai from 'chai';
import chaiSorted from 'chai-sorted';

import { Debouncer } from '../debouncer.js';
import {
  consumerToGenerator,
  getOutputConsumer
} from '../test-utils/consumers.js';
import { S3Mocks } from '../test-utils/s3Mocks.js';
import { SQSMocks } from '../test-utils/sqsMocks.js';
import { SQSInputMessageHandler } from '../input-message-handler/sqs.js';
import { MessageMapper } from '../types.js';

chai.use(chaiSorted);
const { expect } = chai;

describe('DebouncedSQS Integration Tests with SQS + S3', () => {
  let sqsMocks: SQSMocks;
  let s3Mocks: S3Mocks;

  before(async () => {
    sqsMocks = new SQSMocks('test-queue', 'test-debounced-queue');
    await sqsMocks.init();

    s3Mocks = new S3Mocks('test-bucket-debounced', 'test-index-debounced');
    await s3Mocks.init();
  });

  after(async () => {
    await sqsMocks.clear();
    await s3Mocks.clear();
  });

  afterEach(async () => {
    await s3Mocks.clearIndexFiles();
  });

  it('should debounce end-to-end - webhook example with input queue', async () => {
    const messageMapper: MessageMapper = {
      mapInputMessage: async ({ webhookId, data }) => {
        return {
          key: webhookId,
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

    const inputMessageHandler = new SQSInputMessageHandler(
      sqsMocks.sqsClient!,
      sqsMocks.sqsQueueUrl!,
      messageMapper,
      s3Mocks.s3Storage
    );

    const inputConsumer = await inputMessageHandler.createInputConsumer();
    // Given
    const debouncer = new Debouncer({
      index: s3Mocks.s3Storage,
      outputQueueUrl: sqsMocks.sqsDebouncedQueueUrl,
      inputMessageHandler,
      messageMapper,
      sqs: sqsMocks.sqsClient!
    });
    const outputConsumer = getOutputConsumer(
      debouncer.sqs,
      debouncer.outputQueueUrl
    );

    // Given - simulate sending messages including duplicates
    const message1 = {
      tenantId: 100,
      webhookId: 4001,
      data: { external_id: 'example' }
    };
    const message1Duplicate = {
      tenantId: 100,
      webhookId: 4001,
      data: { external_id: 'example' }
    };
    const message2 = {
      tenantId: 100,
      webhookId: 7654,
      data: { external_id: 'example' }
    };
    const message3 = {
      tenantId: 987,
      webhookId: 2000,
      data: { external_id: 'example' }
    };
    await debouncer.enqueue(
      [message1, message1Duplicate, message2, message3],
      sqsMocks.sqsQueueUrl
    );

    const { generator: inputMessages, stop: stopInputConsumer } =
      consumerToGenerator(inputConsumer);

    // Wait for input messages to be processed
    inputConsumer.start();
    const processedInputMessages = await Promise.all([
      inputMessages.next().then(parseMessage),
      inputMessages.next().then(parseMessage),
      inputMessages.next().then(parseMessage),
      inputMessages.next().then(parseMessage)
    ]);
    stopInputConsumer();

    // When - Dispatching indexed messages
    await debouncer.dispatchStoredMessages();
    outputConsumer.startOutputConsumer();
    const processedOutputMessages = await Promise.all([
      outputConsumer.outputMessages.next().then(parseMessage),
      outputConsumer.outputMessages.next().then(parseMessage),
      outputConsumer.outputMessages.next().then(parseMessage)
    ]);
    outputConsumer.stopOutputConsumer();

    // Then
    expect(processedInputMessages).to.have.deep.members([
      message1,
      message1Duplicate,
      message2,
      message3
    ]);
    expect(processedOutputMessages).to.have.deep.members([
      message1,
      message2,
      message3
    ]);
  });
});

const parseMessage = ({ value }: { value: any }) => JSON.parse(value.Body);
