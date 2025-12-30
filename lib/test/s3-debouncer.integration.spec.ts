import * as chai from 'chai';
import chaiSorted from 'chai-sorted';

import { Debouncer } from '../debouncer.js';
import { givenIOConsumers } from '../test-utils/consumers.js';
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
      mapInputMessage: async ({ tenantId, webhookId, data }) => {
        return {
          groupId: tenantId,
          entryId: webhookId,
          payload: data
        };
      },
      mapOutputMessages: async (groupId, entriesIds, payloadsByEntryId) => {
        return entriesIds.map((entryId) => {
          return {
            tenantId: Number(groupId),
            webhookId: Number(entryId),
            data: payloadsByEntryId[entryId]
          };
        });
      }
    };

    const inputMessageHandler = new SQSInputMessageHandler(
      sqsMocks.sqsClient!,
      sqsMocks.sqsQueueUrl!,
      messageMapper,
      s3Mocks.uniqueIndex
    );
    // Given
    const debouncer = new Debouncer({
      mode: 'withPayload',
      index: s3Mocks.uniqueIndex,
      inputQueueUrl: sqsMocks.sqsQueueUrl!,
      outputQueueUrl: sqsMocks.sqsDebouncedQueueUrl!,
      inputMessageHandler,
      messageMapper,
      sqs: sqsMocks.sqsClient!
    });
    const consumers = givenIOConsumers(debouncer);

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
      debouncer.inputQueueUrl
    );

    // Wait for input messages to be processed
    consumers.startInputConsumer();
    const processedInputMessages = await Promise.all([
      consumers.inputMessages.next().then(parseMessage),
      consumers.inputMessages.next().then(parseMessage),
      consumers.inputMessages.next().then(parseMessage),
      consumers.inputMessages.next().then(parseMessage)
    ]);
    consumers.stopInputConsumer();

    // When - Dispatching indexed messages
    await debouncer.dispatchStoredMessages();
    consumers.startOutputConsumer();
    const processedOutputMessages = await Promise.all([
      consumers.outputMessages.next().then(parseMessage),
      consumers.outputMessages.next().then(parseMessage),
      consumers.outputMessages.next().then(parseMessage)
    ]);
    consumers.stopOutputConsumer();

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
