import * as chai from 'chai';
import chaiSorted from 'chai-sorted';

import { Debouncer } from '../debouncer.js';
import { givenIOConsumers } from '../test-utils/consumers.js';
import { SQSMocks } from '../test-utils/sqsMocks.js';
import { MessageMapper } from '../types.js';
import { DDBMocks } from '../test-utils/ddbMocks.js';
import { parseMessage } from '../test-utils/parseMessage.js';
import { DirectInputMessageHandler } from '../input-message-handler/direct.js';

chai.use(chaiSorted);
const { expect } = chai;

describe('DebouncedSQS Integration Tests with SQS + DDB', () => {
  let sqsMocks: SQSMocks;
  let ddbMocks: DDBMocks;

  before(async () => {
    sqsMocks = new SQSMocks('test-queue', 'test-debounced-queue');
    await sqsMocks.init();

    ddbMocks = new DDBMocks('test-table-debounced', 'test-index-debounced');
    await ddbMocks.init();
  });

  after(async () => {
    await sqsMocks.clear();
    await ddbMocks.clear();
  });

  it('should debounce end-to-end - webhook example with DDB and without input queue', async () => {
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

    const inputMessageHandler = new DirectInputMessageHandler(
      ddbMocks.ddbStorage,
      messageMapper
    );
    // Given
    const debouncer = new Debouncer({
      index: ddbMocks.ddbStorage,
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
