import sinon from 'sinon';
import { Debouncer } from '../debouncer.js';
import { DebouncerOptions, IndexedStorage } from '../types.js';

describe('DebouncedSQS', () => {
  it('should load entries payloads when mode is DISPATCH_MODE_WITH_PAYLOAD', async () => {
    // Given
    const { payload1, payload2, index, loadEntriesPayloads } =
      givenStubbedIndex();

    const mapMessagesSpy = sinon.stub();
    const debouncer = new Debouncer({
      mode: 'withPayload',
      index,
      messageMapper: {
        mapOutputMessages: mapMessagesSpy,
        mapInputMessage: sinon.stub()
      }
    } as unknown as DebouncerOptions);
    const enqueueStub = sinon.stub(debouncer, 'enqueue').resolves();

    // When
    await debouncer.dispatchStoredMessages();

    // Then
    sinon.assert.callCount(loadEntriesPayloads, 1);
    sinon.assert.calledWith(mapMessagesSpy, 'group1', ['entry1', 'entry2'], {
      entry1: payload1,
      entry2: payload2
    });
    sinon.assert.callCount(enqueueStub, 1);
  });

  it('should not load entries payloads when mode is DISPATCH_MODE_ONLY_IDS', async () => {
    // Given
    const { index, loadEntriesPayloads } = givenStubbedIndex();

    const mapMessagesSpy = sinon.stub();
    const debouncer = new Debouncer({
      mode: 'onlyIds',
      index,
      messageMapper: {
        mapOutputMessages: mapMessagesSpy,
        mapInputMessage: sinon.stub()
      }
    } as unknown as DebouncerOptions);
    const enqueueStub = sinon.stub(debouncer, 'enqueue').resolves();

    // When
    await debouncer.dispatchStoredMessages();

    // Then
    sinon.assert.callCount(loadEntriesPayloads, 0);
    sinon.assert.calledWith(mapMessagesSpy, 'group1', ['entry1', 'entry2'], {});
    sinon.assert.callCount(enqueueStub, 1);
  });
});

const givenStubbedIndex = () => {
  const groupId = 'group1';
  const entryId1 = 'entry1';
  const entryId2 = 'entry2';
  const payload1 = { data: 'payload1' };
  const payload2 = { data: 'payload2' };

  function* listGroups() {
    yield [groupId];
  }
  function* listEntries() {
    yield [entryId1, entryId2];
  }

  const loadEntriesPayloads = sinon.spy(() => ({
    entry1: payload1,
    entry2: payload2
  }));
  const index = {
    listEntries,
    listGroups,
    loadEntriesPayloads
  } as unknown as IndexedStorage;

  return {
    groupId,
    entryId1,
    entryId2,
    payload1,
    payload2,
    index,
    loadEntriesPayloads
  };
};
