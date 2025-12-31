import { expect } from 'chai';
import { DDBMocks } from '../test-utils/ddbMocks.js';

describe('ddbConnector Integration Tests with S3', () => {
  let ddbMocks: DDBMocks;

  before(async () => {
    ddbMocks = new DDBMocks('test-table', 'test-index');
    await ddbMocks.init();
  });

  after(async () => {
    await ddbMocks.clear();
  });

  it('should add and list entries correctly', async () => {
    const key1 = 'entry1';
    const key2 = 'entry2';
    const payload1 = { data: 'payload1' };
    const payload2 = { data: 'payload2' };

    await ddbMocks.ddbStorage.put(key1, payload1);
    await ddbMocks.ddbStorage.put(key2, payload2);

    const entryKeys: string[] = [];
    for await (const entry of ddbMocks.ddbStorage.list()) {
      entryKeys.push(...entry.map((entry) => entry.key));
    }

    expect(entryKeys).to.include(key1);
    expect(entryKeys).to.include(key2);
  });

  it('should handle duplicate entries for the same group correctly', async () => {
    const key = 'entry1';
    const payload1 = { data: 'payload1' };
    const payload2 = { data: 'payload2' };

    await ddbMocks.ddbStorage.put(key, payload1);
    await ddbMocks.ddbStorage.put(key, payload2);

    const keys = [];
    for await (const entries of ddbMocks.ddbStorage.list()) {
      keys.push(...entries.map((entry) => entry.key));
    }

    expect(keys).to.include(key);
    expect(keys.length).to.equal(1);
  });

  it('should delete an entry and assert list is empty', async () => {
    const key = 'entry1';
    const payload = { data: 'payload' };

    await ddbMocks.ddbStorage.put(key, payload);

    await ddbMocks.ddbStorage.clear();

    const entryKeys = [];
    for await (const entries of ddbMocks.ddbStorage.list()) {
      entryKeys.push(...entries.map((entry) => entry.key));
    }

    expect(entryKeys).to.not.include(key);
    expect(entryKeys.length).to.equal(0);
  });
});
