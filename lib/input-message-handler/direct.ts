import { IndexedStorage, IndexEntry, InputMessageHandler } from '../types.js';

export class DirectInputMessageHandler implements InputMessageHandler {
  constructor(private readonly index: IndexedStorage) {}

  async handleMessage(input: IndexEntry): Promise<void> {
    await this.index.put(input.key, input.payload);
  }
}
