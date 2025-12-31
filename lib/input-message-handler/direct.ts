import {
  IndexedStorage,
  IndexEntry,
  InputMessageHandler,
  MessageMapper
} from '../types.js';

export class DirectInputMessageHandler implements InputMessageHandler {
  constructor(
    private readonly index: IndexedStorage,
    private readonly messageMapper: MessageMapper
  ) {}

  async handleMessage(input: IndexEntry): Promise<void> {
    const { key, payload } = await this.messageMapper.mapInputMessage(input);

    await this.index.put(key, payload);
  }
}
