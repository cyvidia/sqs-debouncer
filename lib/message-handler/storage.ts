import {
  IndexedStorage,
  IndexEntry,
  InputMessageHandler,
  MessageMapper
} from '../types.js';

export class StorageInputMessageHandler implements InputMessageHandler {
  constructor(
    private readonly index: IndexedStorage,
    private readonly messageMapper: MessageMapper
  ) {}

  async handleMessage(input: IndexEntry): Promise<void> {
    const { groupId, entryId, payload } =
      await this.messageMapper.mapInputMessage(input);

    await this.index.add(groupId, entryId, payload);
  }
}
