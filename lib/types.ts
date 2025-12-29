import { SQSClient } from '@aws-sdk/client-sqs';

export type MessagePayload = Record<string, any>;

/**
 * Each entry can store arbitrary data and belongs to a group.
 */
export interface IndexEntry {
  /**
   * The group this entry belongs to.
   */
  groupId: string;

  /**
   * Entry identifier.
   */
  entryId: string;

  /**
   * Entry data.
   */
  payload: MessagePayload;
}

/**
 * Messages are grouped and deduplicated using a unique index. It can be seen as a database, although we are trying to provide connectors with other scalable systems like AWS S3 or AWS DynamoDB.
 */
export interface IndexedStorage {
  /**
   * Add entry to the storage.
   */
  add(groupId: string, entryId: string, payload: MessagePayload): Promise<void>;

  /**
   * Loads groups in chunks.
   */
  listGroups(): AsyncGenerator<string[]>;

  /**
   * Loads entries for `groupId` in chunks.
   */
  listEntries(groupId: string): AsyncGenerator<string[]>;

  /**
   * Loads message payloads for the specificed entries.
   */
  loadEntriesPayloads(
    groupId: string,
    entriesIds: string[]
  ): Promise<Record<string, MessagePayload>>;
}

export type IndexedStorageConnectorEntry = {
  key: string;
  payload: MessagePayload;
};

/**
 * Interface with external index system.
 */
export interface IndexedStorageConnector {
  /**
   * Put `payload` under `key`.
   */
  put(key: string, payload?: MessagePayload): Promise<void>;

  /**
   * Get entries in bulk.
   */
  getMany(keys: string[]): Promise<IndexedStorageConnectorEntry[]>;

  /**
   * List entries having `keyPrefix`;
   */
  list(keyPrefix: string): AsyncGenerator<string[]>;

  /**
   * Delete entries in bulk.
   */
  deleteMany(keys: string[]): Promise<void>;
}

/**
 * Options for {@link IndexedStorage}.
 */
export interface IndexedStorageOptions {
  /**
   * Index name.
   */
  name: string;

  /**
   * External connector.
   */
  connector: IndexedStorageConnector;
}

export interface MessageMapper {
  /**
   * A mapper to convert your message payload into an {@link IndexEntry}.
   */
  mapInputMessage(
    inputMessage: MessagePayload
  ): IndexEntry | Promise<IndexEntry>;

  /**
   * A mapper to convert a debounced group of entries to the desired output format, supporting just one or multiple messages.
   */
  mapOutputMessages(
    groupId: string,
    entriesIds: string[],
    payloadsByEntryId: Record<string, MessagePayload>
  ): MessagePayload[] | Promise<MessagePayload[]>;
}

export type DispatchMode = 'withPayload' | 'onlyIds';

/**
 * Options for the debouncer.
 */
export interface DebouncerOptions {
  /**
   * AWS sqs client instance
   */
  sqs: SQSClient;

  /**
   * The queue from which to receive messages to debounce.
   */
  inputQueueUrl: string;

  /**
   * The queue where to put debounced messages.
   */
  outputQueueUrl: string;

  /**
   * How to map input/output messages.
   */
  messageMapper: MessageMapper;

  /**
   * The message handling strategy to use when receiving messages.
   */
  inputMessageHandler: InputMessageHandler;

  /**
   * Whether to include only `groupId` and `entryId` or load message payloads as well when mapping messages before delivery.
   * Loading message payloads may incur additional round trips to fetch the data.
   *
   * @defaultvalue `withPayload`
   */
  mode: DispatchMode;

  /**
   * The underlying backing storage implementation.
   */
  index: IndexedStorage;
}

export interface InputMessageHandler {
  handleMessage(message: IndexEntry): Promise<void>;
}
