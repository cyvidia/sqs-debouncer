import { SQSClient } from '@aws-sdk/client-sqs';

export type MessagePayload = Record<string, any>;

/**
 * Each entry can store arbitrary data.
 */
export interface IndexEntry {
  /**
   * Entry identifier.
   */
  key: string;

  /**
   * Entry data.
   */
  payload: MessagePayload;
}

export type IndexedStorageConnectorEntry = {
  key: string;
  payload: MessagePayload;
};

/**
 * Interface with external index system.
 */
export interface IndexedStorage {
  /**
   * Put `payload` under `key`
   */
  put(key: string, payload: MessagePayload): Promise<void>;

  /**
   * List all entries.
   */
  list(): AsyncGenerator<IndexedStorageConnectorEntry[]>;

  /**
   * Deletes all entries.
   */
  clear(): Promise<void>;
}

export interface MessageMapper {
  /**
   * A mapper to convert your message payload into an {@link IndexEntry}.
   */
  mapInputMessage(
    inputMessage: MessagePayload
  ): IndexEntry | Promise<IndexEntry>;

  /**
   * A mapper to convert debounced entries to the desired output format, supporting just one or multiple messages.
   */
  mapOutputMessages(
    entries: IndexedStorageConnectorEntry[]
  ): MessagePayload[] | Promise<MessagePayload[]>;
}

/**
 * Options for the debouncer.
 */
export interface DebouncerOptions {
  /**
   * AWS sqs client instance
   */
  sqs: SQSClient;

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
   * The underlying backing storage implementation.
   */
  index: IndexedStorage;
}

export interface InputMessageHandler {
  handleMessage(message: IndexEntry): Promise<void>;
}
