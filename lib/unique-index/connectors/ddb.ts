import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  BatchWriteCommand,
  BatchWriteCommandInput,
  DynamoDBDocumentClient,
  PutCommand,
  ScanCommand
} from '@aws-sdk/lib-dynamodb';
import type {
  IndexedStorage,
  IndexedStorageConnectorEntry,
  MessagePayload
} from '../../types.js';

export class DDBStorage implements IndexedStorage {
  constructor(
    private ddb: DynamoDBDocumentClient,
    private tableName: string,
    private pkName: string = 'key',
    private payloadAttr: string = 'payload'
  ) {}

  static fromEnv(tableName: string) {
    const accessKeyId = process.env.AWS_ACCESS_KEY_ID;
    const secretAccessKey = process.env.AWS_SECRET_ACCESS_KEY;
    const sessionToken = process.env.AWS_SESSION_TOKEN;

    const credentials =
      accessKeyId && secretAccessKey
        ? { accessKeyId, secretAccessKey, sessionToken }
        : undefined;

    const dynamoDB = DynamoDBDocumentClient.from(
      new DynamoDBClient({
        region: process.env.AWS_REGION,
        endpoint:
          process.env.ENV === 'local' ? 'http://localhost:4566' : undefined,
        credentials
      })
    );

    return new DDBStorage(dynamoDB, tableName);
  }

  async put(key: string, payload: MessagePayload) {
    const item = {
      [this.pkName]: key,
      [this.payloadAttr]: payload
    };

    await this.ddb.send(
      new PutCommand({
        TableName: this.tableName,
        Item: item
      })
    );
  }

  async *list(): AsyncGenerator<IndexedStorageConnectorEntry[]> {
    let ExclusiveStartKey: Record<string, any> | undefined = undefined;

    do {
      const res = await this.ddb.send(
        new ScanCommand({
          TableName: this.tableName,
          ExclusiveStartKey,
          ExpressionAttributeNames: {
            '#pk': this.pkName,
            '#payload': this.payloadAttr
          },
          ProjectionExpression: '#pk, #payload'
        })
      );

      const entries: IndexedStorageConnectorEntry[] = (res.Items ?? []).map(
        (item) => {
          return {
            key: item[this.pkName] as string,
            payload: (item[this.payloadAttr] ?? {}) as MessagePayload
          };
        }
      );

      console.log(
        `[DEBOUNCER DEBUGGING] Fetched ${entries.length} entries from DynamoDB`
      );

      if (entries.length) yield entries;

      ExclusiveStartKey = res.LastEvaluatedKey;
    } while (ExclusiveStartKey);
  }

  async clear(): Promise<void> {
    let ExclusiveStartKey: Record<string, any> | undefined;

    do {
      const scanRes = await this.ddb.send(
        new ScanCommand({
          TableName: this.tableName,
          ExclusiveStartKey,
          ProjectionExpression: '#pk',
          ExpressionAttributeNames: { '#pk': this.pkName }
        })
      );

      const keys = (scanRes.Items ?? []).map((item: any) => ({
        [this.pkName]: item[this.pkName]
      }));

      for (let i = 0; i < keys.length; i += 25) {
        const batchKeys = keys.slice(i, i + 25);

        let requestItems: BatchWriteCommandInput['RequestItems'] = {
          [this.tableName]: batchKeys.map((Key) => ({ DeleteRequest: { Key } }))
        };

        while (true) {
          const res = await this.ddb.send(
            new BatchWriteCommand({ RequestItems: requestItems })
          );

          const unprocessed = res.UnprocessedItems?.[this.tableName] ?? [];
          if (unprocessed.length === 0) break;

          requestItems = { [this.tableName]: unprocessed };
        }
      }

      ExclusiveStartKey = scanRes.LastEvaluatedKey;
    } while (ExclusiveStartKey);
  }
}
