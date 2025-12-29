import pLimit from 'p-limit';
import { IndexedStorageConnector, MessagePayload } from '../../types.js';
import {
  BatchGetItemCommand,
  BatchWriteItemCommand,
  DynamoDBClient,
  QueryCommand
} from '@aws-sdk/client-dynamodb';
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb';
import { PutCommand } from '@aws-sdk/lib-dynamodb';

export class ConnectorDDB implements IndexedStorageConnector {
  constructor(
    private ddb: DynamoDBClient,
    private tableName: string,
    private pkName: string = 'pk',
    private skName: string = 'sk',
    private payloadAttr: string = 'payload'
  ) {}

  static maybeFromEnv() {
    const accessKeyId = process.env.AWS_ACCESS_KEY_ID;
    const secretAccessKey = process.env.AWS_SECRET_ACCESS_KEY;
    const sessionToken = process.env.AWS_SESSION_TOKEN;
    const tableName = process.env.MESSAGE_RECEIPTS_TABLE_NAME;

    if (
      tableName === undefined ||
      tableName === null ||
      tableName.trim() === ''
    ) {
      return null;
    }

    const credentials =
      accessKeyId && secretAccessKey
        ? { accessKeyId, secretAccessKey, sessionToken }
        : undefined;

    const dynamoDB = new DynamoDBClient({
      region: process.env.AWS_REGION,
      endpoint:
        process.env.ENV === 'local' ? 'http://localhost:4566' : undefined,
      credentials
    });

    return new ConnectorDDB(dynamoDB, tableName);
  }

  private pkFromKey(key: string): string {
    const idx = key.lastIndexOf('/');
    // If there is no '/', treat the whole key as the partition
    return idx === -1 ? key : key.slice(0, idx);
  }

  async put(key: string, payload: MessagePayload = {}) {
    const pk = this.pkFromKey(key);
    const item = {
      [this.pkName]: pk,
      [this.skName]: key,
      [this.payloadAttr]: payload
    };

    await this.ddb.send(
      new PutCommand({
        TableName: this.tableName,
        Item: marshall(item)
      })
    );
  }

  async getMany(keys: string[]) {
    // DynamoDB BatchGet limits: 100 keys per request
    const chunkSize = 100;
    const chunks: string[][] = [];
    for (let i = 0; i < keys.length; i += chunkSize) {
      chunks.push(keys.slice(i, i + chunkSize));
    }

    const limit = pLimit(5); // keep concurrency reasonable
    const results = await Promise.all(
      chunks.map((chunk) =>
        limit(async () => {
          const requestKeys = chunk.map((key) => ({
            [this.pkName]: { S: this.pkFromKey(key) },
            [this.skName]: { S: key }
          }));

          const resp = await this.ddb.send(
            new BatchGetItemCommand({
              RequestItems: {
                [this.tableName]: {
                  Keys: requestKeys
                }
              }
            })
          );

          const items = resp.Responses?.[this.tableName] ?? [];
          const byKey = new Map<string, any>();
          for (const raw of items) {
            const obj = unmarshall(raw);
            byKey.set(obj[this.skName], obj);
          }

          // preserve input order; missing items return empty payload
          return chunk.map((key) => {
            const obj = byKey.get(key);
            return {
              key,
              payload: (obj?.[this.payloadAttr] ?? {}) as MessagePayload
            };
          });
        })
      )
    );

    return results.flat();
  }

  async *list(keyPrefix: string) {
    // We assume keyPrefix is used as DynamoDB partition key and prefix of `sk`
    // Example: keyPrefix = "group/entry"
    // Query where pk == keyPrefix and sk begins_with(keyPrefix)
    let lastEvaluatedKey: Record<string, any> | undefined = undefined;

    do {
      const resp = await this.ddb.send(
        new QueryCommand({
          TableName: this.tableName,
          KeyConditionExpression: `#pk = :pk AND begins_with(#sk, :skPrefix)`,
          ExpressionAttributeNames: {
            '#pk': this.pkName,
            '#sk': this.skName
          },
          ExpressionAttributeValues: {
            ':pk': { S: keyPrefix },
            ':skPrefix': { S: keyPrefix }
          },
          ExclusiveStartKey: lastEvaluatedKey
        })
      );

      const keys =
        resp.Items?.map((it) => {
          const obj = unmarshall(it);
          return obj[this.skName] as string;
        }) ?? [];

      yield keys;

      lastEvaluatedKey = resp.LastEvaluatedKey;
    } while (lastEvaluatedKey);
  }

  async deleteMany(keys: string[]) {
    // DynamoDB BatchWrite limits: 25 requests per batch
    const chunkSize = 25;
    const chunks: string[][] = [];
    for (let i = 0; i < keys.length; i += chunkSize) {
      chunks.push(keys.slice(i, i + chunkSize));
    }

    const limit = pLimit(5);
    await Promise.all(
      chunks.map((chunk) =>
        limit(async () => {
          const requests = chunk.map((key) => ({
            DeleteRequest: {
              Key: {
                [this.pkName]: { S: this.pkFromKey(key) },
                [this.skName]: { S: key }
              }
            }
          }));

          await this.ddb.send(
            new BatchWriteItemCommand({
              RequestItems: {
                [this.tableName]: requests
              }
            })
          );
        })
      )
    );
  }
}
