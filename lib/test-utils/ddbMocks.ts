import {
  CreateTableCommand,
  DeleteTableCommand,
  DynamoDBClient
} from '@aws-sdk/client-dynamodb';
import { DDBStorage } from '../unique-index/connectors/ddb.js';

export class DDBMocks {
  private ddbClient!: DynamoDBClient;
  public ddbStorage!: DDBStorage;

  constructor(
    public tableName: string,
    public indexName: string,
    public region = 'us-east-1'
  ) {}

  async init() {
    this.ddbClient = new DynamoDBClient({
      region: this.region,
      endpoint: 'http://localhost:4566'
    });

    this.ddbClient.send(
      new CreateTableCommand({
        TableName: this.tableName,
        KeySchema: [{ AttributeName: 'key', KeyType: 'HASH' }],
        AttributeDefinitions: [{ AttributeName: 'key', AttributeType: 'S' }]
      })
    );

    this.ddbStorage = new DDBStorage(this.ddbClient, this.tableName);
  }

  async clear() {
    if (!this.ddbClient) {
      return;
    }
    await this.ddbClient.send(
      new DeleteTableCommand({ TableName: this.tableName })
    );
  }
}
