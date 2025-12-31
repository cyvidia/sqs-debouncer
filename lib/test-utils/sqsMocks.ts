import {
  SQSClient,
  CreateQueueCommand,
  DeleteQueueCommand
} from '@aws-sdk/client-sqs';

export class SQSMocks {
  public sqsClient!: SQSClient;
  public sqsQueueUrl!: string;
  public sqsDebouncedQueueUrl!: string;

  constructor(
    public queueName: string,
    public debouncedQueueName: string,
    public region = 'us-east-1'
  ) {}

  async init() {
    this.sqsClient = new SQSClient({
      region: this.region,
      endpoint: 'http://localhost:4566',
      credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'test',
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'test'
      }
    });

    let result = await this.sqsClient.send(
      new CreateQueueCommand({ QueueName: this.queueName })
    );

    const queueUrl = result.QueueUrl;

    if (!queueUrl) {
      throw new Error('Failed to create SQS queue');
    }

    this.sqsQueueUrl = queueUrl;
    result = await this.sqsClient.send(
      new CreateQueueCommand({ QueueName: this.debouncedQueueName })
    );
    this.sqsDebouncedQueueUrl = queueUrl;
  }

  async clear() {
    if (!this.sqsClient || !this.sqsQueueUrl || !this.sqsDebouncedQueueUrl) {
      return;
    }
    await this.sqsClient.send(
      new DeleteQueueCommand({ QueueUrl: this.sqsQueueUrl })
    );
    await this.sqsClient.send(
      new DeleteQueueCommand({ QueueUrl: this.sqsDebouncedQueueUrl })
    );
  }
}
