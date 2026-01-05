import {
  S3Client,
  CreateBucketCommand,
  DeleteBucketCommand
} from '@aws-sdk/client-s3';
import { S3Storage } from '../unique-index/connectors/s3.js';

export class S3Mocks {
  public s3Client!: S3Client;
  public s3Storage!: S3Storage;

  constructor(
    public bucketName: string,
    public indexName: string,
    public region = 'us-east-1'
  ) {}

  async init() {
    this.s3Client = new S3Client({
      region: this.region,
      endpoint: 'http://localhost:4566',
      forcePathStyle: true,
      credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'test',
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'test'
      }
    });
    this.s3Storage = new S3Storage(this.s3Client, this.bucketName);
    await this.s3Client.send(
      new CreateBucketCommand({ Bucket: this.bucketName })
    );
  }

  async clear() {
    await this.s3Client.send(
      new DeleteBucketCommand({ Bucket: this.bucketName })
    );
  }

  async clearIndexFiles() {
    const keys = [];
    for await (const key of this.s3Storage.listKeys(
      `index/${this.indexName}/`
    )) {
      keys.push(...key);
    }
    await this.s3Storage.deleteMany(keys);
  }
}
