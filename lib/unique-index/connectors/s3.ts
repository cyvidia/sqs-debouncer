import {
  PutObjectCommand,
  ListObjectsV2Command,
  DeleteObjectsCommand,
  S3Client,
  GetObjectCommand
} from '@aws-sdk/client-s3';
import {
  IndexedStorage,
  IndexedStorageConnectorEntry,
  MessagePayload
} from '../../types.js';
import pLimit from 'p-limit';

// TODO: Fix this implementation, it's currently broken.
export class S3Storage implements IndexedStorage {
  constructor(
    private s3Client: S3Client,
    private bucketName: string
  ) {}

  async put(key: string, payload: MessagePayload = {}) {
    const putParams = {
      Bucket: this.bucketName,
      Key: key,
      Body: JSON.stringify(payload)
    };
    await this.s3Client.send(new PutObjectCommand(putParams));
  }

  async *listKeys(prefix: string = ''): AsyncGenerator<string[]> {
    let continuationToken = null;
    do {
      const params = {
        Bucket: this.bucketName,
        ContinuationToken: continuationToken,
        Prefix: prefix
      };
      const data = await this.s3Client.send(new ListObjectsV2Command(params));
      continuationToken = data.NextContinuationToken;

      const keys =
        data.KeyCount === 0 ? [] : data.Contents.map(({ Key }) => Key);
      yield keys as string[];
    } while (continuationToken);
  }

  async *list(
    prefix: string = ''
  ): AsyncGenerator<IndexedStorageConnectorEntry[]> {
    const limit = pLimit(10);

    for await (const keys of this.listKeys(prefix)) {
      const realKeys = (keys ?? []).filter((k) => k && !k.endsWith('/'));
      if (realKeys.length === 0) continue;

      const results = await Promise.all(
        realKeys.map((Key) =>
          limit(async () => {
            try {
              const res = await this.s3Client.send(
                new GetObjectCommand({
                  Bucket: this.bucketName,
                  Key
                })
              );

              const bodyStr =
                res.Body &&
                typeof (res.Body as any).transformToString === 'function'
                  ? await (res.Body as any).transformToString('utf-8')
                  : '';

              if (!bodyStr) {
                return null;
              }

              const payload = JSON.parse(bodyStr);

              const entry: IndexedStorageConnectorEntry = {
                key: Key,
                payload
              };

              return entry;
            } catch (err) {
              console.error(`Failed to read S3 object ${Key}`, err);
              return null;
            }
          })
        )
      );

      const entries = results.filter(Boolean) as IndexedStorageConnectorEntry[];
      if (entries.length) yield entries;
    }
  }

  async clear() {
    for await (const keys of this.listKeys()) {
      if (keys.length === 0) continue;

      const deleteParams = {
        Bucket: this.bucketName,
        Delete: {
          Objects: keys.map((Key) => ({ Key }))
        }
      };
      await this.s3Client.send(new DeleteObjectsCommand(deleteParams));
    }
  }

  async deleteMany(keys: string[]) {
    if (keys.length === 0) return;

    const deleteParams = {
      Bucket: this.bucketName,
      Delete: {
        Objects: keys.map((Key) => ({ Key }))
      }
    };
    await this.s3Client.send(new DeleteObjectsCommand(deleteParams));
  }
}
