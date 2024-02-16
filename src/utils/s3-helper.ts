
import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import { Readable } from "stream";

export function readS3FileToBuffer (s3Client: S3Client, bucket: string, key: string, start: number, size: number): Promise<Buffer> {
  const end = start + size - 1;
  const getObjectParams = {
    Bucket: bucket,
    Key: key,
    Range: `bytes=${start}-${end}`
  };

  return new Promise((resolve, reject) => {
    s3Client.send(new GetObjectCommand(getObjectParams))
      .then(response => {
        const chunks: Buffer[] = [];
        if (response.Body instanceof Readable) {
          const stream = response.Body;
          stream.on('data', (chunk: Buffer) => chunks.push(chunk));
          stream.on('end', () => resolve(Buffer.concat(chunks)));
          stream.on('error', reject);
        } else {
          reject(new Error("Response body is not a Node.js readable stream"));
        }
      })
      .catch(reject);
  });
}
