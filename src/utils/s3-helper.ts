
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

/** 
 * Read bytes from a Node.js readable stream and put them into a Buffer, return the Buffer 
 **/
function readBytes (readable: Readable, byteCount: number): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    let remainingBytes = byteCount;
    const chunks: Buffer[] = [];

    readable.on('data', (chunk: Buffer) => {
      if (remainingBytes >= chunk.length) {
        chunks.push(chunk);
        remainingBytes -= chunk.length;
      } else {
        chunks.push(chunk.slice(0, remainingBytes));
        remainingBytes = 0;
        readable.pause();
        resolve(Buffer.concat(chunks));
      }
    });

    readable.on('end', () => {
      if (remainingBytes > 0) {
        reject(new Error(`Stream ended before reading all the bytes (missing ${remainingBytes} bytes)`));
      } else {
        resolve(Buffer.concat(chunks));
      }
    });

    readable.on('error', (error) => {
      reject(error);
    });
  });
}
