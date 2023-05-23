import { safeLoad as safeLoadYaml } from 'js-yaml'
import { Observable } from 'rxjs'
import { range } from 'ramda'

import { SIZE_IN_BYTES as HEADER_SIZE_IN_BYTES, TelemetryHeader } from './headers/telemetry-header'
import { SIZE_IN_BYTES as DISK_SUB_HEADER_SIZE_IN_BYTES, DiskSubHeader } from './headers/disk-sub-header'
import { SIZE_IN_BYTES as VAR_HEADER_SIZE_IN_BYTES, VarHeader } from './headers/var-header'

import { TelemetrySample } from './telemetry-sample'
import { readS3FileToBuffer } from "./utils/s3-helper";
import { tsc --version, GetObjectCommand, GetObjectCommandOutput } from "@aws-sdk/client-s3";
import { Readable } from "stream";

// Return the Telemetry header from the supplied file descriptor
const telemetryHeaderFromS3 = async (s3client: S3Client, bucket: string, key: string): Promise<TelemetryHeader> =>
  readS3FileToBuffer(s3client, bucket, key, 0, HEADER_SIZE_IN_BYTES)
    .then(TelemetryHeader.fromBuffer)

// Disk sub header telemetry
const diskSubHeaderFromS3 = async (s3client: S3Client, bucket: string, key: string): Promise<DiskSubHeader> =>
  readS3FileToBuffer(s3client, bucket, key, DISK_SUB_HEADER_SIZE_IN_BYTES, HEADER_SIZE_IN_BYTES)
    .then(DiskSubHeader.fromBuffer)

const sessionInfoStringFromS3 = async (s3client: S3Client, bucket: string, key: string, telemetryHeader: TelemetryHeader): Promise<string> =>
  readS3FileToBuffer(s3client, bucket, key, telemetryHeader.sessionInfoOffset, telemetryHeader.sessionInfoLength)
    .then(x => x.toString('ascii'))

const varHeadersFromS3 = async (s3client: S3Client, bucket: string, key: string, telemetryHeader: TelemetryHeader): Promise<VarHeader[]> => {
  const numberOfVariables = telemetryHeader.numVars
  const startPosition = telemetryHeader.varHeaderOffset
  const fullBufferSize = numberOfVariables * VAR_HEADER_SIZE_IN_BYTES

  return readS3FileToBuffer(s3client, bucket, key, startPosition, fullBufferSize)
    .then(buffer => {
      return range(0, numberOfVariables).map(count => {
        const start = count * VAR_HEADER_SIZE_IN_BYTES
        const end = start + VAR_HEADER_SIZE_IN_BYTES
        return VarHeader.fromBuffer(buffer.slice(start, end))
      })
    })
}

/**
 * iRacing Telemetry
 */
export class TelemetryS3 {
  public sessionInfo: any

  /**
     * Telemetry constructor.
     */
  constructor (
    public readonly telemetryHeader: TelemetryHeader,
    public readonly diskSubHeader: DiskSubHeader,
    public readonly sessionInfoYaml: string,
    public readonly varHeaders: VarHeader[],
    private readonly s3bucket: string,
    private readonly s3key: string
  ) {
    this.sessionInfo = safeLoadYaml(sessionInfoYaml)
  }

  static async fromS3Object (s3client: S3Client, bucket: string, key: string): Promise<TelemetryS3> {

    const resolvedHeaders = await Promise.all([
      telemetryHeaderFromS3(s3client,bucket, key),
      diskSubHeaderFromS3(s3client, bucket, key)
    ])
    const telemetryHeader = resolvedHeaders[0] as TelemetryHeader
    const diskSubHeader = resolvedHeaders[1] as DiskSubHeader

    const [ sessionInfo, varHeaders ]: [ string, VarHeader[] ] = await Promise.all([
      sessionInfoStringFromS3(s3client, bucket, key, telemetryHeader),
      varHeadersFromS3(s3client, bucket, key, telemetryHeader)
    ])

    return new TelemetryS3(telemetryHeader, diskSubHeader, sessionInfo, varHeaders, bucket, key)
  }

  /**
     * Generate a unique key for the telemetry session.
     *
     * The unique key is a combination of 3 fields:
     *   accountId-sessionId-subSessionId
     *
     * @return string
     */
  uniqueId () {
    const accountId = this.sessionInfo.DriverInfo.Drivers[this.sessionInfo.DriverInfo.DriverCarIdx].UserID
    const sessionId = this.sessionInfo.WeekendInfo.SessionID
    const subSessionId = this.sessionInfo.WeekendInfo.SubSessionID
    return `${accountId}-${sessionId}-${subSessionId}`
  }

  /**
     * Returns a stream of TelemetrySample objects
     */


  sampleStream (s3Client: S3Client): Observable<TelemetrySample> {
    return new Observable(subscriber => {
      let count = 0
      const chunkSize: number = this.telemetryHeader.bufLen
      const getObjectCommand: GetObjectCommand = new GetObjectCommand({
        Bucket: this.s3bucket,
        Key: this.s3key,
        Range: "bytes=" + this.telemetryHeader.bufOffset }
      );
      
      s3Client.send(getObjectCommand).then(
        (response: GetObjectCommandOutput) => {
          if (response.Body instanceof Readable) {
            const readable: Readable = response.Body as Readable;
            let offset: number = this.telemetryHeader.bufOffset + (count++ * chunkSize);

            readable.on('data', (chunk: Buffer) => {
              readable.pause();
              //We need to parse each sample from the Readable stream
              //Process in a loop until we have no more data


              // If we have more data than we need
              if (offset + chunkSize <= chunk.length) {
                const sampleBuffer: Buffer = chunk.slice(offset, offset + chunkSize);

                offset += chunkSize;
                subscriber.next(new TelemetrySample(sampleBuffer, this.varHeaders))

                // If we've processed all data in this chunk
                if (offset === chunk.length) {
                  offset = 0;
                  readable.resume();
                }
              } else {
                // If we have less data than we need
                // Note: we leave the stream paused until we get enough data
                offset = chunk.length;
              }
            });
          }
        }
      )
      subscriber.complete()
    })
  }
}
  

