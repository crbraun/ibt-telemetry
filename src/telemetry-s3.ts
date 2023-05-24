import { safeLoad as safeLoadYaml } from 'js-yaml'
import { Observable } from 'rxjs'
import { range } from 'ramda'

import { SIZE_IN_BYTES as HEADER_SIZE_IN_BYTES, TelemetryHeader } from './headers/telemetry-header'
import { SIZE_IN_BYTES as DISK_SUB_HEADER_SIZE_IN_BYTES, DiskSubHeader } from './headers/disk-sub-header'
import { SIZE_IN_BYTES as VAR_HEADER_SIZE_IN_BYTES, VarHeader } from './headers/var-header'

import { TelemetrySample } from './telemetry-sample'
import { readS3FileToBuffer } from "./utils/s3-helper";
import { S3Client, GetObjectCommand, GetObjectCommandOutput } from "@aws-sdk/client-s3";
import { Readable } from "stream";
import {readSync} from "fs";

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
      const chunkSize = this.telemetryHeader.bufLen;
      const getObjectCommand: GetObjectCommand = new GetObjectCommand({
        Bucket: this.s3bucket,
        Key: this.s3key,
        Range: "bytes=" + this.telemetryHeader.bufOffset.toString() + "-"
      });

      //print the getobject command to the console in json format
      console.log(JSON.stringify(getObjectCommand, null, 2));

      let currentChunk: Buffer = Buffer.alloc(0);

      s3Client.send(getObjectCommand)
          .then(async (response: GetObjectCommandOutput) => {
            if (response.Body instanceof Readable) {
              for await (const chunk of response.Body) {
                let remainingChunk: Buffer = Buffer.concat([currentChunk, chunk]);

                while (remainingChunk.length >= chunkSize) {
                  const sample = remainingChunk.slice(0, chunkSize);
                  subscriber.next(new TelemetrySample(sample, this.varHeaders));
                  remainingChunk = remainingChunk.slice(chunkSize);
                }

                currentChunk = remainingChunk;
              }
            }

            subscriber.complete();
          })
          .catch(error => subscriber.error(error));
    })
  }
}
  

