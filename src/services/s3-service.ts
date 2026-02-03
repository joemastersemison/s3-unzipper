import type { Readable } from 'node:stream';
import {
  GetObjectCommand,
  HeadObjectCommand,
  PutObjectCommand,
  S3Client,
} from '@aws-sdk/client-s3';
import type { S3ObjectInfo, S3StreamOptions, UploadOptions } from '../types';
import config from '../utils/config';
import logger from '../utils/logger';

export class S3Service {
  private s3Client: S3Client;

  constructor() {
    this.s3Client = new S3Client({
      region: config.region,
    });
  }

  /**
   * Downloads a zip file from S3 as a Buffer
   * For large files, consider using streaming instead
   */
  async getObjectAsBuffer(bucket: string, key: string, options?: S3StreamOptions): Promise<Buffer> {
    try {
      logger.debug('Downloading object from S3', { bucket, key, operation: 's3_download' });

      const command = new GetObjectCommand({
        Bucket: bucket,
        Key: key,
        Range: options?.range,
        VersionId: options?.versionId,
      });

      const response = await this.s3Client.send(command);

      if (!response.Body) {
        throw new Error(`No body returned for object ${key} in bucket ${bucket}`);
      }

      // Convert stream to buffer
      const chunks: Buffer[] = [];
      const stream = response.Body as Readable;

      for await (const chunk of stream) {
        chunks.push(Buffer.from(chunk));
      }

      const buffer = Buffer.concat(chunks);

      logger.debug('Successfully downloaded object', {
        bucket,
        key,
        sizeBytes: buffer.length,
        operation: 's3_download_complete',
      });

      return buffer;
    } catch (error) {
      logger.error('Failed to download object from S3', error as Error, {
        bucket,
        key,
        operation: 's3_download_error',
      });
      throw error;
    }
  }

  /**
   * Gets object metadata without downloading the content
   */
  async getObjectInfo(bucket: string, key: string): Promise<S3ObjectInfo> {
    try {
      const command = new HeadObjectCommand({
        Bucket: bucket,
        Key: key,
      });

      const response = await this.s3Client.send(command);

      return {
        bucket,
        key,
        size: response.ContentLength,
        lastModified: response.LastModified,
      };
    } catch (error) {
      logger.error('Failed to get object info from S3', error as Error, {
        bucket,
        key,
        operation: 's3_head_error',
      });
      throw error;
    }
  }

  /**
   * Uploads a file to S3 from a readable stream
   */
  async uploadStream(
    bucket: string,
    key: string,
    stream: Readable,
    options?: UploadOptions
  ): Promise<void> {
    try {
      logger.debug('Starting upload to S3', { bucket, key, operation: 's3_upload' });

      // Convert stream to buffer for upload
      // In production, consider using multipart upload for larger files
      const chunks: Buffer[] = [];
      for await (const chunk of stream) {
        chunks.push(Buffer.from(chunk));
      }

      const buffer = Buffer.concat(chunks);

      const command = new PutObjectCommand({
        Bucket: bucket,
        Key: key,
        Body: buffer,
        ContentType: options?.contentType || this.guessContentType(key),
        Metadata: options?.metadata,
      });

      await this.s3Client.send(command);

      logger.debug('Successfully uploaded to S3', {
        bucket,
        key,
        sizeBytes: buffer.length,
        operation: 's3_upload_complete',
      });
    } catch (error) {
      logger.error('Failed to upload to S3', error as Error, {
        bucket,
        key,
        operation: 's3_upload_error',
      });
      throw error;
    }
  }

  /**
   * Uploads a buffer to S3
   */
  async uploadBuffer(
    bucket: string,
    key: string,
    buffer: Buffer,
    options?: UploadOptions
  ): Promise<void> {
    try {
      logger.debug('Starting buffer upload to S3', {
        bucket,
        key,
        sizeBytes: buffer.length,
        operation: 's3_buffer_upload',
      });

      const command = new PutObjectCommand({
        Bucket: bucket,
        Key: key,
        Body: buffer,
        ContentType: options?.contentType || this.guessContentType(key),
        Metadata: options?.metadata,
      });

      await this.s3Client.send(command);

      logger.debug('Successfully uploaded buffer to S3', {
        bucket,
        key,
        sizeBytes: buffer.length,
        operation: 's3_buffer_upload_complete',
      });
    } catch (error) {
      logger.error('Failed to upload buffer to S3', error as Error, {
        bucket,
        key,
        operation: 's3_buffer_upload_error',
      });
      throw error;
    }
  }

  /**
   * Checks if an object exists in S3
   */
  async objectExists(bucket: string, key: string): Promise<boolean> {
    try {
      await this.getObjectInfo(bucket, key);
      return true;
    } catch (error) {
      // If it's a "NotFound" error, return false; otherwise re-throw
      if (error && typeof error === 'object' && 'name' in error && error.name === 'NotFound') {
        return false;
      }
      throw error;
    }
  }

  /**
   * Gets a readable stream for an S3 object
   */
  async getObjectStream(bucket: string, key: string, options?: S3StreamOptions): Promise<Readable> {
    try {
      const command = new GetObjectCommand({
        Bucket: bucket,
        Key: key,
        Range: options?.range,
        VersionId: options?.versionId,
      });

      const response = await this.s3Client.send(command);

      if (!response.Body) {
        throw new Error(`No body returned for object ${key} in bucket ${bucket}`);
      }

      return response.Body as Readable;
    } catch (error) {
      logger.error('Failed to get object stream from S3', error as Error, {
        bucket,
        key,
        operation: 's3_stream_error',
      });
      throw error;
    }
  }

  /**
   * Guesses content type based on file extension
   */
  private guessContentType(key: string): string {
    const extension = key.split('.').pop()?.toLowerCase();

    const mimeTypes: Record<string, string> = {
      csv: 'text/csv',
      txt: 'text/plain',
      json: 'application/json',
      xml: 'application/xml',
      pdf: 'application/pdf',
      jpg: 'image/jpeg',
      jpeg: 'image/jpeg',
      png: 'image/png',
      gif: 'image/gif',
      html: 'text/html',
      css: 'text/css',
      js: 'application/javascript',
    };

    return mimeTypes[extension || ''] || 'application/octet-stream';
  }
}
