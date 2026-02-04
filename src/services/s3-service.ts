import type { Readable } from 'node:stream';
import {
  GetObjectCommand,
  HeadObjectCommand,
  PutObjectCommand,
  type S3Client,
} from '@aws-sdk/client-s3';
import type { S3ObjectInfo, S3StreamOptions, UploadOptions } from '../types';
import { createErrorContext } from '../utils/error-context';
import logger from '../utils/logger';
import { memoryMonitor } from '../utils/memory-monitor';
import { retryWithBackoff } from '../utils/retry-handler';
import { getS3Client } from '../utils/s3-client-singleton';
import { streamToBuffer, validateStream } from '../utils/stream-utils';

export class S3Service {
  private s3Client: S3Client;

  constructor() {
    // Use singleton S3Client for better cold start performance
    this.s3Client = getS3Client();
  }

  /**
   * Downloads a zip file from S3 as a Buffer
   * For large files, consider using streaming instead
   */
  async getObjectAsBuffer(bucket: string, key: string, options?: S3StreamOptions): Promise<Buffer> {
    const startTime = new Date();

    return retryWithBackoff.s3Operation(
      async () => {
        // Check memory before starting download
        if (!memoryMonitor.checkMemoryUsage('s3_download_start')) {
          const error = createErrorContext
            .memory('s3_download', memoryMonitor.getMemoryUsage().heapUsed / 1024 / 1024)
            .setS3Location(bucket, key)
            .setTiming(startTime)
            .createError('Cannot start S3 download - memory usage too high', undefined, {
              errorCode: 'MEMORY_LIMIT_EXCEEDED',
              retryable: false,
              severity: 'high',
            });
          throw error;
        }

        logger.debug('Downloading object from S3', { bucket, key, operation: 's3_download' });

        const command = new GetObjectCommand({
          Bucket: bucket,
          Key: key,
          Range: options?.range,
          VersionId: options?.versionId,
        });

        const response = await this.s3Client.send(command);

        if (!response.Body) {
          const error = createErrorContext
            .s3('s3_download', bucket, key)
            .setPhase('response_validation')
            .setTiming(startTime)
            .createError('No body returned for object', undefined, {
              errorCode: 'EMPTY_RESPONSE_BODY',
              retryable: true,
              severity: 'medium',
            });
          throw error;
        }

        // Convert stream to buffer
        const stream = validateStream(response.Body as Readable, 'No body returned for object');

        // Check memory before buffer conversion
        if (!memoryMonitor.checkMemoryUsage('s3_stream_to_buffer')) {
          const error = createErrorContext
            .memory('s3_stream_to_buffer', memoryMonitor.getMemoryUsage().heapUsed / 1024 / 1024)
            .setS3Location(bucket, key)
            .setTiming(startTime)
            .createError('Cannot convert S3 stream to buffer - memory usage too high', undefined, {
              errorCode: 'MEMORY_LIMIT_EXCEEDED',
              retryable: false,
              severity: 'high',
            });
          throw error;
        }

        const buffer = await streamToBuffer(stream);

        logger.debug('Successfully downloaded object', {
          bucket,
          key,
          sizeBytes: buffer.length,
          operation: 's3_download_complete',
        });

        return buffer;
      },
      's3_download',
      bucket,
      key
    );
  }

  /**
   * Validates file size before downloading to prevent processing oversized files
   * Returns true if file size is acceptable, false otherwise
   */
  async validateFileSize(
    bucket: string,
    key: string,
    maxSizeBytes: number = 50 * 1024 * 1024
  ): Promise<{ isValid: boolean; size?: number; reason?: string }> {
    try {
      const objectInfo = await this.getObjectInfo(bucket, key);

      if (!objectInfo.size) {
        return {
          isValid: false,
          reason: 'Cannot determine file size',
        };
      }

      if (objectInfo.size > maxSizeBytes) {
        logger.warn('File size exceeds limit, skipping processing', {
          bucket,
          key,
          fileSizeBytes: objectInfo.size,
          maxSizeBytes,
          fileSizeMB: Math.round((objectInfo.size / 1024 / 1024) * 100) / 100,
          maxSizeMB: Math.round((maxSizeBytes / 1024 / 1024) * 100) / 100,
        });

        return {
          isValid: false,
          size: objectInfo.size,
          reason: `File size ${Math.round((objectInfo.size / 1024 / 1024) * 100) / 100}MB exceeds limit of ${Math.round((maxSizeBytes / 1024 / 1024) * 100) / 100}MB`,
        };
      }

      return {
        isValid: true,
        size: objectInfo.size,
      };
    } catch (error) {
      logger.error('Failed to validate file size', error as Error, { bucket, key });
      return {
        isValid: false,
        reason: `Failed to check file size: ${error instanceof Error ? error.message : String(error)}`,
      };
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
    const startTime = new Date();

    return retryWithBackoff.s3Operation(
      async () => {
        // Check memory before starting upload
        if (!memoryMonitor.checkMemoryUsage('s3_upload_start')) {
          const error = createErrorContext
            .memory('s3_upload_start', memoryMonitor.getMemoryUsage().heapUsed / 1024 / 1024)
            .setS3Location(bucket, key)
            .setTiming(startTime)
            .createError('Cannot start S3 upload - memory usage too high', undefined, {
              errorCode: 'MEMORY_LIMIT_EXCEEDED',
              retryable: false,
              severity: 'high',
            });
          throw error;
        }

        logger.debug('Starting upload to S3', { bucket, key, operation: 's3_upload' });

        // Convert stream to buffer for upload
        // In production, consider using multipart upload for larger files
        const buffer = await streamToBuffer(stream);

        // Check memory after buffer creation
        if (!memoryMonitor.checkMemoryUsage('s3_upload_buffer_created')) {
          const error = createErrorContext
            .memory(
              's3_upload_buffer_created',
              memoryMonitor.getMemoryUsage().heapUsed / 1024 / 1024
            )
            .setS3Location(bucket, key)
            .setTiming(startTime)
            .createError(
              'Cannot upload to S3 - memory usage too high after buffer creation',
              undefined,
              {
                errorCode: 'MEMORY_LIMIT_EXCEEDED',
                retryable: false,
                severity: 'high',
              }
            );
          throw error;
        }

        const command = new PutObjectCommand({
          Bucket: bucket,
          Key: key,
          Body: buffer,
          ContentType: options?.contentType || this.guessContentType(key),
          Metadata: options?.metadata,
          ServerSideEncryption: 'AES256', // Force encryption for security
        });

        await this.s3Client.send(command);

        logger.debug('Successfully uploaded to S3', {
          bucket,
          key,
          sizeBytes: buffer.length,
          operation: 's3_upload_complete',
        });
      },
      's3_upload_stream',
      bucket,
      key
    );
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
      // Check memory before starting upload
      if (!memoryMonitor.checkMemoryUsage('s3_buffer_upload_start')) {
        throw new Error('Cannot start S3 buffer upload - memory usage too high');
      }

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
        ServerSideEncryption: 'AES256', // Force encryption for security
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
