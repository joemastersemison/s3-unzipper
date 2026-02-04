import type * as yauzl from 'yauzl';
import type { CSVProcessor } from '../services/csv-processor';
import type { S3Service } from '../services/s3-service';
import type { ProcessingContext } from '../types';
import logger from './logger';
import { openZipEntryStream } from './zip-utils';

export interface FileUploadOptions {
  bucket: string;
  outputKey: string;
  context: ProcessingContext;
  isCSV: boolean;
  csvProcessingEnabled: boolean;
}

/**
 * Handles file uploads with CSV processing and fallback retry logic
 * Eliminates deeply nested callback patterns
 */
export class FileUploadHandler {
  constructor(
    private s3Service: S3Service,
    private csvProcessor: CSVProcessor
  ) {}

  /**
   * Main upload handler that manages CSV processing and fallback logic
   */
  async uploadFileFromZip(
    entry: yauzl.Entry,
    zipfile: yauzl.ZipFile,
    options: FileUploadOptions
  ): Promise<void> {
    const { bucket, outputKey, context, isCSV, csvProcessingEnabled } = options;

    if (isCSV && csvProcessingEnabled) {
      await this.handleCSVUpload(entry, zipfile, bucket, outputKey, context);
    } else {
      await this.handleDirectUpload(entry, zipfile, bucket, outputKey, context);
    }
  }

  /**
   * Handles CSV file upload with processing and fallback
   */
  private async handleCSVUpload(
    entry: yauzl.Entry,
    zipfile: yauzl.ZipFile,
    bucket: string,
    outputKey: string,
    context: ProcessingContext
  ): Promise<void> {
    logger.info('Processing CSV file', {
      fileName: entry.fileName,
      outputKey,
      requestId: context.requestId,
      operation: 'csv_processing_start',
    });

    try {
      const readStream = await openZipEntryStream(zipfile, entry, context.requestId);
      const processedBuffer = await this.csvProcessor.processCSVStream(readStream, entry.fileName);

      if (processedBuffer) {
        await this.uploadProcessedCSV(bucket, outputKey, processedBuffer, entry, context);
      } else {
        await this.uploadOriginalFileAsFallback(
          entry,
          zipfile,
          bucket,
          outputKey,
          context,
          'processing_failed'
        );
      }
    } catch (csvError) {
      logger.error('CSV processing error, uploading original file', csvError as Error, {
        fileName: entry.fileName,
        outputKey,
        requestId: context.requestId,
        operation: 'csv_processing_error',
      });

      await this.uploadOriginalFileAsFallback(
        entry,
        zipfile,
        bucket,
        outputKey,
        context,
        'processing_error'
      );
    }
  }

  /**
   * Handles direct file upload (non-CSV files)
   */
  private async handleDirectUpload(
    entry: yauzl.Entry,
    zipfile: yauzl.ZipFile,
    bucket: string,
    outputKey: string,
    context: ProcessingContext
  ): Promise<void> {
    const readStream = await openZipEntryStream(zipfile, entry, context.requestId);
    await this.s3Service.uploadStream(bucket, outputKey, readStream);

    logger.debug('Successfully uploaded extracted file', {
      fileName: entry.fileName,
      outputKey,
      sizeBytes: entry.uncompressedSize,
      requestId: context.requestId,
      operation: 'non_csv_upload',
    });
  }

  /**
   * Uploads processed CSV buffer to S3
   */
  private async uploadProcessedCSV(
    bucket: string,
    outputKey: string,
    processedBuffer: Buffer,
    entry: yauzl.Entry,
    context: ProcessingContext
  ): Promise<void> {
    await this.s3Service.uploadBuffer(bucket, outputKey, processedBuffer, {
      contentType: 'text/csv',
    });

    logger.info('Successfully processed and uploaded CSV', {
      fileName: entry.fileName,
      outputKey,
      originalSize: entry.uncompressedSize,
      processedSize: processedBuffer.length,
      requestId: context.requestId,
      operation: 'csv_processing_success',
    });
  }

  /**
   * Uploads original file as fallback when CSV processing fails
   * Eliminates the deeply nested callback retry logic
   */
  private async uploadOriginalFileAsFallback(
    entry: yauzl.Entry,
    zipfile: yauzl.ZipFile,
    bucket: string,
    outputKey: string,
    context: ProcessingContext,
    fallbackReason: 'processing_failed' | 'processing_error'
  ): Promise<void> {
    logger.warn('CSV processing failed, uploading original file', {
      fileName: entry.fileName,
      outputKey,
      requestId: context.requestId,
      operation: 'csv_processing_fallback',
      fallbackReason,
    });

    try {
      // Re-open stream for fallback upload
      const retryStream = await openZipEntryStream(zipfile, entry, context.requestId);
      await this.s3Service.uploadStream(bucket, outputKey, retryStream);

      logger.debug('Successfully uploaded original file as fallback', {
        fileName: entry.fileName,
        outputKey,
        requestId: context.requestId,
        operation: 'fallback_upload_success',
      });
    } catch (retryError) {
      logger.error('Failed to upload original file as fallback', retryError as Error, {
        fileName: entry.fileName,
        outputKey,
        requestId: context.requestId,
        operation: 'fallback_upload_error',
      });
      throw retryError;
    }
  }
}
