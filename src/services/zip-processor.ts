import * as yauzl from 'yauzl';
import type { ProcessingContext, ProcessingResult, ZipEntry } from '../types';
import config from '../utils/config';
import logger from '../utils/logger';
import { CSVProcessor } from './csv-processor';
import { getFilenameDebugInfo } from './filename-parser';
import { S3Service } from './s3-service';

export class ZipProcessor {
  private s3Service: S3Service;
  private csvProcessor: CSVProcessor;

  constructor(s3Service?: S3Service) {
    this.s3Service = s3Service || new S3Service();
    this.csvProcessor = new CSVProcessor({
      timestampColumn: config.csvTimestampColumn,
      dateFormat: 'ISO',
      skipMalformed: config.csvSkipMalformed,
    });
  }

  /**
   * Main entry point for processing a zip file from S3
   */
  async processZipFile(bucket: string, key: string, requestId?: string): Promise<ProcessingResult> {
    const context: ProcessingContext = {
      requestId: requestId || 'unknown',
      bucket,
      inputKey: key,
      outputPath: config.outputPath,
      startTime: new Date(),
    };

    logger.logProcessingStart(bucket, key, requestId);

    try {
      // Download zip file as buffer
      const zipBuffer = await this.s3Service.getObjectAsBuffer(bucket, key);

      logger.info('Downloaded zip file from S3', {
        bucket,
        key,
        sizeBytes: zipBuffer.length,
        requestId,
      });

      // Process the zip file
      const result = await this.extractZipFile(zipBuffer, context);

      logger.logProcessingEnd(bucket, key, result.filesProcessed, requestId);

      return result;
    } catch (error) {
      const errorMsg = `Failed to process zip file: ${error instanceof Error ? error.message : String(error)}`;
      logger.error(errorMsg, error as Error, { bucket, key, requestId });

      return {
        success: false,
        filesProcessed: 0,
        errors: [errorMsg],
      };
    }
  }

  /**
   * Extracts files from a zip buffer using yauzl
   */
  private async extractZipFile(
    zipBuffer: Buffer,
    context: ProcessingContext
  ): Promise<ProcessingResult> {
    return new Promise(resolve => {
      const result: ProcessingResult = {
        success: true,
        filesProcessed: 0,
        errors: [],
      };

      yauzl.fromBuffer(zipBuffer, { lazyEntries: true }, (err, zipfile) => {
        if (err) {
          logger.error('Failed to open zip file', err, {
            operation: 'zip_open',
            requestId: context.requestId,
          });

          resolve({
            success: false,
            filesProcessed: 0,
            errors: [`Failed to open zip file: ${err.message}`],
          });
          return;
        }

        if (!zipfile) {
          resolve({
            success: false,
            filesProcessed: 0,
            errors: ['Zip file object is null'],
          });
          return;
        }

        // Set up event handlers
        zipfile.readEntry();

        zipfile.on('entry', async entry => {
          try {
            await this.processZipEntry(entry, zipfile, context);
            result.filesProcessed++;
            zipfile.readEntry(); // Continue to next entry
          } catch (error) {
            const errorMsg = `Failed to process entry ${entry.fileName}: ${error instanceof Error ? error.message : String(error)}`;
            logger.error(errorMsg, error as Error, {
              fileName: entry.fileName,
              requestId: context.requestId,
            });

            if (!result.errors) {
              result.errors = [];
            }
            result.errors.push(errorMsg);

            // Continue processing other files despite individual failures
            zipfile.readEntry();
          }
        });

        zipfile.on('end', () => {
          // Determine overall success based on whether we processed any files
          // and if there were any critical errors
          if (result.filesProcessed === 0 && result.errors && result.errors.length > 0) {
            result.success = false;
          }

          logger.info('Zip file processing completed', {
            filesProcessed: result.filesProcessed,
            errors: result.errors?.length || 0,
            requestId: context.requestId,
            processingTimeMs: Date.now() - context.startTime.getTime(),
          });

          resolve(result);
        });

        zipfile.on('error', error => {
          logger.error('Zip file processing error', error, {
            operation: 'zip_processing',
            requestId: context.requestId,
          });

          resolve({
            success: false,
            filesProcessed: result.filesProcessed,
            errors: [`Zip processing error: ${error.message}`],
          });
        });
      });
    });
  }

  /**
   * Processes an individual zip entry
   */
  private async processZipEntry(
    entry: yauzl.Entry,
    zipfile: yauzl.ZipFile,
    context: ProcessingContext
  ): Promise<void> {
    // Skip directories
    if (/\/$/.test(entry.fileName)) {
      logger.debug('Skipping directory entry', {
        fileName: entry.fileName,
        requestId: context.requestId,
      });
      return;
    }

    // Skip empty files
    if (entry.uncompressedSize === 0) {
      logger.debug('Skipping empty file', {
        fileName: entry.fileName,
        requestId: context.requestId,
      });
      return;
    }

    // Parse filename to get stem name and debug info
    const debugInfo = getFilenameDebugInfo(entry.fileName);
    const stemName = debugInfo.stemName;

    logger.debug('Processing zip entry', {
      fileName: entry.fileName,
      stemName,
      sizeBytes: entry.uncompressedSize,
      debugInfo,
      requestId: context.requestId,
    });

    // Construct output path
    const outputKey = `${context.outputPath}${stemName}/${entry.fileName}`;

    // Extract file content and upload to S3
    await this.extractAndUploadFile(entry, zipfile, context.bucket, outputKey, context);

    logger.logFileExtracted(entry.fileName, stemName, outputKey);
  }

  /**
   * Extracts a single file from the zip and uploads it to S3
   */
  private async extractAndUploadFile(
    entry: yauzl.Entry,
    zipfile: yauzl.ZipFile,
    bucket: string,
    outputKey: string,
    context: ProcessingContext
  ): Promise<void> {
    return new Promise((resolve, reject) => {
      zipfile.openReadStream(entry, async (err, readStream) => {
        if (err) {
          logger.error('Failed to open read stream for zip entry', err, {
            fileName: entry.fileName,
            requestId: context.requestId,
          });
          reject(err);
          return;
        }

        if (!readStream) {
          const error = new Error('Read stream is null');
          reject(error);
          return;
        }

        try {
          // Check if this is a CSV file and CSV processing is enabled
          const isCSV = this.csvProcessor.isCSVFile(entry.fileName);

          if (isCSV && config.csvProcessingEnabled) {
            logger.info('Processing CSV file', {
              fileName: entry.fileName,
              outputKey,
              requestId: context.requestId,
              operation: 'csv_processing_start',
            });

            try {
              const processedBuffer = await this.csvProcessor.processCSVStream(
                readStream,
                entry.fileName
              );

              if (processedBuffer) {
                // Upload processed CSV
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
              } else {
                // Fallback: upload original file if processing failed
                logger.warn('CSV processing failed, uploading original file', {
                  fileName: entry.fileName,
                  outputKey,
                  requestId: context.requestId,
                  operation: 'csv_processing_fallback',
                });

                // Need to re-open the stream since it was consumed during processing attempt
                zipfile.openReadStream(entry, async (retryErr, retryStream) => {
                  if (retryErr || !retryStream) {
                    reject(retryErr || new Error('Failed to reopen stream for fallback upload'));
                    return;
                  }

                  try {
                    await this.s3Service.uploadStream(bucket, outputKey, retryStream);
                    resolve();
                  } catch (retryUploadError) {
                    reject(retryUploadError);
                  }
                });
                return; // Early return to avoid duplicate resolve
              }
            } catch (csvError) {
              // Error handling: fallback to original upload
              logger.error('CSV processing error, uploading original file', csvError as Error, {
                fileName: entry.fileName,
                outputKey,
                requestId: context.requestId,
                operation: 'csv_processing_error',
              });

              // Need to re-open the stream since it was consumed during processing attempt
              zipfile.openReadStream(entry, async (retryErr, retryStream) => {
                if (retryErr || !retryStream) {
                  reject(retryErr || new Error('Failed to reopen stream for fallback upload'));
                  return;
                }

                try {
                  await this.s3Service.uploadStream(bucket, outputKey, retryStream);
                  resolve();
                } catch (retryUploadError) {
                  reject(retryUploadError);
                }
              });
              return; // Early return to avoid duplicate resolve
            }
          } else {
            // Non-CSV file - use existing streaming upload
            await this.s3Service.uploadStream(bucket, outputKey, readStream);

            logger.debug('Successfully uploaded extracted file', {
              fileName: entry.fileName,
              outputKey,
              sizeBytes: entry.uncompressedSize,
              requestId: context.requestId,
              operation: 'non_csv_upload',
            });
          }

          resolve();
        } catch (uploadError) {
          logger.error('Failed to upload extracted file to S3', uploadError as Error, {
            fileName: entry.fileName,
            outputKey,
            requestId: context.requestId,
          });
          reject(uploadError);
        }
      });
    });
  }

  /**
   * Gets information about zip file entries without extracting
   */
  async getZipFileInfo(bucket: string, key: string): Promise<ZipEntry[]> {
    try {
      const zipBuffer = await this.s3Service.getObjectAsBuffer(bucket, key);

      return new Promise((resolve, reject) => {
        const entries: ZipEntry[] = [];

        yauzl.fromBuffer(zipBuffer, { lazyEntries: true }, (err, zipfile) => {
          if (err) {
            reject(err);
            return;
          }

          if (!zipfile) {
            reject(new Error('Zip file object is null'));
            return;
          }

          zipfile.readEntry();

          zipfile.on('entry', entry => {
            entries.push({
              fileName: entry.fileName,
              size: entry.uncompressedSize,
              isDirectory: /\/$/.test(entry.fileName),
            });
            zipfile.readEntry();
          });

          zipfile.on('end', () => {
            resolve(entries);
          });

          zipfile.on('error', error => {
            reject(error);
          });
        });
      });
    } catch (error) {
      logger.error('Failed to get zip file info', error as Error, { bucket, key });
      throw error;
    }
  }

  /**
   * Validates a zip file without processing it
   */
  async validateZipFile(bucket: string, key: string): Promise<boolean> {
    try {
      const entries = await this.getZipFileInfo(bucket, key);

      // Basic validation - check if we have any extractable files
      const extractableFiles = entries.filter(entry => !entry.isDirectory && entry.size > 0);

      return extractableFiles.length > 0;
    } catch {
      return false;
    }
  }
}
