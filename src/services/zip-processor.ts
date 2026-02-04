import * as path from 'node:path';
import type * as yauzl from 'yauzl';
import type { ProcessingContext, ProcessingResult, ZipEntry } from '../types';
import config from '../utils/config';
import { FileUploadHandler } from '../utils/file-upload-handler';
import logger from '../utils/logger';
import { memoryMonitor } from '../utils/memory-monitor';
import { createZipFileFromBuffer } from '../utils/zip-utils';
import { CSVProcessor } from './csv-processor';
import { getFilenameDebugInfo } from './filename-parser';
import { S3Service } from './s3-service';

export class ZipProcessor {
  public s3Service: S3Service;
  public csvProcessor: CSVProcessor;
  private uploadHandler: FileUploadHandler;

  /**
   * Sanitizes filename to prevent path traversal attacks
   * Removes directory separators, parent references, and special characters
   */
  private sanitizeFilename(filename: string): string {
    if (!filename) {
      return 'unnamed_file';
    }

    // Extract just the basename to prevent path traversal
    let sanitized = path.basename(filename);

    // Remove or replace dangerous characters
    // biome-ignore lint/suspicious/noControlCharactersInRegex: Security sanitization requires control character replacement
    sanitized = sanitized.replace(/[<>:"/\\|?*\x00-\x1f]/g, '_');

    // Prevent empty filename after sanitization
    if (!sanitized || sanitized.trim() === '') {
      return 'unnamed_file';
    }

    // Limit filename length
    if (sanitized.length > 255) {
      const ext = path.extname(sanitized);
      const name = path.basename(sanitized, ext);
      sanitized = name.substring(0, 250 - ext.length) + ext;
    }

    return sanitized;
  }

  constructor(s3Service?: S3Service, csvProcessor?: CSVProcessor) {
    this.s3Service = s3Service || new S3Service();
    this.csvProcessor =
      csvProcessor ||
      new CSVProcessor({
        timestampColumn: config.csvTimestampColumn,
        dateFormat: 'ISO',
        skipMalformed: config.csvSkipMalformed,
      });
    this.uploadHandler = new FileUploadHandler(this.s3Service, this.csvProcessor);
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
      // Check memory before starting
      if (!memoryMonitor.checkMemoryUsage('zip_processing_start')) {
        throw new Error('Cannot start zip processing - memory usage too high');
      }

      // Validate file size before downloading
      const fileSizeValidation = await this.s3Service.validateFileSize(bucket, key);
      if (!fileSizeValidation.isValid) {
        logger.warn('Skipping file due to size validation failure', {
          bucket,
          key,
          reason: fileSizeValidation.reason,
          requestId,
        });
        return {
          success: false,
          filesProcessed: 0,
          errors: [fileSizeValidation.reason || 'File size validation failed'],
        };
      }

      // Download zip file as buffer
      const zipBuffer = await this.s3Service.getObjectAsBuffer(bucket, key);

      logger.info('Downloaded zip file from S3', {
        bucket,
        key,
        sizeBytes: zipBuffer.length,
        requestId,
      });

      // Check memory after download
      if (!memoryMonitor.checkMemoryUsage('zip_download_complete')) {
        throw new Error('Memory usage too high after zip download');
      }

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
   * Refactored to use async/await instead of complex callbacks
   */
  private async extractZipFile(
    zipBuffer: Buffer,
    context: ProcessingContext
  ): Promise<ProcessingResult> {
    const result: ProcessingResult = {
      success: true,
      filesProcessed: 0,
      errors: [],
    };

    // Track decompression limits to prevent zip bombs
    const totalUncompressedSize = 0;
    const maxUncompressedSize = 200 * 1024 * 1024; // 200MB total decompressed
    const maxFileCount = 10000; // Maximum number of files

    try {
      const zipfile = await createZipFileFromBuffer(zipBuffer, context.requestId);

      await this.processZipFileEntries(zipfile, result, context, {
        totalUncompressedSize,
        maxUncompressedSize,
        maxFileCount,
      });

      // Determine overall success
      if (result.filesProcessed === 0 && result.errors && result.errors.length > 0) {
        result.success = false;
      }

      logger.info('Zip file processing completed', {
        filesProcessed: result.filesProcessed,
        errors: result.errors?.length || 0,
        requestId: context.requestId,
        processingTimeMs: Date.now() - context.startTime.getTime(),
      });

      return result;
    } catch (error) {
      const errorMsg = `Failed to process zip file: ${error instanceof Error ? error.message : String(error)}`;
      logger.error(errorMsg, error as Error, {
        operation: 'zip_processing',
        requestId: context.requestId,
      });

      return {
        success: false,
        filesProcessed: result.filesProcessed,
        errors: [errorMsg],
      };
    }
  }

  /**
   * Processes all entries in a zip file with proper resource management
   */
  private async processZipFileEntries(
    zipfile: yauzl.ZipFile,
    result: ProcessingResult,
    context: ProcessingContext,
    limits: {
      totalUncompressedSize: number;
      maxUncompressedSize: number;
      maxFileCount: number;
    }
  ): Promise<void> {
    return new Promise((resolve, reject) => {
      zipfile.readEntry();

      zipfile.on('entry', async entry => {
        try {
          // Check file count limit
          if (result.filesProcessed >= limits.maxFileCount) {
            const errorMsg = `Too many files in zip: exceeds ${limits.maxFileCount} limit`;
            logger.warn('Zip file contains too many files', {
              filesProcessed: result.filesProcessed,
              maxFileCount: limits.maxFileCount,
              requestId: context.requestId,
            });
            if (!result.errors) result.errors = [];
            result.errors.push(errorMsg);
            result.success = false;
            resolve();
            return;
          }

          // Check individual file size and total decompression size
          const fileSize = entry.uncompressedSize;
          const compressedSize = entry.compressedSize || 1; // Avoid division by zero

          // Enhanced compression ratio validation
          const compressionRatio = fileSize / compressedSize;
          const maxCompressionRatio = 100; // 100:1 compression ratio limit

          if (compressionRatio > maxCompressionRatio) {
            const errorMsg = `Suspicious compression ratio detected: ${Math.round(compressionRatio)}:1 exceeds limit of ${maxCompressionRatio}:1 for file ${entry.fileName}`;
            logger.warn('Potential zip bomb detected - high compression ratio', {
              fileName: entry.fileName,
              uncompressedSize: fileSize,
              compressedSize,
              compressionRatio: Math.round(compressionRatio * 100) / 100,
              maxCompressionRatio,
              requestId: context.requestId,
            });
            if (!result.errors) result.errors = [];
            result.errors.push(errorMsg);
            result.success = false;
            resolve();
            return;
          }

          if (limits.totalUncompressedSize + fileSize > limits.maxUncompressedSize) {
            const errorMsg = `Decompression size limit exceeded: ${limits.totalUncompressedSize + fileSize} bytes would exceed ${limits.maxUncompressedSize} bytes`;
            logger.warn('Zip bomb protection triggered', {
              totalUncompressedSize: limits.totalUncompressedSize,
              fileSize,
              maxUncompressedSize: limits.maxUncompressedSize,
              fileName: entry.fileName,
              requestId: context.requestId,
            });
            if (!result.errors) result.errors = [];
            result.errors.push(errorMsg);
            result.success = false;
            resolve();
            return;
          }

          limits.totalUncompressedSize += fileSize;

          // Check memory before processing each entry
          if (!memoryMonitor.checkMemoryUsage('zip_entry_processing')) {
            const errorMsg = 'Memory usage too high - aborting zip entry processing';
            logger.warn('Zip entry processing aborted due to memory pressure', {
              fileName: entry.fileName,
              filesProcessedSoFar: result.filesProcessed,
              requestId: context.requestId,
            });
            if (!result.errors) result.errors = [];
            result.errors.push(errorMsg);
            result.success = false;
            resolve();
            return;
          }

          await this.processZipEntry(entry, zipfile, context);
          result.filesProcessed++;

          // Force garbage collection every 10 files if available
          if (result.filesProcessed % 10 === 0) {
            memoryMonitor.forceGarbageCollection('zip_entry_batch_complete');
          }

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
        resolve();
      });

      zipfile.on('error', error => {
        reject(error);
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
      originalFilename: debugInfo.originalFilename,
      isValid: debugInfo.isValid,
      baseName: debugInfo.baseName,
      extension: debugInfo.extension,
      requestId: context.requestId,
    });

    // Construct output path with sanitized filename to prevent path traversal
    const sanitizedFilename = this.sanitizeFilename(entry.fileName);
    const outputKey = `${context.outputPath}${stemName}/${sanitizedFilename}`;

    // Extract file content and upload to S3
    const isCSV = this.csvProcessor.isCSVFile(entry.fileName);
    await this.uploadHandler.uploadFileFromZip(entry, zipfile, {
      bucket: context.bucket,
      outputKey,
      context,
      isCSV,
      csvProcessingEnabled: config.csvProcessingEnabled,
    });

    logger.logFileExtracted(entry.fileName, stemName, outputKey);
  }

  /**
   * Gets information about zip file entries without extracting
   * Refactored to use async/await instead of complex callbacks
   */
  async getZipFileInfo(bucket: string, key: string): Promise<ZipEntry[]> {
    try {
      const zipBuffer = await this.s3Service.getObjectAsBuffer(bucket, key);
      const zipfile = await createZipFileFromBuffer(zipBuffer);

      return await this.extractZipEntryInfo(zipfile);
    } catch (error) {
      logger.error('Failed to get zip file info', error as Error, { bucket, key });
      throw error;
    }
  }

  /**
   * Extracts entry information from a zip file
   */
  private async extractZipEntryInfo(zipfile: yauzl.ZipFile): Promise<ZipEntry[]> {
    return new Promise((resolve, reject) => {
      const entries: ZipEntry[] = [];

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
