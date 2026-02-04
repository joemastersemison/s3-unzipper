import { Logger } from '@aws-lambda-powertools/logger';
import type { LoggingContext } from '../types';
import config from './config';
import { sanitizeForLogging } from './data-sanitizer';

class CustomLogger {
  private logger: Logger;

  constructor() {
    this.logger = new Logger({
      serviceName: 's3-unzipper',
      logLevel: config.logLevel as 'DEBUG' | 'INFO' | 'WARN' | 'ERROR',
    });
  }

  debug(message: string, context?: LoggingContext): void {
    const sanitizedMessage = sanitizeForLogging.errorMessage(message, context);
    if (context) {
      const sanitizedContext = sanitizeForLogging.context(context);
      this.logger.debug(sanitizedMessage, sanitizedContext);
    } else {
      this.logger.debug(sanitizedMessage);
    }
  }

  info(message: string, context?: LoggingContext): void {
    const sanitizedMessage = sanitizeForLogging.errorMessage(message, context);
    if (context) {
      const sanitizedContext = sanitizeForLogging.context(context);
      this.logger.info(sanitizedMessage, sanitizedContext);
    } else {
      this.logger.info(sanitizedMessage);
    }
  }

  warn(message: string, context?: LoggingContext): void {
    const sanitizedMessage = sanitizeForLogging.errorMessage(message, context);
    if (context) {
      const sanitizedContext = sanitizeForLogging.context(context);
      this.logger.warn(sanitizedMessage, sanitizedContext);
    } else {
      this.logger.warn(sanitizedMessage);
    }
  }

  error(message: string, error?: Error, context?: LoggingContext): void {
    const sanitizedMessage = sanitizeForLogging.errorMessage(message, context);
    const logContext = {
      ...context,
      ...(error && {
        error: {
          name: error.name,
          message: sanitizeForLogging.errorMessage(error.message, context),
          stack: sanitizeForLogging.stackTrace(error.stack || ''),
        },
      }),
    };
    const sanitizedContext = sanitizeForLogging.context(logContext);
    this.logger.error(sanitizedMessage, sanitizedContext);
  }

  addContext(context: LoggingContext): void {
    // The addContext method might not be available in the current version
    // We'll store context manually if needed
    this.logger.addPersistentLogAttributes(context);
  }

  clearContext(): void {
    // Clear persistent log attributes (simplified approach)
    this.logger.addPersistentLogAttributes({});
  }

  // Helper methods for common operations
  logProcessingStart(bucket: string, key: string, requestId?: string): void {
    this.info('Starting zip file processing', {
      bucket,
      key,
      requestId,
      operation: 'process_start',
    });
  }

  logProcessingEnd(bucket: string, key: string, filesProcessed: number, requestId?: string): void {
    this.info('Completed zip file processing', {
      bucket,
      key,
      filesProcessed,
      requestId,
      operation: 'process_complete',
    });
  }

  logFileExtracted(fileName: string, stemName: string, outputPath: string): void {
    this.debug('File extracted and uploaded', {
      fileName,
      stemName,
      outputPath,
      operation: 'file_extracted',
    });
  }

  logError(operation: string, error: Error, context?: LoggingContext): void {
    this.error(`Error during ${operation}`, error, {
      ...context,
      operation,
    });
  }

  // CSV-specific logging methods
  logCSVProcessingStart(fileName: string, requestId?: string): void {
    this.info('Starting CSV processing', {
      fileName,
      requestId,
      operation: 'csv_process_start',
    });
  }

  logCSVProcessingComplete(fileName: string, rowsProcessed: number, requestId?: string): void {
    this.info('CSV processing completed', {
      fileName,
      rowsProcessed,
      requestId,
      operation: 'csv_process_complete',
    });
  }

  logCSVProcessingError(fileName: string, error: Error, requestId?: string): void {
    this.warn('CSV processing failed, using original file', {
      fileName,
      error: error.message,
      requestId,
      operation: 'csv_process_fallback',
    });
  }

  logCSVSkipped(fileName: string, reason: string, requestId?: string): void {
    this.info('CSV processing skipped', {
      fileName,
      reason,
      requestId,
      operation: 'csv_process_skipped',
    });
  }

  logCSVFallback(fileName: string, reason: string, requestId?: string): void {
    this.warn('CSV processing fallback triggered', {
      fileName,
      reason,
      requestId,
      operation: 'csv_process_fallback',
    });
  }
}

// Export singleton instance
export const logger = new CustomLogger();
export default logger;
