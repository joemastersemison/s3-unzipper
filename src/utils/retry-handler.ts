/**
 * Retry handler with exponential backoff for transient failures
 * Provides secure retry logic without exposing sensitive data in error logs
 */

import { sanitizeForLogging } from './data-sanitizer';
import logger from './logger';

export interface RetryOptions {
  maxAttempts: number;
  baseDelayMs: number;
  maxDelayMs: number;
  backoffMultiplier: number;
  retryableErrors: string[];
  timeoutMs?: number;
}

export interface RetryContext {
  operation: string;
  bucket?: string;
  key?: string;
  requestId?: string;
}

export class RetryHandler {
  private static readonly DEFAULT_RETRYABLE_ERRORS = [
    'NetworkingError',
    'TimeoutError',
    'ThrottlingException',
    'ServiceUnavailable',
    'InternalServerError',
    'SlowDown',
    'RequestTimeout',
    'TooManyRequests',
    'Connection',
    'ECONNRESET',
    'ETIMEDOUT',
    'ENOTFOUND',
  ];

  private static readonly DEFAULT_OPTIONS: RetryOptions = {
    maxAttempts: 3,
    baseDelayMs: 1000,
    maxDelayMs: 30000,
    backoffMultiplier: 2,
    retryableErrors: RetryHandler.DEFAULT_RETRYABLE_ERRORS,
    timeoutMs: 60000,
  };

  /**
   * Executes a function with retry logic and exponential backoff
   */
  static async withRetry<T>(
    fn: () => Promise<T>,
    context: RetryContext,
    options: Partial<RetryOptions> = {}
  ): Promise<T> {
    const opts = { ...RetryHandler.DEFAULT_OPTIONS, ...options };
    let lastError: Error | undefined;
    let attempt = 0;

    const sanitizedContext = sanitizeForLogging.context(context);

    logger.debug('Starting operation with retry logic', {
      ...sanitizedContext,
      maxAttempts: opts.maxAttempts,
      baseDelayMs: opts.baseDelayMs,
    });

    while (attempt < opts.maxAttempts) {
      attempt++;

      try {
        const startTime = Date.now();

        // Add timeout wrapper if specified
        const result = opts.timeoutMs
          ? await RetryHandler.withTimeout(fn(), opts.timeoutMs)
          : await fn();

        const duration = Date.now() - startTime;

        if (attempt > 1) {
          logger.info('Operation succeeded after retry', {
            ...sanitizedContext,
            attempt,
            durationMs: duration,
            totalAttempts: attempt,
          });
        } else {
          logger.debug('Operation succeeded on first attempt', {
            ...sanitizedContext,
            durationMs: duration,
          });
        }

        return result;
      } catch (error) {
        lastError = error as Error;
        const isRetryable = RetryHandler.isRetryableError(lastError, opts.retryableErrors);
        const isLastAttempt = attempt >= opts.maxAttempts;

        // Sanitize error information before logging
        const sanitizedErrorMessage = sanitizeForLogging.errorMessage(
          lastError.message,
          sanitizedContext
        );

        const logContext = {
          ...sanitizedContext,
          attempt,
          maxAttempts: opts.maxAttempts,
          errorName: lastError.name,
          errorMessage: sanitizedErrorMessage,
          isRetryable,
          isLastAttempt,
        };

        if (isLastAttempt) {
          logger.error('Operation failed after all retry attempts', lastError, logContext);
          break;
        }

        if (!isRetryable) {
          logger.warn('Operation failed with non-retryable error', logContext);
          break;
        }

        const delayMs = RetryHandler.calculateDelay(attempt - 1, opts);
        logger.warn('Operation failed, retrying after delay', {
          ...logContext,
          delayMs,
          nextAttempt: attempt + 1,
        });

        await RetryHandler.delay(delayMs);
      }
    }

    // If we get here, all attempts failed
    const errorMessage = lastError
      ? sanitizeForLogging.errorMessage(lastError.message, sanitizedContext)
      : 'Unknown error';

    const finalError = new Error(
      `Operation '${context.operation}' failed after ${attempt} attempts: ${errorMessage}`
    );

    // Preserve original error properties but sanitize sensitive data
    if (lastError?.stack) {
      finalError.stack = sanitizeForLogging.stackTrace(lastError.stack);
    }

    throw finalError;
  }

  /**
   * Specialized retry for S3 operations
   */
  static async retryS3Operation<T>(
    operation: () => Promise<T>,
    operationName: string,
    bucket?: string,
    key?: string,
    requestId?: string
  ): Promise<T> {
    const context: RetryContext = {
      operation: operationName,
      bucket,
      key: key ? sanitizeForLogging.s3Key(key) : undefined,
      requestId,
    };

    const s3RetryOptions: Partial<RetryOptions> = {
      maxAttempts: 4, // S3 can benefit from more retries
      baseDelayMs: 500,
      maxDelayMs: 16000,
      backoffMultiplier: 2,
      retryableErrors: [
        ...RetryHandler.DEFAULT_RETRYABLE_ERRORS,
        'NoSuchBucket',
        'AccessDenied', // Sometimes transient due to eventual consistency
        'RequestTimeoutError',
        'SlowDown',
        'BandwidthLimitExceeded',
      ],
      timeoutMs: 45000, // S3 operations can take time
    };

    return RetryHandler.withRetry(operation, context, s3RetryOptions);
  }

  /**
   * Specialized retry for CSV processing operations
   */
  static async retryCSVOperation<T>(
    operation: () => Promise<T>,
    filename: string,
    requestId?: string
  ): Promise<T> {
    const context: RetryContext = {
      operation: 'csv_processing',
      key: sanitizeForLogging.filename(filename),
      requestId,
    };

    const csvRetryOptions: Partial<RetryOptions> = {
      maxAttempts: 2, // CSV processing failures are usually not transient
      baseDelayMs: 1000,
      maxDelayMs: 5000,
      backoffMultiplier: 2,
      retryableErrors: ['NetworkingError', 'TimeoutError', 'EMFILE', 'ENOMEM', 'ENOTFOUND'],
      timeoutMs: 30000,
    };

    return RetryHandler.withRetry(operation, context, csvRetryOptions);
  }

  /**
   * Determines if an error is retryable based on error type and configuration
   */
  private static isRetryableError(error: Error, retryableErrors: string[]): boolean {
    const errorString = error.toString();
    const errorName = error.name;
    const errorMessage = error.message;

    return retryableErrors.some(
      retryableError =>
        errorName.includes(retryableError) ||
        errorMessage.includes(retryableError) ||
        errorString.includes(retryableError)
    );
  }

  /**
   * Calculates delay with exponential backoff and jitter
   */
  private static calculateDelay(attemptNumber: number, options: RetryOptions): number {
    const exponentialDelay = Math.min(
      options.baseDelayMs * options.backoffMultiplier ** attemptNumber,
      options.maxDelayMs
    );

    // Add jitter to prevent thundering herd
    const jitter = Math.random() * 0.1 * exponentialDelay;
    return Math.floor(exponentialDelay + jitter);
  }

  /**
   * Simple delay utility
   */
  private static delay(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  /**
   * Wraps a promise with a timeout
   */
  private static withTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(new Error(`Operation timed out after ${timeoutMs}ms`));
      }, timeoutMs);

      promise
        .then(result => {
          clearTimeout(timer);
          resolve(result);
        })
        .catch(error => {
          clearTimeout(timer);
          reject(error);
        });
    });
  }
}

/**
 * Convenience functions for common retry scenarios
 */
export const retryWithBackoff = {
  s3Operation: RetryHandler.retryS3Operation,
  csvOperation: RetryHandler.retryCSVOperation,
  general: RetryHandler.withRetry,
};
