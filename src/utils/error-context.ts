/**
 * Error context utilities for structured error handling without exposing sensitive data
 * Provides debugging information while maintaining security
 */

import { sanitizeForLogging } from './data-sanitizer';

export interface ErrorContext {
  operation: string;
  phase?: string;
  component?: string;
  bucket?: string;
  key?: string;
  filename?: string;
  requestId?: string;
  // biome-ignore lint/suspicious/noExplicitAny: Generic metadata requires any type for flexibility
  metadata?: Record<string, any>;
  timing?: {
    startTime: Date;
    duration?: number;
  };
  resources?: {
    memoryUsageMB?: number;
    processingProgress?: string;
  };
}

export interface StructuredError extends Error {
  context?: ErrorContext;
  errorCode?: string;
  retryable?: boolean;
  severity?: 'low' | 'medium' | 'high' | 'critical';
}

export class ErrorContextBuilder {
  private context: ErrorContext;

  constructor(operation: string) {
    this.context = {
      operation,
    };
  }

  /**
   * Sets the processing phase (e.g., 'download', 'extract', 'upload')
   */
  setPhase(phase: string): this {
    this.context.phase = phase;
    return this;
  }

  /**
   * Sets the component where the error occurred
   */
  setComponent(component: string): this {
    this.context.component = component;
    return this;
  }

  /**
   * Sets S3 bucket and key (will be sanitized)
   */
  setS3Location(bucket: string, key?: string): this {
    this.context.bucket = bucket;
    if (key) {
      this.context.key = sanitizeForLogging.s3Key(key);
    }
    return this;
  }

  /**
   * Sets filename (will be sanitized)
   */
  setFilename(filename: string): this {
    this.context.filename = sanitizeForLogging.filename(filename);
    return this;
  }

  /**
   * Sets request ID for tracing
   */
  setRequestId(requestId: string): this {
    this.context.requestId = requestId;
    return this;
  }

  /**
   * Adds metadata (will be sanitized)
   */
  // biome-ignore lint/suspicious/noExplicitAny: Generic metadata parameter requires any type for flexibility
  setMetadata(metadata: Record<string, any>): this {
    this.context.metadata = sanitizeForLogging.context(metadata);
    return this;
  }

  /**
   * Sets timing information
   */
  setTiming(startTime: Date, duration?: number): this {
    this.context.timing = {
      startTime,
      duration: duration || Date.now() - startTime.getTime(),
    };
    return this;
  }

  /**
   * Sets resource usage information
   */
  setResources(memoryUsageMB?: number, processingProgress?: string): this {
    this.context.resources = {
      memoryUsageMB,
      processingProgress,
    };
    return this;
  }

  /**
   * Builds the final sanitized context
   */
  build(): ErrorContext {
    return { ...this.context };
  }

  /**
   * Creates a structured error with the built context
   */
  createError(
    message: string,
    originalError?: Error,
    options?: {
      errorCode?: string;
      retryable?: boolean;
      severity?: 'low' | 'medium' | 'high' | 'critical';
    }
  ): StructuredError {
    const sanitizedMessage = sanitizeForLogging.errorMessage(message, this.context);

    const error: StructuredError = new Error(sanitizedMessage);
    error.context = this.build();
    error.name = originalError?.name || 'StructuredError';

    if (options) {
      error.errorCode = options.errorCode;
      error.retryable = options.retryable;
      error.severity = options.severity;
    }

    // Preserve original stack trace if available, but sanitize it
    if (originalError?.stack) {
      error.stack = sanitizeForLogging.stackTrace(originalError.stack);
    }

    return error;
  }
}

/**
 * Pre-configured error context builders for common scenarios
 */
// biome-ignore lint/complexity/noStaticOnlyClass: Factory class pattern for error context builders
export class CommonErrorContexts {
  /**
   * Creates error context for S3 operations
   */
  static s3Operation(
    operation: string,
    bucket: string,
    key?: string,
    requestId?: string
  ): ErrorContextBuilder {
    return new ErrorContextBuilder(operation)
      .setComponent('S3Service')
      .setS3Location(bucket, key)
      .setRequestId(requestId || 'unknown');
  }

  /**
   * Creates error context for zip processing operations
   */
  static zipProcessing(phase: string, filename?: string, requestId?: string): ErrorContextBuilder {
    const builder = new ErrorContextBuilder('zip_processing')
      .setComponent('ZipProcessor')
      .setPhase(phase)
      .setRequestId(requestId || 'unknown');

    if (filename) {
      builder.setFilename(filename);
    }

    return builder;
  }

  /**
   * Creates error context for CSV processing operations
   */
  static csvProcessing(phase: string, filename?: string, requestId?: string): ErrorContextBuilder {
    const builder = new ErrorContextBuilder('csv_processing')
      .setComponent('CSVProcessor')
      .setPhase(phase)
      .setRequestId(requestId || 'unknown');

    if (filename) {
      builder.setFilename(filename);
    }

    return builder;
  }

  /**
   * Creates error context for file upload operations
   */
  static fileUpload(
    bucket: string,
    key?: string,
    filename?: string,
    requestId?: string
  ): ErrorContextBuilder {
    const builder = new ErrorContextBuilder('file_upload')
      .setComponent('FileUploadHandler')
      .setS3Location(bucket, key)
      .setRequestId(requestId || 'unknown');

    if (filename) {
      builder.setFilename(filename);
    }

    return builder;
  }

  /**
   * Creates error context for memory-related errors
   */
  static memoryError(
    operation: string,
    memoryUsageMB?: number,
    requestId?: string
  ): ErrorContextBuilder {
    return new ErrorContextBuilder(operation)
      .setComponent('MemoryMonitor')
      .setPhase('memory_check')
      .setResources(memoryUsageMB)
      .setRequestId(requestId || 'unknown');
  }

  /**
   * Creates error context for validation errors
   */
  static validation(
    validationType: string,
    inputData?: string,
    requestId?: string
  ): ErrorContextBuilder {
    const builder = new ErrorContextBuilder('validation')
      .setComponent('ValidationUtils')
      .setPhase(validationType)
      .setRequestId(requestId || 'unknown');

    if (inputData) {
      // Sanitize input data that might contain sensitive information
      const sanitizedInput = sanitizeForLogging.context({ inputData });
      builder.setMetadata(sanitizedInput);
    }

    return builder;
  }
}

/**
 * Error classification utilities
 */
// biome-ignore lint/complexity/noStaticOnlyClass: Utility class pattern for error classification functions
export class ErrorClassifier {
  /**
   * Determines if an error is retryable based on context and error type
   */
  static isRetryable(error: Error | StructuredError): boolean {
    if ('retryable' in error && error.retryable !== undefined) {
      return error.retryable;
    }

    // Default classification based on error type
    const retryableErrorTypes = [
      'NetworkingError',
      'TimeoutError',
      'ThrottlingException',
      'ServiceUnavailable',
      'InternalServerError',
      'RequestTimeout',
      'Connection',
      'ECONNRESET',
      'ETIMEDOUT',
    ];

    return retryableErrorTypes.some(
      type => error.name.includes(type) || error.message.includes(type)
    );
  }

  /**
   * Determines error severity based on context and error type
   */
  static getSeverity(error: Error | StructuredError): 'low' | 'medium' | 'high' | 'critical' {
    if ('severity' in error && error.severity) {
      return error.severity;
    }

    // Default classification based on error patterns
    if (error.message.includes('memory') || error.message.includes('Memory')) {
      return 'high';
    }

    if (error.message.includes('timeout') || error.message.includes('Timeout')) {
      return 'medium';
    }

    if (error.message.includes('validation') || error.message.includes('Validation')) {
      return 'low';
    }

    if (
      error.name.toLowerCase().includes('security') ||
      error.message.toLowerCase().includes('security')
    ) {
      return 'critical';
    }

    return 'medium';
  }

  /**
   * Creates a sanitized error summary for logging
   */
  // biome-ignore lint/suspicious/noExplicitAny: Error summary requires any type for generic error properties
  static createErrorSummary(error: Error | StructuredError): Record<string, any> {
    // biome-ignore lint/suspicious/noExplicitAny: Error summary object requires any type for flexibility
    const summary: Record<string, any> = {
      errorName: error.name,
      errorMessage: sanitizeForLogging.errorMessage(error.message),
      retryable: ErrorClassifier.isRetryable(error),
      severity: ErrorClassifier.getSeverity(error),
    };

    if ('context' in error && error.context) {
      summary.context = sanitizeForLogging.context(error.context);
    }

    if ('errorCode' in error && error.errorCode) {
      summary.errorCode = error.errorCode;
    }

    if (error.stack) {
      summary.stackTrace = sanitizeForLogging.stackTrace(error.stack);
    }

    return summary;
  }
}

/**
 * Convenience function to create error contexts
 */
export const createErrorContext = {
  s3: CommonErrorContexts.s3Operation,
  zip: CommonErrorContexts.zipProcessing,
  csv: CommonErrorContexts.csvProcessing,
  upload: CommonErrorContexts.fileUpload,
  memory: CommonErrorContexts.memoryError,
  validation: CommonErrorContexts.validation,
  general: (operation: string) => new ErrorContextBuilder(operation),
};
