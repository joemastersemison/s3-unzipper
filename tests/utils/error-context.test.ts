import {
  CommonErrorContexts,
  createErrorContext,
  ErrorClassifier,
  ErrorContextBuilder,
} from '../../src/utils/error-context';

describe('ErrorContextBuilder', () => {
  it('should build basic error context', () => {
    const builder = new ErrorContextBuilder('test_operation');
    const context = builder.build();

    expect(context.operation).toBe('test_operation');
  });

  it('should build complex error context', () => {
    const startTime = new Date();
    const builder = new ErrorContextBuilder('s3_upload')
      .setPhase('buffer_creation')
      .setComponent('S3Service')
      .setS3Location('test-bucket', 'path/to/file.csv')
      .setRequestId('req-123')
      .setTiming(startTime, 1500)
      .setResources(256, '50%')
      .setMetadata({ fileSize: 1024, contentType: 'text/csv' });

    const context = builder.build();

    expect(context.operation).toBe('s3_upload');
    expect(context.phase).toBe('buffer_creation');
    expect(context.component).toBe('S3Service');
    expect(context.bucket).toBe('test-bucket');
    expect(context.requestId).toBe('req-123');
    expect(context.timing?.startTime).toBe(startTime);
    expect(context.timing?.duration).toBe(1500);
    expect(context.resources?.memoryUsageMB).toBe(256);
    expect(context.resources?.processingProgress).toBe('50%');
  });

  it('should sanitize sensitive data in S3 location', () => {
    const builder = new ErrorContextBuilder('test').setS3Location(
      'bucket',
      'path/user@example.com/file.csv'
    );

    const context = builder.build();

    expect(context.key).not.toContain('user@example.com');
    expect(context.key).toContain('***');
  });

  it('should create structured error with context', () => {
    const originalError = new Error('Original error message');
    const builder = new ErrorContextBuilder('test_operation')
      .setPhase('validation')
      .setRequestId('req-123');

    const structuredError = builder.createError('Operation failed', originalError, {
      errorCode: 'TEST_ERROR',
      retryable: true,
      severity: 'high',
    });

    expect(structuredError.message).toContain('Operation failed');
    expect(structuredError.context?.operation).toBe('test_operation');
    expect(structuredError.context?.phase).toBe('validation');
    expect(structuredError.errorCode).toBe('TEST_ERROR');
    expect(structuredError.retryable).toBe(true);
    expect(structuredError.severity).toBe('high');
  });
});

describe('CommonErrorContexts', () => {
  it('should create S3 operation context', () => {
    const builder = CommonErrorContexts.s3Operation(
      's3_download',
      'test-bucket',
      'path/file.csv',
      'req-123'
    );

    const context = builder.build();

    expect(context.operation).toBe('s3_download');
    expect(context.component).toBe('S3Service');
    expect(context.bucket).toBe('test-bucket');
    expect(context.requestId).toBe('req-123');
  });

  it('should create zip processing context', () => {
    const builder = CommonErrorContexts.zipProcessing('extraction', 'archive.zip', 'req-456');

    const context = builder.build();

    expect(context.operation).toBe('zip_processing');
    expect(context.component).toBe('ZipProcessor');
    expect(context.phase).toBe('extraction');
    expect(context.requestId).toBe('req-456');
  });

  it('should create CSV processing context', () => {
    const builder = CommonErrorContexts.csvProcessing('parsing', 'data.csv', 'req-789');

    const context = builder.build();

    expect(context.operation).toBe('csv_processing');
    expect(context.component).toBe('CSVProcessor');
    expect(context.phase).toBe('parsing');
    expect(context.requestId).toBe('req-789');
  });

  it('should create memory error context', () => {
    const builder = CommonErrorContexts.memoryError('zip_processing', 512, 'req-101112');

    const context = builder.build();

    expect(context.operation).toBe('zip_processing');
    expect(context.component).toBe('MemoryMonitor');
    expect(context.phase).toBe('memory_check');
    expect(context.resources?.memoryUsageMB).toBe(512);
    expect(context.requestId).toBe('req-101112');
  });
});

describe('ErrorClassifier', () => {
  it('should classify retryable errors', () => {
    const retryableError = new Error('NetworkingError occurred');
    const nonRetryableError = new Error('ValidationError occurred');

    expect(ErrorClassifier.isRetryable(retryableError)).toBe(true);
    expect(ErrorClassifier.isRetryable(nonRetryableError)).toBe(false);
  });

  it('should respect explicit retryable flag', () => {
    const explicitlyRetryable = new Error('Test error') as any;
    explicitlyRetryable.retryable = true;

    const explicitlyNonRetryable = new Error('Test error') as any;
    explicitlyNonRetryable.retryable = false;

    expect(ErrorClassifier.isRetryable(explicitlyRetryable)).toBe(true);
    expect(ErrorClassifier.isRetryable(explicitlyNonRetryable)).toBe(false);
  });

  it('should classify error severity', () => {
    const memoryError = new Error('Memory limit exceeded');
    const timeoutError = new Error('Request timeout');
    const validationError = new Error('Validation failed');
    const securityError = new Error('Security violation');

    expect(ErrorClassifier.getSeverity(memoryError)).toBe('high');
    expect(ErrorClassifier.getSeverity(timeoutError)).toBe('medium');
    expect(ErrorClassifier.getSeverity(validationError)).toBe('low');
    expect(ErrorClassifier.getSeverity(securityError)).toBe('critical');
  });

  it('should respect explicit severity', () => {
    const error = new Error('Test error') as any;
    error.severity = 'critical';

    expect(ErrorClassifier.getSeverity(error)).toBe('critical');
  });

  it('should create sanitized error summary', () => {
    const contextBuilder = new ErrorContextBuilder('test_operation').setS3Location(
      'bucket',
      'sensitive/user@example.com/file.csv'
    );

    const error = contextBuilder.createError('Test error with sensitive data');
    const summary = ErrorClassifier.createErrorSummary(error);

    expect(summary.errorName).toBe('StructuredError');
    expect(summary.context.key).not.toContain('user@example.com');
    expect(summary.retryable).toBeDefined();
    expect(summary.severity).toBeDefined();
  });
});

describe('createErrorContext convenience functions', () => {
  it('should provide easy access to common error contexts', () => {
    expect(createErrorContext.s3).toBe(CommonErrorContexts.s3Operation);
    expect(createErrorContext.zip).toBe(CommonErrorContexts.zipProcessing);
    expect(createErrorContext.csv).toBe(CommonErrorContexts.csvProcessing);
    expect(createErrorContext.upload).toBe(CommonErrorContexts.fileUpload);
    expect(createErrorContext.memory).toBe(CommonErrorContexts.memoryError);
    expect(createErrorContext.validation).toBe(CommonErrorContexts.validation);

    const generalBuilder = createErrorContext.general('custom_operation');
    expect(generalBuilder.build().operation).toBe('custom_operation');
  });
});
