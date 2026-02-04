import { DataSanitizer, sanitizeForLogging } from '../../src/utils/data-sanitizer';

describe('DataSanitizer', () => {
  describe('sanitizeS3Key', () => {
    it('should sanitize S3 keys with PII', () => {
      const key = 'input/users/john.doe@example.com/data.csv';
      const sanitized = DataSanitizer.sanitizeS3Key(key);

      expect(sanitized).toContain('***');
      expect(sanitized).not.toContain('john.doe@example.com');
      expect(sanitized).toContain('.csv');
    });

    it('should preserve directory structure', () => {
      const key = 'input/safe-folder/data.csv';
      const sanitized = DataSanitizer.sanitizeS3Key(key);

      expect(sanitized).toContain('input/');
      expect(sanitized).toContain('safe-folder/');
      expect(sanitized).toContain('data.csv');
    });

    it('should handle empty keys', () => {
      expect(DataSanitizer.sanitizeS3Key('')).toBe('[empty-key]');
      expect(DataSanitizer.sanitizeS3Key(null as unknown as string)).toBe('[empty-key]');
    });
  });

  describe('sanitizeFilename', () => {
    it('should mask email addresses in filenames', () => {
      const filename = 'user_john.doe@example.com_data.csv';
      const sanitized = DataSanitizer.sanitizeFilename(filename);

      expect(sanitized).not.toContain('john.doe@example.com');
      expect(sanitized).toContain('***');
    });

    it('should mask phone numbers', () => {
      const filename = 'contact_555-123-4567_info.csv';
      const sanitized = DataSanitizer.sanitizeFilename(filename);

      expect(sanitized).not.toContain('555-123-4567');
      expect(sanitized).toContain('***');
    });

    it('should preserve file extension', () => {
      const filename = 'sensitive_user_id_123456789_data.csv';
      const sanitized = DataSanitizer.sanitizeFilename(filename);

      expect(sanitized).toContain('.csv');
    });

    it('should handle very long filenames', () => {
      const longFilename = `${'a'.repeat(100)}.csv`;
      const sanitized = DataSanitizer.sanitizeFilename(longFilename);

      expect(sanitized.length).toBeLessThan(60);
      expect(sanitized).toContain('.csv');
    });
  });

  describe('sanitizeErrorMessage', () => {
    it('should sanitize file paths in error messages', () => {
      const message = 'Failed to process /home/user/sensitive-data/file.csv';
      const sanitized = DataSanitizer.sanitizeErrorMessage(message);

      expect(sanitized).not.toContain('/home/user/sensitive-data');
      expect(sanitized).toContain('file.csv');
    });

    it('should sanitize S3 URLs', () => {
      const message =
        'Failed to upload to https://my-bucket.s3.amazonaws.com/sensitive/path/file.csv';
      const sanitized = DataSanitizer.sanitizeErrorMessage(message);

      expect(sanitized).toContain('[BUCKET]');
      expect(sanitized).not.toContain('my-bucket');
    });

    it('should remove IP addresses', () => {
      const message = 'Connection failed to 192.168.1.100';
      const sanitized = DataSanitizer.sanitizeErrorMessage(message);

      expect(sanitized).toContain('[IP-ADDRESS]');
      expect(sanitized).not.toContain('192.168.1.100');
    });
  });

  describe('sanitizeLoggingContext', () => {
    it('should sanitize context object', () => {
      const context = {
        key: 'input/user@example.com/data.csv',
        fileName: 'sensitive_user_id_123456789.csv',
        bucket: 'safe-bucket',
        operation: 'upload',
      };

      const sanitized = DataSanitizer.sanitizeLoggingContext(context);

      expect(sanitized.key).not.toContain('user@example.com');
      expect(sanitized.fileName).toContain('***');
      expect(sanitized.bucket).toBe('safe-bucket');
      expect(sanitized.operation).toBe('upload');
    });

    it('should handle nested objects', () => {
      const context = {
        error: {
          message: 'Failed to access user@example.com',
          stack: 'Error at /home/user/app/file.js:123:45',
        },
      };

      const sanitized = DataSanitizer.sanitizeLoggingContext(context);

      expect(sanitized.error.message).toContain('[EMAIL]');
      expect(sanitized.error.stack).toContain('.../file.js:123:45');
    });
  });
});

describe('sanitizeForLogging convenience functions', () => {
  it('should provide easy access to sanitization functions', () => {
    expect(sanitizeForLogging.s3Key('test/user@example.com/file.csv')).not.toContain(
      '@example.com'
    );
    expect(sanitizeForLogging.filename('user_123456789.csv')).toContain('***');
    expect(sanitizeForLogging.errorMessage('Error at user@example.com')).toContain('[EMAIL]');
  });
});
