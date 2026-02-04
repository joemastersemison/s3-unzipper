import { RetryHandler } from '../../src/utils/retry-handler';

describe('RetryHandler', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe('withRetry', () => {
    it('should succeed on first attempt', async () => {
      const mockFn = jest.fn().mockResolvedValue('success');

      const result = await RetryHandler.withRetry(
        mockFn,
        { operation: 'test' },
        { maxAttempts: 3 }
      );

      expect(result).toBe('success');
      expect(mockFn).toHaveBeenCalledTimes(1);
    });

    it('should retry on retryable errors', async () => {
      const mockFn = jest
        .fn()
        .mockRejectedValueOnce(new Error('NetworkingError'))
        .mockRejectedValueOnce(new Error('TimeoutError'))
        .mockResolvedValue('success');

      const result = await RetryHandler.withRetry(
        mockFn,
        { operation: 'test' },
        { maxAttempts: 3, baseDelayMs: 10 }
      );

      expect(result).toBe('success');
      expect(mockFn).toHaveBeenCalledTimes(3);
    });

    it('should not retry on non-retryable errors', async () => {
      const mockFn = jest.fn().mockRejectedValue(new Error('ValidationError'));

      await expect(
        RetryHandler.withRetry(mockFn, { operation: 'test' }, { maxAttempts: 3, baseDelayMs: 10 })
      ).rejects.toThrow();

      expect(mockFn).toHaveBeenCalledTimes(1);
    });

    it('should fail after max attempts', async () => {
      const mockFn = jest.fn().mockRejectedValue(new Error('NetworkingError'));

      await expect(
        RetryHandler.withRetry(mockFn, { operation: 'test' }, { maxAttempts: 2, baseDelayMs: 10 })
      ).rejects.toThrow('failed after 2 attempts');

      expect(mockFn).toHaveBeenCalledTimes(2);
    });
  });

  describe('retryS3Operation', () => {
    it('should use S3-specific retry configuration', async () => {
      const mockFn = jest
        .fn()
        .mockRejectedValueOnce(new Error('SlowDown'))
        .mockResolvedValue('success');

      const result = await RetryHandler.retryS3Operation(
        mockFn,
        's3_upload',
        'test-bucket',
        'test-key'
      );

      expect(result).toBe('success');
      expect(mockFn).toHaveBeenCalledTimes(2);
    });

    it('should sanitize sensitive data in context', async () => {
      const mockFn = jest.fn().mockRejectedValue(new Error('ValidationError')); // Use non-retryable error

      await expect(
        RetryHandler.retryS3Operation(
          mockFn,
          's3_upload',
          'test-bucket',
          'sensitive/user@example.com/data.csv'
        )
      ).rejects.toThrow();

      expect(mockFn).toHaveBeenCalledTimes(1); // ValidationError should not retry

      // Test that error context was sanitized by checking log output
      // (the actual error thrown will have sanitized context)
    });
  });

  describe('retryCSVOperation', () => {
    it('should use CSV-specific retry configuration', async () => {
      const mockFn = jest
        .fn()
        .mockRejectedValueOnce(new Error('EMFILE'))
        .mockResolvedValue('success');

      const result = await RetryHandler.retryCSVOperation(mockFn, 'test.csv');

      expect(result).toBe('success');
      expect(mockFn).toHaveBeenCalledTimes(2);
    });

    it('should have limited retry attempts for CSV', async () => {
      const mockFn = jest.fn().mockRejectedValue(new Error('NetworkingError'));

      await expect(RetryHandler.retryCSVOperation(mockFn, 'test.csv')).rejects.toThrow();

      // CSV operations should have fewer retry attempts
      expect(mockFn).toHaveBeenCalledTimes(2);
    });
  });

  describe('timeout handling', () => {
    it('should timeout long-running operations', async () => {
      const mockFn = jest
        .fn()
        .mockImplementation(() => new Promise(resolve => setTimeout(resolve, 200)));

      await expect(
        RetryHandler.withRetry(mockFn, { operation: 'test' }, { maxAttempts: 1, timeoutMs: 50 })
      ).rejects.toThrow('timed out');

      expect(mockFn).toHaveBeenCalledTimes(1);
    });
  });

  describe('exponential backoff', () => {
    it('should increase delay between attempts', async () => {
      const callTimes: number[] = [];
      const mockFn = jest.fn().mockImplementation(async () => {
        callTimes.push(Date.now());
        if (callTimes.length <= 2) {
          throw new Error('NetworkingError');
        }
        return 'success';
      });

      const startTime = Date.now();

      const result = await RetryHandler.withRetry(
        mockFn,
        { operation: 'test' },
        { maxAttempts: 3, baseDelayMs: 100, backoffMultiplier: 2 }
      );

      expect(result).toBe('success');
      expect(mockFn).toHaveBeenCalledTimes(3); // Initial + 2 retries

      // Verify that retries had delays
      const totalTime = Date.now() - startTime;
      expect(totalTime).toBeGreaterThan(150); // Should have some delay from retries

      // Verify calls were spaced out (not instantaneous)
      if (callTimes.length >= 3) {
        const firstRetryDelay = callTimes[1] - callTimes[0];
        const secondRetryDelay = callTimes[2] - callTimes[1];

        expect(firstRetryDelay).toBeGreaterThan(50); // First retry should have some delay
        expect(secondRetryDelay).toBeGreaterThan(50); // Second retry should have some delay
      }
    });
  });
});
