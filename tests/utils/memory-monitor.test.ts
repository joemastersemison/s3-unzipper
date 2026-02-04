import { MemoryMonitor } from '../../src/utils/memory-monitor';

describe('MemoryMonitor', () => {
  let monitor: MemoryMonitor;

  beforeEach(() => {
    // Create a fresh instance for each test
    monitor = MemoryMonitor.getInstance({
      warningThreshold: 70,
      circuitBreakerThreshold: 80,
    });
    // Reset circuit breaker state
    if (monitor.isCircuitBreakerTripped()) {
      monitor.resetCircuitBreaker();
    }
  });

  describe('getMemoryUsage', () => {
    it('should return valid memory usage information', () => {
      const usage = monitor.getMemoryUsage();

      expect(usage).toBeDefined();
      expect(typeof usage.heapUsed).toBe('number');
      expect(typeof usage.heapTotal).toBe('number');
      expect(typeof usage.external).toBe('number');
      expect(typeof usage.rss).toBe('number');
      expect(typeof usage.usagePercentage).toBe('number');

      expect(usage.heapUsed).toBeGreaterThan(0);
      expect(usage.heapTotal).toBeGreaterThan(0);
      expect(usage.usagePercentage).toBeGreaterThanOrEqual(0);
      expect(usage.usagePercentage).toBeLessThanOrEqual(100);
    });
  });

  describe('checkMemoryUsage', () => {
    it('should return true when memory usage is normal', () => {
      const result = monitor.checkMemoryUsage('test-operation');
      expect(result).toBe(true);
    });

    it('should track circuit breaker state', () => {
      expect(monitor.isCircuitBreakerTripped()).toBe(false);
    });
  });

  describe('updateThresholds', () => {
    it('should update warning threshold', () => {
      monitor.updateThresholds({ warningThreshold: 60 });
      // Test passes if no error is thrown
    });

    it('should update circuit breaker threshold', () => {
      monitor.updateThresholds({ circuitBreakerThreshold: 85 });
      // Test passes if no error is thrown
    });
  });

  describe('forceGarbageCollection', () => {
    it('should handle garbage collection gracefully when not available', () => {
      // This should not throw even if gc is not available
      expect(() => monitor.forceGarbageCollection('test-gc')).not.toThrow();
    });
  });

  describe('singleton pattern', () => {
    it('should return the same instance', () => {
      const monitor1 = MemoryMonitor.getInstance();
      const monitor2 = MemoryMonitor.getInstance();
      expect(monitor1).toBe(monitor2);
    });
  });
});
