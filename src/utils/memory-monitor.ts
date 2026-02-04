/**
 * Memory monitoring utility with circuit breaker patterns
 * Tracks heap usage and aborts processing if memory usage exceeds thresholds
 */

import logger from './logger';

export interface MemoryUsage {
  heapUsed: number;
  heapTotal: number;
  external: number;
  rss: number;
  usagePercentage: number;
}

export interface MemoryThresholds {
  warningThreshold: number; // Percentage at which to log warnings
  circuitBreakerThreshold: number; // Percentage at which to abort processing
}

export class MemoryMonitor {
  private static instance: MemoryMonitor;
  private thresholds: MemoryThresholds;
  private circuitBreakerTripped = false;
  private lastCheckTime = 0;
  private readonly CHECK_INTERVAL_MS = 1000; // Check at most once per second

  private constructor(thresholds?: MemoryThresholds) {
    this.thresholds = thresholds || {
      warningThreshold: 70,
      circuitBreakerThreshold: 80,
    };
  }

  static getInstance(thresholds?: MemoryThresholds): MemoryMonitor {
    if (!MemoryMonitor.instance) {
      MemoryMonitor.instance = new MemoryMonitor(thresholds);
    }
    return MemoryMonitor.instance;
  }

  /**
   * Get current memory usage information
   */
  getMemoryUsage(): MemoryUsage {
    const memUsage = process.memoryUsage();
    const usagePercentage = (memUsage.heapUsed / memUsage.heapTotal) * 100;

    return {
      heapUsed: memUsage.heapUsed,
      heapTotal: memUsage.heapTotal,
      external: memUsage.external,
      rss: memUsage.rss,
      usagePercentage,
    };
  }

  /**
   * Check memory usage and trigger warnings or circuit breaker
   * Returns true if processing should continue, false if it should abort
   */
  checkMemoryUsage(operationName?: string): boolean {
    const now = Date.now();

    // Throttle checks to avoid excessive overhead
    if (now - this.lastCheckTime < this.CHECK_INTERVAL_MS) {
      return !this.circuitBreakerTripped;
    }
    this.lastCheckTime = now;

    const usage = this.getMemoryUsage();

    // Log current usage for monitoring
    logger.debug('Memory usage check', {
      operation: operationName || 'unknown',
      heapUsedMB: Math.round(usage.heapUsed / 1024 / 1024),
      heapTotalMB: Math.round(usage.heapTotal / 1024 / 1024),
      usagePercentage: Math.round(usage.usagePercentage * 100) / 100,
      circuitBreakerTripped: this.circuitBreakerTripped,
    });

    // Check warning threshold
    if (
      usage.usagePercentage >= this.thresholds.warningThreshold &&
      usage.usagePercentage < this.thresholds.circuitBreakerThreshold
    ) {
      logger.warn('High memory usage detected', {
        operation: operationName || 'unknown',
        usagePercentage: Math.round(usage.usagePercentage * 100) / 100,
        heapUsedMB: Math.round(usage.heapUsed / 1024 / 1024),
        heapTotalMB: Math.round(usage.heapTotal / 1024 / 1024),
        warningThreshold: this.thresholds.warningThreshold,
      });
    }

    // Check circuit breaker threshold
    if (usage.usagePercentage >= this.thresholds.circuitBreakerThreshold) {
      this.circuitBreakerTripped = true;
      logger.error('Memory circuit breaker triggered - aborting processing', undefined, {
        operation: operationName || 'unknown',
        usagePercentage: Math.round(usage.usagePercentage * 100) / 100,
        heapUsedMB: Math.round(usage.heapUsed / 1024 / 1024),
        heapTotalMB: Math.round(usage.heapTotal / 1024 / 1024),
        circuitBreakerThreshold: this.thresholds.circuitBreakerThreshold,
      });
      return false;
    }

    return true;
  }

  /**
   * Force garbage collection if available and log results
   */
  forceGarbageCollection(operationName?: string): void {
    const beforeUsage = this.getMemoryUsage();

    if (global.gc) {
      try {
        global.gc();
        const afterUsage = this.getMemoryUsage();

        logger.debug('Forced garbage collection', {
          operation: operationName || 'unknown',
          beforeHeapUsedMB: Math.round(beforeUsage.heapUsed / 1024 / 1024),
          afterHeapUsedMB: Math.round(afterUsage.heapUsed / 1024 / 1024),
          freedMB: Math.round((beforeUsage.heapUsed - afterUsage.heapUsed) / 1024 / 1024),
        });
      } catch (error) {
        logger.warn('Failed to force garbage collection', {
          operation: operationName || 'unknown',
          error: error instanceof Error ? error.message : String(error),
        });
      }
    } else {
      logger.debug('Garbage collection not available (--expose-gc flag not set)');
    }
  }

  /**
   * Reset the circuit breaker (use with caution)
   */
  resetCircuitBreaker(): void {
    const usage = this.getMemoryUsage();
    if (usage.usagePercentage < this.thresholds.warningThreshold) {
      this.circuitBreakerTripped = false;
      logger.info('Memory circuit breaker reset', {
        currentUsagePercentage: Math.round(usage.usagePercentage * 100) / 100,
      });
    } else {
      logger.warn('Cannot reset circuit breaker - memory usage still high', {
        currentUsagePercentage: Math.round(usage.usagePercentage * 100) / 100,
        warningThreshold: this.thresholds.warningThreshold,
      });
    }
  }

  /**
   * Check if the circuit breaker is currently tripped
   */
  isCircuitBreakerTripped(): boolean {
    return this.circuitBreakerTripped;
  }

  /**
   * Update memory thresholds
   */
  updateThresholds(newThresholds: Partial<MemoryThresholds>): void {
    this.thresholds = { ...this.thresholds, ...newThresholds };
    logger.info('Memory thresholds updated', {
      warningThreshold: this.thresholds.warningThreshold,
      circuitBreakerThreshold: this.thresholds.circuitBreakerThreshold,
    });
  }
}

/**
 * Convenience function to get the singleton memory monitor instance
 */
export const memoryMonitor = MemoryMonitor.getInstance();

/**
 * Decorator function to wrap methods with memory checking
 */
export function withMemoryCheck(operationName?: string) {
  // biome-ignore lint/suspicious/noExplicitAny: Decorator target parameter requires any type
  return (target: any, propertyKey: string, descriptor: PropertyDescriptor) => {
    const originalMethod = descriptor.value;

    // biome-ignore lint/suspicious/noExplicitAny: Generic method arguments require any[] type
    descriptor.value = async function (...args: any[]) {
      const monitor = MemoryMonitor.getInstance();

      if (!monitor.checkMemoryUsage(operationName || `${target.constructor.name}.${propertyKey}`)) {
        throw new Error('Operation aborted due to high memory usage');
      }

      return await originalMethod.apply(this, args);
    };

    return descriptor;
  };
}
