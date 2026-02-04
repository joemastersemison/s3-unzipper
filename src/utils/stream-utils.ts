import type { Readable } from 'node:stream';
import { memoryMonitor } from './memory-monitor';
import logger from './logger';

/**
 * Converts a readable stream to a Buffer by collecting all chunks
 * Includes memory monitoring and optimization
 * @param stream - The readable stream to convert
 * @param maxSizeBytes - Maximum allowed buffer size (default: 50MB)
 * @returns Promise that resolves to a Buffer containing all stream data
 */
export async function streamToBuffer(
  stream: Readable,
  maxSizeBytes: number = 50 * 1024 * 1024
): Promise<Buffer> {
  const chunks: Buffer[] = [];
  let totalSize = 0;

  for await (const chunk of stream) {
    const chunkBuffer = Buffer.from(chunk);
    totalSize += chunkBuffer.length;

    // Check size limit to prevent memory exhaustion
    if (totalSize > maxSizeBytes) {
      throw new Error(
        `Stream size ${totalSize} bytes exceeds maximum allowed size of ${maxSizeBytes} bytes`
      );
    }

    // Check memory periodically (every 10MB of data)
    if (totalSize % (10 * 1024 * 1024) === 0 || totalSize > 10 * 1024 * 1024) {
      if (!memoryMonitor.checkMemoryUsage('stream_to_buffer')) {
        throw new Error('Stream to buffer conversion aborted - memory usage too high');
      }
    }

    chunks.push(chunkBuffer);
  }

  logger.debug('Stream to buffer conversion completed', {
    totalSizeBytes: totalSize,
    totalSizeMB: Math.round((totalSize / 1024 / 1024) * 100) / 100,
    chunkCount: chunks.length,
  });

  return Buffer.concat(chunks);
}

/**
 * Optimized version of streamToBuffer with chunked processing
 * Processes data in smaller batches and triggers garbage collection
 * @param stream - The readable stream to convert
 * @param maxSizeBytes - Maximum allowed buffer size
 * @param chunkThresholdBytes - Trigger GC after processing this much data
 * @returns Promise that resolves to a Buffer containing all stream data
 */
export async function streamToBufferOptimized(
  stream: Readable,
  maxSizeBytes: number = 50 * 1024 * 1024,
  chunkThresholdBytes: number = 10 * 1024 * 1024
): Promise<Buffer> {
  const chunks: Buffer[] = [];
  let totalSize = 0;
  let processedSinceLastGC = 0;

  for await (const chunk of stream) {
    const chunkBuffer = Buffer.from(chunk);
    totalSize += chunkBuffer.length;
    processedSinceLastGC += chunkBuffer.length;

    // Check size limit to prevent memory exhaustion
    if (totalSize > maxSizeBytes) {
      throw new Error(
        `Stream size ${totalSize} bytes exceeds maximum allowed size of ${maxSizeBytes} bytes`
      );
    }

    chunks.push(chunkBuffer);

    // Periodically check memory and force GC if available
    if (processedSinceLastGC >= chunkThresholdBytes) {
      if (!memoryMonitor.checkMemoryUsage('stream_to_buffer_optimized')) {
        throw new Error('Stream to buffer conversion aborted - memory usage too high');
      }

      // Force garbage collection if available to prevent memory buildup
      memoryMonitor.forceGarbageCollection('stream_to_buffer_chunked');
      processedSinceLastGC = 0;
    }
  }

  logger.debug('Optimized stream to buffer conversion completed', {
    totalSizeBytes: totalSize,
    totalSizeMB: Math.round((totalSize / 1024 / 1024) * 100) / 100,
    chunkCount: chunks.length,
  });

  const result = Buffer.concat(chunks);

  // Clear chunks array to help GC
  chunks.length = 0;

  return result;
}

/**
 * Validates that a stream is not null/undefined and returns it
 * Throws an error if the stream is invalid
 * @param stream - The stream to validate
 * @param errorMessage - Custom error message to use if stream is invalid
 * @returns The validated stream
 */
export function validateStream(
  stream: Readable | null | undefined,
  errorMessage: string
): Readable {
  if (!stream) {
    throw new Error(errorMessage);
  }
  return stream;
}
