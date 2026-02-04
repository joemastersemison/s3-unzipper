/**
 * Module-scoped S3Client singleton for cold start optimization
 * Creates and reuses a single S3Client instance across Lambda invocations
 */

import { S3Client } from '@aws-sdk/client-s3';
import config from './config';
import logger from './logger';

/**
 * Module-scoped S3Client singleton
 * This is initialized once when the module is loaded and reused across invocations
 */
let s3ClientInstance: S3Client | null = null;
let clientInitializedAt: Date | null = null;

/**
 * Get or create the singleton S3Client instance
 * This function is thread-safe and will only create one instance
 */
export function getS3Client(): S3Client {
  if (!s3ClientInstance) {
    logger.debug('Creating new S3Client singleton instance');

    s3ClientInstance = new S3Client({
      region: config.region,
      // Enable connection reuse for better performance
      maxAttempts: 3,
      requestHandler: {
        connectionTimeout: 5000,
        requestTimeout: 30000,
      },
    });

    clientInitializedAt = new Date();

    logger.info('S3Client singleton initialized', {
      region: config.region,
      initializedAt: clientInitializedAt.toISOString(),
    });
  } else {
    logger.debug('Reusing existing S3Client singleton instance', {
      initializedAt: clientInitializedAt?.toISOString(),
      ageMs: clientInitializedAt ? Date.now() - clientInitializedAt.getTime() : 0,
    });
  }

  return s3ClientInstance;
}

/**
 * Get information about the current S3Client instance
 */
export function getS3ClientInfo() {
  return {
    isInitialized: s3ClientInstance !== null,
    initializedAt: clientInitializedAt,
    ageMs: clientInitializedAt ? Date.now() - clientInitializedAt.getTime() : null,
    region: config.region,
  };
}

/**
 * Force recreation of the S3Client (useful for testing or configuration changes)
 * Note: This should rarely be used in production
 */
export function recreateS3Client(): S3Client {
  logger.info('Forcing recreation of S3Client singleton');
  s3ClientInstance = null;
  clientInitializedAt = null;
  return getS3Client();
}

/**
 * Destroy the S3Client singleton (useful for cleanup in tests)
 */
export function destroyS3Client(): void {
  if (s3ClientInstance) {
    logger.debug('Destroying S3Client singleton');

    // The AWS SDK doesn't have an explicit close method for S3Client,
    // but we can clear our reference
    s3ClientInstance = null;
    clientInitializedAt = null;
  }
}
