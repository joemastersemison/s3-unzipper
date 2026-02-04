import type { S3EventRecord } from '../types';
import config from './config';
import logger from './logger';

/**
 * Maximum allowed S3 key length (AWS limit is 1024, we use 512 for safety)
 */
const MAX_S3_KEY_LENGTH = 512;

/**
 * Validates S3 object key after URL decoding
 */
export function validateS3Key(key: string): { isValid: boolean; error?: string } {
  // Check length limits
  if (key.length > MAX_S3_KEY_LENGTH) {
    return {
      isValid: false,
      error: `S3 key length exceeds maximum allowed length of ${MAX_S3_KEY_LENGTH} characters`,
    };
  }

  // Check for null bytes
  if (key.includes('\0')) {
    return {
      isValid: false,
      error: 'S3 key contains null bytes',
    };
  }

  // Check for control characters (0x00-0x1F and 0x7F-0x9F)
  // biome-ignore lint/suspicious/noControlCharactersInRegex: Security validation requires control character detection
  const controlCharPattern = /[\x00-\x1f\x7f-\x9f]/;
  if (controlCharPattern.test(key)) {
    return {
      isValid: false,
      error: 'S3 key contains control characters',
    };
  }

  // Check for suspicious patterns
  const suspiciousPatterns = [
    /\.\./, // Directory traversal
    /^\/+/, // Absolute path
    /\/\/+/, // Double slashes
    // biome-ignore lint/suspicious/noControlCharactersInRegex: Security validation requires control character detection
    /[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]/, // Additional control chars
    /<script/i, // Script injection attempts
    /javascript:/i, // JavaScript protocol
    /data:/i, // Data URL scheme
    /file:/i, // File protocol
  ];

  for (const pattern of suspiciousPatterns) {
    if (pattern.test(key)) {
      return {
        isValid: false,
        error: `S3 key contains suspicious pattern: ${pattern.source}`,
      };
    }
  }

  return { isValid: true };
}

/**
 * Validates that an S3 event record matches expected configuration
 */
export function validateS3Event(record: S3EventRecord): { isValid: boolean; error?: string } {
  // Check if record has required S3 information
  if (!record.s3?.bucket?.name || !record.s3?.object?.key) {
    return {
      isValid: false,
      error: 'S3 event record missing required bucket name or object key',
    };
  }

  const bucket = record.s3.bucket.name;
  const encodedKey = record.s3.object.key;

  // Validate bucket matches configured bucket
  if (bucket !== config.bucketName) {
    return {
      isValid: false,
      error: `S3 event bucket '${bucket}' does not match configured bucket '${config.bucketName}'`,
    };
  }

  // Decode the S3 key and validate it
  let decodedKey: string;
  try {
    decodedKey = decodeURIComponent(encodedKey.replace(/\+/g, ' '));
  } catch (error) {
    return {
      isValid: false,
      error: `Failed to decode S3 key: ${error instanceof Error ? error.message : String(error)}`,
    };
  }

  // Validate the decoded key
  const keyValidation = validateS3Key(decodedKey);
  if (!keyValidation.isValid) {
    return {
      isValid: false,
      error: `S3 key validation failed: ${keyValidation.error}`,
    };
  }

  // Check that key starts with configured input path
  if (!decodedKey.startsWith(config.inputPath)) {
    return {
      isValid: false,
      error: `S3 key '${decodedKey}' does not start with configured input path '${config.inputPath}'`,
    };
  }

  // Additional event-specific validations
  if (record.eventSource && record.eventSource !== 'aws:s3') {
    return {
      isValid: false,
      error: `Unexpected event source: ${record.eventSource}`,
    };
  }

  // Validate event name if present
  if (record.eventName && !record.eventName.startsWith('ObjectCreated:')) {
    logger.warn('Non-ObjectCreated event received', {
      eventName: record.eventName,
      bucket,
      key: decodedKey,
    });
  }

  return { isValid: true };
}

/**
 * Sanitizes a filename component for safe use in S3 paths
 */
export function sanitizeFilename(filename: string): string {
  if (!filename || filename.trim() === '') {
    return 'unknown';
  }

  let sanitized = filename
    // Remove or replace dangerous characters
    // biome-ignore lint/suspicious/noControlCharactersInRegex: Security sanitization requires control character replacement
    .replace(/[\x00-\x1f\x7f-\x9f]/g, '_') // Control characters
    .replace(/[<>:"|?*]/g, '_') // Windows reserved characters
    .replace(/\.\./g, '_') // Directory traversal
    .replace(/^\.+/, '') // Leading dots
    .replace(/\s+/g, '_'); // Whitespace to underscores

  // Only remove excessive underscores (more than 3 consecutive)
  // This preserves meaningful double underscores in filenames like "2026-01-01__2026-01-02"
  sanitized = sanitized.replace(/_{4,}/g, '___');

  // Remove leading/trailing underscores
  sanitized = sanitized.replace(/^_+|_+$/g, '');

  // Ensure we have something meaningful
  if (sanitized === '' || sanitized === '.') {
    sanitized = 'unknown';
  }

  // Truncate if too long (leave room for extensions and paths)
  if (sanitized.length > 100) {
    sanitized = sanitized.substring(0, 100);
  }

  return sanitized;
}

/**
 * Validates that a string is safe for use in S3 paths
 */
export function isValidS3PathComponent(component: string): boolean {
  if (!component || component.trim() === '') {
    return false;
  }

  // Check for dangerous patterns
  const dangerousPatterns = [
    // biome-ignore lint/suspicious/noControlCharactersInRegex: Security validation requires control character detection
    /[\x00-\x1f\x7f-\x9f]/, // Control characters
    /\.\./, // Directory traversal
    /^\.+$/, // Only dots
    /^\/+/, // Leading slashes
    /\/\/+/, // Double slashes
    /[<>:"|?*]/, // Reserved characters
  ];

  return !dangerousPatterns.some(pattern => pattern.test(component));
}
