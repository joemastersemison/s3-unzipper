import type { FilenameComponents } from '../types';
import logger from '../utils/logger';
import { isValidS3PathComponent, sanitizeFilename } from '../utils/validation';

/**
 * Extracts stem name from complex filenames with date patterns
 * Examples:
 * - "2026-01-01__2026-01-02_app_registration_report.csv" → "app_registration_report"
 * - "2026-01-01_badge.csv" → "badge"
 * - "some_file_2026-01-01.txt" → "some_file"
 */
export function extractStemName(filename: string): string {
  try {
    // Handle empty or whitespace-only filenames
    if (!filename || filename.trim() === '') {
      return 'unknown';
    }

    // Handle edge case of just a dot
    if (filename.trim() === '.') {
      return 'unknown';
    }

    const components = parseFilename(filename);
    return components.stemName;
  } catch (error) {
    logger.warn('Failed to parse filename, using fallback', {
      filename,
      error: error instanceof Error ? error.message : String(error),
    });
    return 'unknown';
  }
}

/**
 * Parses a filename into its components
 */
export function parseFilename(filename: string): FilenameComponents {
  // Remove any directory path and get just the filename
  const justFilename = filename.split('/').pop() || filename;

  // Pre-sanitize the entire filename to remove dangerous characters
  const sanitizedFilename = sanitizeFilename(justFilename);

  // Separate extension
  const lastDotIndex = sanitizedFilename.lastIndexOf('.');
  const baseName =
    lastDotIndex > 0 ? sanitizedFilename.substring(0, lastDotIndex) : sanitizedFilename;
  const extension = lastDotIndex > 0 ? sanitizedFilename.substring(lastDotIndex) : '';

  // Split by underscores
  const parts = baseName.split('_');

  // Identify date parts (YYYY-MM-DD format)
  const datePattern = /^\d{4}-\d{2}-\d{2}$/;
  const dateParts: string[] = [];
  const nonDateParts: string[] = [];

  parts.forEach(part => {
    if (datePattern.test(part)) {
      dateParts.push(part);
    } else {
      // Keep empty parts as is for proper parsing, only sanitize non-empty parts
      if (part === '') {
        nonDateParts.push(part);
      } else {
        const sanitizedPart = sanitizeFilename(part);
        nonDateParts.push(sanitizedPart);
      }
    }
  });

  // Extract stem name from non-date parts
  const stemName = extractStemFromParts(nonDateParts);

  return {
    baseName: sanitizeFilename(baseName),
    extension: extension, // Extensions typically don't need sanitization
    dateParts,
    nonDateParts,
    stemName,
  };
}

/**
 * Extracts meaningful stem name from non-date parts
 */
function extractStemFromParts(parts: string[]): string {
  if (parts.length === 0) {
    return 'unknown';
  }

  // Filter out empty parts and sanitize each part
  const validParts = parts
    .filter(part => part && part.trim().length > 0)
    .map(part => sanitizeFilename(part))
    .filter(part => part && part !== 'unknown' && isValidS3PathComponent(part));

  if (validParts.length === 0) {
    return 'unknown';
  }

  // Find first meaningful part (not just starting with letter, but any non-empty part)
  const stemParts: string[] = [];
  let foundStart = false;

  for (const part of validParts) {
    // Start including parts from the first substantial part
    // Allow numbers, letters, or any meaningful content
    if (!foundStart && part.length > 0 && part !== '_') {
      foundStart = true;
    }

    // Once we find the start, include all subsequent parts
    if (foundStart) {
      stemParts.push(part);
    }
  }

  // If no meaningful start found, use all valid parts
  if (stemParts.length === 0) {
    stemParts.push(...validParts);
  }

  // Join with underscores and clean up
  const stemName = stemParts.join('_');

  // Final sanitization and cleanup
  const finalStemName = sanitizeFilename(stemName);

  return finalStemName && isValidS3PathComponent(finalStemName) ? finalStemName : 'unknown';
}

/**
 * Validates if a filename appears to have the expected pattern
 */
export function isValidFilenamePattern(filename: string): boolean {
  try {
    // Handle empty or invalid filenames
    if (!filename || filename.trim() === '' || filename.trim() === '.') {
      return false;
    }

    const components = parseFilename(filename);

    // Valid if we have at least some meaningful content
    const hasContent =
      components.nonDateParts.some(part => part && part.trim().length > 0) ||
      components.dateParts.length > 0;

    return hasContent;
  } catch {
    return false;
  }
}

/**
 * Gets debugging information about filename parsing
 */
export function getFilenameDebugInfo(filename: string): FilenameComponents & {
  originalFilename: string;
  isValid: boolean;
} {
  const components = parseFilename(filename);

  return {
    originalFilename: filename,
    isValid: isValidFilenamePattern(filename),
    ...components,
  };
}
