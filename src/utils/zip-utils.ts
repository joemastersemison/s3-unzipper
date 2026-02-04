import type { Readable } from 'node:stream';
import * as yauzl from 'yauzl';
import logger from './logger';

/**
 * Opens a read stream for a zip entry with proper error handling
 * Converts yauzl's callback-based API to async/await
 * @param zipfile - The yauzl ZipFile instance
 * @param entry - The zip entry to open a stream for
 * @param requestId - Request ID for logging context
 * @returns Promise that resolves to a readable stream
 */
export async function openZipEntryStream(
  zipfile: yauzl.ZipFile,
  entry: yauzl.Entry,
  requestId?: string
): Promise<Readable> {
  return new Promise((resolve, reject) => {
    zipfile.openReadStream(entry, (err, readStream) => {
      if (err) {
        logger.error('Failed to open read stream for zip entry', err, {
          fileName: entry.fileName,
          requestId,
        });
        reject(err);
        return;
      }

      if (!readStream) {
        const error = new Error('Read stream is null');
        logger.error('Read stream is null for zip entry', error, {
          fileName: entry.fileName,
          requestId,
        });
        reject(error);
        return;
      }

      resolve(readStream);
    });
  });
}

/**
 * Creates a zip file instance from buffer with error handling
 * Converts yauzl's callback-based API to async/await
 * @param zipBuffer - The zip file buffer
 * @param requestId - Request ID for logging context
 * @returns Promise that resolves to a ZipFile instance
 */
export async function createZipFileFromBuffer(
  zipBuffer: Buffer,
  requestId?: string
): Promise<yauzl.ZipFile> {
  return new Promise((resolve, reject) => {
    yauzl.fromBuffer(zipBuffer, { lazyEntries: true }, (err, zipfile) => {
      if (err) {
        logger.error('Failed to open zip file', err, {
          operation: 'zip_open',
          requestId,
        });
        reject(err);
        return;
      }

      if (!zipfile) {
        const error = new Error('Zip file object is null');
        logger.error('Zip file object is null', error, {
          operation: 'zip_open',
          requestId,
        });
        reject(error);
        return;
      }

      resolve(zipfile);
    });
  });
}

/**
 * Processes zip file entries with async iterator pattern
 * This eliminates complex event handling callbacks
 * @param zipfile - The yauzl ZipFile instance
 * @param processor - Function to process each entry
 * @param requestId - Request ID for logging context
 */
export async function processZipEntries(
  zipfile: yauzl.ZipFile,
  processor: (entry: yauzl.Entry) => Promise<void>,
  requestId?: string
): Promise<void> {
  return new Promise((resolve, reject) => {
    let hasError = false;

    zipfile.readEntry();

    zipfile.on('entry', async entry => {
      if (hasError) return;

      try {
        await processor(entry);
        zipfile.readEntry();
      } catch (error) {
        hasError = true;
        logger.error('Error processing zip entry', error as Error, {
          fileName: entry.fileName,
          requestId,
        });
        reject(error);
      }
    });

    zipfile.on('end', () => {
      if (!hasError) {
        resolve();
      }
    });

    zipfile.on('error', error => {
      if (!hasError) {
        hasError = true;
        logger.error('Zip file processing error', error, {
          operation: 'zip_processing',
          requestId,
        });
        reject(error);
      }
    });
  });
}
