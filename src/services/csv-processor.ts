import type { Readable } from 'node:stream';
import * as csv from 'fast-csv';
import type { CSVProcessingResult, CSVProcessorOptions } from '../types';
import logger from '../utils/logger';

export class CSVProcessor {
  private options: CSVProcessorOptions;

  constructor(options?: Partial<CSVProcessorOptions>) {
    this.options = {
      timestampColumn: options?.timestampColumn || '_processed',
      dateFormat: options?.dateFormat || 'ISO',
      skipMalformed: options?.skipMalformed !== false,
    };
  }

  /**
   * Determines if a file is a CSV file based on its extension
   */
  isCSVFile(fileName: string): boolean {
    return fileName.toLowerCase().endsWith('.csv');
  }

  /**
   * Processes a CSV stream by adding a timestamp column
   */
  async processCSVStream(stream: Readable, fileName: string): Promise<Buffer | null> {
    try {
      logger.debug('Starting CSV processing', {
        fileName,
        timestampColumn: this.options.timestampColumn,
        operation: 'csv_process_start',
      });

      const rows: any[][] = [];
      let hasHeaders = false;
      let rowCount = 0;
      const timestamp = this.getCurrentTimestamp();

      return new Promise<Buffer | null>((resolve, reject) => {
        const parseStream = csv.parseStream(stream, {
          headers: false, // We'll handle headers manually to be more robust
          ignoreEmpty: true,
          trim: true,
        });

        parseStream.on('data', (row: any[]) => {
          try {
            rowCount++;

            // First row - try to detect if it contains headers
            if (rowCount === 1) {
              hasHeaders = this.detectHeaders(row);

              if (hasHeaders) {
                // Add timestamp column to header row
                row.push(this.options.timestampColumn);
              } else {
                // No headers, so add timestamp to data row
                row.push(timestamp);
              }

              rows.push(row);
            } else {
              // Data row - always add timestamp
              row.push(timestamp);
              rows.push(row);
            }
          } catch (error) {
            logger.warn('Error processing CSV row, skipping', {
              fileName,
              rowNumber: rowCount,
              error: error instanceof Error ? error.message : String(error),
              operation: 'csv_row_error',
            });

            if (!this.options.skipMalformed) {
              parseStream.destroy(error instanceof Error ? error : new Error(String(error)));
              return;
            }
          }
        });

        parseStream.on('end', () => {
          try {
            if (rows.length === 0) {
              logger.warn('CSV file is empty', { fileName });
              resolve(null);
              return;
            }

            // If we have headers but only one row (header row), add the timestamp and continue
            if (hasHeaders && rows.length === 1) {
              // This CSV only has headers, no data rows
              logger.debug('CSV has only headers, no data rows', { fileName });
            } else if (hasHeaders && rows.length > 1) {
              // Add timestamp to all data rows (skip header row)
              for (let i = 1; i < rows.length; i++) {
                // The timestamp was already added in the 'data' event handler
                // but let's ensure consistency
                if (rows[i].length === rows[0].length - 1) {
                  rows[i].push(timestamp);
                }
              }
            }

            // Convert back to CSV format
            const csvOutput = this.convertRowsToCSV(rows);
            const processedBuffer = Buffer.from(csvOutput, 'utf-8');

            const result: CSVProcessingResult = {
              processed: true,
              originalSize: 0, // We don't track original size in this implementation
              processedSize: processedBuffer.length,
              rowsProcessed: hasHeaders ? rows.length - 1 : rows.length,
            };

            logger.debug('CSV processing completed successfully', {
              fileName,
              ...result,
              operation: 'csv_process_complete',
            });

            resolve(processedBuffer);
          } catch (error) {
            logger.error('Error completing CSV processing', error as Error, {
              fileName,
              operation: 'csv_process_finalize',
            });

            if (this.options.skipMalformed) {
              resolve(null);
            } else {
              reject(error);
            }
          }
        });

        parseStream.on('error', error => {
          logger.warn('CSV parsing error', {
            fileName,
            error: error.message,
            operation: 'csv_parse_error',
          });

          if (this.options.skipMalformed) {
            resolve(null);
          } else {
            reject(error);
          }
        });
      });
    } catch (error) {
      return this.handleMalformedCSV(fileName, error as Error);
    }
  }

  /**
   * Attempts to detect if the first row contains headers
   * Uses more conservative heuristics to avoid false positives
   */
  private detectHeaders(firstRow: any[]): boolean {
    if (!firstRow || firstRow.length === 0) {
      return false;
    }

    // More conservative approach: look for typical header patterns
    let likelyHeaderCount = 0;
    let totalNonEmpty = 0;

    for (const value of firstRow) {
      const strValue = String(value).trim();

      if (strValue.length === 0) {
        continue; // Skip empty values
      }

      totalNonEmpty++;

      // Check if it looks like a typical header field name
      // Headers typically:
      // - Are short (< 50 characters)
      // - Don't contain obvious data patterns like emails, phone numbers
      // - May contain underscores or common header words
      // - Are not pure numbers or dates

      const isShort = strValue.length < 50;
      const containsLetters = /[a-zA-Z]/.test(strValue);
      const isNotPureNumber = !/^\d+(\.\d+)?$/.test(strValue);
      const isNotDate =
        !/^\d{4}-\d{2}-\d{2}/.test(strValue) && !/^\d{1,2}\/\d{1,2}\/\d{2,4}/.test(strValue);
      const isNotEmail = !/@/.test(strValue);
      const isNotUrl = !/^https?:\/\//.test(strValue);
      const hasTypicalHeaderChars = /^[a-zA-Z][a-zA-Z0-9_\s-]*$/.test(strValue);

      // Look for common header field names
      const commonHeaderWords =
        /^(id|name|age|email|phone|address|city|state|country|date|time|created|updated|status|type|category|description|title|first|last|user|customer|order|product|price|amount|total|count|quantity|_processed)$/i;

      if (
        isShort &&
        containsLetters &&
        isNotPureNumber &&
        isNotDate &&
        isNotEmail &&
        isNotUrl &&
        (hasTypicalHeaderChars || commonHeaderWords.test(strValue))
      ) {
        likelyHeaderCount++;
      }
    }

    // Only consider it headers if ALL non-empty values look like headers
    // This is more conservative to avoid false positives with data rows
    return totalNonEmpty > 0 && likelyHeaderCount === totalNonEmpty;
  }

  /**
   * Converts rows array back to CSV format
   */
  private convertRowsToCSV(rows: any[][]): string {
    return rows
      .map(row => {
        return row
          .map(cell => {
            const value = String(cell);
            // Escape values that contain commas, quotes, or newlines
            if (
              value.includes(',') ||
              value.includes('"') ||
              value.includes('\n') ||
              value.includes('\r')
            ) {
              return `"${value.replace(/"/g, '""')}"`;
            }
            return value;
          })
          .join(',');
      })
      .join('\n');
  }

  /**
   * Gets current UTC timestamp in ISO format
   */
  private getCurrentTimestamp(): string {
    if (this.options.dateFormat === 'ISO') {
      return new Date().toISOString();
    }

    // Default to ISO format for now
    // Could be extended to support other formats in the future
    return new Date().toISOString();
  }

  /**
   * Handles malformed CSV files with appropriate fallback
   */
  private handleMalformedCSV(fileName: string, error: Error): Buffer | null {
    logger.warn('CSV file is malformed, skipping processing', {
      fileName,
      error: error.message,
      operation: 'csv_malformed_fallback',
    });

    if (this.options.skipMalformed) {
      return null; // Signal to use original file
    } else {
      throw error;
    }
  }

  /**
   * Validates CSV file structure without full processing
   */
  async validateCSVStructure(stream: Readable, _fileName: string): Promise<boolean> {
    try {
      return new Promise<boolean>(resolve => {
        let rowCount = 0;
        const isValid = true;

        const parseStream = csv.parseStream(stream, {
          headers: false,
          ignoreEmpty: true,
        });

        parseStream.on('data', (_row: any[]) => {
          rowCount++;

          // Stop after checking a few rows to minimize processing
          if (rowCount > 5) {
            parseStream.destroy();
            resolve(isValid);
          }
        });

        parseStream.on('end', () => {
          resolve(rowCount > 0 && isValid);
        });

        parseStream.on('error', () => {
          resolve(false);
        });
      });
    } catch {
      return false;
    }
  }

  /**
   * Updates the processor options
   */
  updateOptions(options: Partial<CSVProcessorOptions>): void {
    this.options = {
      ...this.options,
      ...options,
    };
  }
}
