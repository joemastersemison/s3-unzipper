import { Readable } from 'node:stream';
import { CSVProcessor } from '../../src/services/csv-processor';

describe('CSVProcessor', () => {
  let csvProcessor: CSVProcessor;

  beforeEach(() => {
    csvProcessor = new CSVProcessor();
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('isCSVFile', () => {
    test('should return true for .csv files', () => {
      expect(csvProcessor.isCSVFile('test.csv')).toBe(true);
      expect(csvProcessor.isCSVFile('data.CSV')).toBe(true);
      expect(csvProcessor.isCSVFile('nested/path/file.csv')).toBe(true);
    });

    test('should return false for non-CSV files', () => {
      expect(csvProcessor.isCSVFile('test.txt')).toBe(false);
      expect(csvProcessor.isCSVFile('data.json')).toBe(false);
      expect(csvProcessor.isCSVFile('file.xlsx')).toBe(false);
      expect(csvProcessor.isCSVFile('csvfile')).toBe(false);
    });
  });

  describe('processCSVStream', () => {
    test('should process valid CSV with headers', async () => {
      const csvData = 'name,age,city\nJohn,25,New York\nJane,30,San Francisco';
      const stream = Readable.from([csvData]);

      const result = await csvProcessor.processCSVStream(stream, 'test.csv');

      expect(result).not.toBeNull();
      if (result) {
        const outputCsv = result.toString('utf-8');
        expect(outputCsv).toContain('_processed');
        expect(outputCsv).toContain('John,25,New York');
        expect(outputCsv).toContain('Jane,30,San Francisco');

        // Should contain ISO timestamp
        expect(outputCsv).toMatch(/\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/);
      }
    });

    test('should process valid CSV without headers', async () => {
      const csvData = 'John,25,New York\nJane,30,San Francisco';
      const stream = Readable.from([csvData]);

      const result = await csvProcessor.processCSVStream(stream, 'test.csv');

      expect(result).not.toBeNull();
      if (result) {
        const outputCsv = result.toString('utf-8');
        expect(outputCsv).toContain('John,25,New York');
        expect(outputCsv).toContain('Jane,30,San Francisco');

        // Should contain ISO timestamp in each data row
        const lines = outputCsv.split('\n');
        expect(lines[0]).toMatch(/John,25,New York,\d{4}-\d{2}-\d{2}T/);
        expect(lines[1]).toMatch(/Jane,30,San Francisco,\d{4}-\d{2}-\d{2}T/);
      }
    });

    test('should handle CSV with existing _processed column', async () => {
      const csvData = 'name,age,_processed\nJohn,25,2023-01-01T00:00:00.000Z';
      const stream = Readable.from([csvData]);

      const result = await csvProcessor.processCSVStream(stream, 'test.csv');

      expect(result).not.toBeNull();
      if (result) {
        const outputCsv = result.toString('utf-8');
        // Should still add the timestamp column (resulting in two _processed columns)
        expect(outputCsv).toContain('_processed');
      }
    });

    test('should handle empty CSV', async () => {
      const csvData = '';
      const stream = Readable.from([csvData]);

      const result = await csvProcessor.processCSVStream(stream, 'empty.csv');

      expect(result).toBeNull();
    });

    test('should handle CSV with only headers', async () => {
      const csvData = 'name,age,city';
      const stream = Readable.from([csvData]);

      const result = await csvProcessor.processCSVStream(stream, 'headers-only.csv');

      expect(result).not.toBeNull();
      if (result) {
        const outputCsv = result.toString('utf-8');
        expect(outputCsv).toContain('name,age,city,_processed');
      }
    });

    test('should handle CSV with special characters', async () => {
      const csvData = 'name,description\n"John, Jr.","""Hello"" world"\n"Jane","Line1\nLine2"';
      const stream = Readable.from([csvData]);

      const result = await csvProcessor.processCSVStream(stream, 'special-chars.csv');

      expect(result).not.toBeNull();
      if (result) {
        const outputCsv = result.toString('utf-8');
        expect(outputCsv).toContain('_processed');
        expect(outputCsv).toContain('John, Jr.');
        expect(outputCsv).toContain('Hello');
      }
    });

    test('should handle malformed CSV with skipMalformed=true', async () => {
      const csvProcessor = new CSVProcessor({
        timestampColumn: '_processed',
        dateFormat: 'ISO',
        skipMalformed: true,
      });

      // Create a malformed CSV that might cause parsing issues
      const csvData = 'name,age\n"John,25\nJane,30'; // Missing closing quote
      const stream = Readable.from([csvData]);

      const result = await csvProcessor.processCSVStream(stream, 'malformed.csv');

      // Should return null for malformed CSV when skipMalformed is true
      expect(result).toBeNull();
    });

    test('should handle custom timestamp column name', async () => {
      const csvProcessor = new CSVProcessor({
        timestampColumn: 'custom_timestamp',
        dateFormat: 'ISO',
        skipMalformed: true,
      });

      const csvData = 'name,age\nJohn,25';
      const stream = Readable.from([csvData]);

      const result = await csvProcessor.processCSVStream(stream, 'custom-column.csv');

      expect(result).not.toBeNull();
      if (result) {
        const outputCsv = result.toString('utf-8');
        expect(outputCsv).toContain('custom_timestamp');
        expect(outputCsv).not.toContain('_processed');
      }
    });

    test('should handle large number of rows efficiently', async () => {
      // Create a larger CSV to test memory efficiency (reduced size to avoid memory circuit breaker)
      const headerRow = 'id,name,email,city\n';
      const dataRows = Array.from(
        { length: 100 },
        (_, i) => `${i},User${i},user${i}@example.com,City${i}`
      ).join('\n');

      const csvData = headerRow + dataRows;
      const stream = Readable.from([csvData]);

      const startTime = Date.now();
      const result = await csvProcessor.processCSVStream(stream, 'large.csv');
      const endTime = Date.now();

      expect(result).not.toBeNull();
      expect(endTime - startTime).toBeLessThan(5000); // Should complete within 5 seconds

      if (result) {
        const outputCsv = result.toString('utf-8');
        const lines = outputCsv.split('\n');
        expect(lines).toHaveLength(101); // Header + 100 data rows
        expect(outputCsv).toContain('_processed');
      }
    });

    test('should handle CSV with various delimiters correctly', async () => {
      // Test comma-separated values (standard CSV)
      const csvData = 'name,age,city\n"Smith, John",25,"New York, NY"';
      const stream = Readable.from([csvData]);

      const result = await csvProcessor.processCSVStream(stream, 'delimiters.csv');

      expect(result).not.toBeNull();
      if (result) {
        const outputCsv = result.toString('utf-8');
        expect(outputCsv).toContain('Smith, John');
        expect(outputCsv).toContain('New York, NY');
        expect(outputCsv).toContain('_processed');
      }
    });
  });

  describe('validateCSVStructure', () => {
    test('should validate correct CSV structure', async () => {
      const csvData = 'name,age,city\nJohn,25,New York';
      const stream = Readable.from([csvData]);

      const isValid = await csvProcessor.validateCSVStructure(stream, 'test.csv');

      expect(isValid).toBe(true);
    });

    test('should return false for empty stream', async () => {
      const csvData = '';
      const stream = Readable.from([csvData]);

      const isValid = await csvProcessor.validateCSVStructure(stream, 'empty.csv');

      expect(isValid).toBe(false);
    });

    test('should return false for malformed CSV', async () => {
      const csvData = 'name,age\n"John,25'; // Malformed
      const stream = Readable.from([csvData]);

      const isValid = await csvProcessor.validateCSVStructure(stream, 'malformed.csv');

      expect(isValid).toBe(false);
    });
  });

  describe('updateOptions', () => {
    test('should update processor options', () => {
      csvProcessor.updateOptions({
        timestampColumn: 'new_timestamp',
        skipMalformed: false,
      });

      // Test that options are updated by processing a CSV
      const csvData = 'name,age\nJohn,25';
      const stream = Readable.from([csvData]);

      return csvProcessor.processCSVStream(stream, 'test.csv').then(result => {
        expect(result).not.toBeNull();
        if (result) {
          const outputCsv = result.toString('utf-8');
          expect(outputCsv).toContain('new_timestamp');
        }
      });
    });
  });
});
