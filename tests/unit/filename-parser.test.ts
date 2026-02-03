import {
  extractStemName,
  getFilenameDebugInfo,
  isValidFilenamePattern,
  parseFilename,
} from '../../src/services/filename-parser';

describe('Filename Parser', () => {
  describe('extractStemName', () => {
    test('should extract stem name from complex filename with dates', () => {
      const filename = '2026-01-01__2026-01-02_app_registration_report.csv';
      const result = extractStemName(filename);
      expect(result).toBe('app_registration_report');
    });

    test('should extract stem name from simple filename with single date', () => {
      const filename = '2026-01-01_badge.csv';
      const result = extractStemName(filename);
      expect(result).toBe('badge');
    });

    test('should extract stem name from filename with trailing date', () => {
      const filename = 'user_report_2026-01-01.txt';
      const result = extractStemName(filename);
      expect(result).toBe('user_report');
    });

    test('should handle filename without dates', () => {
      const filename = 'simple_file.csv';
      const result = extractStemName(filename);
      expect(result).toBe('simple_file');
    });

    test('should handle filename with only dates', () => {
      const filename = '2026-01-01__2026-01-02.csv';
      const result = extractStemName(filename);
      expect(result).toBe('unknown');
    });

    test('should handle filename without extension', () => {
      const filename = '2026-01-01__2026-01-02_report';
      const result = extractStemName(filename);
      expect(result).toBe('report');
    });

    test('should handle complex filename with mixed patterns', () => {
      const filename = 'data_2026-01-01_export_2026-01-02_final_report.xlsx';
      const result = extractStemName(filename);
      expect(result).toBe('data_export_final_report');
    });

    test('should handle empty or invalid filenames', () => {
      expect(extractStemName('')).toBe('unknown');
      expect(extractStemName('   ')).toBe('unknown');
      expect(extractStemName('.')).toBe('unknown');
    });

    test('should handle path separators', () => {
      const filename = 'path/to/2026-01-01_user_data.csv';
      const result = extractStemName(filename);
      expect(result).toBe('user_data');
    });
  });

  describe('parseFilename', () => {
    test('should correctly parse filename components', () => {
      const filename = '2026-01-01__2026-01-02_app_registration_report.csv';
      const result = parseFilename(filename);

      expect(result.baseName).toBe('2026-01-01__2026-01-02_app_registration_report');
      expect(result.extension).toBe('.csv');
      expect(result.dateParts).toEqual(['2026-01-01', '2026-01-02']);
      expect(result.nonDateParts).toEqual(['', 'app', 'registration', 'report']);
      expect(result.stemName).toBe('app_registration_report');
    });

    test('should handle filename without extension', () => {
      const filename = '2026-01-01_report';
      const result = parseFilename(filename);

      expect(result.baseName).toBe('2026-01-01_report');
      expect(result.extension).toBe('');
      expect(result.dateParts).toEqual(['2026-01-01']);
      expect(result.nonDateParts).toEqual(['report']);
    });

    test('should handle filename with path', () => {
      const filename = 'folder/2026-01-01_data.txt';
      const result = parseFilename(filename);

      expect(result.baseName).toBe('2026-01-01_data');
      expect(result.extension).toBe('.txt');
    });
  });

  describe('isValidFilenamePattern', () => {
    test('should return true for valid patterns', () => {
      expect(isValidFilenamePattern('2026-01-01_report.csv')).toBe(true);
      expect(isValidFilenamePattern('simple_file.txt')).toBe(true);
      expect(isValidFilenamePattern('2026-01-01__2026-01-02_data.xlsx')).toBe(true);
    });

    test('should return false for invalid patterns', () => {
      expect(isValidFilenamePattern('')).toBe(false);
      expect(isValidFilenamePattern('.')).toBe(false);
    });
  });

  describe('getFilenameDebugInfo', () => {
    test('should return comprehensive debug information', () => {
      const filename = '2026-01-01_app_report.csv';
      const result = getFilenameDebugInfo(filename);

      expect(result.originalFilename).toBe(filename);
      expect(result.isValid).toBe(true);
      expect(result.stemName).toBe('app_report');
      expect(result.dateParts).toEqual(['2026-01-01']);
      expect(result.nonDateParts).toEqual(['app', 'report']);
    });
  });

  describe('edge cases', () => {
    test('should handle multiple consecutive underscores', () => {
      const filename = '2026-01-01___app__report.csv';
      const result = extractStemName(filename);
      expect(result).toBe('app_report');
    });

    test('should handle filenames starting with non-letters', () => {
      const filename = '2026-01-01_123_report.csv';
      const result = extractStemName(filename);
      expect(result).toBe('123_report');
    });

    test('should handle very long filenames', () => {
      const filename = '2026-01-01_very_long_filename_with_many_parts_and_components_report.csv';
      const result = extractStemName(filename);
      expect(result).toBe('very_long_filename_with_many_parts_and_components_report');
    });

    test('should handle special characters in filenames', () => {
      const filename = '2026-01-01_app-report_v1.0.csv';
      const result = extractStemName(filename);
      expect(result).toBe('app-report_v1.0');
    });
  });
});
