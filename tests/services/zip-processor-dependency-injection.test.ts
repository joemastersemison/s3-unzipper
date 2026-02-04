import { CSVProcessor } from '../../src/services/csv-processor';
import { S3Service } from '../../src/services/s3-service';
import { ZipProcessor } from '../../src/services/zip-processor';
import { memoryMonitor } from '../../src/utils/memory-monitor';

// Mock the dependencies
jest.mock('../../src/services/s3-service');
jest.mock('../../src/services/csv-processor');
jest.mock('../../src/utils/file-upload-handler');
jest.mock('../../src/utils/memory-monitor');

describe('ZipProcessor Dependency Injection', () => {
  let mockS3Service: jest.Mocked<S3Service>;
  let mockCSVProcessor: jest.Mocked<CSVProcessor>;

  beforeEach(() => {
    jest.clearAllMocks();

    // Create mock instances
    mockS3Service = new S3Service() as jest.Mocked<S3Service>;
    mockCSVProcessor = new CSVProcessor() as jest.Mocked<CSVProcessor>;

    // Mock S3Service methods
    mockS3Service.getObjectAsBuffer = jest.fn();
    mockS3Service.validateFileSize = jest.fn();
    mockS3Service.uploadStream = jest.fn();
    mockS3Service.uploadBuffer = jest.fn();

    // Mock CSVProcessor methods
    mockCSVProcessor.isCSVFile = jest.fn();
    mockCSVProcessor.processCSVStream = jest.fn();

    // Mock memory monitor to always allow processing
    (memoryMonitor.checkMemoryUsage as jest.Mock).mockReturnValue(true);
    (memoryMonitor.isCircuitBreakerTripped as jest.Mock).mockReturnValue(false);
    (memoryMonitor.getMemoryUsage as jest.Mock).mockReturnValue({
      heapUsed: 100 * 1024 * 1024, // 100MB
      heapTotal: 200 * 1024 * 1024, // 200MB
      external: 10 * 1024 * 1024, // 10MB
      rss: 150 * 1024 * 1024, // 150MB
      usagePercentage: 50, // 50% usage
    });
  });

  describe('Constructor Dependency Injection', () => {
    it('should use provided S3Service instance', () => {
      const zipProcessor = new ZipProcessor(mockS3Service);

      expect(zipProcessor).toBeDefined();
      // The processor should use the injected S3Service
      expect(zipProcessor.s3Service).toBe(mockS3Service);
    });

    it('should use provided CSVProcessor instance', () => {
      const zipProcessor = new ZipProcessor(mockS3Service, mockCSVProcessor);

      expect(zipProcessor).toBeDefined();
      // The processor should use the injected CSVProcessor
      expect(zipProcessor.csvProcessor).toBe(mockCSVProcessor);
    });

    it('should create default S3Service when not provided', () => {
      const zipProcessor = new ZipProcessor();

      expect(zipProcessor).toBeDefined();
      // Should have created a default S3Service instance
      expect(zipProcessor.s3Service).toBeInstanceOf(S3Service);
    });

    it('should create default CSVProcessor when not provided', () => {
      const zipProcessor = new ZipProcessor(mockS3Service);

      expect(zipProcessor).toBeDefined();
      // Should have created a default CSVProcessor instance
      expect(zipProcessor.csvProcessor).toBeInstanceOf(CSVProcessor);
    });

    it('should use both injected dependencies', () => {
      const zipProcessor = new ZipProcessor(mockS3Service, mockCSVProcessor);

      expect(zipProcessor.s3Service).toBe(mockS3Service);
      expect(zipProcessor.csvProcessor).toBe(mockCSVProcessor);
    });
  });

  describe('Dependency Usage', () => {
    it('should use injected S3Service for file size validation', async () => {
      mockS3Service.validateFileSize.mockResolvedValue({
        isValid: false,
        reason: 'File too large',
      });

      const zipProcessor = new ZipProcessor(mockS3Service, mockCSVProcessor);

      const result = await zipProcessor.processZipFile(
        'test-bucket',
        'test-file.zip',
        'request-123'
      );

      expect(mockS3Service.validateFileSize).toHaveBeenCalledWith('test-bucket', 'test-file.zip');
      expect(result.success).toBe(false);
      expect(result.errors).toContain('File too large');
    });

    it('should use injected CSVProcessor for CSV detection', async () => {
      mockCSVProcessor.isCSVFile.mockReturnValue(true);

      const zipProcessor = new ZipProcessor(mockS3Service, mockCSVProcessor);

      // Test CSV detection through the injected processor
      const isCSV = zipProcessor.csvProcessor.isCSVFile('test.csv');

      expect(mockCSVProcessor.isCSVFile).toHaveBeenCalledWith('test.csv');
      expect(isCSV).toBe(true);
    });

    it('should allow different CSV processor configurations', () => {
      const customCSVProcessor = new CSVProcessor({
        timestampColumn: 'custom_timestamp',
        dateFormat: 'ISO',
        skipMalformed: false,
      });

      const zipProcessor = new ZipProcessor(mockS3Service, customCSVProcessor);

      expect(zipProcessor.csvProcessor).toBe(customCSVProcessor);
    });
  });

  describe('Testing Benefits', () => {
    it('should allow easy mocking of S3 operations', async () => {
      // Mock S3Service to simulate network error
      mockS3Service.validateFileSize.mockRejectedValue(new Error('Network error'));

      const zipProcessor = new ZipProcessor(mockS3Service, mockCSVProcessor);

      const result = await zipProcessor.processZipFile('test-bucket', 'test-file.zip');

      expect(result.success).toBe(false);
      expect(result.errors?.[0]).toContain('Network error');
    });

    it('should allow easy mocking of CSV processing', () => {
      // Mock CSV processor to simulate different file types
      mockCSVProcessor.isCSVFile.mockImplementation(filename => filename.endsWith('.csv'));

      const zipProcessor = new ZipProcessor(mockS3Service, mockCSVProcessor);

      expect(zipProcessor.csvProcessor.isCSVFile('data.csv')).toBe(true);
      expect(zipProcessor.csvProcessor.isCSVFile('image.jpg')).toBe(false);

      expect(mockCSVProcessor.isCSVFile).toHaveBeenCalledTimes(2);
    });

    it('should enable isolated unit testing', () => {
      // Each dependency can be mocked independently
      const mockFileUploadHandler = {
        uploadFileFromZip: jest.fn().mockResolvedValue(undefined),
      };

      // Mock the FileUploadHandler constructor
      const _FileUploadHandlerMock = jest.fn().mockImplementation(() => mockFileUploadHandler);

      const zipProcessor = new ZipProcessor(mockS3Service, mockCSVProcessor);

      // Verify that dependencies are properly injected
      expect(zipProcessor.s3Service).toBe(mockS3Service);
      expect(zipProcessor.csvProcessor).toBe(mockCSVProcessor);
    });
  });

  describe('Error Handling with Injected Dependencies', () => {
    it('should handle S3Service errors properly', async () => {
      mockS3Service.getObjectAsBuffer.mockRejectedValue(new Error('S3 access denied'));
      mockS3Service.validateFileSize.mockResolvedValue({ isValid: true });

      const zipProcessor = new ZipProcessor(mockS3Service, mockCSVProcessor);

      const result = await zipProcessor.processZipFile('test-bucket', 'test-file.zip');

      expect(result.success).toBe(false);
      expect(result.errors?.[0]).toContain('S3 access denied');
    });

    it('should handle CSVProcessor errors properly', () => {
      mockCSVProcessor.isCSVFile.mockImplementation(() => {
        throw new Error('CSV processor error');
      });

      const zipProcessor = new ZipProcessor(mockS3Service, mockCSVProcessor);

      expect(() => {
        zipProcessor.csvProcessor.isCSVFile('test.csv');
      }).toThrow('CSV processor error');
    });
  });
});
