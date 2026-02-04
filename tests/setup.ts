// Test setup file for global configuration

// Set test environment variables
process.env.NODE_ENV = 'test';
process.env.BUCKET_NAME = 'test-bucket';
process.env.INPUT_PATH = 'test-input/';
process.env.OUTPUT_PATH = 'test-output/';
process.env.LOG_LEVEL = 'ERROR'; // Reduce noise in tests
process.env.AWS_REGION = 'us-east-1';

// Mock AWS SDK clients to prevent actual AWS calls during tests
jest.mock('@aws-sdk/client-s3', () => {
  const mockS3 = {
    send: jest.fn(),
  };

  return {
    S3Client: jest.fn(() => mockS3),
    GetObjectCommand: jest.fn(),
    PutObjectCommand: jest.fn(),
    HeadObjectCommand: jest.fn(),
  };
});

// Global test timeout (for integration tests that might take longer)
jest.setTimeout(30000);
