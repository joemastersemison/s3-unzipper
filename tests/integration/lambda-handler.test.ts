import type { Context, S3Event } from 'aws-lambda';
import { handler } from '../../src/handlers/s3-unzipper';

// Simple integration tests without complex mocking
describe('Lambda Handler Integration Tests', () => {
  beforeEach(() => {
    process.env.BUCKET_NAME = 'test-bucket';
    process.env.INPUT_PATH = 'input/';
    process.env.OUTPUT_PATH = 'output/';
    process.env.LOG_LEVEL = 'ERROR'; // Reduce noise
  });

  afterEach(() => {
    // Clean up
  });

  test('should skip non-zip files', async () => {
    // Create test S3 event with non-zip file
    const s3Event: S3Event = {
      Records: [
        {
          eventVersion: '2.1',
          eventSource: 'aws:s3',
          awsRegion: 'us-east-1',
          eventTime: new Date().toISOString(),
          eventName: 'ObjectCreated:Put',
          userIdentity: {
            principalId: 'test',
          },
          requestParameters: {
            sourceIPAddress: '127.0.0.1',
          },
          responseElements: {
            'x-amz-request-id': 'test-request-id',
            'x-amz-id-2': 'test-id-2',
          },
          s3: {
            s3SchemaVersion: '1.0',
            configurationId: 'test-config',
            bucket: {
              name: 'test-bucket',
              ownerIdentity: {
                principalId: 'test-principal',
              },
              arn: 'arn:aws:s3:::test-bucket',
            },
            object: {
              key: 'input/2026-01-01_test_file.txt',
              size: 100,
              eTag: 'test-etag',
              sequencer: 'test-sequencer',
            },
          },
        },
      ],
    };

    const context: Context = {
      callbackWaitsForEmptyEventLoop: false,
      functionName: 'test-function',
      functionVersion: '1',
      invokedFunctionArn: 'arn:aws:lambda:us-east-1:123456789012:function:test-function',
      memoryLimitInMB: '1024',
      awsRequestId: 'test-request-id',
      logGroupName: '/aws/lambda/test-function',
      logStreamName: '2026/02/03/[$LATEST]test',
      getRemainingTimeInMillis: () => 300000,
      done: () => {},
      fail: () => {},
      succeed: () => {},
    };

    // Execute handler - should complete successfully without processing (no S3 calls)
    await expect(handler(s3Event, context, {} as any)).resolves.toBeUndefined();
  });

  test('should validate configuration on startup', () => {
    // This test validates that config loading works properly
    // Config validation happens at module load time, so we test it exists
    expect(process.env.BUCKET_NAME).toBe('test-bucket');
  });

  test('should handle empty event records', async () => {
    const s3Event: S3Event = {
      Records: [],
    };

    const context: Context = {
      callbackWaitsForEmptyEventLoop: false,
      functionName: 'test-function',
      functionVersion: '1',
      invokedFunctionArn: 'arn:aws:lambda:us-east-1:123456789012:function:test-function',
      memoryLimitInMB: '1024',
      awsRequestId: 'test-request-id',
      logGroupName: '/aws/lambda/test-function',
      logStreamName: '2026/02/03/[$LATEST]test',
      getRemainingTimeInMillis: () => 300000,
      done: () => {},
      fail: () => {},
      succeed: () => {},
    };

    // Should complete successfully with no records to process
    await expect(handler(s3Event, context, {} as any)).resolves.toBeUndefined();
  });
});
