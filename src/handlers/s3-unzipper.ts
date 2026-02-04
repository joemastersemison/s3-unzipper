import type { Context, S3Event, S3Handler } from 'aws-lambda';
import { CSVProcessor } from '../services/csv-processor';
import { S3Service } from '../services/s3-service';
import { ZipProcessor } from '../services/zip-processor';
import type { ProcessingResult, S3EventRecord } from '../types';
import config from '../utils/config';
import logger from '../utils/logger';
import { memoryMonitor } from '../utils/memory-monitor';
import { getS3ClientInfo } from '../utils/s3-client-singleton';
import { validateS3Event } from '../utils/validation';

/**
 * AWS Lambda handler for processing S3 ObjectCreated events on zip files
 */
export const handler: S3Handler = async (event: S3Event, context: Context) => {
  // Add request context to logger
  logger.addContext({
    requestId: context.awsRequestId,
    functionName: context.functionName,
    functionVersion: context.functionVersion,
  });

  // Log initial memory status and S3 client info
  const initialMemory = memoryMonitor.getMemoryUsage();
  const s3ClientInfo = getS3ClientInfo();

  logger.info('S3 Unzipper Lambda started', {
    eventRecords: event.Records.length,
    requestId: context.awsRequestId,
    remainingTimeMs: context.getRemainingTimeInMillis(),
    initialMemoryUsagePercentage: Math.round(initialMemory.usagePercentage * 100) / 100,
    initialHeapUsedMB: Math.round(initialMemory.heapUsed / 1024 / 1024),
    s3ClientReused: s3ClientInfo.isInitialized,
    s3ClientAgeDaysMs: s3ClientInfo.ageMs ?? undefined,
  });

  // Check initial memory state
  if (!memoryMonitor.checkMemoryUsage('lambda_start')) {
    throw new Error('Lambda cannot start - memory usage already too high');
  }

  const results: ProcessingResult[] = [];
  let overallSuccess = true;

  try {
    // Validate configuration
    if (!config.bucketName) {
      throw new Error('BUCKET_NAME environment variable is not set');
    }

    // Initialize services with dependency injection
    const s3Service = new S3Service();
    const csvProcessor = new CSVProcessor({
      timestampColumn: config.csvTimestampColumn,
      dateFormat: 'ISO',
      skipMalformed: config.csvSkipMalformed,
    });
    const zipProcessor = new ZipProcessor(s3Service, csvProcessor);

    // Process each S3 event record
    for (let i = 0; i < event.Records.length; i++) {
      const record = event.Records[i];

      try {
        const result = await processS3Record(record, zipProcessor, context);
        results.push(result);

        if (!result.success) {
          overallSuccess = false;
        }
      } catch (error) {
        const errorMsg = `Failed to process record ${i + 1}: ${error instanceof Error ? error.message : String(error)}`;
        logger.error(errorMsg, error as Error, {
          recordIndex: i,
          requestId: context.awsRequestId,
        });

        results.push({
          success: false,
          filesProcessed: 0,
          errors: [errorMsg],
        });

        overallSuccess = false;
      }

      // Check remaining execution time
      const remainingTime = context.getRemainingTimeInMillis();
      if (remainingTime < 30000) {
        // 30 seconds buffer
        logger.warn('Lambda function running low on time, stopping processing', {
          remainingTimeMs: remainingTime,
          processedRecords: i + 1,
          totalRecords: event.Records.length,
        });
        break;
      }
    }

    // Log overall results with memory information
    const totalFilesProcessed = results.reduce((sum, result) => sum + result.filesProcessed, 0);
    const totalErrors = results.reduce((sum, result) => sum + (result.errors?.length || 0), 0);
    const finalMemory = memoryMonitor.getMemoryUsage();

    logger.info('S3 Unzipper Lambda completed', {
      overallSuccess,
      recordsProcessed: results.length,
      totalRecords: event.Records.length,
      totalFilesProcessed,
      totalErrors,
      requestId: context.awsRequestId,
      remainingTimeMs: context.getRemainingTimeInMillis(),
      finalMemoryUsagePercentage: Math.round(finalMemory.usagePercentage * 100) / 100,
      finalHeapUsedMB: Math.round(finalMemory.heapUsed / 1024 / 1024),
      memoryDeltaPercentage:
        Math.round((finalMemory.usagePercentage - initialMemory.usagePercentage) * 100) / 100,
      circuitBreakerTripped: memoryMonitor.isCircuitBreakerTripped(),
    });

    // Force final garbage collection if available
    memoryMonitor.forceGarbageCollection('lambda_completion');

    // If there were any failures, throw an error to mark the Lambda as failed
    if (!overallSuccess) {
      const errorSummary = results
        .filter(result => !result.success)
        .map(result => result.errors?.join('; '))
        .join(' | ');

      throw new Error(`Processing failed with errors: ${errorSummary}`);
    }

    return;
  } catch (error) {
    const errorMsg = `S3 Unzipper Lambda failed: ${error instanceof Error ? error.message : String(error)}`;
    logger.error(errorMsg, error as Error, {
      requestId: context.awsRequestId,
      totalResultsCount: results.length,
      successfulResults: results.filter(r => r.success).length,
      failedResults: results.filter(r => !r.success).length,
    });

    throw new Error(errorMsg);
  } finally {
    // Clear logger context
    logger.clearContext();
  }
};

/**
 * Processes a single S3 event record
 */
async function processS3Record(
  record: S3EventRecord,
  zipProcessor: ZipProcessor,
  context: Context
): Promise<ProcessingResult> {
  // Validate the S3 event record comprehensively
  const validation = validateS3Event(record);
  if (!validation.isValid) {
    throw new Error(`Invalid S3 event record: ${validation.error}`);
  }

  // Safe to access after validation
  const bucket = record.s3.bucket.name;
  const encodedKey = record.s3.object.key;

  // Decode the S3 key (validation already handles this, but we need the decoded value)
  const key = decodeURIComponent(encodedKey.replace(/\+/g, ' '));

  logger.info('Processing S3 record', {
    eventName: record.eventName,
    eventSource: record.eventSource,
    bucket,
    key,
    objectSize: record.s3.object.size,
    requestId: context.awsRequestId,
  });

  // Validate that this is a zip file
  if (!key.toLowerCase().endsWith('.zip')) {
    logger.warn('Skipping non-zip file', { bucket, key });
    return {
      success: true,
      filesProcessed: 0,
      errors: [],
    };
  }

  // Process the zip file
  try {
    const result = await zipProcessor.processZipFile(bucket, key, context.awsRequestId);

    logger.info('Completed processing S3 record', {
      bucket,
      key,
      success: result.success,
      filesProcessed: result.filesProcessed,
      errors: result.errors?.length || 0,
      requestId: context.awsRequestId,
    });

    return result;
  } catch (error) {
    const errorMsg = `Failed to process zip file ${key}: ${error instanceof Error ? error.message : String(error)}`;
    logger.error(errorMsg, error as Error, {
      bucket,
      key,
      requestId: context.awsRequestId,
    });

    return {
      success: false,
      filesProcessed: 0,
      errors: [errorMsg],
    };
  }
}

/**
 * Health check function for testing
 */
export const healthCheck = async () => {
  return {
    statusCode: 200,
    body: JSON.stringify({
      message: 'S3 Unzipper Lambda is healthy',
      timestamp: new Date().toISOString(),
      config: {
        bucketName: config.bucketName,
        inputPath: config.inputPath,
        outputPath: config.outputPath,
        logLevel: config.logLevel,
        region: config.region,
      },
    }),
  };
};
