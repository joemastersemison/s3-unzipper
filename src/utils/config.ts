import Joi from 'joi';

interface Config {
  bucketName: string;
  inputPath: string;
  outputPath: string;
  logLevel: string;
  region: string;
  maxMemoryUsage: number;
  timeout: number;
  csvProcessingEnabled: boolean;
  csvTimestampColumn: string;
  csvSkipMalformed: boolean;
}

const configSchema = Joi.object({
  bucketName: Joi.string()
    .min(3)
    .max(63)
    .pattern(/^[a-z0-9][a-z0-9\-]*[a-z0-9]$/)
    .required()
    .messages({
      'string.pattern.base':
        'bucketName must be a valid S3 bucket name (lowercase alphanumeric and hyphens only)',
    }),
  inputPath: Joi.string()
    .pattern(/^[a-zA-Z0-9\-_\/]*\/$/)
    .default('input/')
    .messages({
      'string.pattern.base': 'inputPath must be a valid S3 path ending with /',
    }),
  outputPath: Joi.string()
    .pattern(/^[a-zA-Z0-9\-_\/]*\/$/)
    .default('output/')
    .messages({
      'string.pattern.base': 'outputPath must be a valid S3 path ending with /',
    }),
  logLevel: Joi.string()
    .valid('DEBUG', 'INFO', 'WARN', 'ERROR')
    .default(process.env.NODE_ENV === 'production' ? 'INFO' : 'DEBUG'),
  region: Joi.string()
    .pattern(/^[a-z]{2}-[a-z]+-\d+$/)
    .default('us-east-1')
    .messages({
      'string.pattern.base': 'region must be a valid AWS region format',
    }),
  maxMemoryUsage: Joi.number().min(128).max(10240).default(1024),
  timeout: Joi.number().min(30).max(900).default(900),
  csvProcessingEnabled: Joi.boolean().default(true),
  csvTimestampColumn: Joi.string()
    .min(1)
    .max(255)
    .pattern(/^[a-zA-Z_][a-zA-Z0-9_]*$/)
    .default('_processed')
    .messages({
      'string.pattern.base': 'csvTimestampColumn must be a valid column name',
    }),
  csvSkipMalformed: Joi.boolean().default(true),
});

function loadConfig(): Config {
  // Additional security checks for environment variables
  const rawConfig = {
    bucketName: process.env.BUCKET_NAME,
    inputPath: sanitizePath(process.env.INPUT_PATH) || 'input/',
    outputPath: sanitizePath(process.env.OUTPUT_PATH) || 'output/',
    logLevel:
      sanitizeLogLevel(process.env.LOG_LEVEL) ||
      (process.env.NODE_ENV === 'production' ? 'INFO' : 'DEBUG'),
    region: process.env.AWS_REGION || 'us-east-1',
    maxMemoryUsage: parseIntSafely(process.env.MAX_MEMORY_USAGE, 1024, 128, 10240),
    timeout: parseIntSafely(process.env.TIMEOUT, 900, 30, 900),
    csvProcessingEnabled: process.env.CSV_PROCESSING_ENABLED !== 'false',
    csvTimestampColumn: sanitizeColumnName(process.env.CSV_TIMESTAMP_COLUMN) || '_processed',
    csvSkipMalformed: process.env.CSV_SKIP_MALFORMED !== 'false',
  };

  const { error, value } = configSchema.validate(rawConfig);

  if (error) {
    throw new Error(`Configuration validation failed: ${error.message}`);
  }

  // Additional runtime security validations
  validateSecurityConstraints(value);

  return value;
}

/**
 * Sanitizes S3 path to prevent path traversal and ensure safe format
 */
function sanitizePath(path?: string): string | undefined {
  if (!path) return undefined;

  // Remove any path traversal attempts
  const sanitized = path
    .replace(/\.\./g, '')
    .replace(/[<>:"|?*]/g, '')
    .replace(/^\/+/, '');

  // Ensure it ends with / if not empty
  return sanitized && !sanitized.endsWith('/') ? `${sanitized}/` : sanitized;
}

/**
 * Sanitizes log level to prevent injection
 */
function sanitizeLogLevel(logLevel?: string): string | undefined {
  if (!logLevel) return undefined;

  const validLevels = ['DEBUG', 'INFO', 'WARN', 'ERROR'];
  const normalized = logLevel.toUpperCase();

  return validLevels.includes(normalized) ? normalized : undefined;
}

/**
 * Safely parses integer with bounds checking
 */
function parseIntSafely(
  value?: string,
  defaultValue: number = 0,
  min: number = 0,
  max: number = Number.MAX_SAFE_INTEGER
): number {
  if (!value) return defaultValue;

  const parsed = parseInt(value, 10);
  if (isNaN(parsed)) return defaultValue;

  return Math.max(min, Math.min(max, parsed));
}

/**
 * Sanitizes CSV column name to prevent injection
 */
function sanitizeColumnName(columnName?: string): string | undefined {
  if (!columnName) return undefined;

  // Only allow valid identifier characters
  const sanitized = columnName.replace(/[^a-zA-Z0-9_]/g, '');

  // Must start with letter or underscore
  return /^[a-zA-Z_]/.test(sanitized) ? sanitized : undefined;
}

/**
 * Additional security constraint validations
 */
function validateSecurityConstraints(config: Config): void {
  // Ensure paths don't overlap to prevent conflicts
  if (config.inputPath === config.outputPath) {
    throw new Error('inputPath and outputPath cannot be the same');
  }

  // Validate bucket name doesn't contain sensitive patterns
  if (config.bucketName.includes('password') || config.bucketName.includes('secret')) {
    throw new Error('bucketName contains potentially sensitive keywords');
  }

  // Ensure memory limits are reasonable for Lambda
  if (config.maxMemoryUsage > 3008 && process.env.AWS_LAMBDA_FUNCTION_NAME) {
    throw new Error('maxMemoryUsage exceeds Lambda limits');
  }

  // Validate CSV column name doesn't conflict with common data fields
  const reservedColumns = ['password', 'secret', 'key', 'token', 'credential'];
  if (
    reservedColumns.some(reserved => config.csvTimestampColumn.toLowerCase().includes(reserved))
  ) {
    throw new Error('csvTimestampColumn name conflicts with reserved security keywords');
  }
}

export const config = loadConfig();

export default config;
