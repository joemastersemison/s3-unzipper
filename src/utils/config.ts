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
  bucketName: Joi.string().min(3).max(63).required(),
  inputPath: Joi.string().default('input/'),
  outputPath: Joi.string().default('output/'),
  logLevel: Joi.string().valid('DEBUG', 'INFO', 'WARN', 'ERROR').default('INFO'),
  region: Joi.string().default('us-east-1'),
  maxMemoryUsage: Joi.number().min(128).max(10240).default(1024),
  timeout: Joi.number().min(30).max(900).default(900),
  csvProcessingEnabled: Joi.boolean().default(true),
  csvTimestampColumn: Joi.string().default('_processed'),
  csvSkipMalformed: Joi.boolean().default(true),
});

function loadConfig(): Config {
  const rawConfig = {
    bucketName: process.env.BUCKET_NAME,
    inputPath: process.env.INPUT_PATH || 'input/',
    outputPath: process.env.OUTPUT_PATH || 'output/',
    logLevel: process.env.LOG_LEVEL || 'INFO',
    region: process.env.AWS_REGION || 'us-east-1',
    maxMemoryUsage: parseInt(process.env.MAX_MEMORY_USAGE || '1024', 10),
    timeout: parseInt(process.env.TIMEOUT || '900', 10),
    csvProcessingEnabled: process.env.CSV_PROCESSING_ENABLED !== 'false',
    csvTimestampColumn: process.env.CSV_TIMESTAMP_COLUMN || '_processed',
    csvSkipMalformed: process.env.CSV_SKIP_MALFORMED !== 'false',
  };

  const { error, value } = configSchema.validate(rawConfig);

  if (error) {
    throw new Error(`Configuration validation failed: ${error.message}`);
  }

  return value;
}

export const config = loadConfig();

export default config;
