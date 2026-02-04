export interface ProcessingResult {
  success: boolean;
  filesProcessed: number;
  errors?: string[];
}

export interface ZipEntry {
  fileName: string;
  size: number;
  isDirectory: boolean;
}

export interface ExtractedFile {
  originalName: string;
  stemName: string;
  outputPath: string;
  size: number;
}

export interface S3ObjectInfo {
  bucket: string;
  key: string;
  size?: number;
  lastModified?: Date;
}

export interface ProcessingContext {
  requestId: string;
  bucket: string;
  inputKey: string;
  outputPath: string;
  startTime: Date;
}

export interface FilenameComponents {
  baseName: string;
  extension: string;
  dateParts: string[];
  nonDateParts: string[];
  stemName: string;
}

export interface LoggingContext {
  requestId?: string;
  bucket?: string;
  key?: string;
  operation?: string;
  [key: string]: string | number | boolean | undefined;
}

export interface S3StreamOptions {
  range?: string;
  versionId?: string;
}

export interface UploadOptions {
  contentType?: string;
  metadata?: Record<string, string>;
}

export interface CSVProcessingResult {
  processed: boolean;
  originalSize: number;
  processedSize: number;
  rowsProcessed: number;
  error?: string;
}

export interface CSVProcessorOptions {
  timestampColumn: string;
  dateFormat: string;
  skipMalformed: boolean;
}

export interface S3EventRecord {
  eventVersion?: string;
  eventSource?: string;
  eventTime?: string;
  eventName?: string;
  userIdentity?: {
    type?: string;
    principalId?: string;
    arn?: string;
    accountId?: string;
    accessKeyId?: string;
  };
  requestParameters?: {
    sourceIPAddress?: string;
  };
  responseElements?: {
    'x-amz-request-id'?: string;
    'x-amz-id-2'?: string;
  };
  s3: {
    s3SchemaVersion?: string;
    configurationId?: string;
    bucket: {
      name: string;
      ownerIdentity?: {
        principalId?: string;
      };
      arn?: string;
    };
    object: {
      key: string;
      size?: number;
      eTag?: string;
      sequencer?: string;
    };
  };
}
