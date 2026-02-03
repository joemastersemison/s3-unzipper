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
  [key: string]: any;
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
