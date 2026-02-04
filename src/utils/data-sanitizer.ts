/**
 * Data sanitization utilities for secure logging
 * Prevents PII and sensitive information leaks in logs
 */

export interface SanitizationOptions {
  preserveLength?: boolean;
  maskChar?: string;
  visibleChars?: number;
}

/**
 * Sanitizes file paths and S3 keys to remove potentially sensitive information
 * while preserving useful debugging context
 */
export class DataSanitizer {
  private static readonly EMAIL_REGEX = /[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}/g;
  private static readonly PHONE_REGEX =
    /(\+?1)?[-.\s]?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}/g;
  private static readonly SSN_REGEX = /\d{3}-?\d{2}-?\d{4}/g;
  private static readonly CREDIT_CARD_REGEX = /\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}/g;
  private static readonly IP_ADDRESS_REGEX = /(?:[0-9]{1,3}\.){3}[0-9]{1,3}/g;

  // Common PII patterns in filenames
  private static readonly PERSONAL_IDENTIFIERS = [
    /\b(user|customer|client|employee)[-_]?id[-_]?\d+/gi,
    /\b(account|acct)[-_]?\d{8,}/gi,
    /\b(order|invoice|transaction)[-_]?\d{8,}/gi,
    /\bmember[-_]?\d{6,}/gi,
    /\buser[-_]?\d{6,}/gi,
  ];

  /**
   * Sanitizes an S3 key or file path for logging
   * Preserves directory structure and file extension while masking sensitive parts
   */
  static sanitizeS3Key(s3Key: string, options: SanitizationOptions = {}): string {
    if (!s3Key) return '[empty-key]';

    const { maskChar = '*', visibleChars = 3 } = options;

    // Split path into components
    const parts = s3Key.split('/');
    const sanitizedParts = parts.map((part, index) => {
      // Don't sanitize directory names (except the last part which might be a filename)
      if (index < parts.length - 1 && !part.includes('.')) {
        return DataSanitizer.sanitizeDirectoryName(part, { maskChar, visibleChars });
      }

      // Sanitize filename
      return DataSanitizer.sanitizeFilename(part, { maskChar, visibleChars });
    });

    return sanitizedParts.join('/');
  }

  /**
   * Sanitizes a filename while preserving extension and general structure
   */
  static sanitizeFilename(filename: string, options: SanitizationOptions = {}): string {
    if (!filename) return '[empty-filename]';

    const { maskChar = '*' } = options;

    // Check if it contains obvious PII patterns
    let sanitized = filename;

    // Mask email addresses
    const emailRegex = new RegExp(
      DataSanitizer.EMAIL_REGEX.source,
      DataSanitizer.EMAIL_REGEX.flags
    );
    sanitized = sanitized.replace(emailRegex, match =>
      DataSanitizer.maskString(match, { maskChar, visibleChars: 2 })
    );

    // Mask phone numbers
    const phoneRegex = new RegExp(
      DataSanitizer.PHONE_REGEX.source,
      DataSanitizer.PHONE_REGEX.flags
    );
    sanitized = sanitized.replace(phoneRegex, match =>
      DataSanitizer.maskString(match, { maskChar, visibleChars: 0 })
    );

    // Mask SSN patterns
    const ssnRegex = new RegExp(DataSanitizer.SSN_REGEX.source, DataSanitizer.SSN_REGEX.flags);
    sanitized = sanitized.replace(ssnRegex, match =>
      DataSanitizer.maskString(match, { maskChar, visibleChars: 0 })
    );

    // Mask credit card patterns
    const ccRegex = new RegExp(
      DataSanitizer.CREDIT_CARD_REGEX.source,
      DataSanitizer.CREDIT_CARD_REGEX.flags
    );
    sanitized = sanitized.replace(ccRegex, match =>
      DataSanitizer.maskString(match, { maskChar, visibleChars: 0 })
    );

    // Mask personal identifier patterns
    DataSanitizer.PERSONAL_IDENTIFIERS.forEach(pattern => {
      const freshPattern = new RegExp(pattern.source, pattern.flags);
      sanitized = sanitized.replace(freshPattern, match =>
        DataSanitizer.maskString(match, { maskChar, visibleChars: 2 })
      );
    });

    // If filename is very long, truncate middle part but preserve extension
    if (sanitized.length > 50) {
      const extension = DataSanitizer.getFileExtension(sanitized);
      const baseName = sanitized.substring(0, sanitized.length - extension.length);

      if (baseName.length > 40) {
        const start = baseName.substring(0, 15);
        const end = baseName.substring(baseName.length - 10);
        sanitized = `${start}...${maskChar.repeat(5)}...${end}${extension}`;
      }
    }

    return sanitized;
  }

  /**
   * Sanitizes directory names to remove potential PII
   */
  static sanitizeDirectoryName(dirName: string, options: SanitizationOptions = {}): string {
    if (!dirName) return '[empty-dir]';

    const { maskChar = '*', visibleChars = 3 } = options;
    let sanitized = dirName;

    // Check for obvious PII patterns in directory names
    DataSanitizer.PERSONAL_IDENTIFIERS.forEach(pattern => {
      sanitized = sanitized.replace(pattern, match =>
        DataSanitizer.maskString(match, { maskChar, visibleChars })
      );
    });

    // Mask email addresses in directory names
    sanitized = sanitized.replace(DataSanitizer.EMAIL_REGEX, match =>
      DataSanitizer.maskString(match, { maskChar, visibleChars: 2 })
    );

    return sanitized;
  }

  /**
   * Sanitizes error messages to remove sensitive information
   */
  static sanitizeErrorMessage(message: string, context?: Record<string, any>): string {
    if (!message) return '[empty-message]';

    let sanitized = message;

    // Remove potential file paths that might contain sensitive info
    sanitized = sanitized.replace(/\/[^\s]+/g, match => {
      if (match.includes('/tmp/') || match.includes('/var/') || match.includes('/home/')) {
        return DataSanitizer.sanitizeFilePath(match);
      }
      return match;
    });

    // Remove potential S3 URLs
    sanitized = sanitized.replace(/https?:\/\/[^/]+\.amazonaws\.com\/[^\s]+/g, match => {
      return DataSanitizer.sanitizeS3Url(match);
    });

    // Remove IP addresses
    sanitized = sanitized.replace(DataSanitizer.IP_ADDRESS_REGEX, '[IP-ADDRESS]');

    // Remove email addresses
    sanitized = sanitized.replace(DataSanitizer.EMAIL_REGEX, '[EMAIL]');

    // If context contains bucket/key info, use sanitized versions in the message
    if (context?.bucket && context.key) {
      const sanitizedKey = DataSanitizer.sanitizeS3Key(context.key);
      // Escape special regex characters in the key before using it in RegExp
      const escapedKey = context.key.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      sanitized = sanitized.replace(new RegExp(escapedKey, 'g'), sanitizedKey);
    }

    return sanitized;
  }

  /**
   * Sanitizes stack traces to remove sensitive paths
   */
  static sanitizeStackTrace(stackTrace: string): string {
    if (!stackTrace) return '[empty-stack]';

    return stackTrace
      .split('\n')
      .map(line => {
        // Remove full file paths, keep only filename and line number
        return line.replace(/\/[^:]+\/([^/:]+):(\d+):(\d+)/g, '.../$1:$2:$3');
      })
      .join('\n');
  }

  /**
   * Creates a sanitized logging context that removes sensitive information
   */
  static sanitizeLoggingContext(context: Record<string, any>): Record<string, any> {
    const sanitized: Record<string, any> = {};

    for (const [key, value] of Object.entries(context)) {
      if (typeof value === 'string') {
        switch (key) {
          case 'key':
          case 'inputKey':
          case 'outputKey':
          case 'outputPath':
            sanitized[key] = DataSanitizer.sanitizeS3Key(value);
            break;
          case 'fileName':
          case 'filename':
            sanitized[key] = DataSanitizer.sanitizeFilename(value);
            break;
          case 'error':
            sanitized[key] = DataSanitizer.sanitizeErrorMessage(value, context);
            break;
          default:
            // Check if the value contains potential PII
            if (DataSanitizer.containsPII(value)) {
              sanitized[key] = DataSanitizer.maskString(value);
            } else {
              sanitized[key] = value;
            }
        }
      } else if (value && typeof value === 'object') {
        if (key === 'error' && value.stack) {
          sanitized[key] = {
            ...value,
            stack: DataSanitizer.sanitizeStackTrace(value.stack),
            message: DataSanitizer.sanitizeErrorMessage(value.message, context),
          };
        } else {
          sanitized[key] = DataSanitizer.sanitizeLoggingContext(value);
        }
      } else {
        sanitized[key] = value;
      }
    }

    return sanitized;
  }

  /**
   * Utility method to mask strings while preserving some characters for debugging
   */
  private static maskString(str: string, options: SanitizationOptions = {}): string {
    const { maskChar = '*', visibleChars = 3 } = options;

    if (str.length <= visibleChars * 2) {
      return maskChar.repeat(str.length);
    }

    const start = str.substring(0, visibleChars);
    const end = str.substring(str.length - visibleChars);
    const middleLength = Math.max(3, str.length - visibleChars * 2);

    return `${start}${maskChar.repeat(middleLength)}${end}`;
  }

  /**
   * Gets file extension including the dot
   */
  private static getFileExtension(filename: string): string {
    const lastDot = filename.lastIndexOf('.');
    return lastDot >= 0 ? filename.substring(lastDot) : '';
  }

  /**
   * Sanitizes file paths to remove sensitive directory information
   */
  private static sanitizeFilePath(path: string): string {
    const parts = path.split('/');
    // Keep first part and last 2 parts, mask the middle
    if (parts.length <= 3) {
      return path;
    }

    const first = parts[0];
    const last = parts.slice(-2);
    const maskedMiddle = '***';

    return `${first}/${maskedMiddle}/${last.join('/')}`;
  }

  /**
   * Sanitizes S3 URLs to remove bucket and key details
   */
  private static sanitizeS3Url(url: string): string {
    try {
      const urlObj = new URL(url);
      const hostname = urlObj.hostname;
      const _bucket = hostname.split('.')[0];
      const pathParts = urlObj.pathname.split('/').filter(p => p);

      if (pathParts.length === 0) {
        return `https://[BUCKET].s3.amazonaws.com/`;
      }

      const sanitizedKey = DataSanitizer.sanitizeS3Key(pathParts.join('/'));
      return `https://[BUCKET].s3.amazonaws.com/${sanitizedKey}`;
    } catch {
      return '[S3-URL]';
    }
  }

  /**
   * Checks if a string potentially contains PII
   */
  private static containsPII(str: string): boolean {
    return (
      DataSanitizer.EMAIL_REGEX.test(str) ||
      DataSanitizer.PHONE_REGEX.test(str) ||
      DataSanitizer.SSN_REGEX.test(str) ||
      DataSanitizer.CREDIT_CARD_REGEX.test(str) ||
      DataSanitizer.PERSONAL_IDENTIFIERS.some(pattern => pattern.test(str))
    );
  }
}

/**
 * Convenience functions for common sanitization tasks
 */
export const sanitizeForLogging = {
  s3Key: (key: string) => DataSanitizer.sanitizeS3Key(key),
  filename: (filename: string) => DataSanitizer.sanitizeFilename(filename),
  errorMessage: (message: string, context?: Record<string, any>) =>
    DataSanitizer.sanitizeErrorMessage(message, context),
  context: (context: Record<string, any>) => DataSanitizer.sanitizeLoggingContext(context),
  stackTrace: (stack: string) => DataSanitizer.sanitizeStackTrace(stack),
};
