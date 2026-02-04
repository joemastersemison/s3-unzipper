# S3 Unzipper Lambda

A TypeScript AWS Lambda function that processes zip files from S3, extracts them, and organizes output files by extracted "stem names" from complex filenames.

## Overview

This Lambda function is triggered by S3 ObjectCreated events on `.zip` files. It:

1. Downloads zip files from a specified S3 input path
2. Extracts files from the zip archives
3. Parses filenames to extract meaningful "stem names" (e.g., `2026-01-01__2026-01-02_app_registration_report.csv` → `app_registration_report`)
4. Uploads extracted files to organized output paths: `s3://[bucket]/[output-path]/[stem-name]/[filename]`

## Architecture

- **Runtime**: Node.js 20.x
- **Infrastructure**: AWS SAM (Serverless Application Model)
- **Trigger**: S3 ObjectCreated events on `.zip` files
- **Dependencies**: AWS SDK v3, yauzl for zip processing, AWS Lambda Powertools for logging

## Project Structure

```
s3-unzipper/
├── src/
│   ├── handlers/
│   │   └── s3-unzipper.ts          # Main Lambda handler
│   ├── services/
│   │   ├── s3-service.ts           # S3 operations
│   │   ├── zip-processor.ts        # Zip extraction logic
│   │   └── filename-parser.ts      # Stem name extraction
│   ├── types/
│   │   └── index.ts                # TypeScript interfaces
│   └── utils/
│       ├── logger.ts               # Structured logging
│       └── config.ts               # Environment configuration
├── tests/                          # Unit and integration tests
├── events/                         # Sample S3 events for testing
├── template.yaml                   # SAM template
├── package.json
├── tsconfig.json
└── samconfig.toml
```

## Prerequisites

- Node.js 18+ and npm
- AWS CLI configured with appropriate permissions
- AWS SAM CLI
- An existing S3 bucket for processing

## Installation

1. Clone the repository and install dependencies:

```bash
npm install
```

2. Build the TypeScript code:

```bash
npm run build
```

## Configuration

The application uses environment variables for configuration:

- `BUCKET_NAME`: S3 bucket name for processing (required)
- `INPUT_PATH`: S3 prefix for zip file inputs (default: "input/")
- `OUTPUT_PATH`: S3 prefix for extracted file outputs (default: "output/")
- `LOG_LEVEL`: Logging level (default: "INFO")
- `AWS_REGION`: AWS region (default: "us-east-1")

Update `samconfig.toml` with your specific configuration:

```toml
parameter_overrides = [
    "BucketName=your-bucket-name",
    "Region=us-east-1",
    "InputPath=input/",
    "OutputPath=output/",
    "LogLevel=INFO"
]
```

## Development

### Local Testing

1. Test the filename parser:

```bash
npm test -- tests/unit/filename-parser.test.ts
```

2. Run all tests:

```bash
npm test
```

3. Test locally with SAM:

```bash
npm run sam:build
npm run sam:local
```

### Code Quality

- **Linting**: `npm run lint`
- **Type Checking**: `npx tsc --noEmit`
- **Testing**: `npm test`

## Deployment

### First-time Deployment

1. Build and deploy with guided setup:

```bash
npm run deploy:guided
```

2. Follow the prompts to configure:
   - Stack name
   - AWS region
   - Parameter values (bucket name, paths, etc.)

### Subsequent Deployments

```bash
npm run deploy
```

## Filename Processing Logic

The system extracts "stem names" from complex filenames using these rules:

1. **Remove file extensions**: `file.csv` → `file`
2. **Split by underscores**: `part1_part2_part3`
3. **Filter date patterns**: Remove parts matching `YYYY-MM-DD` format
4. **Find meaningful content**: Start from the first part that begins with a letter
5. **Join remaining parts**: Combine with underscores

### Examples

| Input Filename | Stem Name |
|---|---|
| `2026-01-01__2026-01-02_app_registration_report.csv` | `app_registration_report` |
| `2026-01-01_badge.csv` | `badge` |
| `user_report_2026-01-01.txt` | `user_report` |
| `2026-01-01__2026-01-02.csv` | `unknown` |

## Usage Example

1. **Upload a zip file** to your S3 bucket at `s3://your-bucket/input/test.zip`
2. **The Lambda function** is automatically triggered
3. **Files are extracted** and organized at `s3://your-bucket/output/[stem-name]/[filename]`

For a zip containing:
- `2026-01-01_user_report.csv`
- `2026-01-01_badge_data.xlsx`

Output structure:
```
s3://your-bucket/output/
├── user_report/
│   └── 2026-01-01_user_report.csv
└── badge_data/
    └── 2026-01-01_badge_data.xlsx
```

## Monitoring

The Lambda function uses AWS Lambda Powertools for structured logging. Logs are available in CloudWatch under `/aws/lambda/[function-name]`.

Key log events:
- Processing start/end
- File extraction details
- Error handling and debugging

## Troubleshooting

### Common Issues

1. **Permission Denied**
   - Ensure the Lambda execution role has S3 read/write permissions
   - Check bucket policies and CORS settings

2. **Zip Processing Fails**
   - Verify zip file integrity
   - Check file size limits (Lambda has memory/timeout constraints)

3. **Files Not Organized Correctly**
   - Review filename parsing logic
   - Check debug logs for stem name extraction

### Debugging

Enable debug logging by setting `LOG_LEVEL=DEBUG` in your deployment configuration.

## Performance Considerations

- **Memory**: Default 1024MB, increase for larger zip files
- **Timeout**: Default 15 minutes, adjust based on processing needs
- **Concurrency**: Lambda handles multiple zip files in parallel

## Security

- Uses least-privilege IAM roles
- No hardcoded credentials
- Secure environment variable handling
- Input validation and error handling

## Contributing

1. Follow TypeScript best practices
2. Add tests for new functionality
3. Update documentation
4. Ensure all tests pass before submitting

## License

MIT License - see LICENSE file for details.