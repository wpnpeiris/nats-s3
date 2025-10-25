# NATS-S3 Python Examples

Python examples demonstrating how to use NATS-S3 with the boto3 library.

## Prerequisites

1. **Python 3.7+** installed
2. **NATS-S3 running** (via Docker Compose or binary)
3. **pip** package manager

## Quick Start

### Step 1: Start NATS-S3

From the `examples/` directory:

```bash
cd ..
docker-compose up -d
```

Verify it's running:
```bash
docker-compose ps
```

### Step 2: Set Up Python Environment

```bash
cd python

# Create a virtual environment (optional but recommended)
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Step 3: Run the Example

```bash
python nats_s3_example.py
```

## What the Example Does

The example demonstrates all major S3 operations:

1. ✅ Create bucket
2. ✅ List buckets
3. ✅ Upload objects
4. ✅ Upload objects with metadata
5. ✅ List objects
6. ✅ Get object metadata (HEAD)
7. ✅ Download objects
8. ✅ List objects with prefix (hierarchical)
9. ✅ Copy objects
10. ✅ Generate presigned URLs
11. ✅ Delete individual objects
12. ✅ Batch delete objects
13. ✅ Delete bucket

## Expected Output

```
============================================================
NATS-S3 Python Example with Boto3
============================================================

1. Creating bucket 'python-example-bucket'...
   ✓ Bucket created successfully

2. Listing all buckets...
   - python-example-bucket

3. Uploading object to 'python-example-bucket'...
   ✓ Uploaded 'test-file.txt'

... (more operations)

============================================================
✓ All operations completed successfully!
============================================================
```

## Troubleshooting

### Connection Refused

Make sure NATS-S3 is running:
```bash
curl http://localhost:5222/healthz
```

### Authentication Errors

Verify credentials match `examples/credentials.json`:
- Access Key: `demo-access-key`
- Secret Key: `demo-secret-key`

### Import Errors

Install boto3:
```bash
pip install boto3
```

## Using with Your Own Application

```python
import boto3

# Configure client
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:5222',
    aws_access_key_id='demo-access-key',
    aws_secret_access_key='demo-secret-key',
    region_name='us-east-1'
)

# Use standard boto3 operations
s3.create_bucket(Bucket='my-bucket')
s3.put_object(Bucket='my-bucket', Key='file.txt', Body=b'Hello!')
obj = s3.get_object(Bucket='my-bucket', Key='file.txt')
print(obj['Body'].read())
```

## Additional Resources

- [Boto3 Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)
- [AWS S3 API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html)
- [NATS-S3 Conformance](../../CONFORMANCE.md)
