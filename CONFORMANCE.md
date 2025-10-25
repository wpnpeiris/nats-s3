# S3 API Conformance

This project runs comprehensive AWS CLI-based S3 API compatibility tests in CI to ensure nats-s3 maintains high compatibility with the S3 standard.

## Test Coverage

The CI pipeline runs two test suites:

### 1. Basic Conformance Tests (`ci-conformance.sh`)
Quick smoke test covering essential S3 operations:
- ✅ Bucket create (`aws s3 mb`)
- ✅ Service-level bucket listing (`aws s3 ls`)
- ✅ Object upload (`aws s3 cp`)
- ✅ Bucket content listing
- ✅ Object metadata (`aws s3api head-object`)
- ✅ Object download with content verification
- ✅ Object deletion
- ✅ Bucket deletion

### 2. Full Conformance Tests (`ci-conformance-full.sh`)
Comprehensive test suite covering advanced S3 features:

**Core Operations:**
- ✅ Bucket lifecycle (create, duplicate detection, delete)
- ✅ Object CRUD (put, get, head, delete)
- ✅ Content integrity verification
- ✅ Service-level operations

**Advanced Features:**
- ✅ **Metadata & Headers**: Content-Type, custom metadata (`x-amz-meta-*`)
- ✅ **Object Copy**: Server-side copy operations (`CopyObject`)
- ✅ **Hierarchical Listing**: Delimiter and prefix support for directory-like navigation
- ✅ **Range Requests**: HTTP Range GET for partial content (`Range: bytes=0-9`)
- ✅ **Multipart Uploads**: Large file uploads via multipart API
  - Initiate multipart upload
  - Upload parts
  - Complete multipart upload
- ✅ **Batch Operations**: Delete multiple objects (`DeleteObjects`)
- ✅ **Presigned URLs**: Time-limited pre-authenticated URLs
- ✅ **Bucket Validation**: Non-empty bucket deletion prevention

**Current Status:** 25/25 tests passing (100% pass rate)

## S3 API Feature Matrix

| Feature Category | Support Status | Notes |
|-----------------|----------------|-------|
| **Bucket Operations** | ✅ Full | Create, Delete, List with validation |
| **Object Operations** | ✅ Full | PUT, GET, HEAD, DELETE, Copy |
| **Multipart Uploads** | ✅ Full | Complete multipart workflow |
| **Authentication** | ✅ Full | AWS SigV4 (headers + presigned URLs) |
| **Metadata** | ✅ Full | Content-Type, custom metadata |
| **Range Requests** | ✅ Full | HTTP Range GET (RFC 7233) |
| **Batch Operations** | ✅ Full | DeleteObjects |
| **Hierarchical Listing** | ✅ Full | Delimiter/prefix support |
| **Presigned URLs** | ✅ Full | Query-string authentication |
| **ACLs** | ⚠️ Planned | Returns 501 Not Implemented |
| **Versioning** | ⚠️ Planned | Returns 501 Not Implemented |
| **Lifecycle Policies** | ⚠️ Planned | Returns 501 Not Implemented |

## Running Tests Locally

```bash
# Start NATS with JetStream
docker run -d --name nats-js -p 4222:4222 nats:latest -js

# Build and start gateway
make build
./bin/nats-s3 --listen 0.0.0.0:5222 \
  --natsServers nats://127.0.0.1:4222 \
  --s3.credentials credentials.json

# Run basic conformance tests
AWS_ACCESS_KEY_ID=my-access-key \
AWS_SECRET_ACCESS_KEY=my-secret-key \
AWS_DEFAULT_REGION=us-east-1 \
bash scripts/ci-conformance.sh

# Run full conformance tests
AWS_ACCESS_KEY_ID=my-access-key \
AWS_SECRET_ACCESS_KEY=my-secret-key \
AWS_DEFAULT_REGION=us-east-1 \
bash scripts/ci-conformance-full.sh
```

## CI Workflow

The GitHub Actions workflow:
1. Starts NATS server with JetStream enabled
2. Builds the nats-s3 gateway
3. Runs both basic and full conformance test suites
4. Reports results and uploads logs on failure

[![Conformance](https://github.com/wpnpeiris/nats-s3/actions/workflows/conformance.yml/badge.svg)](https://github.com/wpnpeiris/nats-s3/actions/workflows/conformance.yml)

## Artifacts & Debugging

- Test results are saved to `conformance-full.txt`
- On failures, the workflow dumps `gateway.log` for debugging
- Coverage reports available as workflow artifacts
