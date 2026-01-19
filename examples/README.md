# NATS-S3 Examples

This directory contains example configurations and integration guides for NATS-S3.

## Quick Start with Docker Compose

The fastest way to try NATS-S3:

```bash
cd examples
docker-compose up -d
```

Then test with AWS CLI:

```bash
export AWS_ACCESS_KEY_ID=demo-access-key
export AWS_SECRET_ACCESS_KEY=demo-secret-key
export AWS_DEFAULT_REGION=us-east-1

# Create a bucket
aws s3 mb s3://my-bucket --endpoint-url=http://localhost:5222

# Upload a file
echo "Hello NATS-S3!" > test.txt
aws s3 cp test.txt s3://my-bucket/ --endpoint-url=http://localhost:5222

# List objects
aws s3 ls s3://my-bucket --endpoint-url=http://localhost:5222

# Download the file
aws s3 cp s3://my-bucket/test.txt downloaded.txt --endpoint-url=http://localhost:5222
```

## Directory Structure

```
examples/
├── AWS_CLI_USAGE.md         # Comprehensive AWS CLI command reference
├── docker-compose.yml       # Complete Docker Compose setup
├── credentials.json         # Example S3 credentials
├── python/                  # Python/Boto3 examples
├── kubernetes/              # Kubernetes deployment examples (coming soon)
├── rclone/                  # Rclone integration (coming soon)
├── restic/                  # Restic backup integration (coming soon)
└── terraform/               # Terraform examples (coming soon)
```

## Complete AWS CLI Reference

See **[AWS_CLI_USAGE.md](AWS_CLI_USAGE.md)** for comprehensive examples of all supported S3 operations including:
- Bucket operations (create, delete, list)
- Object operations (upload, download, copy, delete)
- Multipart uploads with complete scripts
- Object tagging (NEW in v0.4.0)
- Object retention for compliance
- Advanced features (sync, presigned URLs, batch operations)

## Integration Examples

### AWS SDK (Python/Boto3)

```python
import boto3

s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:5222',
    aws_access_key_id='demo-access-key',
    aws_secret_access_key='demo-secret-key',
    region_name='us-east-1'
)

# Create bucket
s3.create_bucket(Bucket='my-bucket')

# Upload file
s3.upload_file('local-file.txt', 'my-bucket', 'remote-file.txt')

# List objects
response = s3.list_objects_v2(Bucket='my-bucket')
for obj in response.get('Contents', []):
    print(obj['Key'])
```

### AWS SDK (Node.js)

```javascript
const AWS = require('aws-sdk');

const s3 = new AWS.S3({
    endpoint: 'http://localhost:5222',
    accessKeyId: 'demo-access-key',
    secretAccessKey: 'demo-secret-key',
    region: 'us-east-1',
    s3ForcePathStyle: true
});

// Create bucket
s3.createBucket({ Bucket: 'my-bucket' }, (err, data) => {
    if (err) console.error(err);
    else console.log('Bucket created:', data);
});

// Upload file
s3.putObject({
    Bucket: 'my-bucket',
    Key: 'file.txt',
    Body: 'Hello from Node.js!'
}, (err, data) => {
    if (err) console.error(err);
    else console.log('File uploaded:', data);
});
```

### AWS SDK (Go)

```go
package main

import (
    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/credentials"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/s3"
)

func main() {
    sess := session.Must(session.NewSession(&aws.Config{
        Region:      aws.String("us-east-1"),
        Endpoint:    aws.String("http://localhost:5222"),
        Credentials: credentials.NewStaticCredentials(
            "demo-access-key",
            "demo-secret-key",
            "",
        ),
        S3ForcePathStyle: aws.Bool(true),
    }))

    svc := s3.New(sess)

    // Create bucket
    _, err := svc.CreateBucket(&s3.CreateBucketInput{
        Bucket: aws.String("my-bucket"),
    })
    if err != nil {
        panic(err)
    }

    // List buckets
    result, err := svc.ListBuckets(nil)
    if err != nil {
        panic(err)
    }

    for _, b := range result.Buckets {
        fmt.Printf("* %s\n", aws.StringValue(b.Name))
    }
}
```

## Production Deployment

For production use:

1. **Use strong credentials**: Generate secure access/secret key pairs
2. **Enable TLS**: Put NATS-S3 behind a reverse proxy (nginx, traefik) with TLS
3. **Configure NATS clustering**: Use NATS cluster for high availability
4. **Monitor with Prometheus**: NATS-S3 exposes metrics at `/metrics`
5. **Set resource limits**: Configure appropriate memory/CPU limits
6. **Use persistent storage**: Configure NATS JetStream with file storage

See the main [README.md](../README.md) for detailed configuration options.

## Troubleshooting

### Connection Refused
```bash
# Check if services are running
docker-compose ps

# Check logs
docker-compose logs nats-s3
```

### Authentication Errors
```bash
# Verify credentials match
cat examples/credentials.json
echo $AWS_ACCESS_KEY_ID
echo $AWS_SECRET_ACCESS_KEY
```

### Bucket/Object Not Found
```bash
# Check NATS JetStream status
docker exec nats-server nats stream ls
docker exec nats-server nats object store ls
```

## Contributing

Have an integration example to share? Please open a pull request!
