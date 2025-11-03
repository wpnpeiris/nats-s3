# NATS-S3 Gateway

[![Build Status](https://github.com/wpnpeiris/nats-s3/actions/workflows/release.yml/badge.svg)](https://github.com/wpnpeiris/nats-s3/actions/workflows/release.yml)
[![Conformance](https://github.com/wpnpeiris/nats-s3/actions/workflows/conformance.yml/badge.svg)](https://github.com/wpnpeiris/nats-s3/actions/workflows/conformance.yml)
[![Coverage](https://github.com/wpnpeiris/nats-s3/actions/workflows/coverage.yml/badge.svg)](https://github.com/wpnpeiris/nats-s3/actions/workflows/coverage.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/wpnpeiris/nats-s3)](https://goreportcard.com/report/github.com/wpnpeiris/nats-s3)
[![License](https://img.shields.io/github/license/wpnpeiris/nats-s3)](https://github.com/wpnpeiris/nats-s3/blob/main/LICENSE)
[![GitHub Stars](https://img.shields.io/github/stars/wpnpeiris/nats-s3?style=social)](https://github.com/wpnpeiris/nats-s3/stargazers)
[![Docker Pulls](https://img.shields.io/docker/pulls/wpnpeiris/nats-s3)](https://hub.docker.com/r/wpnpeiris/nats-s3)

> **S3-compatible object storage powered by NATS JetStream** - Lightweight, fast, and cloud-native.

## About NATS

NATS is a high‑performance distributed messaging system with pub/sub at its core and a
built‑in persistence layer (JetStream) enabling Streaming, Key‑Value, and Object Store.
It includes authentication/authorization, multi‑tenancy, and rich deployment topologies.

```
  +-----------------------+          +-----------------------+
  |       Clients         |          |        Clients        |
  |  (Publish / Subscribe)|          |    (Apps/Services)    |
  +-----------+-----------+          +-----------+-----------+
              |                                   |
              v                                   v
        +-----+-----------------------------------+-----+
        |               NATS Cluster                    |
        |  +-----------+  +-----------+  +-----------+  |
        |  |  Server   |  |  Server   |  |  Server   |  |
        |  +-----------+  +-----------+  +-----------+  |
        +------------------------------------------------+
```

## NATS‑S3
Modern object stores like [MinIO](https://github.com/minio/minio),
[SeaweedFS](https://github.com/seaweedfs/seaweedfs), [JuiceFS](https://github.com/juicedata/juicefs),
and [AIStore](https://github.com/NVIDIA/aistore) expose S3‑compatible HTTP APIs for simple integration.
NATS‑S3 follows this approach to provide S3 access to NATS JetStream Object Store.

```
NATS-S3 Gateway

  +-------------------+      HTTP (S3 API)      +--------------------+
  |  S3 Clients       +------------------------->+    nats-s3         |
  |  (AWS CLI/SDKs)   |                         |  HTTP Gateway      |
  +-------------------+                         +----------+---------+
                                                           |
                                                           |
                                                +--------------------+
                                                |   NATS Cluster     |
                                                |  JetStream Object  |
                                                |      Store         |
                                                +--------------------+
```


## Quick Start
Follow these steps to spin up NATS-S3, integrated with NATS server, and use AWS CLI to work with a bucket and objects.

1) Prerequisites
- Docker (for NATS)
- AWS CLI (v2 recommended)

2) Start NATS (with JetStream) via Docker
```bash
docker run -p 4222:4222 -ti nats:latest -js
```

3) Create a credentials file
```bash
cat > credentials.json <<EOF
{
  "credentials": [
    {
      "accessKey": "my-access-key",
      "secretKey": "my-secret-key"
    }
  ]
}
EOF
```

4) Start the nats-s3 gateway
```bash
# In a separate terminal
./nats-s3 \
  --listen 0.0.0.0:5222 \
  --natsServers nats://127.0.0.1:4222 \
  --s3.credentials credentials.json
```

5) Configure your AWS CLI to use the same credentials
```bash
export AWS_ACCESS_KEY_ID=my-access-key
export AWS_SECRET_ACCESS_KEY=my-secret-key
# SigV4 scope requires a region; us-east-1 is common
export AWS_DEFAULT_REGION=us-east-1
```

6) Create a bucket
```bash
aws s3 mb s3://bucket1 --endpoint-url=http://localhost:5222
```

7) List buckets
```bash
aws s3 ls --endpoint-url=http://localhost:5222
```

8) Put an object
```bash
echo "hello world" > file.txt
aws s3 cp file.txt s3://bucket1/hello.txt --endpoint-url=http://localhost:5222
```

9) List bucket contents
```bash
aws s3 ls s3://bucket1 --endpoint-url=http://localhost:5222
```

10) Download the object
```bash
aws s3 cp s3://bucket1/hello.txt ./hello_copy.txt --endpoint-url=http://localhost:5222
```

11) Delete the object
```bash
aws s3 rm s3://bucket1/hello.txt --endpoint-url=http://localhost:5222
```

Optional: delete the bucket
```bash
aws s3 rb s3://bucket1 --endpoint-url=http://localhost:5222
```

## Build & Run
- Prereqs: Go 1.22+, a running NATS server (with JetStream enabled for Object Store).

Build
```shell
make build
```

Run
```shell
./nats-s3 \
  --listen 0.0.0.0:5222 \
  --natsServers nats://127.0.0.1:4222 \
  --s3.credentials credentials.json
```

Flags
- `--listen`: HTTP bind address for the S3 gateway (default `0.0.0.0:5222`).
- `--natsServers`: Comma‑separated NATS server URLs (default from `nats.DefaultURL`).
- `--natsUser`, `--natsPassword`: Optional NATS credentials for connecting to NATS server (basic auth).
- `--natsToken`: NATS server token for token-based authentication.
- `--natsNKeyFile`: NATS server NKey seed file path for NKey authentication.
- `--natsCredsFile`: NATS server credentials file path for JWT authentication.
- `--natsReplicas`: Number of NATS replicas for each jetstream element (default 1).
- `--s3.credentials`: Path to S3 credentials file (JSON format, required).
- `--log.format`: Log output format: logfmt or json (default logfmt).
- `--log.level`: Log level: debug, info, warn, error (default info).
- `--http.read-timeout`: HTTP server read timeout (default 15m).
- `--http.write-timeout`: HTTP server write timeout (default 15m).
- `--http.idle-timeout`: HTTP server idle timeout (default 120s).
- `--http.read-header-timeout`: HTTP server read header timeout (default 30s).

### Coverage
Generate coverage profile and HTML report locally:
```bash
make coverage           # writes coverage.out
make coverage-report    # prints total coverage summary
make coverage-html      # writes coverage.html
```

In CI, coverage is generated and uploaded as an artifact.

### Releasing (GoReleaser)
Tagged releases are built and published via GoReleaser.

- Create and push a tag like `v0.2.0`:
```bash
git tag -a v0.2.0 -m "v0.2.0"
git push origin v0.2.0
```

CI will build multi‑platform archives and attach them to the GitHub Release.
Local dry run:
```bash
goreleaser release --snapshot --clean
```

## Docker
Build the image
```bash
docker build -t nats-s3:dev .
```

Run with a locally running NATS on the same Docker network
```bash

# Start NATS (JetStream) on the host network
docker run --network host -p 4222:4222 \
  nats:latest -js

# Start nats-s3 and expose port 5222
# Note: Mount credentials.json file into the container
docker run --network host -p 5222:5222 \
  -v $(pwd)/credentials.json:/credentials.json \
  wpnpeiris/nats-s3:latest \
  --listen 0.0.0.0:5222 \
  --natsServers nats://127.0.0.1:4222 \
  --s3.credentials /credentials.json
```

Test with AWS CLI
```bash
export AWS_ACCESS_KEY_ID=my-access-key
export AWS_SECRET_ACCESS_KEY=my-secret-key
export AWS_DEFAULT_REGION=us-east-1

aws s3 ls --endpoint-url=http://localhost:5222
```

### Use prebuilt image from GHCR
Pull and run the published container image from GitHub Container Registry.

Set a version tag (example: v0.0.2)
```bash
IMAGE_TAG=v0.0.2
docker pull ghcr.io/wpnpeiris/nats-s3:${IMAGE_TAG}
```

Run against a host‑running NATS (portable across OSes)
```bash
# Start NATS locally (JetStream enabled)
docker run --network host -p 4222:4222 \
  nats:latest -js

# Start nats-s3 and point to the host via host-gateway
docker run --network host -p 5222:5222 \
  -v $(pwd)/credentials.json:/credentials.json \
  ghcr.io/wpnpeiris/nats-s3:${IMAGE_TAG} \
  --listen 0.0.0.0:5222 \
  --natsServers nats://127.0.0.1:4222 \
  --s3.credentials /credentials.json
```

Test with AWS CLI
```bash
export AWS_ACCESS_KEY_ID=my-access-key
export AWS_SECRET_ACCESS_KEY=my-secret-key
export AWS_DEFAULT_REGION=us-east-1

aws s3 ls --endpoint-url=http://localhost:5222
```

## Authentication
nats-s3 uses AWS Signature Version 4 (SigV4) for S3 API authentication. The gateway
loads credentials from a JSON file and supports multiple users.

### Credential Store

The credential store is a JSON file containing one or more AWS-style access/secret key pairs:

```json
{
  "credentials": [
    {
      "accessKey": "AKIAIOSFODNN7EXAMPLE",
      "secretKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    },
    {
      "accessKey": "AKIAI44QH8DHBEXAMPLE",
      "secretKey": "je7MtGbClwBF/2Zp9Utk/h3yCo8nvbEXAMPLEKEY"
    }
  ]
}
```

Requirements:
- Access keys must be at least 3 characters
- Secret keys must be at least 8 characters
- Access keys cannot contain reserved characters (`=` or `,`)
- Each access key must be unique

See `credentials.example.json` for a complete example.

### How it works
1. Start the gateway with a credentials file:
```shell
./nats-s3 \
  --listen 0.0.0.0:5222 \
  --natsServers nats://127.0.0.1:4222 \
  --s3.credentials credentials.json
```

2. Clients sign S3 requests using SigV4 with any valid credential from the file:
```shell
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export AWS_DEFAULT_REGION=us-east-1

aws s3 ls --endpoint-url=http://localhost:5222
```

3. The gateway verifies the SigV4 signature and, on success, processes the request.

### Notes
- Both header-based SigV4 and presigned URLs (query-string SigV4) are supported.
- Time skew of ±5 minutes is allowed; presigned URLs honor X-Amz-Expires.
- Multiple users can share the same gateway, each with their own credentials.
- NATS server authentication (`--natsUser`/`--natsPassword`) is independent from S3 credentials.

## Roadmap & Contributing
- See [ROADMAP.md](ROADMAP.md) for planned milestones.
- Contributions welcome! See CONTRIBUTING.md and CODE_OF_CONDUCT.md.


## Contributing

- See [CONTRIBUTING.md](CONTRIBUTING.md) for how to get started.

- Please follow our simple [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).

## Conformance

NATS-S3 achieves **100% pass rate** on core S3 API operations with comprehensive conformance testing. See [CONFORMANCE.md](CONFORMANCE.md) for:
- Full test coverage details (25+ tests)
- S3 API feature matrix
- Instructions for running tests locally
