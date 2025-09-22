# NATS
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
docker run -p 4222:4222 -ti nats:latest -js --user my-access-key --pass my-secret-key
```

3) Start the nats-s3 gateway
- Choose a single access/secret pair. These map to NATS username/password and AWS access/secret.
```bash
# In a separate terminal
./nats-s3 \
  --listen 0.0.0.0:5222 \
  --natsServers nats://127.0.0.1:4222 \
  --natsUser my-access-key \
  --natsPassword my-secret-key
```

4) Configure your AWS CLI to use the same credentials
```bash
export AWS_ACCESS_KEY_ID=my-access-key
export AWS_SECRET_ACCESS_KEY=my-secret-key
# SigV4 scope requires a region; us-east-1 is common
export AWS_DEFAULT_REGION=us-east-1
```

5) Create a bucket
```bash
aws s3 mb s3://bucket1 --endpoint-url=http://localhost:5222
```

6) List buckets
```bash
aws s3 ls --endpoint-url=http://localhost:5222
```

7) Put an object
```bash
echo "hello world" > file.txt
aws s3 cp file.txt s3://bucket1/hello.txt --endpoint-url=http://localhost:5222
```

8) List bucket contents
```bash
aws s3 ls s3://bucket1 --endpoint-url=http://localhost:5222
```

9) Download the object
```bash
aws s3 cp s3://bucket1/hello.txt ./hello_copy.txt --endpoint-url=http://localhost:5222
```

10) Delete the object
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
  --natsUser "" \
  --natsPassword ""
```

Flags
- `--listen`: HTTP bind address for the S3 gateway (default `0.0.0.0:5222`).
- `--natsServers`: Comma‑separated NATS server URLs (default from `nats.DefaultURL`).
- `--natsUser`, `--natsPassword`: Optional NATS credentials.

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
  nats:latest -js --user my-access-key --pass my-secret-key

# Start nats-s3 and expose port 5222
docker run --network host -p 5222:5222 nats-s3:latest \
  --listen 0.0.0.0:5222 \
  --natsServers nats://127.0.0.1:4222 \
  --natsUser my-access-key \
  --natsPassword my-secret-key
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
# Start NATS locally (JetStream enabled) with credentials
docker run --network host -p 4222:4222 \
  nats:latest -js --user my-access-key --pass my-secret-key

# Start nats-s3 and point to the host via host-gateway
docker run --network host -p 5222:5222 ghcr.io/wpnpeiris/nats-s3:${IMAGE_TAG} \
  --listen 0.0.0.0:5222 \
  --natsServers nats://127.0.0.1:4222 \
  --natsUser my-access-key \
  --natsPassword my-secret-key
```

Test with AWS CLI
```bash
export AWS_ACCESS_KEY_ID=my-access-key
export AWS_SECRET_ACCESS_KEY=my-secret-key
export AWS_DEFAULT_REGION=us-east-1

aws s3 ls --endpoint-url=http://localhost:5222
```

## Authentication
nats-s3 uses AWS Signature Version 4 (SigV4) for every S3 request. The gateway is
configured with a single NATS username/password pair, and that same pair is used as
the AWS Access Key ID and Secret Access Key for S3 authentication.

How it works
- Start the gateway with NATS credentials:
  - `--natsUser` is treated as the AWS Access Key ID.
  - `--natsPassword` is treated as the AWS Secret Access Key.
- Clients sign S3 requests using SigV4 with those credentials.
- The gateway verifies the SigV4 signature and, on success, processes the request.

Server example
```shell
./nats-s3 \
  --listen 0.0.0.0:5222 \
  --natsServers nats://127.0.0.1:4222 \
  --natsUser my-access-key \
  --natsPassword my-secret-key
```

Client example (AWS CLI)
```shell
export AWS_ACCESS_KEY_ID=my-access-key
export AWS_SECRET_ACCESS_KEY=my-secret-key

# Pick any region (SigV4 requires a region in the scope; us-east-1 is common)
aws s3 ls --endpoint-url=http://localhost:5222 --region us-east-1
```

Notes
- Both header-based SigV4 and presigned URLs (query-string SigV4) are supported.
- Time skew of ±5 minutes is allowed; presigned URLs honor X-Amz-Expires.
- Only a single credential pair is supported at this time; requests must use the
  same AccessKey/Secret configured on the gateway.

## Roadmap & Contributing
- See [ROADMAP.md](ROADMAP.md) for planned milestones.
- Contributions welcome! See CONTRIBUTING.md and CODE_OF_CONDUCT.md.


## Contributing

- See [CONTRIBUTING.md](CONTRIBUTING.md) for how to get started.

- Please follow our simple [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).

## Conformance
- CI runs a curated subset of S3 compatibility tests using the AWS CLI against the gateway. See details and the pass list: [CONFORMANCE.md](CONFORMANCE.md).
- Workflow status: [![Conformance](https://github.com/wpnpeiris/nats-s3/actions/workflows/conformance.yml/badge.svg)](https://github.com/wpnpeiris/nats-s3/actions/workflows/conformance.yml)
