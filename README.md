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


## Usage
Once `nats-s3` is running and connected to a NATS cluster, the NATS Object Store is
accessible via the S3 API. Examples below use the AWS CLI.

#### List NATS Buckets
```shell
$ aws s3 ls  --endpoint-url=http://localhost:5222
```

#### List content of NATS Bucket, bucket1
```shell
$ aws s3 ls s3://bucket1 --endpoint-url=http://localhost:5222
```

#### List content of NATS Bucket, bucket1
```shell
$ aws s3 ls s3://bucket1 --endpoint-url=http://localhost:5222
```

#### Upload an object to a NATS bucket
```shell
$ aws s3 cp file1.txt s3://bucket1 --endpoint-url=http://localhost:5222
```

#### Download an object from a NATS bucket
```shell
$ aws s3 cp s3://bucket1/file1.txt file1_copy.txt --endpoint-url=http://localhost:5222
```

## Build & Run
- Prereqs: Go 1.22+, a running NATS server (with JetStream enabled for Object Store).

Build
```shell
go build ./cmd/nats-s3
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

## Notes
- This gateway focuses on S3 object basics (list/get/head/put/delete). Many S3
  sub‑resources return 501 Not Implemented.
- Object keys with slashes are supported.

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
- See ROADMAP.md for planned milestones.
- Contributions welcome! See CONTRIBUTING.md and CODE_OF_CONDUCT.md.


## Contributing

- See `CONTRIBUTING.md` for how to get started.

- Please follow our simple `CODE_OF_CONDUCT.md`.
