# Roadmap

_Last updated: 2025-09-11_

## Vision & Goals

nats-s3 is a gateway to allow tools, SDKs, and CLI utilities that expect an S3-compatible interface to operate on 
NATS JetStream Object Store. 
It aims to provide compatibility, security (authentication/signature) with reasonable performance to use familiar 
S3 workflows with NATS Object Store.


## Current Status

| Feature | Status | Notes                                                                            |
|---|---|----------------------------------------------------------------------------------|
| Basic S3 operations (list buckets, list objects, put, get, delete object) | ✅ Implemented | Works with AWS CLI.                                                              |
| SigV4 authentication (header & presigned URLs) | ✅ Implemented | Single credential pair compaired with NATS server username/password credentials. |


## Milestones & Phases

Below are planned features grouped in phases. Priorities may changed based on user feedback or community contributions.

## v0.1 - MVP
- S3 basics: ListBuckets, ListObjects, GetObject, HeadObject, PutObject, DeleteObject
- S3-compatible headers (ETag, Last-Modified RFC1123, Content-Length)
- Configurable auth (NATS creds)
- Docker image and simple CI

## v0.2 – Multipart support and other improvements
- Improve object listing performance (paging, pagination)
- Add basic metrics / endpoints for monitoring (e.g. HTTP endpoint to reflect health, stats)
- Add more examples / sample code for SDKs (Go, Python etc.)

## v0.3 – Credential Store, Policies, and robustness
- Credential store for auth (first-class)
  - Improved keystore that maps AWS AccessKey → {SigV4 secret, NATS auth spec}
  - SigV4 verification against store; support key rotation (overlapping secrets)
- CI improvements
  - Integration tests that spin up NATS + nats-s3 + AWS CLI/SDK flows
  - Auth paths: header/presigned, allowed/denied policy cases

## v0.4 – Compatibility
- Helm chart and K8s manifests
- Detailed metrics & dashboards (Prometheus, Grafana)
- Investigate non-S3 API compatibility / S3 API newer features (e.g. AWS S3 Select, event notifications)
- Formal release versioning (v1.0.0)
