# Roadmap

_Last updated: 2025-10-18_

## Vision & Goals

nats-s3 is a gateway to allow tools, SDKs, and CLI utilities that expect an S3-compatible interface to operate on 
NATS JetStream Object Store. 
It aims to provide compatibility, security (authentication/signature) with reasonable performance to use familiar 
S3 workflows with NATS Object Store.


## Current Status

| Feature | Status | Notes                                                                            |
|---|---|----------------------------------------------------------------------------------|
| Basic S3 operations (list buckets, list objects, put, get, delete object) | ✅ Implemented | Works with AWS CLI.                                                              |
| SigV4 authentication (header & presigned URLs) | ✅ Implemented | Multi-user credential store with JSON file-based configuration. |
| Multipart uploads (initiate/upload part/list parts/complete/abort) | ✅ Implemented | Follows S3 semantics incl. ETag and part pagination. |
| Basic monitoring endpoints (/healthz, /metrics, /stats) | ✅ Implemented | Prometheus text metrics and JSON stats. |
| Credential store | ✅ Implemented | JSON file-based store supporting multiple AWS-style access/secret key pairs. |


## Milestones & Phases

Below are planned features grouped in phases. Priorities may changed based on user feedback or community contributions.

## v0.1 - MVP
- ✅ S3 basics: ListBuckets, ListObjects, GetObject, HeadObject, PutObject, DeleteObject
- ✅ S3-compatible headers (ETag, Last-Modified RFC1123, Content-Length)
- ✅ Configurable auth (NATS creds)
- ✅ Docker image and simple CI

## v0.2 – Multipart support and other improvements (Completed)
- ✅ Multipart upload support (initiate, upload part, list parts, complete, abort)
- ✅ Improve listing behavior for multipart parts (pagination, markers)
- ✅ Add basic metrics / endpoints for monitoring (/healthz, /metrics, /stats)

## v0.3 – Credential Store and robustness (Completed)
- ✅ Credential store for auth (first-class)
  - ✅ JSON file-based credential store supporting multiple users
  - ✅ AWS-style access/secret key pairs with validation
  - ✅ SigV4 verification against credential store
  - ✅ Thread-safe implementation with proper error handling
  - ✅ Mandatory `--s3.credentials` flag for enhanced security

## v0.4 – CI improvements and Compatibility
- CI improvements
  - Integration tests that spin up NATS + nats-s3 + AWS CLI/SDK flows
  - Auth paths: header/presigned, allowed/denied policy cases
  - Support key rotation (overlapping secrets)
- Add more examples / sample code for SDKs (Go, Python etc.)
- Helm chart and K8s manifests
- Detailed metrics & dashboards (Prometheus, Grafana)
- Investigate non-S3 API compatibility / S3 API newer features (e.g. AWS S3 Select, event notifications)

## v1.0 – Production Ready
- Formal release versioning (v1.0.0)
- Comprehensive documentation and deployment guides
- Performance benchmarks and optimization
- Production deployment best practices
