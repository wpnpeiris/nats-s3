Conformance

This project runs a curated AWS CLI–based S3 compatibility subset in CI against the nats-s3 gateway.

How tests run
- The GitHub Actions workflow starts a NATS server (JetStream), builds the gateway, and runs the subset against `http://localhost:5222` using a single access/secret pair.
- The curated subset fails fast on the first mismatch to provide a quick signal.

Curated subset
- Bucket create: `aws s3 mb s3://<bucket>`
- Service list includes bucket: `aws s3 ls`
- Object put: `aws s3 cp <file> s3://<bucket>/hello.txt`
- Bucket list contains object: `aws s3 ls s3://<bucket>`
- Object head: `aws s3api head-object --bucket <bucket> --key hello.txt`
- Object get + content match: `aws s3 cp ...` + `diff`
- Object delete: `aws s3 rm s3://<bucket>/hello.txt`
- Bucket delete: `aws s3 rb s3://<bucket>`

Artifacts & logs
- On failures, the workflow dumps `gateway.log` to aid investigation.
