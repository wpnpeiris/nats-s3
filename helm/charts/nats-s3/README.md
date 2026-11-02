# nats-s3 Helm chart

`nats-s3` gives applications an S3-compatible API backed by NATS JetStream. This chart runs the gateway in Kubernetes and connects it to a NATS server you already have running.

The chart exposes the S3 API through a Kubernetes Service. It also checks `/healthz` before sending traffic to a pod, so Kubernetes can avoid routing requests to a gateway that is not ready yet.

## Getting started

The gateway needs two things: a NATS server URL and at least one S3 access key and secret key. For a quick local install, pass them as Helm values:

```sh
helm install nats-s3 ./helm/charts/nats-s3 \
  --set nats.servers=nats://nats:4222 \
  --set s3.accessKey=my-access-key \
  --set s3.secretKey=my-secret-key
```

The chart writes those S3 credentials to a Kubernetes Secret and mounts the resulting `credentials.json` file into the gateway. Keep real credentials out of shell history and source control. For a longer lived install, create the Secret yourself and reference it from Helm:

```sh
kubectl create secret generic nats-s3-credentials \
  --from-file=credentials.json=./credentials.json

helm install nats-s3 ./helm/charts/nats-s3 \
  --set nats.servers=nats://nats:4222 \
  --set s3.existingCredentialsSecret=nats-s3-credentials
```

The file follows the same format as the application. It can contain more than one credential pair:

```json
{
  "credentials": [
    {"accessKey": "my-access-key", "secretKey": "my-secret-key"}
  ]
}
```

## Connecting to NATS

Set `nats.servers` to one or more comma-separated server URLs. If your NATS server uses basic authentication, set `nats.username` and `nats.password`. For JWT authentication, create a Kubernetes Secret with a `nats.creds` key and set `nats.existingCredentialsSecret` to its name.

The S3 credentials above are separate from NATS authentication. They control access to the S3 API; NATS credentials let the gateway connect to NATS.

## Values

| Name | Default | Description |
| --- | --- | --- |
| `replicaCount` | `1` | Number of gateway replicas. |
| `image.repository` | `ghcr.io/wpnpeiris/nats-s3` | Container image repository. |
| `image.tag` | `latest` | Container image tag. |
| `service.type` | `ClusterIP` | Kubernetes Service type. |
| `service.port` | `5222` | Service port. |
| `nats.servers` | `nats://nats:4222` | Comma-separated NATS server URLs. |
| `nats.username` | `""` | Optional NATS username. |
| `nats.password` | `""` | Optional NATS password. |
| `nats.existingCredentialsSecret` | `""` | Existing Secret containing the `nats.creds` key. |
| `s3.accessKey` | `""` | S3 access key used when creating the chart-managed Secret. |
| `s3.secretKey` | `""` | S3 secret key used when creating the chart-managed Secret. |
| `s3.existingCredentialsSecret` | `""` | Existing Secret containing `credentials.json`; takes precedence over inline S3 keys. |
| `resources` | `{}` | Container resource requests and limits. |
