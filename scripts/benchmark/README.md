# NATS-S3 Benchmark Script

This directory contains a benchmark script to test the performance of the NATS-S3 gateway using MinIO's `warp` benchmarking tool.

## Prerequisites

Before running the benchmark script, ensure you have the following installed:

1. **Docker** - Required to run the NATS-S3 gateway container
2. **NATS Server** - The `nats-server` binary must be available in your system PATH
3. **MinIO Warp** - Install from https://github.com/minio/warp

## Configuration

The script requires a `credentials.json` file in the same directory. A sample file is already provided with the following structure:

```json
{
  "credentials": [
    {
      "accessKey": "testtest",
      "secretKey": "testtest"
    }
  ]
}
```

**Important**: The access key and secret key must both be set to `testtest` for the benchmark to work correctly.

## Running the Benchmark

1. Navigate to the benchmark directory:
   ```bash
   cd scripts/benchmark
   ```

2. Make the script executable (if not already):
   ```bash
   chmod +x benchmark.sh
   ```

3. Run the benchmark:
   ```bash
   ./benchmark.sh
   ```

## What the Script Does

The benchmark script performs the following steps:

1. **Starts NATS Server** - Launches a local NATS server with JetStream enabled
2. **Starts NATS-S3 Gateway** - Runs the NATS-S3 gateway in a Docker container, connected to the NATS server
3. **Runs Benchmark** - Executes MinIO's `warp` tool with a mixed workload:
   - Duration: 20 seconds
   - Object size: 1MB
   - Concurrent operations: 1
   - Number of objects: 10
   - Operation distribution:
     - GET: 1x
     - STAT: 1x
     - PUT: 2x
     - DELETE: 1x

4. **Cleanup** - Automatically stops and removes all containers and cleans up JetStream data, even if the script is interrupted

## Customizing the Benchmark

You can modify the benchmark parameters by editing the `warp` command in the script:

```bash
warp mixed --host=localhost:5222 \
    --access-key=testtest \
    --secret-key=testtest \
    --duration 20s \        # Test duration
    --obj.size 1M \         # Size of test objects
    --concurrent=1 \        # Number of concurrent operations
    --objects=10 \          # Number of objects to use
    --get-distrib 1 \       # GET operation weight
    --stat-distrib 1 \      # STAT operation weight
    --put-distrib 2 \       # PUT operation weight
    --delete-distrib 1      # DELETE operation weight
```

## Troubleshooting

### Port Already in Use

If you get an error about ports already being in use, ensure no other NATS server or S3-compatible service is running on:
- Port 4222 (NATS)
- Port 5222 (NATS-S3 Gateway)

### Docker Permission Issues

If you encounter Docker permission issues, ensure your user is in the `docker` group:
```bash
sudo usermod -aG docker $USER
# Log out and back in for changes to take effect
```

### Cleanup Not Working

If the cleanup doesn't work properly, you can manually clean up:
```bash
# Stop all containers
docker stop $(docker ps -a -q --filter ancestor=wpnpeiris/nats-s3:v0.3.8)
docker rm $(docker ps -a -q --filter ancestor=wpnpeiris/nats-s3:v0.3.8)

# Kill any NATS server processes
pkill nats-server

# Remove JetStream data
rm -rf /tmp/nats/jetstream
```

## Output

The benchmark will display real-time statistics including:
- Operations per second
- Throughput (MB/s)
- Latency percentiles (min, avg, max, p50, p90, p99)
- Error rates

A summary report will be displayed at the end of the benchmark run.
