package testutil

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	srvlog "github.com/nats-io/nats-server/v2/logger"
	"github.com/nats-io/nats-server/v2/server"
	nservertest "github.com/nats-io/nats-server/v2/test"
)

// StartJSServer starts an in-process NATS Server with JetStream enabled
// and a per-test StoreDir to avoid cross-test persistence.
func StartJSServer(t *testing.T) *server.Server {
	t.Helper()
	opts := nservertest.DefaultTestOptions
	opts.Port = server.RANDOM_PORT
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	s := nservertest.RunServer(&opts)
	return s
}

// StartJSServerCluster starts 3 in-process NATS Servers with JetStream enabled
// and a per-test StoreDir to avoid cross-test persistence.
func StartJSServerCluster(t *testing.T) []*server.Server {
	t.Helper()
	clusterPorts := []int{16222, 17222, 18222} // fixed ports for cluster nodes, please ensure they are free

	servers := make([]*server.Server, 3)
	for i := 0; i < 3; i++ {
		opts := nservertest.DefaultTestOptions
		opts.ServerName = fmt.Sprintf("S-%d", i+1)
		opts.Port = server.RANDOM_PORT
		opts.JetStream = true
		opts.StoreDir = filepath.Join(t.TempDir(), fmt.Sprintf("%d", i))
		opts.Cluster.Name = "test-cluster"
		opts.Cluster.Host = "127.0.0.1"
		opts.Cluster.Port = clusterPorts[i]

		opts.Routes = server.RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", clusterPorts[0]))
		s := nservertest.RunServerCallback(&opts, func(s *server.Server) {
			s.SetLogger(srvlog.NewStdLogger(opts.Logtime, opts.Debug, opts.Trace, false, true, srvlog.LogUTC(opts.LogtimeUTC)), false, false)
		})
		t.Logf("Started server %d\n", i)
		servers[i] = s
	}
	now := time.Now()
	t.Logf("Waiting for cluster to be ready\n")
	// Wait for cluster to form
	for {
		time.Sleep(200 * time.Millisecond)
		for _, s := range servers {
			if s.JetStreamIsLeader() {
				t.Logf("Cluster is ready in %s\n", time.Since(now))
				return servers
			}
		}
	}
}
