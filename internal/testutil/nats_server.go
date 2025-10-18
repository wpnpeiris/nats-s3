package testutil

import (
	"testing"

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
