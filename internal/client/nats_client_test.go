package client

import (
	"context"
	"fmt"
	"testing"

	"github.com/nats-io/nats-server/v2/server"

	nservertest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

// TestClientSetupConnectionToNATS verifies that a Client can connect to a
// locally started NATS Server, and that connection options (Name) are applied.
func TestClientSetupConnectionToNATS(t *testing.T) {
	opts := nservertest.DefaultTestOptions
	opts.Port = server.RANDOM_PORT
	s := nservertest.RunServer(&opts)
	defer s.Shutdown()

	c := NewClient(context.Background(), "unit-test")
	if c.ID() == "" {
		t.Fatalf("expected non-empty client ID")
	}

	if err := c.SetupConnectionToNATS(fmt.Sprintf("nats://127.0.0.1:%d", opts.Port)); err != nil {
		t.Fatalf("SetupConnectionToNATS failed: %v", err)
	}

	nc := c.NATS()
	if nc == nil {
		t.Fatalf("expected non-nil NATS connection")
	}
	if !nc.IsConnected() {
		t.Fatalf("expected NATS connection to be connected")
	}
	if got, want := nc.Opts.Name, c.Name(); got != want {
		t.Errorf("unexpected connection name: got %q, want %q", got, want)
	}

	// Avoid test panics from the client's closed handler by overriding it
	// before closing the connection in this test.
	nc.SetClosedHandler(func(_ *nats.Conn) {})
	nc.Close()
}
