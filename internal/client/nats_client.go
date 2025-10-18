package client

import (
	"fmt"
	"log"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

const (
	MultiPartSessionStoreName = "multi_part_session"
	MultiPartTempStoreName    = "multi_part_temp"
)

// Client wraps a NATS connection and metadata used by gateway components.
type Client struct {
	id   string
	kind string
	nc   *nats.Conn
}

// NewClient creates a new Client with a generated ID and the provided kind
// used in connection names and logs.
func NewClient(kind string) *Client {
	id := nuid.Next()
	return &Client{
		id:   id,
		kind: kind,
	}
}

// SetupConnectionToNATS establishes a connection to the given NATS servers
// applying the provided options and sets standard event handlers.
func (c *Client) SetupConnectionToNATS(servers string, options ...nats.Option) error {

	options = append(options, nats.Name(c.Name()))

	nc, err := nats.Connect(servers, options...)
	if err != nil {
		return err
	}
	c.nc = nc

	nc.SetErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
		log.Printf("NATS error: %s\n", err)
	})
	nc.SetReconnectHandler(func(_ *nats.Conn) {
		log.Println("Reconnected to NATS!")
	})
	nc.SetClosedHandler(func(_ *nats.Conn) {
		log.Fatal("Connection to NATS is closed! Service cannot continue.")
	})

	return err
}

// NATS returns the underlying NATS connection.
func (c *Client) NATS() *nats.Conn {
	return c.nc
}

// ID returns the client's stable unique identifier.
func (c *Client) ID() string {
	return c.id
}

// Name returns a human-readable connection name used for NATS.
func (c *Client) Name() string {
	return fmt.Sprintf("%s:%s", c.kind, c.id)
}
