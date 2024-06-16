package client

import (
	"fmt"
	"log"
	"sync"

	"github.com/nats-io/nats.go"

	"github.com/nats-io/nuid"
)

type Client struct {
	cmu sync.Mutex

	id string

	nc *nats.Conn

	kind string
}

func NewClient(kind string) *Client {
	id := nuid.Next()
	return &Client{
		id:   id,
		kind: kind,
	}
}

func (c *Client) SetupConnectionToNATS(servers string, options ...nats.Option) error {

	options = append(options, nats.Name(c.Name()))

	c.cmu.Lock()
	defer c.cmu.Unlock()

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
		panic("Connection to NATS is closed!")
	})

	return err
}

func (c *Client) NATS() *nats.Conn {
	c.cmu.Lock()
	defer c.cmu.Unlock()
	return c.nc
}

func (c *Client) ID() string {
	c.cmu.Lock()
	defer c.cmu.Unlock()
	return c.id
}

func (c *Client) Name() string {
	c.cmu.Lock()
	defer c.cmu.Unlock()
	return fmt.Sprintf("%s:%s", c.kind, c.id)
}
