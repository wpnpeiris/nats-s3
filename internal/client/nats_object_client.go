package client

import (
	"github.com/nats-io/nats.go"
)

// NatsObjectClient provides convenience helpers for common NATS JetStream
// Object Store operations, built on top of the base Client connection.
type NatsObjectClient struct {
	Client *Client
}

func (c *NatsObjectClient) CreateBucket(bucketName string) (nats.ObjectStoreStatus, error) {
	nc := c.Client.NATS()
	js, err := nc.JetStream()
	if err != nil {
		return nil, err
	}

	os, err := js.CreateObjectStore(&nats.ObjectStoreConfig{
		Bucket:  bucketName,
		Storage: nats.FileStorage,
	})
	if err != nil {
		return nil, err
	}

	return os.Status()
}

// DeleteObject removes an object identified by bucket and key.
func (c *NatsObjectClient) DeleteObject(bucket string, key string) error {
	nc := c.Client.NATS()
	js, err := nc.JetStream()
	if err != nil {
		return err
	}
	os, err := js.ObjectStore(bucket)
	if err != nil {
		return err
	}
	return os.Delete(key)
}

// GetObjectInfo fetches metadata for an object.
func (c *NatsObjectClient) GetObjectInfo(bucket string, key string) (*nats.ObjectInfo, error) {
	nc := c.Client.NATS()
	js, err := nc.JetStream()
	if err != nil {
		return nil, err
	}
	os, err := js.ObjectStore(bucket)
	if err != nil {
		return nil, err
	}
	return os.GetInfo(key)
}

// GetObject retrieves an object and its metadata.
func (c *NatsObjectClient) GetObject(bucket string, key string) (*nats.ObjectInfo, []byte, error) {
	nc := c.Client.NATS()
	js, err := nc.JetStream()
	if err != nil {
		return nil, nil, err
	}
	os, err := js.ObjectStore(bucket)
	if err != nil {
		return nil, nil, err
	}
	info, err := os.GetInfo(key)
	if err != nil {
		return nil, nil, err
	}
	res, err := os.GetBytes(key)
	if err != nil {
		return nil, nil, err
	}
	return info, res, nil
}

// ListBuckets returns a channel of object store statuses for all buckets.
func (c *NatsObjectClient) ListBuckets() (<-chan nats.ObjectStoreStatus, error) {
	nc := c.Client.NATS()
	js, err := nc.JetStream()
	if err != nil {
		return nil, err
	}
	return js.ObjectStores(), nil
}

// ListObjects lists all objects in the given bucket.
func (c *NatsObjectClient) ListObjects(bucket string) ([]*nats.ObjectInfo, error) {
	nc := c.Client.NATS()
	js, err := nc.JetStream()
	if err != nil {
		return nil, err
	}
	os, err := js.ObjectStore(bucket)
	if err != nil {
		return nil, err
	}
	return os.List()
}

// PutObject writes an object to the given bucket with the provided key.
func (c *NatsObjectClient) PutObject(bucket string, key string, data []byte) (*nats.ObjectInfo, error) {
	nc := c.Client.NATS()
	js, err := nc.JetStream()
	if err != nil {
		return nil, err
	}
	os, err := js.ObjectStore(bucket)
	if err != nil {
		return nil, err
	}
	return os.PutBytes(key, data)
}
