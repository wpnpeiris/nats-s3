package client

import (
	"bytes"
	"errors"
	"github.com/nats-io/nats.go"
	"log"
)

var ErrBucketNotFound = errors.New("bucket not found")
var ErrObjectNotFound = errors.New("object not found")

// NatsObjectClient provides convenience helpers for common NATS JetStream
// Object Store operations, built on top of the base Client connection.
type NatsObjectClient struct {
	Client *Client
}

func (c *NatsObjectClient) CreateBucket(bucketName string) (nats.ObjectStoreStatus, error) {
	nc := c.Client.NATS()
	js, err := nc.JetStream()
	if err != nil {
		log.Printf("Error at CreateBucket when nc.JetStream(): %v\n", err)
		return nil, err
	}

	os, err := js.CreateObjectStore(&nats.ObjectStoreConfig{
		Bucket:  bucketName,
		Storage: nats.FileStorage,
	})
	if err != nil {
		log.Printf("Error at CreateBucket when js.CreateObjectStore: %v\n", err)
		return nil, err
	}

	return os.Status()
}

// DeleteBucket delete an bucket identified by bucket name.
func (c *NatsObjectClient) DeleteBucket(bucket string) error {
	nc := c.Client.NATS()
	js, err := nc.JetStream()
	if err != nil {
		log.Printf("Error at DeleteBucket when nc.JetStream(): %v\n", err)
		return err
	}
	err = js.DeleteObjectStore(bucket)
	if err != nil {
		log.Printf("Error at DeleteBucket when js.DeleteObjectStore(): %v\n", err)
		if errors.Is(err, nats.ErrStreamNotFound) {
			return ErrBucketNotFound
		}
		return err
	}
	return nil
}

// DeleteObject removes an object identified by bucket and key.
func (c *NatsObjectClient) DeleteObject(bucket string, key string) error {
	nc := c.Client.NATS()
	js, err := nc.JetStream()
	if err != nil {
		log.Printf("Error at DeleteObject when nc.JetStream(): %v\n", err)
		return err
	}
	os, err := js.ObjectStore(bucket)
	if err != nil {
		log.Printf("Error at DeleteObject when js.ObjectStore: %v\n", err)
		if errors.Is(err, nats.ErrStreamNotFound) {
			return ErrBucketNotFound
		}
		return err
	}
	err = os.Delete(key)
	if err != nil {
		log.Printf("Error at DeleteObject when os.Delete: %v\n", err)
		if errors.Is(err, nats.ErrObjectNotFound) {
			return ErrObjectNotFound
		}
		return err
	}

	return nil
}

// GetObjectInfo fetches metadata for an object.
func (c *NatsObjectClient) GetObjectInfo(bucket string, key string) (*nats.ObjectInfo, error) {
	nc := c.Client.NATS()
	js, err := nc.JetStream()
	if err != nil {
		log.Printf("Error at GetObjectInfo when js.JetStream: %v\n", err)
		return nil, err
	}
	os, err := js.ObjectStore(bucket)
	if err != nil {
		log.Printf("Error at GetObjectInfo when js.ObjectStore: %v\n", err)
		if errors.Is(err, nats.ErrStreamNotFound) {
			return nil, ErrBucketNotFound
		}
		return nil, err
	}
	obj, err := os.GetInfo(key)
	if err != nil {
		log.Printf("Error at GetObjectInfo when os.GetInfo: %v\n", err)
		if errors.Is(err, nats.ErrObjectNotFound) {
			return nil, ErrObjectNotFound
		}
		return nil, err
	}

	return obj, err
}

// GetObject retrieves an object and its metadata.
func (c *NatsObjectClient) GetObject(bucket string, key string) (*nats.ObjectInfo, []byte, error) {
	nc := c.Client.NATS()
	js, err := nc.JetStream()
	if err != nil {
		log.Printf("Error at GetObjectInfo when js.JetStream: %v\n", err)
		return nil, nil, err
	}
	os, err := js.ObjectStore(bucket)
	if err != nil {
		log.Printf("Error at GetObjectInfo when js.ObjectStore: %v\n", err)
		if errors.Is(err, nats.ErrStreamNotFound) {
			return nil, nil, ErrBucketNotFound
		}
		return nil, nil, err
	}
	info, err := os.GetInfo(key)
	if err != nil {
		log.Printf("Error at GetObjectInfo when os.GetInfo: %v\n", err)
		if errors.Is(err, nats.ErrObjectNotFound) {
			return nil, nil, ErrObjectNotFound
		}
		return nil, nil, err
	}
	res, err := os.GetBytes(key)
	if err != nil {
		log.Printf("Error at GetObjectInfo when os.GetBytes: %v\n", err)
		return nil, nil, err
	}
	return info, res, nil
}

// ListBuckets returns a channel of object store statuses for all buckets.
func (c *NatsObjectClient) ListBuckets() (<-chan nats.ObjectStoreStatus, error) {
	nc := c.Client.NATS()
	js, err := nc.JetStream()
	if err != nil {
		log.Printf("Error at ListBuckets when js.JetStream: %v\n", err)
		return nil, err
	}
	return js.ObjectStores(), nil
}

// ListObjects lists all objects in the given bucket.
func (c *NatsObjectClient) ListObjects(bucket string) ([]*nats.ObjectInfo, error) {
	nc := c.Client.NATS()
	js, err := nc.JetStream()
	if err != nil {
		log.Printf("Error at ListObjects when js.JetStream: %v\n", err)
		return nil, err
	}
	os, err := js.ObjectStore(bucket)
	if err != nil {
		log.Printf("Error at ListObjects when js.ObjectStore: %v\n", err)
		if errors.Is(err, nats.ErrStreamNotFound) {
			return nil, ErrBucketNotFound
		}
		return nil, err
	}
	ls, err := os.List()
	if err != nil {
		log.Printf("Error at ListObjects when js.List: %v\n", err)
		if errors.Is(err, nats.ErrNoObjectsFound) {
			return nil, ErrObjectNotFound
		}
		return nil, err
	}
	return ls, err
}

// PutObject writes an object to the given bucket with the provided key and metadata
func (c *NatsObjectClient) PutObject(bucket string,
	key string,
	contentType string,
	metadata map[string]string,
	data []byte) (*nats.ObjectInfo, error) {
	nc := c.Client.NATS()
	js, err := nc.JetStream()
	if err != nil {
		log.Printf("Error at PutObject when js.JetStream: %v\n", err)
		return nil, err
	}
	os, err := js.ObjectStore(bucket)
	if err != nil {
		log.Printf("Error at PutObject when js.ObjectStore: %v\n", err)
		if errors.Is(err, nats.ErrStreamNotFound) {
			return nil, ErrBucketNotFound
		}
		return nil, err
	}

	meta := nats.ObjectMeta{
		Name:     key,
		Metadata: metadata,
		Headers: nats.Header{
			"Content-Type": []string{contentType},
		},
	}

	return os.Put(&meta, bytes.NewReader(data))
}
