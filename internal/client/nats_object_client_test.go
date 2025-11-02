package client

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/wpnpeiris/nats-s3/internal/logging"
	"github.com/wpnpeiris/nats-s3/internal/testutil"
)

func TestNatsObjectClient_BasicCRUD(t *testing.T) {
	s := testutil.StartJSServer(t)
	defer s.Shutdown()

	url := s.ClientURL()

	c := NewClient(context.Background(), "object-test")
	if err := c.SetupConnectionToNATS(url); err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	nc := c.NATS()
	// Avoid panic-on-close during tests.
	nc.SetClosedHandler(func(_ *nats.Conn) {})
	defer nc.Close()

	js := c.JetStream()

	bucket := "testbucket"
	key := "path/to/object.txt"
	data := []byte("hello world")

	// Create the bucket for object store operations.
	if _, err := js.CreateObjectStore(c.ctx, jetstream.ObjectStoreConfig{Bucket: bucket}); err != nil {
		t.Fatalf("create object store failed: %v", err)
	}

	logger := logging.NewLogger(logging.Config{Level: "debug"})
	oc, err := NewNatsObjectClient(context.Background(), logger, c, NatsObjectClientOptions{})
	if err != nil {
		t.Fatalf("NewNatsObjectClient failed: %v", err)
	}

	// Put
	info, err := oc.PutObject(c.ctx, bucket, key, "text/plain", map[string]string{"k": "v"}, data)
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}
	if info == nil ||
		info.Name != key ||
		info.Headers.Get("Content-Type") != "text/plain" ||
		info.Metadata["k"] != "v" {
		t.Fatalf("unexpected PutObject info: %+v", info)
	}

	// GetInfo
	gi, err := oc.GetObjectInfo(c.ctx, bucket, key)
	if err != nil {
		t.Fatalf("GetObjectInfo failed: %v", err)
	}
	if gi == nil || gi.Size != uint64(len(data)) {
		t.Fatalf("unexpected GetObjectInfo: %+v", gi)
	}

	// Get
	gotInfo, gotData, err := oc.GetObject(c.ctx, bucket, key)
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	if gotInfo == nil || !bytes.Equal(gotData, data) {
		t.Fatalf("unexpected GetObject: info=%+v data=%q", gotInfo, string(gotData))
	}

	// ListObjects should include our key
	list, err := oc.ListObjects(c.ctx, bucket)
	if err != nil {
		t.Fatalf("ListObjects failed: %v", err)
	}
	found := false
	for _, o := range list {
		if o.Name == key {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("ListObjects did not contain key %q", key)
	}

	// ListBuckets channel should yield our bucket
	ch, err := oc.ListBuckets(c.ctx)
	if err != nil {
		t.Fatalf("ListBuckets failed: %v", err)
	}
	found = false
	timeout := time.After(2 * time.Second)
	for {
		select {
		case st, ok := <-ch:
			if !ok {
				if !found {
					t.Fatalf("bucket %q not found in ListBuckets", bucket)
				}
				goto delete
			}
			if st.Bucket() == bucket {
				found = true
			}
		case <-timeout:
			t.Fatalf("timeout waiting for ListBuckets")
		}
	}

delete:
	// Delete
	if err := oc.DeleteObject(c.ctx, bucket, key); err != nil {
		t.Fatalf("DeleteObject failed: %v", err)
	}

	// Verify deletion by attempting GetObjectInfo and expecting error
	if _, err := oc.GetObjectInfo(c.ctx, bucket, key); err == nil {
		t.Fatalf("expected error getting deleted object info")
	}
}
