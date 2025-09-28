package client

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

var ErrBucketNotFound = errors.New("bucket not found")
var ErrObjectNotFound = errors.New("object not found")
var ErrUploadNotFound = errors.New("multipart upload not found")
var ErrUploadCompleted = errors.New("completed multipart upload")
var ErrMissingPart = errors.New("missing part")

// PartMeta describes a single part in a multipart upload session.
// It records the part number, ETag (checksum), size in bytes, and the
// time the part was stored (Unix seconds).
type PartMeta struct {
	Number   int    `json:"number"`
	ETag     string `json:"etag"`
	Size     uint64 `json:"size"`
	StoredAt int64  `json:"stored_at_unix"`
}

// UploadMeta captures the server-side state of a multipart upload session.
// It includes identifiers (UploadID, Bucket, Key), initiation time (UTC),
// optional owner, constraints (minimum part size and max parts), the
// collected parts, and completion/abort flags. The JSON value is persisted
// in a Key-Value store under a session-specific key.
type UploadMeta struct {
	UploadID  string           `json:"upload_id"`
	Bucket    string           `json:"bucket"`
	Key       string           `json:"key"`
	Initiated time.Time        `json:"initiated"`
	Owner     string           `json:"owner,omitempty"` // optional, auth principal
	MinPartSz int64            `json:"min_part_size"`   // default 5MiB
	MaxParts  int              `json:"max_parts"`       // default 10000
	Parts     map[int]PartMeta `json:"parts"`
	Completed bool             `json:"completed"`
	Aborted   bool             `json:"aborted"`
}

// MultiPartStore groups storage backends used for multipart uploads.
// SessionStore tracks session metadata in a Key-Value bucket, while
// TempPartStore holds uploaded parts in a temporary Object Store.
type MultiPartStore struct {
	SessionStore  nats.KeyValue
	TempPartStore nats.ObjectStore
}

// createUploadMeta persists the given session value at the provided key in the
// session Key-Value store. The value is expected to be a JSON-encoded
// UploadMeta blob. Returns any error encountered during the put operation.
func (m *MultiPartStore) createUploadMeta(meta UploadMeta) error {
	log.Printf("createUploadMeta : %v\n", meta)
	data, err := json.Marshal(meta)
	if err != nil {
		log.Printf("Error at createUploadMeta when json.Marshal(): %v\n", err)
		return err
	}
	key := sessionKey(meta.Bucket, meta.Key, meta.UploadID)
	_, err = m.SessionStore.Put(key, data)
	if err != nil {
		log.Printf("Error at createUploadMeta when SessionStore.Put(): %v\n", err)
		return err
	}
	return nil
}

func (m *MultiPartStore) saveUploadMeta(meta UploadMeta, revision uint64) error {
	data, err := json.Marshal(meta)
	if err != nil {
		log.Printf("Error at saveUploadMeta when json.Marshal(): %v\n", err)
		return err
	}
	key := sessionKey(meta.Bucket, meta.Key, meta.UploadID)
	// FIXME: Retries on revision conflicts to synchronize concurrent updates across goroutines/processes.
	_, err = m.SessionStore.Update(key, data, revision)
	if err != nil {
		log.Printf("Error at saveUploadMeta when SessionStore.Update(): %v\n", err)
		return err
	}
	return nil
}

// createPartUpload streams a part from the provided reader into the temporary
// Object Store under the given part key and returns the stored object's info.
func (m *MultiPartStore) createPartUpload(partKey string, dataReader *io.PipeReader) (*nats.ObjectInfo, error) {
	log.Printf("createPartUpload : %s\n", partKey)
	obj, err := m.TempPartStore.Put(&nats.ObjectMeta{Name: partKey}, dataReader)
	if err != nil {
		log.Printf("Error at createPartUpload when TempPartStore.Put(): %v\n", err)
		return nil, err
	}
	return obj, nil
}

func (m *MultiPartStore) getPartUpload(partKey string) (nats.ObjectResult, error) {
	return m.TempPartStore.Get(partKey)
}

func (m *MultiPartStore) cleanMultipartUpload(meta UploadMeta) error {
	for pn := range meta.Parts {
		key := partKey(meta.Bucket, meta.Key, meta.UploadID, pn)
		err := m.TempPartStore.Delete(key)
		if err != nil {
			log.Printf("Error at cleanMultipartUpload when TempPartStore.Delete(): %v\n", err)
			return err
		}
	}

	sessionKey := sessionKey(meta.Bucket, meta.Key, meta.UploadID)
	err := m.SessionStore.Delete(sessionKey)
	if err != nil {
		log.Printf("Error at cleanMultipartUpload when SessionStore.Delete(): %v\n", err)
		return err
	}

	return nil
}

// getUploadMeta fetches the KV entry for a multipart upload session, including
// its current revision number for optimistic updates.
func (m *MultiPartStore) getUploadMeta(sessionKey string) (nats.KeyValueEntry, error) {
	entry, err := m.SessionStore.Get(sessionKey)
	if err != nil {
		log.Printf("Error at getSession when kv.Get(): %v\n", err)
		return nil, err
	}
	return entry, nil
}

// NatsObjectClient provides convenience helpers for common NATS JetStream
// Object Store operations, built on top of the base Client connection.
type NatsObjectClient struct {
	Client         *Client
	MultiPartStore *MultiPartStore
}

// CreateBucket creates a JetStream Object Store bucket.
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

// DeleteBucket deletes a bucket identified by its name.
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

// GetObject retrieves an object's metadata and bytes.
func (c *NatsObjectClient) GetObject(bucket string, key string) (*nats.ObjectInfo, []byte, error) {
	nc := c.Client.NATS()
	js, err := nc.JetStream()
	if err != nil {
		log.Printf("Error at GetObject when nc.JetStream(): %v\n", err)
		return nil, nil, err
	}
	os, err := js.ObjectStore(bucket)
	if err != nil {
		log.Printf("Error at GetObject when js.ObjectStore(): %v\n", err)
		if errors.Is(err, nats.ErrStreamNotFound) {
			return nil, nil, ErrBucketNotFound
		}
		return nil, nil, err
	}
	info, err := os.GetInfo(key)
	if err != nil {
		log.Printf("Error at GetObject when os.GetInfo(): %v\n", err)
		if errors.Is(err, nats.ErrObjectNotFound) {
			return nil, nil, ErrObjectNotFound
		}
		return nil, nil, err
	}
	res, err := os.GetBytes(key)
	if err != nil {
		log.Printf("Error at GetObject when os.GetBytes(): %v\n", err)
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

// PutObject writes an object to the given bucket with the provided key and metadata.
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

// InitMultipartUpload creates and persists a new multipart upload session
// for the given bucket/key and uploadID.
func (c *NatsObjectClient) InitMultipartUpload(bucket string, key string, uploadID string) error {
	meta := UploadMeta{
		UploadID:  uploadID,
		Bucket:    bucket,
		Key:       key,
		Initiated: time.Now().UTC(),
		MinPartSz: 5 * 1024 * 1024,
		MaxParts:  10000,
		Parts:     map[int]PartMeta{},
	}

	return c.MultiPartStore.createUploadMeta(meta)
}

// UploadPart streams a part into temporary storage and records its ETag/size
// under the multipart session. Returns the hex ETag (without quotes).
func (c *NatsObjectClient) UploadPart(bucket string, key string, uploadID string, part int, dataReader io.ReadCloser) (string, error) {
	sessionKey := sessionKey(bucket, key, uploadID)
	sessionData, err := c.MultiPartStore.getUploadMeta(sessionKey)
	if err != nil {
		return "", ErrUploadNotFound
	}

	var meta UploadMeta
	if err := json.Unmarshal(sessionData.Value(), &meta); err != nil {
		log.Printf("Error at UploadPart when json.Unmarshal(): %v\n", err)
		return "", err
	}

	if meta.Aborted || meta.Completed {
		return "", ErrUploadCompleted
	}

	h := md5.New()
	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		_, err := io.Copy(io.MultiWriter(h, pw), dataReader)
		if err != nil {
			_ = pw.CloseWithError(err)
		}
	}()

	partKey := partKey(bucket, key, uploadID, part)
	obj, err := c.MultiPartStore.createPartUpload(partKey, pr)
	if err != nil {
		return "", err
	}

	etag := strings.ToLower(hex.EncodeToString(h.Sum(nil)))
	meta.Parts[part] = PartMeta{
		Number: part, ETag: `"` + etag + `"`, Size: obj.Size, StoredAt: time.Now().Unix(),
	}

	err = c.MultiPartStore.saveUploadMeta(meta, sessionData.Revision())
	if err != nil {
		return "", err
	}
	return etag, nil
}

func (c *NatsObjectClient) CompleteMultipartUpload(bucket string, key string, uploadID string, sortedPartNumbers []int) (string, error) {
	sessionKey := sessionKey(bucket, key, uploadID)
	sessionData, err := c.MultiPartStore.getUploadMeta(sessionKey)
	if err != nil {
		return "", ErrUploadNotFound
	}

	var meta UploadMeta
	if err := json.Unmarshal(sessionData.Value(), &meta); err != nil {
		log.Printf("Error at UploadPart when json.Unmarshal(): %v\n", err)
		return "", err
	}

	md5Concat := md5.New()
	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		for _, pn := range sortedPartNumbers {
			pmeta, ok := meta.Parts[pn]
			if !ok {
				_ = pw.CloseWithError(ErrMissingPart)
				return
			}
			partKey := partKey(bucket, key, uploadID, pn)
			rpart, err := c.MultiPartStore.getPartUpload(partKey)
			if err != nil {
				_ = pw.CloseWithError(err)
				return
			}

			rawHex := strings.Trim(pmeta.ETag, `"`)
			b, _ := hex.DecodeString(rawHex)
			md5Concat.Write(b)

			_, err = io.Copy(pw, rpart)
			rpart.Close()
			if err != nil {
				_ = pw.CloseWithError(err)
				return
			}
		}
	}()

	nc := c.Client.NATS()
	js, err := nc.JetStream()
	if err != nil {
		log.Printf("Error at CompleteMultipartUpload when js.JetStream: %v\n", err)
		return "", err
	}
	os, err := js.ObjectStore(bucket)
	if err != nil {
		log.Printf("Error at CompleteMultipartUpload when js.ObjectStore: %v\n", err)
		if errors.Is(err, nats.ErrStreamNotFound) {
			return "", ErrBucketNotFound
		}
		return "", err
	}
	_, err = os.Put(&nats.ObjectMeta{Name: key}, pr)
	if err != nil {
		log.Printf("Error at CompleteMultipartUpload when os.Put(): %v\n", err)
		return "", err
	}

	etagHex := hex.EncodeToString(md5Concat.Sum(nil))
	finalETag := fmt.Sprintf(`"%s-%d"`, strings.ToLower(etagHex), len(sortedPartNumbers))

	err = c.MultiPartStore.cleanMultipartUpload(meta)
	if err != nil {
		log.Printf("WARN, failed to clean multipart session/temp data: %v\n", err)
	}

	return finalETag, nil
}

// sessionKey builds the KV key used to persist state for a multipart upload.
func sessionKey(bucket, key, uploadID string) string {
	return fmt.Sprintf("multi_parts/%s/%s/%s", bucket, key, uploadID)
}

// partKey constructs the Object Store key for a specific uploaded part
func partKey(bucket, key, uploadID string, part int) string {
	return fmt.Sprintf("multi_parts/%s/%s/%s/%06d", bucket, key, uploadID, part)
}
