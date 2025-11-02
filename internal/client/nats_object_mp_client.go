package client

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/wpnpeiris/nats-s3/internal/logging"
)

// PartMeta describes a single part in a multipart upload.
// It records the part number, ETag (checksum), size in bytes, and the
// time the part was stored (Unix seconds).
type PartMeta struct {
	Number   int    `json:"number"`
	ETag     string `json:"etag"`
	Size     uint64 `json:"size"`
	StoredAt int64  `json:"stored_at_unix"`
}

// UploadMeta captures the server-side state of a multipart upload.
// It includes identifiers (UploadID, Bucket, Key), initiation time (UTC),
// optional owner, and constraints (minimum part size and max parts).
// The JSON value is persisted in a Key-Value store under a session-specific key.
// Individual part metadata is stored in separate KV entries to avoid write conflicts.
// The Parts field is populated on-demand when calling ListParts or CompleteMultipartUpload.
type UploadMeta struct {
	UploadID  string           `json:"upload_id"`
	Bucket    string           `json:"bucket"`
	Key       string           `json:"key"`
	Initiated time.Time        `json:"initiated"`
	Owner     string           `json:"owner,omitempty"` // optional, auth principal
	MinPartSz int64            `json:"min_part_size"`   // default 5MiB
	MaxParts  int              `json:"max_parts"`       // default 10000
	Parts     map[int]PartMeta `json:"-"`               // Not persisted, populated on-demand
}

type MultiPartStoreOptions struct {
	Replicas int
}

// MultiPartStore groups storage backends used for multipart uploads.
// metaStore tracks session metadata in a Key-Value bucket, while
// tempPartStore holds uploaded parts in a temporary Object Store.
type MultiPartStore struct {
	logger          log.Logger
	client          *Client
	metaStore       jetstream.KeyValue
	partMetaStore   jetstream.KeyValue
	partObjectStore jetstream.ObjectStore
}

func NewMultiPartStore(ctx context.Context, logger log.Logger,
	c *Client, opts MultiPartStoreOptions) (*MultiPartStore, error) {
	if opts.Replicas < 1 {
		logging.Info(logger, "msg", fmt.Sprintf("Invalid replicas count, defaulting to 1: [%d]", opts.Replicas))
		opts.Replicas = 1
	}
	js := c.JetStream()

	metaKV, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:   MetaStoreName,
		Replicas: opts.Replicas,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to upsert multipart meta store when calling js.KeyValue(): %w", err)
	}

	partMetaKV, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:   PartMetaStoreName,
		Replicas: opts.Replicas,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to upsert multipart part-meta store when calling js.KeyValue(): %w", err)
	}

	partOS, err := js.CreateOrUpdateObjectStore(ctx, jetstream.ObjectStoreConfig{
		Bucket:   TempStoreName,
		Replicas: opts.Replicas,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to upsert multipart temp store when calling js.ObjectStore(): %w", err)
	}

	return &MultiPartStore{
		logger:          logger,
		client:          c,
		metaStore:       metaKV,
		partMetaStore:   partMetaKV,
		partObjectStore: partOS,
	}, nil
}

// InitMultipartUpload creates and persists a new multipart upload session
// for the given bucket/key and uploadID.
func (m *MultiPartStore) InitMultipartUpload(ctx context.Context, bucket string, key string, uploadID string) error {
	logging.Info(m.logger, "msg", fmt.Sprintf("Init multipart upload: [%s/%s]", bucket, key))
	meta := UploadMeta{
		UploadID:  uploadID,
		Bucket:    bucket,
		Key:       key,
		Initiated: time.Now().UTC(),
		MinPartSz: 5 * 1024 * 1024,
		MaxParts:  10000,
	}

	return m.saveUploadMeta(ctx, meta)
}

// UploadPart streams a part into temporary storage and records its ETag/size
// under the multipart session. Returns the hex ETag (without quotes).
func (m *MultiPartStore) UploadPart(ctx context.Context, bucket string, key string, uploadID string, part int, dataReader io.ReadCloser) (string, error) {
	logging.Info(m.logger, "msg", fmt.Sprintf("Upload part:%06d [%s/%s], UploadID: %s", part, bucket, key, uploadID))

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
	obj, err := m.savePartData(ctx, partKey, pr)
	if err != nil {
		// Close the reader to signal the goroutine to stop
		_ = pr.Close()
		return "", err
	}

	etag := strings.ToLower(hex.EncodeToString(h.Sum(nil)))
	partMeta := PartMeta{
		Number: part, ETag: `"` + etag + `"`, Size: obj.Size, StoredAt: time.Now().Unix(),
	}

	// Save part metadata in its own KV entry
	err = m.savePartMeta(ctx, bucket, key, uploadID, partMeta)
	if err != nil {
		return "", err
	}
	return etag, nil
}

// AbortMultipartUpload aborts an in‑progress multipart upload, deleting any
// uploaded parts and removing the session metadata.
func (m *MultiPartStore) AbortMultipartUpload(ctx context.Context, bucket string, key string, uploadID string) error {
	logging.Info(m.logger, "msg", fmt.Sprintf("Abort multipart upload: [%s/%s], UploadID: %s", bucket, key, uploadID))
	mk := metaKey(bucket, key, uploadID)
	md, err := m.getUploadMeta(ctx, mk)
	if err != nil {
		return ErrUploadNotFound
	}

	var meta UploadMeta
	if err := json.Unmarshal(md.Value(), &meta); err != nil {
		logging.Error(m.logger, "msg", "Error at AbortMultipartUpload", "err", err)
		return err
	}

	// Get all part metadata to find all parts to delete
	parts, err := m.getAllPartMeta(ctx, bucket, key, uploadID)
	if err != nil {
		logging.Warn(m.logger, "msg", "Error getting part metadata at AbortMultipartUpload", "err", err)
		// Continue with cleanup even if we can't get all parts
		parts = make(map[int]PartMeta)
	}

	// Delete temporary part data from Object Store
	for pn := range parts {
		partKey := partKey(bucket, key, uploadID, pn)
		err := m.removePartData(ctx, partKey)
		if err != nil {
			logging.Warn(m.logger, "msg", "Error deleting part upload at AbortMultipartUpload", "err", err)
		}
	}

	// Delete part metadata from KV store
	err = m.deleteAllPartMeta(ctx, bucket, key, uploadID)
	if err != nil {
		logging.Warn(m.logger, "msg", "Failed to delete part metadata at AbortMultipartUpload", "err", err)
	}

	// Delete session metadata
	err = m.removeUploadMeta(ctx, meta)
	if err != nil {
		logging.Warn(m.logger, "msg", "Failed to delete multipart session data at AbortMultipartUpload", "err", err)
		return err
	}

	return nil
}

// ListParts returns the multipart upload metadata for the given
// bucket/key/uploadID, including uploaded parts with sizes and ETags.
func (m *MultiPartStore) ListParts(ctx context.Context, bucket string, key string, uploadID string) (*UploadMeta, error) {
	logging.Info(m.logger, "msg", fmt.Sprintf("List parts: [%s/%s], UploadID: %s", bucket, key, uploadID))
	mk := metaKey(bucket, key, uploadID)
	md, err := m.getUploadMeta(ctx, mk)
	if err != nil {
		return nil, ErrUploadNotFound
	}

	var meta UploadMeta
	if err := json.Unmarshal(md.Value(), &meta); err != nil {
		logging.Error(m.logger, "msg", "Error at ListParts", "err", err)
		return nil, err
	}

	// Populate parts from individual KV entries
	parts, err := m.getAllPartMeta(ctx, bucket, key, uploadID)
	if err != nil {
		logging.Error(m.logger, "msg", "Error at ListParts when getAllPartMeta()", "err", err)
		return nil, err
	}
	meta.Parts = parts

	return &meta, nil
}

// CompleteMultipartUpload concatenates the uploaded parts into the final
// object, computes and returns the multipart ETag, and cleans up temporary
// parts and metadata.
func (m *MultiPartStore) CompleteMultipartUpload(ctx context.Context, bucket string, key string, uploadID string, sortedPartNumbers []int) (string, error) {
	logging.Info(m.logger, "msg", fmt.Sprintf("Complete multipart upload: [%s/%s], UploadID: %s", bucket, key, uploadID))
	mk := metaKey(bucket, key, uploadID)
	md, err := m.getUploadMeta(ctx, mk)
	if err != nil {
		return "", ErrUploadNotFound
	}

	var meta UploadMeta
	if err := json.Unmarshal(md.Value(), &meta); err != nil {
		logging.Error(m.logger, "msg", "Error at CompleteMultipartUpload", "err", err)
		return "", err
	}

	// Populate parts from individual KV entries
	parts, err := m.getAllPartMeta(ctx, bucket, key, uploadID)
	if err != nil {
		logging.Error(m.logger, "msg", "Error at CompleteMultipartUpload when getAllPartMeta()", "err", err)
		return "", err
	}
	meta.Parts = parts

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
			rpart, err := m.getPartData(ctx, partKey)
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

	// Ensure the goroutine is cleaned up on early return
	defer func() {
		if err != nil {
			// Close reader to unblock the goroutine if we're returning with an error
			_ = pr.Close()
		}
	}()
	js := m.client.JetStream()
	os, err := js.ObjectStore(ctx, bucket)
	if err != nil {
		logging.Error(m.logger, "msg", "Error at CompleteMultipartUpload", "err", err)
		if errors.Is(err, jetstream.ErrBucketNotFound) {
			return "", ErrBucketNotFound
		}
		return "", err
	}
	_, err = os.Put(ctx, jetstream.ObjectMeta{Name: key}, pr)
	if err != nil {
		logging.Error(m.logger, "msg", "Error at CompleteMultipartUpload", "err", err)
		return "", err
	}

	etagHex := hex.EncodeToString(md5Concat.Sum(nil))
	finalETag := fmt.Sprintf(`"%s-%d"`, strings.ToLower(etagHex), len(sortedPartNumbers))

	// Delete temporary part data from Object Store
	err = m.removeAllPartData(ctx, bucket, key, uploadID, meta.Parts)
	if err != nil {
		logging.Warn(m.logger, "msg", "Failed to clean multipart temp part data at CompleteMultipartUpload", "err", err)
	}

	// Delete part metadata from KV store
	err = m.deleteAllPartMeta(ctx, bucket, key, uploadID)
	if err != nil {
		logging.Warn(m.logger, "msg", "Failed to clean multipart part metadata at CompleteMultipartUpload", "err", err)
	}

	// Delete metadata
	err = m.removeUploadMeta(ctx, meta)
	if err != nil {
		logging.Warn(m.logger, "Failed to delete multipart meta data", "err", err)
		return "", err
	}

	return finalETag, nil
}

// saveUploadMeta persists the given meta value at the provided key in the
// UploadMeta Key-Value store. The value is expected to be a JSON-encoded
// UploadMeta blob. Returns any error encountered during the put operation.
func (m *MultiPartStore) saveUploadMeta(ctx context.Context, meta UploadMeta) error {
	logging.Debug(m.logger, "msg", fmt.Sprintf("creating upload meta: %v", meta))
	data, err := json.Marshal(meta)
	if err != nil {
		logging.Error(m.logger, "msg", "Error at saveUploadMeta when json.Marshal()", "err", err)
		return err
	}
	key := metaKey(meta.Bucket, meta.Key, meta.UploadID)
	_, err = m.metaStore.Put(ctx, key, data)
	if err != nil {
		logging.Error(m.logger, "msg", "Error at saveUploadMeta when sessionStore.Put()", "err", err)
		return err
	}
	return nil
}

// removeUploadMeta delete the persisted multipart upload metadata.
func (m *MultiPartStore) removeUploadMeta(ctx context.Context, meta UploadMeta) error {
	logging.Debug(m.logger, "msg", fmt.Sprintf("remove upload meta: %v", meta))
	key := metaKey(meta.Bucket, meta.Key, meta.UploadID)
	err := m.metaStore.Delete(ctx, key)
	if err != nil {
		logging.Error(m.logger, "msg", "Error at removeUploadMeta when sessionStore.Delete()", "err", err)
		return err
	}

	return nil
}

// getUploadMeta fetches the KV entry for a multipart upload session, including
// its current revision number for optimistic updates.
func (m *MultiPartStore) getUploadMeta(ctx context.Context, sessionKey string) (jetstream.KeyValueEntry, error) {
	logging.Debug(m.logger, "msg", fmt.Sprintf("get upload meta: %s", sessionKey))
	entry, err := m.metaStore.Get(ctx, sessionKey)
	if err != nil {
		logging.Error(m.logger, "msg", "Error at getSession when kv.Get()", "err", err)
		return nil, err
	}
	return entry, nil
}

// savePartData streams a part from the provided reader into the temporary
// Object Store under the given part key and returns the stored object's info.
func (m *MultiPartStore) savePartData(ctx context.Context, partKey string, dataReader *io.PipeReader) (*jetstream.ObjectInfo, error) {
	logging.Debug(m.logger, "msg", fmt.Sprintf("uploading part: %s", partKey))
	obj, err := m.partObjectStore.Put(ctx, jetstream.ObjectMeta{Name: partKey}, dataReader)
	if err != nil {
		logging.Error(m.logger, "msg", "Error at savePartData when tempPartStore.Put()", "err", err)
		return nil, err
	}
	return obj, nil
}

// getPartData return part from the temporary Object Store.
func (m *MultiPartStore) getPartData(ctx context.Context, partKey string) (jetstream.ObjectResult, error) {
	logging.Debug(m.logger, "msg", fmt.Sprintf("get part upload: %s", partKey))
	return m.partObjectStore.Get(ctx, partKey)
}

// removePartData delete part from the temporary Object Store.
func (m *MultiPartStore) removePartData(ctx context.Context, partKey string) error {
	logging.Debug(m.logger, "msg", fmt.Sprintf("delete part upload: %s", partKey))
	return m.partObjectStore.Delete(ctx, partKey)
}

// removeAllPartData delete all parts from the temporary Object Store.
func (m *MultiPartStore) removeAllPartData(ctx context.Context, bucket, key, uploadID string, parts map[int]PartMeta) error {
	logging.Debug(m.logger, "msg", fmt.Sprintf("delete all part upload: bucket=%s key=%s uploadID=%s", bucket, key, uploadID))
	for pn := range parts {
		partKey := partKey(bucket, key, uploadID, pn)
		err := m.partObjectStore.Delete(ctx, partKey)
		if err != nil {
			logging.Error(m.logger, "msg", "Error at all when tempPartStore.Delete()", "err", err)
			return err
		}
	}

	return nil
}

// savePartMeta stores metadata for a single part in the KV store.
func (m *MultiPartStore) savePartMeta(ctx context.Context, bucket, key, uploadID string, partMeta PartMeta) error {
	logging.Debug(m.logger, "msg", fmt.Sprintf("save part meta: bucket=%s key=%s uploadID=%s part=%d", bucket, key, uploadID, partMeta.Number))
	pm, err := json.Marshal(partMeta)
	if err != nil {
		logging.Error(m.logger, "msg", "Error at savePartMeta when json.Marshal()", "err", err)
		return err
	}
	pmk := partMetaKey(bucket, key, uploadID, partMeta.Number)
	_, err = m.partMetaStore.Put(ctx, pmk, pm)
	if err != nil {
		logging.Error(m.logger, "msg", "Error at savePartMeta when sessionStore.Put()", "err", err)
		return err
	}
	return nil
}

// getPartMeta retrieves metadata for a single part from the KV store.
func (m *MultiPartStore) getPartMeta(ctx context.Context, bucket, key, uploadID string, partNumber int) (*PartMeta, error) {
	logging.Debug(m.logger, "msg", fmt.Sprintf("get part meta: bucket=%s key=%s uploadID=%s part=%d", bucket, key, uploadID, partNumber))
	partMetaKey := partMetaKey(bucket, key, uploadID, partNumber)
	entry, err := m.partMetaStore.Get(ctx, partMetaKey)
	if err != nil {
		logging.Error(m.logger, "msg", "Error at getPartMeta when sessionStore.Get()", "err", err)
		return nil, err
	}
	var partMeta PartMeta
	if err := json.Unmarshal(entry.Value(), &partMeta); err != nil {
		logging.Error(m.logger, "msg", "Error at getPartMeta when json.Unmarshal()", "err", err)
		return nil, err
	}
	return &partMeta, nil
}

// getAllPartMeta retrieves all part metadata for a given upload session.
func (m *MultiPartStore) getAllPartMeta(ctx context.Context, bucket, key, uploadID string) (map[int]PartMeta, error) {
	logging.Debug(m.logger, "msg", fmt.Sprintf("get all part meta: bucket=%s key=%s uploadID=%s", bucket, key, uploadID))
	prefix := partMetaPrefix(bucket, key, uploadID)

	keys, err := m.partMetaStore.ListKeys(ctx)
	if err != nil {
		logging.Error(m.logger, "msg", "Error at getAllPartMeta when partMetaStore.Keys()", "err", err)
		return nil, err
	}

	parts := make(map[int]PartMeta)
	for kvKey := range keys.Keys() {
		if strings.HasPrefix(kvKey, prefix) {
			entry, err := m.partMetaStore.Get(ctx, kvKey)
			if err != nil {
				logging.Warn(m.logger, "msg", "Error getting part metadata", "key", kvKey, "err", err)
				continue
			}
			var partMeta PartMeta
			if err := json.Unmarshal(entry.Value(), &partMeta); err != nil {
				logging.Warn(m.logger, "msg", "Error unmarshaling part metadata", "key", kvKey, "err", err)
				continue
			}
			parts[partMeta.Number] = partMeta
		}
	}

	return parts, nil
}

// deleteAllPartMeta deletes all part metadata entries for a given upload session.
func (m *MultiPartStore) deleteAllPartMeta(ctx context.Context, bucket, key, uploadID string) error {
	logging.Debug(m.logger, "msg", fmt.Sprintf("delete all part meta: bucket=%s key=%s uploadID=%s", bucket, key, uploadID))
	prefix := partMetaPrefix(bucket, key, uploadID)

	keys, err := m.partMetaStore.ListKeys(ctx)
	if err != nil {
		logging.Error(m.logger, "msg", "Error at deleteAllPartMeta when partMetaStore.Keys()", "err", err)
		return err
	}

	for kvKey := range keys.Keys() {
		if strings.HasPrefix(kvKey, prefix) {
			err := m.partMetaStore.Delete(ctx, kvKey)
			if err != nil {
				logging.Warn(m.logger, "msg", "Error deleting part metadata", "key", kvKey, "err", err)
			}
		}
	}

	return nil
}

// metaKey builds the KV key used to persist state for a multipart upload.
func metaKey(bucket, key, uploadID string) string {
	return fmt.Sprintf("multi_parts/%s/%s/%s", bucket, key, uploadID)
}

// partKey constructs the Object Store key for a specific uploaded part
func partKey(bucket, key, uploadID string, part int) string {
	return fmt.Sprintf("multi_parts/%s/%s/%s/%06d", bucket, key, uploadID, part)
}

// partMetaKey builds the KV key for a single part's metadata.
func partMetaKey(bucket, key, uploadID string, part int) string {
	return fmt.Sprintf("multi_parts/%s/%s/%s/parts/%06d", bucket, key, uploadID, part)
}

// partMetaPrefix returns the KV key prefix for all parts of an upload metadata.
func partMetaPrefix(bucket, key, uploadID string) string {
	return fmt.Sprintf("multi_parts/%s/%s/%s/parts/", bucket, key, uploadID)
}
