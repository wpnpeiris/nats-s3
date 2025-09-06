package s3api

import (
	"fmt"
	"net/http"
)

// handleJetStreamError writes a generic 500 response when JetStream
// initialization or operations fail unexpectedly.
func handleJetStreamError(err error, w http.ResponseWriter) {
    fmt.Printf("Error at creating JetStreamContext, %s", err)
    http.Error(w, "Unexpected", http.StatusInternalServerError)
}

// handleObjectStoreError writes a 404 response when the requested bucket
// cannot be found or the Object Store cannot be opened.
func handleObjectStoreError(err error, w http.ResponseWriter) {
    fmt.Printf("Error at creating ObjectStore instance for bucket, %s", err)
    http.Error(w, "Bucket not found", http.StatusNotFound)
}
