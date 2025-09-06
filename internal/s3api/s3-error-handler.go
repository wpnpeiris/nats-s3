package s3api

import (
	"fmt"
	"net/http"
)

func handleJetStreamError(err error, w http.ResponseWriter) {
	fmt.Printf("Error at creating JetStreamContext, %s", err)
	http.Error(w, "Unexpected", http.StatusInternalServerError)
}

func handleObjectStoreError(err error, w http.ResponseWriter) {
	fmt.Printf("Error at creating ObjectStore instance for bucket, %s", err)
	http.Error(w, "Bucket not found", http.StatusNotFound)
}
