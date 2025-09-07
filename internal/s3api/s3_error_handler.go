package s3api

import (
	"fmt"
	"net/http"
)

// handleInternalError writes a generic 500 response when JetStream
// initialization or operations fail unexpectedly.
func handleInternalError(err error, w http.ResponseWriter) {
	fmt.Printf("Error at JetStreamContext, %s", err)
	http.Error(w, "Unexpected", http.StatusInternalServerError)
}
