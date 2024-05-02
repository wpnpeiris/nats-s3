package s3

import "net/http"

func (s3Gateway *S3Gateway) SetOptionHeaders(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Expose-Headers", "*")
	w.Header().Set("Access-Control-Allow-Methods", "*")
	w.Header().Set("Access-Control-Allow-Headers", "*")

	WriteEmptyResponse(w, r, http.StatusOK)
}
