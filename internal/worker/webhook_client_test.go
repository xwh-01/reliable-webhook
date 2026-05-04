package worker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestWebhookClientSendClassifiesHTTPStatuses(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		wantErr    bool
		wantRetry  bool
	}{
		{name: "success", statusCode: http.StatusOK, wantErr: false, wantRetry: false},
		{name: "too_many_requests", statusCode: http.StatusTooManyRequests, wantErr: true, wantRetry: true},
		{name: "server_error", statusCode: http.StatusInternalServerError, wantErr: true, wantRetry: true},
		{name: "bad_request", statusCode: http.StatusBadRequest, wantErr: true, wantRetry: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost {
					t.Fatalf("method = %s, want POST", r.Method)
				}
				if got := r.Header.Get("Content-Type"); got != "application/json" {
					t.Fatalf("content-type = %q, want application/json", got)
				}
				w.WriteHeader(tt.statusCode)
			}))
			defer server.Close()

			client := NewWebhookClient()
			result := client.Send(context.Background(), server.URL, `{"ok":true}`)

			if result.StatusCode != tt.statusCode {
				t.Fatalf("status code = %d, want %d", result.StatusCode, tt.statusCode)
			}
			if (result.Err != nil) != tt.wantErr {
				t.Fatalf("err presence = %v, want %v", result.Err != nil, tt.wantErr)
			}
			if result.Retryable != tt.wantRetry {
				t.Fatalf("retryable = %v, want %v", result.Retryable, tt.wantRetry)
			}
		})
	}
}

func TestWebhookClientSendRejectsInvalidURL(t *testing.T) {
	client := NewWebhookClient()

	result := client.Send(context.Background(), "://bad-url", `{"ok":true}`)

	if result.Err == nil {
		t.Fatal("err = nil, want error")
	}
	if result.Retryable {
		t.Fatal("retryable = true, want false")
	}
}

func TestBackoff(t *testing.T) {
	tests := []struct {
		attemptNo int
		want      time.Duration
	}{
		{attemptNo: 1, want: 5 * time.Second},
		{attemptNo: 2, want: 15 * time.Second},
		{attemptNo: 3, want: 30 * time.Second},
		{attemptNo: 10, want: 30 * time.Second},
	}

	for _, tt := range tests {
		if got := backoff(tt.attemptNo); got != tt.want {
			t.Fatalf("backoff(%d) = %s, want %s", tt.attemptNo, got, tt.want)
		}
	}
}
