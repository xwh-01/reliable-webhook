package worker

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"time"
)

type DeliveryResult struct {
	StatusCode int
	Retryable  bool
	Err        error
}

type WebhookClient struct {
	httpClient *http.Client
}

func NewWebhookClient() *WebhookClient {
	return &WebhookClient{
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

func (c *WebhookClient) Send(ctx context.Context, targetURL string, payload string) DeliveryResult {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewBufferString(payload))
	if err != nil {
		return DeliveryResult{
			Err:       fmt.Errorf("build request failed: %w", err),
			Retryable: false,
		}
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		// 网络错误 / timeout，通常可重试
		return DeliveryResult{
			Err:       fmt.Errorf("send webhook failed: %w", err),
			Retryable: true,
		}
	}
	defer resp.Body.Close()

	status := resp.StatusCode

	// 成功
	if status >= 200 && status < 300 {
		return DeliveryResult{
			StatusCode: status,
			Retryable:  false,
			Err:        nil,
		}
	}

	// 429 / 5xx 通常可重试
	if status == http.StatusTooManyRequests || status >= 500 {
		return DeliveryResult{
			StatusCode: status,
			Retryable:  true,
			Err:        fmt.Errorf("unexpected status code: %d", status),
		}
	}

	// 其他 4xx 通常不重试
	return DeliveryResult{
		StatusCode: status,
		Retryable:  false,
		Err:        fmt.Errorf("unexpected status code: %d", status),
	}
}