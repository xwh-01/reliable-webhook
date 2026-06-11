// HTTP 客户端，用于向 target_url 发起 webhook POST 请求
//
// 返回 DeliveryResult，而不是 (statusCode, error)：
//   - 成功（2xx）→ StatusCode 有值，Err=nil
//   - 可重试失败（网络错误/超时/429/5xx）→ Retryable=true
//   - 不可重试失败（其他 4xx/URL 格式错误）→ Retryable=false
//
// 注意：这个 Send 方法自身也有 context 超时。
// 上层 delivery_worker 传入的 execCtx 已有超时，这里再传一次：
//   实际生效的超时 = min(execCtx.Deadline - now, httpClient.Timeout)
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
	Retryable  bool // 调用方根据此决定是 retry 还是 dead
	Err        error
}

// WebhookClient 内部维护一个 *http.Client，可复用 TCP 连接
type WebhookClient struct {
	httpClient *http.Client
}

func NewWebhookClient() *WebhookClient {
	return &WebhookClient{
		httpClient: &http.Client{
			Timeout: 5 * time.Second, // 连接+等待响应的总超时
		},
	}
}

// Send 向 target_url 发起 POST，payload 为 JSON 字符串
// Content-Type 硬编码为 application/json
func (c *WebhookClient) Send(ctx context.Context, targetURL string, payload string) DeliveryResult {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewBufferString(payload))
	if err != nil {
		// URL 格式错误 → 不可重试
		return DeliveryResult{
			Err:       fmt.Errorf("build request failed: %w", err),
			Retryable: false,
		}
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		// 网络错误 / DNS 解析失败 / 连接超时 → 通常可重试
		return DeliveryResult{
			Err:       fmt.Errorf("send webhook failed: %w", err),
			Retryable: true,
		}
	}
	defer resp.Body.Close()

	status := resp.StatusCode

	// 2xx → 成功
	if status >= 200 && status < 300 {
		return DeliveryResult{
			StatusCode: status,
			Retryable:  false,
			Err:        nil,
		}
	}

	// 429 Too Many Requests / 5xx Server Error → 可重试
	if status == http.StatusTooManyRequests || status >= 500 {
		return DeliveryResult{
			StatusCode: status,
			Retryable:  true,
			Err:        fmt.Errorf("unexpected status code: %d", status),
		}
	}

	// 其他 4xx（400/401/403/404/405 等）→ 不可重试
	// 原因：重试依然是同样的请求，下游大概率返回同样的 4xx
	// 与其浪费重试次数，不如直接标记 dead，让运维排查
	return DeliveryResult{
		StatusCode: status,
		Retryable:  false,
		Err:        fmt.Errorf("unexpected status code: %d", status),
	}
}
