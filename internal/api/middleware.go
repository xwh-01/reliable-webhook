// 中间件：为每个 HTTP 请求设置 context 超时
//
// Gin 的 gin.Context 包装了 *http.Request，中间件通过 c.Request.WithContext()
// 将具有超时的 context 注入到后续 handler 的调用链中。
// 如果 handler 或下游操作（如数据库查询）超过了 timeout，context 会自动取消。
package api

import(
	"context"
	"time"

	"github.com/gin-gonic/gin"
)

func RequestTimeout(timeout time.Duration) gin.HandlerFunc{
	return func(c *gin.Context){
		ctx, cancel:= context.WithTimeout(c.Request.Context(),timeout)
		defer cancel() // handler 返回后释放资源

		c.Request = c.Request.WithContext(ctx)
		c.Next()
	}
}
