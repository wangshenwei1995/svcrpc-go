// notify-svc 示例微服务 B：提供 SendEmail 方法，演示"被调用方"。
//
// 运行: go run ./examples/notify-svc   （默认监听 127.0.0.1:9002）
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"svcrpc/host"
)

type emailReq struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rdb := redis.NewClient(&redis.Options{Addr: getenv("REDIS_ADDR", "127.0.0.1:6379")})
	defer rdb.Close()
	h, err := host.NewHost(rdb, host.Config{
		Service: "notify",
		Addr:    getenv("NOTIFY_ADDR", "127.0.0.1:9002"),
		// RedisWait: 允许服务先于 Redis 启动，最多等待 30s 后自动注册
		RedisWait: 30 * time.Second,
	})
	if err != nil {
		log.Error("new host", "error", err)
		os.Exit(1)
	}

	h.Handle("SendEmail", func(ctx context.Context, payload []byte) ([]byte, error) {
		var req emailReq
		if err := json.Unmarshal(payload, &req); err != nil {
			return nil, status.Error(codes.InvalidArgument, "bad request: "+err.Error())
		}
		if req.To == "" || req.Subject == "" {
			return nil, status.Error(codes.InvalidArgument, "to and subject are required")
		}

		// 模拟发送邮件耗时
		select {
		case <-time.After(30 * time.Millisecond):
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		log.Info("email sent", "to", req.To, "subject", req.Subject)

		out, _ := json.Marshal(map[string]any{
			"status":  "sent",
			"to":      req.To,
			"subject": req.Subject,
		})
		return out, nil
	})

	log.Info("notify-svc starting")
	if err := h.Serve(ctx); err != nil {
		log.Error("notify-svc exited with error", "error", err)
		os.Exit(1)
	}
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
