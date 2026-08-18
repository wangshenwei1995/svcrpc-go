// order-svc 示例微服务 A：提供 CreateOrder / GetOrder。
// 演示"调用方 + 被调用方"双重身份：
//   - 被 demo 客户端调用（orders.CreateOrder / orders.GetOrder）
//   - 在 CreateOrder 内部通过插件跨服务调用 notify.SendEmail
//
// 运行: go run ./examples/order-svc   （默认监听 127.0.0.1:9001）
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"svcrpc/client"
	"svcrpc/host"
)

type createOrderReq struct {
	Item  string `json:"item"`
	Qty   int    `json:"qty"`
	Email string `json:"email"`
}

type getOrderReq struct {
	ID string `json:"id"`
}

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rdb := redis.NewClient(&redis.Options{Addr: getenv("REDIS_ADDR", "192.168.1.20:45157")})
	defer rdb.Close()
	// 调用方插件：用于跨服务调用 notify
	caller := client.New(rdb, client.Config{})
	defer caller.Close()

	h, err := host.NewHost(rdb, host.Config{
		Service: "orders",
		Addr:    getenv("ORDERS_ADDR", "127.0.0.1:9001"),
		// RedisWait: 允许服务先于 Redis 启动，最多等待 30s 后自动注册
		RedisWait: 30 * time.Second,
	})
	if err != nil {
		log.Error("new host", "error", err)
		os.Exit(1)
	}

	h.Handle("CreateOrder", func(ctx context.Context, payload []byte) ([]byte, error) {
		var req createOrderReq
		if err := json.Unmarshal(payload, &req); err != nil {
			return nil, status.Error(codes.InvalidArgument, "bad request: "+err.Error())
		}
		if req.Item == "" || req.Qty <= 0 {
			return nil, status.Error(codes.InvalidArgument, "item and qty>0 are required")
		}

		order := map[string]any{
			"id":    fmt.Sprintf("ORD-%05d", rand.Intn(100000)),
			"item":  req.Item,
			"qty":   req.Qty,
			"total": req.Qty * 100,
		}

		// —— 跨服务调用：通过插件调用 notify 服务 ——
		emailJSON, _ := json.Marshal(map[string]any{
			"to":      req.Email,
			"subject": "订单已创建",
			"body":    fmt.Sprintf("您的订单 %v 已创建，金额 %v", order["id"], order["total"]),
		})
		emailResp, err := caller.Call(ctx, "notify", "SendEmail", emailJSON, map[string]string{
			"trace_id": fmt.Sprintf("trace-%v", order["id"]), // 元数据透传示例
		})
		if err != nil {
			// 通知失败不回滚订单，把错误带给调用方参考
			order["email"] = map[string]any{"error": status.Convert(err).Message()}
			log.Warn("notify call failed", "error", err)
		} else {
			var emailOut map[string]any
			_ = json.Unmarshal(emailResp, &emailOut)
			order["email"] = emailOut
		}

		out, _ := json.Marshal(order)
		return out, nil
	})

	h.Handle("GetOrder", func(ctx context.Context, payload []byte) ([]byte, error) {
		var req getOrderReq
		if err := json.Unmarshal(payload, &req); err != nil {
			return nil, status.Error(codes.InvalidArgument, "bad request: "+err.Error())
		}
		if req.ID == "" {
			return nil, status.Error(codes.InvalidArgument, "id is required")
		}
		// 模拟查库
		return json.Marshal(map[string]any{
			"id": req.ID, "item": "拿铁咖啡", "qty": 2, "total": 200,
		})
	})

	log.Info("order-svc starting")
	if err := h.Serve(ctx); err != nil {
		log.Error("order-svc exited with error", "error", err)
		os.Exit(1)
	}
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
