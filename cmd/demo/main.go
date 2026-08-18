// demo 演示"调用方"：通过插件调用 orders 服务（不写任何 gRPC 代码）。
//
// 运行: go run ./cmd/demo
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/status"

	"svcrpc/client"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	slog.SetDefault(log)

	rdb := redis.NewClient(&redis.Options{Addr: getenv("REDIS_ADDR", "192.168.1.20:45157")})
	defer rdb.Close()

	// 一行创建调用方插件
	c := client.New(rdb, client.Config{Timeout: 5 * time.Second, Retries: 2})
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 1. 普通跨服务调用
	fmt.Println("== 1. 调用 orders.GetOrder ==")
	resp, err := c.Call(ctx, "orders", "GetOrder", []byte(`{"id":"ORD-00001"}`), nil)
	mustPrint(resp, err)

	// 2. 跨服务调用，且目标服务内部再调用 notify（两层链路）
	fmt.Println("== 2. 调用 orders.CreateOrder（内部会再调用 notify.SendEmail）==")
	resp, err = c.Call(ctx, "orders", "CreateOrder",
		[]byte(`{"item":"拿铁","qty":2,"email":"user@example.com"}`),
		map[string]string{"source": "demo"})
	mustPrint(resp, err)

	// 3. 错误传播：服务端业务错误码原样带回
	fmt.Println("== 3. 调用 orders.GetOrder 但缺少参数（应得到 InvalidArgument）==")
	_, err = c.Call(ctx, "orders", "GetOrder", []byte(`{}`), nil)
	fmt.Printf("error: %v\n", err)

	// 4. 调用不存在的方法（应得到 NotFound）
	fmt.Println("== 4. 调用不存在的方法 orders.Nope（应得到 NotFound）==")
	_, err = c.Call(ctx, "orders", "Nope", nil, nil)
	fmt.Printf("error: %v\n", err)

	// 5. 调用未注册的服务（应得到 Unavailable）
	fmt.Println("== 5. 调用未注册的服务 ghost.Do（应得到 Unavailable）==")
	_, err = c.Call(ctx, "ghost", "Do", nil, nil)
	fmt.Printf("error: %v\n", err)

	fmt.Println("\ndemo done")
}

func mustPrint(resp []byte, err error) {
	if err != nil {
		fmt.Printf("error: %v (code=%v)\n", err, status.Code(err))
		return
	}
	fmt.Printf("ok: %s\n", string(resp))
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
