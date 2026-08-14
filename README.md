# svcrpc — 微服务调用插件（Go + gRPC + Redis）

让微服务之间互相调用像调用本地函数一样简单：**一行代码完成跨服务调用**，
不需要为每个服务编写 proto、生成客户端代码、手动管理连接和服务地址。

```
┌────────────┐   Call(ctx, "orders", "CreateOrder", json)
│ 调用方      │ ─────────────────────────────────────────────▶
│ (任意服务)  │                                                  │
└────────────┘                                                  ▼
                                                  ┌─────────────────────────┐
                                                  │ svcrpc 调用方插件 (client) │
                                                  │  · Redis 服务发现(缓存)    │
                                                  │  · 连接池复用             │
                                                  │  · 随机负载均衡            │
                                                  │  · 故障转移重试(Unavailable)│
                                                  └────────────┬────────────┘
                                                               │ gRPC 通用协议
                                                  ┌────────────▼────────────┐
                                                  │ 目标微服务 (host 插件)    │
                                                  │  · 暴露 Invoker.Call     │
                                                  │  · 自动注册+心跳(TTL淘汰)  │
                                                  │  · 按方法名分发到 handler  │
                                                  └─────────────────────────┘
                                                               ▲
                                            Redis 注册中心（svcrpc:svc:* / svcrpc:inst:*）
```

## 核心用法

### 被调用方（服务端）：3 步接入

```go
h, _ := host.NewHost(rdb, host.Config{Service: "orders", Addr: "0.0.0.0:9001"})

h.Handle("CreateOrder", func(ctx context.Context, payload []byte) ([]byte, error) {
    // payload 为请求体（建议 JSON），返回响应体
    return json.Marshal(map[string]any{"id": "ORD-1"})
})

h.Serve(ctx) // 阻塞：注册 + 心跳 + 优雅停机 + 注销
```

### 调用方：1 行调用

```go
c := client.New(rdb, client.Config{}) // 可全局复用
defer c.Close()

resp, err := c.Call(ctx, "orders", "CreateOrder", []byte(`{"item":"咖啡","qty":2}`), nil)
```

- 服务端用 `status.Error(codes.InvalidArgument, ...)` 返回的业务错误码**原样透传**给调用方；
- 目标服务无可用实例 / 连接失败返回 `codes.Unavailable`，调用方自动切换实例重试（默认 2 次）。

## 在你的项目中使用（接入指南）

svcrpc 是一个可依赖的 Go 库，你的业务服务只引入 `host`（被调用方）和/或 `client`（调用方）两个包，示例代码（examples/）不需要复制。

### 第 1 步：引入依赖

**本地开发（推荐先这样跑通）：** 在你的项目 `go.mod` 中加两行：

```
require svcrpc v0.0.0
replace svcrpc => ../svcrpc        // 相对路径，或写绝对路径 E:\go_test\svcrpc
```

然后 `go mod tidy`，代码里照常 `import "svcrpc/host"`。

**团队协作：** 把 svcrpc 推到 Git 仓库（GitHub/Gitee/私有 GitLab）：

```bash
cd E:\go_test\svcrpc
git init && git add . && git commit -m "svcrpc 微服务调用插件"
git remote add origin <你的仓库地址> && git push -u origin main
```

其他人用 `go get <仓库地址>/svcrpc` 引入（建议同时把 module 名改为仓库路径，如 `github.com/you/svcrpc`，import 路径随之变化，代码不用改）。

### 第 2 步：被调用方接入（你的服务提供方法给别人调）

```go
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"svcrpc/host"
)

// 你的业务层已有函数，直接复用
type CreateOrderReq struct {
	Item  string `json:"item"`
	Qty   int    `json:"qty"`
	Email string `json:"email"`
}

func main() {
	ctx, stop := signalNotifyContext() // 你的信号处理，参考 examples/order-svc
	defer stop()

	rdb := redis.NewClient(&redis.Options{Addr: os.Getenv("REDIS_ADDR")})

	h, _ := host.NewHost(rdb, host.Config{
		Service: "orders",        // 服务名：全局唯一，调用方按这个名字找到你
		Addr:    "0.0.0.0:9001",  // 监听地址
		// 容器/NAT 场景必须填调用方可达的地址，如 Pod IP：
		// AdvertiseAddr: "10.0.1.5:9001",
	})

	// 把业务方法包成 handler，一个服务可注册多个方法
	h.Handle("CreateOrder", func(ctx context.Context, payload []byte) ([]byte, error) {
		var req CreateOrderReq
		if err := json.Unmarshal(payload, &req); err != nil {
			return nil, status.Error(codes.InvalidArgument, "bad request: "+err.Error())
		}
		order, err := YourService.CreateOrder(ctx, req) // 复用已有业务逻辑
		if err != nil {
			return nil, err // 建议返回 status.Error，业务码会透传给调用方
		}
		return json.Marshal(order)
	})

	// 阻塞运行：注册 + 心跳 + 优雅停机 + 注销
	if err := h.Serve(ctx); err != nil {
		slog.Error("serve failed", "error", err)
		os.Exit(1)
	}
}
```

### 第 3 步：调用方接入（你的服务调用别人）

```go
import "svcrpc/client"

// 进程内做成单例（并发安全），放包级变量或依赖注入容器
var callers = client.New(rdb, client.Config{Timeout: 5 * time.Second, Retries: 2})
defer callers.Close() // 进程退出时调用一次

// 业务代码里任意位置：
resp, err := callers.Call(ctx, "notify", "SendEmail", emailJSON, map[string]string{
	"trace_id": traceID, // 透传给对端，做链路追踪
})
if err != nil {
	// err 是 gRPC status：业务码原样透传；Unavailable 表示目标不可达（已自动重试过）
}
```

### 第 4 步：部署检查清单

| 事项 | 说明 |
|---|---|
| 启动顺序 | **服务之间无顺序要求**（注册与发现解耦）；唯一前提是 Redis。给 `host.Config` 设 `RedisWait`（如 30s）后，服务可先于 Redis 启动，Redis 就绪后自动完成注册 |
| Redis 地址 | 每个服务注入 `REDIS_ADDR` 环境变量 |
| 端口规划 | 每个服务固定一个端口，写进部署清单；确保调用方可访问该端口 |
| 容器场景 | `AdvertiseAddr` 必须填调用方可达的地址（Pod IP / 宿主机 IP），**不能**填 `0.0.0.0:9001` |
| 心跳参数 | 网络抖动大时调大 `InstanceTTL`（如 30s）与 `HeartbeatInterval`（如 5s），避免误判下线 |
| 服务命名 | 全局统一命名规范（如 `orders`、`notify`），一个服务一个名字，不要用实例名当服务名 |

### 第 5 步：团队规范建议

- **服务名登记**：新增服务名时在团队文档登记，避免冲突；
- **参数校验**：handler 入口统一校验并返回 `codes.InvalidArgument`；
- **链路追踪**：调用时把 trace_id 放进 metadata，对端从 `CallRequest.Metadata` 取出透传；
- **封装层**：调用同一服务的代码多了以后，在各自服务里封装一个 `api/orders.go` 包，
  内部调 `callers.Call`，业务代码只面对强类型函数，调用细节不扩散。

## 快速开始

前置：本机可用的 Redis。开 4 个终端：

```bash
# 终端 1：Redis（此处用项目内下载的 Windows 版）
.tools\redis\redis-server.exe --port 6379 --bind 127.0.0.1

# 终端 2：示例服务 B（被调用方）
go run ./examples/notify-svc          # 127.0.0.1:9002，方法 SendEmail

# 终端 3：示例服务 A（被调用方 + 调用方）
go run ./examples/order-svc           # 127.0.0.1:9001，方法 CreateOrder/GetOrder
                                      # CreateOrder 内部会再调用 notify.SendEmail

# 终端 4：演示客户端（调用方）
go run ./cmd/demo
```

预期输出（节选）：`CreateOrder` 返回 `email.status = "sent"`，
说明订单服务通过插件完成了对通知服务的跨服务调用；错误场景分别得到
`InvalidArgument` / `NotFound` / `Unavailable`。

## 设计要点

| 能力 | 实现 |
|---|---|
| 免生成客户端 | 全系统只有一份 `proto/invoke.proto`（Invoker.Call），任何语言都能实现该协议接入 |
| 服务注册 | 启动时 `SADD svcrpc:svc:<名> <ID>` + `HSET svcrpc:inst:<ID>`（详情带 TTL） |
| 心跳续约 | 默认 3s 一次 `EXPIRE`；失败自动重新注册 |
| 宕机淘汰 | 心跳停止 → TTL（默认 10s）到期 → Discover 过滤僵尸 ID |
| 负载均衡 | 每次调用随机起点遍历实例（`rand.Perm`），多实例下天然均匀 |
| 故障转移 | `Unavailable` 错误自动换下一个实例重试（默认 2 次），非可用性错误不重试 |
| 连接池 | 按目标地址复用 `grpc.ClientConn`（双检锁懒建立） |
| 发现缓存 | 结果缓存 3s，降低 Redis 压力 |
| 错误透传 | 服务端 `status.Error` 的业务码（NotFound/InvalidArgument/...）原样到达调用方 |
| 优雅停机 | SIGINT/SIGTERM → 排空在途请求 → 注销实例 |

## 配置

### 环境变量（示例服务）

| 变量 | 默认值 | 说明 |
|---|---|---|
| `REDIS_ADDR` | `127.0.0.1:6379` | Redis 地址 |
| `ORDERS_ADDR` | `127.0.0.1:9001` | order-svc 监听地址 |
| `NOTIFY_ADDR` | `127.0.0.1:9002` | notify-svc 监听地址 |

### 插件参数（代码配置）

- `host.Config`：`Service`（必填）、`Addr`（必填）、`AdvertiseAddr`（容器/NAT 覆盖）、
  `HeartbeatInterval`（默认 3s）、`InstanceTTL`（默认 10s）、
  `RedisWait`（启动时等待 Redis 就绪的最长时间；0 表示不等待、Redis 不可达即失败退出）
- `client.Config`：`Timeout`（默认 5s）、`Retries`（默认 2）、`DiscoveryTTL`（默认 3s）、`DialTimeout`（默认 3s）

## 目录结构

```
svcrpc/
├── proto/invoke.proto        # 通用调用协议（唯一契约）
├── gen/invoke/               # protoc 生成的代码
├── registry/                 # Redis 注册与发现（注册/心跳/注销/发现）
├── host/                     # 服务宿主插件（Invoker 实现 + 自动注册心跳）
├── client/                   # 调用方插件（发现/连接池/均衡/故障转移）
├── examples/notify-svc/      # 示例：被调用方
├── examples/order-svc/       # 示例：被调用方 + 调用方（演示嵌套调用）
├── cmd/demo/                 # 示例：纯调用方
└── scripts/gen.ps1           # 修改 proto 后重新生成
```

## 限制与扩展方向

- 目前仅 **unary 调用**；需要流式（订阅/上传下载）可扩展 `Invoker.Stream` 双向流。
- 明文传输；生产环境建议叠加 TLS/mTLS 与鉴权拦截器（`grpc.WithTransportCredentials` 接入点已留好）。
- 负载均衡为随机策略；需要权重/最小连接数/一致性哈希可扩展 `client` 的选择器。
- 注册中心依赖 Redis 单点；大规模场景可换成 etcd/Nacos，`registry` 接口已隔离。
- 单条消息受 gRPC 默认 4MB 上限约束。

## 排障：模块下载损坏（HTTP/2 被中间设备破坏）

部分网络环境下（尤其国内网络），`go` 通过 goproxy.cn 等代理以 **HTTP/2** 下载模块时，
响应内容可能被中间设备零填充，表现为：

```
verifying xxx@vX.Y.Z: zip: not a valid zip file
read ...\xxx.go: unexpected NUL in input
```

解决办法：强制 Go 使用 HTTP/1.1 重新下载并清缓存：

```powershell
$env:GODEBUG = 'http2client=0'
go clean -modcache
go mod tidy
```
