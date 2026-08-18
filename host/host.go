// Package host 提供微服务宿主：把本地 handler 注册为通用 Invoker
// gRPC 服务，并自动完成 Redis 注册、心跳续约与优雅注销。
// 一个服务只需要：NewHost + Handle + Serve 三件事。
package host

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	pb "github.com/wangshenwei1995/svcrpc-go/gen/invoke"
	"github.com/wangshenwei1995/svcrpc-go/registry"
)

// Handler 处理一个方法的通用调用。payload 为请求体（建议 JSON），返回值同理。
// 返回 error 时建议用 status.Error(...) 携带具体业务错误码，会被原样透传给调用方。
type Handler func(ctx context.Context, payload []byte) ([]byte, error)

// Config 是宿主的配置。
type Config struct {
	Service           string        // 服务名（注册与调用的唯一标识），必填
	Addr              string        // 监听地址，如 "0.0.0.0:9001"，必填
	AdvertiseAddr     string        // 注册到注册中心的地址；默认取 Addr（容器/NAT 场景需覆盖）
	InstanceID        string        // 实例 ID；默认 hostname@AdvertiseAddr
	HeartbeatInterval time.Duration // 心跳间隔，默认 3s
	InstanceTTL       time.Duration // 实例有效期，默认 10s（须大于心跳间隔）
	RedisWait         time.Duration // 启动时等待 Redis 就绪的最长时间；0 表示不等待、Redis 不可达即失败退出
	Logger            *slog.Logger
}

// Host 是服务宿主。
type Host struct {
	pb.UnimplementedInvokerServer

	cfg      Config
	rdb      *redis.Client
	reg      *registry.Registry
	log      *slog.Logger
	gs       *grpc.Server
	handlers map[string]Handler
	mu       sync.RWMutex
	inst     registry.Instance
}

// NewHost 创建服务宿主。
func NewHost(rdb *redis.Client, cfg Config) (*Host, error) {
	if cfg.Service == "" {
		return nil, fmt.Errorf("host: service name is required")
	}
	if cfg.Addr == "" {
		return nil, fmt.Errorf("host: addr is required")
	}
	if cfg.AdvertiseAddr == "" {
		cfg.AdvertiseAddr = cfg.Addr
	}
	if cfg.InstanceID == "" {
		hostname, _ := os.Hostname()
		cfg.InstanceID = fmt.Sprintf("%s@%s", hostname, cfg.AdvertiseAddr)
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 3 * time.Second
	}
	if cfg.InstanceTTL <= 0 {
		cfg.InstanceTTL = 10 * time.Second
	}
	if cfg.InstanceTTL <= cfg.HeartbeatInterval {
		return nil, fmt.Errorf("host: InstanceTTL (%s) must be greater than HeartbeatInterval (%s)",
			cfg.InstanceTTL, cfg.HeartbeatInterval)
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Host{
		cfg:      cfg,
		rdb:      rdb,
		reg:      registry.New(rdb, cfg.InstanceTTL),
		log:      cfg.Logger,
		handlers: map[string]Handler{},
	}, nil
}

// waitRedis 轮询等待 Redis 就绪（每 1s 重试），直到超时或 ctx 取消。
func (h *Host) waitRedis(ctx context.Context) error {
	deadline := time.Now().Add(h.cfg.RedisWait)
	for attempt := 1; ; attempt++ {
		err := h.rdb.Ping(ctx).Err()
		if err == nil {
			h.log.Info("redis ready", "service", h.cfg.Service)
			return nil
		}
		if time.Now().After(deadline) {
			return err
		}
		h.log.Warn("redis not ready, retrying",
			"service", h.cfg.Service, "attempt", attempt, "error", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// Handle 注册一个方法处理器。方法名即调用方 Call 请求中的 method。
func (h *Host) Handle(method string, fn Handler) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.handlers[method] = fn
}

// Call 实现通用调用入口：按方法名分发到本地处理器。
func (h *Host) Call(ctx context.Context, req *pb.CallRequest) (*pb.CallResponse, error) {
	h.mu.RLock()
	fn, ok := h.handlers[req.GetMethod()]
	h.mu.RUnlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "method %q not found on service %q",
			req.GetMethod(), h.cfg.Service)
	}
	out, err := fn(ctx, req.GetPayload())
	if err != nil {
		// status 错误原样透传（保留业务错误码），普通 error 包装为 Internal。
		if s, ok := status.FromError(err); ok && s.Code() != codes.Unknown {
			return nil, s.Err()
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.CallResponse{Payload: out, Metadata: req.GetMetadata()}, nil
}

// Serve 启动服务：监听端口、注册到 Redis、后台心跳续约；
// 阻塞直到 ctx 取消，随后优雅停机并注销实例。
// 若配置了 RedisWait > 0，Redis 未就绪时会轮询等待（服务可先于 Redis 启动）。
func (h *Host) Serve(ctx context.Context) error {
	h.inst = registry.Instance{
		ID:      h.cfg.InstanceID,
		Service: h.cfg.Service,
		Addr:    h.cfg.AdvertiseAddr,
	}

	if h.cfg.RedisWait > 0 {
		if err := h.waitRedis(ctx); err != nil {
			return fmt.Errorf("host: redis not ready: %w", err)
		}
	}

	lis, err := net.Listen("tcp", h.cfg.Addr)
	if err != nil {
		return fmt.Errorf("host: listen %s: %w", h.cfg.Addr, err)
	}
	if err := h.reg.Register(ctx, h.inst); err != nil {
		lis.Close()
		return fmt.Errorf("host: register %q: %w", h.cfg.Service, err)
	}
	h.log.Info("service registered",
		"service", h.cfg.Service, "instance", h.inst.ID, "addr", h.cfg.AdvertiseAddr)

	h.gs = grpc.NewServer()
	pb.RegisterInvokerServer(h.gs, h)
	reflection.Register(h.gs)

	// 心跳：失败时尝试重新注册，防止 TTL 到期被摘除。
	go func() {
		t := time.NewTicker(h.cfg.HeartbeatInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := h.reg.Heartbeat(ctx, h.inst.ID); err != nil {
					h.log.Warn("heartbeat failed, re-registering", "error", err)
					if err := h.reg.Register(ctx, h.inst); err != nil {
						h.log.Error("re-register failed", "error", err)
					}
				}
			}
		}
	}()

	errCh := make(chan error, 1)
	go func() { errCh <- h.gs.Serve(lis) }()
	h.log.Info("host serving", "service", h.cfg.Service, "addr", lis.Addr().String())

	select {
	case <-ctx.Done():
		h.log.Info("shutdown requested", "service", h.cfg.Service)
	case err := <-errCh:
		if err != nil {
			_ = h.reg.Deregister(context.Background(), h.inst)
			return fmt.Errorf("host: serve: %w", err)
		}
		return nil
	}

	done := make(chan struct{})
	go func() {
		h.gs.GracefulStop() // 排空在途请求
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		h.gs.Stop()
	}
	if err := h.reg.Deregister(context.Background(), h.inst); err != nil {
		h.log.Warn("deregister failed", "error", err)
	}
	h.log.Info("host stopped", "service", h.cfg.Service)
	return nil
}
