// Package client 提供微服务调用方：一行代码完成跨服务调用。
// 自动处理：Redis 服务发现（带缓存）、连接复用、随机负载均衡、
// 故障转移（Unavailable 时切换实例重试）、超时控制。
package client

import (
	"context"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	pb "svcrpc/gen/invoke"
	"svcrpc/registry"
)

// Config 是调用方配置。
type Config struct {
	Timeout      time.Duration // 单次尝试超时，默认 5s
	Retries      int           // 失败后额外重试次数（每次切换实例），默认 2
	DiscoveryTTL time.Duration // 服务发现结果缓存时长，默认 3s
	DialTimeout  time.Duration // 建立连接超时，默认 3s
	Logger       *slog.Logger
}

type cacheEntry struct {
	instances []registry.Instance
	expire    time.Time
}

// Client 是通用调用客户端（并发安全）。
type Client struct {
	rdb   *redis.Client
	reg   *registry.Registry
	cfg   Config
	log   *slog.Logger
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
	cache map[string]cacheEntry
}

// New 创建调用客户端。
func New(rdb *redis.Client, cfg Config) *Client {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}
	if cfg.Retries < 0 {
		cfg.Retries = 0
	}
	if cfg.DiscoveryTTL <= 0 {
		cfg.DiscoveryTTL = 3 * time.Second
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 3 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Client{
		rdb:   rdb,
		reg:   registry.New(rdb, 0),
		cfg:   cfg,
		log:   cfg.Logger,
		conns: map[string]*grpc.ClientConn{},
		cache: map[string]cacheEntry{},
	}
}

// Call 调用目标服务的某个方法。
//   - service: 目标服务名（注册时使用的名字）
//   - method:  目标方法名（服务端 Handle 注册的名字）
//   - payload: 请求体（bytes，建议 JSON）
//   - metadata: 透传元数据，可为 nil
//
// 返回响应体；错误为 gRPC status：服务端业务错误码原样透传，
// 无可用实例/连接失败返回 codes.Unavailable。
func (c *Client) Call(ctx context.Context, service, method string, payload []byte, metadata map[string]string) ([]byte, error) {
	insts, err := c.discover(ctx, service)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "discover service %q: %v", service, err)
	}
	if len(insts) == 0 {
		return nil, status.Errorf(codes.Unavailable, "no available instance for service %q", service)
	}

	// 随机起点遍历实例：天然负载均衡 + 故障转移。
	order := rand.Perm(len(insts))
	attempts := c.cfg.Retries + 1
	if attempts > len(insts) {
		attempts = len(insts)
	}

	var lastErr error
	for i := 0; i < attempts; i++ {
		inst := insts[order[i]]
		callCtx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
		out, err := c.invoke(callCtx, inst, method, payload, metadata)
		cancel()
		if err == nil {
			return out, nil
		}
		lastErr = err
		if status.Code(err) != codes.Unavailable {
			return nil, err // 业务错误（NotFound/InvalidArgument 等）不重试
		}
		c.log.Warn("call unavailable, failover to next instance",
			"service", service, "method", method, "instance", inst.Addr, "error", err)
	}
	return nil, lastErr
}

func (c *Client) invoke(ctx context.Context, inst registry.Instance, method string, payload []byte, metadata map[string]string) ([]byte, error) {
	conn, err := c.conn(ctx, inst.Addr)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "dial %s: %v", inst.Addr, err)
	}
	resp, err := pb.NewInvokerClient(conn).Call(ctx, &pb.CallRequest{
		Method:   method,
		Payload:  payload,
		Metadata: metadata,
	})
	if err != nil {
		return nil, err
	}
	return resp.GetPayload(), nil
}

// conn 返回到某地址的复用连接（懒建立、双检锁）。
func (c *Client) conn(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	if conn, ok := c.conns[addr]; ok {
		c.mu.Unlock()
		return conn, nil
	}
	c.mu.Unlock()

	dialCtx, cancel := context.WithTimeout(ctx, c.cfg.DialTimeout)
	defer cancel()
	conn, err := grpc.DialContext(dialCtx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.conns[addr]; ok {
		conn.Close()
		return existing, nil
	}
	c.conns[addr] = conn
	return conn, nil
}

// discover 返回服务实例列表，带短 TTL 缓存降低 Redis 压力。
func (c *Client) discover(ctx context.Context, service string) ([]registry.Instance, error) {
	c.mu.Lock()
	if e, ok := c.cache[service]; ok && time.Now().Before(e.expire) {
		insts := e.instances
		c.mu.Unlock()
		return insts, nil
	}
	c.mu.Unlock()

	insts, err := c.reg.Discover(ctx, service)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	c.cache[service] = cacheEntry{instances: insts, expire: time.Now().Add(c.cfg.DiscoveryTTL)}
	c.mu.Unlock()
	return insts, nil
}

// Close 关闭所有复用连接。
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, conn := range c.conns {
		conn.Close()
	}
	c.conns = map[string]*grpc.ClientConn{}
}
