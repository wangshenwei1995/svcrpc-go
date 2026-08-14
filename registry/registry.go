// Package registry 提供基于 Redis 的轻量服务注册与发现。
//
// 存储结构（前缀 svcrpc:）：
//   - svcrpc:svc:<服务名>  → SET，成员为实例 ID
//   - svcrpc:inst:<实例ID> → HASH{service, addr, meta}，带 TTL（由心跳续约）
//
// 实例心跳停止后 TTL 到期自动淘汰；Discover 还会过滤掉
// "集合里还有、但详情已过期"的僵尸 ID。
package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Instance 描述一个服务实例。
type Instance struct {
	ID      string            `json:"id"`      // 实例唯一 ID（如 hostname@addr）
	Service string            `json:"service"` // 服务名
	Addr    string            `json:"addr"`    // 可访问地址 host:port
	Meta    map[string]string `json:"meta"`    // 附加元数据（版本、区域等）
}

// Registry 是 Redis 注册中心的客户端。
type Registry struct {
	rdb *redis.Client
	ttl time.Duration // 实例有效期，由心跳续约
}

const (
	keyService = "svcrpc:svc:%s"  // SET：服务名 → 实例 ID 集合
	keyInst    = "svcrpc:inst:%s" // HASH：实例 ID → 实例详情
)

// New 创建注册中心客户端。ttl 为实例有效期，应大于服务端心跳间隔。
func New(rdb *redis.Client, ttl time.Duration) *Registry {
	if ttl <= 0 {
		ttl = 10 * time.Second
	}
	return &Registry{rdb: rdb, ttl: ttl}
}

// TTL 返回实例有效期。
func (r *Registry) TTL() time.Duration { return r.ttl }

// Register 注册一个实例：加入服务集合，并写入带 TTL 的实例详情。
func (r *Registry) Register(ctx context.Context, inst Instance) error {
	meta, err := json.Marshal(inst.Meta)
	if err != nil {
		return fmt.Errorf("registry: encode meta: %w", err)
	}
	pipe := r.rdb.TxPipeline()
	pipe.SAdd(ctx, fmt.Sprintf(keyService, inst.Service), inst.ID)
	pipe.HSet(ctx, fmt.Sprintf(keyInst, inst.ID),
		"service", inst.Service,
		"addr", inst.Addr,
		"meta", string(meta),
	)
	pipe.Expire(ctx, fmt.Sprintf(keyInst, inst.ID), r.ttl)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("registry: register %s/%s: %w", inst.Service, inst.ID, err)
	}
	return nil
}

// Heartbeat 续约实例有效期。
func (r *Registry) Heartbeat(ctx context.Context, id string) error {
	return r.rdb.Expire(ctx, fmt.Sprintf(keyInst, id), r.ttl).Err()
}

// Deregister 注销实例（优雅停机时调用）。
func (r *Registry) Deregister(ctx context.Context, inst Instance) error {
	pipe := r.rdb.TxPipeline()
	pipe.SRem(ctx, fmt.Sprintf(keyService, inst.Service), inst.ID)
	pipe.Del(ctx, fmt.Sprintf(keyInst, inst.ID))
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("registry: deregister %s/%s: %w", inst.Service, inst.ID, err)
	}
	return nil
}

// Discover 返回某服务的所有存活实例。
func (r *Registry) Discover(ctx context.Context, service string) ([]Instance, error) {
	ids, err := r.rdb.SMembers(ctx, fmt.Sprintf(keyService, service)).Result()
	if err != nil {
		return nil, fmt.Errorf("registry: list instances of %q: %w", service, err)
	}
	if len(ids) == 0 {
		return nil, nil
	}

	// 批量过滤已过期的实例
	pipe := r.rdb.Pipeline()
	alive := make([]*redis.IntCmd, len(ids))
	for i, id := range ids {
		alive[i] = pipe.Exists(ctx, fmt.Sprintf(keyInst, id))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("registry: check liveness: %w", err)
	}
	live := make([]string, 0, len(ids))
	for i, id := range ids {
		if alive[i].Val() == 1 {
			live = append(live, id)
		}
	}
	if len(live) == 0 {
		return nil, nil
	}

	// 批量读取实例详情
	pipe2 := r.rdb.Pipeline()
	details := make([]*redis.MapStringStringCmd, len(live))
	for i, id := range live {
		details[i] = pipe2.HGetAll(ctx, fmt.Sprintf(keyInst, id))
	}
	if _, err := pipe2.Exec(ctx); err != nil {
		return nil, fmt.Errorf("registry: load instances: %w", err)
	}

	out := make([]Instance, 0, len(live))
	for i, id := range live {
		m := details[i].Val()
		if len(m) == 0 || m["addr"] == "" {
			continue
		}
		var meta map[string]string
		_ = json.Unmarshal([]byte(m["meta"]), &meta)
		out = append(out, Instance{ID: id, Service: service, Addr: m["addr"], Meta: meta})
	}
	return out, nil
}
