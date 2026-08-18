package registry

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// testClient 创建指向测试 Redis 的客户端（可用 REDIS_ADDR 覆盖，默认 192.168.1.20:45157）。
func testClient(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "192.168.1.20:45157"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { rdb.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis %s 不可达（可用 REDIS_ADDR 覆盖）: %v", addr, err)
	}
	return rdb
}

// uniqueService 生成带时间戳的唯一服务名，避免历史残留/并行干扰。
func uniqueService(t *testing.T) string {
	t.Helper()
	return "test-" + fmt.Sprintf("%d", time.Now().UnixNano())
}

func TestRegisterDiscoverDeregister(t *testing.T) {
	rdb := testClient(t)
	reg := New(rdb, 10*time.Second)
	ctx := context.Background()
	svc := uniqueService(t)

	inst := Instance{
		ID:      svc + "-1",
		Service: svc,
		Addr:    "10.0.0.1:9001",
		Meta:    map[string]string{"ver": "1.0"},
	}
	if err := reg.Register(ctx, inst); err != nil {
		t.Fatalf("register: %v", err)
	}
	t.Cleanup(func() { _ = reg.Deregister(context.Background(), inst) })

	got, err := reg.Discover(ctx, svc)
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if len(got) != 1 || got[0].Addr != inst.Addr || got[0].Meta["ver"] != "1.0" {
		t.Fatalf("discover = %+v, want 1 instance with addr/meta", got)
	}

	if err := reg.Deregister(ctx, inst); err != nil {
		t.Fatalf("deregister: %v", err)
	}
	got, err = reg.Discover(ctx, svc)
	if err != nil {
		t.Fatalf("discover after deregister: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("after deregister, discover = %+v, want none", got)
	}
}

// TestHeartbeatMissingInstanceReturnsError 验证 Bug A 修复：
// 对已不存在的实例做心跳必须返回错误（此前 Expire 静默成功，服务"假活"）。
func TestHeartbeatMissingInstanceReturnsError(t *testing.T) {
	rdb := testClient(t)
	reg := New(rdb, 10*time.Second)
	ctx := context.Background()
	id := "test-heartbeat-missing-" + fmt.Sprintf("%d", time.Now().UnixNano())

	_ = rdb.Del(ctx, fmt.Sprintf(keyInst, id)) // 确保实例详情不存在
	if err := reg.Heartbeat(ctx, id); err == nil {
		t.Fatal("heartbeat of non-existent instance: want error, got nil")
	}
}

// TestHeartbeatExtendsTTL 验证正常心跳能续约 TTL。
func TestHeartbeatExtendsTTL(t *testing.T) {
	rdb := testClient(t)
	reg := New(rdb, 3*time.Second) // 短 TTL 便于测试
	ctx := context.Background()
	svc := uniqueService(t)
	inst := Instance{ID: svc + "-1", Service: svc, Addr: "10.0.0.1:9001"}
	if err := reg.Register(ctx, inst); err != nil {
		t.Fatalf("register: %v", err)
	}
	t.Cleanup(func() { _ = reg.Deregister(context.Background(), inst) })

	time.Sleep(1 * time.Second) // 等 TTL 消耗一部分
	if err := reg.Heartbeat(ctx, inst.ID); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	ttl := rdb.TTL(ctx, fmt.Sprintf(keyInst, inst.ID)).Val()
	if ttl < 2*time.Second {
		t.Fatalf("after heartbeat, ttl = %v, want >= 2s (refreshed)", ttl)
	}
}

// TestDiscoverCleansZombieIDs 验证 Bug B 修复：
// 集合里"详情已过期"的僵尸 ID 会在 Discover 时被顺带清理，防止集合无限膨胀。
func TestDiscoverCleansZombieIDs(t *testing.T) {
	rdb := testClient(t)
	reg := New(rdb, 30*time.Second)
	ctx := context.Background()
	svc := uniqueService(t)
	live := Instance{ID: svc + "-live", Service: svc, Addr: "10.0.0.2:9001"}
	dead := Instance{ID: svc + "-dead", Service: svc, Addr: "10.0.0.3:9001"}
	if err := reg.Register(ctx, live); err != nil {
		t.Fatalf("register live: %v", err)
	}
	if err := reg.Register(ctx, dead); err != nil {
		t.Fatalf("register dead: %v", err)
	}
	t.Cleanup(func() {
		_ = reg.Deregister(context.Background(), live)
		// 清理空集合 key，避免测试在共享 Redis 上残留
		_ = rdb.Del(context.Background(), fmt.Sprintf(keyService, svc))
	})

	// 模拟崩溃残留：直接删掉 dead 的实例详情（不执行 Deregister）
	if err := rdb.Del(ctx, fmt.Sprintf(keyInst, dead.ID)).Err(); err != nil {
		t.Fatalf("del dead inst: %v", err)
	}
	if in := rdb.SIsMember(ctx, fmt.Sprintf(keyService, svc), dead.ID).Val(); !in {
		t.Fatal("precondition: dead id should still be in service set")
	}

	got, err := reg.Discover(ctx, svc)
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if len(got) != 1 || got[0].ID != live.ID {
		t.Fatalf("discover = %+v, want only live instance", got)
	}
	if in := rdb.SIsMember(ctx, fmt.Sprintf(keyService, svc), dead.ID).Val(); in {
		t.Fatal("zombie id still in service set after Discover")
	}
}
