package queue

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// testRedis returns a client and unique queue name, or skips if Redis is down.
func testRedis(t testing.TB) (*redis.Client, string) {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     os.Getenv("REDIS_PASSWORD"),
		PoolSize:     32,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		t.Skipf("redis not available at %s: %v", addr, err)
	}
	name := fmt.Sprintf("qgo-bench-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupQueue(rdb, name)
		_ = rdb.Close()
	})
	return rdb, name
}

func cleanupQueue(rdb *redis.Client, name string) {
	ctx := context.Background()
	// Best-effort: drop list keys for this run. Idempotency keys expire in 24h.
	_ = rdb.Del(ctx, name, name+processingSuffix, name+dlqSuffix).Err()
}
