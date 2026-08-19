// Package redisclient connects to Redis and provides helpers.
package redisclient

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// New returns a connected client, verified with a PING.
func New(ctx context.Context, addr string) (*redis.Client, error) {
	c := redis.NewClient(&redis.Options{Addr: addr})
	if err := c.Ping(ctx).Err(); err != nil {
		return nil, err
	}
	return c, nil
}

// TryLock attempts to acquire a distributed lock.
// Returns true if lock acquired, false if already held.
// The lock expires after ttl to prevent deadlock on crashes.
func TryLock(ctx context.Context, rdb *redis.Client, key string, ttl time.Duration) (bool, error) {
	// SET key value NX EX ttl - only set if not exists, with expiry
	ok, err := rdb.SetNX(ctx, key, "1", ttl).Result()
	if err != nil {
		return false, err
	}
	return ok, nil
}

// Unlock releases a distributed lock.
// Uses Lua script to only delete if value matches (prevents releasing someone else's lock).
func Unlock(ctx context.Context, rdb *redis.Client, key string) error {
	// Lua script: if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end
	script := redis.NewScript(`
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("del", KEYS[1])
		else
			return 0
		end
	`)
	_, err := script.Run(ctx, rdb, []string{key}, "1").Result()
	return err
}
