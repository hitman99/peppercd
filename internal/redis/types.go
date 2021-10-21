package redis

import (
	"context"
	"errors"
	"fmt"
	goredis "github.com/go-redis/redis/v8"
	"time"
)

const (
	PIPELINES_KEY  = "pipelines"
	LEASE_DURATION = time.Minute
)

var (
	LeaseExpired  = errors.New("lease expired")
	LeaseNotFound = errors.New("lease not found")
	LeaseExists   = errors.New("lease already exists")
)

type RedisClient interface {
	TxPipeline() goredis.Pipeliner
	LRange(ctx context.Context, key string, start, stop int64) *goredis.StringSliceCmd
	Get(ctx context.Context, key string) *goredis.StringCmd
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *goredis.StatusCmd
	LPush(ctx context.Context, key string, values ...interface{}) *goredis.IntCmd
	Expire(ctx context.Context, key string, expiration time.Duration) *goredis.BoolCmd
	Del(ctx context.Context, keys ...string) *goredis.IntCmd
	HSet(ctx context.Context, key string, values ...interface{}) *goredis.IntCmd
	HGet(ctx context.Context, key, field string) *goredis.StringCmd
	HKeys(ctx context.Context, key string) *goredis.StringSliceCmd
}

type client struct {
	rc RedisClient
}

type PipelineLease struct {
	PipelineUid string    `json:"pipelineUid"`
	Owner       string    `json:"owner"`
	Acquired    time.Time `json:"acquired"`
	Expires     time.Time `json:"expires"`
}

type RedisError struct {
	nested error
}

func (e *RedisError) Error() string {
	return fmt.Sprintf("persistance error: %s", e.nested.Error())
}
