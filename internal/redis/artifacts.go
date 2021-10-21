package redis

import (
	"context"
	"github.com/go-redis/redis"
	"time"
)

func (c *client) SaveArtifact(pipelineUid, artifactName string, artifact []byte, retention time.Duration) error {
	ctx := context.TODO()
	pipe := c.rc.TxPipeline()
	pipe.HSet(ctx, artifactKey(pipelineUid), artifactName, artifact)
	pipe.Expire(ctx, artifactKey(pipelineUid), retention)
	_, err := pipe.Exec(ctx)
	return wrapErr(err)
}
func (c *client) GetArtifact(pipelineUid, artifactName string) ([]byte, error) {
	ctx := context.TODO()
	res, err := c.rc.HGet(ctx, artifactKey(pipelineUid), artifactName).Result()
	if err != nil && err != redis.Nil {
		return nil, wrapErr(err)
	} else {
		if err == redis.Nil {
			return nil, nil
		}
	}
	return []byte(res), nil
}
func (c *client) ListArtifacts(pipelineUid string) ([]string, error) {
	ctx := context.TODO()
	res, err := c.rc.HKeys(ctx, artifactKey(pipelineUid)).Result()
	if err != nil {
		return nil, wrapErr(err)
	}
	return res, nil
}

func (c *client) GetArtifacts(pipelineUid string) (map[string][]byte, error) {
	list, err := c.ListArtifacts(pipelineUid)
	if err != nil {
		return nil, err
	}
	artifacts := map[string][]byte{}
	for _, afName := range list {
		data, err := c.GetArtifact(pipelineUid, afName)
		if err != nil {
			continue
		}
		artifacts[afName] = data
	}
	return artifacts, nil
}
