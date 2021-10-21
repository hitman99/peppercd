package redis

import (
	"fmt"
	"github.com/go-redis/redis/v8"
	"time"
)

type Client interface {
	GetPipelines(targets []interface{}) error
	GetPipeline(uid string, target interface{}) error
	SavePipeline(uid string, data interface{}) (*PipelineLease, error)
	CreatePipeline(uid, owner string, data interface{}) (*PipelineLease, error)

	AcquireLease(owner, pipelineUid string) (*PipelineLease, error)
	ExtendLease(owner, pipelineUid string) (*PipelineLease, error)
	TerminateLease(pipelineUid string) error

	SaveArtifact(pipelineUid, artifactName string, artifact []byte, retention time.Duration) error
	GetArtifact(pipelineUid, artifactName string) ([]byte, error)
	ListArtifacts(pipelineUid string) ([]string, error)
	GetArtifacts(pipelineUid string) (map[string][]byte, error)
}

func MustNewClient(isCluster bool, address string) Client {
	if isCluster == true {
		return &client{
			rc: redis.NewClusterClient(&redis.ClusterOptions{
				Addrs: []string{address},
			}),
		}
	} else {
		return &client{
			rc: redis.NewClient(&redis.Options{
				Addr: address,
			}),
		}
	}
}

func leaseKey(uuid string) string {
	return fmt.Sprintf("lease-%s", uuid)
}

func pipelineKey(uuid string) string {
	return fmt.Sprintf("pipeline-%s", uuid)
}

func artifactKey(pipeUid string) string {
	return fmt.Sprintf("af-%s", pipeUid)
}

func wrapErr(err error) error {
	if err != nil && err != redis.Nil {
		return &RedisError{nested: err}
	}
	return nil
}
