package redis

import (
	"context"
	"encoding/json"
	"github.com/go-redis/redis"
	"time"
)

func (c *client) AcquireLease(owner, pipelineUid string) (*PipelineLease, error) {
	// check lease first, reject request if lease does not exist
	ctx := context.TODO()
	mLease, err := c.rc.Get(ctx, leaseKey(pipelineUid)).Result()
	if err != nil {
		if err != redis.Nil {
			return nil, err
		} else {
			lease, mLease, err := newLease(owner, pipelineUid)
			if err != nil {
				return nil, err
			}
			return lease, wrapErr(c.rc.Set(ctx, leaseKey(pipelineUid), mLease, LEASE_DURATION).Err())
		}
	}
	lease := &PipelineLease{}
	err = json.Unmarshal([]byte(mLease), lease)
	if err != nil {
		return nil, err
	}
	return nil, LeaseExists
}

func (c *client) ExtendLease(owner, uid string) (*PipelineLease, error) {
	ctx := context.TODO()
	mLease, err := c.rc.Get(ctx, leaseKey(uid)).Result()
	if err != nil {
		if err != redis.Nil {
			return nil, wrapErr(err)
		} else {
			return nil, LeaseExpired
		}
	}
	lease := &PipelineLease{}
	err = json.Unmarshal([]byte(mLease), lease)
	if err != nil {
		return nil, err
	}
	if lease.Owner != owner {
		return nil, LeaseExists
	}
	lease.Expires = time.Now().Add(LEASE_DURATION)
	extendedLease, err := json.Marshal(lease)
	if err != nil {
		return nil, err
	}
	err = c.rc.Set(ctx, leaseKey(uid), extendedLease, LEASE_DURATION).Err()
	if err != nil {
		return nil, wrapErr(err)
	}
	return lease, nil
}

func (c *client) TerminateLease(leaseUid string) error {
	ctx := context.TODO()
	return wrapErr(c.rc.Del(ctx, leaseKey(leaseUid)).Err())
}

func newLease(owner, uid string) (*PipelineLease, string, error) {
	lease := &PipelineLease{
		PipelineUid: uid,
		Owner:       owner,
		Acquired:    time.Now(),
		Expires:     time.Now().Add(LEASE_DURATION),
	}
	mLease, err := json.Marshal(lease)
	if err != nil {
		return nil, "", err
	}
	return lease, string(mLease), nil
}
