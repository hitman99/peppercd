package redis

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/go-redis/redis/v8"
	"reflect"
	"time"
)

func (c *client) GetPipelines(targets []interface{}) error {
	ctx := context.TODO()
	uids, err := c.rc.LRange(ctx, PIPELINES_KEY, 0, -1).Result()
	if err != nil {
		return wrapErr(err)
	}
	pipeRes := []*redis.StringCmd{}
	pipe := c.rc.TxPipeline()
	for _, uuid := range uids {
		pipeRes = append(pipeRes, pipe.Get(ctx, pipelineKey(uuid)))
	}
	_, err = pipe.Exec(ctx)
	if err != nil {
		return wrapErr(err)
	}
	t := reflect.TypeOf(targets).Elem()
	for _, pr := range pipeRes {
		val := reflect.Zero(t).Interface()
		err := json.Unmarshal([]byte(pr.Val()), &val)
		if err != nil {
			return err
		}
		targets = append(targets, val)
	}
	return nil
}

func (c *client) GetPipeline(uid string, target interface{}) error {
	ctx := context.TODO()
	if reflect.ValueOf(target).IsNil() {
		return errors.New("target cannot be nil pointer")
	}
	res, err := c.rc.Get(ctx, pipelineKey(uid)).Result()
	if err != nil {
		return wrapErr(err)
	}
	return json.Unmarshal([]byte(res), target)
}

// save pipeline and extend the existing lease
func (c *client) SavePipeline(uid string, data interface{}) (*PipelineLease, error) {
	ctx := context.TODO()
	// check lease first, reject request if lease does not exist
	mLease, err := c.rc.Get(ctx, leaseKey(uid)).Result()
	if err != nil {
		if err != redis.Nil {
			return nil, err
		} else {
			return nil, LeaseNotFound
		}
	}
	lease := &PipelineLease{}
	err = json.Unmarshal([]byte(mLease), lease)
	if err != nil {
		return nil, err
	}
	//
	if lease.Expires.Before(time.Now()) {
		return nil, LeaseExpired
	}
	// extend lease duration
	lease.Expires = time.Now().Add(LEASE_DURATION)
	marshaled, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	extendedLease, err := json.Marshal(lease)
	if err != nil {
		return nil, err
	}
	pipe := c.rc.TxPipeline()
	pipe.Set(ctx, pipelineKey(uid), marshaled, 0)
	pipe.Set(ctx, leaseKey(uid), extendedLease, LEASE_DURATION)
	_, err = pipe.Exec(ctx)
	return lease, wrapErr(err)
}

func (c *client) CreatePipeline(uid, owner string, data interface{}) (*PipelineLease, error) {
	ctx := context.TODO()
	marshaled, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	// prepare lease
	lease, mLease, err := newLease(owner, uid)
	if err != nil {
		return nil, err
	}
	pipe := c.rc.TxPipeline()
	pipe.LPush(ctx, PIPELINES_KEY, uid)
	pipe.Set(ctx, pipelineKey(uid), marshaled, 0)
	pipe.Set(ctx, leaseKey(uid), mLease, LEASE_DURATION)
	_, err = pipe.Exec(ctx)
	return lease, wrapErr(err)
}
