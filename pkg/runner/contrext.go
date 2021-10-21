package runner

import "context"

type Request struct {
	PipelineDef *PipelineDef
	Context     *BuildContext
}

func NewBuildContext(vals BCEntries) *BuildContext {
	bc := &BuildContext{
		ctx: context.Background(),
	}
	return bc.WithValues(vals)
}

type BuildContext struct {
	ctx context.Context
}

type BCEntries map[string]interface{}

func (c *BuildContext) WithValue(key string, val interface{}) *BuildContext {
	c.ctx = context.WithValue(c.ctx, key, val)
	return c
}

func (c *BuildContext) WithValues(vals BCEntries) *BuildContext {
	for key, val := range vals {
		c.ctx = context.WithValue(c.ctx, key, val)
	}
	return c
}

func (c *BuildContext) Get() context.Context {
	return c.ctx
}

func (c *BuildContext) GetVal(key string) interface{} {
	return c.ctx.Value(key)
}
