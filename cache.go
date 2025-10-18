package slacker

import (
	"context"
	"fmt"
	"time"

	"github.com/patrickmn/go-cache"
)

type slackerCache map[string]SlackerContextPayload

// cache key is "pipeline_name:slacker_event_id"
func buildSlackerCache(c *cache.Cache, ctx context.Context, pipelineName, stepName string, payload SlackerContextPayload) {
	key := fmt.Sprintf("%s:%s", pipelineName, ctx.Value(SlackerEventIDKey).(string))
	// Get existing cache, then repopulate the cache with new payload
	existingCache := getSlackerCache(c, pipelineName, ctx)
	if existingCache == nil {

		slackerPayloadCache := map[string]SlackerContextPayload{
			stepName: payload,
		}

		c.Set(key, slackerPayloadCache, 1*time.Minute)
	}

	ca := *existingCache

	ca[stepName] = payload

	c.Replace(key, ca, 1*time.Minute)
}

func getSlackerCache(c *cache.Cache, pipelineName string, ctx context.Context) *slackerCache {
	// Get existing cache
	key := buildCacheKey(ctx, pipelineName)

	data, ok := c.Get(key)
	if !ok {
		return nil
	}

	payload := data.(slackerCache)

	return &payload

}

func buildCacheKey(ctx context.Context, pipelineName string) string {
	return fmt.Sprintf("%s:%s", pipelineName, ctx.Value(SlackerEventIDKey).(string))
}
