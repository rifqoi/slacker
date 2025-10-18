package slacker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/patrickmn/go-cache"
)

type slackerCache map[string]SlackerContextPayload

// cache key is "pipeline_name:slacker_event_id"
func buildSlackerCache(c *cache.Cache, ctx context.Context, pipelineName, stepName string, payload SlackerContextPayload) {
	key := buildCacheKey(ctx, pipelineName)
	// Get existing cache, then repopulate the cache with new payload
	existingCache := getSlackerCache(c, pipelineName, ctx)
	if existingCache == nil {

		slackerPayloadCache := slackerCache{
			stepName: payload,
		}

		c.Set(key, slackerPayloadCache, 10*time.Minute)

		return
	}

	ca := *existingCache

	ca[stepName] = payload
	slog.Info("replacing existing cache", "new_cache", ca, "old_cache", *existingCache)

	c.Replace(key, ca, 10*time.Minute)
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
