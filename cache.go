package slacker

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// StorePayload is the in-memory representation of stored step payloads for a pipeline.
// It's currently a map[string]SlackerContextPayload but may be converted to an
// interface in the future to support alternative storage backends.
type StorePayload map[string]SlackerContextPayload

// cache key is "pipeline_name:slacker_event_id"
func (sl *Slacker) storeStepPayload(ctx context.Context, pipelineName, stepName string, payload SlackerContextPayload) {
	key := pipelineCacheKey(ctx, pipelineName)
	// Get existing cache, then repopulate the cache with new payload
	existingCache := sl.loadPipelineStore(pipelineName, ctx)
	if existingCache == nil {

		pipelinePayloadCache := StorePayload{
			stepName: payload,
		}

		sl.cache.Set(key, pipelinePayloadCache, 10*time.Minute)

		// log creation of new pipeline cache entry
		sl.logger.DebugContext(ctx, "created new pipeline cache", slog.String("pipeline", pipelineName), slog.String("step", stepName))

		return
	}

	ca := *existingCache

	ca[stepName] = payload
	// Use debug-level structured logging with context
	sl.logger.DebugContext(ctx, "replacing existing cache", slog.String("key", key), slog.Any("new_cache", ca), slog.Any("old_cache", *existingCache))

	sl.cache.Replace(key, ca, 10*time.Minute)
}

func (sl *Slacker) loadPipelineStore(pipelineName string, ctx context.Context) *StorePayload {
	// Get existing cache
	key := pipelineCacheKey(ctx, pipelineName)

	data, ok := sl.cache.Get(key)
	if !ok {
		return nil
	}

	payload, ok := data.(StorePayload)
	if !ok {
		return nil
	}

	return &payload

}

func pipelineCacheKey(ctx context.Context, pipelineName string) string {
	slackerEventId, ok := ctx.Value(slackerEventIDKey).(string)

	if !ok {
		slackerEventId = ""
	}

	return fmt.Sprintf("%s:%s", pipelineName, slackerEventId)
}

func (sl *Slacker) isIdempotenceEvent(ctx context.Context, pipelineName string, stepName string) bool {

	eventID, ok := ctx.Value(slackerEventIDKey).(string)
	if !ok {
		return false
	}

	// pipelineName:stepName:eventID
	key := fmt.Sprintf("%s:%s:%s", pipelineName, stepName, eventID)
	if _, found := sl.cache.Get(key); found {
		sl.logger.DebugContext(ctx, "Idempotent Event", slog.String("key", key))
		return true
	}

	return false
}

func (sl *Slacker) setIdempotenceEvent(ctx context.Context, pipelineName string, stepName string) {

	eventID, ok := ctx.Value(slackerEventIDKey).(string)
	if !ok {
		return
	}

	// pipelineName:stepName:eventID
	key := fmt.Sprintf("%s:%s:%s", pipelineName, stepName, eventID)

	sl.cache.Set(key, true, 5*time.Minute)
	sl.logger.DebugContext(ctx, "setting up idempotence event", slog.String("key", key))
}
