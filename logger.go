package slacker

import (
	"context"
	"log/slog"
)

type LoggerCtxKey struct{}

type ContextHandler struct {
	slog.Handler
}

// Handle adds contextual attributes to the Record before calling the underlying
// handler
func (h ContextHandler) Handle(ctx context.Context, r slog.Record) error {
	// Support two ways of storing context attributes:
	// - a slice of slog.Attr stored under slackerEventIDKey
	// - a string event id stored under slackerEventIDKey
	if attrs, ok := ctx.Value(slackerEventIDKey).([]slog.Attr); ok {
		for _, v := range attrs {
			r.AddAttrs(v)
		}
	} else if id, ok := ctx.Value(slackerEventIDKey).(string); ok && id != "" {
		// Attach event id using the key name
		r.AddAttrs(slog.String(slackerEventIDKey.String(), id))
	}

	return h.Handler.Handle(ctx, r)
}

// LoggerWithContext returns a new context with the provided logger
// Later, use slacker.LoggerFromContext to retrieve the logger
func LoggerWithContext(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, LoggerCtxKey{}, logger)
}

// slacker.LoggerFromContext retrieves the logger from the context
// Context with event ID is in the context
//
// Usage:
//
//	logger := slacker.LoggerFromContext(ctx)
//	logger.Info("message")
func LoggerFromContext(ctx context.Context) *slog.Logger {
	logger, ok := ctx.Value(LoggerCtxKey{}).(*slog.Logger)
	if !ok {
		return slog.Default()
	}
	return logger
}
