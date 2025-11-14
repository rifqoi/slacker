package slacker

import "errors"

var (
	// ErrStepEventNotFound is returned when no event payload for a step
	ErrStepEventNotFound  = errors.New("step-event-not-found")
	ErrInvalidOnStart     = errors.New("invalid-on-start")
	ErrInvalidSlackerStep = errors.New("invalid-slacker-step")
	ErrInvalidPrefix      = errors.New("invalid-prefix")
)
