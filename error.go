package slacker

import "errors"

var (
	// ErrStepEventNotFound is returned when no event payload for a step
	ErrStepEventNotFound = errors.New("step-event-not-found")
)
