package slacker

type SlackerError struct {
	s string
}

func (s SlackerError) Error() string {
	return s.s
}

var (
	ErrStepEventNotFound = SlackerError{"step-event-not-found"}
)
