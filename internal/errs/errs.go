// Package errs defines the distinguishable error categories returned by
// the service: invalid parameters, capacity/backpressure rejection, and
// index temporarily unavailable.
package errs

import "errors"

// Kind classifies an error so callers (and tests) can distinguish the
// rejection reason.
type Kind int

const (
	KindInvalid     Kind = iota + 1 // parameter invalid (the request is never accepted)
	KindCapacity                    // capacity/concurrency limit exceeded; backpressure
	KindUnavailable                 // index/store temporarily unavailable (e.g. shutting down)
)

func (k Kind) String() string {
	switch k {
	case KindInvalid:
		return "invalid_parameter"
	case KindCapacity:
		return "capacity_exceeded"
	case KindUnavailable:
		return "index_unavailable"
	default:
		return "unknown"
	}
}

// Error is the typed service error carrying a machine-readable Kind.
type Error struct {
	Kind    Kind
	Message string
}

func (e *Error) Error() string { return e.Kind.String() + ": " + e.Message }

// New builds a typed error.
func New(kind Kind, msg string) *Error { return &Error{Kind: kind, Message: msg} }

// KindOf returns the Kind of a service error, or 0 if err is nil/not typed.
func KindOf(err error) Kind {
	if err == nil {
		return 0
	}
	var e *Error
	if errors.As(err, &e) {
		return e.Kind
	}
	return 0
}
