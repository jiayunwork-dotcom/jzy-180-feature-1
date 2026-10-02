package store

import (
	"errors"

	"sampling-svc/internal/plan"
)

// fieldMessenger is implemented by the inspection package's field errors.
type fieldMessenger interface {
	Error() string
}

// validationErr normalizes domain validation errors into plan.FieldError
// so the HTTP layer reports the offending field uniformly.
func validationErr(err error) error {
	if err == nil {
		return nil
	}
	// inspection.fieldErr exposes Error() only; map its text by type
	// assertion on the concrete message pair via a small adapter.
	type withFields interface {
		Fields() (string, string)
	}
	if wf, ok := err.(withFields); ok {
		f, m := wf.Fields()
		return plan.FieldError{Field: f, Message: m}
	}
	return err
}

// mapWriteErr passes through sentinel errors and wraps unknown ones.
func mapWriteErr(err error) error {
	if err == nil {
		return nil
	}
	var fe plan.FieldError
	if errors.As(err, &fe) {
		return fe
	}
	var ve plan.ValidationErrors
	if errors.As(err, &ve) {
		return ve
	}
	return err
}
