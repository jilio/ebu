package main

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestShutdownOTelProvidersAttemptsEveryProvider(t *testing.T) {
	firstErr := errors.New("first shutdown")
	secondErr := errors.New("second shutdown")
	var calls []string

	err := shutdownOTelProviders(
		context.Background(),
		func(context.Context) error {
			calls = append(calls, "first")
			return firstErr
		},
		func(context.Context) error {
			calls = append(calls, "second")
			return secondErr
		},
	)

	if !reflect.DeepEqual(calls, []string{"first", "second"}) {
		t.Fatalf("shutdown calls = %v, want both providers in order", calls)
	}
	if !errors.Is(err, firstErr) || !errors.Is(err, secondErr) {
		t.Fatalf("shutdown error = %v, want both provider errors", err)
	}
}
