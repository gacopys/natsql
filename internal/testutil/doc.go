// Package testutil provides shared test helpers for natsql packages.
//
// These helpers reduce duplication of embedded NATS setup and stream
// creation across test packages. All natsql test packages should use
// these instead of duplicating the setup code.
//
// Usage:
//
//	nc, js := testutil.StartEmbeddedNATS(t)
//	testutil.CreateStream(t, ctx, js, "my-stream")
package testutil
