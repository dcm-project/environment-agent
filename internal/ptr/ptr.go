// Package ptr provides generic pointer helper functions.
package ptr

// To returns a pointer to the given value.
func To[T any](v T) *T { return &v }
