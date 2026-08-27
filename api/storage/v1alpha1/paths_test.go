package v1alpha1_test

import (
	"testing"

	v1alpha1 "github.com/dcm-project/environment-agent/api/storage/v1alpha1"
)

func TestPostPath(t *testing.T) {
	got, err := v1alpha1.PostPath()
	if err != nil {
		t.Fatalf("PostPath() returned unexpected error: %v", err)
	}
	if got == "" {
		t.Fatal("PostPath() returned empty string")
	}

	const want = "/api/v1alpha1/volumes"
	if got != want {
		t.Errorf("PostPath() = %q, want %q", got, want)
	}
}
