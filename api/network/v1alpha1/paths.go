package v1alpha1

import "fmt"

// PostPath returns the first path that defines a POST operation in the
// embedded OpenAPI specification. It is used to derive endpoint suffixes
// from the spec rather than hardcoding them.
func PostPath() (string, error) {
	spec, err := GetSwagger()
	if err != nil {
		return "", fmt.Errorf("loading OpenAPI spec: %w", err)
	}
	for _, p := range spec.Paths.InMatchingOrder() {
		if spec.Paths.Value(p).Post != nil {
			return p, nil
		}
	}
	return "", fmt.Errorf("no POST path found in OpenAPI spec")
}
