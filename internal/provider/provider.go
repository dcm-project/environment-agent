// Package provider implements SP registration and management.
package provider

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"sync"

	"github.com/google/uuid"
)

var (
	providerIDRegex    = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)
	schemaVersionRegex = regexp.MustCompile(`^v[0-9]+(alpha|beta)?[0-9]*$`)
)

// ValidateName checks that name is non-empty (after trimming whitespace).
func ValidateName(name string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("must not be empty")
	}
	return nil
}

// ValidateServiceType checks that serviceType is non-empty (after trimming whitespace).
func ValidateServiceType(serviceType string) error {
	if strings.TrimSpace(serviceType) == "" {
		return fmt.Errorf("must not be empty")
	}
	return nil
}

// ValidateProviderID checks that id matches the AEP-122 resource ID pattern:
// lowercase alphanumeric and hyphens, 1-63 chars, must not start or end with hyphen.
func ValidateProviderID(id string) error {
	if id == "" {
		return fmt.Errorf("provider ID must not be empty")
	}
	if len(id) > 63 {
		return fmt.Errorf("provider ID must be at most 63 characters")
	}
	if id[0] == '-' || id[len(id)-1] == '-' {
		return fmt.Errorf("provider ID must not start or end with a hyphen")
	}
	if !providerIDRegex.MatchString(id) {
		return fmt.Errorf("provider ID must contain only lowercase alphanumeric characters and hyphens")
	}
	return nil
}

// ValidateSchemaVersion checks that version matches the schema version pattern
// (e.g., "v1", "v2alpha1", "v1beta3"). Major-only versions like "v1" are valid.
func ValidateSchemaVersion(version string) error {
	if version == "" {
		return fmt.Errorf("schema version must not be empty")
	}
	if !schemaVersionRegex.MatchString(version) {
		return fmt.Errorf("schema version must match format vN[alpha|beta]N (e.g. v1alpha1)")
	}
	return nil
}

// ValidateEndpoint checks that endpoint is a well-formed http(s) URL with a host
// and no query or fragment.
func ValidateEndpoint(endpoint string) error {
	u, err := url.Parse(endpoint)
	if err != nil {
		return fmt.Errorf("malformed URI")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("scheme must be http or https")
	}
	if u.Host == "" {
		return fmt.Errorf("missing host")
	}
	if u.RawQuery != "" {
		return fmt.Errorf("endpoint must not contain query parameters")
	}
	if u.Fragment != "" {
		return fmt.Errorf("endpoint must not contain a fragment")
	}
	return nil
}

// GenerateProviderID produces a new UUID v4 string suitable for use as a provider ID.
func GenerateProviderID() string {
	return uuid.New().String()
}

// Registry tracks which service type slot is occupied by which provider.
// One provider per service type per agent (REQ-SPR-200).
type Registry struct {
	mu    sync.RWMutex
	slots map[string]string
}

// NewRegistry creates an empty slot registry.
func NewRegistry() *Registry {
	return &Registry{slots: make(map[string]string)}
}

// Claim reserves serviceType for providerName.
// Returns an error identifying the current holder if the slot is already occupied by a different provider.
// Idempotent: re-claiming the same slot by the same provider succeeds.
func (r *Registry) Claim(providerName, serviceType string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if holder, ok := r.slots[serviceType]; ok && holder != providerName {
		return fmt.Errorf("service type '%s' is already served by provider '%s'", serviceType, holder)
	}
	r.slots[serviceType] = providerName
	return nil
}

// Move atomically releases oldType and claims newType for providerName.
// Returns an error if newType is occupied by a different provider, or if
// oldType is held by a different provider — the latter check prevents a
// registry/store desync from letting one provider's Move call delete a
// different provider's active slot. oldType remains held (or unoccupied,
// as it already was) on any error.
func (r *Registry) Move(providerName, oldType, newType string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if holder, ok := r.slots[newType]; ok && holder != providerName {
		return fmt.Errorf("service type '%s' is already served by provider '%s'", newType, holder)
	}
	if holder, ok := r.slots[oldType]; ok && holder != providerName {
		return fmt.Errorf("cannot move from service type '%s': it is held by a different provider '%s', not '%s'", oldType, holder, providerName)
	}
	delete(r.slots, oldType)
	r.slots[newType] = providerName
	return nil
}

// Release frees the given service type slot.
func (r *Registry) Release(serviceType string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.slots, serviceType)
}

// Lookup returns the provider holding serviceType, if any.
func (r *Registry) Lookup(serviceType string) (providerName string, occupied bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	providerName, occupied = r.slots[serviceType]
	return
}
