// Package registration handles DCM service provider registration and version watching.
package registration

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"sync"
	"time"

	spmv1alpha1 "github.com/dcm-project/service-provider-manager/api/v1alpha1/provider"
	spmclient "github.com/dcm-project/service-provider-manager/pkg/client/provider"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/dcm-project/environment-agent/api/acm/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/acmcluster/config"
)

var endpointSuffix = mustPostPath()

func mustPostPath() string {
	p, err := v1alpha1.PostPath()
	if err != nil {
		panic(fmt.Sprintf("registration: resolving endpoint path from OpenAPI spec: %v", err))
	}
	return p
}

// retryableError wraps an error to indicate the operation can be retried.
type retryableError struct {
	err error
}

func (e *retryableError) Error() string {
	return e.err.Error()
}

func (e *retryableError) Unwrap() error {
	return e.err
}

func isRetryable(err error) bool {
	var re *retryableError
	return errors.As(err, &re)
}

// Registrar handles DCM registration and version watching.
type Registrar struct {
	cfg               config.RegistrationConfig
	dcmClient         *spmclient.ClientWithResponses
	k8sClient         client.Client
	logger            *slog.Logger
	versionDiscoverer *VersionDiscoverer
	startOnce         sync.Once
	done              chan struct{}
}

// New creates a Registrar with the given dependencies.
func New(cfg config.RegistrationConfig, dcmClient *spmclient.ClientWithResponses, k8sClient client.Client, logger *slog.Logger, matrix CompatibilityMatrix) *Registrar {
	return &Registrar{
		cfg:               cfg,
		dcmClient:         dcmClient,
		k8sClient:         k8sClient,
		logger:            logger,
		versionDiscoverer: NewVersionDiscoverer(k8sClient, matrix),
		done:              make(chan struct{}),
	}
}

// Start launches registration in a background goroutine (non-blocking).
// Calling Start multiple times is safe; only the first call launches a goroutine.
func (r *Registrar) Start(ctx context.Context) {
	r.startOnce.Do(func() {
		go func() {
			defer close(r.done)
			r.run(ctx)
		}()
	})
}

// Done returns a channel that is closed when the registration goroutine exits.
func (r *Registrar) Done() <-chan struct{} {
	return r.done
}

// Register performs a single registration attempt with DCM.
func (r *Registrar) Register(ctx context.Context) error {
	versions, err := r.versionDiscoverer.DiscoverVersions(ctx)
	if err != nil {
		return fmt.Errorf("discovering versions: %w", err)
	}

	payload := r.buildPayload(versions)

	resp, err := r.dcmClient.CreateProviderWithResponse(ctx, nil, payload)
	if err != nil {
		return &retryableError{err: fmt.Errorf("calling DCM registry: %w", err)}
	}

	statusCode := resp.StatusCode()
	switch {
	case statusCode == http.StatusOK || statusCode == http.StatusCreated:
		return nil
	case statusCode >= 400 && statusCode < 500:
		return fmt.Errorf("registration rejected by DCM: status %d: %s", statusCode, truncateBody(resp.Body))
	default:
		return &retryableError{err: fmt.Errorf("registration failed: status %d: %s", statusCode, truncateBody(resp.Body))}
	}
}

func truncateBody(body []byte) string {
	const maxLen = 200
	if len(body) > maxLen {
		return string(body[:maxLen])
	}
	return string(body)
}

func (r *Registrar) buildPayload(versions []string) spmv1alpha1.Provider {
	ops := []string{"CREATE", "DELETE", "READ"}

	metadata := &spmv1alpha1.ProviderMetadata{}
	if r.cfg.ProviderRegion != "" {
		metadata.RegionCode = &r.cfg.ProviderRegion
	}
	if r.cfg.ProviderZone != "" {
		metadata.Zone = &r.cfg.ProviderZone
	}
	metadata.Set("supportedPlatforms", []string{"kubevirt", "baremetal"})
	metadata.Set("supportedProvisioningTypes", []string{"hypershift"})
	metadata.Set("kubernetesSupportedVersions", versions)

	endpoint, err := url.JoinPath(r.cfg.ProviderEndpoint, endpointSuffix)
	if err != nil {
		endpoint = r.cfg.ProviderEndpoint
		r.logger.Error("registration: joining endpoint and suffix", "error", err)
	}
	provider := spmv1alpha1.Provider{
		Name:          r.cfg.ProviderName,
		ServiceType:   "cluster",
		SchemaVersion: "v1alpha1",
		Endpoint:      endpoint,
		Operations:    &ops,
		Metadata:      metadata,
	}

	if r.cfg.ProviderDisplayName != "" {
		provider.DisplayName = &r.cfg.ProviderDisplayName
	}

	return provider
}

func (r *Registrar) registerWithRetry(ctx context.Context) bool {
	backoff := r.cfg.RegistrationInitialBackoff

	for {
		err := r.Register(ctx)
		if err == nil {
			r.logger.Info("registration successful")
			return true
		}

		if !isRetryable(err) {
			r.logger.Error("registration failed with non-retryable error", "error", err)
			return false
		}

		r.logger.Warn("registration failed, will retry",
			"error", err, "retry_in", backoff)

		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-timer.C:
		}

		backoff *= 2
		if backoff > r.cfg.RegistrationMaxBackoff {
			backoff = r.cfg.RegistrationMaxBackoff
		}
	}
}

func (r *Registrar) run(ctx context.Context) {
	if !r.registerWithRetry(ctx) {
		return
	}

	lastVersions, err := r.versionDiscoverer.DiscoverVersions(ctx)
	if err != nil {
		r.logger.Error("failed to get baseline versions after registration", "error", err)
		return
	}

	ticker := time.NewTicker(r.cfg.VersionCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			currentVersions, err := r.versionDiscoverer.DiscoverVersions(ctx)
			if err != nil {
				r.logger.Error("failed to discover versions during check", "error", err)
				continue
			}

			if slices.Equal(lastVersions, currentVersions) {
				continue
			}

			r.logger.Info("version change detected, re-registering",
				"previous", lastVersions, "current", currentVersions)
			ok := r.registerWithRetry(ctx)
			if !ok {
				r.logger.Error("failed to re-register after version change", "error", err)
				continue
			}
			lastVersions = currentVersions
		}
	}
}
