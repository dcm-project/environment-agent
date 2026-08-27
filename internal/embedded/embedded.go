// Package embedded wires in-process OpenShift service providers into the agent.
package embedded

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/dcm-project/environment-agent/internal/config"
	"github.com/dcm-project/environment-agent/internal/embedded/cluster"
	"github.com/dcm-project/environment-agent/internal/embedded/container"
	"github.com/dcm-project/environment-agent/internal/embedded/storage"
	"github.com/dcm-project/environment-agent/internal/embedded/vm"
	"github.com/dcm-project/environment-agent/internal/health/monitor"
	"github.com/dcm-project/environment-agent/internal/routing"
)

// Bundles holds optional embedded SP runtimes keyed by service type wiring.
type Bundles struct {
	Cluster   *cluster.Bundle
	Container *container.Bundle
	Storage   *storage.Bundle
	VM        *vm.Bundle
}

// Setup constructs all embedded SPs enabled in agent configuration.
func Setup(ctx context.Context, agentCfg *config.Config, logger *slog.Logger) (*Bundles, error) {
	clusterBundle, err := cluster.Setup(ctx, agentCfg, logger)
	if err != nil {
		return nil, fmt.Errorf("cluster embedded setup: %w", err)
	}
	containerBundle, err := container.Setup(ctx, agentCfg, logger)
	if err != nil {
		if clusterBundle != nil {
			_ = clusterBundle.Close()
		}
		return nil, fmt.Errorf("container embedded setup: %w", err)
	}
	storageBundle, err := storage.Setup(ctx, agentCfg, logger)
	if err != nil {
		if clusterBundle != nil {
			_ = clusterBundle.Close()
		}
		if containerBundle != nil {
			_ = containerBundle.Close()
		}
		return nil, fmt.Errorf("storage embedded setup: %w", err)
	}
	vmBundle, err := vm.Setup(ctx, agentCfg, logger)
	if err != nil {
		if clusterBundle != nil {
			_ = clusterBundle.Close()
		}
		if containerBundle != nil {
			_ = containerBundle.Close()
		}
		if storageBundle != nil {
			_ = storageBundle.Close()
		}
		return nil, fmt.Errorf("vm embedded setup: %w", err)
	}
	return &Bundles{
		Cluster:   clusterBundle,
		Container: containerBundle,
		Storage:   storageBundle,
		VM:        vmBundle,
	}, nil
}

// Handlers returns forwarder handlers for all enabled embedded SP types.
func Handlers(b *Bundles) map[string]routing.EmbeddedHandler {
	if b == nil {
		return nil
	}
	handlers := make(map[string]routing.EmbeddedHandler)
	if b.Cluster != nil && b.Cluster.Handler != nil {
		handlers[cluster.ServiceType] = b.Cluster.Handler
	}
	if b.Container != nil && b.Container.Handler != nil {
		handlers[container.ServiceType] = b.Container.Handler
	}
	if b.Storage != nil && b.Storage.Handler != nil {
		handlers[storage.ServiceType] = b.Storage.Handler
	}
	if b.VM != nil && b.VM.Handler != nil {
		handlers[vm.ServiceType] = b.VM.Handler
	}
	if len(handlers) == 0 {
		return nil
	}
	return handlers
}

// Checkers returns health checkers for all enabled embedded SP types.
func Checkers(b *Bundles) map[string]monitor.Checker {
	if b == nil {
		return nil
	}
	checkers := make(map[string]monitor.Checker)
	if b.Cluster != nil && b.Cluster.Checker != nil {
		checkers[cluster.ServiceType] = b.Cluster.Checker
	}
	if b.Container != nil && b.Container.Checker != nil {
		checkers[container.ServiceType] = b.Container.Checker
	}
	if b.Storage != nil && b.Storage.Checker != nil {
		checkers[storage.ServiceType] = b.Storage.Checker
	}
	if b.VM != nil && b.VM.Checker != nil {
		checkers[vm.ServiceType] = b.VM.Checker
	}
	if len(checkers) == 0 {
		return nil
	}
	return checkers
}

// Start launches background workers for all embedded bundles.
func (b *Bundles) Start(ctx context.Context) {
	if b == nil {
		return
	}
	if b.Cluster != nil {
		b.Cluster.Start(ctx)
	}
	if b.Container != nil {
		b.Container.Start(ctx)
	}
	if b.Storage != nil {
		b.Storage.Start(ctx)
	}
	if b.VM != nil {
		b.VM.Start(ctx)
	}
}

// Close releases resources for all embedded bundles.
func (b *Bundles) Close() error {
	if b == nil {
		return nil
	}
	var err error
	if b.Cluster != nil {
		err = joinClose(err, b.Cluster.Close())
	}
	if b.Container != nil {
		err = joinClose(err, b.Container.Close())
	}
	if b.Storage != nil {
		err = joinClose(err, b.Storage.Close())
	}
	if b.VM != nil {
		err = joinClose(err, b.VM.Close())
	}
	return err
}

func joinClose(first, next error) error {
	return errors.Join(first, next)
}
