// Package vm embeds the KubeVirt VM service provider in the agent.
package vm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	vmapi "github.com/dcm-project/environment-agent/api/vm/v1alpha1"
	embutil "github.com/dcm-project/environment-agent/internal/embedded/util"
	"github.com/dcm-project/environment-agent/internal/routing"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	kubevirtv1 "kubevirt.io/api/core/v1"
)

// ServiceType is the embedded SP identifier for the KubeVirt VM service provider.
const ServiceType = "vm"

// Operations are the resource operations advertised at registration time.
// They mirror the createVM/getVM/listVMs/deleteVM operations declared by the
// VM capability contract in api/vm/v1alpha1.
var Operations = []string{"CREATE", "READ", "DELETE"}

// vmLifecycle is the subset of kubevirt.Client the embedded handler needs.
type vmLifecycle interface {
	GetVirtualMachine(ctx context.Context, vmID string) (*kubevirtv1.VirtualMachine, error)
	CreateVirtualMachine(ctx context.Context, vm *kubevirtv1.VirtualMachine) (*kubevirtv1.VirtualMachine, error)
	DeleteVirtualMachine(ctx context.Context, vmID string) error
}

type vmMapper interface {
	VMSpecToVirtualMachine(spec *vmapi.VMSpec, vmID string) (*kubevirtv1.VirtualMachine, error)
}

// vmHandler implements routing.EmbeddedHandler for in-process VM lifecycle.
type vmHandler struct {
	lifecycle vmLifecycle
	mapper    vmMapper
}

var _ routing.EmbeddedHandler = (*vmHandler)(nil)

// NewVMHandler creates an embedded VM handler.
func NewVMHandler(lifecycle vmLifecycle, mapper vmMapper) routing.EmbeddedHandler {
	if lifecycle == nil {
		panic("embedded VM handler: lifecycle must not be nil")
	}
	if mapper == nil {
		panic("embedded VM handler: mapper must not be nil")
	}
	return &vmHandler{lifecycle: lifecycle, mapper: mapper}
}

func (h *vmHandler) CreateResource(ctx context.Context, req routing.CreateResourceRequest) error {
	vmSpec, err := parseVMSpec(req.Spec)
	if err != nil {
		return &routing.SPResponseError{StatusCode: http.StatusBadRequest, Message: err.Error()}
	}

	vmID := req.ResourceID
	if existing, err := h.lifecycle.GetVirtualMachine(ctx, vmID); err == nil && existing != nil {
		return &routing.SPResponseError{
			StatusCode: http.StatusConflict,
			Message:    fmt.Sprintf("VM with instance ID %s already exists", vmID),
		}
	}

	virtualMachine, err := h.mapper.VMSpecToVirtualMachine(vmSpec, vmID)
	if err != nil {
		return &routing.SPResponseError{
			StatusCode: http.StatusBadRequest,
			Message:    fmt.Sprintf("failed to convert VM spec: %s", err.Error()),
		}
	}

	_, err = h.lifecycle.CreateVirtualMachine(ctx, virtualMachine)
	if err != nil {
		return mapKubeVirtError(err)
	}
	return nil
}

func (h *vmHandler) DeleteResource(ctx context.Context, req routing.DeleteResourceRequest) error {
	err := h.lifecycle.DeleteVirtualMachine(ctx, req.ResourceID)
	if err != nil {
		return mapKubeVirtError(err)
	}
	return nil
}

// mapKubeVirtError copies known Kubernetes statuses onto SPResponseError and keeps the original message.
func mapKubeVirtError(err error) error {
	var statusErr *apierrors.StatusError
	if errors.As(err, &statusErr) {
		switch statusErr.ErrStatus.Code {
		case http.StatusConflict, http.StatusUnprocessableEntity, http.StatusBadRequest, http.StatusNotFound:
			return &routing.SPResponseError{
				StatusCode: int(statusErr.ErrStatus.Code),
				Message:    err.Error(),
			}
		}
	}
	return &routing.SPResponseError{
		StatusCode: http.StatusInternalServerError,
		Message:    err.Error(),
	}
}

func parseVMSpec(raw json.RawMessage) (*vmapi.VMSpec, error) {
	payload, err := embutil.SpecJSON(raw)
	if err != nil {
		return nil, err
	}

	var spec vmapi.VMSpec
	if err := json.Unmarshal(payload, &spec); err != nil {
		return nil, fmt.Errorf("invalid VM spec: %w", err)
	}
	if spec.Metadata.Name == "" {
		return nil, fmt.Errorf("spec.metadata.name is required")
	}
	return &spec, nil
}
