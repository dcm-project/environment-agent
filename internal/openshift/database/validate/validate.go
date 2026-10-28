// Package validate provides request validation for database create operations.
package validate

import (
	"fmt"
	"sort"
	"strings"

	"github.com/dcm-project/environment-agent/api/database/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/database/dcm"
	"github.com/dcm-project/environment-agent/internal/openshift/database/store"
	"github.com/dcm-project/environment-agent/internal/openshift/database/units"
	"k8s.io/apimachinery/pkg/api/resource"
	k8svalidation "k8s.io/apimachinery/pkg/util/validation"
)

type validationError struct {
	Detail  string
	Pointer string
}

const (
	ptrDatabaseID   = "#/spec/id"
	ptrMetadataName = "#/spec/metadata/name"
	ptrCPUMin       = "#/spec/resources/cpu/min"
	ptrCPUMax       = "#/spec/resources/cpu/max"
	ptrMemMin       = "#/spec/resources/memory/min"
	ptrMemMax       = "#/spec/resources/memory/max"
	ptrStorage      = "#/spec/resources/storage"
)

func jsonPointerEscape(s string) string {
	s = strings.ReplaceAll(s, "~", "~0")
	s = strings.ReplaceAll(s, "/", "~1")
	return s
}

func labelPointer(key string) string {
	return "#/spec/metadata/labels/" + jsonPointerEscape(key)
}

func checkDatabaseID(id string) *validationError {
	if id == "health" {
		return &validationError{
			Detail:  fmt.Sprintf("database ID %q is reserved and cannot be used", id),
			Pointer: ptrDatabaseID,
		}
	}
	if msgs := k8svalidation.IsDNS1123Label(id); len(msgs) > 0 {
		return &validationError{
			Detail:  fmt.Sprintf("database ID %q is invalid: %s", id, strings.Join(msgs, "; ")),
			Pointer: ptrDatabaseID,
		}
	}
	return nil
}

func checkMetadataName(name string) *validationError {
	if msgs := k8svalidation.IsDNS1123Label(name); len(msgs) > 0 {
		return &validationError{
			Detail:  fmt.Sprintf("metadata.name %q is invalid: %s", name, strings.Join(msgs, "; ")),
			Pointer: ptrMetadataName,
		}
	}
	return nil
}

func checkResources(res v1alpha1.DatabaseResources) []validationError {
	var errs []validationError

	minCPU, minCPUErr := resource.ParseQuantity(res.Cpu.Min)
	if minCPUErr != nil {
		errs = append(errs, validationError{
			Detail:  fmt.Sprintf("invalid cpu.min %q: %v", res.Cpu.Min, minCPUErr),
			Pointer: ptrCPUMin,
		})
	} else if minCPU.Sign() == -1 {
		errs = append(errs, validationError{
			Detail:  fmt.Sprintf("cpu.min %q must not be negative", res.Cpu.Min),
			Pointer: ptrCPUMin,
		})
	}

	maxCPU, maxCPUErr := resource.ParseQuantity(res.Cpu.Max)
	if maxCPUErr != nil {
		errs = append(errs, validationError{
			Detail:  fmt.Sprintf("invalid cpu.max %q: %v", res.Cpu.Max, maxCPUErr),
			Pointer: ptrCPUMax,
		})
	} else if maxCPU.Sign() == -1 {
		errs = append(errs, validationError{
			Detail:  fmt.Sprintf("cpu.max %q must not be negative", res.Cpu.Max),
			Pointer: ptrCPUMax,
		})
	}
	if len(errs) == 0 && minCPU.Cmp(maxCPU) > 0 {
		errs = append(errs, validationError{
			Detail:  fmt.Sprintf("cpu.min (%s) must not exceed cpu.max (%s)", res.Cpu.Min, res.Cpu.Max),
			Pointer: ptrCPUMin,
		})
	}

	minMem, minErr := units.ConvertBytes(res.Memory.Min)
	if minErr != nil {
		errs = append(errs, validationError{
			Detail:  fmt.Sprintf("invalid memory.min %q: %v", res.Memory.Min, minErr),
			Pointer: ptrMemMin,
		})
	}
	maxMem, maxErr := units.ConvertBytes(res.Memory.Max)
	if maxErr != nil {
		errs = append(errs, validationError{
			Detail:  fmt.Sprintf("invalid memory.max %q: %v", res.Memory.Max, maxErr),
			Pointer: ptrMemMax,
		})
	}
	if minErr == nil && maxErr == nil && minMem.Cmp(maxMem) > 0 {
		errs = append(errs, validationError{
			Detail:  fmt.Sprintf("memory.min (%s) must not exceed memory.max (%s)", res.Memory.Min, res.Memory.Max),
			Pointer: ptrMemMin,
		})
	}

	storage, storageErr := units.ConvertBytes(res.Storage)
	if storageErr != nil || storage.String() == "0" {
		errs = append(errs, validationError{
			Detail:  fmt.Sprintf("invalid storage %q: %v", res.Storage, storageErr),
			Pointer: ptrStorage,
		})
	}

	return errs
}

func checkUserLabels(labels *map[string]string) []validationError {
	if labels == nil {
		return nil
	}

	keys := make([]string, 0, len(dcm.ReservedLabelKeys))
	for k := range dcm.ReservedLabelKeys {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var errs []validationError
	for _, k := range keys {
		if _, ok := (*labels)[k]; ok {
			errs = append(errs, validationError{
				Detail:  fmt.Sprintf("label %q is reserved by DCM and cannot be set by the user", k),
				Pointer: labelPointer(k),
			})
		}
	}
	return errs
}

// ValidateCreate checks Database ID, metadata name, resource ranges, memory format, and reserved labels.
func ValidateCreate(id string, spec v1alpha1.DatabaseSpec) error {
	if err := checkDatabaseID(id); err != nil {
		return &store.InvalidArgumentError{Message: err.Detail}
	}
	if err := checkMetadataName(spec.Metadata.Name); err != nil {
		return &store.InvalidArgumentError{Message: err.Detail}
	}

	errs := checkResources(spec.Resources)
	errs = append(errs, checkUserLabels(spec.Metadata.Labels)...)
	if len(errs) == 0 {
		return nil
	}
	return &store.InvalidArgumentError{Message: errs[0].Detail}
}
