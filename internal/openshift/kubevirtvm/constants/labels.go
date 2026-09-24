// Package constants defines label keys for kubevirt VM resources.
package constants

// DCM label keys used throughout the project
const (
	// DCMLabelManagedBy indicates that a resource is managed by DCM
	DCMLabelManagedBy = "dcm.project/managed-by"

	// DCMLabelInstanceID contains the DCM instance ID for a resource
	DCMLabelInstanceID = "dcm.project/dcm-instance-id"

	// DCMManagedByValue is the value used for the managed-by label
	DCMManagedByValue = "dcm"
)

// DCM annotation keys used throughout the project
const (
	// DCMAnnotationResourceName preserves the resource name requested in the
	// DCM spec. VirtualMachines are created with a generated cluster name, so
	// this is the only way to report the requested name back to the caller.
	// An annotation rather than a label because DCM names are not constrained
	// to the label value syntax.
	DCMAnnotationResourceName = "dcm.project/resource-name"
)
