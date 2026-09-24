package kubevirt_test

import (
	"fmt"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	k8sv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubevirtv1 "kubevirt.io/api/core/v1"

	"github.com/dcm-project/environment-agent/api/vm/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/kubevirtvm/constants"
	"github.com/dcm-project/environment-agent/internal/openshift/kubevirtvm/kubevirt"
)

func TestMapper(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Mapper Suite")
}

var _ = Describe("Mapper", func() {
	var mapper *kubevirt.Mapper

	BeforeEach(func() {
		mapper = kubevirt.NewMapper("default")
	})

	Describe("VMSpecToVirtualMachine", func() {
		It("should convert a basic VMSpec to VirtualMachine without errors", func() {
			vmSpec := &v1alpha1.VMSpec{
				ServiceType: v1alpha1.Vm,
				Metadata: v1alpha1.ServiceMetadata{
					Name: "test-vm",
				},
				GuestOs: v1alpha1.GuestOS{
					Type: "ubuntu",
				},
				Vcpu: v1alpha1.Vcpu{
					Count: 2,
				},
				Memory: v1alpha1.Memory{
					Size: "2Gi",
				},
				Storage: v1alpha1.Storage{
					Disks: []v1alpha1.Disk{
						{
							Name:     "boot",
							Capacity: "10Gi",
						},
					},
				},
			}

			vm, err := mapper.VMSpecToVirtualMachine(vmSpec, "00000000-0000-0000-0000-000000000001")

			Expect(err).NotTo(HaveOccurred())
			Expect(vm).NotTo(BeNil())

			// Check basic metadata
			Expect(vm.GenerateName).To(Equal("dcm-"))
			Expect(vm.Namespace).To(Equal("default"))
			Expect(vm.TypeMeta.APIVersion).To(Equal("kubevirt.io/v1"))
			Expect(vm.TypeMeta.Kind).To(Equal("VirtualMachine"))
		})

		It("should record the requested resource name in an annotation", func() {
			vmSpec := &v1alpha1.VMSpec{
				ServiceType: v1alpha1.Vm,
				Metadata:    v1alpha1.ServiceMetadata{Name: "my-dev-vm"},
				GuestOs:     v1alpha1.GuestOS{Type: "ubuntu"},
				Vcpu:        v1alpha1.Vcpu{Count: 1},
				Memory:      v1alpha1.Memory{Size: "1Gi"},
			}

			vm, err := mapper.VMSpecToVirtualMachine(vmSpec, "00000000-0000-0000-0000-00000000000a")

			Expect(err).NotTo(HaveOccurred())
			// GenerateName means the cluster name is never the requested one,
			// so the annotation is the only way back to it.
			Expect(vm.Name).To(BeEmpty())
			Expect(vm.Annotations).To(HaveKeyWithValue(constants.DCMAnnotationResourceName, "my-dev-vm"))
		})

		It("should handle empty storage with default boot disk", func() {
			vmSpec := &v1alpha1.VMSpec{
				ServiceType: v1alpha1.Vm,
				Metadata: v1alpha1.ServiceMetadata{
					Name: "minimal-vm",
				},
				GuestOs: v1alpha1.GuestOS{
					Type: "cirros",
				},
				Vcpu: v1alpha1.Vcpu{
					Count: 1,
				},
				Memory: v1alpha1.Memory{
					Size: "1Gi",
				},
				Storage: v1alpha1.Storage{
					Disks: []v1alpha1.Disk{},
				},
			}

			vm, err := mapper.VMSpecToVirtualMachine(vmSpec, "00000000-0000-0000-0000-000000000002")

			Expect(err).NotTo(HaveOccurred())
			Expect(vm).NotTo(BeNil())
			Expect(vm.Spec.Template.Spec.Domain.Devices.Disks).To(HaveLen(1))
			Expect(vm.Spec.Template.Spec.Domain.Devices.Disks[0].Name).To(Equal("boot"))
		})
	})

	Describe("VirtualMachineToVMSpec", func() {
		It("should convert a VirtualMachine back to VMSpec with correct CPU, memory, guest OS and disks", func() {
			vmSpec := &v1alpha1.VMSpec{
				ServiceType: v1alpha1.Vm,
				Metadata: v1alpha1.ServiceMetadata{
					Name: "roundtrip-vm",
				},
				GuestOs: v1alpha1.GuestOS{
					Type: "ubuntu",
				},
				Vcpu: v1alpha1.Vcpu{
					Count: 4,
				},
				Memory: v1alpha1.Memory{
					Size: "4Gi",
				},
				Storage: v1alpha1.Storage{
					Disks: []v1alpha1.Disk{
						{Name: "boot", Capacity: "20Gi"},
						{Name: "data", Capacity: "10Gi"},
					},
				},
			}

			vm, err := mapper.VMSpecToVirtualMachine(vmSpec, "00000000-0000-0000-0000-000000000003")
			Expect(err).NotTo(HaveOccurred())
			Expect(vm).NotTo(BeNil())

			back, err := mapper.VirtualMachineToVMSpec(vm)
			Expect(err).NotTo(HaveOccurred())
			Expect(back).NotTo(BeNil())

			Expect(back.ServiceType).To(Equal(v1alpha1.Vm))
			Expect(back.Metadata.Name).To(Equal("roundtrip-vm"))
			Expect(back.Vcpu.Count).To(Equal(4))
			Expect(back.Memory.Size).To(Equal("4Gi"))
			Expect(back.GuestOs.Type).To(Equal("ubuntu"))
			Expect(back.Storage.Disks).To(HaveLen(2))
			Expect(back.Storage.Disks[0].Name).To(Equal("boot"))
			Expect(back.Storage.Disks[1].Name).To(Equal("data"))
		})

		It("should report the DCM instance ID, creation time and printable status", func() {
			created := metav1.NewTime(time.Date(2026, 1, 13, 10, 30, 0, 0, time.UTC))
			vm := kubevirtVMWithContainerDisk("quay.io/containerdisks/ubuntu:latest", 2, "2Gi")
			vm.Labels = map[string]string{constants.DCMLabelInstanceID: "00000000-0000-0000-0000-000000000004"}
			vm.CreationTimestamp = created
			vm.Status.PrintableStatus = kubevirtv1.VirtualMachineStatusStarting

			back, err := mapper.VirtualMachineToVMSpec(vm)

			Expect(err).NotTo(HaveOccurred())
			Expect(back.Id).To(HaveValue(Equal("00000000-0000-0000-0000-000000000004")))
			Expect(back.CreateTime).To(HaveValue(Equal(created.Time.UTC())))
			Expect(back.Status).To(HaveValue(Equal("Starting")))
		})

		It("should leave status unset when KubeVirt has not reported one yet", func() {
			vm := kubevirtVMWithContainerDisk("quay.io/containerdisks/ubuntu:latest", 1, "1Gi")

			back, err := mapper.VirtualMachineToVMSpec(vm)

			Expect(err).NotTo(HaveOccurred())
			Expect(back.Status).To(BeNil())
			Expect(back.Id).To(BeNil())
			Expect(back.StatusMessage).To(BeNil())
		})

		It("should surface the Ready condition message while the VM is not ready", func() {
			vm := kubevirtVMWithContainerDisk("quay.io/containerdisks/ubuntu:latest", 1, "1Gi")
			vm.Status.PrintableStatus = kubevirtv1.VirtualMachineStatusUnschedulable
			vm.Status.Conditions = []kubevirtv1.VirtualMachineCondition{{
				Type:    kubevirtv1.VirtualMachineReady,
				Status:  k8sv1.ConditionFalse,
				Reason:  "Unschedulable",
				Message: "0/3 nodes are available: insufficient memory",
			}}

			back, err := mapper.VirtualMachineToVMSpec(vm)

			Expect(err).NotTo(HaveOccurred())
			Expect(back.StatusMessage).To(HaveValue(Equal("0/3 nodes are available: insufficient memory")))
		})

		It("should not report a status message for a ready VM", func() {
			vm := kubevirtVMWithContainerDisk("quay.io/containerdisks/ubuntu:latest", 1, "1Gi")
			vm.Status.PrintableStatus = kubevirtv1.VirtualMachineStatusRunning
			vm.Status.Conditions = []kubevirtv1.VirtualMachineCondition{{
				Type:   kubevirtv1.VirtualMachineReady,
				Status: k8sv1.ConditionTrue,
			}}

			back, err := mapper.VirtualMachineToVMSpec(vm)

			Expect(err).NotTo(HaveOccurred())
			Expect(back.StatusMessage).To(BeNil())
		})

		It("should fall back to the cluster name when the annotation is absent", func() {
			vm := kubevirtVMWithContainerDisk("quay.io/containerdisks/ubuntu:latest", 1, "1Gi")

			back, err := mapper.VirtualMachineToVMSpec(vm)

			Expect(err).NotTo(HaveOccurred())
			Expect(back.Metadata.Name).To(Equal("test-vm"))
		})

		It("should still identify a VM that has no template instead of returning a zeroed spec", func() {
			vm := &kubevirtv1.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "dcm-abc12",
					Namespace:   "default",
					Labels:      map[string]string{constants.DCMLabelInstanceID: "00000000-0000-0000-0000-000000000005"},
					Annotations: map[string]string{constants.DCMAnnotationResourceName: "templateless-vm"},
				},
				Status: kubevirtv1.VirtualMachineStatus{
					PrintableStatus: kubevirtv1.VirtualMachineStatusStopped,
				},
			}

			back, err := mapper.VirtualMachineToVMSpec(vm)

			Expect(err).NotTo(HaveOccurred())
			Expect(back.ServiceType).To(Equal(v1alpha1.Vm))
			Expect(back.Metadata.Name).To(Equal("templateless-vm"))
			Expect(back.Id).To(HaveValue(Equal("00000000-0000-0000-0000-000000000005")))
			Expect(back.Status).To(HaveValue(Equal("Stopped")))
		})

		It("should infer guest OS from container disk image", func() {
			vm := kubevirtVMWithContainerDisk("quay.io/kubevirt/fedora-container-disk-demo:latest", 2, "2Gi")

			back, err := mapper.VirtualMachineToVMSpec(vm)
			Expect(err).NotTo(HaveOccurred())
			Expect(back).NotTo(BeNil())
			Expect(back.GuestOs.Type).To(Equal("fedora"))
			Expect(back.Vcpu.Count).To(Equal(2))
			Expect(back.Memory.Size).To(Equal("2Gi"))
		})

		It("should default to cirros and boot disk when VM has minimal or no domain data", func() {
			vm := kubevirtVMWithContainerDisk("quay.io/something/unknown:latest", 1, "1Gi")

			back, err := mapper.VirtualMachineToVMSpec(vm)
			Expect(err).NotTo(HaveOccurred())
			Expect(back).NotTo(BeNil())
			Expect(back.GuestOs.Type).To(Equal("cirros"))
			Expect(back.Storage.Disks).NotTo(BeEmpty())
			Expect(back.Storage.Disks[0].Name).To(Equal("boot"))
		})
	})
})

// kubevirtVMWithContainerDisk builds a typed VirtualMachine with the given container disk image, CPU count and memory.
func kubevirtVMWithContainerDisk(containerImage string, cpuCount int, memorySize string) *kubevirtv1.VirtualMachine {
	if cpuCount == 0 {
		cpuCount = 1
	}
	bootOrder := uint(1)
	running := true

	return &kubevirtv1.VirtualMachine{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "kubevirt.io/v1",
			Kind:       "VirtualMachine",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-vm",
			Namespace: "default",
		},
		Spec: kubevirtv1.VirtualMachineSpec{
			Running: &running,
			Template: &kubevirtv1.VirtualMachineInstanceTemplateSpec{
				Spec: kubevirtv1.VirtualMachineInstanceSpec{
					Domain: kubevirtv1.DomainSpec{
						Resources: kubevirtv1.ResourceRequirements{
							Requests: k8sv1.ResourceList{
								k8sv1.ResourceCPU:    resource.MustParse(fmt.Sprintf("%d", cpuCount)),
								k8sv1.ResourceMemory: resource.MustParse(memorySize),
							},
						},
						Devices: kubevirtv1.Devices{
							Disks: []kubevirtv1.Disk{
								{
									Name: "boot",
									DiskDevice: kubevirtv1.DiskDevice{
										Disk: &kubevirtv1.DiskTarget{
											Bus: kubevirtv1.DiskBusVirtio,
										},
									},
									BootOrder: &bootOrder,
								},
							},
						},
					},
					Volumes: []kubevirtv1.Volume{
						{
							Name: "boot",
							VolumeSource: kubevirtv1.VolumeSource{
								ContainerDisk: &kubevirtv1.ContainerDiskSource{
									Image: containerImage,
								},
							},
						},
					},
				},
			},
		},
	}
}
