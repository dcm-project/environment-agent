package kubernetes_test

import (
	"context"
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	v1alpha1 "github.com/dcm-project/environment-agent/api/container/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/container/store"
	corev1 "k8s.io/api/core/v1"
)

var _ = Describe("K8s Store", func() {
	Describe("Create Operations", func() {
		// TC-I009: Create produces a Deployment with replicas=1
		It("produces a Deployment with replicas=1 (TC-I009)", func() {
			s, client := newTestStore(defaultConfig())
			c := minimalContainer("my-app")

			result, err := s.Create(context.Background(), c, "test-id-009")
			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())

			// Verify Deployment was created with replicas=1
			deploy, err := getCreatedDeployment(client, "default")
			Expect(err).NotTo(HaveOccurred())
			Expect(*deploy.Spec.Replicas).To(Equal(int32(1)))

			// Verify read-only fields are populated
			Expect(result.Id).NotTo(BeNil())
			Expect(result.Status).NotTo(BeNil())
			Expect(result.CreateTime).NotTo(BeNil())
		})

		// TC-I010: Created Deployment and Pod template carry DCM labels
		It("carries DCM labels on Deployment and Pod template (TC-I010)", func() {
			s, client := newTestStore(defaultConfig())
			c := minimalContainer("my-app")

			_, err := s.Create(context.Background(), c, "abc-123")
			Expect(err).NotTo(HaveOccurred())

			deploy, err := getCreatedDeployment(client, "default")
			Expect(err).NotTo(HaveOccurred())

			expectedLabels := map[string]string{
				"dcm.project/managed-by":       "dcm",
				"dcm.project/dcm-instance-id":  "abc-123",
				"dcm.project/dcm-service-type": "container",
			}

			for k, v := range expectedLabels {
				Expect(deploy.Labels).To(HaveKeyWithValue(k, v))
				Expect(deploy.Spec.Template.Labels).To(HaveKeyWithValue(k, v))
			}
		})

		// TC-I011: Deployment uses the specified container image
		It("uses the specified container image (TC-I011)", func() {
			s, client := newTestStore(defaultConfig())
			c := minimalContainer("my-app")
			c.Image.Reference = "quay.io/myapp:v1.2"

			_, err := s.Create(context.Background(), c, "test-id-011")
			Expect(err).NotTo(HaveOccurred())

			deploy, err := getCreatedDeployment(client, "default")
			Expect(err).NotTo(HaveOccurred())
			Expect(deploy.Spec.Template.Spec.Containers).To(HaveLen(1))
			Expect(deploy.Spec.Template.Spec.Containers[0].Image).To(Equal("quay.io/myapp:v1.2"))
		})

		// TC-I012: CPU resources map to Kubernetes requests and limits
		It("maps CPU resources to requests and limits (TC-I012)", func() {
			s, client := newTestStore(defaultConfig())
			c := minimalContainer("my-app")
			c.Resources.Cpu = v1alpha1.ContainerCpu{Min: "1000m", Max: "4000m"}

			_, err := s.Create(context.Background(), c, "test-id-012")
			Expect(err).NotTo(HaveOccurred())

			deploy, err := getCreatedDeployment(client, "default")
			Expect(err).NotTo(HaveOccurred())
			container := deploy.Spec.Template.Spec.Containers[0]
			Expect(container.Resources.Requests.Cpu().MilliValue()).To(Equal(int64(1000)))
			Expect(container.Resources.Limits.Cpu().MilliValue()).To(Equal(int64(4000)))
		})

		// Malformed CPU values that bypass OpenAPI validation are rejected.
		It("rejects a container spec with malformed CPU values", func() {
			s, _ := newTestStore(defaultConfig())
			c := minimalContainer("my-app")
			c.Resources.Cpu = v1alpha1.ContainerCpu{Min: "invalid", Max: "2000m"}

			_, err := s.Create(context.Background(), c, "test-id-012-invalid")
			Expect(err).To(BeAssignableToTypeOf(&store.InvalidArgumentError{}))
			Expect(err.Error()).To(ContainSubstring("cpu.min"))
		})

		// TC-I013: Memory resources convert and map correctly
		It("converts and maps memory resources correctly (TC-I013)", func() {
			s, client := newTestStore(defaultConfig())
			c := minimalContainer("my-app")
			c.Resources.Memory = v1alpha1.ContainerMemory{Min: "1GB", Max: "2GB"}

			_, err := s.Create(context.Background(), c, "test-id-013")
			Expect(err).NotTo(HaveOccurred())

			deploy, err := getCreatedDeployment(client, "default")
			Expect(err).NotTo(HaveOccurred())
			container := deploy.Spec.Template.Spec.Containers[0]
			Expect(container.Resources.Requests.Memory().String()).To(Equal("1Gi"))
			Expect(container.Resources.Limits.Memory().String()).To(Equal("2Gi"))
		})

		// TC-I014: Process command maps to container spec command
		It("maps process command to container spec (TC-I014)", func() {
			s, client := newTestStore(defaultConfig())
			c := minimalContainer("my-app")
			cmd := []string{"/app/start"}
			c.Process = &v1alpha1.ContainerProcess{
				Command: &cmd,
			}

			_, err := s.Create(context.Background(), c, "test-id-014")
			Expect(err).NotTo(HaveOccurred())

			deploy, err := getCreatedDeployment(client, "default")
			Expect(err).NotTo(HaveOccurred())
			Expect(deploy.Spec.Template.Spec.Containers[0].Command).To(Equal([]string{"/app/start"}))
		})

		// TC-I015: Process args map to container spec args
		It("maps process args to container spec (TC-I015)", func() {
			s, client := newTestStore(defaultConfig())
			c := minimalContainer("my-app")
			args := []string{"--config", "/etc/config.yaml"}
			c.Process = &v1alpha1.ContainerProcess{
				Args: &args,
			}

			_, err := s.Create(context.Background(), c, "test-id-015")
			Expect(err).NotTo(HaveOccurred())

			deploy, err := getCreatedDeployment(client, "default")
			Expect(err).NotTo(HaveOccurred())
			Expect(deploy.Spec.Template.Spec.Containers[0].Args).To(Equal([]string{"--config", "/etc/config.yaml"}))
		})

		// TC-I016: Environment variables map to container spec env
		It("maps environment variables to container spec (TC-I016)", func() {
			s, client := newTestStore(defaultConfig())
			c := minimalContainer("my-app")
			env := []v1alpha1.ContainerEnvVar{
				{Name: "ENV", Value: "prod"},
				{Name: "LOG_LEVEL", Value: "debug"},
			}
			c.Process = &v1alpha1.ContainerProcess{
				Env: &env,
			}

			_, err := s.Create(context.Background(), c, "test-id-016")
			Expect(err).NotTo(HaveOccurred())

			deploy, err := getCreatedDeployment(client, "default")
			Expect(err).NotTo(HaveOccurred())
			containerEnv := deploy.Spec.Template.Spec.Containers[0].Env
			Expect(containerEnv).To(ContainElements(
				corev1.EnvVar{Name: "ENV", Value: "prod"},
				corev1.EnvVar{Name: "LOG_LEVEL", Value: "debug"},
			))
		})

		// TC-I017: Network ports map to container spec ports
		It("maps network ports to container spec (TC-I017)", func() {
			s, client := newTestStore(defaultConfig())
			c := containerWithPorts("my-app", 8080, 9090)

			_, err := s.Create(context.Background(), c, "test-id-017")
			Expect(err).NotTo(HaveOccurred())

			deploy, err := getCreatedDeployment(client, "default")
			Expect(err).NotTo(HaveOccurred())
			containerPorts := deploy.Spec.Template.Spec.Containers[0].Ports
			Expect(containerPorts).To(ContainElements(
				HaveField("ContainerPort", int32(8080)),
				HaveField("ContainerPort", int32(9090)),
			))
		})

		// TC-I018: Optional fields omitted when not provided
		It("omits optional fields when not provided (TC-I018)", func() {
			s, client := newTestStore(defaultConfig())
			c := minimalContainer("my-app")

			_, err := s.Create(context.Background(), c, "test-id-018")
			Expect(err).NotTo(HaveOccurred())

			deploy, err := getCreatedDeployment(client, "default")
			Expect(err).NotTo(HaveOccurred())
			container := deploy.Spec.Template.Spec.Containers[0]
			Expect(container.Command).To(BeEmpty())
			Expect(container.Args).To(BeEmpty())
			Expect(container.Env).To(BeEmpty())
			Expect(container.Ports).To(BeEmpty())
		})

		// TC-I069: Create rejects duplicate dcm-instance-id
		It("rejects duplicate dcm-instance-id (TC-I069)", func() {
			s, client := newTestStore(defaultConfig())

			// Pre-create a Deployment with the target instance ID
			err := createFakeDeployment(client, "existing-app", "existing-id")
			Expect(err).NotTo(HaveOccurred())

			// Attempt to create with the same ID but different name
			c := minimalContainer("different-name")
			_, err = s.Create(context.Background(), c, "existing-id")

			var conflictErr *store.ConflictError
			Expect(errors.As(err, &conflictErr)).To(BeTrue(), "expected ConflictError, got: %v", err)
		})

		// TC-I070: Create applies user-specified metadata.labels
		It("applies user-specified metadata labels (TC-I070)", func() {
			s, client := newTestStore(defaultConfig())
			c := minimalContainer("my-app")
			labels := map[string]string{"env": "staging", "team": "platform"}
			c.Metadata.Labels = &labels

			_, err := s.Create(context.Background(), c, "test-id-070")
			Expect(err).NotTo(HaveOccurred())

			deploy, err := getCreatedDeployment(client, "default")
			Expect(err).NotTo(HaveOccurred())

			// User labels present
			Expect(deploy.Labels).To(HaveKeyWithValue("env", "staging"))
			Expect(deploy.Labels).To(HaveKeyWithValue("team", "platform"))
			// DCM labels also present
			Expect(deploy.Labels).To(HaveKeyWithValue("dcm.project/managed-by", "dcm"))
			Expect(deploy.Labels).To(HaveKeyWithValue("dcm.project/dcm-instance-id", "test-id-070"))

			// Same on pod template
			Expect(deploy.Spec.Template.Labels).To(HaveKeyWithValue("env", "staging"))
			Expect(deploy.Spec.Template.Labels).To(HaveKeyWithValue("team", "platform"))
		})
	})
})
