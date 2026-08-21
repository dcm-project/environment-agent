package cluster_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"io"
	"log/slog"

	"github.com/dcm-project/environment-agent/internal/acmcluster/cluster"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ = Describe("EnsurePullSecret", func() {
	It("TC-OPS-UT-018: creates shared PullSecret Secret at startup with correct name, type, content, and labels", func() {
		ctx := context.Background()
		cfg := defaultConfig()

		rawContent := `{"auths":{"registry.example.com":{"auth":"dXNlcjpwYXNz"}}}`
		cfg.PullSecret = base64.StdEncoding.EncodeToString([]byte(rawContent))
		cfg.PullSecretName = "test-sp-pull-secret"

		k8s := buildFakeClient(nil, nil)
		logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

		err := cluster.EnsurePullSecret(ctx, k8s, cfg, logger)
		Expect(err).NotTo(HaveOccurred())

		// Verify Secret was created
		var secret corev1.Secret
		Expect(k8s.Get(ctx, client.ObjectKey{
			Name:      "test-sp-pull-secret",
			Namespace: testNamespace,
		}, &secret)).To(Succeed())

		// Type must be kubernetes.io/dockerconfigjson
		Expect(secret.Type).To(Equal(corev1.SecretTypeDockerConfigJson))

		// Content must be base64-decoded
		Expect(secret.Data).To(HaveKey(".dockerconfigjson"))
		Expect(string(secret.Data[".dockerconfigjson"])).To(Equal(rawContent))

		// DCM labels present (no instance-id)
		Expect(secret.Labels).To(HaveKeyWithValue("dcm.project/managed-by", "dcm"))
		Expect(secret.Labels).To(HaveKeyWithValue("dcm.project/dcm-service-type", "cluster"))
		Expect(secret.Labels).NotTo(HaveKey("dcm.project/dcm-instance-id"))
	})

	It("TC-OPS-UT-019: updates existing PullSecret with correct data and labels", func() {
		ctx := context.Background()
		cfg := defaultConfig()

		rawContent := `{"auths":{"registry.example.com":{"auth":"bmV3Y3JlZHM="}}}`
		cfg.PullSecret = base64.StdEncoding.EncodeToString([]byte(rawContent))
		cfg.PullSecretName = "test-sp-pull-secret"

		// Pre-seed an existing Secret with correct type but old data
		existing := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-sp-pull-secret",
				Namespace: testNamespace,
			},
			Data: map[string][]byte{
				".dockerconfigjson": []byte(`{"auths":{}}`),
			},
			Type: corev1.SecretTypeDockerConfigJson,
		}

		k8s := buildFakeClient([]client.Object{existing}, nil)
		logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

		err := cluster.EnsurePullSecret(ctx, k8s, cfg, logger)
		Expect(err).NotTo(HaveOccurred())

		var secret corev1.Secret
		Expect(k8s.Get(ctx, client.ObjectKey{
			Name:      "test-sp-pull-secret",
			Namespace: testNamespace,
		}, &secret)).To(Succeed())

		// Data updated
		Expect(string(secret.Data[".dockerconfigjson"])).To(Equal(rawContent))

		// Labels updated
		Expect(secret.Labels).To(HaveKeyWithValue("dcm.project/managed-by", "dcm"))
		Expect(secret.Labels).To(HaveKeyWithValue("dcm.project/dcm-service-type", "cluster"))

		// Type unchanged
		Expect(secret.Type).To(Equal(corev1.SecretTypeDockerConfigJson))
	})

	It("TC-OPS-UT-020: warns when existing PullSecret has wrong type", func() {
		ctx := context.Background()
		cfg := defaultConfig()

		rawContent := `{"auths":{"registry.example.com":{"auth":"dXNlcjpwYXNz"}}}`
		cfg.PullSecret = base64.StdEncoding.EncodeToString([]byte(rawContent))
		cfg.PullSecretName = "test-sp-pull-secret"

		// Pre-seed an existing Secret with WRONG type (Opaque)
		existing := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-sp-pull-secret",
				Namespace: testNamespace,
			},
			Data: map[string][]byte{
				".dockerconfigjson": []byte(`{"auths":{}}`),
			},
			Type: corev1.SecretTypeOpaque,
		}

		k8s := buildFakeClient([]client.Object{existing}, nil)
		var logBuf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&logBuf, nil))

		err := cluster.EnsurePullSecret(ctx, k8s, cfg, logger)
		Expect(err).NotTo(HaveOccurred())

		// Warning logged with type mismatch details
		logOutput := logBuf.String()
		Expect(logOutput).To(ContainSubstring("existing pull secret has unexpected type"))
		Expect(logOutput).To(ContainSubstring("Opaque"))

		var secret corev1.Secret
		Expect(k8s.Get(ctx, client.ObjectKey{
			Name:      "test-sp-pull-secret",
			Namespace: testNamespace,
		}, &secret)).To(Succeed())

		// Data and Labels updated
		Expect(string(secret.Data[".dockerconfigjson"])).To(Equal(rawContent))
		Expect(secret.Labels).To(HaveKeyWithValue("dcm.project/managed-by", "dcm"))
		Expect(secret.Labels).To(HaveKeyWithValue("dcm.project/dcm-service-type", "cluster"))

		// Type NOT changed — K8s immutability
		Expect(secret.Type).To(Equal(corev1.SecretTypeOpaque))
	})

	It("TC-OPS-UT-021: returns error on invalid base64 in SP_PULL_SECRET", func() {
		ctx := context.Background()
		cfg := defaultConfig()

		cfg.PullSecret = "!!!not-base64!!!"
		cfg.PullSecretName = "test-sp-pull-secret"

		k8s := buildFakeClient(nil, nil)
		logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

		err := cluster.EnsurePullSecret(ctx, k8s, cfg, logger)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("decoding SP_PULL_SECRET"))
	})
})
