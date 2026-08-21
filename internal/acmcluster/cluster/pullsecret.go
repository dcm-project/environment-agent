package cluster

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/dcm-project/environment-agent/internal/acmcluster/config"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// EnsurePullSecret creates or updates the shared pull secret in the cluster
// namespace. The secret content is base64-decoded from cfg.PullSecret (REQ-ACM-190).
func EnsurePullSecret(ctx context.Context, c client.Client, cfg config.ClusterConfig, logger *slog.Logger) error {
	decoded, err := base64.StdEncoding.DecodeString(cfg.PullSecret)
	if err != nil {
		return fmt.Errorf("decoding SP_PULL_SECRET: %w", err)
	}

	var dockerCfg struct {
		Auths map[string]json.RawMessage `json:"auths"`
	}
	if err := json.Unmarshal(decoded, &dockerCfg); err != nil {
		return fmt.Errorf("parsing SP_PULL_SECRET as .dockerconfigjson: %w", err)
	}
	if len(dockerCfg.Auths) == 0 {
		return fmt.Errorf("SP_PULL_SECRET .dockerconfigjson is missing required 'auths' entries")
	}

	labels := map[string]string{
		LabelManagedBy:   ValueManagedBy,
		LabelServiceType: ValueServiceType,
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cfg.PullSecretName,
			Namespace: cfg.ClusterNamespace,
			Labels:    labels,
		},
		Data: map[string][]byte{
			".dockerconfigjson": decoded,
		},
		Type: corev1.SecretTypeDockerConfigJson,
	}

	if err := c.Create(ctx, secret); err != nil {
		if !k8serrors.IsAlreadyExists(err) {
			return fmt.Errorf("creating pull secret: %w", err)
		}
		existing := &corev1.Secret{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(secret), existing); err != nil {
			return fmt.Errorf("getting existing pull secret: %w", err)
		}
		existing.Data = secret.Data
		if existing.Labels == nil {
			existing.Labels = make(map[string]string)
		}
		for k, v := range labels {
			existing.Labels[k] = v
		}
		if existing.Type != corev1.SecretTypeDockerConfigJson {
			logger.Warn("existing pull secret has unexpected type",
				"name", existing.Name, "namespace", existing.Namespace,
				"expected", string(corev1.SecretTypeDockerConfigJson),
				"actual", string(existing.Type))
		}
		if err := c.Update(ctx, existing); err != nil {
			return fmt.Errorf("updating pull secret: %w", err)
		}
	}

	return nil
}
