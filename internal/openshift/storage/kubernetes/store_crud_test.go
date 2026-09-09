package kubernetes_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sort"
	"strconv"

	v1alpha1 "github.com/dcm-project/environment-agent/api/storage/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/storage/dcm"
	k8sstore "github.com/dcm-project/environment-agent/internal/openshift/storage/kubernetes"
	"github.com/dcm-project/environment-agent/internal/openshift/storage/store"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

var _ store.VolumeRepository = (*k8sstore.K8sVolumeStore)(nil)

func newTestStore(cfg k8sstore.K8sConfig) (*k8sstore.K8sVolumeStore, *fake.Clientset) {
	client := fake.NewClientset()
	installPVCListPagination(client)
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	return k8sstore.NewK8sVolumeStore(client, cfg, logger), client
}

// installPVCListPagination teaches the fake client Limit/Continue behavior so
// unit tests exercise the same pagination contract as a real apiserver.
func installPVCListPagination(client *fake.Clientset) {
	gvr := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "persistentvolumeclaims"}
	gvk := schema.GroupVersionKind{Group: "", Version: "v1", Kind: "PersistentVolumeClaim"}

	client.PrependReactor("list", "persistentvolumeclaims", func(action clienttesting.Action) (bool, runtime.Object, error) {
		la, ok := action.(clienttesting.ListActionImpl)
		if !ok {
			return false, nil, nil
		}
		opts := la.GetListOptions()

		obj, err := client.Tracker().List(gvr, gvk, la.GetNamespace())
		if err != nil {
			return true, nil, err
		}
		full := obj.(*corev1.PersistentVolumeClaimList)

		selector := labels.Everything()
		if opts.LabelSelector != "" {
			selector, err = labels.Parse(opts.LabelSelector)
			if err != nil {
				return true, nil, err
			}
		}

		filtered := make([]corev1.PersistentVolumeClaim, 0, len(full.Items))
		for i := range full.Items {
			if selector.Matches(labels.Set(full.Items[i].Labels)) {
				filtered = append(filtered, full.Items[i])
			}
		}
		sort.Slice(filtered, func(i, j int) bool {
			return filtered[i].Name < filtered[j].Name
		})

		offset := 0
		if opts.Continue != "" {
			offset, err = strconv.Atoi(opts.Continue)
			if err != nil || offset < 0 {
				return true, nil, apierrors.NewBadRequest("invalid continue token")
			}
		}

		limit := int(opts.Limit)
		if limit <= 0 {
			limit = len(filtered)
		}
		if offset > len(filtered) {
			offset = len(filtered)
		}
		end := min(offset+limit, len(filtered))

		page := filtered[offset:end]
		continueToken := ""
		if end < len(filtered) {
			continueToken = strconv.Itoa(end)
		}

		return true, &corev1.PersistentVolumeClaimList{
			ListMeta: metav1.ListMeta{Continue: continueToken},
			Items:    page,
		}, nil
	})
}

func defaultConfig() k8sstore.K8sConfig {
	return k8sstore.K8sConfig{
		Namespace:         "default",
		DefaultAccessMode: "ReadWriteOnce",
	}
}

func minimalVolumeSpec(name string) v1alpha1.StorageSpec {
	return v1alpha1.StorageSpec{
		ServiceType: v1alpha1.Storage,
		Capacity:    "10Gi",
		Metadata: v1alpha1.VolumeMetadata{
			Name: name,
		},
	}
}

func createStorageClass(client *fake.Clientset, name string) {
	sc := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{Name: name},
	}
	_, err := client.StorageV1().StorageClasses().Create(context.Background(), sc, metav1.CreateOptions{})
	Expect(err).NotTo(HaveOccurred())
}

var _ = Describe("K8s Volume Store CRUD", func() {
	Describe("Create", func() {
		It("creates a PVC with DCM labels and returns PROVISIONING (TC-U060)", func() {
			s, client := newTestStore(defaultConfig())
			spec := minimalVolumeSpec("app-data")

			result, err := s.Create(context.Background(), spec, "app-data")
			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())
			Expect(*result.Id).To(Equal("app-data"))
			Expect(*result.Path).To(Equal("volumes/app-data"))
			Expect(*result.Status).To(Equal(v1alpha1.PROVISIONING))
			Expect(result.Spec.Metadata.Namespace).NotTo(BeNil())
			Expect(*result.Spec.Metadata.Namespace).To(Equal("default"))

			pvc, err := client.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), "app-data", metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc.Labels).To(HaveKeyWithValue(dcm.LabelManagedBy, dcm.ValueManagedByDCM))
			Expect(pvc.Labels).To(HaveKeyWithValue(dcm.LabelInstanceID, "app-data"))
			Expect(pvc.Labels).To(HaveKeyWithValue(dcm.LabelServiceType, dcm.ValueServiceType))
			Expect(pvc.Spec.AccessModes).To(Equal([]corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}))
			Expect(pvc.Spec.Resources.Requests.Storage().String()).To(Equal("10Gi"))
		})

		It("applies provider hints for storage class, access mode, and volume mode", func() {
			s, client := newTestStore(defaultConfig())
			createStorageClass(client, "fast-ssd")
			accessMode := v1alpha1.ReadWriteMany
			volumeMode := v1alpha1.Block
			sc := "fast-ssd"
			spec := minimalVolumeSpec("hinted-vol")
			spec.ProviderHints = &v1alpha1.ProviderHints{
				Kubernetes: &v1alpha1.KubernetesProviderHints{
					StorageClass: &sc,
					AccessMode:   &accessMode,
					VolumeMode:   &volumeMode,
				},
			}

			_, err := s.Create(context.Background(), spec, "hinted-vol")
			Expect(err).NotTo(HaveOccurred())

			pvc, err := client.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), "hinted-vol", metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc.Spec.StorageClassName).NotTo(BeNil())
			Expect(*pvc.Spec.StorageClassName).To(Equal("fast-ssd"))
			Expect(pvc.Spec.AccessModes).To(Equal([]corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}))
			Expect(pvc.Spec.VolumeMode).NotTo(BeNil())
			Expect(*pvc.Spec.VolumeMode).To(Equal(corev1.PersistentVolumeBlock))
		})

		It("applies DefaultStorageClass when hints omit storage_class", func() {
			cfg := defaultConfig()
			cfg.DefaultStorageClass = "default-sc"
			s, client := newTestStore(cfg)
			createStorageClass(client, "default-sc")

			_, err := s.Create(context.Background(), minimalVolumeSpec("app-data"), "app-data")
			Expect(err).NotTo(HaveOccurred())

			pvc, err := client.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), "app-data", metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc.Spec.StorageClassName).NotTo(BeNil())
			Expect(*pvc.Spec.StorageClassName).To(Equal("default-sc"))
		})

		It("names the PVC from the instance id, overwriting metadata.name", func() {
			s, client := newTestStore(defaultConfig())
			result, err := s.Create(context.Background(), minimalVolumeSpec("catalog-default"), "other-id")
			Expect(err).NotTo(HaveOccurred())
			Expect(*result.Id).To(Equal("other-id"))
			Expect(result.Spec.Metadata.Name).To(Equal("other-id"))

			_, err = client.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), "catalog-default", metav1.GetOptions{})
			Expect(apierrors.IsNotFound(err)).To(BeTrue())

			pvc, err := client.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), "other-id", metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc.Labels).To(HaveKeyWithValue(dcm.LabelInstanceID, "other-id"))
		})

		It("returns conflict when volume already exists (TC-U061)", func() {
			s, _ := newTestStore(defaultConfig())
			_, err := s.Create(context.Background(), minimalVolumeSpec("app-data"), "app-data")
			Expect(err).NotTo(HaveOccurred())

			_, err = s.Create(context.Background(), minimalVolumeSpec("app-data"), "app-data")
			Expect(err).To(HaveOccurred())
			var conflict *store.ConflictError
			Expect(errors.As(err, &conflict)).To(BeTrue())
		})

		It("returns conflict when a non-DCM PVC with the same name already exists", func() {
			s, client := newTestStore(defaultConfig())
			existing := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "app-data", Namespace: "default"},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
					},
				},
			}
			_, err := client.CoreV1().PersistentVolumeClaims("default").Create(context.Background(), existing, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			_, err = s.Create(context.Background(), minimalVolumeSpec("app-data"), "app-data")
			Expect(err).To(HaveOccurred())
			var conflict *store.ConflictError
			Expect(errors.As(err, &conflict)).To(BeTrue())
		})

		It("returns failed precondition when StorageClass does not exist", func() {
			s, _ := newTestStore(defaultConfig())
			sc := "missing-sc"
			spec := minimalVolumeSpec("app-data")
			spec.ProviderHints = &v1alpha1.ProviderHints{
				Kubernetes: &v1alpha1.KubernetesProviderHints{StorageClass: &sc},
			}

			_, err := s.Create(context.Background(), spec, "app-data")
			Expect(err).To(HaveOccurred())
			var failed *store.FailedPreconditionError
			Expect(errors.As(err, &failed)).To(BeTrue())
		})

		It("returns invalid argument for bad capacity", func() {
			s, _ := newTestStore(defaultConfig())
			spec := minimalVolumeSpec("app-data")
			spec.Capacity = "not-a-quantity"

			_, err := s.Create(context.Background(), spec, "app-data")
			Expect(err).To(HaveOccurred())
			var invalid *store.InvalidArgumentError
			Expect(errors.As(err, &invalid)).To(BeTrue())
		})

		It("returns invalid argument for unsupported volume mode", func() {
			s, _ := newTestStore(defaultConfig())
			badMode := v1alpha1.VolumeMode("Raw")
			spec := minimalVolumeSpec("app-data")
			spec.ProviderHints = &v1alpha1.ProviderHints{
				Kubernetes: &v1alpha1.KubernetesProviderHints{VolumeMode: &badMode},
			}

			_, err := s.Create(context.Background(), spec, "app-data")
			Expect(err).To(HaveOccurred())
			var invalid *store.InvalidArgumentError
			Expect(errors.As(err, &invalid)).To(BeTrue())
			Expect(invalid.Message).To(ContainSubstring("volume mode"))
		})
	})

	Describe("Get", func() {
		It("returns the volume by instance ID (TC-U065)", func() {
			s, _ := newTestStore(defaultConfig())
			_, err := s.Create(context.Background(), minimalVolumeSpec("app-data"), "app-data")
			Expect(err).NotTo(HaveOccurred())

			got, err := s.Get(context.Background(), "app-data")
			Expect(err).NotTo(HaveOccurred())
			Expect(*got.Id).To(Equal("app-data"))
			Expect(got.Spec.Metadata.Name).To(Equal("app-data"))
		})

		It("returns not found when missing (TC-U066)", func() {
			s, _ := newTestStore(defaultConfig())
			_, err := s.Get(context.Background(), "missing")
			Expect(err).To(HaveOccurred())
			var notFound *store.NotFoundError
			Expect(errors.As(err, &notFound)).To(BeTrue())
		})

		It("returns not found for a PVC without DCM labels", func() {
			s, client := newTestStore(defaultConfig())
			foreign := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "foreign", Namespace: "default"},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
					},
				},
			}
			_, err := client.CoreV1().PersistentVolumeClaims("default").Create(context.Background(), foreign, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			_, err = s.Get(context.Background(), "foreign")
			Expect(err).To(HaveOccurred())
			var notFound *store.NotFoundError
			Expect(errors.As(err, &notFound)).To(BeTrue())
		})
	})

	Describe("List", func() {
		It("lists DCM-managed volumes only (TC-U063, TC-U064)", func() {
			s, client := newTestStore(defaultConfig())
			_, err := s.Create(context.Background(), minimalVolumeSpec("vol-a"), "vol-a")
			Expect(err).NotTo(HaveOccurred())
			_, err = s.Create(context.Background(), minimalVolumeSpec("vol-b"), "vol-b")
			Expect(err).NotTo(HaveOccurred())
			_, err = s.Create(context.Background(), minimalVolumeSpec("vol-c"), "vol-c")
			Expect(err).NotTo(HaveOccurred())

			foreign := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "foreign", Namespace: "default"},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
					},
				},
			}
			_, err = client.CoreV1().PersistentVolumeClaims("default").Create(context.Background(), foreign, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			list, err := s.List(context.Background(), 0, "")
			Expect(err).NotTo(HaveOccurred())
			Expect(list.Volumes).NotTo(BeNil())
			Expect(*list.Volumes).To(HaveLen(3))
		})

		It("omits PVCs whose dcm-instance-id label does not match the PVC name", func() {
			s, client := newTestStore(defaultConfig())
			_, err := s.Create(context.Background(), minimalVolumeSpec("good-vol"), "good-vol")
			Expect(err).NotTo(HaveOccurred())

			badLabels := dcm.Labels("wrong-id")
			badPVC := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "bad-vol",
					Namespace: "default",
					Labels:    badLabels,
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
					},
				},
			}
			_, err = client.CoreV1().PersistentVolumeClaims("default").Create(context.Background(), badPVC, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			list, err := s.List(context.Background(), 0, "")
			Expect(err).NotTo(HaveOccurred())
			Expect(*list.Volumes).To(HaveLen(1))
			Expect(*(*list.Volumes)[0].Id).To(Equal("good-vol"))
		})

		It("paginates with Kubernetes continue tokens", func() {
			s, _ := newTestStore(defaultConfig())
			for _, name := range []string{"a", "b", "c"} {
				volName := "vol-" + name
				_, err := s.Create(context.Background(), minimalVolumeSpec(volName), volName)
				Expect(err).NotTo(HaveOccurred())
			}

			page1, err := s.List(context.Background(), 2, "")
			Expect(err).NotTo(HaveOccurred())
			Expect(*page1.Volumes).To(HaveLen(2))
			Expect(page1.NextPageToken).NotTo(BeNil())

			page2, err := s.List(context.Background(), 2, *page1.NextPageToken)
			Expect(err).NotTo(HaveOccurred())
			Expect(*page2.Volumes).To(HaveLen(1))
			Expect(page2.NextPageToken).To(BeNil())
		})

		It("rejects invalid page tokens", func() {
			s, _ := newTestStore(defaultConfig())
			_, err := s.List(context.Background(), 10, "!!!")
			Expect(err).To(HaveOccurred())
			var invalid *store.InvalidArgumentError
			Expect(errors.As(err, &invalid)).To(BeTrue())
		})

		DescribeTable("clamps max page size",
			func(maxPageSize int32, wantLimit int64) {
				s, client := newTestStore(defaultConfig())
				var gotLimit int64
				client.PrependReactor("list", "persistentvolumeclaims", func(action clienttesting.Action) (bool, runtime.Object, error) {
					gotLimit = action.(clienttesting.ListActionImpl).ListOptions.Limit
					return false, nil, nil
				})

				_, err := s.List(context.Background(), maxPageSize, "")
				Expect(err).NotTo(HaveOccurred())
				Expect(gotLimit).To(Equal(wantLimit))
			},
			Entry("zero uses default", int32(0), int64(50)),
			Entry("negative uses default", int32(-1), int64(50)),
			Entry("within range unchanged", int32(50), int64(50)),
			Entry("at max unchanged", int32(1000), int64(1000)),
			Entry("above max clamps", int32(1001), int64(1000)),
			Entry("far above max clamps", int32(5000), int64(1000)),
		)
	})

	Describe("Delete", func() {
		It("deletes the PVC (TC-U069)", func() {
			s, client := newTestStore(defaultConfig())
			_, err := s.Create(context.Background(), minimalVolumeSpec("app-data"), "app-data")
			Expect(err).NotTo(HaveOccurred())

			err = s.Delete(context.Background(), "app-data")
			Expect(err).NotTo(HaveOccurred())

			_, err = client.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), "app-data", metav1.GetOptions{})
			Expect(err).To(HaveOccurred())
		})

		It("returns not found when missing (TC-U070)", func() {
			s, _ := newTestStore(defaultConfig())
			err := s.Delete(context.Background(), "missing")
			Expect(err).To(HaveOccurred())
			var notFound *store.NotFoundError
			Expect(errors.As(err, &notFound)).To(BeTrue())
		})

		It("succeeds when PVC is deleted between get and delete", func() {
			s, client := newTestStore(defaultConfig())
			_, err := s.Create(context.Background(), minimalVolumeSpec("app-data"), "app-data")
			Expect(err).NotTo(HaveOccurred())

			client.PrependReactor("delete", "persistentvolumeclaims", func(clienttesting.Action) (bool, runtime.Object, error) {
				return true, nil, apierrors.NewNotFound(schema.GroupResource{Resource: "persistentvolumeclaims"}, "app-data")
			})

			err = s.Delete(context.Background(), "app-data")
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("MapPVCToStatus", func() {
		It("maps Pending to PROVISIONING", func() {
			pvc := &corev1.PersistentVolumeClaim{Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending}}
			status, _ := k8sstore.MapPVCToStatus(pvc)
			Expect(status).To(Equal(v1alpha1.PROVISIONING))
		})

		It("maps Bound to RUNNING", func() {
			pvc := &corev1.PersistentVolumeClaim{Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound}}
			status, _ := k8sstore.MapPVCToStatus(pvc)
			Expect(status).To(Equal(v1alpha1.RUNNING))
		})

		It("maps Lost to FAILED (TC-U033)", func() {
			pvc := &corev1.PersistentVolumeClaim{Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimLost}}
			status, _ := k8sstore.MapPVCToStatus(pvc)
			Expect(status).To(Equal(v1alpha1.FAILED))
		})

		It("maps deleting PVC to DELETING (TC-U034)", func() {
			now := metav1.Now()
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &now},
				Status:     corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
			}
			status, _ := k8sstore.MapPVCToStatus(pvc)
			Expect(status).To(Equal(v1alpha1.DELETING))
		})

		It("maps Bound with resize condition to PROVISIONING", func() {
			pvc := &corev1.PersistentVolumeClaim{
				Status: corev1.PersistentVolumeClaimStatus{
					Phase: corev1.ClaimBound,
					Conditions: []corev1.PersistentVolumeClaimCondition{{
						Type:   corev1.PersistentVolumeClaimResizing,
						Status: corev1.ConditionTrue,
					}},
				},
			}
			status, _ := k8sstore.MapPVCToStatus(pvc)
			Expect(status).To(Equal(v1alpha1.PROVISIONING))
		})

		It("maps nil PVC to DELETED", func() {
			status, _ := k8sstore.MapPVCToStatus(nil)
			Expect(status).To(Equal(v1alpha1.DELETED))
		})
	})
})
