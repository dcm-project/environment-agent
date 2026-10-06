package kubernetes_test

import (
	k8sstore "github.com/dcm-project/environment-agent/internal/openshift/network/kubernetes"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
)

var _ = DescribeTable("BuildOutputSpec",
	func(svc *corev1.Service, expected map[string]any) {
		Expect(k8sstore.BuildOutputSpec(svc)).To(Equal(expected))
	},
	Entry("TC-U125: LoadBalancer IP + named port -> one endpoint",
		&corev1.Service{
			Spec: corev1.ServiceSpec{
				Type:  corev1.ServiceTypeLoadBalancer,
				Ports: []corev1.ServicePort{{Name: "http", Port: 80, Protocol: corev1.ProtocolTCP}},
			},
			Status: corev1.ServiceStatus{LoadBalancer: corev1.LoadBalancerStatus{
				Ingress: []corev1.LoadBalancerIngress{{IP: "34.123.45.67"}},
			}},
		},
		map[string]any{"endpoints": []map[string]any{
			{"address": "34.123.45.67", "port": 80, "scope": "external", "name": "http", "protocol": "TCP"},
		}},
	),
	Entry("TC-U126: hostname ingress with no IP -> address is the hostname",
		&corev1.Service{
			Spec: corev1.ServiceSpec{
				Type:  corev1.ServiceTypeLoadBalancer,
				Ports: []corev1.ServicePort{{Port: 80}},
			},
			Status: corev1.ServiceStatus{LoadBalancer: corev1.LoadBalancerStatus{
				Ingress: []corev1.LoadBalancerIngress{{Hostname: "lb.example.com"}},
			}},
		},
		map[string]any{"endpoints": []map[string]any{
			{"address": "lb.example.com", "port": 80, "scope": "external"},
		}},
	),
	Entry("TC-U127: non-LoadBalancer service -> nil",
		&corev1.Service{
			Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP, Ports: []corev1.ServicePort{{Port: 80}}},
		},
		nil,
	),
	Entry("TC-U128: LoadBalancer with no ingress assigned yet -> nil",
		&corev1.Service{
			Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer, Ports: []corev1.ServicePort{{Port: 80}}},
		},
		nil,
	),
)

var _ = It("TC-U129: composes the cross product of external IPs and ports", func() {
	svc := &corev1.Service{
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeLoadBalancer,
			Ports: []corev1.ServicePort{
				{Name: "http", Port: 80, Protocol: corev1.ProtocolTCP},
				{Name: "metrics", Port: 9090, Protocol: corev1.ProtocolTCP},
			},
		},
		Status: corev1.ServiceStatus{LoadBalancer: corev1.LoadBalancerStatus{
			Ingress: []corev1.LoadBalancerIngress{{IP: "34.123.45.67"}, {IP: "34.123.45.68"}},
		}},
	}

	outputSpec := k8sstore.BuildOutputSpec(svc)

	Expect(outputSpec).NotTo(BeNil())
	Expect(outputSpec["endpoints"]).To(HaveLen(4))
})
