package k8s

import (
	"context"
	"fmt"
	"os"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type Manager struct {
	clientset *kubernetes.Clientset
}

func NewManager() (*Manager, error) {
	var config *rest.Config
	var err error

	// Try in-cluster config first
	config, err = rest.InClusterConfig()
	if err != nil {
		// Fallback to local kubeconfig
		kubeconfig := os.Getenv("KUBECONFIG")
		if kubeconfig == "" {
			kubeconfig = os.Getenv("HOME") + "/.kube/config"
		}
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, fmt.Errorf("failed to get kubernetes config: %v", err)
		}
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create clientset: %v", err)
	}

	return &Manager{clientset: clientset}, nil
}

func (m *Manager) CreateNetworkPolicy(ctx context.Context, namespace, submissionID string) error {
	policy := &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("isolate-%s", submissionID),
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{
				MatchLabels: map[string]string{
					"submission-id": submissionID,
				},
			},
			PolicyTypes: []networkingv1.PolicyType{
				networkingv1.PolicyTypeIngress,
				networkingv1.PolicyTypeEgress,
			},
			Ingress: []networkingv1.NetworkPolicyIngressRule{
				{
					// Only allow traffic from the Load Generator
					From: []networkingv1.NetworkPolicyPeer{
						{
							PodSelector: &metav1.LabelSelector{
								MatchLabels: map[string]string{
									"app": "trade-bench-load-generator",
								},
							},
						},
					},
					Ports: []networkingv1.NetworkPolicyPort{
						{
							Protocol: protoPtr(corev1.ProtocolTCP),
							Port:     &intstr.IntOrString{IntVal: 8080},
						},
					},
				},
			},
			Egress: []networkingv1.NetworkPolicyEgressRule{
				// Deny all egress by default (no rules means deny)
			},
		},
	}

	_, err := m.clientset.NetworkingV1().NetworkPolicies(namespace).Create(ctx, policy, metav1.CreateOptions{})
	return err
}

func (m *Manager) DeploySubmission(ctx context.Context, namespace, submissionID, image string) error {
	// For "Guaranteed" QoS class (needed for potential CPU pinning), Requests must equal Limits.
	cpuLimit := resource.MustParse("1")
	memLimit := resource.MustParse("1Gi")

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("sub-%s", submissionID),
			Labels: map[string]string{
				"app":           "trade-bench-submission",
				"submission-id": submissionID,
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: int32Ptr(1),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"submission-id": submissionID,
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"submission-id": submissionID,
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "contestant-app",
							Image: image,
							Resources: corev1.ResourceRequirements{
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    cpuLimit,
									corev1.ResourceMemory: memLimit,
								},
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    cpuLimit,
									corev1.ResourceMemory: memLimit,
								},
							},
							Ports: []corev1.ContainerPort{
								{
									ContainerPort: 8080,
								},
							},
							SecurityContext: &corev1.SecurityContext{
								AllowPrivilegeEscalation: boolPtr(false),
								ReadOnlyRootFilesystem:   boolPtr(true),
								RunAsNonRoot:             boolPtr(true),
								RunAsUser:                int64Ptr(1000),
								Capabilities: &corev1.Capabilities{
									Drop: []corev1.Capability{"ALL"},
								},
								SeccompProfile: &corev1.SeccompProfile{
									Type: corev1.SeccompProfileTypeRuntimeDefault,
								},
							},
						},
					},
				},
			},
		},
	}

	_, err := m.clientset.AppsV1().Deployments(namespace).Create(ctx, deployment, metav1.CreateOptions{})
	return err
}

func (m *Manager) DeployLoadGenerator(ctx context.Context, namespace, submissionID, natsURL string, botCount int) error {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("load-gen-%s", submissionID),
			Labels: map[string]string{
				"app":           "trade-bench-load-generator",
				"submission-id": submissionID,
			},
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "load-generator",
							Image: "trade-bench-load-generator:latest", // Assumes image is available
							Env: []corev1.EnvVar{
								{Name: "SUBMISSION_ID", Value: submissionID},
								{Name: "BOT_COUNT", Value: fmt.Sprintf("%d", botCount)},
								{Name: "NATS_URL", Value: natsURL},
								{Name: "TARGET_URL", Value: fmt.Sprintf("http://sub-%s:8080", submissionID)},
								{Name: "DURATION", Value: "5m"},
							},
						},
					},
					RestartPolicy: corev1.RestartPolicyNever,
				},
			},
		},
	}

	_, err := m.clientset.BatchV1().Jobs(namespace).Create(ctx, job, metav1.CreateOptions{})
	return err
}

func int32Ptr(i int32) *int32 { return &i }
func int64Ptr(i int64) *int64 { return &i }
func boolPtr(b bool) *bool    { return &b }
func protoPtr(p corev1.Protocol) *corev1.Protocol { return &p }
