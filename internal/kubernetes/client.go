package kube

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	NamespaceFilePath    = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
	NamespaceOverrideEnv = "HOME_BACKUP_NAMESPACE"
	PodNameOverrideEnv   = "HOME_BACKUP_POD_NAME"
	CronJobNameEnv       = "HOME_BACKUP_POD_TEMPLATE_CRONJOB"
)

type Clients struct {
	Core    kubernetes.Interface
	Dynamic dynamic.Interface
}

func NewClients() (*Clients, error) {
	cfg, err := loadRESTConfig(rest.InClusterConfig, kubeconfig)
	if err != nil {
		return nil, err
	}
	core, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("creating Kubernetes clientset: %w", err)
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("creating Kubernetes dynamic client: %w", err)
	}
	return &Clients{Core: core, Dynamic: dyn}, nil
}

type restConfigLoader func() (*rest.Config, error)

func loadRESTConfig(inCluster, local restConfigLoader) (*rest.Config, error) {
	cfg, err := inCluster()
	if err == nil {
		return cfg, nil
	}
	if !errors.Is(err, rest.ErrNotInCluster) {
		return nil, fmt.Errorf("creating in-cluster Kubernetes config: %w", err)
	}
	cfg, err = local()
	if err != nil {
		return nil, fmt.Errorf("creating local Kubernetes config: %w", err)
	}
	return cfg, nil
}

func CurrentNamespace() (string, error) {
	if namespace := strings.TrimSpace(os.Getenv(NamespaceOverrideEnv)); namespace != "" {
		return namespace, nil
	}
	bytes, err := os.ReadFile(NamespaceFilePath)
	if err != nil {
		return "", fmt.Errorf("reading namespace from %s: %w", NamespaceFilePath, err)
	}
	namespace := strings.TrimSpace(string(bytes))
	if namespace == "" {
		return "", fmt.Errorf("namespace file %s is empty", NamespaceFilePath)
	}
	return namespace, nil
}

func CurrentPodName() (string, error) {
	if podName := strings.TrimSpace(os.Getenv(PodNameOverrideEnv)); podName != "" {
		return podName, nil
	}
	podName, err := os.Hostname()
	if err != nil {
		return "", fmt.Errorf("getting current pod hostname: %w", err)
	}
	if podName == "" {
		return "", fmt.Errorf("current pod hostname is empty")
	}
	return podName, nil
}

func ResolveCronJob(ctx context.Context, clients *Clients, namespace string) (*batchv1.CronJob, error) {
	if cronJobName := strings.TrimSpace(os.Getenv(CronJobNameEnv)); cronJobName != "" {
		cronJob, err := clients.Core.BatchV1().CronJobs(namespace).Get(ctx, cronJobName, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("getting CronJob %s/%s: %w", namespace, cronJobName, err)
		}
		return cronJob, nil
	}

	podName, err := CurrentPodName()
	if err != nil {
		return nil, err
	}
	pod, err := clients.Core.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("getting current Pod %s/%s: %w", namespace, podName, err)
	}
	jobOwner, err := controllerOwner(pod.OwnerReferences, "Job")
	if err != nil {
		return nil, fmt.Errorf("detecting Job for Pod %s/%s: %w", namespace, podName, err)
	}
	job, err := clients.Core.BatchV1().Jobs(namespace).Get(ctx, jobOwner.Name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("getting Job %s/%s: %w", namespace, jobOwner.Name, err)
	}
	cronJobOwner, err := controllerOwner(job.OwnerReferences, "CronJob")
	if err != nil {
		return nil, fmt.Errorf("detecting CronJob for Job %s/%s: %w", namespace, job.Name, err)
	}
	cronJob, err := clients.Core.BatchV1().CronJobs(namespace).Get(ctx, cronJobOwner.Name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("getting CronJob %s/%s: %w", namespace, cronJobOwner.Name, err)
	}
	return cronJob, nil
}

func controllerOwner(owners []metav1.OwnerReference, kind string) (*metav1.OwnerReference, error) {
	for i := range owners {
		if owners[i].Kind == kind && owners[i].Controller != nil && *owners[i].Controller {
			return &owners[i], nil
		}
	}
	return nil, fmt.Errorf("no controlling %s owner reference", kind)
}

func kubeconfig() (*rest.Config, error) {
	kubeconfigPath := os.Getenv("KUBECONFIG")
	if kubeconfigPath == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, err
		}
		kubeconfigPath = filepath.Join(home, ".kube", "config")
	}
	return clientcmd.BuildConfigFromFlags("", kubeconfigPath)
}
