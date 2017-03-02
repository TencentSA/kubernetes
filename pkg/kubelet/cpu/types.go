package cpu

import "k8s.io/kubernetes/pkg/api"

// CPUManager manages CPUs on a local node.
// Implementations are expected to be thread safe.
type CPUManager interface {
	// Start logically initializes CPUManager
	Start(node *api.Node) error
	// AllocateCPU attempts to allocate CPUs for input container.
	// Returns CPUs on success.
	// Returns an error on failure.
	AllocateCPU(pod *api.Pod, container *api.Container) (map[string]string, error)
}
