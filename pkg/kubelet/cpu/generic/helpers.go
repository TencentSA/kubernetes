package generic

import (
	"k8s.io/kubernetes/pkg/api"
)

const (
	// 24 core perl node
	defaultNodeCPUs int64 = 24
)

func getNodeCPUs(resource *api.ResourceList) int64 {
	var outCPUs int64
	// Override if un-set, but not if explicitly set to zero
	if _, found := (*resource)[api.ResourceCPU]; !found {
		outCPUs = defaultNodeCPUs
	} else {
		outCPUs = resource.Cpu().Value()
	}
	return outCPUs
}
