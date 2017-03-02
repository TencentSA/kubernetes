package generic

import (
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/golang/glog"
	"github.com/hustcat/go-lib/bitmap"
	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/kubelet/cpu"
	"k8s.io/kubernetes/pkg/kubelet/dockertools"
	"k8s.io/kubernetes/pkg/kubelet/leaky"
	"k8s.io/kubernetes/pkg/util/sets"
)

const (
	PodInfraContainerName = leaky.PodInfraContainerName
)

type NUMAInfo struct {
	// Available node number
	Nodes int `json:"nodes"`
	// Numa distribution type
	// For example:
	// E5-2630 (Topology == 0)
	// node0: [0,1,2,3,4,5,12,13,14,15,16,17]
	// node1: [6,7,8,9,10,11,18,19,20,21,22,23]
	//
	// E5-2670 (Topology == 1)
	// node0: [0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30]
	// node1: [1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31]
	Topology int `json:"topology"`
}

type activePodsLister interface {
	// Returns a list of active pods on the node.
	GetActivePods() []*api.Pod
}

// CPUManager manages cpus.
type genericCPUManager struct {
	sync.Mutex
	node             *api.Node
	numaInfo         *NUMAInfo
	dockerClient     dockertools.DockerInterface
	activePodsLister activePodsLister
}

// NewCPUManager returns a CPUManager that manages local CPUs.
func NewGenericCPUManager(activePodsLister activePodsLister, dockerClient dockertools.DockerInterface) (cpu.CPUManager, error) {
	if dockerClient == nil {
		return nil, fmt.Errorf("invalid docker client specified")
	}
	return &genericCPUManager{
		dockerClient:     dockerClient,
		activePodsLister: activePodsLister,
	}, nil
}

func (gcm *genericCPUManager) Start(node *api.Node) error {
	if gcm.dockerClient == nil {
		return fmt.Errorf("Invalid docker client specified in CPU Manager")
	}
	gcm.Lock()
	defer gcm.Unlock()

	gcm.node = node
	// Initialize the numa info
	if err := gcm.generateNUMAInfo(); err != nil {
		glog.Errorf("Generate numa info err: %v", err)
		return err
	}
	return nil
}

// Return the CPU as needed, otherwise, return error.
// We only need to set the CPUSet of network containers, because cni/sriov need CPUSet information.
// And the app containers with network share the same set of CPUSet
func (gcm *genericCPUManager) AllocateCPU(pod *api.Pod, container *api.Container) (map[string]string, error) {
	var cpusNeeded int64 = 0
	if container.Name != PodInfraContainerName {
		return nil, nil
	}
	for _, container := range pod.Spec.Containers {
		cpusNeeded += container.Resources.Limits.Cpu().Value()
	}
	if cpusNeeded == 0 {
		return nil, nil
	}
	gcm.Lock()
	defer gcm.Unlock()
	nodeCPUs := getNodeCPUs(&gcm.node.Status.Allocatable)
	cpuMap := bitmap.NewNumaBitmapSize(uint(nodeCPUs), gcm.numaInfo.Nodes)
	// Calculate the cpus all have been used
	for _, c := range gcm.cpusInUse2() {
		coreNo, err := strconv.Atoi(c)
		if err != nil {
			glog.Errorf("Failed to allocate cpus to pod %s: %v", pod.Name, err)
			continue
		}
		cpuMap.SetBit(uint(coreNo), 1)
	}
	ret := make(map[string]string)
	cpuset, err := gcm.cpusAvailable(cpusNeeded, cpuMap)
	if err != nil {
		return nil, err
	}
	glog.V(3).Infof("Allocate cpuset ( %s ) to pod %s", cpuset, pod.Name)
	ret["cpus"] = cpuset
	return ret, nil
}

func (gcm *genericCPUManager) cpusAvailable(requestCPUs int64, cpuMap *bitmap.NumaBitmap) (string, error) {
	var (
		cpuset    []string
		err       error
		freeCores [][]uint
	)
	if gcm.numaInfo.Topology == 1 {
		freeCores, err = cpuMap.Get0BitOffsNumaVer(uint(gcm.numaInfo.Nodes))
	} else {
		freeCores, err = cpuMap.Get0BitOffsNuma(uint(gcm.numaInfo.Nodes))
	}
	if err != nil {
		return "", err
	}
	// Not numa cpus
	freeCores = append(freeCores, cpuMap.Get0BitOffs())
	for i := 0; i < len(freeCores); i++ {
		offs := freeCores[i]
		if int64(len(offs)) >= requestCPUs {
			for j := int64(0); j < requestCPUs; j++ {
				off := offs[j]
				cpuset = append(cpuset, strconv.Itoa(int(off)))
			}
			break
		}
	}
	if int64(len(cpuset)) == requestCPUs {
		return strings.Join(cpuset, ","), nil
	}
	return "", fmt.Errorf("Don't have enough available CPU")
}

func (gcm *genericCPUManager) cpusInUse2() []string {
	var ret []string
	containers, err := dockertools.GetKubeletDockerContainers(gcm.dockerClient, false)
	if err != nil {
		glog.Errorf("Get running container failed: %v", err)
		return ret
	}
	for i := range containers {
		containerJSON, err := gcm.dockerClient.InspectContainer(containers[i].ID)
		if err != nil {
			glog.V(5).Infof("Failed to inspect container %v while attempting to reconcile cpu in use: %v", containers[i].ID, err)
			continue
		}
		glog.V(5).Infof("Container %v cpus: %v", containers[i].ID, containerJSON.HostConfig.CpusetCpus)
		if containerJSON.HostConfig.CpusetCpus != "" {
			for _, c := range strings.Split(containerJSON.HostConfig.CpusetCpus, ",") {
				ret = append(ret, c)
			}
		}
	}
	return ret
}

func (gcm *genericCPUManager) cpusInUse() []string {
	pods := gcm.activePodsLister.GetActivePods()
	type containerIdentifier struct {
		id   string
		name string
	}
	type podContainers struct {
		uid        string
		containers []containerIdentifier
	}
	// List of containers to inspect.
	podContainersToInspect := []podContainers{}
	for _, pod := range pods {
		containers := sets.NewString()
		for _, container := range pod.Spec.Containers {
			// CPUs are expected to be specified only in limits.
			if !container.Resources.Limits.Cpu().IsZero() {
				containers.Insert(container.Name)
			} else if !container.Resources.Requests.Cpu().IsZero() {
				containers.Insert(container.Name)
			}
		}
		// If no CPUs were requested skip this pod.
		if containers.Len() == 0 {
			continue
		}
		// TODO: If kubelet restarts right after allocating a CPU to a pod, the container might not have started yet and so container status might not be available yet.
		// Use an internal checkpoint instead or try using the CRI if its checkpoint is reliable.
		var containersToInspect []containerIdentifier
		for _, container := range pod.Status.ContainerStatuses {
			if containers.Has(container.Name) && container.ContainerID != "" {
				containerID := strings.Replace(container.ContainerID, "docker://", "", -1)
				containersToInspect = append(containersToInspect, containerIdentifier{containerID, container.Name})
			}
		}
		// add the pod and its containers that need to be inspected.
		podContainersToInspect = append(podContainersToInspect, podContainers{string(pod.UID), containersToInspect})
	}
	var ret []string
	for _, podContainer := range podContainersToInspect {
		for _, containerIdentifier := range podContainer.containers {
			containerJSON, err := gcm.dockerClient.InspectContainer(containerIdentifier.id)
			if err != nil {
				glog.V(3).Infof("Failed to inspect container %q in pod %q while attempting to reconcile cpu in use: %v", containerIdentifier.id, podContainer.uid, err)
				continue
			}
			for _, c := range strings.Split(containerJSON.HostConfig.CpusetCpus, ",") {
				ret = append(ret, c)
			}
		}
	}
	return ret
}

func (gcm *genericCPUManager) generateNUMAInfo() error {
	// match numa node. e,g. "available: 2 nodes (0-1)"
	availableNodesRegexp := regexp.MustCompile(`\w+:\s*(\d)`)
	out, err := exec.Command("numactl", "--hardware").CombinedOutput()
	if err != nil {
		return err
	}
	gcm.numaInfo = &NUMAInfo{
		Nodes:    2,
		Topology: 0,
	}
	parts := strings.Split(string(out), "\n")
	if len(parts) <= 0 {
		return fmt.Errorf("numa information is empty")
	}
	match := availableNodesRegexp.FindStringSubmatch(parts[0])
	if len(match) < 2 {
		return fmt.Errorf("Failed to get numa nodes")
	}
	nodes, err := strconv.Atoi(match[1])
	if err != nil {
		return fmt.Errorf("Failed to get numa nodes: %v", err)
	}
	gcm.numaInfo.Nodes = nodes
	for _, item := range parts {
		if strings.Contains(item, "cpus") {
			if ok, _ := regexp.Match(`\b0\s+1\b`, []byte(item)); !ok {
				gcm.numaInfo.Topology = 1
			}
			break
		}
	}
	glog.V(3).Infof("NUMAInfo: %+v", gcm.numaInfo)
	return nil
}
