/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cpumanager

import (
	"fmt"
	"strconv"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog"
	v1qos "k8s.io/kubernetes/pkg/apis/core/v1/helper/qos"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/state"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/topology"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpuset"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager/bitmask"
	"k8s.io/kubernetes/pkg/kubelet/cm/devicemanager"
)

// PolicyStatic is the name of the static policy
const PolicyStatic policyName = "static"

// Define namespaces used by platform infrastructure pods
var infraNamespaces = [...]string{
	"kube-system", "armada", "cert-manager", "platform-deployment-manager", "portieris", "vault",
}

// staticPolicy is a CPU manager policy that does not change CPU
// assignments for exclusively pinned guaranteed containers after the main
// container process starts.
//
// This policy allocates CPUs exclusively for a container if all the following
// conditions are met:
//
// - The pod QoS class is Guaranteed.
// - The CPU request is a positive integer.
//
// The static policy maintains the following sets of logical CPUs:
//
// - SHARED: Burstable, BestEffort, and non-integral Guaranteed containers
//   run here. Initially this contains all CPU IDs on the system. As
//   exclusive allocations are created and destroyed, this CPU set shrinks
//   and grows, accordingly. This is stored in the state as the default
//   CPU set.
//
// - RESERVED: A subset of the shared pool which is not exclusively
//   allocatable. The membership of this pool is static for the lifetime of
//   the Kubelet. The size of the reserved pool is
//   ceil(systemreserved.cpu + kubereserved.cpu).
//   Reserved CPUs are taken topologically starting with lowest-indexed
//   physical core, as reported by cAdvisor.
//
// - ASSIGNABLE: Equal to SHARED - RESERVED. Exclusive CPUs are allocated
//   from this pool.
//
// - EXCLUSIVE ALLOCATIONS: CPU sets assigned exclusively to one container.
//   These are stored as explicit assignments in the state.
//
// When an exclusive allocation is made, the static policy also updates the
// default cpuset in the state abstraction. The CPU manager's periodic
// reconcile loop takes care of rewriting the cpuset in cgroupfs for any
// containers that may be running in the shared pool. For this reason,
// applications running within exclusively-allocated containers must tolerate
// potentially sharing their allocated CPUs for up to the CPU manager
// reconcile period.
type staticPolicy struct {
	// cpu socket topology
	topology *topology.CPUTopology
	// set of CPUs that is not available for exclusive assignment
	reserved cpuset.CPUSet
	// subset of reserved CPUs with isolcpus attribute
	isolcpus cpuset.CPUSet
	// parent containerManager, used to get device list
	deviceManager devicemanager.Manager
	// If true, default CPUSet should exclude reserved CPUs
	excludeReserved bool
	// topology manager reference to get container Topology affinity
	affinity topologymanager.Store
}

// Ensure staticPolicy implements Policy interface
var _ Policy = &staticPolicy{}

// NewStaticPolicy returns a CPU manager policy that does not change CPU
// assignments for exclusively pinned guaranteed containers after the main
// container process starts.
func NewStaticPolicy(topology *topology.CPUTopology, numReservedCPUs int, reservedCPUs cpuset.CPUSet, isolCPUs cpuset.CPUSet, affinity topologymanager.Store, deviceManager devicemanager.Manager, excludeReserved bool) (Policy, error) {
	allCPUs := topology.CPUDetails.CPUs()
	var reserved cpuset.CPUSet
	if reservedCPUs.Size() > 0 {
		reserved = reservedCPUs
	} else {
		// takeByTopology allocates CPUs associated with low-numbered cores from
		// allCPUs.
		//
		// For example: Given a system with 8 CPUs available and HT enabled,
		// if numReservedCPUs=2, then reserved={0,4}
		reserved, _ = takeByTopology(topology, allCPUs, numReservedCPUs)
	}

	if reserved.Size() != numReservedCPUs {
		err := fmt.Errorf("[cpumanager] unable to reserve the required amount of CPUs (size of %s did not equal %d)", reserved, numReservedCPUs)
		return nil, err
	}

	klog.Infof("[cpumanager] reserved %d CPUs (\"%s\") not available for exclusive assignment", reserved.Size(), reserved)

	if !isolCPUs.IsSubsetOf(reserved) {
		klog.Errorf("[cpumanager] isolCPUs %v is not a subset of reserved %v", isolCPUs, reserved)
		reserved = reserved.Union(isolCPUs)
		klog.Warningf("[cpumanager] mismatch isolCPUs %v, force reserved %v", isolCPUs, reserved)
	}

	return &staticPolicy{
		topology: topology,
		reserved: reserved,
		isolcpus: isolCPUs,
		deviceManager: deviceManager,
		excludeReserved: excludeReserved,
		affinity: affinity,
	}, nil
}

func (p *staticPolicy) Name() string {
	return string(PolicyStatic)
}

func (p *staticPolicy) Start(s state.State) error {
	if err := p.validateState(s); err != nil {
		klog.Errorf("[cpumanager] static policy invalid state: %v, please drain node and remove policy state file", err)
		return err
	}
	return nil
}

func (p *staticPolicy) validateState(s state.State) error {
	tmpAssignments := s.GetCPUAssignments()
	tmpDefaultCPUset := s.GetDefaultCPUSet()

	// Default cpuset cannot be empty when assignments exist
	if tmpDefaultCPUset.IsEmpty() {
		if len(tmpAssignments) != 0 {
			return fmt.Errorf("default cpuset cannot be empty")
		}
		// state is empty initialize
		allCPUs := p.topology.CPUDetails.CPUs()
		if p.excludeReserved {
			// Exclude reserved CPUs from the default CPUSet to keep containers off them
			// unless explicitly affined.
			s.SetDefaultCPUSet(allCPUs.Difference(p.reserved))
		} else {
			s.SetDefaultCPUSet(allCPUs)
		}
		klog.Infof("[cpumanager] static policy: CPUSet: allCPUs:%v, reserved:%v, isolcpus:%v, default:%v\n",
			allCPUs, p.reserved, p.isolcpus, s.GetDefaultCPUSet())
		return nil
	}

	// State has already been initialized from file (is not empty)
	// 1. Check if the reserved cpuset is not part of default cpuset because:
	// - kube/system reserved have changed (increased) - may lead to some containers not being able to start
	// - user tampered with file
	if !p.excludeReserved {
		if !p.reserved.Intersection(tmpDefaultCPUset).Equals(p.reserved) {
			return fmt.Errorf("not all reserved cpus: \"%s\" are present in defaultCpuSet: \"%s\"",
				p.reserved.String(), tmpDefaultCPUset.String())
		}
	}

	// 2. Check if state for static policy is consistent
	for pod := range tmpAssignments {
		for container, cset := range tmpAssignments[pod] {
			// None of the cpu in DEFAULT cset should be in s.assignments
			if !tmpDefaultCPUset.Intersection(cset).IsEmpty() {
				return fmt.Errorf("pod: %s, container: %s cpuset: \"%s\" overlaps with default cpuset \"%s\"",
					pod, container, cset.String(), tmpDefaultCPUset.String())
			}
		}
	}

	// 3. It's possible that the set of available CPUs has changed since
	// the state was written. This can be due to for example
	// offlining a CPU when kubelet is not running. If this happens,
	// CPU manager will run into trouble when later it tries to
	// assign non-existent CPUs to containers. Validate that the
	// topology that was received during CPU manager startup matches with
	// the set of CPUs stored in the state.
	totalKnownCPUs := tmpDefaultCPUset.Clone()
	tmpCPUSets := []cpuset.CPUSet{}
	for pod := range tmpAssignments {
		for _, cset := range tmpAssignments[pod] {
			tmpCPUSets = append(tmpCPUSets, cset)
		}
	}
	totalKnownCPUs = totalKnownCPUs.UnionAll(tmpCPUSets)
	if p.excludeReserved {
		totalKnownCPUs = totalKnownCPUs.Union(p.reserved)
	}
	if !totalKnownCPUs.Equals(p.topology.CPUDetails.CPUs()) {
		return fmt.Errorf("current set of available CPUs \"%s\" doesn't match with CPUs in state \"%s\"",
			p.topology.CPUDetails.CPUs().String(), totalKnownCPUs.String())
	}

	return nil
}

// assignableCPUs returns the set of unassigned CPUs minus the reserved set.
func (p *staticPolicy) assignableCPUs(s state.State) cpuset.CPUSet {
	return s.GetDefaultCPUSet().Difference(p.reserved)
}

func (p *staticPolicy) Allocate(s state.State, pod *v1.Pod, container *v1.Container) error {
	// Process infra pods before guaranteed pods
	if isKubeInfra(pod) {
		// Container belongs in reserved pool.
		// We don't want to fall through to the p.guaranteedCPUs() clause below so return either nil or error.
		if _, ok := s.GetCPUSet(string(pod.UID), container.Name); ok {
				klog.Infof("[cpumanager] static policy: reserved container already present in state, skipping " +
					"(namespace: %s, pod UID: %s, pod: %s, container: %s)",
					pod.Namespace, string(pod.UID), pod.Name, container.Name)
				return nil
		}

		// TODO: Is the clone actually needed?
		cpuset := p.reserved.Clone().Difference(p.isolcpus)
		if cpuset.IsEmpty() {
			// If this happens then someone messed up.
			return fmt.Errorf("[cpumanager] static policy: reserved container unable to allocate cpus " +
				"(namespace: %s, pod UID: %s, pod: %s, container: %s); cpuset=%v, reserved:%v, isolcpus:%v",
				pod.Namespace, string(pod.UID), pod.Name, container.Name, cpuset, p.reserved, p.isolcpus)
		}
		s.SetCPUSet(string(pod.UID), container.Name, cpuset)
		klog.Infof("[cpumanager] static policy: reserved: AddContainer " +
			"(namespace: %s, pod UID: %s, pod: %s, container: %s); cpuset=%v",
			pod.Namespace, string(pod.UID), pod.Name, container.Name, cpuset)
		return nil
	}


	if numCPUs := p.guaranteedCPUs(pod, container); numCPUs != 0 {
		klog.Infof("[cpumanager] static policy: Allocate (pod: %s, container: %s)", pod.Name, container.Name)
		// container belongs in an exclusively allocated pool

		if _, ok := s.GetCPUSet(string(pod.UID), container.Name); ok {
			klog.Infof("[cpumanager] static policy: container already present in state, skipping (pod: %s, container: %s)", pod.Name, container.Name)
			return nil
		}

		// Call Topology Manager to get the aligned socket affinity across all hint providers.
		hint := p.affinity.GetAffinity(string(pod.UID), container.Name)
		klog.Infof("[cpumanager] Pod %v, Container %v Topology Affinity is: %v", pod.UID, container.Name, hint)

		// Allocate CPUs according to the NUMA affinity contained in the hint.
		cpuset, err := p.allocateCPUs(s, numCPUs, hint.NUMANodeAffinity)
		if err != nil {
			klog.Errorf("[cpumanager] unable to allocate %d CPUs (pod: %s, container: %s, error: %v)", numCPUs, pod.Name, container.Name, err)
			return err
		}
		s.SetCPUSet(string(pod.UID), container.Name, cpuset)

		// Check if the container that has just been allocated resources is an init container.
		// If so, release its CPUs back into the shared pool so they can be reallocated.
		for _, initContainer := range pod.Spec.InitContainers {
			if container.Name == initContainer.Name {
				if toRelease, ok := s.GetCPUSet(string(pod.UID), container.Name); ok {
					// Mutate the shared pool, adding released cpus.
					s.SetDefaultCPUSet(s.GetDefaultCPUSet().Union(toRelease))
				}
			}
		}
		klog.Infof("[cpumanager] guaranteed: AddContainer " +
					"(namespace: %s, pod UID: %s, pod: %s, container: %s); numCPUS=%d, cpuset=%v",
					pod.Namespace, string(pod.UID), pod.Name, container.Name, numCPUs, cpuset)
		return nil
	}

	if isolcpus := p.podIsolCPUs(pod, container); isolcpus.Size() > 0 {
		// container has requested isolated CPUs
		if set, ok := s.GetCPUSet(string(pod.UID), container.Name); ok {
			if set.Equals(isolcpus) {
				klog.Infof("[cpumanager] isolcpus container already present in state, skipping " +
							"(namespace: %s, pod UID: %s, pod: %s, container: %s)",
							pod.Namespace, string(pod.UID), pod.Name, container.Name)
				return nil
			} else {
				klog.Infof("[cpumanager] isolcpus container state has cpus %v, should be %v" +
							"(namespace: %s, pod UID: %s, pod: %s, container: %s)",
							isolcpus, set, pod.Namespace, string(pod.UID), pod.Name, container.Name)
			}
		}
		// Note that we do not do anything about init containers here.
		// It looks like devices are allocated per-pod based on effective requests/limits
		// and extra devices from initContainers are not freed up when the regular containers start.
		// TODO: confirm this is still true for 1.18
		s.SetCPUSet(string(pod.UID), container.Name, isolcpus)
		klog.Infof("[cpumanager] isolcpus: AddContainer " +
					"(namespace: %s, pod UID: %s, pod: %s, container: %s); cpuset=%v",
					pod.Namespace, string(pod.UID), pod.Name, container.Name, isolcpus)
		return nil
	}

	// container belongs in the shared pool (nothing to do; use default cpuset)
	return nil
}

func (p *staticPolicy) RemoveContainer(s state.State, podUID string, containerName string) error {
	klog.Infof("[cpumanager] static policy: RemoveContainer (pod: %s, container: %s)", podUID, containerName)
	if toRelease, ok := s.GetCPUSet(podUID, containerName); ok {
		s.Delete(podUID, containerName)
		if p.excludeReserved {
			toRelease = toRelease.Difference(p.reserved)
		}
		// Mutate the shared pool, adding released cpus.
		s.SetDefaultCPUSet(s.GetDefaultCPUSet().Union(toRelease))
	}
	return nil
}

func (p *staticPolicy) allocateCPUs(s state.State, numCPUs int, numaAffinity bitmask.BitMask) (cpuset.CPUSet, error) {
	klog.Infof("[cpumanager] allocateCpus: (numCPUs: %d, socket: %v)", numCPUs, numaAffinity)

	// If there are aligned CPUs in numaAffinity, attempt to take those first.
	result := cpuset.NewCPUSet()
	if numaAffinity != nil {
		alignedCPUs := cpuset.NewCPUSet()
		for _, numaNodeID := range numaAffinity.GetBits() {
			alignedCPUs = alignedCPUs.Union(p.assignableCPUs(s).Intersection(p.topology.CPUDetails.CPUsInNUMANodes(numaNodeID)))
		}

		numAlignedToAlloc := alignedCPUs.Size()
		if numCPUs < numAlignedToAlloc {
			numAlignedToAlloc = numCPUs
		}

		alignedCPUs, err := takeByTopology(p.topology, alignedCPUs, numAlignedToAlloc)
		if err != nil {
			return cpuset.NewCPUSet(), err
		}

		result = result.Union(alignedCPUs)
	}

	// Get any remaining CPUs from what's leftover after attempting to grab aligned ones.
	remainingCPUs, err := takeByTopology(p.topology, p.assignableCPUs(s).Difference(result), numCPUs-result.Size())
	if err != nil {
		return cpuset.NewCPUSet(), err
	}
	result = result.Union(remainingCPUs)

	// Remove allocated CPUs from the shared CPUSet.
	s.SetDefaultCPUSet(s.GetDefaultCPUSet().Difference(result))

	klog.Infof("[cpumanager] allocateCPUs: returning \"%v\"", result)
	return result, nil
}

func (p *staticPolicy) guaranteedCPUs(pod *v1.Pod, container *v1.Container) int {
	if v1qos.GetPodQOS(pod) != v1.PodQOSGuaranteed {
		return 0
	}
	cpuQuantity := container.Resources.Requests[v1.ResourceCPU]
	if cpuQuantity.Value()*1000 != cpuQuantity.MilliValue() {
		return 0
	}
	// Infrastructure pods use reserved CPUs even if they're in the Guaranteed QoS class
	if isKubeInfra(pod) {
		return 0
	}
	// Safe downcast to do for all systems with < 2.1 billion CPUs.
	// Per the language spec, `int` is guaranteed to be at least 32 bits wide.
	// https://golang.org/ref/spec#Numeric_types
	return int(cpuQuantity.Value())
}

func (p *staticPolicy) GetTopologyHints(s state.State, pod *v1.Pod, container *v1.Container) map[string][]topologymanager.TopologyHint {
	// If there are no CPU resources requested for this container, we do not
	// generate any topology hints.
	if _, ok := container.Resources.Requests[v1.ResourceCPU]; !ok {
		return nil
	}

	// Get a count of how many guaranteed CPUs have been requested.
	requested := p.guaranteedCPUs(pod, container)

	// If there are no guaranteed CPUs being requested, we do not generate
	// any topology hints. This can happen, for example, because init
	// containers don't have to have guaranteed CPUs in order for the pod
	// to still be in the Guaranteed QOS tier.
	if requested == 0 {
		return nil
	}

	// Short circuit to regenerate the same hints if there are already
	// guaranteed CPUs allocated to the Container. This might happen after a
	// kubelet restart, for example.
	if allocated, exists := s.GetCPUSet(string(pod.UID), container.Name); exists {
		if allocated.Size() != requested {
			klog.Errorf("[cpumanager] CPUs already allocated to (pod %v, container %v) with different number than request: requested: %d, allocated: %d", string(pod.UID), container.Name, requested, allocated.Size())
			return map[string][]topologymanager.TopologyHint{
				string(v1.ResourceCPU): {},
			}
		}
		klog.Infof("[cpumanager] Regenerating TopologyHints for CPUs already allocated to (pod %v, container %v)", string(pod.UID), container.Name)
		return map[string][]topologymanager.TopologyHint{
			string(v1.ResourceCPU): p.generateCPUTopologyHints(allocated, requested),
		}
	}

	// Get a list of available CPUs.
	available := p.assignableCPUs(s)

	// Generate hints.
	cpuHints := p.generateCPUTopologyHints(available, requested)
	klog.Infof("[cpumanager] TopologyHints generated for pod '%v', container '%v': %v", pod.Name, container.Name, cpuHints)

	return map[string][]topologymanager.TopologyHint{
		string(v1.ResourceCPU): cpuHints,
	}
}

// generateCPUtopologyHints generates a set of TopologyHints given the set of
// available CPUs and the number of CPUs being requested.
//
// It follows the convention of marking all hints that have the same number of
// bits set as the narrowest matching NUMANodeAffinity with 'Preferred: true', and
// marking all others with 'Preferred: false'.
func (p *staticPolicy) generateCPUTopologyHints(availableCPUs cpuset.CPUSet, request int) []topologymanager.TopologyHint {
	// Initialize minAffinitySize to include all NUMA Nodes.
	minAffinitySize := p.topology.CPUDetails.NUMANodes().Size()
	// Initialize minSocketsOnMinAffinity to include all Sockets.
	minSocketsOnMinAffinity := p.topology.CPUDetails.Sockets().Size()

	// Iterate through all combinations of socket bitmask and build hints from them.
	hints := []topologymanager.TopologyHint{}
	bitmask.IterateBitMasks(p.topology.CPUDetails.NUMANodes().ToSlice(), func(mask bitmask.BitMask) {
		// First, update minAffinitySize and minSocketsOnMinAffinity for the
		// current request size.
		cpusInMask := p.topology.CPUDetails.CPUsInNUMANodes(mask.GetBits()...).Size()
		socketsInMask := p.topology.CPUDetails.SocketsInNUMANodes(mask.GetBits()...).Size()
		if cpusInMask >= request && mask.Count() < minAffinitySize {
			minAffinitySize = mask.Count()
			if socketsInMask < minSocketsOnMinAffinity {
				minSocketsOnMinAffinity = socketsInMask
			}
		}

		// Then check to see if we have enough CPUs available on the current
		// socket bitmask to satisfy the CPU request.
		numMatching := 0
		for _, c := range availableCPUs.ToSlice() {
			if mask.IsSet(p.topology.CPUDetails[c].NUMANodeID) {
				numMatching++
			}
		}

		// If we don't, then move onto the next combination.
		if numMatching < request {
			return
		}

		// Otherwise, create a new hint from the socket bitmask and add it to the
		// list of hints.  We set all hint preferences to 'false' on the first
		// pass through.
		hints = append(hints, topologymanager.TopologyHint{
			NUMANodeAffinity: mask,
			Preferred:        false,
		})
	})

	// Loop back through all hints and update the 'Preferred' field based on
	// counting the number of bits sets in the affinity mask and comparing it
	// to the minAffinitySize. Only those with an equal number of bits set (and
	// with a minimal set of sockets) will be considered preferred.
	for i := range hints {
		if hints[i].NUMANodeAffinity.Count() == minAffinitySize {
			nodes := hints[i].NUMANodeAffinity.GetBits()
			numSockets := p.topology.CPUDetails.SocketsInNUMANodes(nodes...).Size()
			if numSockets == minSocketsOnMinAffinity {
				hints[i].Preferred = true
			}
		}
	}

	return hints
}

// check if a given pod is in a platform infrastructure namespace
func isKubeInfra(pod *v1.Pod) bool {
	for _, namespace := range infraNamespaces {
		if namespace == pod.Namespace {
			return true
		}
	}
	return false
}

// get the isolated CPUs (if any) from the devices associated with a specific container
func (p *staticPolicy) podIsolCPUs(pod *v1.Pod, container *v1.Container) cpuset.CPUSet {
	// NOTE: This is required for TestStaticPolicyAdd() since makePod() does
	// not create UID. We also need a way to properly stub devicemanager.
	if len(string(pod.UID)) == 0 {
		return cpuset.NewCPUSet()
	}
	devices := p.deviceManager.GetDevices(string(pod.UID), container.Name)
	for _, dev := range devices {
		// this resource name needs to match the isolcpus device plugin
		if dev.ResourceName == "windriver.com/isolcpus" {
			cpuStrList := dev.DeviceIds
			if len(cpuStrList) > 0 {
				cpuSet := cpuset.NewCPUSet()
				// loop over the list of strings, convert each one to int, add to cpuset
				for _, cpuStr := range cpuStrList {
					cpu, err := strconv.Atoi(cpuStr)
					if err != nil {
						panic(err)
					}
					cpuSet = cpuSet.Union(cpuset.NewCPUSet(cpu))
				}
				return cpuSet
			}
		}
	}
	return cpuset.NewCPUSet()
}
