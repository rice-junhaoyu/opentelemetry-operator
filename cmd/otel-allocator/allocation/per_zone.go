// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package allocation

import (
	"context"
	"fmt"
	"github.com/buraksezer/consistent"
	"github.com/open-telemetry/opentelemetry-operator/cmd/otel-allocator/target"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"time"
)

const perZoneStrategyName = "per-zone"
const cacheTTL = 120 * time.Minute

type ZonedNode struct {
	NodeName string
	Zone     string
}

type CollectorZonedNode struct {
	ZonedNode
	collector string
}

type TargetZonedNode struct {
	ZonedNode
	target string
}

func collectorZonedNodeKeyFunc(collectorZonedNode interface{}) (string, error) {
	return collectorZonedNode.(CollectorZonedNode).collector, nil
}

func targetZonedNodeKeyFunc(targetZonedNode interface{}) (string, error) {
	return targetZonedNode.(TargetZonedNode).target, nil
}

var _ Strategy = &perZoneStrategy{}

type perZoneStrategy struct {
	k8sClient              kubernetes.Interface
	config                 consistent.Config
	collectorToZonedNode   cache.Store
	targetToZonedNode      cache.Store
	consistentHasherByZone map[string]*consistent.Consistent
}

func newPerZoneStrategy(k8sClient kubernetes.Interface) Strategy {
	config := consistent.Config{
		PartitionCount:    1061,
		ReplicationFactor: 5,
		Load:              1.1,
		Hasher:            hasher{},
	}
	perZoneStrategy := &perZoneStrategy{
		k8sClient:              k8sClient,
		config:                 config,
		consistentHasherByZone: make(map[string]*consistent.Consistent),
		collectorToZonedNode:   cache.NewTTLStore(collectorZonedNodeKeyFunc, cacheTTL),
		targetToZonedNode:      cache.NewTTLStore(targetZonedNodeKeyFunc, cacheTTL),
	}
	return perZoneStrategy
}

func (s *perZoneStrategy) GetCollectorForTarget(collectors map[string]*Collector, item *target.Item) (*Collector, error) {
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	targetUrl := item.TargetURL
	targetNodeName := item.GetNodeName()
	targetZonedNode, exist, err := s.targetToZonedNode.GetByKey(targetUrl)
	if err != nil {
		fmt.Printf("failed to retrieve target zoned node from cache: %s", err)
	}
	if !exist || targetZonedNode.(TargetZonedNode).ZonedNode.NodeName != item.GetNodeName() {
		k8sNode, err := s.retrieveK8sNode(ctx, targetNodeName)
		if err != nil {
			fmt.Printf("err retrieving k8s node %s for target %s: %s\n", item.GetNodeName(), targetUrl, err)
		}
		targetNodeZone, exist := k8sNode.ObjectMeta.Labels[v1.LabelTopologyZone]
		if !exist {
			fmt.Printf("succeeded to find the target node %s in the cluster but it doesn't support zone awareness", targetNodeName)
		}
		targetZonedNode = TargetZonedNode{
			ZonedNode: ZonedNode{
				NodeName: targetNodeName,
				Zone:     targetNodeZone,
			},
			target: targetUrl,
		}
		err = s.targetToZonedNode.Add(targetZonedNode)
		if err != nil {
			fmt.Printf("error adding a cache for the node and zone information that relates to target %s: %s", targetUrl, err)
		}
	}

	zonedConsistentHasher, exist := s.consistentHasherByZone[targetZonedNode.(TargetZonedNode).ZonedNode.Zone]
	if !exist {
		return nil, fmt.Errorf("unknown Zone %s", targetZonedNode.(TargetZonedNode).ZonedNode.Zone)
	}
	member := zonedConsistentHasher.LocateKey([]byte(targetUrl))
	collectorName := member.String()
	collector, exist := collectors[collectorName]
	if !exist {
		return nil, fmt.Errorf("unknown collector %s", collectorName)
	}
	return collector, nil
}

func (s *perZoneStrategy) SetCollectors(collectors map[string]*Collector) {
	clear(s.consistentHasherByZone)

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	collectorsByZone := make(map[string][]string)
	defer clear(collectorsByZone)

	for _, collector := range collectors {
		collectorNodeName := collector.NodeName
		collectorName := collector.Name
		collectorZonedNode, exist, err := s.collectorToZonedNode.GetByKey(collectorName)
		if err != nil {
			fmt.Printf("failed to retrieve collector zoned node from cache: %s", err)
		}
		if !exist || collectorZonedNode.(CollectorZonedNode).ZonedNode.NodeName != collector.NodeName {
			k8sNode, err := s.retrieveK8sNode(ctx, collectorNodeName)
			if err != nil {
				fmt.Printf("error retrieving k8s node %s for collector %s: %s\n", collectorNodeName, collectorName, err)
				continue
			}
			collectorNodeZone, exist := k8sNode.ObjectMeta.Labels[v1.LabelTopologyZone]
			if !exist {
				fmt.Printf("succeeded to find the collector node %s for collector %s in the cluster but it doesn't support zone awareness\n", collectorNodeName, collectorName)
				continue
			}
			collectorZonedNode = CollectorZonedNode{
				ZonedNode: ZonedNode{
					NodeName: collectorNodeName,
					Zone:     collectorNodeZone,
				},
				collector: collectorName,
			}
			err = s.collectorToZonedNode.Add(collectorZonedNode)
			if err != nil {
				fmt.Printf("error adding a cache for the node and zone information that relates to collector %s: %s \n", collectorName, err)
			}
		}
		collectorsByZone[collectorZonedNode.(CollectorZonedNode).ZonedNode.Zone] = append(collectorsByZone[collectorZonedNode.(CollectorZonedNode).ZonedNode.Zone], collectorName)
	}

	var members []consistent.Member
	for zone, collectorNames := range collectorsByZone {
		members = make([]consistent.Member, 0, len(collectors))
		for _, collectorName := range collectorNames {
			members = append(members, collectors[collectorName])
		}
		s.consistentHasherByZone[zone] = consistent.New(members, s.config)
	}
}

func (s *perZoneStrategy) GetName() string {
	return perZoneStrategyName
}

func (s *perZoneStrategy) SetFallbackStrategy(fallbackStrategy Strategy) {}

func (s *perZoneStrategy) retrieveK8sNode(ctx context.Context, nodeName string) (*v1.Node, error) {
	var node *v1.Node
	var err error
	if node, err = s.k8sClient.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{}); err != nil {
		if errors.IsNotFound(err) {
			return nil, fmt.Errorf("could not find the node %s in the cluster\n", nodeName)
		}
		return nil, fmt.Errorf("error when finding the node %s in the cluster, see error %s\n", nodeName, err)
	}
	return node, nil
}
