// Copyright 2024-2025 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chqstatsprocessor

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"strings"
	"sync"
)

type ResourceEntity struct {
	Name       string
	Type       string
	Attributes map[string]string
	Edges      map[string]string // Key: (entityName:entityType), Value: relationship info
}

type ResourceEntityCache struct {
	entityMap   map[string]*ResourceEntity
	entityLocks map[string]*sync.RWMutex
	mapLock     sync.RWMutex // Global lock for entityMap
}

func NewResourceEntityCache() *ResourceEntityCache {
	return &ResourceEntityCache{
		entityMap:   make(map[string]*ResourceEntity),
		entityLocks: make(map[string]*sync.RWMutex),
	}
}

func (ec *ResourceEntityCache) getOrCreateEntityLock(name string) *sync.RWMutex {
	ec.mapLock.Lock()
	defer ec.mapLock.Unlock()

	if _, exists := ec.entityLocks[name]; !exists {
		ec.entityLocks[name] = &sync.RWMutex{}
	}
	return ec.entityLocks[name]
}

func toEntityId(name, entityType string) string {
	return name + ":" + entityType
}

func (ec *ResourceEntityCache) PutEntity(name, entityType string, attributes map[string]string) *ResourceEntity {
	entityLock := ec.getOrCreateEntityLock(name)
	entityLock.Lock()
	defer entityLock.Unlock()

	ec.mapLock.Lock()
	entity, exists := ec.entityMap[name]
	if !exists {
		entity = &ResourceEntity{
			Name:       name,
			Type:       entityType,
			Attributes: make(map[string]string),
			Edges:      make(map[string]string),
		}
		ec.entityMap[toEntityId(name, entityType)] = entity
	}
	ec.mapLock.Unlock()

	// Update attributes
	for key, value := range attributes {
		entity.Attributes[key] = value
	}
	return entity
}

func (ec *ResourceEntityCache) GetAllEntities() map[string]*ResourceEntity {
	ec.mapLock.Lock()
	defer ec.mapLock.Unlock()

	entities := ec.entityMap
	ec.entityMap = make(map[string]*ResourceEntity)
	ec.entityLocks = make(map[string]*sync.RWMutex)
	return entities
}

func (re *ResourceEntity) AddEdge(targetName, targetType, relationship string) {
	if re.Edges == nil {
		re.Edges = make(map[string]string)
	}
	re.Edges[toEntityId(targetName, targetType)] = relationship
}

type EntityInfo struct {
	Type              string
	Relationships     map[string]string
	AttributePrefixes []string
}

type RelationshipMap map[string]*EntityInfo

var KubernetesRelationships = RelationshipMap{
	// Service
	string(semconv.ServiceNameKey): {
		Type: "service",
		Relationships: map[string]string{
			string(semconv.K8SNamespaceNameKey):   "belongs to namespace",
			string(semconv.K8SClusterNameKey):     "is part of cluster",
			string(semconv.K8SPodNameKey):         "is deployed on pod",
			string(semconv.K8SNodeNameKey):        "is running on node",
			string(semconv.K8SDeploymentNameKey):  "is managed by deployment",
			string(semconv.K8SStatefulSetNameKey): "is managed by statefulset",
			string(semconv.K8SDaemonSetNameKey):   "is managed by daemonset",
			string(semconv.K8SReplicaSetNameKey):  "is managed by replicaset",
		},
		AttributePrefixes: []string{"service."},
	},

	// Cluster
	string(semconv.K8SClusterNameKey): {
		Type: "k8s.cluster",
		Relationships: map[string]string{
			string(semconv.K8SNodeNameKey):        "has a node",
			string(semconv.K8SNamespaceNameKey):   "has a namespace",
			string(semconv.K8SDeploymentNameKey):  "manages deployments",
			string(semconv.K8SDaemonSetNameKey):   "manages daemon sets",
			string(semconv.K8SStatefulSetNameKey): "manages stateful sets",
			string(semconv.K8SJobNameKey):         "manages jobs",
			string(semconv.K8SCronJobNameKey):     "manages cron jobs",
		},
		AttributePrefixes: []string{"k8s.cluster."},
	},

	// Node
	string(semconv.K8SNodeNameKey): {
		Type: "k8s.node",
		Relationships: map[string]string{
			string(semconv.K8SClusterNameKey): "belongs to cluster",
			string(semconv.K8SPodNameKey):     "schedules pod",
			string(semconv.OSNameKey):         "runs on operating system",
		},
		AttributePrefixes: []string{"k8s.node."},
	},

	// Namespace
	string(semconv.K8SNamespaceNameKey): {
		Type: "k8s.namespace",
		Relationships: map[string]string{
			string(semconv.K8SClusterNameKey):     "belongs to cluster",
			string(semconv.K8SPodNameKey):         "contains pod",
			string(semconv.K8SDeploymentNameKey):  "contains deployment",
			string(semconv.K8SStatefulSetNameKey): "contains statefulset",
			string(semconv.K8SDaemonSetNameKey):   "contains daemonset",
			string(semconv.K8SReplicaSetNameKey):  "contains replicaset",
			string(semconv.K8SJobNameKey):         "contains job",
			string(semconv.K8SCronJobNameKey):     "contains cronjob",
		},
		AttributePrefixes: []string{"k8s.namespace."},
	},

	// Pod
	string(semconv.K8SPodNameKey): {
		Type: "k8s.pod",
		Relationships: map[string]string{
			string(semconv.K8SNamespaceNameKey):   "belongs to namespace",
			string(semconv.K8SNodeNameKey):        "is scheduled on node",
			string(semconv.K8SClusterNameKey):     "is part of cluster",
			string(semconv.K8SReplicaSetNameKey):  "is managed by replicaset",
			string(semconv.K8SDeploymentNameKey):  "is part of deployment",
			string(semconv.K8SStatefulSetNameKey): "is part of statefulset",
			string(semconv.K8SDaemonSetNameKey):   "is part of daemonset",
		},
		AttributePrefixes: []string{"k8s.pod."},
	},

	// Container
	string(semconv.K8SContainerNameKey): {
		Type: "k8s.container",
		Relationships: map[string]string{
			string(semconv.K8SPodNameKey):       "runs in pod",
			string(semconv.K8SNamespaceNameKey): "is part of namespace",
			string(semconv.K8SNodeNameKey):      "is deployed on node",
		},
		AttributePrefixes: []string{"k8s.container."},
	},

	// ReplicaSet
	string(semconv.K8SReplicaSetNameKey): {
		Type: "k8s.replicaset",
		Relationships: map[string]string{
			string(semconv.K8SNamespaceNameKey):  "belongs to namespace",
			string(semconv.K8SDeploymentNameKey): "is managed by deployment",
			string(semconv.K8SClusterNameKey):    "is part of cluster",
		},
		AttributePrefixes: []string{"k8s.replicaset."},
	},

	// Deployment
	string(semconv.K8SDeploymentNameKey): {
		Type: "k8s.deployment",
		Relationships: map[string]string{
			string(semconv.K8SNamespaceNameKey):  "belongs to namespace",
			string(semconv.K8SClusterNameKey):    "is managed by cluster",
			string(semconv.K8SReplicaSetNameKey): "manages replicaset",
		},
		AttributePrefixes: []string{"k8s.deployment."},
	},

	// DaemonSet
	string(semconv.K8SDaemonSetNameKey): {
		Type: "k8s.daemonset",
		Relationships: map[string]string{
			string(semconv.K8SNamespaceNameKey): "belongs to namespace",
			string(semconv.K8SClusterNameKey):   "is managed by cluster",
		},
		AttributePrefixes: []string{"k8s.daemonset."},
	},

	// StatefulSet
	string(semconv.K8SStatefulSetNameKey): {
		Type: "k8s.statefulset",
		Relationships: map[string]string{
			string(semconv.K8SNamespaceNameKey): "belongs to namespace",
			string(semconv.K8SClusterNameKey):   "is managed by cluster",
		},
		AttributePrefixes: []string{"k8s.statefulset."},
	},

	// Job
	string(semconv.K8SJobNameKey): {
		Type: "k8s.job",
		Relationships: map[string]string{
			string(semconv.K8SNamespaceNameKey): "belongs to namespace",
			string(semconv.K8SClusterNameKey):   "is managed by cluster",
		},
		AttributePrefixes: []string{"k8s.job."},
	},

	// CronJob
	string(semconv.K8SCronJobNameKey): {
		Type: "k8s.cronjob",
		Relationships: map[string]string{
			string(semconv.K8SNamespaceNameKey): "belongs to namespace",
			string(semconv.K8SClusterNameKey):   "is managed by cluster",
		},
		AttributePrefixes: []string{"k8s.cronjob."},
	},
}

var ContainerRelationships = RelationshipMap{
	// Container Entity
	string(semconv.ContainerNameKey): {
		Type: "container",
		Relationships: map[string]string{
			string(semconv.ContainerImageNameKey): "uses image",
		},
		AttributePrefixes: []string{
			"container.command",
			"container.",
			"oci.",
		},
	},

	// Container Image Entity
	string(semconv.ContainerImageNameKey): {
		Type: "container.image",
		Relationships: map[string]string{
			string(semconv.ContainerNameKey): "is used by container",
		},
		AttributePrefixes: []string{
			"container.image",
		},
	},
}

var OSRelationships = RelationshipMap{
	string(semconv.OSNameKey): {
		Type:              "os",
		Relationships:     map[string]string{},
		AttributePrefixes: []string{"os."},
	},
}

var ProcessRelationships = RelationshipMap{
	string(semconv.ProcessCommandKey): {
		Type:              "process",
		Relationships:     map[string]string{},
		AttributePrefixes: []string{"process."},
	},
}

var AWSECSRelationships = RelationshipMap{
	// ECS Container
	string(semconv.AWSECSContainerARNKey): {
		Type: "aws.ecs.container",
		Relationships: map[string]string{
			string(semconv.AWSECSClusterARNKey): "is part of cluster",
			string(semconv.AWSECSTaskIDKey):     "is associated with task",
		},
		AttributePrefixes: []string{
			"aws.ecs.task.",
		},
	},

	// ECS Task
	string(semconv.AWSECSTaskIDKey): {
		Type: "aws.ecs.task",
		Relationships: map[string]string{
			string(semconv.AWSECSClusterARNKey): "is part of cluster",
		},
		AttributePrefixes: []string{
			"aws.ecs.task.",
		},
	},

	// ECS Cluster
	string(semconv.AWSECSClusterARNKey): {
		Type: "aws.ecs.cluster",
		Relationships: map[string]string{
			string(semconv.AWSECSTaskIDKey): "contains task",
		},
		AttributePrefixes: []string{
			"aws.ecs.launchtype.",
		},
	},
}

var AWSEKSRelationships = RelationshipMap{
	// EKS Cluster
	string(semconv.AWSEKSClusterARNKey): {
		Type: "aws.eks.cluster",
		Relationships: map[string]string{
			string(semconv.K8SClusterNameKey): "is associated with cluster",
			string(semconv.K8SNodeNameKey):    "is associated with node",
		},
		AttributePrefixes: []string{
			"aws.eks.",
		},
	},
}

var FAASRelationships = RelationshipMap{
	// FaaS Instance
	string(semconv.FaaSInstanceKey): {
		Type: "faas.instance",
		Relationships: map[string]string{
			string(semconv.FaaSNameKey): "is instance of function",
		},
		AttributePrefixes: []string{"faas.instance."},
	},

	// FaaS Function
	string(semconv.FaaSNameKey): {
		Type: "faas.function",
		Relationships: map[string]string{
			string(semconv.FaaSInstanceKey): "has instance",
		},
		AttributePrefixes: []string{"faas."},
	},
}

var CloudRelationships = RelationshipMap{
	// Cloud Provider (e.g., AWS, GCP, Azure)
	string(semconv.CloudProviderKey): {
		Type: "cloud.provider",
		Relationships: map[string]string{
			string(semconv.CloudAccountIDKey):        "manages account",
			string(semconv.CloudRegionKey):           "contains region",
			string(semconv.CloudAvailabilityZoneKey): "contains availability zone",
		},
		AttributePrefixes: []string{"cloud."},
	},

	// Cloud Account
	string(semconv.CloudAccountIDKey): {
		Type: "cloud.account",
		Relationships: map[string]string{
			string(semconv.CloudProviderKey): "belongs to provider",
			string(semconv.CloudRegionKey):   "has resources in region",
		},
		AttributePrefixes: []string{"cloud.account."},
	},

	// Cloud Region
	string(semconv.CloudRegionKey): {
		Type: "cloud.region",
		Relationships: map[string]string{
			string(semconv.CloudProviderKey):         "belongs to provider",
			string(semconv.CloudAvailabilityZoneKey): "contains availability zone",
			string(semconv.CloudAccountIDKey):        "contains resources from account",
		},
		AttributePrefixes: []string{"cloud.region."},
	},

	// Cloud Availability Zone
	string(semconv.CloudAvailabilityZoneKey): {
		Type: "cloud.availability_zone",
		Relationships: map[string]string{
			string(semconv.CloudRegionKey): "belongs to region",
		},
		AttributePrefixes: []string{"cloud.availability_zone."},
	},
}

var AllRelationships = []RelationshipMap{
	KubernetesRelationships,
	ContainerRelationships,
	OSRelationships,
	ProcessRelationships,
	AWSECSRelationships,
	AWSEKSRelationships,
	FAASRelationships,
	CloudRelationships,
}

func (ec *ResourceEntityCache) Provision(attributes pcommon.Map) {
	// Shared global entity map across all relationship maps
	globalEntityMap := make(map[string]*ResourceEntity)

	for _, relationship := range AllRelationships {
		ec.provisionEntities(attributes, relationship, globalEntityMap)
	}

	for _, relationship := range AllRelationships {
		ec.provisionRelationships(relationship, globalEntityMap)
	}
}

func (ec *ResourceEntityCache) provisionRelationships(relationships RelationshipMap, globalEntityMap map[string]*ResourceEntity) {
	for parentKey, entityInfo := range relationships {
		parentEntity, parentExists := globalEntityMap[parentKey]
		if !parentExists {
			continue
		}

		parentLock := ec.getOrCreateEntityLock(parentEntity.Name)
		parentLock.Lock()

		for childKey, relationship := range entityInfo.Relationships {
			childEntity, childExists := globalEntityMap[childKey]
			if childExists {
				parentEntity.AddEdge(childEntity.Name, childEntity.Type, relationship)
			}
		}

		parentLock.Unlock()
	}
}

func (ec *ResourceEntityCache) provisionEntities(attributes pcommon.Map, relationships RelationshipMap, entityMap map[string]*ResourceEntity) {
	attributes.Range(func(k string, v pcommon.Value) bool {
		entityValue := v.AsString()
		if entityInfo, exists := relationships[k]; exists {
			entityAttrs := make(map[string]string)

			attributes.Range(func(attrKey string, attrValue pcommon.Value) bool {
				for _, prefix := range entityInfo.AttributePrefixes {
					if strings.HasPrefix(attrKey, prefix) {
						entityAttrs[attrKey] = attrValue.AsString()
						break
					}
				}
				return true
			})

			entity := ec.PutEntity(entityValue, entityInfo.Type, entityAttrs)
			entityMap[k] = entity
		}
		return true
	})
}
