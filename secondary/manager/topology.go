// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package manager

import (
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
)

/////////////////////////////////////////////////////////////////////////
// Type Definition
////////////////////////////////////////////////////////////////////////

type GlobalTopology struct {
	TopologyKeys []string `json:"topologyKeys,omitempty"`
}

type IndexTopology struct {
	Version     uint64                  `json:"version,omitempty"`
	Bucket      string                  `json:"bucket,omitempty"`
	Definitions []IndexDefnDistribution `json:"definitions,omitempty"`
}

type IndexDefnDistribution struct {
	Bucket    string                  `json:"bucket,omitempty"`
	Name      string                  `json:"name,omitempty"`
	DefnId    uint64                  `json:"defnId,omitempty"`
	Instances []IndexInstDistribution `json:"instances,omitempty"`
}

type IndexInstDistribution struct {
	InstId         uint64                  `json:"instId,omitempty"`
	State          uint32                  `json:"state,omitempty"`
	StreamId       uint32                  `json:"steamId,omitempty"`
	Error          string                  `json:"error,omitempty"`
	Partitions     []IndexPartDistribution `json:"partitions,omitempty"`
	NumPartitions  uint32                  `json:"numPartitions,omitempty"`
	RState         uint32                  `json:"rRtate,omitempty"`
	Version        uint64                  `json:"version,omitempty"`
	ReplicaId      uint64                  `json:"replicaId,omitempty"`
	Scheduled      bool                    `json:"scheduled,omitempty"`
	StorageMode    string                  `json:"storageMode,omitempty"`
	OldStorageMode string                  `json:"oldStorageMode,omitempty"`
	RealInstId     uint64                  `json:"realInstId,omitempty"`
}

type IndexPartDistribution struct {
	PartId          uint64                      `json:"partId,omitempty"`
	Version         uint64                      `json:"version,omitempty"`
	SinglePartition IndexSinglePartDistribution `json:"singlePartition,omitempty"`
	KeyPartition    IndexKeyPartDistribution    `json:"keyPartition,omitempty"`
}

type IndexSinglePartDistribution struct {
	Slices []IndexSliceLocator `json:"slices,omitempty"`
}

type IndexKeyPartDistribution struct {
	Keys             []string                      `json:"keys,omitempty"`
	SinglePartitions []IndexSinglePartDistribution `json:"singlePartitions,omitempty"`
}

type IndexSliceLocator struct {
	SliceId   uint64 `json:"sliceId,omitempty"`
	State     uint32 `json:"state,omitempty"`
	IndexerId string `json:"indexerId,omitempty"`
}

//
// topologyChange captures changes in a topology
//
type changeRecord struct {
	definition *IndexDefnDistribution
	instance   *IndexInstDistribution
}

/////////////////////////////////////////////////////////////////////////
// Global Topology Maintenance
////////////////////////////////////////////////////////////////////////

// Add a topology key
func (g *GlobalTopology) AddTopologyKeyIfNecessary(key string) bool {
	for _, topkey := range g.TopologyKeys {
		if topkey == key {
			return false
		}
	}

	g.TopologyKeys = append(g.TopologyKeys, key)
	return true
}

// Remove a topology key
func (g *GlobalTopology) RemoveTopologyKey(key string) {
	for i, topkey := range g.TopologyKeys {
		if topkey == key {
			if i < len(g.TopologyKeys)-1 {
				g.TopologyKeys = append(g.TopologyKeys[:i], g.TopologyKeys[i+1:]...)
			} else {
				g.TopologyKeys = g.TopologyKeys[:i]
			}
			break
		}
	}
}

/////////////////////////////////////////////////////////////////////////
// Topology Maintenance
////////////////////////////////////////////////////////////////////////

//
// Add an index definition to Topology.
//
func (t *IndexTopology) AddIndexDefinition(bucket string, name string, defnId uint64, instId uint64, state uint32, indexerId string,
	instVersion uint64, rState uint32, replicaId uint64, partitions []common.PartitionId, versions []int, numPartitions uint32,
	scheduled bool, storageMode string, realInstId uint64) {

	t.RemoveIndexDefinitionById(common.IndexDefnId(defnId))

	inst := new(IndexInstDistribution)
	inst.InstId = instId
	inst.State = state
	inst.Version = instVersion
	inst.RState = rState
	inst.ReplicaId = replicaId
	inst.Scheduled = scheduled
	inst.StorageMode = storageMode
	inst.NumPartitions = numPartitions
	inst.RealInstId = realInstId

	for i, partnId := range partitions {
		slice := new(IndexSliceLocator)
		slice.SliceId = 0
		slice.IndexerId = indexerId
		slice.State = state

		part := new(IndexPartDistribution)
		part.PartId = uint64(partnId)
		part.Version = uint64(versions[i])
		part.SinglePartition.Slices = append(part.SinglePartition.Slices, *slice)
		inst.Partitions = append(inst.Partitions, *part)
	}

	defn := new(IndexDefnDistribution)
	defn.Bucket = bucket
	defn.Name = name
	defn.DefnId = defnId
	defn.Instances = append(defn.Instances, *inst)

	t.Definitions = append(t.Definitions, *defn)
}

func (t *IndexTopology) AddIndexInstance(bucket string, name string, defnId uint64, instId uint64, state uint32, indexerId string,
	instVersion uint64, rState uint32, replicaId uint64, partitions []common.PartitionId, versions []int, numPartitions uint32,
	scheduled bool, storageMode string, realInstId uint64) {

	inst := IndexInstDistribution{}
	inst.InstId = instId
	inst.State = state
	inst.Version = instVersion
	inst.RState = rState
	inst.ReplicaId = replicaId
	inst.Scheduled = scheduled
	inst.StorageMode = storageMode
	inst.NumPartitions = numPartitions
	inst.RealInstId = realInstId

	for i, partnId := range partitions {
		slice := IndexSliceLocator{}
		slice.SliceId = 0
		slice.IndexerId = indexerId
		slice.State = state

		part := IndexPartDistribution{}
		part.PartId = uint64(partnId)
		part.Version = uint64(versions[i])
		part.SinglePartition.Slices = append(part.SinglePartition.Slices, slice)
		inst.Partitions = append(inst.Partitions, part)
	}

	for i, defnRef := range t.Definitions {
		if defnRef.DefnId == defnId {
			t.Definitions[i].Instances = append(t.Definitions[i].Instances, inst)
			break
		}
	}
}

func (t *IndexTopology) RemoveIndexDefinitionById(id common.IndexDefnId) {

	for i, defnRef := range t.Definitions {
		if common.IndexDefnId(defnRef.DefnId) == id {
			if i == len(t.Definitions)-1 {
				t.Definitions = t.Definitions[:i]
			} else {
				t.Definitions = append(t.Definitions[0:i], t.Definitions[i+1:]...)
			}
			return
		}
	}
}

//
// Get all index instance Id's for a specific defnition
//
func (t *IndexTopology) FindIndexDefinition(bucket string, name string) *IndexDefnDistribution {

	for _, defnRef := range t.Definitions {
		if defnRef.Bucket == bucket && defnRef.Name == name {
			return &defnRef
		}
	}
	return nil
}

//
// Get all index instance Id's for a specific defnition
//
func (t *IndexTopology) FindIndexDefinitionById(id common.IndexDefnId) *IndexDefnDistribution {

	for _, defnRef := range t.Definitions {
		if defnRef.DefnId == uint64(id) {
			return &defnRef
		}
	}
	return nil
}

//
// Update Index Status on instance
//
func (t *IndexTopology) GetIndexInstByDefn(defnId common.IndexDefnId, instId common.IndexInstId) *IndexInstDistribution {

	for i, _ := range t.Definitions {
		if t.Definitions[i].DefnId == uint64(defnId) {
			for j, _ := range t.Definitions[i].Instances {
				if t.Definitions[i].Instances[j].InstId == uint64(instId) {
					return &t.Definitions[i].Instances[j]
				}
			}
		}
	}

	return nil
}

//
// Update Index Status on instance
//
func (t *IndexTopology) UpdateStateForIndexInst(defnId common.IndexDefnId, instId common.IndexInstId, state common.IndexState) bool {

	for i, _ := range t.Definitions {
		if t.Definitions[i].DefnId == uint64(defnId) {
			for j, _ := range t.Definitions[i].Instances {
				if t.Definitions[i].Instances[j].InstId == uint64(instId) {
					if t.Definitions[i].Instances[j].State != uint32(state) {
						t.Definitions[i].Instances[j].State = uint32(state)
						logging.Debugf("IndexTopology.UpdateStateForIndexInst(): Update index '%v' inst '%v' state to '%v'",
							defnId, t.Definitions[i].Instances[j].InstId, t.Definitions[i].Instances[j].State)
						return true
					}
				}
			}
		}
	}
	return false
}

//
// Set scheduled flag
//
func (t *IndexTopology) UpdateScheduledFlagForIndexInst(defnId common.IndexDefnId, instId common.IndexInstId, scheduled bool) bool {

	for i, _ := range t.Definitions {
		if t.Definitions[i].DefnId == uint64(defnId) {
			for j, _ := range t.Definitions[i].Instances {
				if t.Definitions[i].Instances[j].InstId == uint64(instId) {
					if t.Definitions[i].Instances[j].Scheduled != scheduled {
						t.Definitions[i].Instances[j].Scheduled = scheduled
						logging.Debugf("IndexTopology.UnsetScheduledFlagForIndexInst(): Unset scheduled flag for index '%v' inst '%v'",
							defnId, t.Definitions[i].Instances[j].InstId)
						return true
					}
				}
			}
		}
	}
	return false
}

//
// Update Index Rebalance Status on instance
//
func (t *IndexTopology) UpdateRebalanceStateForIndexInst(defnId common.IndexDefnId, instId common.IndexInstId, state common.RebalanceState) bool {

	for i, _ := range t.Definitions {
		if t.Definitions[i].DefnId == uint64(defnId) {
			for j, _ := range t.Definitions[i].Instances {
				if t.Definitions[i].Instances[j].InstId == uint64(instId) {
					if t.Definitions[i].Instances[j].RState != uint32(state) {
						t.Definitions[i].Instances[j].RState = uint32(state)
						logging.Debugf("IndexTopology.UpdateRebalanceStateForIndexInst(): Update index '%v' inst '%v' rebalance state to '%v'",
							defnId, t.Definitions[i].Instances[j].InstId, t.Definitions[i].Instances[j].RState)
						return true
					}
				}
			}
		}
	}
	return false
}

//
// Update Storage Mode on instance
//
func (t *IndexTopology) UpdateStorageModeForIndexInst(defnId common.IndexDefnId, instId common.IndexInstId, storageMode string) bool {

	for i, _ := range t.Definitions {
		if t.Definitions[i].DefnId == uint64(defnId) {
			for j, _ := range t.Definitions[i].Instances {
				if t.Definitions[i].Instances[j].InstId == uint64(instId) {
					if t.Definitions[i].Instances[j].StorageMode != storageMode {
						t.Definitions[i].Instances[j].StorageMode = storageMode
						logging.Debugf("IndexTopology.UpdateStorageModeForIndexInst(): Update index '%v' inst '%v' storage mode to '%v'",
							defnId, t.Definitions[i].Instances[j].InstId, t.Definitions[i].Instances[j].StorageMode)
						return true
					}
				}
			}
		}
	}
	return false
}

//
// Update Old Storage Mode on instance
//
func (t *IndexTopology) UpdateOldStorageModeForIndexInst(defnId common.IndexDefnId, instId common.IndexInstId, storageMode string) bool {

	for i, _ := range t.Definitions {
		if t.Definitions[i].DefnId == uint64(defnId) {
			for j, _ := range t.Definitions[i].Instances {
				if t.Definitions[i].Instances[j].InstId == uint64(instId) {
					if t.Definitions[i].Instances[j].OldStorageMode != storageMode {
						t.Definitions[i].Instances[j].OldStorageMode = storageMode
						logging.Debugf("IndexTopology.UpdateOldStorageModeForIndexInst(): Update index '%v' inst '%v' old storage mode to '%v'",
							defnId, t.Definitions[i].Instances[j].InstId, t.Definitions[i].Instances[j].OldStorageMode)
						return true
					}
				}
			}
		}
	}
	return false
}

//
// Update StreamId on instance
//
func (t *IndexTopology) UpdateStreamForIndexInst(defnId common.IndexDefnId, instId common.IndexInstId, stream common.StreamId) bool {

	for i, _ := range t.Definitions {
		if t.Definitions[i].DefnId == uint64(defnId) {
			for j, _ := range t.Definitions[i].Instances {
				if t.Definitions[i].Instances[j].InstId == uint64(instId) {
					if t.Definitions[i].Instances[j].StreamId != uint32(stream) {
						t.Definitions[i].Instances[j].StreamId = uint32(stream)
						logging.Debugf("IndexTopology.UpdateStreamForIndexInst(): Update index '%v' inst '%v stream to '%v'",
							defnId, t.Definitions[i].Instances[j].InstId, t.Definitions[i].Instances[j].StreamId)
						return true
					}
				}
			}
		}
	}
	return false
}

//
// Update Version on instance
//
func (t *IndexTopology) UpdateVersionForIndexInst(defnId common.IndexDefnId, instId common.IndexInstId, version uint64) bool {

	for i, _ := range t.Definitions {
		if t.Definitions[i].DefnId == uint64(defnId) {
			for j, _ := range t.Definitions[i].Instances {
				if t.Definitions[i].Instances[j].InstId == uint64(instId) {
					if t.Definitions[i].Instances[j].Version != version {
						t.Definitions[i].Instances[j].Version = version
						logging.Debugf("IndexTopology.UpdateVersionForIndexInst(): Update index '%v' inst '%v' version to '%v'",
							defnId, t.Definitions[i].Instances[j].InstId, t.Definitions[i].Instances[j].Version)
						return true
					}
				}
			}
		}
	}
	return false
}

func (t *IndexTopology) AddPartitionsForIndexInst(defnId common.IndexDefnId, instId common.IndexInstId, indexerId string,
	partitions []uint64, versions []int) bool {

	for i, _ := range t.Definitions {
		if t.Definitions[i].DefnId == uint64(defnId) {
			for j, _ := range t.Definitions[i].Instances {
				if t.Definitions[i].Instances[j].InstId == uint64(instId) {

					newPartitions := make([]IndexPartDistribution, 0, len(partitions))
					for k, partnId := range partitions {
						found := false
						for _, partition := range t.Definitions[i].Instances[j].Partitions {
							if partnId == partition.PartId {
								found = true
							}
						}

						if !found {

							slice := IndexSliceLocator{}
							slice.SliceId = 0
							slice.IndexerId = indexerId
							slice.State = t.Definitions[i].Instances[j].State

							part := IndexPartDistribution{}
							part.PartId = partnId
							part.Version = uint64(versions[k])
							part.SinglePartition.Slices = append(part.SinglePartition.Slices, slice)

							newPartitions = append(newPartitions, part)
						}
					}

					if len(newPartitions) != 0 {
						t.Definitions[i].Instances[j].Partitions = append(t.Definitions[i].Instances[j].Partitions, newPartitions...)
						return true
					}
				}
			}
		}
	}

	return false
}

func (t *IndexTopology) SplitPartitionsForIndexInst(defnId common.IndexDefnId, instId common.IndexInstId, tombstoneInstId common.IndexInstId,
	partitions []common.PartitionId) bool {

	for i, _ := range t.Definitions {
		if t.Definitions[i].DefnId == uint64(defnId) {
			for j, _ := range t.Definitions[i].Instances {
				if t.Definitions[i].Instances[j].InstId == uint64(instId) {

					var tombstone IndexInstDistribution
					tombstone = t.Definitions[i].Instances[j]
					tombstone.InstId = uint64(tombstoneInstId)
					tombstone.RealInstId = uint64(instId)
					tombstone.State = uint32(common.INDEX_STATE_DELETED)
					tombstone.RState = uint32(common.REBAL_PENDING_DELETE)
					tombstone.Partitions = nil

					for _, partnId := range partitions {
						for k, partition := range t.Definitions[i].Instances[j].Partitions {
							if uint64(partnId) == partition.PartId {

								// remove partition from the existing instance
								if k == len(t.Definitions[i].Instances[j].Partitions)-1 {
									t.Definitions[i].Instances[j].Partitions = t.Definitions[i].Instances[j].Partitions[:k]
								} else {
									t.Definitions[i].Instances[j].Partitions =
										append(t.Definitions[i].Instances[j].Partitions[0:k], t.Definitions[i].Instances[j].Partitions[k+1:]...)
								}

								// add partition to the tombstone
								tombstone.Partitions = append(tombstone.Partitions, partition)
							}
						}
					}

					if len(t.Definitions[i].Instances[j].Partitions) == 0 {
						t.Definitions[i].Instances[j].Partitions = nil
					}

					change := len(tombstone.Partitions) != 0
					if change {
						t.Definitions[i].Instances = append(t.Definitions[i].Instances, tombstone)
					}
					return change
				}
			}
		}
	}

	return false
}

func (t *IndexTopology) RemovePartitionsFromTombstone(defnId common.IndexDefnId, instId common.IndexInstId, partitions []uint64) bool {

	change := false
	for i, _ := range t.Definitions {
		if t.Definitions[i].DefnId == uint64(defnId) {
			for j, _ := range t.Definitions[i].Instances {

				if t.Definitions[i].Instances[j].RealInstId == uint64(instId) &&
					t.Definitions[i].Instances[j].State == uint32(common.INDEX_STATE_DELETED) &&
					t.Definitions[i].Instances[j].RState == uint32(common.REBAL_PENDING_DELETE) {

					logging.Infof("IndexTopology::RemovePartitionsFromTombstone Considering DefnId %v InstId %v Partitions %v", defnId, t.Definitions[i].Instances[j].InstId, t.Definitions[i].Instances[j].Partitions)

					for _, partnId := range partitions {
						for k, partition := range t.Definitions[i].Instances[j].Partitions {
							if partnId == partition.PartId {
								change = true

								logging.Infof("IndexTopology::RemovePartitionsFromTombstone Removing DefnId %v InstId %v Partitions %v", defnId, t.Definitions[i].Instances[j].InstId, partnId)

								// remove partition from the existing instance
								if k == len(t.Definitions[i].Instances[j].Partitions)-1 {
									t.Definitions[i].Instances[j].Partitions = t.Definitions[i].Instances[j].Partitions[:k]
								} else {
									t.Definitions[i].Instances[j].Partitions =
										append(t.Definitions[i].Instances[j].Partitions[0:k], t.Definitions[i].Instances[j].Partitions[k+1:]...)
								}
							}
						}
					}

					if len(t.Definitions[i].Instances[j].Partitions) == 0 {
						t.Definitions[i].Instances[j].Partitions = nil
					}

				}
			}
		}
	}

	return change
}

func (t *IndexTopology) DeleteAllPartitionsForIndexInst(defnId common.IndexDefnId, instId common.IndexInstId) bool {

	for i, _ := range t.Definitions {
		if t.Definitions[i].DefnId == uint64(defnId) {
			for j, _ := range t.Definitions[i].Instances {
				if t.Definitions[i].Instances[j].InstId == uint64(instId) {
					t.Definitions[i].Instances[j].Partitions = nil
				}
			}
		}
	}

	return true
}

//
// Set Error on instance
//
func (t *IndexTopology) SetErrorForIndexInst(defnId common.IndexDefnId, instId common.IndexInstId, errorStr string) bool {

	for i, _ := range t.Definitions {
		if t.Definitions[i].DefnId == uint64(defnId) {
			for j, _ := range t.Definitions[i].Instances {
				if t.Definitions[i].Instances[j].InstId == uint64(instId) {
					if t.Definitions[i].Instances[j].Error != errorStr {
						t.Definitions[i].Instances[j].Error = errorStr
						logging.Debugf("IndexTopology.SetErrorForIndexInst(): Set error for index '%v' inst '%v.  Error = '%v'",
							defnId, t.Definitions[i].Instances[j].InstId, t.Definitions[i].Instances[j].Error)
						return true
					}
				}
			}
		}
	}
	return false
}

//
// Update Index Status on instance
//
func (t *IndexTopology) ChangeStateForIndexInst(defnId common.IndexDefnId, instId common.IndexInstId, fromState, toState common.IndexState) {

	for i, _ := range t.Definitions {
		if t.Definitions[i].DefnId == uint64(defnId) {
			for j, _ := range t.Definitions[i].Instances {
				if t.Definitions[i].Instances[j].InstId == uint64(instId) {
					if t.Definitions[i].Instances[j].State == uint32(fromState) {
						t.Definitions[i].Instances[j].State = uint32(toState)
						logging.Debugf("IndexTopology.UpdateStateForIndexInst(): Update index '%v' inst '%v' state to '%v'",
							defnId, t.Definitions[i].Instances[j].InstId, t.Definitions[i].Instances[j].State)
					}
				}
			}
		}
	}
}

//
// Update Index Status on instance
//
func (t *IndexTopology) GetStatusByInst(defnId common.IndexDefnId, instId common.IndexInstId) (common.IndexState, string) {

	for i, _ := range t.Definitions {
		if t.Definitions[i].DefnId == uint64(defnId) {
			for j, _ := range t.Definitions[i].Instances {
				if t.Definitions[i].Instances[j].InstId == uint64(instId) {
					return common.IndexState(t.Definitions[i].Instances[j].State), t.Definitions[i].Instances[j].Error
				}
			}
		}
	}
	return common.INDEX_STATE_NIL, ""
}

func (t *IndexTopology) GetRStatusByInst(defnId common.IndexDefnId, instId common.IndexInstId) common.RebalanceState {

	for i, _ := range t.Definitions {
		if t.Definitions[i].DefnId == uint64(defnId) {
			for j, _ := range t.Definitions[i].Instances {
				if t.Definitions[i].Instances[j].InstId == uint64(instId) {
					return common.RebalanceState(t.Definitions[i].Instances[j].RState)
				}
			}
		}
	}
	return common.REBAL_ACTIVE
}

func (t IndexInstDistribution) IsProxy() bool {
	return t.RealInstId != 0
}

func (t *IndexTopology) IsProxyIndexInst(defnId common.IndexDefnId, instId common.IndexInstId) bool {

	for i, _ := range t.Definitions {
		if t.Definitions[i].DefnId == uint64(defnId) {
			for j, _ := range t.Definitions[i].Instances {
				if t.Definitions[i].Instances[j].InstId == uint64(instId) {
					return t.Definitions[i].Instances[j].IsProxy()
				}
			}
		}
	}
	return false
}

func (t *IndexTopology) RemoveIndexInstanceById(defnId common.IndexDefnId, instId common.IndexInstId) {

	for i, _ := range t.Definitions {
		if t.Definitions[i].DefnId == uint64(defnId) {
			for j, _ := range t.Definitions[i].Instances {
				if t.Definitions[i].Instances[j].InstId == uint64(instId) {

					if j == len(t.Definitions[i].Instances)-1 {
						t.Definitions[i].Instances = t.Definitions[i].Instances[:j]
					} else {
						t.Definitions[i].Instances = append(t.Definitions[i].Instances[0:j], t.Definitions[i].Instances[j+1:]...)
					}
					return
				}
			}
		}
	}
}

//
// Update Index Status on instance
//
func (t *IndexTopology) GetIndexInstancesByDefn(defnId common.IndexDefnId) []IndexInstDistribution {

	for i, _ := range t.Definitions {
		if t.Definitions[i].DefnId == uint64(defnId) {
			return t.Definitions[i].Instances
		}
	}
	return nil
}

//
// Get all index instance Id's for a specific defnition
//
func GetIndexInstancesIdByDefn(mgr *IndexManager, bucket string, defnId common.IndexDefnId) ([]uint64, error) {
	// Get the topology from the dictionary
	topology, err := mgr.GetTopologyByBucket(bucket)
	if err != nil || topology == nil {
		// TODO: Determine if it is a real error, or just topology does not exist in dictionary
		// If there is an error, return an empty array.  This assume that the topology does not exist.
		logging.Debugf("GetIndexInstancesByDefn(): Cannot find topology for bucket %s.  Skip.", bucket)
		return nil, nil
	}

	var result []uint64 = nil

	for _, defnRef := range topology.Definitions {
		if defnRef.DefnId == uint64(defnId) {
			for _, inst := range defnRef.Instances {
				result = append(result, inst.InstId)
			}
			break
		}
	}

	return result, nil
}

//
// Get all deleted index instance Id's
//
func GetAllDeletedIndexInstancesId(mgr *IndexManager, buckets []string) ([]uint64, error) {

	var result []uint64 = nil

	// Get the topology from the dictionary
	for _, bucket := range buckets {
		topology, err := mgr.GetTopologyByBucket(bucket)
		if err != nil || topology == nil {
			// TODO: Determine if it is a real error, or just topology does not exist in dictionary
			// If there is an error, return an empty array.  This assume that the topology does not exist.
			logging.Debugf("GetAllDeletedIndexInstances(): Cannot find topology for bucket %s.  Skip.", bucket)
			continue
		}

		for _, defnRef := range topology.Definitions {
			for _, inst := range defnRef.Instances {
				if common.IndexState(inst.State) == common.INDEX_STATE_DELETED {
					result = append(result, inst.InstId)
				}
			}
		}
	}

	return result, nil
}
