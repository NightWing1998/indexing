// Package planner defines index placement. this file defines shard distribution aspect of the same
package planner

import (
	"fmt"
	"slices"
	"time"

	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
)

const maxDataUsageOfShard = 250 * 1024 * 1024 * 1024      // 250GB
const softLimitDataUsageOfShard = maxDataUsageOfShard / 2 // 150 GB

// ShardCategory defines the shard category for an index
// it can be Standard, Vector or Bhive
type ShardCategory uint8

const (
	// DefaultShardCategory - default shard category only to be used for empty struct initialisation
	DefaultShardCategory ShardCategory = iota // 0
	// StandardShardCategory - shards for standard plasma indexes. also any index whose type is not
	// recognised by older versions of planner will fall back to this category
	StandardShardCategory // 1
	// VectorShardCategory - shards for plasma based composite vector indexes
	VectorShardCategory // 2
	// BhiveShardCategory - shards for vector indexes with bhive storgae
	BhiveShardCategory // 3
	// InvalidShardCategory - indexes which do not require shard, for eg MOI or fdb
	InvalidShardCategory = 255 // 255
)

func (ic ShardCategory) String() string {
	switch ic {
	case DefaultShardCategory:
		return "DefaultShardCategory"
	case StandardShardCategory:
		return "StandardShardCategory"
	case VectorShardCategory:
		return "VectorShardCategory"
	case BhiveShardCategory:
		return "BHiveShardCategory"
	case InvalidShardCategory:
		return "InvalidShardCategory"
	default:
		return "InvalidShardCategory"
	}
}

func getIndexCategory(partn *IndexUsage) ShardCategory {
	if partn.IsPlasma() {
		if partn.Instance != nil && partn.Instance.Defn.IsVectorIndex {
			if partn.Instance.Defn.VectorMeta != nil && partn.Instance.Defn.VectorMeta.IsBhive {
				return BhiveShardCategory
			}
			return VectorShardCategory
		}
		return StandardShardCategory
	}
	return InvalidShardCategory
}

type pseudoShardContainer struct {
	insts              map[c.IndexInstId][]*IndexUsage // instId to multiple partitions
	totalPartitions    uint64
	memUsage           uint64 // derived from index cumulation
	diskUsage          uint64 // derived from index cumulation
	dataSize           uint64 // derived from index cumulation
	memUsageFromStats  uint64 // populated from indexer stats
	diskUsageFromStats uint64 // populated from indexer stats
	dataSizeFromStats  uint64 // populated from indexer stats
}

func newPseudoShardContainer() *pseudoShardContainer {
	return &pseudoShardContainer{
		insts:              make(map[c.IndexInstId][]*IndexUsage),
		totalPartitions:    0,
		memUsage:           0,
		diskUsage:          0,
		dataSize:           0,
		memUsageFromStats:  0,
		diskUsageFromStats: 0,
		dataSizeFromStats:  0,
	}
}

// record inst in shard container. returns true if the index is added else returns false
func (psc *pseudoShardContainer) addInstToShardContainer(index *IndexUsage) bool {
	if index == nil {
		return false
	}
	if psc.insts == nil {
		psc.insts = make(map[c.IndexInstId][]*IndexUsage)
	}

	// if _, exists := psc.insts[index.InstId]; !exists {
	// 	psc.insts[index.InstId] = make([]*IndexUsage, 0)
	// }

	for _, partn := range psc.insts[index.InstId] {
		if partn.PartnId == index.PartnId {
			return false
		}
	}

	psc.insts[index.InstId] = append(psc.insts[index.InstId], index)
	psc.totalPartitions++
	psc.memUsage += index.ActualMemUsage
	psc.diskUsage += index.ActualDiskSize
	psc.dataSize += index.ActualDataSize
	return true
}

// type aliasing to make code more easy to read
type (
	asSlotID    = c.AlternateShard_SlotId
	asReplicaID = c.AlternateShard_ReplicaId
	asGroupID   = c.AlternateShard_GroupId
)

// ShardDealer is a shard distributor on cluster level. it is a part of the solution from planner
type ShardDealer struct {
	// slots per category
	slotsPerCategory map[ShardCategory]map[asSlotID]bool
	// cluster level picture
	slotsMap map[asSlotID]map[asReplicaID]map[asGroupID]*pseudoShardContainer
	// defnId to slotId
	indexSlots map[c.IndexDefnId]asSlotID

	// per node pic of which shard pair belongs to which node
	nodeToSlotMap       map[string]map[asSlotID]asReplicaID
	nodeToShardCountMap map[string]uint64

	// config
	minShardsPerNode      uint64
	minPartitionsPerShard uint64
	shardCapacityPerNode  uint64

	alternateShardIDGenerator func() (*c.AlternateShardId, error)
}

func (sd *ShardDealer) logDealerConfig() {
	logging.Infof(
		"ShardDealer::logDealerConfig: config - minShardsPerNode %v; minPartitionsPerShard %v; shardCapacityPerNode - %v;",
		sd.minShardsPerNode,
		sd.minPartitionsPerShard,
		sd.shardCapacityPerNode,
	)
}

// NewShardDealer is a constructor for the ShardDealer
func NewShardDealer(minShardsPerNode, minPartitionsPerShard, shardCapacity uint64,
	alternateShardIDGenerater func() (*c.AlternateShardId, error)) *ShardDealer {
	return &ShardDealer{
		minShardsPerNode:      minShardsPerNode,
		minPartitionsPerShard: minPartitionsPerShard,
		shardCapacityPerNode:  shardCapacity,

		slotsPerCategory:    make(map[ShardCategory]map[asSlotID]bool),
		slotsMap:            make(map[asSlotID]map[asReplicaID]map[asGroupID]*pseudoShardContainer),
		indexSlots:          make(map[c.IndexDefnId]asSlotID),
		nodeToShardCountMap: make(map[string]uint64),

		alternateShardIDGenerator: alternateShardIDGenerater,
	}
}

// NewShardDealer2 is a constructor for the ShardDealer with default alternate shard ID generator
func NewShardDealer2(minShardsPerNode, minPartitionsPerShard, shardCapacity uint64) *ShardDealer {
	return NewShardDealer(minShardsPerNode, minPartitionsPerShard, shardCapacity, c.NewAlternateId)
}

// RecordIndexUsage takes 2 parameters - index and node so that the dealer can record an already
// created shard in its book keeping; node is required so it can track the node this index to record
// for as it can be either destination node or initial node record always happen using initialASIs
// as they are not expected to change.
// if index, or initialASI is nil, this func does not return an error but node cannot be nil
func (sd *ShardDealer) RecordIndexUsage(index *IndexUsage, node *IndexerNode, isInit bool) error {
	if index == nil {
		return nil
	}

	if isInit && len(index.InitialAlternateShardIds) == 0 {
		return nil
	} else if !isInit && len(index.AlternateShardIds) == 0 {
		return nil
	}

	if index.IsShardProxy {
		// TODO: handle adding shard proxies recursively
		// or we can also use the shard proxy in a separate call to also use the stats from shard
		// proxy (reported by shard directly)
	}

	var category = getIndexCategory(index)
	if category == InvalidShardCategory {
		return fmt.Errorf("invalid shard category for index defn %v", index.DefnId)
	}

	var inputAlternateShardIDs = index.AlternateShardIds
	if isInit {
		inputAlternateShardIDs = index.InitialAlternateShardIds
	}

	// calculate alternate shard id of main index
	var alternateShardID, err = c.ParseAlternateId(inputAlternateShardIDs[0])
	if err != nil {
		return err
	}

	// record slot id in category
	var slotID = alternateShardID.GetSlotId()
	var replicaID = alternateShardID.GetReplicaId()
	var mainstoreGroupID = alternateShardID.GetGroupId()

	if sd.slotsPerCategory == nil {
		sd.slotsPerCategory = make(map[ShardCategory]map[asSlotID]bool)
	}
	if sd.slotsPerCategory[category] == nil {
		sd.slotsPerCategory[category] = make(map[asSlotID]bool)
	}
	sd.slotsPerCategory[category][slotID] = true

	// record defnId in indexSlots map
	if sd.indexSlots == nil {
		sd.indexSlots = make(map[c.IndexDefnId]asSlotID)
	}
	sd.indexSlots[index.DefnId] = slotID

	// record index in slotsMap
	if sd.slotsMap == nil {
		sd.slotsMap = make(
			map[asSlotID]map[asReplicaID]map[asGroupID]*pseudoShardContainer,
		)
	}
	if sd.slotsMap[slotID] == nil {
		sd.slotsMap[slotID] = make(
			map[asReplicaID]map[asGroupID]*pseudoShardContainer,
		)
	}
	if sd.slotsMap[slotID][replicaID] == nil {
		sd.slotsMap[slotID][replicaID] = make(map[asGroupID]*pseudoShardContainer)
	}
	var newShardCount uint64
	if sd.slotsMap[slotID][replicaID][mainstoreGroupID] == nil {
		sd.slotsMap[slotID][replicaID][mainstoreGroupID] = newPseudoShardContainer()
		newShardCount++
	}
	var isNewPartn = sd.slotsMap[slotID][replicaID][mainstoreGroupID].addInstToShardContainer(index)

	if len(inputAlternateShardIDs) > 1 {
		backstoreShardID, _ := c.ParseAlternateId(inputAlternateShardIDs[1])
		var backstoreGroupID = backstoreShardID.GetGroupId()
		if sd.slotsMap[slotID][replicaID][backstoreGroupID] == nil {
			sd.slotsMap[slotID][replicaID][backstoreGroupID] = newPseudoShardContainer()
			newShardCount++
		}
		sd.slotsMap[slotID][replicaID][backstoreGroupID].addInstToShardContainer(index)
	}

	// record what slot ids are present on which node
	if sd.nodeToSlotMap == nil {
		sd.nodeToSlotMap = make(map[string]map[asSlotID]asReplicaID)
	}
	if sd.nodeToSlotMap[node.NodeUUID] == nil {
		sd.nodeToSlotMap[node.NodeUUID] = make(
			map[asSlotID]asReplicaID,
		)
	}
	sd.nodeToSlotMap[node.NodeUUID][slotID] = alternateShardID.GetReplicaId()

	// record new partn in a shard count only if the partn is new to the shard container
	if isNewPartn {
		if sd.nodeToShardCountMap == nil {
			sd.nodeToShardCountMap = make(map[string]uint64)
		}
		if _, exists := sd.nodeToShardCountMap[node.NodeUUID]; !exists {
			sd.nodeToShardCountMap[node.NodeUUID] = 0
		}
		sd.nodeToShardCountMap[node.NodeUUID] += newShardCount
	}

	return nil
}

// GetSlot - returns an appropriate Slot to place the indexes of the defn `defnId` into
// This could be a new slot or it could be an old slot being re-used
// GetSlot is the implementation of the 3 pass shard distribution
func (sd *ShardDealer) GetSlot(defnID c.IndexDefnId,
	replicaMap map[int]map[*IndexerNode]*IndexUsage) asSlotID {

	logging.Tracef("ShardDealer::GetSlot called for defnID %v with replica map %v",
		defnID, replicaMap)
	defer logging.Tracef("ShardDealer::GetSlot done for defnID %v", defnID)

	var mainstoreShard, backstoreShard *c.AlternateShardId

	// setStoreAnAllUsages is util func to set mainstoreShard, backstoreShard on all
	// index usages in replica map. only call once mainstore and backstore have the SlotID and
	// GroupID initialised. This func will set the ReplicaID to the shards. It does *not* update
	// internal book keeping of the Shard Dealer
	var setStoreOnAllUsages = func() {
		for replicaID, nodeMap := range replicaMap {
			for idxrNode, indexUsage := range nodeMap {
				if indexUsage == nil {
					logging.Warnf(
						"ShardDealer::GetSlot: nil index {defnID: %v, replicaID: %v, partnID: %v, nodeUUID: %v}. skipping",
						defnID,
						replicaID,
						"-",
						idxrNode.NodeUUID,
					)
					continue
				}

				var shardIDs = make([]string, 0, 2)
				mainstoreShard.SetReplicaId(asReplicaID(replicaID))
				shardIDs = append(shardIDs, mainstoreShard.String())
				if !indexUsage.IsPrimary {
					backstoreShard.SetReplicaId(asReplicaID(replicaID))
					shardIDs = append(shardIDs, backstoreShard.String())
				}

				if len(indexUsage.AlternateShardIds) != 0 {
					if indexUsage.AlternateShardIds[0] == shardIDs[0] {
						continue
					}

					// Index Usage existing shard ID does not match with ShardDealer book
					// keeping slot. force update the same
					logging.Warnf(
						"ShardDealer::GetSlot: index {defnID: %v, replicaID: %v, partnID: %v, nodeUUID: %v} curr shards %v does not match shard dealer book keeping. Forcing new Alternate Shards",
						defnID,
						replicaID,
						indexUsage.PartnId,
						idxrNode.NodeUUID,
						indexUsage.AlternateShardIds,
					)
				}

				indexUsage.AlternateShardIds = shardIDs
				logging.Infof(
					"ShardDealer::GetSlot: assiging AlternateShardIDs %v to index {defnID: %v, replicaID: %v, partnID: %v, nodeUUID: %v}",
					shardIDs,
					defnID,
					replicaID,
					indexUsage.PartnId,
					idxrNode.NodeUUID,
				)
			}
		}
	}

	// updateShardDealerRecords is a util func which updates shard dealer book keeping. if there are
	// no updates then this func is a no-op
	var updateShardDealerRecords = func() []error {
		var errSlice []error = nil
		for replicaID, nodeMap := range replicaMap {
			for idxrNode, indexUsage := range nodeMap {
				var err = sd.RecordIndexUsage(indexUsage, idxrNode, false)
				if err != nil {
					logging.Warnf(
						"ShardDealer::GetSlot failed to update book keeping with err %v for index {defnID: %v, replicaID: %v, partnID: %v, nodeUUID: %v}",
						err,
						defnID,
						replicaID,
						indexUsage.PartnId,
						idxrNode.NodeUUID,
					)

					indexUsage.AlternateShardIds = nil
					// TODO: delete book keeping updates if any

					errSlice = append(errSlice, err)
				}
			}
		}
		return errSlice
	}

	var setSlotInShards = func(slotID asSlotID) {
		mainstoreShard, backstoreShard = &c.AlternateShardId{}, &c.AlternateShardId{}

		mainstoreShard.SetSlotId(slotID)
		backstoreShard.SetSlotId(slotID)

		mainstoreShard.SetGroupId(0)
		backstoreShard.SetGroupId(1)
	}

	// Check if defnID already has a slot assigned. If that is the case, use the same slot to
	// maintain consistency
	if alternateShard, exists := sd.indexSlots[defnID]; exists {
		logging.Tracef(
			"ShardDealer::GetSlot slot %v already in-use for defn %v. setting slot on all indexes in %v",
			alternateShard,
			defnID,
			replicaMap,
		)

		// update shards
		setSlotInShards(alternateShard)

		// update index usages
		setStoreOnAllUsages()

		// update book keeping
		var errSlice = updateShardDealerRecords()
		if len(errSlice) != 0 {
			return 0
		}

		return mainstoreShard.GetSlotId()
	}

	var nodesForShard = make(map[string]bool, 0)
	for _, nodeMap := range replicaMap {
		for idxrNode := range nodeMap {
			nodesForShard[idxrNode.NodeUUID] = true
		}
	}

	// Pass 0: are all the indexer nodes under minShardsPerNode?
	var nodesUnderMinShards = make([]string, 0, len(nodesForShard))
	var nodesUnderShardCapacity = make([]string, 0, len(nodesForShard))
	for nodeUUID := range nodesForShard {
		if sd.nodeToShardCountMap[nodeUUID] < sd.minShardsPerNode {
			nodesUnderMinShards = append(nodesUnderMinShards, nodeUUID)
			nodesUnderShardCapacity = append(nodesUnderShardCapacity, nodeUUID)
		} else if sd.nodeToShardCountMap[nodeUUID] < sd.shardCapacityPerNode {
			nodesUnderShardCapacity = append(nodesUnderShardCapacity, nodeUUID)
		}
	}

	logging.Debugf(
		"ShardDealer::GetSlot nodes under minShardsPerNode - %v, all nodes - %v for defnID %v",
		nodesUnderMinShards,
		nodesForShard,
		defnID,
	)

	if len(nodesUnderMinShards) == len(nodesForShard) {
		// all nodes under min shard. create new alternate shards and return
		logging.Tracef(
			"ShardDealer::GetSlot pass-0 success. all nodes %v under min shard capacity. creating new slot for defnID %v",
			nodesUnderMinShards,
			defnID,
		)

		var newSlotID, err = sd.getNewAlternateSlotID()
		if err != nil {
			return 0
		}

		setSlotInShards(newSlotID)

		// update index usages
		setStoreOnAllUsages()

		// update book keeping
		var errSlice = updateShardDealerRecords()
		if len(errSlice) != 0 {
			return 0
		}

		return mainstoreShard.GetSlotId()
	}

	var indexShardCategory ShardCategory = InvalidShardCategory
	for _, nodes := range replicaMap {
		for _, index := range nodes {
			if index == nil {
				continue
			}
			indexShardCategory = getIndexCategory(index)
			logging.Debugf("ShardDealer::GetSlot shard category for inst (d:%v-i:%v-p:%v) is %v",
				defnID, index.InstId, index.PartnId, indexShardCategory)
			if indexShardCategory != InvalidShardCategory {
				break
			}
		}
	}
	if indexShardCategory == InvalidShardCategory {
		logging.Warnf(
			"ShardDealer::GetSlot index defn %v not of a valid shard category. skipping slot allotment",
			defnID,
		)
		return 0
	}

	// Pass 1: find nodes under soft_limit
	var nodesUnderSoftLimit = make(map[string]map[asSlotID]*pseudoShardContainer)
	for nodeUUID := range nodesForShard {
		var slotsUnderSoftLimit = sd.findShardUnderSoftLimit(nodeUUID, indexShardCategory)

		logging.Debugf("ShardDealer::GetSlot node %v shards under soft limit %v",
			nodeUUID, slotsUnderSoftLimit)

		if len(slotsUnderSoftLimit) != 0 {
			nodesUnderSoftLimit[nodeUUID] = slotsUnderSoftLimit
		}
	}

	// find a common Slot from nodesUnderSoftLimit
	var commonSlotIDs = make(map[asSlotID]*pseudoShardContainer, 0)

	for _, alternateShardIDs := range nodesUnderSoftLimit {
		if len(commonSlotIDs) == 0 {
			logging.Tracef(
				"ShardDealer::GetSlot empty common slots. initialising with %v",
				alternateShardIDs,
			)
			commonSlotIDs = alternateShardIDs
			continue
		}

		for commonSlot, maxContainer := range commonSlotIDs {
			if shardContainer, exists := alternateShardIDs[commonSlot]; exists {
				if shardContainer.dataSize > maxContainer.dataSize ||
					shardContainer.totalPartitions > maxContainer.totalPartitions {
					commonSlotIDs[commonSlot] = maxContainer
				}
			} else {
				delete(commonSlotIDs, commonSlot)
			}
		}
		logging.Tracef("ShardDealer::GetSlot: update common slots - %v", commonSlotIDs)

		if len(commonSlotIDs) == 0 {
			logging.Debugf(
				"ShardDealer::GetSlot no common slots across nodes %v for shard category %v",
				nodesUnderSoftLimit,
				indexShardCategory,
			)
			break
		}
	}

	if len(commonSlotIDs) != 0 {
		var sortedSlots = sortedSlotsByContainerUse(commonSlotIDs)
		// target slot is the min slot which is present on all nodes
		var minSlot asSlotID
		for _, slotID := range sortedSlots {
			if isSlotOnAllRequiredNodes(slotID, nodesForShard, sd.nodeToSlotMap) {
				minSlot = slotID
				break
			}
		}

		if minSlot == 0 {
			// this scenario should not be possible
			logging.Warnf("ShardDealer::GetSlot failed to get min slot from %v for defnID %v",
				commonSlotIDs, defnID)
		} else {
			logging.Debugf("ShardDealer::GetSlot pass-1 success. using common slot %v for defnID %v as it is under soft limit",
				commonSlotIDs[0], defnID)

			// set minSlot to shards
			setSlotInShards(minSlot)

			// update index usages
			setStoreOnAllUsages()

			// TODO: make sure that the slot selected ensures the index-replicaID and slot-replicaID
			// match for all nodes. if not, move index-replica around to ensure that

			// update book keeping
			var errSlice = updateShardDealerRecords()
			if len(errSlice) != 0 {
				return 0
			}

			return mainstoreShard.GetSlotId()
		}
	}

	// Pass-2 - if all nodes under shardCapacity, create new shard
	logging.Debugf("ShardDealer::GetSlot nodes under shard capacity - %v", nodesUnderShardCapacity)
	if len(nodesUnderShardCapacity) == len(nodesForShard) {
		// all nodes under min shard. create new alternate shards and return
		logging.Tracef(
			"ShardDealer::GetSlot pass-2 success. all nodes %v under shard capacity. creating new slot for defnID %v",
			nodesUnderMinShards,
			defnID,
		)

		var newSlotID, err = sd.getNewAlternateSlotID()
		if err != nil {
			return 0
		}

		setSlotInShards(newSlotID)

		// update index usages
		setStoreOnAllUsages()

		// update book keeping
		var errSlice = updateShardDealerRecords()
		if len(errSlice) != 0 {
			return 0
		}

		return mainstoreShard.GetSlotId()
	}

	// Pass-3 find common slot of category across all nodes
	commonSlotIDs = make(map[asSlotID]*pseudoShardContainer, 0)

	for nodeUUID := range nodesForShard {
		var alternateShardIDs = sd.getSlotsOfCategory(nodeUUID, indexShardCategory)
		if len(commonSlotIDs) == 0 {
			logging.Tracef(
				"ShardDealer::GetSlot empty common slots. initialising with %v",
				alternateShardIDs,
			)
			commonSlotIDs = alternateShardIDs
			continue
		}

		for commonSlot, maxContainer := range commonSlotIDs {
			if shardContainer, exists := alternateShardIDs[commonSlot]; exists {
				if shardContainer.dataSize > maxContainer.dataSize ||
					shardContainer.totalPartitions > maxContainer.totalPartitions {
					commonSlotIDs[commonSlot] = maxContainer
				}
			} else {
				delete(commonSlotIDs, commonSlot)
			}
		}
		logging.Tracef("ShardDealer::GetSlot: update common slots - %v", commonSlotIDs)

		if len(commonSlotIDs) == 0 {
			logging.Debugf(
				"ShardDealer::GetSlot no common slots across nodes %v for shard category %v",
				nodesUnderSoftLimit,
				indexShardCategory,
			)
			break
		}
	}

	if len(commonSlotIDs) != 0 {
		var sortedSlots = sortedSlotsByContainerUse(commonSlotIDs)
		// target slot is the min slot which is present on all nodes
		var minSlot asSlotID
		for _, slotID := range sortedSlots {
			if isSlotOnAllRequiredNodes(slotID, nodesForShard, sd.nodeToSlotMap) {
				minSlot = slotID
				break
			}
		}

		if minSlot == 0 {
			// this scenario should not be possible
			logging.Warnf("ShardDealer::GetSlot failed to get min slot from %v for defnID %v",
				commonSlotIDs, defnID)
		} else {
			logging.Debugf("ShardDealer::GetSlot pass-3 success. using common slot %v for defnID %v as it is under soft limit",
				commonSlotIDs[0], defnID)

			// set minSlot to all shards
			setSlotInShards(minSlot)

			// update index usages
			setStoreOnAllUsages()

			// TODO: make sure that the slot selected ensures the index-replicaID and slot-replicaID
			// match for all nodes. if not, move index-replica around to ensure that

			// update book keeping
			var errSlice = updateShardDealerRecords()
			if len(errSlice) != 0 {
				return 0
			}

			return mainstoreShard.GetSlotId()
		}
	}

	// FailSafe - no common slot found across nodes. Create a new slot
	logging.Warnf(
		"ShardDealer::GetSlot no common slot found across nodes %v for category %v. Creating new shard beyond shard capacity",
		nodesUnderShardCapacity,
		indexShardCategory,
	)

	var newSlotID, err = sd.getNewAlternateSlotID()
	if err != nil {
		return 0
	}

	setSlotInShards(newSlotID)

	setStoreOnAllUsages()

	// update book keeping
	var errSlice = updateShardDealerRecords()
	if len(errSlice) != 0 {
		return 0
	}

	return mainstoreShard.GetSlotId()
}

// findShardUnderSoftLimit returns a map of `SlotIDs` of `category` which are under
// the soft limit on node `nodeUUID`
func (sd *ShardDealer) findShardUnderSoftLimit(nodeUUID string,
	category ShardCategory) map[asSlotID]*pseudoShardContainer {

	var slotsOnNode = sd.nodeToSlotMap[nodeUUID]
	if slotsOnNode == nil {
		return nil
	}

	var slotsToContainerMap = make(map[asSlotID]*pseudoShardContainer)

	for slotID, replicaID := range slotsOnNode {
		if !sd.isSlotOfCategory(slotID, category) {
			continue
		}
		var slotGroup = sd.slotsMap[slotID][replicaID]
		var aboveCapacity = false

		// We only look at GroupID 0 aka mainstore as backstore could have different size
		// but num-indexes remain the same
		var mainstoreGroupID asGroupID = 0
		var shardContainer = slotGroup[mainstoreGroupID]
		if shardContainer != nil {
			if shardContainer.dataSize > softLimitDataUsageOfShard {
				aboveCapacity = true
			}

			if !aboveCapacity && shardContainer.totalPartitions >= sd.minPartitionsPerShard {
				aboveCapacity = true
			}

		}
		if !aboveCapacity {
			slotsToContainerMap[slotID] = shardContainer
		}
	}

	return slotsToContainerMap
}

func (sd *ShardDealer) isSlotOfCategory(slotID asSlotID,
	category ShardCategory) bool {

	if slotsOfCategory := sd.slotsPerCategory[category]; len(slotsOfCategory) != 0 {
		return slotsOfCategory[slotID]
	}

	return false
}

func (sd *ShardDealer) getSlotsOfCategory(
	nodeUUID string, category ShardCategory,
) map[asSlotID]*pseudoShardContainer {
	var slotsOnNode = sd.nodeToSlotMap[nodeUUID]
	if slotsOnNode == nil {
		return nil
	}

	var slotsToContainerMap = make(map[asSlotID]*pseudoShardContainer)

	for slotID, replicaID := range slotsOnNode {
		if !sd.isSlotOfCategory(slotID, category) {
			continue
		}
		slotsToContainerMap[slotID] = sd.slotsMap[slotID][replicaID][0]
	}

	return slotsToContainerMap
}

func minSlotsFromContainerUse(
	slotsToContainerMap map[asSlotID]*pseudoShardContainer,
) asSlotID {

	var minSlotID asSlotID
	var minContainer *pseudoShardContainer
	for slotID, container := range slotsToContainerMap {
		if minSlotID == 0 || minContainer == nil {
			minSlotID = slotID
			minContainer = container
			continue
		}
		if (container.dataSize == 0 && container.totalPartitions <
			minContainer.totalPartitions) ||
			((container.dataSize / container.totalPartitions) <
				(minContainer.dataSize / minContainer.totalPartitions)) {
			minContainer = container
			minSlotID = slotID
		}
	}
	return minSlotID
}

func sortedSlotsByContainerUse(
	slotsToContainerMap map[asSlotID]*pseudoShardContainer,
) []asSlotID {
	var slotIDs = make([]asSlotID, 0, len(slotsToContainerMap))
	var computeParams = make(map[asSlotID]float64)
	for altID, container := range slotsToContainerMap {
		slotIDs = append(slotIDs, altID)
		var computeParam float64
		if container == nil || container.totalPartitions == 0 {
			computeParam = 0
		} else {
			computeParam = float64(container.dataSize)/float64(container.totalPartitions) +
				float64(container.totalPartitions)
		}
		computeParams[altID] = computeParam
	}

	slices.SortFunc(slotIDs, func(a asSlotID, b asSlotID) int {
		return int(computeParams[a] - computeParams[b])
	})

	return slotIDs
}

func isSlotOnAllRequiredNodes(
	slotID asSlotID,
	nodes map[string]bool,
	nodeToSlotMap map[string]map[asSlotID]asReplicaID,
) bool {

	for node := range nodes {
		var nodeMap = nodeToSlotMap[node]
		if len(nodeMap) == 0 {
			return false
		}
		if _, exists := nodeMap[slotID]; !exists {
			return false
		}
	}

	return len(nodes) != 0
}

func (sd *ShardDealer) getNewAlternateSlotID() (asSlotID, error) {
	var alternateShardID *c.AlternateShardId
	var err error

	err = c.NewRetryHelper(10, 1*time.Millisecond, 1,
		func(attempt int, _ error) error {
			alternateShardID, err = sd.alternateShardIDGenerator()
			if err != nil {
				logging.Warnf(
					"ShardDealer::getNewAlternateSlotID failed to generate new alternate ID with error %v",
					err,
				)
				return err
			}
			if _, exists := sd.slotsMap[alternateShardID.GetSlotId()]; exists {
				logging.Warnf(
					"ShardDealer::getNewAlternateSlotID new slot ID collided with existing slot ID - %v. retrying",
					alternateShardID.GetSlotId(),
				)
				return fmt.Errorf("duplicate slot ID %v", alternateShardID.GetSlotId())
			}
			if attempt > 0 {
				logging.Infof("ShardDealer::getNewAlternateSlotID successful")
			}
			return nil
		},
	).Run()

	if err != nil {
		return 0, err
	}
	return alternateShardID.GetSlotId(), nil
}
