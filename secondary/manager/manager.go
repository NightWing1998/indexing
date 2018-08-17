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
	//"fmt"
	"encoding/json"
	"fmt"
	gometaC "github.com/couchbase/gometa/common"
	gometaL "github.com/couchbase/gometa/log"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/manager/client"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

///////////////////////////////////////////////////////
// Type Definition
///////////////////////////////////////////////////////

var USE_MASTER_REPO = false

type IndexManager struct {
	repo          *MetadataRepo
	coordinator   *Coordinator
	eventMgr      *eventManager
	lifecycleMgr  *LifecycleMgr
	cinfoClient   *common.ClusterInfoClient
	requestServer RequestServer
	basepath      string
	quota         uint64
	clusterURL    string

	// bucket monitor
	monitorKillch chan bool

	mutex    sync.Mutex
	isClosed bool
}

//
// Index Lifecycle
// 1) Index Creation
//   A) When an index is created, the index definition is assigned to a 64 bits UUID (IndexDefnId).
//   B) IndexManager will persist the index definition.
//   C) IndexManager will persist the index instance with INDEX_STATE_CREATED status.
//      Each instance is assigned a 64 bits IndexInstId. For the first instance of an index,
//      the IndexInstId is equal to the IndexDefnId.
//   D) IndexManager will invovke MetadataNotifier.OnIndexCreate().
//   E) IndexManager will update instance to status INDEX_STATE_READY.
//   F) If there is any error in (1B) - (1E), IndexManager will cleanup by deleting index definition and index instance.
//      Since there is no atomic transaction, cleanup may not be completed, and the index will be left in an invalid state.
//      See (5) for conditions where the index is considered valid.
//   G) If there is any error in (1E), IndexManager will also invoke OnIndexDelete()
//   H) Any error from (1A) or (1F), the error will be reported back to MetadataProvider.
//
// 2) Immediate Index Build (index definition is persisted successfully and deferred build flag is false)
//   A) MetadataNotifier.OnIndexBuild() is invoked.   OnIndexBuild() is responsible for updating the state of the index
//      instance (e.g. from READY to INITIAL).
//   B) If there is an error in (2A), the error will be returned to the MetadataProvider.
//   C) No cleanup will be perfromed by IndexManager if OnIndexBuild() fails.  In other words, the index can be left in
//      INDEX_STATE_READY.   The user should be able to kick off index build again using deferred build.
//   D) OnIndexBuild() can be running on a separate go-rountine.  It can invoke UpdateIndexInstance() at any time during
//      index build.  This update will be queued serially and apply to the topology specific for that index instance (will
//      not affect any other index instance).  The new index state will be returned to the MetadataProvider asynchronously.
//
// 3) Deferred Index Build
//    A) For Deferred Index Build, it will follow step (2A) - (2D).
//
// 4) Index Deletion
//    A) When an index is deleted, IndexManager will set the index to INDEX_STATE_DELETED.
//    B) If (4A) fails, the error will be returned and the index is considered as NOT deleted.
//    C) IndexManager will then invoke MetadataNotifier.OnIndexDelete().
//    D) The IndexManager will delete the index definition first before deleting the index instance.  since there is no atomic
//       transaction, the cleanup may not be completed, and index can be in inconsistent state. See (5) for valid index state.
//    E) Any error returned from (4C) to (4D) will not be returned to the client (since these are cleanup steps)
//
// 5) Valid Index States
//    A) Both index definition and index instance exist.
//    B) Index Instance is not in INDEX_STATE_CREATE or INDEX_STATE_DELETED.
//
type MetadataNotifier interface {
	OnIndexCreate(*common.IndexDefn, common.IndexInstId, int, *common.MetadataRequestContext) error
	OnIndexDelete(common.IndexInstId, string, *common.MetadataRequestContext) error
	OnIndexBuild([]common.IndexInstId, []string, *common.MetadataRequestContext) map[common.IndexInstId]error
	OnFetchStats() error
}

type RequestServer interface {
	MakeRequest(opCode gometaC.OpCode, key string, value []byte) error
	MakeAsyncRequest(opCode gometaC.OpCode, key string, value []byte) error
}

///////////////////////////////////////////////////////
// public function
///////////////////////////////////////////////////////

//
// Create a new IndexManager
//
func NewIndexManager(config common.Config, storageMode common.StorageMode) (mgr *IndexManager, err error) {

	return NewIndexManagerInternal(config, storageMode)
}

//
// Create a new IndexManager
//
func NewIndexManagerInternal(config common.Config, storageMode common.StorageMode) (mgr *IndexManager, err error) {

	gometaL.Current = &logging.SystemLogger

	mgr = new(IndexManager)
	mgr.isClosed = false

	if storageMode == common.StorageMode(common.FORESTDB) {
		mgr.quota = mgr.calcBufCacheFromMemQuota(config)
	} else {
		mgr.quota = 1 * 1024 * 1024 //1 MB
	}

	// Initialize the event manager.  This is non-blocking.  The event manager can be
	// called indirectly by watcher/meta-repo when new metadata changes are sent
	// from gometa master to the indexer node.
	mgr.eventMgr, err = newEventManager()
	if err != nil {
		mgr.Close()
		return nil, err
	}

	// Initialize LifecycleMgr.
	mgr.clusterURL = config["clusterAddr"].String()
	lifecycleMgr, err := NewLifecycleMgr(nil, mgr.clusterURL)
	if err != nil {
		mgr.Close()
		return nil, err
	}
	mgr.lifecycleMgr = lifecycleMgr

	// Initialize MetadataRepo.  This a blocking call until the
	// the metadataRepo (including watcher) is operational (e.g.
	// finish sync with remote metadata repo master).
	//mgr.repo, err = NewMetadataRepo(requestAddr, leaderAddr, config, mgr)
	mgr.basepath = config["storage_dir"].String()
	os.Mkdir(mgr.basepath, 0755)
	repoName := filepath.Join(mgr.basepath, gometaC.REPOSITORY_NAME)

	cic, err := common.NewClusterInfoClient(mgr.clusterURL, common.DEFAULT_POOL, config)
	if err != nil {
		mgr.Close()
		return nil, err
	}
	mgr.cinfoClient = cic

	cinfo := cic.GetClusterInfoCache()
	cinfo.RLock()
	defer cinfo.RUnlock()

	adminPort, err := cinfo.GetLocalServicePort(common.INDEX_ADMIN_SERVICE)
	if err != nil {
		mgr.Close()
		return nil, err
	}

	mgr.repo, mgr.requestServer, err = NewLocalMetadataRepo(adminPort, mgr.eventMgr, mgr.lifecycleMgr, repoName, mgr.quota)
	if err != nil {
		mgr.Close()
		return nil, err
	}

	// start lifecycle manager
	mgr.lifecycleMgr.Run(mgr.repo, mgr.requestServer)

	// coordinator
	mgr.coordinator = nil

	// monitor bucket
	mgr.monitorKillch = make(chan bool)
	go mgr.monitorBucket(mgr.monitorKillch)

	return mgr, nil
}

/*
//
// Create a new IndexManager
//
func NewIndexManager(requestAddr string,
    leaderAddr string,
    config string) (mgr *IndexManager, err error) {

    return NewIndexManagerInternal(requestAddr, leaderAddr, config, nil)
}
*/

func (mgr *IndexManager) RegisterRestEndpoints(mux *http.ServeMux) {
	// register request handler
	registerRequestHandler(mgr, mgr.clusterURL, mux)
}

func (mgr *IndexManager) StartCoordinator(config string) {

	mgr.mutex.Lock()
	defer mgr.mutex.Unlock()

	// Initialize Coordinator.  This is non-blocking.  The coordinator
	// is operational only after it can syncrhonized with the majority
	// of the indexers.   Any request made to the coordinator will be
	// put in a channel for later processing (once leader election is done).
	// Once the coordinator becomes the leader, it will invoke teh stream
	// manager.
	mgr.coordinator = NewCoordinator(mgr.repo, mgr, mgr.basepath)
	go mgr.coordinator.Run(config)
}

func (m *IndexManager) IsClose() bool {

	m.mutex.Lock()
	defer m.mutex.Unlock()

	return m.isClosed
}

//
// Clean up the IndexManager
//
func (m *IndexManager) Close() {

	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.isClosed {
		return
	}

	if m.coordinator != nil {
		m.coordinator.Terminate()
	}

	if m.eventMgr != nil {
		m.eventMgr.close()
	}

	if m.lifecycleMgr != nil {
		m.lifecycleMgr.Terminate()
	}

	if m.repo != nil {
		m.repo.Close()
	}

	if m.cinfoClient != nil {
		m.cinfoClient.Close()
	}

	if m.monitorKillch != nil {
		close(m.monitorKillch)
	}

	m.isClosed = true
}

func (m *IndexManager) FetchNewClusterInfoCache() (*common.ClusterInfoCache, error) {

	return common.FetchNewClusterInfoCache(m.clusterURL, common.DEFAULT_POOL)
}

///////////////////////////////////////////////////////
// public function - Metadata Operation
///////////////////////////////////////////////////////

func (m *IndexManager) GetMemoryQuota() uint64 {
	return m.quota
}

func (m *IndexManager) RegisterNotifier(notifier MetadataNotifier) {
	m.repo.RegisterNotifier(notifier)
	m.lifecycleMgr.RegisterNotifier(notifier)
}

func (m *IndexManager) SetLocalValue(key string, value string) error {
	return m.repo.SetLocalValue(key, value)
}

func (m *IndexManager) DeleteLocalValue(key string) error {
	return m.repo.DeleteLocalValue(key)
}

func (m *IndexManager) GetLocalValue(key string) (string, error) {
	return m.repo.GetLocalValue(key)
}

//
// Get an index definiton by id
//
func (m *IndexManager) GetIndexDefnById(id common.IndexDefnId) (*common.IndexDefn, error) {
	return m.repo.GetIndexDefnById(id)
}

//
// Get Metadata Iterator for index definition
//
func (m *IndexManager) NewIndexDefnIterator() (*MetaIterator, error) {
	return m.repo.NewIterator()
}

//
// Listen to create Index Request
//
func (m *IndexManager) StartListenIndexCreate(id string) (<-chan interface{}, error) {
	return m.eventMgr.register(id, EVENT_CREATE_INDEX)
}

//
// Stop Listen to create Index Request
//
func (m *IndexManager) StopListenIndexCreate(id string) {
	m.eventMgr.unregister(id, EVENT_CREATE_INDEX)
}

//
// Listen to delete Index Request
//
func (m *IndexManager) StartListenIndexDelete(id string) (<-chan interface{}, error) {
	return m.eventMgr.register(id, EVENT_DROP_INDEX)
}

//
// Stop Listen to delete Index Request
//
func (m *IndexManager) StopListenIndexDelete(id string) {
	m.eventMgr.unregister(id, EVENT_DROP_INDEX)
}

//
// Listen to update Topology Request
//
func (m *IndexManager) StartListenTopologyUpdate(id string) (<-chan interface{}, error) {
	return m.eventMgr.register(id, EVENT_UPDATE_TOPOLOGY)
}

//
// Stop Listen to update Topology Request
//
func (m *IndexManager) StopListenTopologyUpdate(id string) {
	m.eventMgr.unregister(id, EVENT_UPDATE_TOPOLOGY)
}

//
// Handle Create Index DDL.  This function will block until
// 1) The index defn is persisted durably in the dictionary
// 2) The index defn is applied locally to each "active" indexer
//    node.  An active node is a running node that is in the same
//    network partition as the leader.   A leader is always in
//    the majority partition.
//
// This function will return an error if the outcome of the
// request is not known (e.g. the node is partitioned
// from the network).  It may still mean that the request
// is able to go through (processed by some other nodes).
//
// A Index DDL can be processed by any node. If this node is a leader,
// then the DDL request will be processed by the leader.  If it is a
// follower, it will forward the request to the leader.
//
// This function will not be processed until the index manager
// is either a leader or follower. Therefore, if (1) the node is
// in the minority partition after network partition or (2) the leader
// dies, this node will unblock any in-flight request initiated
// by this node (by returning error).  The node will run leader
// election again. Until this node has became a leader or follower,
// it will not be able to handle another request.
//
// If this node is partitioned from its leader, it can still recieve
// updates from the dictionary if this node still connects to it.
//
func (m *IndexManager) HandleCreateIndexDDL(defn *common.IndexDefn, isRebalReq bool) error {

	key := fmt.Sprintf("%d", defn.DefnId)
	content, err := common.MarshallIndexDefn(defn)
	if err != nil {
		return err
	}

	if USE_MASTER_REPO {
		if !m.coordinator.NewRequest(uint32(OPCODE_ADD_IDX_DEFN), indexDefnIdStr(defn.DefnId), content) {
			// TODO: double check if it exists in the dictionary
			return NewError(ERROR_MGR_DDL_CREATE_IDX, NORMAL, INDEX_MANAGER, nil,
				fmt.Sprintf("Fail to complete processing create index statement for index '%s'", defn.Name))
		}
	} else {
		if isRebalReq {
			return m.requestServer.MakeRequest(client.OPCODE_CREATE_INDEX_REBAL, key, content)

		} else {
			return m.requestServer.MakeRequest(client.OPCODE_CREATE_INDEX, key, content)
		}
	}

	return nil
}

func (m *IndexManager) HandleDeleteIndexDDL(defnId common.IndexDefnId) error {

	key := fmt.Sprintf("%d", defnId)

	if USE_MASTER_REPO {

		if !m.coordinator.NewRequest(uint32(OPCODE_DEL_IDX_DEFN), indexDefnIdStr(defnId), nil) {
			// TODO: double check if it exists in the dictionary
			return NewError(ERROR_MGR_DDL_DROP_IDX, NORMAL, INDEX_MANAGER, nil,
				fmt.Sprintf("Fail to complete processing delete index statement for index id = '%d'", defnId))
		}
	} else {
		return m.requestServer.MakeRequest(client.OPCODE_DROP_INDEX_REBAL, key, []byte(""))
	}

	return nil
}

func (m *IndexManager) HandleBuildIndexDDL(indexIds client.IndexIdList) error {

	key := fmt.Sprintf("%d", indexIds.DefnIds[0])
	content, _ := client.MarshallIndexIdList(&indexIds)
	//TODO handle err

	/*
		if USE_MASTER_REPO {

			if !m.coordinator.NewRequest(uint32(OPCODE_DEL_IDX_DEFN), indexDefnIdStr(defnId), nil) {
				// TODO: double check if it exists in the dictionary
				return NewError(ERROR_MGR_DDL_DROP_IDX, NORMAL, INDEX_MANAGER, nil,
					fmt.Sprintf("Fail to complete processing build index statement for index id = '%d'", defnId))
			}
		} else {
	*/
	return m.requestServer.MakeRequest(client.OPCODE_BUILD_INDEX_REBAL, key, content)

	return nil
}
func (m *IndexManager) UpdateIndexInstance(bucket string, defnId common.IndexDefnId, state common.IndexState,
	streamId common.StreamId, err string, buildTime []uint64, rState common.RebalanceState) error {

	inst := &topologyChange{
		Bucket:    bucket,
		DefnId:    uint64(defnId),
		State:     uint32(state),
		StreamId:  uint32(streamId),
		Error:     err,
		BuildTime: buildTime,
		RState:    uint32(rState)}

	buf, e := json.Marshal(&inst)
	if e != nil {
		return e
	}

	// Update index instance is an async operation.  Since indexer may update index instance during
	// callback from MetadataNotifier.  By making async, it avoids deadlock.
	logging.Debugf("IndexManager.UpdateIndexInstance(): making request for Index instance update")
	return m.requestServer.MakeAsyncRequest(client.OPCODE_UPDATE_INDEX_INST, fmt.Sprintf("%v", defnId), buf)
}

func (m *IndexManager) UpdateIndexInstanceSync(bucket string, defnId common.IndexDefnId, state common.IndexState,
	streamId common.StreamId, err string, buildTime []uint64, rState common.RebalanceState) error {

	inst := &topologyChange{
		Bucket:    bucket,
		DefnId:    uint64(defnId),
		State:     uint32(state),
		StreamId:  uint32(streamId),
		Error:     err,
		BuildTime: buildTime,
		RState:    uint32(rState)}

	buf, e := json.Marshal(&inst)
	if e != nil {
		return e
	}

	logging.Debugf("IndexManager.UpdateIndexInstanceSync(): making request for Index instance update")
	return m.requestServer.MakeRequest(client.OPCODE_UPDATE_INDEX_INST, fmt.Sprintf("%v", defnId), buf)
}

func (m *IndexManager) ResetIndex(index common.IndexDefn) error {

	content, err := common.MarshallIndexDefn(&index)
	if err != nil {
		return err
	}

	logging.Debugf("IndexManager.ResetIndex(): making request for Index reset")
	return m.requestServer.MakeRequest(client.OPCODE_RESET_INDEX, fmt.Sprintf("%v", index.DefnId), content)
}

func (m *IndexManager) DeleteIndexForBucket(bucket string, streamId common.StreamId) error {

	logging.Debugf("IndexManager.DeleteIndexForBucket(): making request for deleting index for bucket")
	return m.requestServer.MakeAsyncRequest(client.OPCODE_DELETE_BUCKET, bucket, []byte{byte(streamId)})
}

func (m *IndexManager) CleanupIndex(defnId common.IndexDefnId) error {

	logging.Debugf("IndexManager.CleanupIndex(): making request for cleaning up index")
	return m.requestServer.MakeAsyncRequest(client.OPCODE_CLEANUP_INDEX, fmt.Sprintf("%v", defnId), nil)
}

func (m *IndexManager) NotifyIndexerReady() error {

	logging.Debugf("IndexManager.NotifyIndexerReady(): making request to notify indexer is ready ")
	return m.requestServer.MakeAsyncRequest(client.OPCODE_INDEXER_READY, "", []byte{})
}

func (m *IndexManager) NotifyStats(stats common.Statistics) error {

	logging.Debugf("IndexManager.NotifyStats(): making request for new stats")

	buf, e := json.Marshal(&stats)
	if e != nil {
		return e
	}

	return m.requestServer.MakeAsyncRequest(client.OPCODE_BROADCAST_STATS, "", buf)
}

func (m *IndexManager) NotifyConfigUpdate(config common.Config) error {

	logging.Debugf("IndexManager.NotifyConfigUpdate(): making request for new config update")

	buf, e := json.Marshal(&config)
	if e != nil {
		return e
	}

	return m.requestServer.MakeAsyncRequest(client.OPCODE_CONFIG_UPDATE, "", buf)
}

//
// Get Topology from dictionary
//
func (m *IndexManager) GetTopologyByBucket(bucket string) (*IndexTopology, error) {

	return m.repo.GetTopologyByBucket(bucket)
}

//
// Set Topology to dictionary
//
func (m *IndexManager) SetTopologyByBucket(bucket string, topology *IndexTopology) error {

	return m.repo.SetTopologyByBucket(bucket, topology)
}

//
// Get the global topology
//
func (m *IndexManager) GetGlobalTopology() (*GlobalTopology, error) {

	return m.repo.GetGlobalTopology()
}

///////////////////////////////////////////////////////
// public function - Bucket Monitor
///////////////////////////////////////////////////////

func (m *IndexManager) monitorBucket(killch chan bool) {

	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			buckets, err := m.getBucketForCleanup()
			if err == nil {
				for _, bucket := range buckets {
					logging.Infof("IndexManager.MonitorBucket(): making request for deleting defer index for bucket %v", bucket)
					// Make sure it is making a synchronous request.  So if indexer main loop cannot proceed to delete the indexes
					// (e.g. indexer is slow or blocked), it won't keep generating new request.
					m.requestServer.MakeRequest(client.OPCODE_CLEANUP_DEFER_INDEX, bucket, []byte{})
				}
			}
		case <-killch:
			return
		}
	}
}

func (m *IndexManager) getBucketForCleanup() ([]string, error) {

	var result []string = nil

	// Get Global Topology
	globalTop, err := m.GetGlobalTopology()
	if err != nil {
		return nil, err
	}
	if globalTop == nil {
		return result, nil
	}

	// iterate through each bucket
	for _, key := range globalTop.TopologyKeys {

		bucket := getBucketFromTopologyKey(key)

		// Get bucket UUID.  bucket uuid could be BUCKET_UUID_NIL for non-existent bucket.
		currentUUID, err := m.lifecycleMgr.getBucketUUID(bucket)
		if err != nil {
			// If err != nil, then cannot connect to fetch bucket info.  Retry it at later time.
			return nil, err
		}

		topology, err := m.repo.GetTopologyByBucket(bucket)
		if err == nil && topology != nil {

			definitions := make([]IndexDefnDistribution, len(topology.Definitions))
			copy(definitions, topology.Definitions)

			hasValidActiveIndex := false
			hasInvalidDeferIndex := false

			for _, defnRef := range definitions {

				// Check for index with active stream.  If there is any index with active stream, all
				// index in the bucket will be deleted when the stream is closed due to bucket delete.
				if defnRef.Instances[0].State != uint32(common.INDEX_STATE_DELETED) &&
					common.StreamId(defnRef.Instances[0].StreamId) != common.NIL_STREAM {
					hasValidActiveIndex = true
					break
				}

				// Check if this is a defer index from a non-existent bucket. If so, this could be a candidate
				// for cleanup.
				if defn, err := m.repo.GetIndexDefnById(common.IndexDefnId(defnRef.DefnId)); err == nil && defn != nil {
					if defn.BucketUUID != currentUUID && defn.Deferred &&
						defnRef.Instances[0].State != uint32(common.INDEX_STATE_DELETED) &&
						common.StreamId(defnRef.Instances[0].StreamId) == common.NIL_STREAM {
						hasInvalidDeferIndex = true
					}
				}
			}

			if !hasValidActiveIndex && hasInvalidDeferIndex {
				result = append(result, bucket)
			}
		}
	}

	return result, nil
}

///////////////////////////////////////////////////////
// package local function
///////////////////////////////////////////////////////

//
// Get MetadataRepo
// Any caller uses MetadatdaRepo should only for read purpose.
// Writer operation should go through LifecycleMgr
//
func (m *IndexManager) getMetadataRepo() *MetadataRepo {
	return m.repo
}

//
// Get lifecycle manager
//
func (m *IndexManager) getLifecycleMgr() *LifecycleMgr {
	return m.lifecycleMgr
}

//
// Notify new event
//
func (m *IndexManager) notify(evtType EventType, obj interface{}) {
	m.eventMgr.notify(evtType, obj)
}

func (m *IndexManager) startMasterService() error {
	return nil
}

func (m *IndexManager) stopMasterService() {
}

//Calculate forestdb  buffer cache from memory quota
func (m *IndexManager) calcBufCacheFromMemQuota(config common.Config) uint64 {

	totalQuota := config["settings.memory_quota"].Uint64()

	//calculate queue memory
	fracQueueMem := config["mutation_manager.fdb.fracMutationQueueMem"].Float64()
	queueMem := uint64(fracQueueMem * float64(totalQuota))
	queueMaxMem := config["mutation_manager.maxQueueMem"].Uint64()
	if queueMem > queueMaxMem {
		queueMem = queueMaxMem
	}

	overhead := uint64(0.15 * float64(totalQuota))
	//max overhead 5GB
	if overhead > 5*1024*1024*1024 {
		overhead = 5 * 1024 * 1024 * 1024
	}

	bufcache := totalQuota - queueMem - overhead
	//min 256MB
	if bufcache < 256*1024*1024 {
		bufcache = 256 * 1024 * 1024
	}

	return bufcache

}
