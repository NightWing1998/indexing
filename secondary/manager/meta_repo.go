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
	//"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/couchbase/gometa/protocol"
	repo "github.com/couchbase/gometa/repository"
	gometa "github.com/couchbase/gometa/server"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/manager/client"
	"net/rpc"
	"strconv"
	"strings"
	"sync"
)

type MetadataRepo struct {
	repo       RepoRef
	mutex      sync.Mutex
	isClosed   bool
	defnCache  map[common.IndexDefnId]*common.IndexDefn
	topoCache  map[string]*IndexTopology
	globalTopo *GlobalTopology
}

type RepoRef interface {
	getMeta(name string) ([]byte, error)
	setMeta(name string, value []byte) error
	broadcast(name string, value []byte) error
	deleteMeta(name string) error
	newIterator() (*repo.RepoIterator, error)
	registerNotifier(notifier MetadataNotifier)
	setLocalValue(name string, value string) error
	getLocalValue(name string) (string, error)
	deleteLocalValue(name string) error
	close()
}

type RemoteRepoRef struct {
	remoteReqAddr string
	repository    *repo.Repository
	watcher       *watcher
}

type LocalRepoRef struct {
	server   *gometa.EmbeddedServer
	eventMgr *eventManager
	notifier MetadataNotifier
}

type MetaIterator struct {
	arr []*common.IndexDefn
	pos int
}

type TopologyIterator struct {
	arr []*IndexTopology
	pos int
}

type Request struct {
	OpCode string
	Key    string
	Value  []byte
}

type Reply struct {
	Result []byte
}

type MetadataKind byte

const (
	KIND_UNKNOWN MetadataKind = iota
	KIND_INDEX_DEFN
	KIND_TOPOLOGY
	KIND_GLOBAL_TOPOLOGY
	KIND_STABILITY_TIMESTAMP
)

///////////////////////////////////////////////////////
//  Public Function : MetadataRepo
///////////////////////////////////////////////////////

func NewMetadataRepo(requestAddr string,
	leaderAddr string,
	config string,
	mgr *IndexManager) (*MetadataRepo, error) {

	ref, err := newRemoteRepoRef(requestAddr, leaderAddr, config, mgr)
	if err != nil {
		return nil, err
	}
	repo := &MetadataRepo{repo: ref,
		isClosed:   false,
		defnCache:  make(map[common.IndexDefnId]*common.IndexDefn),
		topoCache:  make(map[string]*IndexTopology),
		globalTopo: nil}

	if err := repo.loadDefn(); err != nil {
		return nil, err
	}

	if err := repo.loadTopology(); err != nil {
		return nil, err
	}

	return repo, nil
}

func NewLocalMetadataRepo(msgAddr string,
	eventMgr *eventManager,
	reqHandler protocol.CustomRequestHandler,
	repoName string,
	quota uint64) (*MetadataRepo, RequestServer, error) {

	ref, err := newLocalRepoRef(msgAddr, eventMgr, reqHandler, repoName, quota)
	if err != nil {
		return nil, nil, err
	}
	repo := &MetadataRepo{repo: ref,
		isClosed:   false,
		defnCache:  make(map[common.IndexDefnId]*common.IndexDefn),
		topoCache:  make(map[string]*IndexTopology),
		globalTopo: nil}

	if err := repo.loadDefn(); err != nil {
		return nil, nil, err
	}

	if err := repo.loadTopology(); err != nil {
		return nil, nil, err
	}

	return repo, ref.server, nil
}

func (c *MetadataRepo) GetLocalIndexerId() (common.IndexerId, error) {
	val, err := c.GetLocalValue("IndexerId")
	return common.IndexerId(val), err
}

func (c *MetadataRepo) GetLocalNodeUUID() (string, error) {
	val, err := c.GetLocalValue("IndexerNodeUUID")
	return string(val), err
}

func (c *MetadataRepo) RegisterNotifier(notifier MetadataNotifier) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.repo.registerNotifier(notifier)
}

func (c *MetadataRepo) SetLocalValue(key string, value string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.repo.setLocalValue(key, value)
}

func (c *MetadataRepo) DeleteLocalValue(key string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.repo.deleteLocalValue(key)
}

func (c *MetadataRepo) GetLocalValue(key string) (string, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.repo.getLocalValue(key)
}

func (c *MetadataRepo) Close() {

	/*
		defer func() {
			if r := recover(); r != nil {
				logging.Warnf("panic in MetadataRepo.Close() : %s.  Ignored.\n", r)
				logging.Warnf("%s", debug.Stack())
			}
		}()
	*/

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if !c.isClosed {
		c.isClosed = true
		c.repo.close()
	}
}

///////////////////////////////////////////////////////
// Public Function : ID generation
///////////////////////////////////////////////////////

func (c *MetadataRepo) GetNextPartitionId() (common.PartitionId, error) {

	id, err := common.NewUUID()
	if err != nil {
		return common.PartitionId(0), err
	}

	return common.PartitionId(id.Uint64()), nil
}

func (c *MetadataRepo) GetNextIndexInstId() (common.IndexInstId, error) {

	id, err := common.NewUUID()
	if err != nil {
		return common.IndexInstId(0), err
	}

	return common.IndexInstId(id.Uint64()), nil
}

///////////////////////////////////////////////////////
//  Public Function : Index Defnition Lookup
///////////////////////////////////////////////////////

func (c *MetadataRepo) GetIndexDefnById(id common.IndexDefnId) (*common.IndexDefn, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	defn, ok := c.defnCache[id]
	if ok && defn != nil {
		return defn, nil
	}

	lookupName := indexDefnKeyById(id)
	data, err := c.getMeta(lookupName)
	if err != nil && strings.Contains(err.Error(), "FDB_RESULT_KEY_NOT_FOUND") {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	defn, err = common.UnmarshallIndexDefn(data)
	if err != nil {
		return nil, err
	}

	c.defnCache[id] = defn
	return defn, nil
}

func (c *MetadataRepo) GetIndexDefnByName(bucket string, name string) (*common.IndexDefn, error) {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	for _, defn := range c.defnCache {
		if defn.Name == name && defn.Bucket == bucket {
			return defn, nil
		}
	}

	return nil, nil
}

///////////////////////////////////////////////////////
//  Public Function : Index Topology
///////////////////////////////////////////////////////

func (c *MetadataRepo) GetTopologyByBucket(bucket string) (*IndexTopology, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	topology, ok := c.topoCache[bucket]
	if ok && topology != nil {
		return topology, nil
	}

	lookupName := indexTopologyKey(bucket)
	data, err := c.getMeta(lookupName)
	if err != nil && strings.Contains(err.Error(), "FDB_RESULT_KEY_NOT_FOUND") {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	topology, err = unmarshallIndexTopology(data)
	if err != nil {
		return nil, err
	}

	c.topoCache[bucket] = topology
	return topology, nil
}

func (c *MetadataRepo) SetTopologyByBucket(bucket string, topology *IndexTopology) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	topology.Version = topology.Version + 1

	data, err := MarshallIndexTopology(topology)
	if err != nil {
		return err
	}

	lookupName := indexTopologyKey(bucket)
	if err := c.setMeta(lookupName, data); err != nil {
		// clear the cache if there is any error
		delete(c.topoCache, bucket)
		return err
	}

	c.topoCache[bucket] = topology
	return nil
}

func (c *MetadataRepo) GetGlobalTopology() (*GlobalTopology, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.globalTopo != nil {
		return c.globalTopo, nil
	}

	lookupName := globalTopologyKey()
	data, err := c.getMeta(lookupName)
	if err != nil && strings.Contains(err.Error(), "FDB_RESULT_KEY_NOT_FOUND") {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	topo, err := unmarshallGlobalTopology(data)
	if err != nil {
		return nil, err
	}

	c.globalTopo = topo
	return topo, nil
}

func (c *MetadataRepo) SetGlobalTopology(topology *GlobalTopology) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	data, err := marshallGlobalTopology(topology)
	if err != nil {
		return err
	}

	lookupName := globalTopologyKey()
	if err := c.setMeta(lookupName, data); err != nil {
		return err
	}

	c.globalTopo = topology
	return nil
}

///////////////////////////////////////////////////////
//  Public Function : Indexer Info
///////////////////////////////////////////////////////

func (c *MetadataRepo) BroadcastServiceMap(serviceMap *client.ServiceMap) error {

	data, err := client.MarshallServiceMap(serviceMap)
	if err != nil {
		return err
	}

	lookupName := serviceMapKey()
	if err := c.broadcast(lookupName, data); err != nil {
		return err
	}

	return nil
}

func (c *MetadataRepo) BroadcastIndexStats(stats *client.IndexStats) error {

	data, err := client.MarshallIndexStats(stats)
	if err != nil {
		return err
	}

	lookupName := indexStatsKey()
	if err := c.broadcast(lookupName, data); err != nil {
		return err
	}

	return nil
}

///////////////////////////////////////////////////////
//  Public Function : Index DDL
///////////////////////////////////////////////////////

//
// TODO: This function is not transactional.
//
func (c *MetadataRepo) CreateIndex(defn *common.IndexDefn) error {

	// check if defn already exist
	exist, err := c.GetIndexDefnById(defn.DefnId)
	if exist != nil {
		// TODO: should not return error if not found (should return nil)
		return NewError(ERROR_META_IDX_DEFN_EXIST, NORMAL, METADATA_REPO, nil,
			fmt.Sprintf("Index Definition '%s' already exists", defn.Name))
	}

	defn = (*defn).Clone()

	// marshall the defn
	data, err := common.MarshallIndexDefn(defn)
	if err != nil {
		return err
	}

	// save by defn id
	lookupName := indexDefnKeyById(defn.DefnId)
	if err := c.setMeta(lookupName, data); err != nil {
		return err
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.defnCache[defn.DefnId] = defn

	return nil
}

func (c *MetadataRepo) DropIndexById(id common.IndexDefnId) error {

	// check if defn already exist
	exist, _ := c.GetIndexDefnById(id)
	if exist == nil {
		return NewError(ERROR_META_IDX_DEFN_NOT_EXIST, NORMAL, METADATA_REPO, nil,
			fmt.Sprintf("Index Definition '%s' does not exist", id))
	}

	lookupName := indexDefnKeyById(id)
	if err := c.deleteMeta(lookupName); err != nil {
		return err
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	delete(c.defnCache, id)

	return nil
}

func (c *MetadataRepo) UpdateIndex(defn *common.IndexDefn) error {

	// check if defn already exist
	exist, _ := c.GetIndexDefnById(defn.DefnId)
	if exist == nil {
		return NewError(ERROR_META_IDX_DEFN_NOT_EXIST, NORMAL, METADATA_REPO, nil,
			fmt.Sprintf("Index Definition '%s' does not exist", defn.DefnId))
	}

	defn = (*defn).Clone()

	// marshall the defn
	data, err := common.MarshallIndexDefn(defn)
	if err != nil {
		return err
	}

	// save by defn id
	lookupName := indexDefnKeyById(defn.DefnId)
	if err := c.setMeta(lookupName, data); err != nil {
		return err
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.defnCache[defn.DefnId] = defn

	return nil
}

/////////////////////////////////////////////////////////////////////////////
// Private Function : Initialization
/////////////////////////////////////////////////////////////////////////////

func (c *MetadataRepo) loadDefn() error {

	iter, err := c.repo.newIterator()
	if err != nil {
		return err
	}
	defer iter.Close()

	for {
		key, content, err := iter.Next()
		if err != nil {
			return nil
		}

		if isIndexDefnKey(key) {
			id := indexDefnIdFromKey(key)
			if id != "" {
				defn, err := common.UnmarshallIndexDefn(content)
				if err != nil {
					return err
				}

				c.defnCache[defn.DefnId] = defn
			}
		}
	}
}

func (c *MetadataRepo) loadTopology() error {

	iter, err := c.repo.newIterator()
	if err != nil {
		return err
	}
	defer iter.Close()

	for {
		key, content, err := iter.Next()
		if err != nil {
			return nil
		}

		if isIndexTopologyKey(key) {
			topology, err := unmarshallIndexTopology(content)
			if err != nil {
				return err
			}

			bucket := getBucketFromTopologyKey(key)
			c.topoCache[bucket] = topology
		}
	}
}

/////////////////////////////////////////////////////////////////////////////
// Public Function : RepoIterator
/////////////////////////////////////////////////////////////////////////////

//
// Create a new iterator
//
func (c *MetadataRepo) NewIterator() (*MetaIterator, error) {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	iter := &MetaIterator{pos: 0, arr: nil}
	for _, defn := range c.defnCache {
		iter.arr = append(iter.arr, defn)
	}
	return iter, nil
}

// Get value from iterator
func (i *MetaIterator) Next() (string, *common.IndexDefn, error) {

	if i.pos >= len(i.arr) {
		return "", nil, errors.New("No data")
	}
	defn := i.arr[i.pos]
	i.pos++
	return fmt.Sprintf("%v", defn.DefnId), defn, nil
}

// close iterator
func (i *MetaIterator) Close() {
	// no op
}

//
// Create a new topology iterator
//
func (c *MetadataRepo) NewTopologyIterator() (*TopologyIterator, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	iter := &TopologyIterator{pos: 0, arr: nil}
	for _, topo := range c.topoCache {
		iter.arr = append(iter.arr, topo)
	}
	return iter, nil
}

// Get value from iterator
func (i *TopologyIterator) Next() (*IndexTopology, error) {

	if i.pos >= len(i.arr) {
		return nil, errors.New("No data")
	}
	topo := i.arr[i.pos]
	i.pos++
	return topo, nil
}

// close iterator
func (i *TopologyIterator) Close() {
	// no op
}

///////////////////////////////////////////////////////
// private function : LocalRepoRef
///////////////////////////////////////////////////////

func newLocalRepoRef(msgAddr string,
	eventMgr *eventManager,
	reqHandler protocol.CustomRequestHandler,
	repoName string,
	quota uint64) (*LocalRepoRef, error) {

	repoRef := &LocalRepoRef{eventMgr: eventMgr, notifier: nil}
	server, err := gometa.RunEmbeddedServerWithCustomHandler(msgAddr, nil, reqHandler, repoName, quota)
	if err != nil {
		return nil, err
	}

	repoRef.server = server
	return repoRef, nil
}

func (c *LocalRepoRef) getMeta(name string) ([]byte, error) {
	return c.server.GetValue(name)
}

func (c *LocalRepoRef) setMeta(name string, value []byte) error {
	if err := c.server.Set(name, value); err != nil {
		return err
	}

	evtType := getEventType(name)
	if c.eventMgr != nil && evtType != EVENT_NONE {
		c.eventMgr.notify(evtType, value)
	}
	return nil
}

func (c *LocalRepoRef) broadcast(name string, value []byte) error {
	if err := c.server.Broadcast(name, value); err != nil {
		return err
	}

	evtType := getEventType(name)
	if c.eventMgr != nil && evtType != EVENT_NONE {
		c.eventMgr.notify(evtType, value)
	}
	return nil
}

func (c *LocalRepoRef) deleteMeta(name string) error {
	if err := c.server.Delete(name); err != nil {
		return err
	}

	if c.eventMgr != nil && findTypeFromKey(name) == KIND_INDEX_DEFN {
		c.eventMgr.notify(EVENT_DROP_INDEX, []byte(name))
	}
	return nil
}

func (c *LocalRepoRef) newIterator() (*repo.RepoIterator, error) {
	return c.server.GetIterator("/", "")
}

func (c *LocalRepoRef) close() {

	// c.server.Terminate() is idempotent
	c.server.Terminate()
}

func getEventType(key string) EventType {

	evtType := EVENT_NONE
	metaType := findTypeFromKey(key)
	if metaType == KIND_INDEX_DEFN {
		evtType = EVENT_CREATE_INDEX
	} else if metaType == KIND_TOPOLOGY {
		evtType = EVENT_UPDATE_TOPOLOGY
	}

	return evtType
}

func (c *LocalRepoRef) registerNotifier(notifier MetadataNotifier) {
	c.notifier = notifier
}

func (c *LocalRepoRef) setLocalValue(key string, value string) error {
	return c.server.SetConfigValue(key, value)
}

func (c *LocalRepoRef) deleteLocalValue(key string) error {
	return c.server.DeleteConfigValue(key)
}

func (c *LocalRepoRef) getLocalValue(key string) (string, error) {
	return c.server.GetConfigValue(key)
}

///////////////////////////////////////////////////////
// private function : RemoteRepoRef
///////////////////////////////////////////////////////

func newRemoteRepoRef(requestAddr string,
	leaderAddr string,
	config string,
	mgr *IndexManager) (*RemoteRepoRef, error) {

	// Initialize local repository
	repository, err := repo.OpenRepository()
	if err != nil {
		return nil, err
	}

	// This is a blocking call unit the watcher is ready.  This means
	// the watcher has succesfully synchronized with the remote metadata
	// repository.

	var watcherId string
	env, err := newEnv(config)
	if err == nil {
		watcherId = env.getHostElectionPort()
	} else {
		uuid, err := common.NewUUID()
		if err != nil {
			return nil, err
		}
		watcherId = strconv.FormatUint(uuid.Uint64(), 10)
	}

	watcher, err := startWatcher(mgr, repository, leaderAddr, watcherId)
	if err != nil {
		return nil, err
	}

	repoRef := &RemoteRepoRef{remoteReqAddr: requestAddr,
		repository: repository,
		watcher:    watcher}

	return repoRef, nil
}

func (c *RemoteRepoRef) newIterator() (*repo.RepoIterator, error) {
	return c.repository.NewIterator(repo.MAIN, "/", "")
}

func (c *RemoteRepoRef) getMetaFromWatcher(name string) ([]byte, error) {

	// Get the value from the local cache first
	value, err := c.watcher.Get(name)
	if err == nil && value != nil {
		logging.Debugf("RemoteRepoRef.getMeta(): Found metadata in local repository for key %s", name)
		return value, nil
	}

	return nil, err
}

func (c *RemoteRepoRef) getMeta(name string) ([]byte, error) {
	logging.Debugf("RemoteRepoRef.getMeta(): key=%s", name)

	// Get the metadata locally from watcher first
	value, err := c.getMetaFromWatcher(name)
	if err == nil && value != nil {
		return value, nil
	}

	// If metadata not exist, check the remote dictionary
	request := &Request{OpCode: "Get", Key: name, Value: nil}
	var reply *Reply
	if err := c.newDictionaryRequest(request, &reply); err != nil {
		return nil, err
	}

	logging.Debugf("RemoteRepoRef.getMeta(): remote metadata for key %s exist=%v", name, reply != nil && reply.Result != nil)
	if reply != nil {
		// reply.Result can be nil if metadata does not exist
		return reply.Result, nil
	}

	return nil, nil
}

func (c *RemoteRepoRef) setMeta(name string, value []byte) error {

	request := &Request{OpCode: "Set", Key: name, Value: value}
	var reply *Reply
	if err := c.newDictionaryRequest(request, &reply); err != nil {
		return err
	}

	return nil
}

func (c *RemoteRepoRef) broadcast(name string, value []byte) error {

	request := &Request{OpCode: "Broadcast", Key: name, Value: value}
	var reply *Reply
	if err := c.newDictionaryRequest(request, &reply); err != nil {
		return err
	}

	return nil
}

func (c *RemoteRepoRef) deleteMeta(name string) error {

	request := &Request{OpCode: "Delete", Key: name, Value: nil}
	var reply *Reply
	if err := c.newDictionaryRequest(request, &reply); err != nil {
		return err
	}

	return nil
}

func (c *RemoteRepoRef) newDictionaryRequest(request *Request, reply **Reply) error {

	client, err := rpc.DialHTTP("tcp", c.remoteReqAddr)
	if err != nil {
		return err
	}

	err = client.Call("RequestReceiver.NewRequest", request, reply)
	if err != nil {
		logging.Debugf("MetadataRepo.newDictionaryRequest(): Got Error = %s", err.Error())
		return err
	}

	return nil
}

func (c *RemoteRepoRef) close() {
	if c.repository != nil {
		c.repository.Close()
		c.repository = nil
	}

	if c.watcher != nil {
		c.watcher.Close()
		c.watcher = nil
	}
}

func (c *RemoteRepoRef) registerNotifier(notifier MetadataNotifier) {
	panic("Function not supported")
}

func (c *RemoteRepoRef) setLocalValue(key string, value string) error {
	panic("Function not supported")
}

func (c *RemoteRepoRef) deleteLocalValue(key string) error {
	panic("Function not supported")
}

func (c *RemoteRepoRef) getLocalValue(key string) (string, error) {
	panic("Function not supported")
}

///////////////////////////////////////////////////////
// private function
///////////////////////////////////////////////////////

func (c *MetadataRepo) getMeta(name string) ([]byte, error) {
	return c.repo.getMeta(name)
}

func (c *MetadataRepo) setMeta(name string, value []byte) error {
	return c.repo.setMeta(name, value)
}

func (c *MetadataRepo) broadcast(name string, value []byte) error {
	return c.repo.broadcast(name, value)
}

func (c *MetadataRepo) deleteMeta(name string) error {
	return c.repo.deleteMeta(name)
}

func findTypeFromKey(key string) MetadataKind {

	if isIndexDefnKey(key) {
		return KIND_INDEX_DEFN
	} else if isIndexTopologyKey(key) {
		return KIND_TOPOLOGY
	} else if isGlobalTopologyKey(key) {
		return KIND_GLOBAL_TOPOLOGY
	}
	return KIND_UNKNOWN
}

///////////////////////////////////////////////////////
// package local function : Index Definition
///////////////////////////////////////////////////////

func indexDefnIdStr(id common.IndexDefnId) string {
	return strconv.FormatUint(uint64(id), 10)
}

func indexDefnId(key string) (common.IndexDefnId, error) {
	val, err := strconv.ParseUint(key, 10, 64)
	if err != nil {
		return common.IndexDefnId(0), err
	}
	return common.IndexDefnId(val), nil
}

func indexDefnKeyById(id common.IndexDefnId) string {
	return fmt.Sprintf("IndexDefinitionId/%d", id)
}

func isIndexDefnKey(key string) bool {
	return strings.Contains(key, "IndexDefinitionId/")
}

func indexDefnIdFromKey(key string) string {

	i := strings.Index(key, "IndexDefinitionId/")
	if i != -1 {
		return key[i+len("IndexDefinitionId/"):]
	}

	return ""
}

///////////////////////////////////////////////////////
// package local function : Index Topology
///////////////////////////////////////////////////////

func indexTopologyKey(bucket string) string {
	return fmt.Sprintf("IndexTopology/%s", bucket)
}

func getBucketFromTopologyKey(key string) string {
	i := strings.Index(key, "IndexTopology/")
	if i != -1 {
		return key[i+len("IndexTopology/"):]
	}

	return ""
}

func isIndexTopologyKey(key string) bool {
	return strings.Contains(key, "IndexTopology/")
}

func MarshallIndexTopology(topology *IndexTopology) ([]byte, error) {

	buf, err := json.Marshal(&topology)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func unmarshallIndexTopology(data []byte) (*IndexTopology, error) {

	topology := new(IndexTopology)
	if err := json.Unmarshal(data, topology); err != nil {
		return nil, err
	}

	return topology, nil
}

func globalTopologyKey() string {
	return "GlobalIndexTopology"
}

func isGlobalTopologyKey(key string) bool {
	return strings.Contains(key, "GlobalIndexTopology")
}

func marshallGlobalTopology(topology *GlobalTopology) ([]byte, error) {

	buf, err := json.Marshal(&topology)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func unmarshallGlobalTopology(data []byte) (*GlobalTopology, error) {

	topology := new(GlobalTopology)
	if err := json.Unmarshal(data, topology); err != nil {
		return nil, err
	}

	return topology, nil
}

///////////////////////////////////////////////////////
// package local function : IndexerInfo
///////////////////////////////////////////////////////

func serviceMapKey() string {
	return "ServiceMap"
}

func isServiceMap(key string) bool {
	return strings.Contains(key, "ServiceMap")
}

func indexStatsKey() string {
	return "IndexStats"
}

func isIndexStats(key string) bool {
	return strings.Contains(key, "IndexStats")
}

///////////////////////////////////////////////////////////
// package local function : Index Definition and Topology
///////////////////////////////////////////////////////////

//
// Add Index to Topology
//
func (m *MetadataRepo) addIndexToTopology(defn *common.IndexDefn, instId common.IndexInstId, replicaId int, scheduled bool) error {

	// get existing topology
	topology, err := m.GetTopologyByBucket(defn.Bucket)
	if err != nil {
		return err
	}
	if topology == nil {
		topology = new(IndexTopology)
		topology.Bucket = defn.Bucket
		topology.Version = 0
	}

	indexerId, err := m.GetLocalIndexerId()
	if err != nil {
		return err
	}

	rState := uint32(common.REBAL_ACTIVE)
	if defn.InstVersion != 0 {
		rState = uint32(common.REBAL_PENDING)
	}

	topology.AddIndexDefinition(defn.Bucket, defn.Name, uint64(defn.DefnId),
		uint64(instId), uint32(common.INDEX_STATE_CREATED), string(indexerId),
		uint64(defn.InstVersion), rState, uint64(replicaId), scheduled,
		string(defn.Using))

	// Add a reference of the bucket-level topology to the global topology.
	// If it fails later to create bucket-level topology, it will have
	// a dangling reference, but it is easier to discover this issue.  Otherwise,
	// we can end up having a bucket-level topology without being referenced.
	if err = m.addToGlobalTopologyIfNecessary(topology.Bucket); err != nil {
		return err
	}

	if err = m.SetTopologyByBucket(topology.Bucket, topology); err != nil {
		return err
	}

	return nil
}

//
// Delete Index from Topology
//
func (m *MetadataRepo) deleteIndexFromTopology(bucket string, id common.IndexDefnId) error {

	// get existing topology
	topology, err := m.GetTopologyByBucket(bucket)
	if err != nil {
		return err
	}
	if topology == nil {
		return nil
	}

	topology.RemoveIndexDefinitionById(id)

	if err = m.SetTopologyByBucket(topology.Bucket, topology); err != nil {
		return err
	}

	return nil
}

//
// Add a reference of the bucket-level index topology to global topology.
// If not exist, create a new one.
//
func (m *MetadataRepo) addToGlobalTopologyIfNecessary(bucket string) error {

	globalTop, err := m.GetGlobalTopology()
	if err != nil {
		return err
	}
	if globalTop == nil {
		globalTop = new(GlobalTopology)
	}

	if globalTop.AddTopologyKeyIfNecessary(indexTopologyKey(bucket)) {
		return m.SetGlobalTopology(globalTop)
	}

	return nil
}

/*
///////////////////////////////////////////////////////
//  Interface : EventNotifier
///////////////////////////////////////////////////////

func (m *LocalRepoRef) OnNewProposal(txnid c.Txnid, op c.OpCode, key string, content []byte) error {

	if m.notifier == nil {
		return nil
	}

	logging.Debugf("LocalRepoRef.OnNewProposal(): key %s", key)

	switch op {
	case c.OPCODE_ADD:
		if isIndexDefnKey(key) {
			return m.onNewProposalForCreateIndexDefn(txnid, op, key, content)
		}

	case c.OPCODE_SET:
		if isIndexDefnKey(key) {
			return m.onNewProposalForCreateIndexDefn(txnid, op, key, content)
		}

	case c.OPCODE_DELETE:
		if isIndexDefnKey(key) {
			return m.onNewProposalForDeleteIndexDefn(txnid, op, key, content)
		}
	}

	return nil
}

func (m *LocalRepoRef) OnCommit(txnid c.Txnid, key string) {
	// nothing to do
}

func (m *LocalRepoRef) onNewProposalForCreateIndexDefn(txnid c.Txnid, op c.OpCode, key string, content []byte) error {

	logging.Debugf("LocalRepoRef.OnNewProposalForCreateIndexDefn(): key %s", key)

	indexDefn, err := common.UnmarshallIndexDefn(content)
	if err != nil {
		logging.Debugf("LocalRepoRef.OnNewProposalForCreateIndexDefn(): fail to unmarshall index defn for key %s", key)
		return &c.RecoverableError{Reason: err.Error()}
	}

	if err := m.notifier.OnIndexCreate(indexDefn); err != nil {
		return &c.RecoverableError{Reason: err.Error()}
	}

	return nil
}

func (m *LocalRepoRef) onNewProposalForDeleteIndexDefn(txnid c.Txnid, op c.OpCode, key string, content []byte) error {

	logging.Debugf("LocalRepoRef.OnNewProposalForDeleteIndexDefn(): key %s", key)

	i := strings.Index(key, "/")
	if i != -1 && i < len(key)-1 {

		id, err := strconv.ParseUint(key[i+1:], 10, 64)
		if err != nil {
			logging.Debugf("LocalRepoRef.OnNewProposalForDeleteIndexDefn(): fail to unmarshall IndexDefnId key %s", key)
			return &c.RecoverableError{Reason: err.Error()}
		}

		if err := m.notifier.OnIndexDelete(common.IndexDefnId(id)); err != nil {
			return &c.RecoverableError{Reason: err.Error()}
		}
		return nil

	} else {
		logging.Debugf("LocalRepoRef.OnNewProposalForDeleteIndexDefn(): fail to unmarshall IndexDefnId key %s", key)
		err := NewError(ERROR_META_FAIL_TO_PARSE_INT, NORMAL, METADATA_REPO, nil,
			"MetadataRepo.OnNewProposalForDeleteIndexDefn() : cannot parse index definition id")
		return &c.RecoverableError{Reason: err.Error()}
	}
}
*/
