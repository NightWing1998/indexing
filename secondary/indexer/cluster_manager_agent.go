// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package indexer

import (
	"errors"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/manager"
	"net"
	"sync/atomic"
	"time"
)

//ClustMgrAgent provides the mechanism to talk to Index Coordinator
type ClustMgrAgent interface {
	// Used to register rest apis served by cluster manager.
	RegisterRestEndpoints()
}

type clustMgrAgent struct {
	supvCmdch  MsgChannel //supervisor sends commands on this channel
	supvRespch MsgChannel //channel to send any message to supervisor

	mgr    *manager.IndexManager //handle to index manager
	config common.Config

	metaNotifier *metaNotifier

	stats      IndexerStatsHolder
	statsCount uint64
}

func NewClustMgrAgent(supvCmdch MsgChannel, supvRespch MsgChannel, cfg common.Config, storageMode common.StorageMode) (
	ClustMgrAgent, Message) {

	//Init the clustMgrAgent struct
	c := &clustMgrAgent{
		supvCmdch:  supvCmdch,
		supvRespch: supvRespch,
		config:     cfg,
	}

	mgr, err := manager.NewIndexManager(cfg, storageMode)
	if err != nil {
		logging.Errorf("ClustMgrAgent::NewClustMgrAgent Error In Init %v", err)
		return nil, &MsgError{
			err: Error{code: ERROR_CLUSTER_MGR_AGENT_INIT,
				severity: FATAL,
				category: CLUSTER_MGR,
				cause:    err}}

	}

	c.mgr = mgr

	metaNotifier := NewMetaNotifier(supvRespch, cfg, c)
	if metaNotifier == nil {
		logging.Errorf("ClustMgrAgent::NewClustMgrAgent Error In Init %v", err)
		return nil, &MsgError{
			err: Error{code: ERROR_CLUSTER_MGR_AGENT_INIT,
				severity: FATAL,
				category: CLUSTER_MGR}}

	}

	mgr.RegisterNotifier(metaNotifier)

	c.metaNotifier = metaNotifier

	//start clustMgrAgent loop which listens to commands from its supervisor
	go c.run()

	//register with Index Manager for notification of metadata updates

	return c, &MsgSuccess{}

}

func (c *clustMgrAgent) RegisterRestEndpoints() {
	mux := GetHTTPMux()
	c.mgr.RegisterRestEndpoints(mux)
}

//run starts the clustmgrAgent loop which listens to messages
//from it supervisor(indexer)
func (c *clustMgrAgent) run() {

	//main ClustMgrAgent loop

	defer c.mgr.Close()

	defer c.panicHandler()

loop:
	for {
		select {

		case cmd, ok := <-c.supvCmdch:
			if ok {
				if cmd.GetMsgType() == CLUST_MGR_AGENT_SHUTDOWN {
					logging.Infof("ClusterMgrAgent: Shutting Down")
					c.supvCmdch <- &MsgSuccess{}
					break loop
				}
				c.handleSupvervisorCommands(cmd)
			} else {
				//supervisor channel closed. exit
				break loop
			}

		}
	}
}

func (c *clustMgrAgent) handleSupvervisorCommands(cmd Message) {

	switch cmd.GetMsgType() {

	case CLUST_MGR_INDEXER_READY:
		c.handleIndexerReady(cmd)

	case CLUST_MGR_REBALANCE_RUNNING:
		c.handleRebalanceRunning(cmd)

	case CLUST_MGR_UPDATE_TOPOLOGY_FOR_INDEX:
		c.handleUpdateTopologyForIndex(cmd)

	case CLUST_MGR_RESET_INDEX:
		c.handleResetIndex(cmd)

	case CLUST_MGR_GET_GLOBAL_TOPOLOGY:
		c.handleGetGlobalTopology(cmd)

	case CLUST_MGR_GET_LOCAL:
		c.handleGetLocalValue(cmd)

	case CLUST_MGR_SET_LOCAL:
		c.handleSetLocalValue(cmd)

	case CLUST_MGR_DEL_LOCAL:
		c.handleDelLocalValue(cmd)

	case CLUST_MGR_DEL_BUCKET:
		c.handleDeleteBucket(cmd)

	case CLUST_MGR_CLEANUP_INDEX:
		c.handleCleanupIndex(cmd)

	case UPDATE_INDEX_INSTANCE_MAP:
		c.handleIndexMap(cmd)

	case INDEX_STATS_DONE:
		c.handleStats(cmd)

	case CONFIG_SETTINGS_UPDATE:
		c.handleConfigUpdate(cmd)

	case CLUST_MGR_CLEANUP_PARTITION:
		c.handleCleanupPartition(cmd)

	case CLUST_MGR_MERGE_PARTITION:
		c.handleMergePartition(cmd)

	default:
		logging.Errorf("ClusterMgrAgent::handleSupvervisorCommands Unknown Message %v", cmd)
	}

}

func (c *clustMgrAgent) handleUpdateTopologyForIndex(cmd Message) {

	logging.Infof("ClustMgr:handleUpdateTopologyForIndex %v", cmd)

	indexList := cmd.(*MsgClustMgrUpdate).GetIndexList()
	updatedFields := cmd.(*MsgClustMgrUpdate).GetUpdatedFields()
	syncUpdate := cmd.(*MsgClustMgrUpdate).GetIsSyncUpdate()
	respCh := cmd.(*MsgClustMgrUpdate).GetRespCh()

	for _, index := range indexList {
		updatedState := common.INDEX_STATE_NIL
		updatedStream := common.NIL_STREAM
		updatedError := ""
		updatedRState := common.REBAL_NIL
		updatedPartitions := []uint64(nil)
		updatedVersions := []int(nil)
		updatedInstVersion := -1

		if updatedFields.state {
			updatedState = index.State
		}
		if updatedFields.stream {
			updatedStream = index.Stream
		}
		if updatedFields.err {
			updatedError = index.Error
		}
		if updatedFields.rstate {
			updatedRState = index.RState
		}
		if updatedFields.partitions {
			for _, partition := range index.Pc.GetAllPartitions() {
				updatedPartitions = append(updatedPartitions, uint64(partition.GetPartitionId()))
				updatedVersions = append(updatedVersions, int(partition.GetVersion()))
			}
		}
		if updatedFields.version {
			updatedInstVersion = index.Version
		}

		updatedBuildTs := index.BuildTs

		var err error
		if syncUpdate {
			go func() {
				err = c.mgr.UpdateIndexInstanceSync(index.Defn.Bucket, index.Defn.DefnId, index.InstId,
					updatedState, updatedStream, updatedError, updatedBuildTs, updatedRState, updatedPartitions,
					updatedVersions, updatedInstVersion)
				respCh <- err
			}()
		} else {
			err = c.mgr.UpdateIndexInstance(index.Defn.Bucket, index.Defn.DefnId, index.InstId,
				updatedState, updatedStream, updatedError, updatedBuildTs, updatedRState, updatedPartitions,
				updatedVersions, updatedInstVersion)
		}
		common.CrashOnError(err)
	}

	c.supvCmdch <- &MsgSuccess{}

}

func (c *clustMgrAgent) handleCleanupPartition(cmd Message) {

	logging.Infof("ClustMgr:handleCleanupPartition%v", cmd)

	msg := cmd.(*MsgClustMgrCleanupPartition)
	defn := msg.GetDefn()

	defn.InstId = msg.GetInstId()
	defn.ReplicaId = msg.GetReplicaId()
	defn.Partitions = append(defn.Partitions, msg.GetPartitionId())

	if err := c.mgr.CleanupPartition(defn, msg.UpdateStatusOnly()); err != nil {
		common.CrashOnError(err)
	}

	c.supvCmdch <- &MsgSuccess{}
}

func (c *clustMgrAgent) handleMergePartition(cmd Message) {

	logging.Infof("ClustMgr:handleMergePartition%v", cmd)

	defnId := cmd.(*MsgClustMgrMergePartition).GetDefnId()
	srcInstId := cmd.(*MsgClustMgrMergePartition).GetSrcInstId()
	srcRState := cmd.(*MsgClustMgrMergePartition).GetSrcRState()
	tgtInstId := cmd.(*MsgClustMgrMergePartition).GetTgtInstId()
	tgtPartitions := cmd.(*MsgClustMgrMergePartition).GetTgtPartitions()
	tgtVersions := cmd.(*MsgClustMgrMergePartition).GetTgtVersions()
	tgtInstVersion := cmd.(*MsgClustMgrMergePartition).GetTgtInstVersion()
	respch := cmd.(*MsgClustMgrMergePartition).GetRespch()

	go func() {
		respch <- c.mgr.MergePartition(defnId, srcInstId, srcRState, tgtInstId, tgtInstVersion, tgtPartitions, tgtVersions)
	}()

	c.supvCmdch <- &MsgSuccess{}
}

func (c *clustMgrAgent) handleResetIndex(cmd Message) {

	logging.Infof("ClustMgr:handleResetIndex %v", cmd)

	index := cmd.(*MsgClustMgrResetIndex).GetIndex()

	if err := c.mgr.ResetIndex(index); err != nil {
		common.CrashOnError(err)
	}

	c.supvCmdch <- &MsgSuccess{}
}

func (c *clustMgrAgent) handleIndexMap(cmd Message) {

	logging.Infof("ClustMgr:handleIndexMap %v", cmd)

	statsObj := cmd.(*MsgUpdateInstMap).GetStatsObject()
	if statsObj != nil {
		c.stats.Set(statsObj)
	}

	c.supvCmdch <- &MsgSuccess{}
}

func (c *clustMgrAgent) handleStats(cmd Message) {

	c.supvCmdch <- &MsgSuccess{}

	c.handleStatsInternal()
	atomic.AddUint64(&c.statsCount, 1)
}

func (c *clustMgrAgent) handleStatsInternal() {

	stats := c.stats.Get()
	if stats != nil {
		c.mgr.NotifyStats(stats.GetStats(false, false))
	}
}

func (c *clustMgrAgent) handleConfigUpdate(cmd Message) {

	logging.Infof("ClustMgr:handleConfigUpdate")
	c.supvCmdch <- &MsgSuccess{}

	cfgUpdate := cmd.(*MsgConfigUpdate)
	config := cfgUpdate.GetConfig()
	c.mgr.NotifyConfigUpdate(config)
}

func (c *clustMgrAgent) handleGetGlobalTopology(cmd Message) {

	logging.Infof("ClustMgr:handleGetGlobalTopology %v", cmd)

	//get the latest topology from manager
	metaIter, err := c.mgr.NewIndexDefnIterator()
	if err != nil {
		common.CrashOnError(err)
	}
	defer metaIter.Close()

	indexInstMap := make(common.IndexInstMap)

	for _, defn, err := metaIter.Next(); err == nil; _, defn, err = metaIter.Next() {

		var idxDefn common.IndexDefn
		idxDefn = *defn

		t, e := c.mgr.GetTopologyByBucket(idxDefn.Bucket)
		if e != nil {
			common.CrashOnError(e)
		}
		if t == nil {
			logging.Warnf("ClustMgr:handleGetGlobalTopology Index Instance Not "+
				"Found For Index Definition %v. Ignored.", idxDefn)
			continue
		}

		insts := t.GetIndexInstancesByDefn(idxDefn.DefnId)

		if len(insts) == 0 {
			logging.Warnf("ClustMgr:handleGetGlobalTopology Index Instance Not "+
				"Found For Index Definition %v. Ignored.", idxDefn)
			continue
		}

		//init Desc for pre-Spock indexes
		if idxDefn.Desc == nil {
			idxDefn.Desc = make([]bool, len(idxDefn.SecExprs))
		}

		for _, inst := range insts {

			// create partitions
			partitions := make([]common.PartitionId, len(inst.Partitions))
			versions := make([]int, len(inst.Partitions))
			for i, partn := range inst.Partitions {
				partitions[i] = common.PartitionId(partn.PartId)
				versions[i] = int(partn.Version)
			}
			pc := c.metaNotifier.makeDefaultPartitionContainer(partitions, versions, inst.NumPartitions, idxDefn.PartitionScheme, idxDefn.HashScheme)

			// create index instance
			idxInst := common.IndexInst{
				InstId:         common.IndexInstId(inst.InstId),
				Defn:           idxDefn,
				State:          common.IndexState(inst.State),
				Stream:         common.StreamId(inst.StreamId),
				ReplicaId:      int(inst.ReplicaId),
				Version:        int(inst.Version),
				RState:         common.RebalanceState(inst.RState),
				Scheduled:      inst.Scheduled,
				StorageMode:    inst.StorageMode,
				OldStorageMode: inst.OldStorageMode,
				Pc:             pc,
				RealInstId:     common.IndexInstId(inst.RealInstId),
			}

			indexInstMap[idxInst.InstId] = idxInst
		}
	}

	c.supvCmdch <- &MsgClustMgrTopology{indexInstMap: indexInstMap}
}

func (c *clustMgrAgent) handleGetLocalValue(cmd Message) {

	key := cmd.(*MsgClustMgrLocal).GetKey()

	logging.Infof("ClustMgr:handleGetLocalValue Key %v", key)

	val, err := c.mgr.GetLocalValue(key)

	c.supvCmdch <- &MsgClustMgrLocal{
		mType: CLUST_MGR_GET_LOCAL,
		key:   key,
		value: val,
		err:   err,
	}

}

func (c *clustMgrAgent) handleSetLocalValue(cmd Message) {

	key := cmd.(*MsgClustMgrLocal).GetKey()
	val := cmd.(*MsgClustMgrLocal).GetValue()

	logging.Infof("ClustMgr:handleSetLocalValue Key %v Value %v", key, val)

	err := c.mgr.SetLocalValue(key, val)

	c.supvCmdch <- &MsgClustMgrLocal{
		mType: CLUST_MGR_SET_LOCAL,
		key:   key,
		value: val,
		err:   err,
	}

}

func (c *clustMgrAgent) handleDelLocalValue(cmd Message) {

	key := cmd.(*MsgClustMgrLocal).GetKey()

	logging.Infof("ClustMgr:handleDelLocalValue Key %v", key)

	err := c.mgr.DeleteLocalValue(key)

	c.supvCmdch <- &MsgClustMgrLocal{
		mType: CLUST_MGR_DEL_LOCAL,
		key:   key,
		err:   err,
	}

}

func (c *clustMgrAgent) handleDeleteBucket(cmd Message) {

	logging.Infof("ClustMgr:handleDeleteBucket %v", cmd)

	bucket := cmd.(*MsgClustMgrUpdate).GetBucket()
	streamId := cmd.(*MsgClustMgrUpdate).GetStreamId()

	err := c.mgr.DeleteIndexForBucket(bucket, streamId)
	common.CrashOnError(err)

	c.supvCmdch <- &MsgSuccess{}
}

func (c *clustMgrAgent) handleCleanupIndex(cmd Message) {

	logging.Infof("ClustMgr:handleCleanupIndex %v", cmd)

	index := cmd.(*MsgClustMgrUpdate).GetIndexList()[0]

	err := c.mgr.CleanupIndex(index)
	common.CrashOnError(err)

	c.supvCmdch <- &MsgSuccess{}
}

func (c *clustMgrAgent) handleIndexerReady(cmd Message) {

	logging.Infof("ClustMgr:handleIndexerReady %v", cmd)

	err := c.mgr.NotifyIndexerReady()
	common.CrashOnError(err)

	c.supvCmdch <- &MsgSuccess{}
}

func (c *clustMgrAgent) handleRebalanceRunning(cmd Message) {

	logging.Infof("ClustMgr:handleRebalanceRunning %v", cmd)

	c.mgr.RebalanceRunning()
	c.supvCmdch <- &MsgSuccess{}
}

//panicHandler handles the panic from index manager
func (c *clustMgrAgent) panicHandler() {

	//panic recovery
	if rc := recover(); rc != nil {
		var err error
		switch x := rc.(type) {
		case string:
			err = errors.New(x)
		case error:
			err = x
		default:
			err = errors.New("Unknown panic")
		}

		logging.Fatalf("ClusterMgrAgent Panic Err %v", err)
		logging.Fatalf("%s", logging.StackTrace())

		//panic, propagate to supervisor
		msg := &MsgError{
			err: Error{code: ERROR_INDEX_MANAGER_PANIC,
				severity: FATAL,
				category: CLUSTER_MGR,
				cause:    err}}
		c.supvRespch <- msg
	}

}

type metaNotifier struct {
	adminCh MsgChannel
	config  common.Config
	mgr     *clustMgrAgent
}

func NewMetaNotifier(adminCh MsgChannel, config common.Config, mgr *clustMgrAgent) *metaNotifier {

	if adminCh == nil {
		return nil
	}

	return &metaNotifier{
		adminCh: adminCh,
		config:  config,
		mgr:     mgr,
	}

}

func (meta *metaNotifier) OnIndexCreate(indexDefn *common.IndexDefn, instId common.IndexInstId,
	replicaId int, partitions []common.PartitionId, versions []int, numPartitions uint32, realInstId common.IndexInstId,
	reqCtx *common.MetadataRequestContext) error {

	logging.Infof("clustMgrAgent::OnIndexCreate Notification "+
		"Received for Create Index %v %v partitions %v", indexDefn, reqCtx, partitions)

	pc := meta.makeDefaultPartitionContainer(partitions, versions, numPartitions, indexDefn.PartitionScheme, indexDefn.HashScheme)

	idxInst := common.IndexInst{InstId: instId,
		Defn:       *indexDefn,
		State:      common.INDEX_STATE_CREATED,
		Pc:         pc,
		ReplicaId:  replicaId,
		RealInstId: realInstId,
		Version:    indexDefn.InstVersion,
	}

	if idxInst.Defn.InstVersion != 0 {
		idxInst.RState = common.REBAL_PENDING
	}

	respCh := make(MsgChannel)

	meta.adminCh <- &MsgCreateIndex{mType: CLUST_MGR_CREATE_INDEX_DDL,
		indexInst: idxInst,
		respCh:    respCh,
		reqCtx:    reqCtx}

	//wait for response
	if res, ok := <-respCh; ok {

		switch res.GetMsgType() {

		case MSG_SUCCESS:
			logging.Infof("clustMgrAgent::OnIndexCreate Success "+
				"for Create Index %v", indexDefn)
			return nil

		case MSG_ERROR:
			logging.Errorf("clustMgrAgent::OnIndexCreate Error "+
				"for Create Index %v. Error %v.", indexDefn, res)
			err := res.(*MsgError).GetError()
			return &common.IndexerError{Reason: err.String(), Code: err.convertError()}

		default:
			logging.Fatalf("clustMgrAgent::OnIndexCreate Unknown Response "+
				"Received for Create Index %v. Response %v", indexDefn, res)
			common.CrashOnError(errors.New("Unknown Response"))

		}

	} else {

		logging.Fatalf("clustMgrAgent::OnIndexCreate Unexpected Channel Close "+
			"for Create Index %v", indexDefn)
		common.CrashOnError(errors.New("Unknown Response"))
	}

	return nil
}
func (meta *metaNotifier) OnIndexBuild(indexInstList []common.IndexInstId,
	buckets []string, reqCtx *common.MetadataRequestContext) map[common.IndexInstId]error {

	logging.Infof("clustMgrAgent::OnIndexBuild Notification "+
		"Received for Build Index %v %v", indexInstList, reqCtx)

	respCh := make(MsgChannel)

	meta.adminCh <- &MsgBuildIndex{indexInstList: indexInstList,
		respCh:     respCh,
		bucketList: buckets,
		reqCtx:     reqCtx}

	//wait for response
	if res, ok := <-respCh; ok {

		switch res.GetMsgType() {

		case CLUST_MGR_BUILD_INDEX_DDL_RESPONSE:
			errMap := res.(*MsgBuildIndexResponse).GetErrorMap()
			logging.Infof("clustMgrAgent::OnIndexBuild returns "+
				"for Build Index %v", indexInstList)
			return errMap

		case MSG_ERROR:
			logging.Errorf("clustMgrAgent::OnIndexBuild Error "+
				"for Build Index %v. Error %v.", indexInstList, res)
			err := res.(*MsgError).GetError()
			errMap := make(map[common.IndexInstId]error)
			for _, instId := range indexInstList {
				errMap[instId] = &common.IndexerError{Reason: err.String(), Code: err.convertError()}
			}

			return errMap

		default:
			logging.Fatalf("clustMgrAgent::OnIndexBuild Unknown Response "+
				"Received for Build Index %v. Response %v", indexInstList, res)
			common.CrashOnError(errors.New("Unknown Response"))

		}

	} else {

		logging.Fatalf("clustMgrAgent::OnIndexBuild Unexpected Channel Close "+
			"for Create Index %v", indexInstList)
		common.CrashOnError(errors.New("Unknown Response"))
	}

	return nil
}

func (meta *metaNotifier) OnIndexDelete(instId common.IndexInstId,
	bucket string, reqCtx *common.MetadataRequestContext) error {

	logging.Infof("clustMgrAgent::OnIndexDelete Notification "+
		"Received for Drop IndexId %v %v", instId, reqCtx)

	respCh := make(MsgChannel)

	//Treat DefnId as InstId for now
	meta.adminCh <- &MsgDropIndex{mType: CLUST_MGR_DROP_INDEX_DDL,
		indexInstId: instId,
		respCh:      respCh,
		bucket:      bucket,
		reqCtx:      reqCtx}

	//wait for response
	if res, ok := <-respCh; ok {

		switch res.GetMsgType() {

		case MSG_SUCCESS:
			logging.Infof("clustMgrAgent::OnIndexDelete Success "+
				"for Drop IndexId %v", instId)
			return nil

		case MSG_ERROR:
			logging.Errorf("clustMgrAgent::OnIndexDelete Error "+
				"for Drop IndexId %v. Error %v", instId, res)
			err := res.(*MsgError).GetError()
			return &common.IndexerError{Reason: err.String(), Code: err.convertError()}

		default:
			logging.Fatalf("clustMgrAgent::OnIndexDelete Unknown Response "+
				"Received for Drop IndexId %v. Response %v", instId, res)
			common.CrashOnError(errors.New("Unknown Response"))

		}

	} else {
		logging.Fatalf("clustMgrAgent::OnIndexDelete Unexpected Channel Close "+
			"for Drop IndexId %v", instId)
		common.CrashOnError(errors.New("Unknown Response"))

	}

	return nil
}

func (meta *metaNotifier) OnPartitionPrune(instId common.IndexInstId, partitions []common.PartitionId, reqCtx *common.MetadataRequestContext) error {

	logging.Infof("clustMgrAgent::OnPartitionPrune Notification "+
		"Received for Prune Partition IndexId %v %v %v", instId, partitions, reqCtx)

	respCh := make(MsgChannel)

	//Treat DefnId as InstId for now
	meta.adminCh <- &MsgClustMgrPrunePartition{
		instId:     instId,
		partitions: partitions,
		respCh:     respCh}

	//wait for response
	if res, ok := <-respCh; ok {

		switch res.GetMsgType() {

		case MSG_SUCCESS:
			logging.Infof("clustMgrAgent::OnPrunePartition Success "+
				"for IndexId %v", instId)
			return nil

		case MSG_ERROR:
			logging.Errorf("clustMgrAgent::OnPrunePartition Error "+
				"for IndexId %v. Error %v", instId, res)
			err := res.(*MsgError).GetError()
			return &common.IndexerError{Reason: err.String(), Code: err.convertError()}

		default:
			logging.Fatalf("clustMgrAgent::OnPrunePartition Unknown Response "+
				"Received for IndexId %v. Response %v", instId, res)
			common.CrashOnError(errors.New("Unknown Response"))

		}

	} else {
		logging.Fatalf("clustMgrAgent::OnPrunePartition Unexpected Channel Close "+
			"for IndexId %v", instId)
		common.CrashOnError(errors.New("Unknown Response"))
	}

	return nil
}

func (meta *metaNotifier) OnFetchStats() error {

	go meta.fetchStats()

	return nil
}

func (meta *metaNotifier) fetchStats() {

	ticker := time.NewTicker(time.Millisecond * 500)
	defer ticker.Stop()
	startTime := time.Now()

	for range ticker.C {
		if meta.mgr.stats.Get() != nil && atomic.LoadUint64(&meta.mgr.statsCount) != 0 {
			meta.mgr.handleStatsInternal()
			logging.Infof("Fetch new stats upon request by life cycle manager")
			return
		}

		if time.Now().Sub(startTime) > time.Minute {
			return
		}
	}
}

func (meta *metaNotifier) makeDefaultPartitionContainer(partitions []common.PartitionId, versions []int, numPartitions uint32,
	scheme common.PartitionScheme, hash common.HashScheme) common.PartitionContainer {

	numVbuckets := meta.config["numVbuckets"].Int()
	pc := common.NewKeyPartitionContainer(numVbuckets, int(numPartitions), scheme, hash)

	//Add one partition for now
	addr := net.JoinHostPort("", meta.config["streamMaintPort"].String())
	endpt := []common.Endpoint{common.Endpoint(addr)}

	for i, partnId := range partitions {
		partnDefn := common.KeyPartitionDefn{Id: partnId, Version: versions[i], Endpts: endpt}
		pc.AddPartition(partnId, partnDefn)
	}

	return pc

}
