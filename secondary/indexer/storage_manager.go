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
	"bytes"
	"encoding/gob"
	"errors"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbaselabs/goforestdb"
)

var (
	ErrIndexRollback = errors.New("Indexer rollback")
)

//StorageManager manages the snapshots for the indexes and responsible for storing
//indexer metadata in a config database
//TODO - Add config database storage

const INST_MAP_KEY_NAME = "IndexInstMap"

type StorageManager interface {
}

type storageMgr struct {
	supvCmdch  MsgChannel //supervisor sends commands on this channel
	supvRespch MsgChannel //channel to send any async message to supervisor

	indexInstMap  common.IndexInstMap
	indexPartnMap IndexPartnMap

	// Latest readable index snapshot for each index instance
	indexSnapMap map[common.IndexInstId]IndexSnapshot
	// List of waiters waiting for a snapshot to be created with expected
	// atleast-timestamp
	waitersMap map[common.IndexInstId][]*snapshotWaiter

	dbfile *forestdb.File
	meta   *forestdb.KVStore // handle for index meta

	config common.Config
}

type snapshotWaiter struct {
	wch       chan interface{}
	ts        *common.TsVbuuid
	idxInstId common.IndexInstId
}

func newSnapshotWaiter(idxId common.IndexInstId, ts *common.TsVbuuid,
	ch chan interface{}) *snapshotWaiter {

	return &snapshotWaiter{
		ts:        ts,
		wch:       ch,
		idxInstId: idxId,
	}
}

func (w *snapshotWaiter) Notify(is IndexSnapshot) {
	w.wch <- is
}

func (w *snapshotWaiter) Error(err error) {
	w.wch <- err
}

//NewStorageManager returns an instance of storageMgr or err message
//It listens on supvCmdch for command and every command is followed
//by a synchronous response of the supvCmdch.
//Any async response to supervisor is sent to supvRespch.
//If supvCmdch get closed, storageMgr will shut itself down.
func NewStorageManager(supvCmdch MsgChannel, supvRespch MsgChannel,
	indexPartnMap IndexPartnMap, config common.Config) (
	StorageManager, Message) {

	//Init the storageMgr struct
	s := &storageMgr{
		supvCmdch:    supvCmdch,
		supvRespch:   supvRespch,
		indexSnapMap: make(map[common.IndexInstId]IndexSnapshot),
		waitersMap:   make(map[common.IndexInstId][]*snapshotWaiter),
		config:       config,
	}

	fdbconfig := forestdb.DefaultConfig()
	kvconfig := forestdb.DefaultKVStoreConfig()
	var err error

	if s.dbfile, err = forestdb.Open("meta", fdbconfig); err != nil {
		return nil, &MsgError{err: Error{cause: err}}
	}

	// Make use of default kvstore provided by forestdb
	if s.meta, err = s.dbfile.OpenKVStore("default", kvconfig); err != nil {
		return nil, &MsgError{err: Error{cause: err}}
	}

	s.updateIndexSnapMap(indexPartnMap)

	//start Storage Manager loop which listens to commands from its supervisor
	go s.run()

	return s, &MsgSuccess{}

}

//run starts the storage manager loop which listens to messages
//from its supervisor(indexer)
func (s *storageMgr) run() {

	//main Storage Manager loop
loop:
	for {
		select {

		case cmd, ok := <-s.supvCmdch:
			if ok {
				if cmd.GetMsgType() == STORAGE_MGR_SHUTDOWN {
					common.Infof("StorageManager::run Shutting Down")
					s.supvCmdch <- &MsgSuccess{}
					break loop
				}
				s.handleSupvervisorCommands(cmd)
			} else {
				//supervisor channel closed. exit
				break loop
			}

		}
	}
}

func (s *storageMgr) handleSupvervisorCommands(cmd Message) {

	switch cmd.GetMsgType() {

	case MUT_MGR_FLUSH_DONE:
		s.handleCreateSnapshot(cmd)

	case INDEXER_ROLLBACK:
		s.handleRollback(cmd)

	case UPDATE_INDEX_INSTANCE_MAP:
		s.handleUpdateIndexInstMap(cmd)

	case UPDATE_INDEX_PARTITION_MAP:
		s.handleUpdateIndexPartnMap(cmd)

	case STORAGE_INDEX_SNAP_REQUEST:
		s.handleGetIndexSnapshot(cmd)

	case STORAGE_INDEX_STORAGE_STATS:
		s.handleGetIndexStorageStats(cmd)
	}
}

//handleCreateSnapshot will create the necessary snapshots
//after flush has completed
func (s *storageMgr) handleCreateSnapshot(cmd Message) {

	common.Tracef("StorageMgr::handleCreateSnapshot %v", cmd)

	bucket := cmd.(*MsgMutMgrFlushDone).GetBucket()
	tsVbuuid := cmd.(*MsgMutMgrFlushDone).GetTS()
	numVbuckets := s.config["numVbuckets"].Int()
	// TODO: Fill this flag from incoming message
	var needsCommit bool = true

	//for every index managed by this indexer
	for idxInstId, partnMap := range s.indexPartnMap {
		idxInst := s.indexInstMap[idxInstId]

		//if index belongs to the flushed bucket
		if idxInst.Defn.Bucket == bucket {

			lastIndexSnap := s.indexSnapMap[idxInstId]
			// List of snapshots for reading current timestamp
			var isSnapCreated bool = true

			partnSnaps := make(map[common.PartitionId]PartitionSnapshot)
			//for all partitions managed by this indexer
			for partnId, partnInst := range partnMap {
				var lastPartnSnap PartitionSnapshot

				if lastIndexSnap != nil {
					lastPartnSnap = lastIndexSnap.Partitions()[partnId]
				}
				sc := partnInst.Sc

				sliceSnaps := make(map[SliceId]SliceSnapshot)
				//create snapshot for all the slices
				for _, slice := range sc.GetAllSlices() {
					var latestSnapshot Snapshot
					if lastIndexSnap != nil {
						lastSliceSnap := lastPartnSnap.Slices()[slice.Id()]
						latestSnapshot = lastSliceSnap.Snapshot()
					}

					//if flush timestamp is greater than last
					//snapshot timestamp, create a new snapshot

					snapTs := NewTimestamp(numVbuckets)
					if latestSnapshot != nil {
						snapTsVbuuid := latestSnapshot.Timestamp()
						snapTs = getStabilityTSFromTsVbuuid(snapTsVbuuid)
					}

					ts := getStabilityTSFromTsVbuuid(tsVbuuid)

					//if the flush TS is greater than the last snapshot TS
					//TODO Is it better to have a IsDirty() in Slice interface
					//rather than comparing the last snapshot?
					if latestSnapshot == nil || ts.GreaterThan(snapTs) {
						//commit the outstanding data

						common.Tracef("StorageMgr::handleCreateSnapshot \n\tCommit Data Index: "+
							"%v PartitionId: %v SliceId: %v", idxInstId, partnId, slice.Id())

						newTsVbuuid := tsVbuuid.Copy()
						var err error
						var info SnapshotInfo
						var newSnapshot Snapshot

						common.Tracef("StorageMgr::handleCreateSnapshot \n\tCreating New Snapshot "+
							"Index: %v PartitionId: %v SliceId: %v Commit:%v", idxInstId, partnId, slice.Id(), needsCommit)
						if info, err = slice.NewSnapshot(newTsVbuuid, needsCommit); err != nil {
							common.Errorf("handleCreateSnapshot::handleCreateSnapshot \n\tError "+
								"Creating new snapshot Slice Index: %v Slice: %v. Skipped. Error %v", idxInstId,
								slice.Id(), err)
							isSnapCreated = false
							continue
						}

						if newSnapshot, err = slice.OpenSnapshot(info); err != nil {
							common.Errorf("StorageMgr::handleCreateSnapshot \n\tError Creating Snapshot "+
								"for Index: %v Slice: %v. Skipped. Error %v", idxInstId,
								slice.Id(), err)
							isSnapCreated = false
							continue
						}

						common.Debugf("StorageMgr::handleCreateSnapshot \n\tAdded New Snapshot Index: %v "+
							"PartitionId: %v SliceId: %v (%v)", idxInstId, partnId, slice.Id(), info)

						ss := &sliceSnapshot{
							id:   slice.Id(),
							snap: newSnapshot,
						}
						sliceSnaps[slice.Id()] = ss
					} else {
						// Increment reference
						latestSnapshot.Open()
						ss := &sliceSnapshot{
							id:   slice.Id(),
							snap: latestSnapshot,
						}
						sliceSnaps[slice.Id()] = ss
						common.Debugf("StorageMgr::handleCreateSnapshot \n\tSkipped Creating New Snapshot for Index %v "+
							"PartitionId %v SliceId %v. No New Mutations.", idxInstId, partnId, slice.Id())
						continue
					}
				}

				ps := &partitionSnapshot{
					id:     partnId,
					slices: sliceSnaps,
				}
				partnSnaps[partnId] = ps
			}

			is := &indexSnapshot{
				instId: idxInstId,
				ts:     tsVbuuid,
				partns: partnSnaps,
			}

			if isSnapCreated {
				// Update index-snapshot map whenever a snapshot is created for an index
				DestroyIndexSnapshot(s.indexSnapMap[idxInstId])
				s.indexSnapMap[idxInstId] = is

				// Also notify any waiters for snapshots creation
				var newWaiters []*snapshotWaiter
				for _, w := range s.waitersMap[idxInstId] {
					if w.ts == nil || tsVbuuid.AsRecent(w.ts) {
						snap := CloneIndexSnapshot(is)
						w.Notify(snap)
					} else {
						newWaiters = append(newWaiters, w)
					}
				}
				s.waitersMap[idxInstId] = newWaiters
			} else {
				DestroyIndexSnapshot(is)
			}
		}
	}

	s.supvCmdch <- &MsgSuccess{}

}

//handleRollback will rollback to given timestamp
func (sm *storageMgr) handleRollback(cmd Message) {

	streamId := cmd.(*MsgRollback).GetStreamId()
	rollbackTs := cmd.(*MsgRollback).GetRollbackTs()
	numVbuckets := sm.config["numVbuckets"].Int()

	respTs := make(map[string]*common.TsVbuuid)

	//for every index managed by this indexer
	for idxInstId, partnMap := range sm.indexPartnMap {
		idxInst := sm.indexInstMap[idxInstId]

		//if this bucket needs to be rolled back
		if ts, ok := rollbackTs[idxInst.Defn.Bucket]; ok {

			//for all partitions managed by this indexer
			for partnId, partnInst := range partnMap {
				sc := partnInst.Sc

				//rollback all slices
				for _, slice := range sc.GetAllSlices() {
					infos, err := slice.GetSnapshots()
					// TODO: Proper error handling if possible
					if err != nil {
						panic("Unable read snapinfo -" + err.Error())
					}
					s := NewSnapshotInfoContainer(infos)
					snapInfo := s.GetOlderThanTS(ts)
					if snapInfo != nil {
						err := slice.Rollback(snapInfo)
						if err == nil {
							common.Debugf("StorageMgr::handleRollback \n\t Rollback Index: %v "+
								"PartitionId: %v SliceId: %v To Snapshot %v ", idxInstId, partnId,
								slice.Id(), snapInfo)
							respTs[idxInst.Defn.Bucket] = snapInfo.Timestamp()
						} else {
							//send error response back
							//TODO handle the case where some of the slices fail to rollback
							sm.supvCmdch <- &MsgError{err: Error{code: ERROR_STORAGE_MGR_ROLLBACK_FAIL,
								severity: FATAL,
								category: STORAGE_MGR,
								cause:    err}}
							return
						}

					} else {
						//if there is no snapshot available, rollback to zero
						err := slice.RollbackToZero()
						if err == nil {
							common.Debugf("StorageMgr::handleRollback \n\t Rollback Index: %v "+
								"PartitionId: %v SliceId: %v To Zero ", idxInstId, partnId,
								slice.Id())
							respTs[idxInst.Defn.Bucket] = common.NewTsVbuuid(idxInst.Defn.Bucket, numVbuckets)
						} else {
							//send error response back
							//TODO handle the case where some of the slices fail to rollback
							sm.supvCmdch <- &MsgError{err: Error{code: ERROR_STORAGE_MGR_ROLLBACK_FAIL,
								severity: FATAL,
								category: STORAGE_MGR,
								cause:    err}}
							return
						}
					}

				}
			}
		}
	}

	// Notify all scan waiters for all indexes with error
	for idxInstId, waiters := range sm.waitersMap {
		idxInst := sm.indexInstMap[idxInstId]
		if _, ok := rollbackTs[idxInst.Defn.Bucket]; ok {
			for _, w := range waiters {
				w.Error(ErrIndexRollback)
			}
		}
	}

	sm.updateIndexSnapMap(sm.indexPartnMap)

	sm.supvCmdch <- &MsgRollback{streamId: streamId,
		rollbackTs: respTs}
}

func (s *storageMgr) handleUpdateIndexInstMap(cmd Message) {

	common.Infof("StorageMgr::handleUpdateIndexInstMap %v", cmd)
	indexInstMap := cmd.(*MsgUpdateInstMap).GetIndexInstMap()
	s.indexInstMap = common.CopyIndexInstMap(indexInstMap)

	// Remove all snapshot waiters for indexes that do not exist anymore
	for id, ws := range s.waitersMap {
		if _, ok := s.indexInstMap[id]; !ok {
			for _, w := range ws {
				w.Error(ErrIndexNotFound)
			}
			delete(s.waitersMap, id)
		}
	}

	// Cleanup all invalid index's snapshots
	for idxInstId, is := range s.indexSnapMap {
		if _, ok := s.indexInstMap[idxInstId]; !ok {
			DestroyIndexSnapshot(is)
			delete(s.indexSnapMap, idxInstId)
		}
	}

	instMap := common.CopyIndexInstMap(s.indexInstMap)

	for id, inst := range instMap {
		inst.Pc = nil
		instMap[id] = inst
	}

	//store indexInstMap in metadata store
	var instBytes bytes.Buffer
	var err error

	enc := gob.NewEncoder(&instBytes)
	err = enc.Encode(instMap)
	if err != nil {
		common.Errorf("StorageMgr::handleUpdateIndexInstMap \n\t Error Marshalling "+
			"IndexInstMap %v. Err %v", instMap, err)
	}

	if err = s.meta.SetKV([]byte(INST_MAP_KEY_NAME), instBytes.Bytes()); err != nil {
		common.Errorf("StorageMgr::handleUpdateIndexInstMap \n\tError "+
			"Storing IndexInstMap %v", err)
	}

	s.dbfile.Commit(forestdb.COMMIT_MANUAL_WAL_FLUSH)

	s.supvCmdch <- &MsgSuccess{}
}

func (s *storageMgr) handleUpdateIndexPartnMap(cmd Message) {

	common.Infof("StorageMgr::handleUpdateIndexPartnMap %v", cmd)
	indexPartnMap := cmd.(*MsgUpdatePartnMap).GetIndexPartnMap()
	s.indexPartnMap = CopyIndexPartnMap(indexPartnMap)

	s.supvCmdch <- &MsgSuccess{}
}

// Process req for providing an index snapshot for index scan.
// The request contains atleast-timestamp and the storage
// manager will reply with a index snapshot soon after a
// snapshot meeting requested criteria is available.
// The requester will block wait until the response is
// available.
func (s *storageMgr) handleGetIndexSnapshot(cmd Message) {
	s.supvCmdch <- &MsgSuccess{}

	req := cmd.(*MsgIndexSnapRequest)
	_, found := s.indexInstMap[req.GetIndexId()]
	if !found {
		req.respch <- ErrIndexNotFound
		return
	}

	// Return snapshot immediately if a matching snapshot exists already
	// Otherwise add into waiters list so that next snapshot creation event
	// can notify the requester when a snapshot with matching timestamp
	// is available.
	is := s.indexSnapMap[req.GetIndexId()]
	// - If atleast-ts is nil and no snapshot is available, send nil ts
	// - If atleast-ts is not-nil and no snapshot is available, wait until
	// it is available.
	if req.GetTS() == nil || is.Timestamp().AsRecent(req.GetTS()) {
		snap := CloneIndexSnapshot(is)
		req.respch <- snap
	} else {
		w := newSnapshotWaiter(req.GetIndexId(), req.GetTS(), req.GetReplyChannel())
		ws, exists := s.waitersMap[req.GetIndexId()]
		if exists {
			s.waitersMap[req.idxInstId] = append(ws, w)
		} else {
			s.waitersMap[req.idxInstId] = []*snapshotWaiter{w}
		}
	}
}

func (s *storageMgr) handleGetIndexStorageStats(cmd Message) {
	s.supvCmdch <- &MsgSuccess{}
	req := cmd.(*MsgIndexStorageStats)
	replych := req.GetReplyChannel()

	var stats []IndexStorageStats
	var err error
	var sts StorageStatistics

	for idxInstId, partnMap := range s.indexPartnMap {
		var dataSz, diskSz int64
	loop:
		for _, partnInst := range partnMap {
			for _, slice := range partnInst.Sc.GetAllSlices() {
				sts, err = slice.Statistics()
				if err != nil {
					break loop
				}

				dataSz += sts.DataSize
				diskSz += sts.DiskSize
			}
		}

		if err == nil {
			stat := IndexStorageStats{
				InstId: idxInstId,
				Stats: StorageStatistics{
					DataSize: dataSz,
					DiskSize: diskSz,
				},
			}

			stats = append(stats, stat)
		}
	}

	replych <- stats
}

// Update index-snapshot map using index partition map
// This function should be called only during initialization
// of storage manager and during rollback.
// FIXME: Current implementation makes major assumption that
// single slice is supported.
func (s *storageMgr) updateIndexSnapMap(indexPartnMap IndexPartnMap) {
	var tsVbuuid *common.TsVbuuid
	for idxInstId, partnMap := range indexPartnMap {
		//there is only one partition for now
		partnInst := partnMap[0]
		sc := partnInst.Sc

		//there is only one slice for now
		slice := sc.GetSliceById(0)
		infos, err := slice.GetSnapshots()
		// TODO: Proper error handling if possible
		if err != nil {
			panic("Unable to read snapinfo -" + err.Error())
		}

		DestroyIndexSnapshot(s.indexSnapMap[idxInstId])
		delete(s.indexSnapMap, idxInstId)

		snapInfoContainer := NewSnapshotInfoContainer(infos)
		latestSnapshotInfo := snapInfoContainer.GetLatest()

		if latestSnapshotInfo != nil {
			common.Infof("StorageMgr::updateIndexSnapMap IndexInst:%v Attempting to open snapshot (%v)",
				idxInstId, latestSnapshotInfo)
			latestSnapshot, err := slice.OpenSnapshot(latestSnapshotInfo)
			if err != nil {
				panic("Unable to open snapshot -" + err.Error())
			}
			ss := &sliceSnapshot{
				id:   SliceId(0),
				snap: latestSnapshot,
			}

			tsVbuuid = latestSnapshotInfo.Timestamp()

			sid := SliceId(0)
			pid := common.PartitionId(0)

			ps := &partitionSnapshot{
				id:     pid,
				slices: map[SliceId]SliceSnapshot{sid: ss},
			}

			is := &indexSnapshot{
				instId: idxInstId,
				ts:     tsVbuuid,
				partns: map[common.PartitionId]PartitionSnapshot{pid: ps},
			}
			s.indexSnapMap[idxInstId] = is
		}
	}
}
