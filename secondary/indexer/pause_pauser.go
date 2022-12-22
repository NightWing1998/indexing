// @copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indexer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/manager"
)

////////////////////////////////////////////////////////////////////////////////////////////////////
// PauseUploadToken and PauseUploadState
// Used to convey state change information between Pause master and follower nodes.
////////////////////////////////////////////////////////////////////////////////////////////////////

type PauseUploadState byte

const (
	// Posted is the initial state as generated by master. Indicates to followers to start upload work.
	// Followers change state to InProgress.
	PauseUploadTokenPosted PauseUploadState = iota

	// InProgess indicates that followers are actually performing the upload work. In addition, any
	// upload work to be done only by master is also carried out. Once all the work is done, followers
	// change the state to Processed.
	PauseUploadTokenInProgess

	// Processed indicates that for a follower, all the upload work is completed. Master will delete the token
	// and once all followers are done, cleanup is initiated.
	PauseUploadTokenProcessed

	// Error indicates that for a follower, an error was encountered during upload. Master will initiate cleanup.
	PauseUploadTokenError
)

func (s PauseUploadState) String() string {
	switch s {
	case PauseUploadTokenPosted:
		return "PauseUploadTokenPosted"
	case PauseUploadTokenInProgess:
		return "PauseUploadTokenInProgess"
	case PauseUploadTokenProcessed:
		return "PauseUploadTokenProcessed"
	case PauseUploadTokenError:
		return "PauseUploadTokenError"
	}

	return fmt.Sprintf("PauseUploadState-UNKNOWN-STATE-[%v]", byte(s))
}

const PauseUploadTokenTag = "PauseUploadToken"

type PauseUploadToken struct {
	MasterId   string
	FollowerId string
	PauseId    string
	State      PauseUploadState
	BucketName string
	Error      string
}

func newPauseUploadToken(masterUuid, followerUuid, pauseId, bucketName string) (string, *PauseUploadToken, error) {
	put := &PauseUploadToken{
		MasterId:   masterUuid,
		FollowerId: followerUuid,
		PauseId:    pauseId,
		State:      PauseUploadTokenPosted,
		BucketName: bucketName,
	}

	ustr, err := common.NewUUID()
	if err != nil {
		logging.Warnf("newPauseUploadToken: Failed to generate uuid: err[%v]", err)
		return "", nil, err
	}

	putId := fmt.Sprintf("%s%s", PauseUploadTokenTag, ustr.Str())

	return putId, put, nil
}

func decodePauseUploadToken(path string, value []byte) (string, *PauseUploadToken, error) {

	putIdPos := strings.Index(path, PauseUploadTokenTag)
	putId := path[putIdPos:]

	put := &PauseUploadToken{}
	err := json.Unmarshal(value, put)
	if err != nil {
		logging.Errorf("decodePauseUploadToken: Failed to unmarshal value[%s] path[%v]: err[%v]",
			string(value), path, err)
		return "", nil, err
	}

	return putId, put, nil
}

func (put *PauseUploadToken) Clone() *PauseUploadToken {
	put1 := *put
	put2 := put1
	return &put2
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Pauser class - Perform the Pause of a given bucket (similar to Rebalancer's role).
// This is used only on the master node of a task_PAUSE task to do the GSI orchestration.
////////////////////////////////////////////////////////////////////////////////////////////////////

// Pauser object holds the state of Pause orchestration
type Pauser struct {
	// nodeDir is "node_<nodeId>/" for this node, where nodeId is the 32-digit hex ID from ns_server
	nodeDir string

	// otherIndexAddrs is "host:port" to all the known Index Service nodes EXCLUDING this one
	otherIndexAddrs []string

	// pauseMgr is the singleton parent of this object
	pauseMgr *PauseServiceManager

	// task is the task_PAUSE task we are executing (protected by task.taskMu). It lives in the
	// pauseMgr.tasks map (protected by pauseMgr.tasksMu). Only the current object should change or
	// delete task at this point, but GetTaskList and other processing may concurrently read it.
	// Thus Pauser needs to write lock task.taskMu for changes but does not need to read lock it.
	task *taskObj
}

// RunPauser creates a Pauser instance to execute the given task. It saves a pointer to itself in
// task.pauser (visible to pauseMgr parent) and launches a goroutine for the work.
//
//	pauseMgr - parent object (singleton)
//	task - the task_PAUSE task this object will execute
//	master - true iff this node is the master
func RunPauser(pauseMgr *PauseServiceManager, task *taskObj, master bool) {
	pauser := &Pauser{
		pauseMgr: pauseMgr,
		task:     task,
		nodeDir:  "node_" + string(pauseMgr.genericMgr.nodeInfo.NodeID) + "/",
	}

	task.taskMu.Lock()
	task.pauser = pauser
	task.taskMu.Unlock()

	go pauser.run(master)
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Methods
////////////////////////////////////////////////////////////////////////////////////////////////////

// restGetLocalIndexMetadataBinary calls the /getLocalndexMetadata REST API (request_handler.go) via
// self-loopback to get the index metadata for the current node and the task's bucket (tenant). This
// verifies it can be unmarshaled, but it returns a checksummed and optionally compressed byte slice
// version of the data rather than the unmarshaled object.
func (this *Pauser) restGetLocalIndexMetadataBinary(compress bool) ([]byte, *manager.LocalIndexMetadata, error) {
	const _restGetLocalIndexMetadataBinary = "Pauser::restGetLocalIndexMetadataBinary:"

	url := fmt.Sprintf("%v/getLocalIndexMetadata?useETag=false&bucket=%v",
		this.pauseMgr.httpAddr, this.task.bucket)
	resp, err := getWithAuth(url)
	if err != nil {
		this.failPause(_restGetLocalIndexMetadataBinary, fmt.Sprintf("getWithAuth(%v)", url), err)
		return nil, nil, err
	}
	defer resp.Body.Close()

	byteSlice, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		this.failPause(_restGetLocalIndexMetadataBinary, "ReadAll(resp.Body)", err)
		return nil, nil, err
	}

	// Verify response can be unmarshaled
	metadata := new(manager.LocalIndexMetadata)
	err = json.Unmarshal(byteSlice, metadata)
	if err != nil {
		this.failPause(_restGetLocalIndexMetadataBinary, "Unmarshal localMeta", err)
		return nil, nil, err
	}
	if len(metadata.IndexDefinitions) == 0 {
		return nil, nil, nil
	}

	// Return checksummed and optionally compressed byte slice, not the unmarshaled object
	return common.ChecksumAndCompress(byteSlice, compress), metadata, nil
}

// failPause logs an error using the caller's logPrefix and a provided context string and aborts the
// Pause task. If there is a set of known Indexer nodes, it will also try to notify them.
func (this *Pauser) failPause(logPrefix string, context string, error error) {
	logging.Errorf("%v Aborting Pause task %v due to %v error: %v", logPrefix,
		this.task.taskId, context, error)

	// Mark the task as failed directly here on master node (avoids dependency on loopback REST)
	this.task.TaskObjSetFailed(error.Error())

	// Notify other Index nodes
	this.pauseMgr.RestNotifyFailedTask(this.otherIndexAddrs, this.task, error.Error())
}

// run is a goroutine for the main body of Pause work for this.task.
//
//	master - true iff this node is the master
func (this *Pauser) run(master bool) {

	// Get the list of Index node host:port addresses EXCLUDING this one
	this.otherIndexAddrs = this.pauseMgr.GetIndexerNodeAddresses(this.pauseMgr.httpAddr)

	var byteSlice []byte
	var err error
	reader := bytes.NewReader(nil)

	/////////////////////////////////////////////
	// Work done by master only
	/////////////////////////////////////////////

	if master {
		// Write the version.json file to the archive
		byteSlice = []byte(fmt.Sprintf("{\"version\":%v}\n", ARCHIVE_VERSION))
		reader.Reset(byteSlice)
		err = Upload(this.task.archivePath, FILENAME_VERSION, reader)
		if err != nil {
			this.failPause("Pauser::run:", "Upload "+FILENAME_VERSION, err)
			return
		}

		logging.Tracef("Pauser::run: indexer version successfully uploaded to %v%v for taskId %v", this.task.archivePath, FILENAME_VERSION, this.task.taskId)

		// Notify the followers to start working on this task
		this.pauseMgr.RestNotifyPause(this.otherIndexAddrs, this.task)
	} // if master

	/////////////////////////////////////////////
	// Work done by both master and followers
	/////////////////////////////////////////////

	// nodePath is the path to the node-specific archive subdirectory for the current node
	nodePath := this.task.archivePath + this.nodeDir
	dbg := true // TODO: use system config here

	// Get the index metadata from all nodes and write it as a single file to the archive
	byteSlice, indexMetadata, err := this.restGetLocalIndexMetadataBinary(!dbg)
	if err != nil {
		this.failPause("Pauser::run:", "getLocalInstanceMetadata", err)
		return
	}
	if byteSlice == nil {
		// there are no indexes on this node for bucket. pause is a no-op
		logging.Infof("Pauser::run: pause is a no-op for bucket %v", this.task.bucket)
		return
	}
	reader.Reset(byteSlice)
	err = Upload(nodePath, FILENAME_METADATA, reader)
	if err != nil {
		this.failPause("Pauser::run:", "Upload "+FILENAME_METADATA, err)
		return
	}
	logging.Infof("Pauser::run: metadata successfully uploaded to %v%v for taskId %v", nodePath, FILENAME_METADATA, this.task.taskId)

	getIndexInstanceIds := func() []common.IndexInstId {
		res := make([]common.IndexInstId, 0, len(indexMetadata.IndexDefinitions))
		for _, topology := range indexMetadata.IndexTopologies {
			for _, indexDefn := range topology.Definitions {
				res = append(res, common.IndexInstId(indexDefn.Instances[0].InstId))
			}
		}
		logging.Tracef("Pauser::run::getIndexInstanceId: index instance ids: %v for bucket %v", res, this.task.bucket)
		return res
	}

	// Write the persistent stats to the archive
	byteSlice, err = this.pauseMgr.genericMgr.statsMgr.GetStatsForIndexesToBePersisted(getIndexInstanceIds(), !dbg)
	if err != nil {
		this.failPause("Pauser::run:", "GetStatsForIndexesToBePersisted", err)
		return
	}
	reader.Reset(byteSlice)
	err = Upload(nodePath, FILENAME_STATS, reader)
	if err != nil {
		this.failPause("Pauser::run:", "Upload "+FILENAME_STATS, err)
		return
	}

	logging.Infof("Pauser::run: stats successfully uploaded to %v%v for taskId %v", nodePath, FILENAME_STATS, this.task.taskId)

	getShardIds := func() []common.ShardId {
		uniqueShardIds := make(map[common.ShardId]bool)
		for _, topology := range indexMetadata.IndexTopologies {
			for _, indexDefn := range topology.Definitions {
				for _, instance := range indexDefn.Instances {
					for _, partition := range instance.Partitions {
						for _, shard := range partition.ShardIds {
							uniqueShardIds[shard] = true
						}
					}

				}
			}
		}

		shardIds := make([]common.ShardId, 0, len(uniqueShardIds))
		for shardId, _ := range uniqueShardIds {
			shardIds = append(shardIds, shardId)
		}
		logging.Tracef("Pauser::run::getShardIds: found shard Ids %v for bucket %v", shardIds, this.task.bucket)

		return shardIds
	}

	// TODO: add contextWithCancel to task and reuse it here
	ctx := context.Background()
	closeCh := ctx.Done()
	respCh := make(chan Message)
	// progressCh := make(chan *ShardTransferStatistics, 1000) TODO: progress reporting

	msg := &MsgStartShardTransfer{
		shardIds:    getShardIds(),
		taskId:      this.task.taskId,
		transferId:  this.task.bucket,
		taskType:    common.PauseResumeTask,
		destination: nodePath,

		cancelCh:   closeCh,
		doneCh:     closeCh,
		respCh:     respCh,
		progressCh: nil,
	}

	this.pauseMgr.supvMsgch <- msg

	resp, ok := (<-respCh).(*MsgShardTransferResp)
	if !ok || resp == nil {
		err := fmt.Errorf("couldn't get a valid response from ShardTransferManager")
		this.failPause("Pauser::run", "Upload plasma shards", err)
		logging.Errorf("Pauser::run %v for taskId %v", err, this.task.taskId)
		return
	}
	errMap := resp.GetErrorMap()
	var errMsg strings.Builder
	for shard, err := range errMap {
		if err != nil {
			fmt.Fprintf(&errMsg, "Error in shardId %v upload: %v\n", shard, err)
		}
	}
	if errMsg.Len() != 0 {
		err = errors.New(errMsg.String())
		this.failPause("Pauser::run", "Upload plasma shards", err)
		logging.Errorf("Pauser::run shard uploads failed: %v for taskId %v", err, this.task.taskId)
		return
	}

	// TODO: return shard paths back to master
	logging.Infof("Pauser::run Shards saved at: %v for bucket %v", resp.GetShardPaths(), this.task.bucket)
}
