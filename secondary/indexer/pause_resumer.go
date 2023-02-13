// @copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indexer

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/manager"
	"github.com/couchbase/indexing/secondary/planner"
	"github.com/couchbase/plasma"
)

func newResumeDownloadToken(masterUuid, followerUuid, resumeId, bucketName, uploaderId string) (
	string, *common.ResumeDownloadToken, error) {

	rdt := &common.ResumeDownloadToken{
		MasterId:   masterUuid,
		FollowerId: followerUuid,
		ResumeId:   resumeId,
		State:      common.ResumeDownloadTokenPosted,
		BucketName: bucketName,
		UploaderId: uploaderId,
	}

	ustr, err := common.NewUUID()
	if err != nil {
		logging.Warnf("newResumeDownloadToken: Failed to generate uuid: err[%v]", err)
		return "", nil, err
	}

	rdtId := fmt.Sprintf("%s%s", common.ResumeDownloadTokenTag, ustr.Str())

	return rdtId, rdt, nil
}

func decodeResumeDownloadToken(path string, value []byte) (string, *common.ResumeDownloadToken, error) {

	rdtIdPos := strings.Index(path, common.ResumeDownloadTokenTag)
	if rdtIdPos < 0 {
		return "", nil, fmt.Errorf("ResumeDownloadTokenTag[%v] not present in metakv path[%v]",
			common.ResumeDownloadTokenTag, path)
	}

	rdtId := path[rdtIdPos:]

	rdt := &common.ResumeDownloadToken{}
	err := json.Unmarshal(value, rdt)
	if err != nil {
		logging.Errorf("decodeResumeDownloadToken: Failed to unmarshal value[%s] path[%v]: err[%v]",
			string(value), path, err)
		return "", nil, err
	}

	return rdtId, rdt, nil
}

func setResumeDownloadTokenInMetakv(rdtId string, rdt *common.ResumeDownloadToken) {

	rhCb := func(r int, err error) error {
		if r > 0 {
			logging.Warnf("setResumeDownloadTokenInMetakv::rhCb: err[%v], Retrying[%d]", err, r)
		}

		return common.MetakvSet(PauseMetakvDir+rdtId, rdt)
	}

	rh := common.NewRetryHelper(10, time.Second, 1, rhCb)
	err := rh.Run()

	if err != nil {
		logging.Fatalf("setResumeDownloadTokenInMetakv: Failed to set ResumeDownloadToken In Meta Storage:"+
			" rdtId[%v] rdt[%v] err[%v]", rdtId, rdt, err)
		common.CrashOnError(err)
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Resumer class - Perform the Resume of a given bucket (similar to Rebalancer's role).
// This is used only on the master node of a task_RESUME task to do the GSI orchestration.
////////////////////////////////////////////////////////////////////////////////////////////////////

// Resumer object holds the state of Resume orchestration
type Resumer struct {
	// otherIndexAddrs is "host:port" to all the known Index Service nodes EXCLUDING this one
	otherIndexAddrs []string

	// pauseMgr is the singleton parent of this object
	pauseMgr *PauseServiceManager

	// task is the task_RESUME task we are executing (protected by task.taskMu). It lives in the
	// pauseMgr.tasks map (protected by pauseMgr.tasksMu). Only the current object should change or
	// delete task at this point, but GetTaskList and other processing may concurrently read it.
	// Thus Resumer needs to write lock task.taskMu for changes but does not need to read lock it.
	task *taskObj

	// Channels used for signalling
	// Used to signal that the ResumeDownloadTokens have been published.
	waitForTokenPublish chan struct{}
	// Used to signal metakv observer to stop
	metakvCancel chan struct{}

	metakvMutex sync.RWMutex
	wg          sync.WaitGroup

	// Global token associated with this Resume task
	pauseToken *PauseToken

	// in-memory bookkeeping for observed tokens
	masterTokens, followerTokens map[string]*common.ResumeDownloadToken

	// lock protecting access to maps like masterTokens and followerTokens
	mu sync.RWMutex

	// For cleanup
	retErr      error
	cleanupOnce sync.Once
}

// NewResumer creates a Resumer instance to execute the given task. It saves a pointer to itself in
// task.pauser (visible to pauseMgr parent) and launches a goroutine for the work.
//
//	pauseMgr - parent object (singleton)
//	task - the task_RESUME task this object will execute
//	master - true iff this node is the master
func NewResumer(pauseMgr *PauseServiceManager, task *taskObj, pauseToken *PauseToken) *Resumer {
	resumer := &Resumer{
		pauseMgr: pauseMgr,
		task:     task,

		waitForTokenPublish: make(chan struct{}),
		metakvCancel:        make(chan struct{}),
		pauseToken:          pauseToken,

		masterTokens:   make(map[string]*common.ResumeDownloadToken),
		followerTokens: make(map[string]*common.ResumeDownloadToken),
	}

	task.taskMu.Lock()
	task.resumer = resumer
	task.taskMu.Unlock()

	return resumer
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Methods
////////////////////////////////////////////////////////////////////////////////////////////////////

func (r *Resumer) startWorkers() {
	go r.observeResume()

	if r.task.isMaster() {
		go r.initResumeAsync()
	} else {
		// if not master, no need to wait for publishing of tokens
		close(r.waitForTokenPublish)
	}
}

func (r *Resumer) initResumeAsync() {

	// TODO: init progress update

	// TODO: Generate Tokens

	// Publish tokens to metaKV
	// will crash if cannot set in metaKV even after retries.
	r.publishResumeDownloadTokens(nil)

	// Ask observe to continue
	close(r.waitForTokenPublish)
}

func (r *Resumer) publishResumeDownloadTokens(rdts map[string]*common.ResumeDownloadToken) {
	for rdtId, rdt := range rdts {
		setResumeDownloadTokenInMetakv(rdtId, rdt)
		logging.Infof("Pauser::publishResumeDownloadTokens Published resume upload token: %v", rdtId)
	}
}

func (r *Resumer) observeResume() {
	logging.Infof("Resumer::observeResume pauseToken[%v] master[%v]", r.pauseToken, r.task.isMaster())

	<-r.waitForTokenPublish

	err := metakv.RunObserveChildren(PauseMetakvDir, r.processDownloadTokens, r.metakvCancel)
	if err != nil {
		logging.Errorf("Resumer::observeResume Exiting on metaKV observe: err[%v]", err)

		r.finishResume(err)
	}

	logging.Infof("Resumer::observeResume exiting: err[%v]", err)
}

// processDownloadTokens is metakv callback, not intended to be called otherwise
func (r *Resumer) processDownloadTokens(kve metakv.KVEntry) error {

	if kve.Path == buildMetakvPathForPauseToken(r.pauseToken.PauseId) {
		// Process PauseToken

		logging.Infof("Resumer::processDownloadTokens: PauseToken path[%v] value[%s]", kve.Path, kve.Value)

		if kve.Value == nil {
			logging.Infof("Resumer::processDownloadTokens: PauseToken Deleted. Mark Done.")
			r.cancelMetakv()
			r.finishResume(nil)
		}

	} else if strings.Contains(kve.Path, common.ResumeDownloadTokenPathPrefix) {
		// Process ResumeDownloadTokens

		if kve.Value != nil {
			rdtId, rdt, err := decodeResumeDownloadToken(kve.Path, kve.Value)
			if err != nil {
				logging.Errorf("Resumer::processDownloadTokens: Failed to decode ResumeDownloadToken. Ignored.")
				return nil
			}

			r.processResumeDownloadToken(rdtId, rdt)

		} else {
			logging.Infof("Resumer::processDownloadTokens: Received empty or deleted ResumeDownloadToken path[%v]",
				kve.Path)

		}
	}

	return nil
}

func (r *Resumer) cancelMetakv() {
	r.metakvMutex.Lock()
	defer r.metakvMutex.Unlock()

	if r.metakvCancel != nil {
		close(r.metakvCancel)
		r.metakvCancel = nil
	}
}

func (r *Resumer) processResumeDownloadToken(rdtId string, rdt *common.ResumeDownloadToken) {
	logging.Infof("Resumer::processResumeDownloadToken rdtId[%v] rdt[%v]", rdtId, rdt)
	if !r.addToWaitGroup() {
		logging.Errorf("Resumer::processResumeDownloadToken: Failed to add to resumer waitgroup.")
		return
	}

	defer r.wg.Done()

	// TODO: Check DDL running

	// "processed" var ensures only the incoming token state gets processed by this
	// call, as metakv will call parent processDownloadTokens again for each state change.
	var processed bool

	nodeUUID := string(r.pauseMgr.nodeInfo.NodeID)

	if rdt.MasterId == nodeUUID {
		processed = r.processResumeDownloadTokenAsMaster(rdtId, rdt)
	}

	if rdt.FollowerId == nodeUUID && !processed {
		r.processResumeDownloadTokenAsFollower(rdtId, rdt)
	}
}

func (r *Resumer) addToWaitGroup() bool {
	r.metakvMutex.Lock()
	defer r.metakvMutex.Unlock()

	if r.metakvCancel != nil {
		r.wg.Add(1)
		return true
	}
	return false
}

func (r *Resumer) processResumeDownloadTokenAsMaster(rdtId string, rdt *common.ResumeDownloadToken) bool {

	logging.Infof("Resumer::processResumeDownloadTokenAsMaster: rdtId[%v] rdt[%v]", rdtId, rdt)

	if rdt.ResumeId != r.task.taskId {
		logging.Warnf("Resumer::processResumeDownloadTokenAsMaster: Found ResumeDownloadToken[%v] with Unknown "+
			"ResumeId. Expected to match local taskId[%v]", rdt, r.task.taskId)

		return true
	}

	if rdt.Error != "" {
		logging.Errorf("Resumer::processResumeDownloadTokenAsMaster: Detected PauseUploadToken[%v] in Error state."+
			" Abort.", rdt)

		r.cancelMetakv()
		go r.finishResume(errors.New(rdt.Error))

		return true
	}

	if !r.checkValidNotifyState(rdtId, rdt, "master") {
		return true
	}

	switch rdt.State {

	case common.ResumeDownloadTokenPosted:
		// Follower owns token, do nothing

		return false

	case common.ResumeDownloadTokenInProgess:
		// Follower owns token, just mark in memory maps.

		r.updateInMemToken(rdtId, rdt, "master")
		return false

	case common.ResumeDownloadTokenProcessed:
		// Master owns token

		// Follower completed work, delete token
		err := common.MetakvDel(PauseMetakvDir + rdtId)
		if err != nil {
			logging.Fatalf("Resumer::processResumeDownloadTokenAsMaster: Failed to delete ResumeDownloadToken[%v] with"+
				" rdtId[%v] In Meta Storage: err[%v]", rdt, rdtId, err)
			common.CrashOnError(err)
		}

		r.updateInMemToken(rdtId, rdt, "master")

		if r.checkAllTokensDone() {
			// All the followers completed work

			// TODO: set progress 100%

			logging.Infof("Resumer::processResumeDownloadTokenAsMaster: No Tokens Found. Mark Done.")

			r.cancelMetakv()

			go r.finishResume(nil)
		}

		return true

	default:
		return false

	}

}

func (r *Resumer) finishResume(err error) {

	if r.retErr == nil {
		r.retErr = err
	}

	r.cleanupOnce.Do(r.doFinish)
}

func (r *Resumer) doFinish() {
	logging.Infof("Resumer::doFinish Cleanup: retErr[%v]", r.retErr)

	// TODO: signal others that we are cleaning up using done channel

	r.cancelMetakv()
	r.wg.Wait()

	// TODO: call done callback to start the cleanup phase
}

func (r *Resumer) processResumeDownloadTokenAsFollower(rdtId string, rdt *common.ResumeDownloadToken) bool {

	logging.Infof("Resumer::processResumeDownloadTokenAsFollower: rdtId[%v] rdt[%v]", rdtId, rdt)

	if rdt.ResumeId != r.task.taskId {
		logging.Warnf("Resumer::processResumeDownloadTokenAsFollower: Found ResumeDownloadToken[%v] with Unknown "+
			"PauseId. Expected to match local taskId[%v]", rdt, r.task.taskId)

		return true
	}

	if !r.checkValidNotifyState(rdtId, rdt, "follower") {
		return true
	}

	switch rdt.State {

	case common.ResumeDownloadTokenPosted:
		// Follower owns token, update in-memory and move to InProgress State

		r.updateInMemToken(rdtId, rdt, "follower")

		rdt.State = common.ResumeDownloadTokenInProgess
		setResumeDownloadTokenInMetakv(rdtId, rdt)

		return true

	case common.ResumeDownloadTokenInProgess:
		// Follower owns token, update in-memory and start pause work

		r.updateInMemToken(rdtId, rdt, "follower")

		go r.startResumeDownload(rdtId, rdt)

		return true

	case common.ResumeDownloadTokenProcessed:
		// Master owns token, just mark in memory maps

		r.updateInMemToken(rdtId, rdt, "follower")

		return false

	default:
		return false
	}
}

func (r *Resumer) startResumeDownload(rdtId string, rdt *common.ResumeDownloadToken) {
	start := time.Now()
	logging.Infof("Resumer::startResumeDownload: Begin work: rdtId[%v] rdt[%v]", rdtId, rdt)
	defer logging.Infof("Resumer::startResumeDownload: Done work: rdtId[%v] rdt[%v] took[%v]",
		rdtId, rdt, time.Since(start))

	if !r.addToWaitGroup() {
		logging.Errorf("Resumer::startResumeDownload: Failed to add to resumer waitgroup.")
		return
	}
	defer r.wg.Done()

	// TODO: Replace sleep with actual work
	time.Sleep(5 * time.Second)

	// work done, change state, master handler will pick it up and do cleanup.
	rdt.State = common.ResumeDownloadTokenProcessed
	setResumeDownloadTokenInMetakv(rdtId, rdt)
}

// Often, metaKV can send multiple notifications for the same state change (probably
// due to the eventual consistent nature of metaKV). Keep track of all state changes
// in in-memory bookkeeping and ignore the duplicate notifications
func (r *Resumer) checkValidNotifyState(rdtId string, rdt *common.ResumeDownloadToken, caller string) bool {

	// As the default state is "ResumeDownloadTokenPosted"
	// do not check for valid state changes for this state
	if rdt.State == common.ResumeDownloadTokenPosted {
		return true
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	var inMemToken *common.ResumeDownloadToken
	var ok bool

	if caller == "master" {
		inMemToken, ok = r.masterTokens[rdtId]
	} else if caller == "follower" {
		inMemToken, ok = r.followerTokens[rdtId]
	}

	if ok {
		// Token seen before, validate the state

		// < for invalid state change
		// == for duplicate notification
		if rdt.State <= inMemToken.State {
			logging.Warnf("Resumer::checkValidNotifyState Detected Invalid State Change Notification"+
				" for [%v]. rdtId[%v] Local[%v] Metakv[%v]", caller, rdtId, inMemToken.State, rdt.State)

			return false
		}
	}

	return true
}

func (r *Resumer) updateInMemToken(rdtId string, rdt *common.ResumeDownloadToken, caller string) {

	r.mu.Lock()
	defer r.mu.Unlock()

	if caller == "master" {
		r.masterTokens[rdtId] = rdt.Clone()
	} else if caller == "follower" {
		r.followerTokens[rdtId] = rdt.Clone()
	}
}

func (r *Resumer) checkAllTokensDone() bool {

	r.mu.Lock()
	defer r.mu.Unlock()

	for rdtId, rdt := range r.masterTokens {
		if rdt.State < common.ResumeDownloadTokenProcessed {
			// Either posted or processing

			logging.Infof("Resumer::checkAllTokensDone ResumeDownloadToken: rdtId[%v] is in state[%v]",
				rdtId, rdt.State)

			return false
		}
	}

	return true
}

// failResume logs an error using the caller's logPrefix and a provided context string and aborts
// the Resume task. If there is a set of known Indexer nodes, it will also try to notify them.
func (this *Resumer) failResume(logPrefix string, context string, error error) {
	logging.Errorf("%v Aborting Resume task %v due to %v error: %v", logPrefix,
		this.task.taskId, context, error)

	// Mark the task as failed directly here on master node (avoids dependency on loopback REST)
	this.task.TaskObjSetFailed(error.Error())

	// Notify other Index nodes
	// this.pauseMgr.RestNotifyFailedTask(this.otherIndexAddrs, this.task, error.Error())
}

// masterGenerateResumePlan: this method downloads all the metadata, stats from archivePath and
// plans which nodes resume indexes for given bucket
func (r *Resumer) masterGenerateResumePlan() (newNodes map[service.NodeID]service.NodeID, err error) {
	// Step 1: download PauseMetadata
	logging.Infof("Resumer::masterGenerateResumePlan: downloading pause metadata from %v for resume task ID: %v", r.task.archivePath, r.task.taskId)
	ctx := r.task.ctx
	plasmaCfg := plasma.DefaultConfig()

	copier := plasma.MakeFileCopier(r.task.archivePath, "", plasmaCfg.Environment, plasmaCfg.CopyConfig)
	if copier == nil {
		err = fmt.Errorf("object store not supported")
		logging.Errorf("Resumer::masterGenerateResumePlan: %v", err)
		return
	}

	data, err := copier.DownloadBytes(ctx, fmt.Sprintf("%v%v", r.task.archivePath, FILENAME_PAUSE_METADATA))
	if err != nil {
		logging.Errorf("Resumer::masterGenerateResumePlan: failed to download pause metadata err: %v for resume task ID: %v", err, r.task.taskId)
		return
	}
	data, err = common.ChecksumAndUncompress(data)
	if err != nil {
		logging.Errorf("Resumer::masterGenerateResumePlan: failed to read valid pause metadata err: %v for resume task ID: %v", err, r.task.taskId)
		return
	}
	pauseMetadata := new(PauseMetadata)
	err = json.Unmarshal(data, pauseMetadata)
	if err != nil {
		logging.Errorf("Resumer::masterGenerateResumePlan: couldn't unmarshal pause metadata err: %v for resume task ID: %v", err, r.task.taskId)
		return
	}

	// Step 2: Download metadata and stats for all nodes in pause metadata
	indexMetadataPerNode := make(map[service.NodeID]*planner.LocalIndexMetadata)
	statsPerNode := make(map[service.NodeID]map[string]interface{})

	var dWaiter sync.WaitGroup
	var dLock sync.Mutex
	dErrCh := make(chan error, len(pauseMetadata.Data))
	for nodeId := range pauseMetadata.Data {
		dWaiter.Add(1)
		go func(nodeId service.NodeID) {
			defer dWaiter.Done()

			nodeDir := fmt.Sprintf("%vnode_%v", r.task.archivePath, nodeId)
			indexMetadata, stats, err := r.downloadNodeMetadataAndStats(nodeDir)
			if err != nil {
				err = fmt.Errorf("couldn't get metadata and stats err: %v for nodeId %v, resume task ID: %v", err, nodeId, r.task.taskId)
				logging.Errorf("Resumer::masterGenerateResumePlan: %v", err)
				dErrCh <- err
				return
			}
			dLock.Lock()
			defer dLock.Unlock()
			indexMetadataPerNode[nodeId] = indexMetadata
			statsPerNode[nodeId] = stats
		}(nodeId)
	}
	dWaiter.Wait()
	close(dErrCh)

	var errStr strings.Builder
	for err := range dErrCh {
		errStr.WriteString(err.Error() + "\n")
	}
	if errStr.Len() != 0 {
		err = errors.New(errStr.String())
		return
	}

	// Step 3: get replacement node for old paused data
	resumeNodes := make([]*planner.IndexerNode, len(pauseMetadata.Data))
	config := r.pauseMgr.config.Load()
	clusterVersion := r.pauseMgr.genericMgr.cinfo.GetClusterVersion()
	// since we don't support mixed mode for pause resume, we can use the current server version
	// as the indexer version
	indexerVersion, err := r.pauseMgr.genericMgr.cinfo.GetServerVersion(
		r.pauseMgr.genericMgr.cinfo.GetCurrentNode(),
	)
	if err != nil {
		// we should never hit this err condition as we should always be able to read current node's
		// server version. if we do, should we fail here?
		logging.Warnf("Resumer::masterGenerateResumePlan: couldn't fetch this node's server version. hit unreachable error. err: %v for taskId %v", err, r.task.taskId)
		err = nil
		// use min indexer version required for Pause-Resume
		indexerVersion = common.INDEXER_72_VERSION
	}

	for nodeId := range pauseMetadata.Data {
		// Step 3a: generate IndexerNode for planner

		idxMetadata, ok := indexMetadataPerNode[nodeId]
		if !ok {
			err = fmt.Errorf("unable to read indexMetadata for node %v", nodeId)
			logging.Errorf("Resumer::masterGenerateResumePlan: %v", err)
			return
		}
		statsPerNode, ok := statsPerNode[nodeId]
		if !ok {
			err = fmt.Errorf("unable to read stats for node %v", nodeId)
			logging.Errorf("Resumer::masterGenerateResumePlan: %v", err)
			return
		}

		// TODO: replace with planner calls to generate []IndexerNode with []IndexUsage
		logging.Infof("Resumer::masterGenerateResumePlan: metadata and stats from node %v available. Total Idx definitions: %v, Total stats: %v for task ID: %v", nodeId, len(idxMetadata.IndexDefinitions), len(statsPerNode))

		indexerNode := planner.CreateIndexerNodeWithIndexes(string(nodeId), nil, nil)

		// Step 3b: populate IndexerNode with metadata
		var indexerUsage []*planner.IndexUsage
		indexerUsage, err = planner.ConvertToIndexUsages(config, idxMetadata, indexerNode, nil,
			nil)
		if err != nil {
			logging.Errorf("Resumer::masterGenerateResumePlan: couldn't generate index usage. err: %v for task ID: %v", err, r.task.taskId)
			return
		}

		indexerNode.Indexes = indexerUsage

		planner.SetStatsInIndexer(indexerNode, statsPerNode, clusterVersion, indexerVersion, config)

		resumeNodes = append(resumeNodes, indexerNode)
	}
	newNodes, err = StubExecuteTenantAwarePlanForResume(config["clusterAddr"].String(), resumeNodes)

	return
}

func (r *Resumer) downloadNodeMetadataAndStats(nodeDir string) (metadata *planner.LocalIndexMetadata, stats map[string]interface{}, err error) {
	defer func() {
		if err == nil {
			logging.Infof("Resumer::downloadNodeMetadataAndStats: successfully downloaded metadata and stats from %v", nodeDir)
		}
	}()

	ctx := r.task.ctx
	plasmaCfg := plasma.DefaultConfig()

	copier := plasma.MakeFileCopier(nodeDir, "", plasmaCfg.Environment, plasmaCfg.CopyConfig)
	if copier == nil {
		err = fmt.Errorf("object store not supported")
		logging.Errorf("Resumer::downloadNodeMetadataAndStats: %v", err)
		return
	}

	url, err := copier.GetPathEncoding(fmt.Sprintf("%v%v", nodeDir, FILENAME_METADATA))
	if err != nil {
		logging.Errorf("Resumer::downloadNodeMetadataAndStats: url encoding failed %v", err)
		return
	}
	data, err := copier.DownloadBytes(ctx, url)
	if err != nil {
		logging.Errorf("Resumer::downloadNodeMetadataAndStats: failed to download metadata err: %v", err)
		return
	}
	data, err = common.ChecksumAndUncompress(data)
	if err != nil {
		logging.Errorf("Resumer::downloadNodeMetadataAndStats: invalid metadata in object store err :%v", err)
		return
	}

	// NOTE: pause uploads manager.LocalIndexMetadata which has the similar fields as planner.LocalIndexMetadata
	mgrMetadata := new(manager.LocalIndexMetadata)
	err = json.Unmarshal(data, mgrMetadata)
	if err != nil {
		logging.Errorf("Resumer::downloadnodeMetadataAndStats: failed to unmarshal index metadata err: %v", err)
		return
	}
	metadata = manager.TransformMetaToPlannerMeta(mgrMetadata)

	url, err = copier.GetPathEncoding(fmt.Sprintf("%v%v", nodeDir, FILENAME_STATS))
	if err != nil {
		logging.Errorf("Resumer::downloadNodeMetadataAndStats: url encoding failed %v", err)
		return
	}
	data, err = copier.DownloadBytes(ctx, url)
	if err != nil {
		logging.Errorf("Resumer::downloadNodeMetadataAndStats: failed to download stats err: %v", err)
		return
	}
	data, err = common.ChecksumAndUncompress(data)
	if err != nil {
		logging.Errorf("Resumer::downloadNodeMetadataAndStats: invalid stats in object store err :%v", err)
		return
	}
	err = json.Unmarshal(data, &stats)
	if err != nil {
		logging.Errorf("Resumer::downloadnodeMetadataAndStats: failed to unmarshal stats err: %v", err)
	}

	return
}

func StubExecuteTenantAwarePlanForResume(clusterUrl string, resumeNodes []*planner.IndexerNode) (map[service.NodeID]service.NodeID, error) {
	logging.Infof("Resumer::StubExecuteTenantAwarePlanForResume: TODO: call actual planner")
	return nil, nil
}
