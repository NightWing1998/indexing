// @copyright 2016-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.
package indexer

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/indexing/secondary/common"
	c "github.com/couchbase/indexing/secondary/common"
	forestdb "github.com/couchbase/indexing/secondary/fdb"
	"github.com/couchbase/indexing/secondary/logging"
	l "github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/manager"
	"github.com/couchbase/indexing/secondary/manager/client"
	mc "github.com/couchbase/indexing/secondary/manager/common"
	"github.com/couchbase/indexing/secondary/planner"
)

type DoneCallback func(err error, cancel <-chan struct{})
type ProgressCallback func(progress float64, cancel <-chan struct{})

type Callbacks struct {
	progress ProgressCallback
	done     DoneCallback
}

type Rebalancer struct {
	// Transfer token maps from ttid to token
	transferTokens map[string]*c.TransferToken // all TTs for this rebalance
	acceptedTokens map[string]*c.TransferToken // accepted TTs
	sourceTokens   map[string]*c.TransferToken // TTs for which a source index exists (move case)
	mu             sync.RWMutex                // for transferTokens, acceptedTokens, sourceTokens, currBatchTokens

	currBatchTokens      []string   // ttids of all TTs in current batch
	transferTokenBatches [][]string // slice of all TT batches (2nd dimension is ttid)

	drop         map[string]bool
	pendingBuild int32
	dropQueue    chan string

	rebalToken *RebalanceToken
	nodeId     string
	master     bool

	cb Callbacks

	cancel         chan struct{} // closed to signal rebalance canceled
	done           chan struct{} // closed to signal rebalance done (not canceled)
	dropIndexDone  chan struct{} // closed to signal rebalance is done with dropping duplicate indexes
	removeDupIndex bool          // duplicate index removal is called

	isDone int32

	metakvCancel chan struct{} // close this to end metakv.RunObserveChildren callbacks
	muCleanup    sync.RWMutex  // for metakvCancel

	supvMsgch MsgChannel

	localaddr string

	wg sync.WaitGroup

	cleanupOnce sync.Once

	waitForTokenPublish chan struct{}

	retErr error

	config c.ConfigHolder

	lastKnownProgress map[c.IndexInstId]float64

	change     *service.TopologyChange
	runPlanner bool // should this rebalance run the planner?

	runParam *runParams
}

// NewRebalancer creates the Rebalancer object that will master a rebalance and starts
// go routines that perform it asynchronously. If this is a worker node it launches
// an observeRebalance go routine that does the token processing for this node. If this
// is the master that will be launched later by initRebalAsync --> doRebalance.
func NewRebalancer(transferTokens map[string]*c.TransferToken, rebalToken *RebalanceToken,
	nodeId string, master bool, progress ProgressCallback, done DoneCallback,
	supvMsgch MsgChannel, localaddr string, config c.Config, change *service.TopologyChange,
	runPlanner bool, runParam *runParams) *Rebalancer {

	l.Infof("NewRebalancer nodeId %v rebalToken %v master %v localaddr %v runPlanner %v runParam %v", nodeId,
		rebalToken, master, localaddr, runPlanner, runParam)

	r := &Rebalancer{
		transferTokens: transferTokens,
		rebalToken:     rebalToken,
		master:         master,
		nodeId:         nodeId,

		cb: Callbacks{progress, done},

		cancel:         make(chan struct{}),
		done:           make(chan struct{}),
		dropIndexDone:  make(chan struct{}),
		removeDupIndex: false,

		metakvCancel: make(chan struct{}),

		supvMsgch: supvMsgch,

		acceptedTokens: make(map[string]*c.TransferToken),
		sourceTokens:   make(map[string]*c.TransferToken),
		drop:           make(map[string]bool),
		dropQueue:      make(chan string, 10000),
		localaddr:      localaddr,

		waitForTokenPublish: make(chan struct{}),
		lastKnownProgress:   make(map[c.IndexInstId]float64),

		change:     change,
		runPlanner: runPlanner,

		transferTokenBatches: make([][]string, 0),

		runParam: runParam,
	}

	r.config.Store(config)

	if master {
		go r.initRebalAsync()
	} else {
		close(r.waitForTokenPublish)
		go r.observeRebalance()
	}

	go r.processDropIndexQueue()

	return r
}

// RemoveDuplicateIndexes function gets input of hosts and index definitions to be removed from hosts,
// these are indexes that have been detected as duplicate and are being removed as part of rebalance.
// The method itself deos not do anything to identfy or detect duplicates and only drops the input set of index
// that it recieves but the name has been given as removeDuplicateIndexes to identify its purpose
// which is different than other dropIndex cleanup that rebalance process does.
// Also if there is an error while dropping one or more indexes it simply logs the error but does not fail the rebalance.
func (r *Rebalancer) RemoveDuplicateIndexes(hostIndexMap map[string]map[common.IndexDefnId]*common.IndexDefn) {
	defer close(r.dropIndexDone)
	var mu sync.Mutex
	var wg sync.WaitGroup
	const method string = "Rebalancer::RemoveDuplicateIndexes:"
	errMap := make(map[string]map[common.IndexDefnId]error)

	uniqueDefns := make(map[common.IndexDefnId]bool)
	for _, indexes := range hostIndexMap {
		for _, index := range indexes {
			// true is set if we are not able to post dropToken, in which case index is not removed
			// initially all indexes to be dropped.
			uniqueDefns[index.DefnId] = false
		}
	}

	// Place delete token for recovery and any corner cases.
	// We place the drop tokens for every identified index except for the case of rebalance being canceled/done
	// If we fail to place drop token even after 3 retries we will not drop that index to avoid any errors in dropIndex
	// causing metadata consistency and cleanup problems
	for defnId, _ := range uniqueDefns {
	loop:
		for i := 0; i < 3; i++ { // 3 retries in case of error on PostDeleteCommandToken
			select {
			case <-r.cancel:
				l.Warnf("%v Cancel Received. Skip processing drop duplicate indexes.", method)
				return
			case <-r.done:
				l.Warnf("%v Cannot drop duplicate index when rebalance is done.", method)
				return
			default:
				l.Infof("%v posting dropToken for defnid %v", method, defnId)
				if err := mc.PostDeleteCommandToken(defnId, true); err != nil {
					if i == 2 { // all retries have failed to post drop token
						uniqueDefns[defnId] = true
						l.Errorf("%v failed to post delete command token after 3 retries, for index defnId %v due to internal errors.  Error=%v.", method, defnId, err)
					} else {
						l.Warnf("%v failed to post delete command token for index defnId %v due to internal errors.  Error=%v.", method, defnId, err)
					}
					break // break select and retry PostDeleteCommandToken
				}
				break loop // break retry loop and move to next defn
			}
		}
	}

	removeIndexes := func(host string, indexes map[common.IndexDefnId]*common.IndexDefn) {
		defer wg.Done()

		for _, index := range indexes {
			select {
			case <-r.cancel:
				l.Warnf("%v Cancel Received. Skip processing drop duplicate indexes.", method)
				return
			case <-r.done:
				l.Warnf("%v Cannot drop duplicate index when rebalance is done.", method)
				return
			default:
				if uniqueDefns[index.DefnId] == true { // we were not able to post dropToken for this index.
					break // break select move to next index
				}
				if err := r.makeDropIndexRequest(index, host); err != nil {
					// we will only record the error and proceeded, not failing rebalance just because we could not delete
					// a index.
					mu.Lock()
					if _, ok := errMap[host]; !ok {
						errMap[host] = make(map[common.IndexDefnId]error)
					}
					errMap[host][index.DefnId] = err
					mu.Unlock()
				}
			}
		}
	}

	select {
	case <-r.cancel:
		l.Warnf("%v Cancel Received. Skip processing drop duplicate indexes.", method)
		return
	case <-r.done:
		l.Warnf("%v Cannot drop duplicate index when rebalance is done.", method)
		return
	default:
		for host, indexes := range hostIndexMap {
			wg.Add(1)
			go removeIndexes(host, indexes)
		}

		wg.Wait()

		for host, indexes := range errMap {
			for defnId, err := range indexes { // not really an error as we already posted drop tokens
				l.Warnf("%v encountered error while removing index on host %v, defnId %v, err %v", method, host, defnId, err)
			}
		}
	}
}

func (r *Rebalancer) makeDropIndexRequest(defn *common.IndexDefn, host string) error {
	const method string = "Rebalancer::makeDropIndexRequest" // for logging
	req := manager.IndexRequest{Index: *defn}
	body, err := json.Marshal(&req)
	if err != nil {
		l.Errorf("%v error in marshal drop index defnId %v err %v", method, defn.DefnId, err)
		return err
	}

	bodybuf := bytes.NewBuffer(body)

	url := "/dropIndex"
	resp, err := postWithAuth(host+url, "application/json", bodybuf)
	if err != nil {
		l.Errorf("%v error in drop index on host %v, defnId %v, err %v", method, host, defn.DefnId, err)
		if err == io.EOF {
			// should not rety in case of rebalance done or cancel
			select {
			case <-r.cancel:
				return err // return original error
			case <-r.done:
				return err
			default:
				bodybuf := bytes.NewBuffer(body)
				resp, err = postWithAuth(host+url, "application/json", bodybuf)
				if err != nil {
					l.Errorf("%v error in drop index on host %v, defnId %v, err %v", method, host, defn.DefnId, err)
					return err
				}
			}
		} else {
			return err
		}
	}

	response := new(manager.IndexResponse)
	err = convertResponse(resp, response)
	if err != nil {
		l.Errorf("%v encountered error parsing response, host %v, defnId %v, err %v", method, host, defn.DefnId, err)
		return err
	}

	if response.Code == manager.RESP_ERROR {
		if strings.Contains(response.Error, forestdb.FDB_RESULT_KEY_NOT_FOUND.Error()) {
			l.Errorf("%v error dropping index, host %v defnId %v err %v. Ignored.", method, host, defn.DefnId, response.Error)
			return nil
		}
		l.Errorf("%v error dropping index, host %v, defnId %v, err %v", method, host, defn.DefnId, response.Error)
		return err
	}
	l.Infof("%v removed index defnId %v, defn %v, from host %v", method, defn.DefnId, defn, host)
	return nil
}

// initRebalAsync runs as a helper go routine for NewRebalancer on the rebalance
// master. It calls the planner if needed, then launches a separate go routine,
// doRebalance, to manage the fine-grained steps of the rebalance.
func (r *Rebalancer) initRebalAsync() {

	var hostToIndexToRemove map[string]map[common.IndexDefnId]*common.IndexDefn
	//short circuit
	if len(r.transferTokens) == 0 && !r.runPlanner {
		r.cb.progress(1.0, r.cancel)
		r.finish(nil)
		return
	}

	// Launch the progress updater goroutine
	if r.cb.progress != nil {
		go r.updateProgress()
	}

	if r.runPlanner {
		cfg := r.config.Load()
	loop:
		for {
			select {
			case <-r.cancel:
				l.Infof("Rebalancer::initRebalAsync Cancel Received")
				return

			case <-r.done:
				l.Infof("Rebalancer::initRebalAsync Done Received")
				return

			default:
				allWarmedup, _ := checkAllIndexersWarmedup(cfg["clusterAddr"].String())

				if allWarmedup {

					topology, err := getGlobalTopology(r.localaddr)
					if err != nil {
						l.Errorf("Rebalancer::initRebalAsync Error Fetching Topology %v", err)
						go r.finish(err)
						return
					}

					l.Infof("Rebalancer::initRebalAsync Global Topology %v", topology)

					onEjectOnly := cfg["rebalance.node_eject_only"].Bool()
					optimizePlacement := cfg["settings.rebalance.redistribute_indexes"].Bool()
					disableReplicaRepair := cfg["rebalance.disable_replica_repair"].Bool()
					timeout := cfg["planner.timeout"].Int()
					threshold := cfg["planner.variationThreshold"].Float64()
					cpuProfile := cfg["planner.cpuProfile"].Bool()
					minIterPerTemp := cfg["planner.internal.minIterPerTemp"].Int()
					maxIterPerTemp := cfg["planner.internal.maxIterPerTemp"].Int()

					//user setting redistribute_indexes overrides the internal setting
					//onEjectOnly. onEjectOnly is not expected to be used in production
					//as this is not documented.
					if optimizePlacement {
						onEjectOnly = false
					} else {
						onEjectOnly = true
					}

					start := time.Now()
					r.transferTokens, hostToIndexToRemove, err = planner.ExecuteRebalance(cfg["clusterAddr"].String(), *r.change,
						r.nodeId, onEjectOnly, disableReplicaRepair, threshold, timeout, cpuProfile,
						minIterPerTemp, maxIterPerTemp)
					if err != nil {
						l.Errorf("Rebalancer::initRebalAsync Planner Error %v", err)
						go r.finish(err)
						return
					}
					if len(hostToIndexToRemove) > 0 {
						r.removeDupIndex = true
						go r.RemoveDuplicateIndexes(hostToIndexToRemove)
					}
					if len(r.transferTokens) == 0 {
						r.transferTokens = nil
					}

					elapsed := time.Since(start)
					l.Infof("Rebalancer::initRebalAsync Planner Time Taken %v", elapsed)
					break loop
				}
			}
			l.Errorf("Rebalancer::initRebalAsync All Indexers Not Active. Waiting...")
			time.Sleep(5 * time.Second)
		}
	}

	go r.doRebalance()
}

// Cancel cancels a currently running rebalance or failover and waits
// for its go routines to finish.
func (r *Rebalancer) Cancel() {
	l.Infof("Rebalancer::Cancel Exiting")

	r.cancelMetakv()

	close(r.cancel)
	r.wg.Wait()
}

func (r *Rebalancer) finish(err error) {

	if err == nil && r.master && r.change != nil {
		// Note that this function tansfers the ownership of only those
		// tokens, which are not owned by keep nodes. Ownership of other
		// tokens remains unchanged.

		keepNodes := make(map[string]bool)
		for _, node := range r.change.KeepNodes {
			keepNodes[string(node.NodeInfo.NodeID)] = true
		}

		cfg := r.config.Load()

		// Suppress error if any. The background task to handle failover
		// will do necessary retry for transferring ownership.
		_ = transferScheduleTokens(keepNodes, cfg["clusterAddr"].String())
	}

	r.retErr = err
	r.cleanupOnce.Do(r.doFinish)

}

func (r *Rebalancer) doFinish() {

	l.Infof("Rebalancer::doFinish Cleanup %v", r.retErr)

	atomic.StoreInt32(&r.isDone, 1)
	close(r.done)
	r.cancelMetakv()

	r.wg.Wait()
	r.cb.done(r.retErr, r.cancel)

}

func (r *Rebalancer) isFinish() bool {
	return atomic.LoadInt32(&r.isDone) == 1
}

// cancelMetakv closes the metakvCancel channel, thus terminating metakv's
// transfer token callback loop that was initiated by metakv.RunObserveChildren.
// Must be done when rebalance finishes (successful or not).
func (r *Rebalancer) cancelMetakv() {

	r.muCleanup.Lock()
	defer r.muCleanup.Unlock()

	if r.metakvCancel != nil {
		close(r.metakvCancel)
		r.metakvCancel = nil
	}

}

// addToWaitGroup adds caller to r.wg waitgroup and returns true if metakv.RunObserveChildren
// callbacks are active, else it does nothing and returns false.
func (r *Rebalancer) addToWaitGroup() bool {

	r.muCleanup.Lock()
	defer r.muCleanup.Unlock()

	if r.metakvCancel != nil {
		r.wg.Add(1)
		return true
	} else {
		return false
	}

}

// doRebalance runs as a go routine helper to initRebalanceAsync that manages
// the fine-grained steps of a rebalance. It creates the transfer token batches
// and publishes the first one. (If more than one batch exists the others will
// be published later by processTokenAsMaster.) Then it launches an
// observeRebalance go routine that does the real rebalance work for the master.
func (r *Rebalancer) doRebalance() {

	if r.master && r.runPlanner && r.removeDupIndex {
		<-r.dropIndexDone // wait for duplicate index removal to finish
	}
	if r.transferTokens != nil {

		if ddl, err := r.checkDDLRunning(); ddl {
			r.finish(err)
			return
		}

		select {
		case <-r.cancel:
			l.Infof("Rebalancer::doRebalance Cancel Received. Skip Publishing Tokens.")
			return

		default:
			r.createTransferBatches()
			r.publishTransferTokenBatch()
			close(r.waitForTokenPublish)
			go r.observeRebalance()
		}
	} else {
		r.cb.progress(1.0, r.cancel)
		r.finish(nil)
		return
	}

}

// createTransferBatches is a helper for doRebalance (master node only) that creates
// all the transfer token batches.
func (r *Rebalancer) createTransferBatches() {

	cfg := r.config.Load()
	batchSize := cfg["rebalance.transferBatchSize"].Int()

	var batch []string
	for ttid, _ := range r.transferTokens {
		if batch == nil {
			batch = make([]string, 0, batchSize)
		}
		batch = append(batch, ttid)

		if len(batch) == batchSize {
			r.transferTokenBatches = append(r.transferTokenBatches, batch)
			batch = nil
		}
	}

	if len(batch) != 0 {
		r.transferTokenBatches = append(r.transferTokenBatches, batch)
	}

	l.Infof("Rebalancer::createTransferBatches Transfer Batches %v", r.transferTokenBatches)
}

// publishTransferTokenBatch publishes the next batch of transfer tokens into
// metakv so they will start being processed. For batches other than the first,
// it is only called once the prior batch completes.
func (r *Rebalancer) publishTransferTokenBatch() {

	r.mu.Lock()
	defer r.mu.Unlock()

	r.currBatchTokens = r.transferTokenBatches[0]
	r.transferTokenBatches = r.transferTokenBatches[1:]

	l.Infof("Rebalancer::publishTransferTokenBatch Registered Transfer Token In Metakv %v", r.currBatchTokens)

	for _, ttid := range r.currBatchTokens {
		setTransferTokenInMetakv(ttid, r.transferTokens[ttid])
	}

}

// observeRebalance runs as a go routine on both master and worker nodes of a rebalance.
// It registers the processTokens function as a callback in metakv on the rebalance
// transfer token (TT) directory. Metakv will call the callback function on each TT and
// on each mutation of a TT until an error occurs or its stop channel is closed. These
// callbacks trigger the individual index movement steps of the rebalance.
func (r *Rebalancer) observeRebalance() {

	l.Infof("Rebalancer::observeRebalance %v master:%v", r.rebalToken, r.master)

	<-r.waitForTokenPublish

	err := metakv.RunObserveChildren(RebalanceMetakvDir, r.processTokens, r.metakvCancel)
	if err != nil {
		l.Infof("Rebalancer::observeRebalance Exiting On Metakv Error %v", err)
		r.finish(err)
	}

	l.Infof("Rebalancer::observeRebalance exiting err %v", err)

}

// processTokens is the callback registered on the metakv transfer token directory for
// a rebalance and gets called by metakv for each token and each change to a token.
// This decodes the token and hands it off to processTransferToken.
func (r *Rebalancer) processTokens(path string, value []byte, rev interface{}) error {

	if path == RebalanceTokenPath || path == MoveIndexTokenPath {
		l.Infof("Rebalancer::processTokens RebalanceToken %v %v", path, value)

		if value == nil {
			l.Infof("Rebalancer::processTokens Rebalance Token Deleted. Mark Done.")
			r.cancelMetakv()
			r.finish(nil)
		}
	} else if strings.Contains(path, TransferTokenTag) {
		if value != nil {
			ttid, tt, err := r.decodeTransferToken(path, value)
			if err != nil {
				l.Errorf("Rebalancer::processTokens Unable to decode transfer token. Ignored")
				return nil
			}
			r.processTransferToken(ttid, tt)
		} else {
			l.Infof("Rebalancer::processTokens Received empty or deleted transfer token %v", path)
		}
	}

	return nil

}

// processTransferTokens performs the work needed from this node, if any, for a
// transfer token. The work is split into three helpers for work specific to
// 1) the master (non-movement bookkeeping, including publishing the next token
// batch when the prior one completes), 2) source of an index move, 3) destination
// of a move.
func (r *Rebalancer) processTransferToken(ttid string, tt *c.TransferToken) {

	if !r.addToWaitGroup() {
		return
	}

	defer r.wg.Done()

	if ddl, err := r.checkDDLRunning(); ddl {
		r.setTransferTokenError(ttid, tt, err.Error())
		return
	}

	// "processed" var ensures only the incoming token state gets processed by this
	// call, as metakv will call parent processTokens again for each TT state change.
	var processed bool
	if tt.MasterId == r.nodeId {
		processed = r.processTokenAsMaster(ttid, tt)
	}

	if tt.SourceId == r.nodeId && !processed {
		processed = r.processTokenAsSource(ttid, tt)
	}

	if tt.DestId == r.nodeId && !processed {
		processed = r.processTokenAsDest(ttid, tt)
	}
}

// processTokenAsSource performs the work of the source node of an index move
// reflected by the transfer token, which is to queue up the source index drops
// for later processing. It handles only TT state TransferTokenReady. Returns
// true iff it is considered to have processed this token (including handling
// some error cases).
func (r *Rebalancer) processTokenAsSource(ttid string, tt *c.TransferToken) bool {

	if tt.RebalId != r.rebalToken.RebalId {
		l.Warnf("Rebalancer::processTokenAsSource Found TransferToken with Unknown "+
			"RebalanceId. Local RId %v Token %v. Ignored.", r.rebalToken.RebalId, tt)
		return true
	}

	switch tt.State {

	case c.TransferTokenReady:
		if !r.checkValidNotifyStateSource(ttid, tt) {
			return true
		}

		r.mu.Lock()
		defer r.mu.Unlock()
		r.sourceTokens[ttid] = tt
		logging.Infof("Rebalancer::processTokenAsSource Processing transfer token: %v", tt)

		//TODO batch this rather than one per index
		r.queueDropIndex(ttid)

	default:
		return false
	}
	return true

}

func (r *Rebalancer) processDropIndexQueue() {

	notifych := make(chan bool, 2)

	first := true
	cfg := r.config.Load()
	waitTime := cfg["rebalance.drop_index.wait_time"].Int()

	for {
		select {
		case <-r.cancel:
			l.Infof("Rebalancer::processDropIndexQueue Cancel Received")
			return
		case <-r.done:
			l.Infof("Rebalancer::processDropIndexQueue Done Received")
			return
		case ttid := <-r.dropQueue:
			var tt c.TransferToken
			logging.Infof("Rebalancer::processDropIndexQueue processing drop index request for ttid: %v", ttid)
			if first {
				// If it is the first drop, let wait to give a chance for the target's metaadta
				// being synchronized with the cbq nodes.  This is to ensure that the cbq nodes
				// can direct scan to the target nodes, before we start dropping the index in the source.
				time.Sleep(time.Duration(waitTime) * time.Second)
				first = false
			}

			r.mu.Lock()
			tt1, ok := r.sourceTokens[ttid]
			if ok {
				tt = *tt1
			}
			r.mu.Unlock()

			if !ok {
				l.Warnf("Rebalancer::processDropIndexQueue: Cannot find token %v in r.sourceTokens. Skip drop index.", ttid)
				continue
			}

			if tt.State == c.TransferTokenReady {
				if !r.drop[ttid] {
					if r.addToWaitGroup() {
						notifych <- true
						go r.dropIndexWhenIdle(ttid, &tt, notifych)
					} else {
						logging.Warnf("Rebalancer::processDropIndexQueue Skip processing drop index request for tt: %v as rebalancer can not add to wait group", tt)
					}
				} else {
					logging.Infof("Rebalancer::processDropIndexQueue: Skip processing tt: %v as it is already added to drop list: %v", tt, r.drop)
				}
			} else {
				logging.Warnf("Rebalancer::processDropIndexQueue Skipping drop index request for tt: %v", tt)
			}
		}
	}
}

//
// Must hold lock when calling this function
//
func (r *Rebalancer) queueDropIndex(ttid string) {

	select {
	case <-r.cancel:
		l.Warnf("Rebalancer::queueDropIndex: Cannot drop index when rebalance being cancel.")
		return

	case <-r.done:
		l.Warnf("Rebalancer::queueDropIndex: Cannot drop index when rebalance is done.")
		return

	default:
		if r.checkIndexReadyToDrop() {
			select {
			case r.dropQueue <- ttid:
				logging.Infof("Rebalancer::queueDropIndex Successfully queued index for drop, ttid: %v", ttid)
			default:
				tt := r.sourceTokens[ttid]
				if tt.State == c.TransferTokenReady {
					if !r.drop[ttid] {
						if r.addToWaitGroup() {
							go r.dropIndexWhenIdle(ttid, tt, nil)
						} else {
							logging.Warnf("Rebalancer::queueDropIndex Could not add to wait group, hence not attempting drop, ttid: %v", ttid)
						}
					} else {
						logging.Infof("Rebalancer::queueDropIndex Did not queue drop index as index is already in drop list, ttid: %v, drop list: %v", ttid, r.drop)
					}
				} else {
					logging.Infof("Rebalancer::queueDropIndex Did not queue index for drop as tt state is: %v, ttid: %v", tt.State, ttid)
				}
			}
		} else {
			logging.Warnf("Rebalancer::queueDropIndex Failed to queue index for drop, ttid: %v", ttid)
		}
	}
}

//
// Must hold lock when calling this function
//
func (r *Rebalancer) dropIndexWhenReady() {

	if r.checkIndexReadyToDrop() {
		for ttid, tt := range r.sourceTokens {
			if tt.State == c.TransferTokenReady {
				if !r.drop[ttid] {
					r.queueDropIndex(ttid)
				}
			}
		}
	}
}

// isMissingBSC determines whether an error message is due to a bucket, scope,
// or collection not existing. These can be dropped after a TT referencing them
// was created, in which case we will set the TT forward to TransferTokenCommit
// state to abort the index move without failing the rebalance. In some cases
// the target error string will be contained inside brackets within a more
// detailed user-visible errMsg.
func isMissingBSC(errMsg string) bool {
	return errMsg == common.ErrCollectionNotFound.Error() ||
		errMsg == common.ErrScopeNotFound.Error() ||
		errMsg == common.ErrBucketNotFound.Error() ||
		strings.Contains(errMsg, "["+common.ErrCollectionNotFound.Error()+"]") ||
		strings.Contains(errMsg, "["+common.ErrScopeNotFound.Error()+"]") ||
		strings.Contains(errMsg, "["+common.ErrBucketNotFound.Error()+"]")
}

// isIndexNotFoundRebal checks whether a build error returned for rebalance is the
// special error ErrIndexNotFoundRebal indicating the key returned for this error
// is the originally submitted defnId instead of an instId because the metadata
// for defnId could not be found. This error should only be used for this purpose.
func isIndexNotFoundRebal(errMsg string) bool {
	return errMsg == common.ErrIndexNotFoundRebal.Error()
}

// dropIndexWhenIdle performs the source index drop asynchronously. This is the last real
// action of an index move during rebalance; the remainder are token bookkeeping operations.
// Changes the TT state to TransferTokenCommit when the drop is completed.
func (r *Rebalancer) dropIndexWhenIdle(ttid string, tt *c.TransferToken, notifych chan bool) {
	defer r.wg.Done()
	defer func() {
		if notifych != nil {
			// Blocking wait to ensure indexes are dropped sequentially
			<-notifych
		}
	}()

	r.mu.Lock()
	if r.drop[ttid] {
		r.mu.Unlock()
		return
	}
	r.drop[ttid] = true
	r.mu.Unlock()

	missingStatRetry := 0
loop:
	for {
	labelselect:
		select {
		case <-r.cancel:
			l.Infof("Rebalancer::dropIndexWhenIdle Cancel Received")
			break loop
		case <-r.done:
			l.Infof("Rebalancer::dropIndexWhenIdle Done Received")
			break loop
		default:

			stats, err := getLocalStats(r.localaddr, true)
			if err != nil {
				l.Errorf("Rebalancer::dropIndexWhenIdle Error Fetching Local Stats %v %v", r.localaddr, err)
				break
			}

			statsMap := stats.ToMap()
			if statsMap == nil {
				l.Infof("Rebalancer::dropIndexWhenIdle Nil Stats. Retrying...")
				break
			}

			tt.IndexInst.Defn.SetCollectionDefaults()

			pending := float64(0)
			for _, partitionId := range tt.IndexInst.Defn.Partitions {

				defn := tt.IndexInst.Defn

				prefix := common.GetStatsPrefix(defn.Bucket, defn.Scope, defn.Collection,
					defn.Name, tt.IndexInst.ReplicaId, int(partitionId), true)

				sname_completed := common.GetIndexStatKey(prefix, "num_completed_requests")
				sname_requests := common.GetIndexStatKey(prefix, "num_requests")

				var num_completed, num_requests float64
				if _, ok := statsMap[sname_completed]; ok {
					num_completed = statsMap[sname_completed].(float64)
					num_requests = statsMap[sname_requests].(float64)
				} else {
					l.Infof("Rebalancer::dropIndexWhenIdle Missing Stats %v %v. Retrying...", sname_completed, sname_requests)
					missingStatRetry++
					if missingStatRetry > 10 {
						if r.needRetryForDrop(ttid, tt) {
							break labelselect
						} else {
							break loop
						}
					}
					break labelselect
				}
				pending += num_requests - num_completed
			}

			if pending > 0 {
				l.Infof("Rebalancer::dropIndexWhenIdle Index %v:%v:%v:%v Pending Scan %v", tt.IndexInst.Defn.Bucket, tt.IndexInst.Defn.Scope, tt.IndexInst.Defn.Collection, tt.IndexInst.Defn.Name, pending)
				break
			}

			defn := tt.IndexInst.Defn
			defn.InstId = tt.InstId
			defn.RealInstId = tt.RealInstId
			req := manager.IndexRequest{Index: defn}
			body, err := json.Marshal(&req)
			if err != nil {
				l.Errorf("Rebalancer::dropIndexWhenIdle Error marshal drop index %v", err)
				r.setTransferTokenError(ttid, tt, err.Error())
				return
			}

			bodybuf := bytes.NewBuffer(body)

			url := "/dropIndex"
			resp, err := postWithAuth(r.localaddr+url, "application/json", bodybuf)
			if err != nil {
				// Error from HTTP layer, not from index processing code
				l.Errorf("Rebalancer::dropIndexWhenIdle Error drop index on %v %v", r.localaddr+url, err)
				r.setTransferTokenError(ttid, tt, err.Error())
				return
			}

			response := new(manager.IndexResponse)
			if err := convertResponse(resp, response); err != nil {
				l.Errorf("Rebalancer::dropIndexWhenIdle Error unmarshal response %v %v", r.localaddr+url, err)
				r.setTransferTokenError(ttid, tt, err.Error())
				return
			}

			if response.Code == manager.RESP_ERROR {
				// Error from index processing code (e.g. the string from common.ErrCollectionNotFound)
				if !isMissingBSC(response.Error) {
					l.Errorf("Rebalancer::dropIndexWhenIdle Error dropping index %v %v", r.localaddr+url, response.Error)
					r.setTransferTokenError(ttid, tt, response.Error)
					return
				}
				// Ok: failed to drop source index because b/s/c was dropped. Continue to TransferTokenCommit state.
				l.Infof("Rebalancer::dropIndexWhenIdle Source index already dropped due to bucket/scope/collection dropped. tt %v.", tt)
			}
			tt.State = c.TransferTokenCommit
			setTransferTokenInMetakv(ttid, tt)

			r.mu.Lock()
			r.sourceTokens[ttid] = tt
			r.mu.Unlock()

			break loop
		}
		time.Sleep(5 * time.Second)
	}
}

func (r *Rebalancer) needRetryForDrop(ttid string, tt *c.TransferToken) bool {

	localMeta, err := getLocalMeta(r.localaddr)
	if err != nil {
		l.Errorf("Rebalancer::dropIndexWhenIdle Error Fetching Local Meta %v %v", r.localaddr, err)
		return true
	}
	indexState, errStr := getIndexStatusFromMeta(tt, localMeta)
	if errStr != "" {
		l.Errorf("Rebalancer::dropIndexWhenIdle Error Fetching Index Status %v %v", r.localaddr, errStr)
		return true
	}

	if indexState == c.INDEX_STATE_NIL {
		//if index cannot be found in metadata, most likely its drop has already succeeded.
		//instead of waiting indefinitely, it is better to assume success and proceed.
		l.Infof("Rebalancer::dropIndexWhenIdle Missing Metadata for %v. Assume success and abort retry", tt.IndexInst)
		tt.State = c.TransferTokenCommit
		setTransferTokenInMetakv(ttid, tt)

		r.mu.Lock()
		r.sourceTokens[ttid] = tt
		r.mu.Unlock()

		return false
	}

	return true
}

// processTokenAsDest performs the work of the destination node of an index
// move reflected by the transfer token. It directly handles TT states
// TransferTokenCreated and TransferTokenInitiate, and indirectly (via token
// tokenMergeOrReady call here or in buildAcceptedIndexes --> waitForIndexBuild)
// states TransferTokenInProgress and TransferTokenMerge. Returns true iff it is
// considered to have processed this token (including handling some error cases).
func (r *Rebalancer) processTokenAsDest(ttid string, tt *c.TransferToken) bool {

	if tt.RebalId != r.rebalToken.RebalId {
		l.Warnf("Rebalancer::processTokenAsDest Found TransferToken with Unknown "+
			"RebalanceId. Local RId %v Token %v. Ignored.", r.rebalToken.RebalId, tt)
		return true
	}

	if !r.checkValidNotifyStateDest(ttid, tt) {
		return true
	}

	switch tt.State {
	case c.TransferTokenCreated:

		indexDefn := tt.IndexInst.Defn
		indexDefn.SetCollectionDefaults()

		indexDefn.Nodes = nil
		indexDefn.Deferred = true
		indexDefn.InstId = tt.InstId
		indexDefn.RealInstId = tt.RealInstId

		getReqBody := func() (*bytes.Buffer, bool) {

			ir := manager.IndexRequest{Index: indexDefn}
			body, err := json.Marshal(&ir)
			if err != nil {
				l.Errorf("Rebalancer::processTokenAsDest Error marshal clone index %v", err)
				r.setTransferTokenError(ttid, tt, err.Error())
				return nil, true
			}

			bodybuf := bytes.NewBuffer(body)
			return bodybuf, false
		}

		bodybuf, isErr := getReqBody()
		if isErr {
			return true
		}

		var resp *http.Response
		var err error
		url := "/createIndexRebalance"
		resp, err = postWithAuth(r.localaddr+url, "application/json", bodybuf)
		if err != nil {
			// Error from HTTP layer, not from index processing code
			l.Errorf("Rebalancer::processTokenAsDest Error register clone index on %v %v", r.localaddr+url, err)
			// If the error is io.EOF, then it is possible that server side
			// may have closed the connection while client is about the send the request.
			// Though this is extremely unlikely, this is observed for multiple users
			// in golang community. See: https://github.com/golang/go/issues/19943,
			// https://groups.google.com/g/golang-nuts/c/A46pBUjdgeM/m/jrn35_IxAgAJ for
			// more details
			//
			// In such a case, instead of failing the rebalance with io.EOF error, retry
			// the POST request. Two scenarios exist here:
			// (a) Server has received the request and closed the connection (Very unlikely)
			//     In this case, the request will be processed by server but client will see
			//     EOF error. Retry will fail rebalance that index definition already exists.
			// (b) Server has not received this request. Then retry will work and rebalance
			//     will not fail.
			//
			// Instead of failing rebalance with io.EOF error, we retry the request and reduce
			// probability of failure
			if strings.HasSuffix(err.Error(), ": EOF") {
				bodybuf, isErr := getReqBody()
				if isErr {
					return true
				}
				resp, err = postWithAuth(r.localaddr+url, "application/json", bodybuf)
				if err != nil {
					l.Errorf("Rebalancer::processTokenAsDest Error register clone index during retry on %v %v", r.localaddr+url, err)
					r.setTransferTokenError(ttid, tt, err.Error())
					return true
				} else {
					l.Infof("Rebalancer::processTokenAsDest Successful POST of createIndexRebalance during retry on %v, defnId: %v, instId: %v",
						r.localaddr+url, indexDefn.DefnId, indexDefn.InstId)
				}
			} else {
				r.setTransferTokenError(ttid, tt, err.Error())
				return true
			}
		}

		response := new(manager.IndexResponse)
		if err := convertResponse(resp, response); err != nil {
			l.Errorf("Rebalancer::processTokenAsDest Error unmarshal response %v %v", r.localaddr+url, err)
			r.setTransferTokenError(ttid, tt, err.Error())
			return true
		}
		if response.Code == manager.RESP_ERROR {
			// Error from index processing code (e.g. the string from common.ErrCollectionNotFound)
			if !isMissingBSC(response.Error) {
				l.Errorf("Rebalancer::processTokenAsDest Error cloning index %v %v", r.localaddr+url, response.Error)
				r.setTransferTokenError(ttid, tt, response.Error)
				return true
			}
			// Ok: failed to create dest index because b/s/c was dropped. Skip to TransferTokenCommit state.
			l.Infof("Rebalancer::processTokenAsDest Create destination index failed due to bucket/scope/collection dropped. Skipping. tt %v.", tt)
			tt.State = c.TransferTokenCommit
		} else {
			tt.State = c.TransferTokenAccepted
		}
		setTransferTokenInMetakv(ttid, tt)

		r.mu.Lock()
		r.acceptedTokens[ttid] = tt
		r.mu.Unlock()

	case c.TransferTokenInitiate:

		r.mu.Lock()
		defer r.mu.Unlock()
		att, ok := r.acceptedTokens[ttid]
		if !ok {
			l.Errorf("Rebalancer::processTokenAsDest Unknown TransferToken for Initiate %v %v", ttid, tt)
			r.setTransferTokenError(ttid, tt, "Unknown TransferToken For Initiate")
			return true
		}
		if tt.IndexInst.Defn.Deferred && tt.IndexInst.State == c.INDEX_STATE_READY {

			atomic.AddInt32(&r.pendingBuild, 1)
			r.tokenMergeOrReady(ttid, tt)
			att.State = tt.State

		} else {
			att.State = c.TransferTokenInProgress
			tt.State = c.TransferTokenInProgress
			setTransferTokenInMetakv(ttid, tt)
			atomic.AddInt32(&r.pendingBuild, 1)
		}
		logging.Infof("Rebalancer::processTokenAsDest, Incremening pendingBuild due to ttid: %v, pendingBuild value: %v", ttid, atomic.LoadInt32(&r.pendingBuild))

		if r.checkIndexReadyToBuild() == true {
			if !r.addToWaitGroup() {
				return true
			}
			go r.buildAcceptedIndexes()
		}

	case c.TransferTokenInProgress:
		// Nothing to do here; TT state transitions done in tokenMergeOrReady

	case c.TransferTokenMerge:
		// Nothing to do here; TT state transitions done in tokenMergeOrReady

	default:
		return false
	}
	return true
}

func (r *Rebalancer) checkValidNotifyStateDest(ttid string, tt *c.TransferToken) bool {

	r.mu.Lock()
	defer r.mu.Unlock()

	if tto, ok := r.acceptedTokens[ttid]; ok {
		if tt.State <= tto.State {
			l.Warnf("Rebalancer::checkValidNotifyStateDest Detected Invalid State "+
				"Change Notification. Token Id %v Local State %v Metakv State %v", ttid,
				tto.State, tt.State)
			return false
		}
	}
	return true

}

func (r *Rebalancer) checkValidNotifyStateSource(ttid string, tt *c.TransferToken) bool {

	r.mu.Lock()
	defer r.mu.Unlock()

	if tto, ok := r.sourceTokens[ttid]; ok {
		if tt.State <= tto.State {
			l.Warnf("Rebalancer::checkValidNotifyStateSource Detected Invalid State "+
				"Change Notification. Token Id %v Local State %v Metakv State %v", ttid,
				tto.State, tt.State)
			return false
		}
	}
	return true

}

func (r *Rebalancer) checkIndexReadyToBuild() bool {

	for ttid, tt := range r.acceptedTokens {
		if tt.State != c.TransferTokenMerge &&
			tt.State != c.TransferTokenInProgress &&
			tt.State != c.TransferTokenReady &&
			tt.State != c.TransferTokenCommit {
			l.Infof("Rebalancer::checkIndexReadyToBuild Not ready to build %v %v", ttid, tt)
			return false
		}
	}
	return true

}

// buildAcceptedIndexes runs in a go routine and submits a request to Indexer to build all indexes
// for accepted transfer tokens that are ready to start building. The builds occur asynchronously
// in Indexer handleBuildIndex. This function calls waitForIndexBuild to wait for them to finish.
func (r *Rebalancer) buildAcceptedIndexes() {

	defer r.wg.Done()

	var idList client.IndexIdList                   // defnIds of the indexes to build
	ttMap := make(map[string]*common.TransferToken) // ttids to tts of the building indexes
	var errStr string
	r.mu.Lock()
	for ttid, tt := range r.acceptedTokens {
		if tt.State != c.TransferTokenReady &&
			tt.State != c.TransferTokenCommit &&
			tt.State != c.TransferTokenMerge {

			idList.DefnIds = append(idList.DefnIds, uint64(tt.IndexInst.Defn.DefnId))
			ttMap[ttid] = tt
		}
	}
	r.mu.Unlock()

	if len(idList.DefnIds) == 0 {
		l.Infof("Rebalancer::buildAcceptedIndexes Nothing to build")
		return
	}

	response := new(manager.IndexResponse)
	url := "/buildIndexRebalance"

	getReqBody := func() (*bytes.Buffer, error) {
		ir := manager.IndexRequest{IndexIds: idList}
		body, err := json.Marshal(&ir)
		if err != nil {
			l.Errorf("Rebalancer::buildAcceptedIndexes Error marshalling index inst list: %v, err: %v", idList, err)
			return nil, err
		}
		bodybuf := bytes.NewBuffer(body)
		return bodybuf, nil
	}

	var bodybuf *bytes.Buffer
	var resp *http.Response
	var err error

	bodybuf, err = getReqBody()
	if err != nil {
		errStr = err.Error()
		goto cleanup
	}

	resp, err = postWithAuth(r.localaddr+url, "application/json", bodybuf)
	if err != nil {
		// Error from HTTP layer, not from index processing code
		l.Errorf("Rebalancer::buildAcceptedIndexes Error register clone index on %v %v", r.localaddr+url, err)
		if strings.HasSuffix(err.Error(), ": EOF") {
			// Retry build again before failing rebalance
			bodybuf, err = getReqBody()
			resp, err = postWithAuth(r.localaddr+url, "application/json", bodybuf)
			if err != nil {
				l.Errorf("Rebalancer::buildAcceptedIndexes Error register clone index during retry on %v %v", r.localaddr+url, err)
				errStr = err.Error()
				goto cleanup
			} else {
				l.Infof("Rebalancer::buildAcceptedIndexes Successful POST of buildIndexRebalance during retry on %v, instIdList: %v", r.localaddr+url, idList)
			}
		} else {
			errStr = err.Error()
			goto cleanup
		}
	}

	if err := convertResponse(resp, response); err != nil {
		l.Errorf("Rebalancer::buildAcceptedIndexes Error unmarshal response %v %v", r.localaddr+url, err)
		errStr = err.Error()
		goto cleanup
	}
	if response.Code == manager.RESP_ERROR {
		// Error from index processing code. For rebalance this returns either ErrMarshalFailed.Error
		// or a JSON string of a marshaled map[IndexInstId]string of error messages per instance ID. The
		// keys for any entries having magic error string ErrIndexNotFoundRebal.Error are the submitted
		// defnIds instead of instIds, as the metadata could not be found for these.
		l.Errorf("Rebalancer::buildAcceptedIndexes Error cloning index %v %v", r.localaddr+url, response.Error)
		errStr = response.Error
		if errStr == common.ErrMarshalFailed.Error() { // no detailed error info available
			goto cleanup
		}

		// Unmarshal the detailed error info
		var errMap map[c.IndexInstId]string
		err = json.Unmarshal([]byte(errStr), &errMap)
		if err != nil {
			errStr = c.ErrUnmarshalFailed.Error()
			goto cleanup
		}

		// Deal with the individual errors
		for id, errStr := range errMap {
			if isMissingBSC(errStr) {
				// id is an instId. Move the TT for this instId to TransferTokenCommit
				r.mu.Lock()
				for ttid, tt := range ttMap {
					if id == tt.IndexInst.InstId {
						l.Infof("Rebalancer::buildAcceptedIndexes Build destination index failed due to bucket/scope/collection dropped. Skipping. tt %v.", tt)
						tt.State = c.TransferTokenCommit
						setTransferTokenInMetakv(ttid, tt)
						atomic.AddInt32(&r.pendingBuild, -1)
						break
					}
				}
				r.mu.Unlock()
			} else if isIndexNotFoundRebal(errStr) {
				// id is a defnId. Move all TTs for this defnId to TransferTokenCommit
				defnId := c.IndexDefnId(id)
				r.mu.Lock()
				for ttid, tt := range ttMap {
					if defnId == tt.IndexInst.Defn.DefnId {
						l.Infof("Rebalancer::buildAcceptedIndexes Build destination index failed due to index metadata missing; bucket/scope/collection likely dropped. Skipping. tt %v.", tt)
						tt.State = c.TransferTokenCommit
						setTransferTokenInMetakv(ttid, tt)
						atomic.AddInt32(&r.pendingBuild, -1)
					}
				}
				r.mu.Unlock()
			} else {
				goto cleanup // unrecoverable error
			}
		}
	}

	r.waitForIndexBuild(ttMap)

	return

cleanup: // fail the rebalance; mark all accepted transfer tokens with error
	r.mu.Lock()
	for ttid, tt := range r.acceptedTokens {
		r.setTransferTokenError(ttid, tt, errStr)
	}
	r.mu.Unlock()
	return
}

// waitForIndexBuild waits for all currently running rebalamnce-initiated index
// builds to complete. Transfer token state changes and some processing are
// delegated to tokenMergeOrReady.
func (r *Rebalancer) waitForIndexBuild(buildTokens map[string]*common.TransferToken) {

	buildStartTime := time.Now()
	cfg := r.config.Load()
	maxRemainingBuildTime := cfg["rebalance.maxRemainingBuildTime"].Uint64()

loop:
	for {
		select {
		case <-r.cancel:
			l.Infof("Rebalancer::waitForIndexBuild Cancel Received")
			break loop
		case <-r.done:
			l.Infof("Rebalancer::waitForIndexBuild Done Received")
			break loop
		default:

			stats, err := getLocalStats(r.localaddr, false)
			if err != nil {
				l.Errorf("Rebalancer::waitForIndexBuild Error Fetching Local Stats %v %v", r.localaddr, err)
				break
			}

			statsMap := stats.ToMap()
			if statsMap == nil {
				l.Infof("Rebalancer::waitForIndexBuild Nil Stats. Retrying...")
				break
			}

			if state, ok := statsMap["indexer_state"]; ok {
				if state == "Paused" {
					l.Errorf("Rebalancer::waitForIndexBuild Paused state detected for %v", r.localaddr)
					r.mu.Lock()
					for ttid, tt := range r.acceptedTokens {
						l.Errorf("Rebalancer::waitForIndexBuild Token State Changed to Error %v %v", ttid, tt)
						r.setTransferTokenError(ttid, tt, "Indexer In Paused State")
					}
					r.mu.Unlock()
					break
				}
			}

			localMeta, err := getLocalMeta(r.localaddr)
			if err != nil {
				l.Errorf("Rebalancer::waitForIndexBuild Error Fetching Local Meta %v %v", r.localaddr, err)
				break
			}

			r.mu.Lock()
			allTokensReady := true
			for ttid, tt := range buildTokens {
				if tt.State == c.TransferTokenReady || tt.State == c.TransferTokenCommit {
					continue
				}
				allTokensReady = false

				if tt.State == c.TransferTokenMerge {
					continue
				}

				indexState, err := getIndexStatusFromMeta(tt, localMeta)
				if indexState == c.INDEX_STATE_NIL || indexState == c.INDEX_STATE_DELETED {
					l.Infof("Rebalancer::waitForIndexBuild Could not get index status; bucket/scope/collection likely dropped. Skipping. indexState %v, err %v, tt %v.",
						indexState, err, tt)
					tt.State = c.TransferTokenCommit // skip forward instead of failing rebalance
					setTransferTokenInMetakv(ttid, tt)

					atomic.AddInt32(&r.pendingBuild, -1)
				} else if err != "" {
					l.Errorf("Rebalancer::waitForIndexBuild Error Fetching Index Status %v %v", r.localaddr, err)
					break
				}

				defn := tt.IndexInst.Defn
				defn.SetCollectionDefaults()

				prefix := common.GetStatsPrefix(defn.Bucket, defn.Scope, defn.Collection,
					defn.Name, tt.IndexInst.ReplicaId, 0, false)

				sname_pend := common.GetIndexStatKey(prefix, "num_docs_pending")
				sname_queued := common.GetIndexStatKey(prefix, "num_docs_queued")
				sname_processed := common.GetIndexStatKey(prefix, "num_docs_processed")

				var num_pend, num_queued, num_processed float64
				if _, ok := statsMap[sname_pend]; ok {
					num_pend = statsMap[sname_pend].(float64)
					num_queued = statsMap[sname_queued].(float64)
					num_processed = statsMap[sname_processed].(float64)
				} else {
					l.Infof("Rebalancer::waitForIndexBuild Missing Stats %v %v. Retrying...", sname_queued, sname_pend)
					break
				}

				tot_remaining := num_pend + num_queued

				elapsed := time.Since(buildStartTime).Seconds()
				if elapsed == 0 {
					elapsed = 1
				}

				processing_rate := num_processed / elapsed
				remainingBuildTime := maxRemainingBuildTime
				if processing_rate != 0 {
					remainingBuildTime = uint64(tot_remaining / processing_rate)
				}

				if tot_remaining == 0 {
					remainingBuildTime = 0
				}

				l.Infof("Rebalancer::waitForIndexBuild Index: %v:%v:%v:%v State: %v"+
					" Pending: %v EstTime: %v Partitions: %v Destination: %v",
					defn.Bucket, defn.Scope, defn.Collection, defn.Name, indexState,
					tot_remaining, remainingBuildTime, defn.Partitions, r.localaddr)

				if indexState == c.INDEX_STATE_ACTIVE && remainingBuildTime < maxRemainingBuildTime {
					r.tokenMergeOrReady(ttid, tt)
				}
			}
			r.mu.Unlock()

			if allTokensReady {
				l.Infof("Rebalancer::waitForIndexBuild Batch Done")
				break loop
			}
			time.Sleep(3 * time.Second)
		}
	}
} // waitForIndexBuild

func (r *Rebalancer) checkIndexReadyToDrop() bool {
	return atomic.LoadInt32(&r.pendingBuild) == 0
}

// tokenMergeOrReady handles dest TT transitions from TransferTokenInProgress,
// possibly through TransferTokenMerge, and then to TransferTokenReady (move
// case) or TransferTokenCommit (non-move case == replica repair; no source
// index to delete). TransferTokenMerge state is for partitioned indexes where
// a partn is being moved to a node that already has another partn of the same
// index. There cannot be two IndexDefns of the same index, so in this situation
// the moving partition (called a "proxy") gets a "fake" IndexDefn that later
// must "merge" with the "real" IndexDefn once it has completed its move.
func (r *Rebalancer) tokenMergeOrReady(ttid string, tt *c.TransferToken) {

	// There is no proxy (no merge needed)
	if tt.RealInstId == 0 {

		respch := make(chan error)
		r.supvMsgch <- &MsgUpdateIndexRState{
			instId: tt.InstId,
			rstate: c.REBAL_ACTIVE,
			respch: respch}
		err := <-respch
		c.CrashOnError(err)

		if tt.TransferMode == c.TokenTransferModeMove {
			tt.State = c.TransferTokenReady
		} else {
			tt.State = c.TransferTokenCommit // no source to delete in non-move case
		}
		setTransferTokenInMetakv(ttid, tt)
		atomic.AddInt32(&r.pendingBuild, -1)
		logging.Infof("Rebalancer::tokenMergeOrReady, Decrementing pendingBuild due to ttid: %v, pendingBuild value: %v", ttid, atomic.LoadInt32(&r.pendingBuild))

		r.dropIndexWhenReady()

	} else {
		// There is a proxy (merge needed). The proxy partitions need to be move
		// to the real index instance before Token can move to Ready state.
		tt.State = c.TransferTokenMerge
		setTransferTokenInMetakv(ttid, tt)

		go func(ttid string, tt c.TransferToken) {

			respch := make(chan error)
			r.supvMsgch <- &MsgMergePartition{
				srcInstId:  tt.InstId,
				tgtInstId:  tt.RealInstId,
				rebalState: c.REBAL_ACTIVE,
				respCh:     respch}

			var err error
			select {
			case err = <-respch:
			case <-r.cancel:
				l.Infof("Rebalancer::tokenMergeOrReady: rebalancer cancel Received")
				return
			case <-r.done:
				l.Infof("Rebalancer::tokenMergeOrReady: rebalancer done Received")
				return
			}

			if err != nil {
				// If there is an error, move back to InProgress state.
				// An error condition indicates that merge has not been
				// committed yet, so we are safe to revert.
				tt.State = c.TransferTokenInProgress
				setTransferTokenInMetakv(ttid, &tt)

				// The indexer could be in an inconsistent state.
				// So we will need to restart the indexer.
				time.Sleep(100 * time.Millisecond)
				c.CrashOnError(err)
			}

			// There is no error from merging index instance. Update token in metakv.
			if tt.TransferMode == c.TokenTransferModeMove {
				tt.State = c.TransferTokenReady
			} else {
				tt.State = c.TransferTokenCommit // no source to delete in non-move case
			}
			setTransferTokenInMetakv(ttid, &tt)

			// if rebalancer is still active, then update its runtime state.
			if !r.isFinish() {
				r.mu.Lock()
				defer r.mu.Unlock()

				if newTT := r.acceptedTokens[ttid]; newTT != nil {
					newTT.State = tt.State
				}
				atomic.AddInt32(&r.pendingBuild, -1)
				logging.Infof("Rebalancer::tokenMergeOrReady, Decrementing pendingBuild due to ttid: %v, pendingBuild value: %v", ttid, atomic.LoadInt32(&r.pendingBuild))

				r.dropIndexWhenReady()
			}

		}(ttid, *tt)
	}
}

// processTokenAsMaster performs master node TT bookkeeping for a rebalance. It
// handles transfer token states TransferTokenAccepted, TransferTokenCommit,
// TransferTokenDeleted, and NO-OP TransferTokenRefused. Returns true iff it is
// considered to have processed this token (including handling some error cases).
func (r *Rebalancer) processTokenAsMaster(ttid string, tt *c.TransferToken) bool {

	if tt.RebalId != r.rebalToken.RebalId {
		l.Warnf("Rebalancer::processTokenAsMaster Found TransferToken with Unknown "+
			"RebalanceId. Local RId %v Token %v. Ignored.", r.rebalToken.RebalId, tt)
		return true
	}

	if tt.Error != "" {
		l.Errorf("Rebalancer::processTokenAsMaster Detected TransferToken in Error state %v. Abort.", tt)

		r.cancelMetakv()
		go r.finish(errors.New(tt.Error))
		return true
	}

	switch tt.State {

	case c.TransferTokenAccepted:
		tt.State = c.TransferTokenInitiate
		setTransferTokenInMetakv(ttid, tt)

	case c.TransferTokenRefused:
		//TODO replan

	case c.TransferTokenCommit:
		tt.State = c.TransferTokenDeleted
		setTransferTokenInMetakv(ttid, tt)

		r.updateMasterTokenState(ttid, c.TransferTokenCommit)

	case c.TransferTokenDeleted:
		err := c.MetakvDel(RebalanceMetakvDir + ttid)
		if err != nil {
			l.Fatalf("Rebalancer::processTokenAsMaster Unable to set TransferToken In "+
				"Meta Storage. %v. Err %v", tt, err)
			c.CrashOnError(err)
		}

		r.updateMasterTokenState(ttid, c.TransferTokenDeleted)

		if r.checkAllTokensDone() {
			if r.cb.progress != nil {
				r.cb.progress(1.0, r.cancel)
			}
			l.Infof("Rebalancer::processTokenAsMaster No Tokens Found. Mark Done.")
			r.cancelMetakv()
			go r.finish(nil)
		} else {
			if r.checkCurrBatchDone() {
				r.publishTransferTokenBatch()
			}
		}

	default:
		return false
	}
	return true
}

func (r *Rebalancer) setTransferTokenError(ttid string, tt *c.TransferToken, err string) {
	tt.Error = err
	setTransferTokenInMetakv(ttid, tt)

}

func (r *Rebalancer) updateMasterTokenState(ttid string, state c.TokenState) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if tt, ok := r.transferTokens[ttid]; ok {
		tt.State = state
		r.transferTokens[ttid] = tt
	} else {
		return false
	}

	return true
}

// setTransferTokenInMetakv stores a transfer token in metakv with several retries before
// failure. Creation and changes to a token in metakv will trigger the next step of token
// processing via callback from metakv to LifecycleMgr set by its prior OnNewRequest call.
func setTransferTokenInMetakv(ttid string, tt *c.TransferToken) {

	fn := func(r int, err error) error {
		if r > 0 {
			l.Warnf("Rebalancer::setTransferTokenInMetakv error=%v Retrying (%d)", err, r)
		}
		err = c.MetakvSet(RebalanceMetakvDir+ttid, tt)
		return err
	}

	rh := c.NewRetryHelper(10, time.Second, 1, fn)
	err := rh.Run()

	if err != nil {
		l.Fatalf("Rebalancer::setTransferTokenInMetakv Unable to set TransferToken In "+
			"Meta Storage. %v %v. Err %v", ttid, tt, err)
		c.CrashOnError(err)
	}
}

// decodeTransferToken unmarshals a transfer token received in a callback from metakv.
func (r *Rebalancer) decodeTransferToken(path string, value []byte) (string, *c.TransferToken, error) {

	ttidpos := strings.Index(path, TransferTokenTag)
	ttid := path[ttidpos:]

	var tt c.TransferToken
	err := json.Unmarshal(value, &tt)
	if err != nil {
		l.Fatalf("Rebalancer::decodeTransferToken Failed unmarshalling value for %s: %s\n%s",
			path, err.Error(), string(value))
		return "", nil, err
	}

	l.Infof("Rebalancer::decodeTransferToken TransferToken %v %v", ttid, tt)

	return ttid, &tt, nil

}

// updateProgress runs in a master-node go routine to update the progress of processing
// a single transfer token. It is started when the TransferTokenAccepted state is processed.
func (r *Rebalancer) updateProgress() {

	if !r.addToWaitGroup() {
		return
	}
	defer r.wg.Done()

	l.Infof("Rebalancer::updateProgress goroutine started")
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			progress := r.computeProgress()
			r.cb.progress(progress, r.cancel)
		case <-r.cancel:
			l.Infof("Rebalancer::updateProgress Cancel Received")
			return
		case <-r.done:
			l.Infof("Rebalancer::updateProgress Done Received")
			return
		}
	}
}

// computeProgress is a helper for updateProgress called periodically to compute the progress of index builds.
func (r *Rebalancer) computeProgress() (progress float64) {

	url := "/getIndexStatus?getAll=true"
	resp, err := getWithAuth(r.localaddr + url)
	if err != nil {
		l.Errorf("Rebalancer::computeProgress Error getting local metadata %v %v", r.localaddr+url, err)
		return
	}

	defer resp.Body.Close()
	statusResp := new(manager.IndexStatusResponse)
	bytes, _ := ioutil.ReadAll(resp.Body)
	if err := json.Unmarshal(bytes, &statusResp); err != nil {
		l.Errorf("Rebalancer::computeProgress Error unmarshal response %v %v", r.localaddr+url, err)
		return
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	totTokens := len(r.transferTokens)

	var totalProgress float64
	for _, tt := range r.transferTokens {
		state := tt.State
		// All states not tested in the if-else if are treated as 0% progress
		if state == c.TransferTokenReady || state == c.TransferTokenMerge ||
			state == c.TransferTokenCommit || state == c.TransferTokenDeleted {
			totalProgress += 100.00
		} else if state == c.TransferTokenInProgress {
			totalProgress += r.getBuildProgressFromStatus(statusResp, tt)
		}
	}

	progress = (totalProgress / float64(totTokens)) / 100.0
	l.Infof("Rebalancer::computeProgress %v", progress)

	if progress < 0.1 || math.IsNaN(progress) {
		progress = 0.1
	} else if progress == 1.0 {
		progress = 0.99
	}
	return
}

func (r *Rebalancer) checkAllTokensDone() bool {

	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, tt := range r.transferTokens {
		if tt.State != c.TransferTokenDeleted {
			return false
		}
	}
	return true
}

// checkCurrBatchDone returns true iff all transfer tokens in the current
// batch have finished processing (reached TransferTokenDeleted state).
func (r *Rebalancer) checkCurrBatchDone() bool {

	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, ttid := range r.currBatchTokens {
		tt := r.transferTokens[ttid]
		if tt.State != c.TransferTokenDeleted {
			return false
		}
	}
	return true
}

func (r *Rebalancer) checkDDLRunning() (bool, error) {

	if r.runParam != nil && r.runParam.ddlRunning {
		l.Errorf("Rebalancer::doRebalance Found index build running. Cannot process rebalance.")
		fmtMsg := "indexer rebalance failure - index build is in progress for indexes: %v."
		err := errors.New(fmt.Sprintf(fmtMsg, r.runParam.ddlRunningIndexNames))
		return true, err
	}
	return false, nil
}

// getIndexStatusFromMeta gets the index state and error message, if present, for the
// index referenced by a transfer token from index metadata (topology tree). It returns
// state INDEX_STATE_NIL if the metadata is not found (e.g. if the index has been
// dropped). If an error message is returned with a state other than INDEX_STATE_NIL,
// the metadata was found and the message is from a field in the topology tree itself.
func getIndexStatusFromMeta(tt *c.TransferToken, localMeta *manager.LocalIndexMetadata) (c.IndexState, string) {

	inst := tt.IndexInst

	topology := findTopologyByCollection(localMeta.IndexTopologies, inst.Defn.Bucket, inst.Defn.Scope, inst.Defn.Collection)
	if topology == nil {
		return c.INDEX_STATE_NIL, fmt.Sprintf("Topology Information Missing for Bucket %v Scope %v Collection %v",
			inst.Defn.Bucket, inst.Defn.Scope, inst.Defn.Collection)
	}

	state, errMsg := topology.GetStatusByInst(inst.Defn.DefnId, tt.InstId)
	if state == c.INDEX_STATE_NIL && tt.RealInstId != 0 {
		return topology.GetStatusByInst(inst.Defn.DefnId, tt.RealInstId)
	}

	return state, errMsg
}

// getDestNode returns the key of the partitionMap entry whose value
// contains partitionId. This is the node hosting that partition.
func getDestNode(partitionId c.PartitionId, partitionMap map[string][]int) string {
	for node, partIds := range partitionMap {
		for _, partId := range partIds {
			if partId == int(partitionId) {
				return node
			}
		}
	}
	return "" // should not reach here
} // getDestNode

// getBuildProgressFromStatus is a helper for computeProgress that gets an estimate of index build progress for the
// given transfer token from the status arg.
func (r *Rebalancer) getBuildProgressFromStatus(status *manager.IndexStatusResponse, tt *c.TransferToken) float64 {

	instId := tt.InstId
	realInstId := tt.RealInstId // for partitioned indexes

	// If it is a partitioned index, it is possible that we cannot find progress from instId:
	// 1) partition is built using realInstId
	// 2) partition has already been merged to instance with realInstId
	// In either case, count will be 0 after calling getBuildProgress(instId) and it will find progress
	// using realInstId instead.
	realInstProgress, count := r.getBuildProgress(status, tt, instId)
	if count == 0 {
		realInstProgress, count = r.getBuildProgress(status, tt, realInstId)
	}
	if count > 0 {
		r.lastKnownProgress[instId] = realInstProgress / float64(count)
	}

	if p, ok := r.lastKnownProgress[instId]; ok {
		return p
	}
	return 0.0
} // getBuildProgressFromStatus

// getBuildProgress is a helper for getBuildProgressFromStatus that gets the progress of an inde≈ build
// for a given instId. Return value count gives the number of partitions of instId found to be building.
// If this is 0 the caller will try again with realInstId to find the progress of a partitioned index.
func (r *Rebalancer) getBuildProgress(status *manager.IndexStatusResponse, tt *c.TransferToken, instId c.IndexInstId) (
	realInstProgress float64, count int) {

	destId := tt.DestId
	defn := tt.IndexInst.Defn

	for _, idx := range status.Status {
		if idx.InstId == instId {
			// This function is called for every transfer token before it has becomes COMMITTED or DELETED.
			// The index may have not be in REAL_PENDING state but the token has not yet moved to COMMITTED/DELETED state.
			// So we need to return progress even if it is not replicating.
			// Pre-7.0 nodes will report "Replicating" instead of "Moving" so check for both.
			if idx.Status == "Moving" || idx.Status == "Replicating" || idx.NodeUUID == destId {
				progress, ok := r.lastKnownProgress[instId]
				if !ok || idx.Progress > 0 {
					progress = idx.Progress
				}

				destNode := getDestNode(defn.Partitions[0], idx.PartitionMap)
				l.Infof("Rebalancer::getBuildProgress Index: %v:%v:%v:%v"+
					" Progress: %v InstId: %v RealInstId: %v Partitions: %v Destination: %v",
					defn.Bucket, defn.Scope, defn.Collection, defn.Name,
					progress, idx.InstId, tt.RealInstId, defn.Partitions, destNode)

				realInstProgress += progress
				count++
			}
		}
	}
	return realInstProgress, count
}

//
// This function gets the indexer stats for a specific indexer host.
//
func getLocalStats(addr string, partitioned bool) (*c.Statistics, error) {

	queryStr := "/stats?async=true&consumerFilter=rebalancer"
	if partitioned {
		queryStr += "&partition=true"
	}
	resp, err := getWithAuth(addr + queryStr)
	if err != nil {
		return nil, err
	}

	stats := new(c.Statistics)
	if err := convertResponse(resp, stats); err != nil {
		return nil, err
	}

	return stats, nil
}

func getLocalMeta(addr string) (*manager.LocalIndexMetadata, error) {

	url := "/getLocalIndexMetadata?useETag=false"
	resp, err := getWithAuth(addr + url)
	if err != nil {
		l.Errorf("Rebalancer::getLocalMeta Error getting local metadata %v %v", addr+url, err)
		return nil, err
	}

	defer resp.Body.Close()
	localMeta := new(manager.LocalIndexMetadata)
	bytes, _ := ioutil.ReadAll(resp.Body)
	if err := json.Unmarshal(bytes, &localMeta); err != nil {
		l.Errorf("Rebalancer::getLocalMeta Error unmarshal response %v %v", addr+url, err)
		return nil, err
	}

	return localMeta, nil
}

//returs if all indexers are warmedup and addr of any paused indexer
func checkAllIndexersWarmedup(clusterURL string) (bool, []string) {

	var pausedAddr []string

	cinfo, err := c.FetchNewClusterInfoCache2(clusterURL, c.DEFAULT_POOL, "checkAllIndexersWarmedup")
	if err != nil {
		l.Errorf("Rebalancer::checkAllIndexersWarmedup Error Fetching Cluster Information %v", err)
		return false, nil
	}

	if err := cinfo.FetchNodesAndSvsInfo(); err != nil {
		l.Errorf("Rebalancer::checkAllIndexersWarmedup Error Fetching Nodes and serives Information %v", err)
		return false, nil
	}

	nids := cinfo.GetNodesByServiceType(c.INDEX_HTTP_SERVICE)
	url := "/stats?async=true"

	allWarmedup := true

	for _, nid := range nids {

		addr, err := cinfo.GetServiceAddress(nid, c.INDEX_HTTP_SERVICE, true)
		if err == nil {

			resp, err := getWithAuth(addr + url)
			if err != nil {
				l.Infof("Rebalancer::checkAllIndexersWarmedup Error Fetching Stats %v From %v", err, addr)
				return false, nil
			}

			stats := new(c.Statistics)
			if err := convertResponse(resp, stats); err != nil {
				l.Infof("Rebalancer::checkAllIndexersWarmedup Error Convert Response %v From %v", err, addr)
				return false, nil
			}

			statsMap := stats.ToMap()
			if statsMap == nil {
				l.Infof("Rebalancer::checkAllIndexersWarmedup Nil Stats From %v", addr)
				return false, nil
			}

			if state, ok := statsMap["indexer_state"]; ok {
				if state == "Paused" {
					l.Infof("Rebalancer::checkAllIndexersWarmedup Paused state detected for %v", addr)
					pausedAddr = append(pausedAddr, addr)
				} else if state != "Active" {
					l.Infof("Rebalancer::checkAllIndexersWarmedup Indexer %v State %v", addr, state)
					allWarmedup = false
				}
			}
		} else {
			l.Errorf("Rebalancer::checkAllIndexersWarmedup Error Fetching Service Address %v", err)
			return false, nil
		}
	}

	return allWarmedup, pausedAddr
}

//
// This function unmarshalls a response.
//
func convertResponse(r *http.Response, resp interface{}) error {
	defer r.Body.Close()

	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(r.Body); err != nil {
		return err
	}

	if err := json.Unmarshal(buf.Bytes(), resp); err != nil {
		return err
	}

	return nil
}
