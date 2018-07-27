// @copyright 2016 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package indexer

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/cbauth/metakv"
	c "github.com/couchbase/indexing/secondary/common"
	l "github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/manager"
	"github.com/couchbase/indexing/secondary/manager/client"
)

type DoneCallback func(err error, cancel <-chan struct{})
type ProgressCallback func(progress float64, cancel <-chan struct{})

type Callbacks struct {
	progress ProgressCallback
	done     DoneCallback
}

type Rebalancer struct {
	transferTokens map[string]*c.TransferToken
	acceptedTokens map[string]*c.TransferToken
	sourceTokens   map[string]*c.TransferToken

	rebalToken *RebalanceToken
	nodeId     string
	master     bool

	cb Callbacks

	cancel chan struct{}
	done   chan struct{}

	metakvCancel chan struct{}

	supvMsgch MsgChannel

	mu sync.RWMutex

	muCleanup sync.RWMutex

	localaddr string

	wg sync.WaitGroup

	progressInitOnce sync.Once
	cleanupOnce      sync.Once

	waitForTokenPublish chan struct{}

	retErr error

	config c.ConfigHolder

	lastKnownProgress map[c.IndexDefnId]float64

	transferTokenBatches [][]string
	currBatchTokens      []string
}

func NewRebalancer(transferTokens map[string]*c.TransferToken, rebalToken *RebalanceToken,
	nodeId string, master bool, progress ProgressCallback, done DoneCallback,
	supvMsgch MsgChannel, localaddr string, config c.Config) *Rebalancer {

	l.Infof("NewRebalancer nodeId %v rebalToken %v master %v localaddr %v", nodeId,
		rebalToken, master, localaddr)

	r := &Rebalancer{
		transferTokens: transferTokens,
		rebalToken:     rebalToken,
		master:         master,
		nodeId:         nodeId,

		cb: Callbacks{progress, done},

		cancel: make(chan struct{}),
		done:   make(chan struct{}),

		metakvCancel: make(chan struct{}),

		supvMsgch: supvMsgch,

		acceptedTokens: make(map[string]*c.TransferToken),
		sourceTokens:   make(map[string]*c.TransferToken),
		localaddr:      localaddr,

		waitForTokenPublish:  make(chan struct{}),
		lastKnownProgress:    make(map[c.IndexDefnId]float64),
		transferTokenBatches: make([][]string, 0),
	}

	r.config.Store(config)

	if master {
		go r.doRebalance()
	} else {
		close(r.waitForTokenPublish)
	}

	if r.transferTokens != nil || !master {
		go r.observeRebalance()
	}
	return r
}

func (r *Rebalancer) Cancel() {
	l.Infof("Rebalancer::Cancel Exiting")

	r.cancelMetakv()

	close(r.cancel)
	r.wg.Wait()
}

func (r *Rebalancer) finish(err error) {

	r.retErr = err
	r.cleanupOnce.Do(r.doFinish)

}

func (r *Rebalancer) doFinish() {

	l.Infof("Rebalancer::doFinish Cleanup %v", r.retErr)

	close(r.done)
	r.cancelMetakv()

	r.wg.Wait()
	r.cb.done(r.retErr, r.cancel)

}

func (r *Rebalancer) cancelMetakv() {

	r.muCleanup.Lock()
	defer r.muCleanup.Unlock()

	if r.metakvCancel != nil {
		close(r.metakvCancel)
		r.metakvCancel = nil
	}

}

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

func (r *Rebalancer) doRebalance() {

	if r.transferTokens != nil {
		r.createTransferBatches()
		r.publishTransferTokenBatch()
	} else {
		r.cb.progress(1.0, r.cancel)
		r.finish(nil)
	}

	close(r.waitForTokenPublish)
}

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

func (r *Rebalancer) publishTransferTokenBatch() {

	r.mu.Lock()
	defer r.mu.Unlock()

	r.currBatchTokens = r.transferTokenBatches[0]
	r.transferTokenBatches = r.transferTokenBatches[1:]

	l.Infof("Rebalancer::publishTransferTokenBatch Registered Transfer Token In Metakv %v", r.currBatchTokens)

	for _, ttid := range r.currBatchTokens {
		r.setTransferTokenInMetakv(ttid, r.transferTokens[ttid])
	}

}

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

func (r *Rebalancer) processTransferToken(ttid string, tt *c.TransferToken) {

	if !r.addToWaitGroup() {
		return
	}

	defer r.wg.Done()

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

		if !r.addToWaitGroup() {
			return true
		}

		r.mu.Lock()
		r.sourceTokens[ttid] = tt
		r.mu.Unlock()

		//TODO batch this rather than one per index
		go r.dropIndexWhenIdle(ttid, tt)

	default:
		return false
	}
	return true

}

func (r *Rebalancer) dropIndexWhenIdle(ttid string, tt *c.TransferToken) {
	defer r.wg.Done()

loop:
	for {
		select {
		case <-r.cancel:
			l.Infof("Rebalancer::dropIndexWhenIdle Cancel Received")
			break loop
		case <-r.done:
			l.Infof("Rebalancer::dropIndexWhenIdle Done Received")
			break loop
		default:

			stats, err := getLocalStats(r.localaddr)
			if err != nil {
				l.Errorf("Rebalancer::dropIndexWhenIdle Error Fetching Local Stats %v %v", r.localaddr, err)
				break
			}

			statsMap := stats.ToMap()
			if statsMap == nil {
				l.Infof("Rebalancer::dropIndexWhenIdle Nil Stats. Retrying...")
				break
			}

			sname := fmt.Sprintf("%s:%s:", tt.IndexInst.Defn.Bucket, tt.IndexInst.DisplayName())
			sname_completed := sname + "num_completed_requests"
			sname_requests := sname + "num_requests"

			var num_completed, num_requests float64
			if _, ok := statsMap[sname_completed]; ok {
				num_completed = statsMap[sname_completed].(float64)
				num_requests = statsMap[sname_requests].(float64)
			} else {
				l.Infof("Rebalancer::dropIndexWhenIdle Missing Stats %v %v. Retrying...", sname_completed, sname_requests)
				break
			}

			pending := num_requests - num_completed

			if pending > 0 {
				l.Infof("Rebalancer::dropIndexWhenIdle Index %v:%v Pending Scan %v", tt.IndexInst.Defn.Bucket, tt.IndexInst.Defn.Name, pending)
				break
			}

			req := manager.IndexRequest{Index: tt.IndexInst.Defn}
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
				l.Errorf("Rebalancer::dropIndexWhenIdle Error dropping index %v %v", r.localaddr+url, response.Error)
				r.setTransferTokenError(ttid, tt, response.Error)
				return
			}

			tt.State = c.TransferTokenCommit
			r.setTransferTokenInMetakv(ttid, tt)

			r.mu.Lock()
			r.sourceTokens[ttid] = tt
			r.mu.Unlock()

			break loop
		}
		time.Sleep(5 * time.Second)
	}
}

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
		indexDefn.Nodes = nil
		indexDefn.Deferred = true
		indexDefn.InstId = tt.InstId

		ir := manager.IndexRequest{Index: indexDefn}
		body, err := json.Marshal(&ir)
		if err != nil {
			l.Errorf("Rebalancer::processTokenAsDest Error marshal clone index %v", err)
			r.setTransferTokenError(ttid, tt, err.Error())
			return true
		}

		bodybuf := bytes.NewBuffer(body)

		url := "/createIndexRebalance"
		resp, err := postWithAuth(r.localaddr+url, "application/json", bodybuf)
		if err != nil {
			l.Errorf("Rebalancer::processTokenAsDest Error register clone index on %v %v", r.localaddr+url, err)
			r.setTransferTokenError(ttid, tt, err.Error())
			return true
		}

		response := new(manager.IndexResponse)
		if err := convertResponse(resp, response); err != nil {
			l.Errorf("Rebalancer::processTokenAsDest Error unmarshal response %v %v", r.localaddr+url, err)
			r.setTransferTokenError(ttid, tt, err.Error())
			return true
		}
		if response.Code == manager.RESP_ERROR {
			l.Errorf("Rebalancer::processTokenAsDest Error cloning index %v %v", r.localaddr+url, response.Error)
			r.setTransferTokenError(ttid, tt, response.Error)
			return true
		}

		tt.State = c.TransferTokenAccepted
		r.setTransferTokenInMetakv(ttid, tt)

		r.mu.Lock()
		r.acceptedTokens[ttid] = tt
		r.mu.Unlock()

	case c.TransferTokenInitate:

		r.mu.Lock()
		defer r.mu.Unlock()
		att, ok := r.acceptedTokens[ttid]
		if !ok {
			l.Errorf("Rebalancer::processTokenAsDest Unknown TransferToken for Initiate %v %v", ttid, tt)
			r.setTransferTokenError(ttid, tt, "Unknown TransferToken For Initiate")
			return true
		}
		if tt.IndexInst.Defn.Deferred && tt.IndexInst.State == c.INDEX_STATE_READY {
			respch := make(chan error)
			r.supvMsgch <- &MsgUpdateIndexRState{defnId: tt.IndexInst.Defn.DefnId,
				rstate: c.REBAL_ACTIVE,
				respch: respch}
			err := <-respch
			c.CrashOnError(err)

			if tt.TransferMode == c.TokenTransferModeMove {
				tt.State = c.TransferTokenReady
				att.State = c.TransferTokenReady
			} else {
				tt.State = c.TransferTokenCommit
				att.State = c.TransferTokenCommit
			}
		} else {
			att.State = c.TransferTokenInProgress
			tt.State = c.TransferTokenInProgress
		}
		r.setTransferTokenInMetakv(ttid, tt)

		if r.checkIndexReadyToBuild() == true {
			if !r.addToWaitGroup() {
				return true
			}
			go r.buildAcceptedIndexes()
		}

	case c.TransferTokenInProgress:
		//Nothing to do

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
		if tt.State != c.TransferTokenInProgress && tt.State != c.TransferTokenReady &&
			tt.State != c.TransferTokenCommit {
			l.Infof("Rebalancer::checkIndexReadyToBuild Not ready to build %v %v", ttid, tt)
			return false
		}
	}
	return true

}

func (r *Rebalancer) buildAcceptedIndexes() {

	defer r.wg.Done()

	var idList client.IndexIdList
	var errStr string
	r.mu.Lock()
	for _, tt := range r.acceptedTokens {
		if tt.State != c.TransferTokenReady && tt.State != c.TransferTokenCommit {
			idList.DefnIds = append(idList.DefnIds, uint64(tt.IndexInst.Defn.DefnId))
		}
	}
	r.mu.Unlock()

	if len(idList.DefnIds) == 0 {
		l.Infof("Rebalancer::buildAcceptedIndexes Nothing to build")
		return
	}

	response := new(manager.IndexResponse)
	url := "/buildIndex"

	ir := manager.IndexRequest{IndexIds: idList}
	body, _ := json.Marshal(&ir)
	bodybuf := bytes.NewBuffer(body)

	resp, err := postWithAuth(r.localaddr+url, "application/json", bodybuf)
	if err != nil {
		l.Errorf("Rebalancer::buildAcceptedIndexes Error register clone index on %v %v", r.localaddr+url, err)
		errStr = err.Error()
		goto cleanup
	}

	if err := convertResponse(resp, response); err != nil {
		l.Errorf("Rebalancer::buildAcceptedIndexes Error unmarshal response %v %v", r.localaddr+url, err)
		errStr = err.Error()
		goto cleanup
	}
	if response.Code == manager.RESP_ERROR {
		l.Errorf("Rebalancer::buildAcceptedIndexes Error cloning index %v %v", r.localaddr+url, response.Error)
		errStr = response.Error
		goto cleanup
	}

	r.waitForIndexBuild()

	return

cleanup:
	r.mu.Lock()
	for ttid, tt := range r.acceptedTokens {
		r.setTransferTokenError(ttid, tt, errStr)
	}
	r.mu.Unlock()
	return

}

func (r *Rebalancer) waitForIndexBuild() {

	allTokensReady := true

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

			stats, err := getLocalStats(r.localaddr)
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

			url := "/getLocalIndexMetadata"
			resp, err := getWithAuth(r.localaddr + url)
			if err != nil {
				l.Errorf("Rebalancer::waitForIndexBuild Error getting local metadata %v %v", r.localaddr+url, err)
				break
			}

			defer resp.Body.Close()
			localMeta := new(manager.LocalIndexMetadata)
			bytes, _ := ioutil.ReadAll(resp.Body)
			if err := json.Unmarshal(bytes, &localMeta); err != nil {
				l.Errorf("Rebalancer::waitForIndexBuild Error unmarshal response %v %v", r.localaddr+url, err)
				return
			}

			r.mu.Lock()
			allTokensReady = true
			for ttid, tt := range r.acceptedTokens {
				if tt.State == c.TransferTokenReady || tt.State == c.TransferTokenCommit {
					continue
				}
				allTokensReady = false

				status, err := getIndexStatusFromMeta(&tt.IndexInst.Defn, localMeta)
				if err != "" {
					l.Errorf("Rebalancer::waitForIndexBuild Error Fetching Index Status %v %v", r.localaddr+url, err)
					break
				}
				sname := fmt.Sprintf("%s:%s:", tt.IndexInst.Defn.Bucket, tt.IndexInst.DisplayName())
				sname_pend := sname + "num_docs_pending"
				sname_queued := sname + "num_docs_queued"
				sname_processed := sname + "num_docs_processed"

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

				l.Infof("Rebalancer::waitForIndexBuild Index %s State %v Pending %v EstTime %v", sname,
					c.IndexState(status), tot_remaining, remainingBuildTime)

				if c.IndexState(status) == c.INDEX_STATE_ACTIVE && remainingBuildTime < maxRemainingBuildTime {
					respch := make(chan error)
					r.supvMsgch <- &MsgUpdateIndexRState{defnId: tt.IndexInst.Defn.DefnId,
						rstate: c.REBAL_ACTIVE,
						respch: respch}
					err := <-respch
					c.CrashOnError(err)
					if tt.TransferMode == c.TokenTransferModeMove {
						tt.State = c.TransferTokenReady
					} else {
						tt.State = c.TransferTokenCommit
					}
					r.setTransferTokenInMetakv(ttid, tt)
				}
			}
			r.mu.Unlock()

			if allTokensReady {
				break
			}

			time.Sleep(3 * time.Second)
		}

	}

	r.acceptedTokens = make(map[string]*c.TransferToken)

}

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
		tt.State = c.TransferTokenInitate
		r.setTransferTokenInMetakv(ttid, tt)

		if r.cb.progress != nil {
			go r.progressInitOnce.Do(r.updateProgress)
		}

	case c.TransferTokenRefused:
		//TODO replan

	case c.TransferTokenCommit:
		tt.State = c.TransferTokenDeleted
		r.setTransferTokenInMetakv(ttid, tt)

		r.updateMasterTokenState(ttid, c.TransferTokenCommit)

	case c.TransferTokenDeleted:
		err := MetakvDel(RebalanceMetakvDir + ttid)
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
	r.setTransferTokenInMetakv(ttid, tt)

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

func (r *Rebalancer) setTransferTokenInMetakv(ttid string, tt *c.TransferToken) {

	fn := func(r int, err error) error {
		if r > 0 {
			l.Warnf("Rebalancer::setTransferTokenInMetakv error=%v Retrying (%d)", err, r)
		}
		err = MetakvSet(RebalanceMetakvDir+ttid, tt)
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

func (r *Rebalancer) updateProgress() {

	if !r.addToWaitGroup() {
		return
	}

	defer r.wg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			progress := r.computeProgress()
			if progress > 0 {
				r.cb.progress(progress, r.cancel)
			}
		case <-r.cancel:
			l.Infof("Rebalancer::updateProgress Cancel Received")
			return
		case <-r.done:
			l.Infof("Rebalancer::updateProgress Done Received")
			return
		}
	}

}

func (r *Rebalancer) computeProgress() (progress float64) {

	url := "/getIndexStatus"
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
		if state == c.TransferTokenCommit || state == c.TransferTokenDeleted {
			totalProgress += 100.00
		} else {
			totalProgress += r.getBuildProgressFromStatus(statusResp, tt.IndexInst.Defn.DefnId)
		}
	}

	progress = (totalProgress / float64(totTokens)) / 100.0
	l.Infof("Rebalancer::computeProgress %v", progress)

	if progress < 0.1 {
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

func getIndexStatusFromMeta(defn *c.IndexDefn, localMeta *manager.LocalIndexMetadata) (c.IndexState, string) {

	topology := findTopologyByBucket(localMeta.IndexTopologies, defn.Bucket)
	if topology == nil {
		return c.INDEX_STATE_NIL, fmt.Sprintf("Topology Information Missing for %v Bucket", defn.Bucket)
	}

	return topology.GetStatusByDefn(defn.DefnId)
}

func (r *Rebalancer) getBuildProgressFromStatus(status *manager.IndexStatusResponse, defnId c.IndexDefnId) float64 {

	for _, idx := range status.Status {
		if idx.DefnId == defnId && idx.Status == "Replicating" {
			l.Infof("Rebalancer::getBuildProgressFromStatus %v %v", idx.DefnId, idx.Progress)
			r.lastKnownProgress[idx.DefnId] = idx.Progress
			return idx.Progress
		}
		if p, ok := r.lastKnownProgress[idx.DefnId]; ok {
			return p
		}
	}
	return 0.0

}

//
// This function gets the indexer stats for a specific indexer host.
//
func getLocalStats(addr string) (*c.Statistics, error) {

	resp, err := getWithAuth(addr + "/stats?async=true")
	if err != nil {
		return nil, err
	}

	stats := new(c.Statistics)
	if err := convertResponse(resp, stats); err != nil {
		return nil, err
	}

	return stats, nil
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
