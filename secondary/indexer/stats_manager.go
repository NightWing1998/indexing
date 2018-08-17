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
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/stats"
	"github.com/couchbase/indexing/secondary/stubs/nitro/mm"
)

var uptime time.Time
var num_cpu_core int

func init() {
	uptime = time.Now()
	num_cpu_core = runtime.NumCPU()
}

type BucketStats struct {
	bucket     string
	indexCount int

	numRollbacks       stats.Int64Val
	mutationQueueSize  stats.Int64Val
	numMutationsQueued stats.Int64Val

	tsQueueSize   stats.Int64Val
	numNonAlignTS stats.Int64Val
}

func (s *BucketStats) Init() {
	s.numRollbacks.Init()
	s.mutationQueueSize.Init()
	s.numMutationsQueued.Init()
	s.tsQueueSize.Init()
	s.numNonAlignTS.Init()
}

type IndexTimingStats struct {
	stCloneHandle           stats.TimingStat
	stNewIterator           stats.TimingStat
	stIteratorNext          stats.TimingStat
	stSnapshotCreate        stats.TimingStat
	stSnapshotClose         stats.TimingStat
	stPersistSnapshotCreate stats.TimingStat
	stScanPipelineIterate   stats.TimingStat
	stCommit                stats.TimingStat
	stKVGet                 stats.TimingStat
	stKVSet                 stats.TimingStat
	stKVDelete              stats.TimingStat
	stKVInfo                stats.TimingStat
	stKVMetaGet             stats.TimingStat
	stKVMetaSet             stats.TimingStat
	dcpSeqs                 stats.TimingStat
}

func (it *IndexTimingStats) Init() {
	it.stCloneHandle.Init()
	it.stCommit.Init()
	it.stNewIterator.Init()
	it.stSnapshotCreate.Init()
	it.stSnapshotClose.Init()
	it.stPersistSnapshotCreate.Init()
	it.stKVGet.Init()
	it.stKVSet.Init()
	it.stIteratorNext.Init()
	it.stScanPipelineIterate.Init()
	it.stKVDelete.Init()
	it.stKVInfo.Init()
	it.stKVMetaGet.Init()
	it.stKVMetaSet.Init()
	it.dcpSeqs.Init()
}

type IndexStats struct {
	name, bucket string
	replicaId    int

	scanDuration          stats.Int64Val
	scanReqDuration       stats.Int64Val
	scanReqInitDuration   stats.Int64Val
	scanReqAllocDuration  stats.Int64Val
	dcpSeqsDuration       stats.Int64Val
	insertBytes           stats.Int64Val
	numDocsPending        stats.Int64Val
	scanWaitDuration      stats.Int64Val
	numDocsIndexed        stats.Int64Val
	numDocsProcessed      stats.Int64Val
	numRequests           stats.Int64Val
	numCompletedRequests  stats.Int64Val
	numRowsReturned       stats.Int64Val
	diskSize              stats.Int64Val
	buildProgress         stats.Int64Val
	completionProgress    stats.Int64Val
	numDocsQueued         stats.Int64Val
	deleteBytes           stats.Int64Val
	dataSize              stats.Int64Val
	scanBytesRead         stats.Int64Val
	getBytes              stats.Int64Val
	itemsCount            stats.Int64Val
	numCommits            stats.Int64Val
	numSnapshots          stats.Int64Val
	numCompactions        stats.Int64Val
	numItemsFlushed       stats.Int64Val
	avgTsInterval         stats.Int64Val
	avgTsItemsCount       stats.Int64Val
	lastNumFlushQueued    stats.Int64Val
	lastTsTime            stats.Int64Val
	numDocsFlushQueued    stats.Int64Val
	fragPercent           stats.Int64Val
	sinceLastSnapshot     stats.Int64Val
	numSnapshotWaiters    stats.Int64Val
	numLastSnapshotReply  stats.Int64Val
	numItemsRestored      stats.Int64Val
	diskSnapStoreDuration stats.Int64Val
	diskSnapLoadDuration  stats.Int64Val
	notReadyError         stats.Int64Val
	clientCancelError     stats.Int64Val
	avgScanRate           stats.Int64Val
	avgMutationRate       stats.Int64Val
	avgDrainRate          stats.Int64Val
	lastScanGatherTime    stats.Int64Val
	lastNumRowsReturned   stats.Int64Val
	lastMutateGatherTime  stats.Int64Val
	lastNumDocsIndexed    stats.Int64Val
	lastNumItemsFlushed   stats.Int64Val
	lastRollbackTime      stats.TimeVal
	progressStatTime      stats.TimeVal
	residentPercent       stats.Int64Val
	cacheHitPercent       stats.Int64Val

	Timings IndexTimingStats
}

type IndexerStatsHolder struct {
	ptr unsafe.Pointer
}

func (h IndexerStatsHolder) Get() *IndexerStats {
	return (*IndexerStats)(atomic.LoadPointer(&h.ptr))
}

func (h *IndexerStatsHolder) Set(s *IndexerStats) {
	atomic.StorePointer(&h.ptr, unsafe.Pointer(s))
}

func (s *IndexStats) Init() {
	s.scanDuration.Init()
	s.scanReqDuration.Init()
	s.scanReqInitDuration.Init()
	s.scanReqAllocDuration.Init()
	s.insertBytes.Init()
	s.numDocsPending.Init()
	s.scanWaitDuration.Init()
	s.numDocsIndexed.Init()
	s.numDocsProcessed.Init()
	s.numRequests.Init()
	s.numCompletedRequests.Init()
	s.numRowsReturned.Init()
	s.diskSize.Init()
	s.buildProgress.Init()
	s.completionProgress.Init()
	s.numDocsQueued.Init()
	s.deleteBytes.Init()
	s.dataSize.Init()
	s.fragPercent.Init()
	s.scanBytesRead.Init()
	s.getBytes.Init()
	s.itemsCount.Init()
	s.avgTsInterval.Init()
	s.avgTsItemsCount.Init()
	s.lastNumFlushQueued.Init()
	s.lastTsTime.Init()
	s.numCommits.Init()
	s.numSnapshots.Init()
	s.numCompactions.Init()
	s.numItemsFlushed.Init()
	s.numDocsFlushQueued.Init()
	s.sinceLastSnapshot.Init()
	s.numSnapshotWaiters.Init()
	s.numLastSnapshotReply.Init()
	s.numItemsRestored.Init()
	s.diskSnapStoreDuration.Init()
	s.diskSnapLoadDuration.Init()
	s.notReadyError.Init()
	s.clientCancelError.Init()
	s.avgScanRate.Init()
	s.avgMutationRate.Init()
	s.avgDrainRate.Init()
	s.lastScanGatherTime.Init()
	s.lastNumRowsReturned.Init()
	s.lastMutateGatherTime.Init()
	s.lastNumDocsIndexed.Init()
	s.lastNumItemsFlushed.Init()
	s.lastRollbackTime.Init()
	s.progressStatTime.Init()
	s.residentPercent.Init()
	s.cacheHitPercent.Init()

	s.Timings.Init()
}

type IndexerStats struct {
	indexes map[common.IndexInstId]*IndexStats
	buckets map[string]*BucketStats

	numConnections    stats.Int64Val
	memoryQuota       stats.Int64Val
	memoryUsed        stats.Int64Val
	memoryUsedStorage stats.Int64Val
	memoryUsedQueue   stats.Int64Val
	needsRestart      stats.BoolVal
	statsResponse     stats.TimingStat
	notFoundError     stats.Int64Val

	indexerState stats.Int64Val
}

func (s *IndexerStats) Init() {
	s.indexes = make(map[common.IndexInstId]*IndexStats)
	s.buckets = make(map[string]*BucketStats)
	s.numConnections.Init()
	s.memoryQuota.Init()
	s.memoryUsed.Init()
	s.memoryUsedStorage.Init()
	s.memoryUsedQueue.Init()
	s.needsRestart.Init()
	s.statsResponse.Init()
	s.indexerState.Init()
	s.notFoundError.Init()
}

func (s *IndexerStats) Reset() {
	old := *s
	*s = IndexerStats{}
	s.Init()
	for k, v := range old.indexes {
		s.AddIndex(k, v.bucket, v.name, v.replicaId)
	}
}

func (s *IndexerStats) AddIndex(id common.IndexInstId, bucket string, name string, replicaId int) {

	idxStats := &IndexStats{name: name, bucket: bucket, replicaId: replicaId}
	idxStats.Init()
	s.indexes[id] = idxStats

	b, ok := s.buckets[bucket]
	if !ok {
		b = &BucketStats{bucket: bucket}
		b.Init()
		s.buckets[bucket] = b
	}

	b.indexCount++
}

func (s *IndexerStats) RemoveIndex(id common.IndexInstId) {
	idx, ok := s.indexes[id]
	if !ok {
		return
	}
	delete(s.indexes, id)
	b := s.buckets[idx.bucket]
	b.indexCount--
	if b.indexCount == 0 {
		delete(s.buckets, idx.bucket)
	}
}

func (is IndexerStats) GetStats() common.Statistics {

	var prefix string

	statsMap := make(map[string]interface{})
	addStat := func(k string, v interface{}) {
		statsMap[fmt.Sprintf("%s%s", prefix, k)] = v
	}

	addStat("uptime", fmt.Sprintf("%s", time.Since(uptime)))
	addStat("num_connections", is.numConnections.Value())
	addStat("index_not_found_errcount", is.notFoundError.Value())
	addStat("memory_quota", is.memoryQuota.Value())
	addStat("memory_used", is.memoryUsed.Value())
	addStat("memory_used_storage", is.memoryUsedStorage.Value())
	addStat("memory_used_queue", is.memoryUsedQueue.Value())
	addStat("needs_restart", is.needsRestart.Value())
	storageMode := fmt.Sprintf("%s", common.GetStorageMode())
	addStat("storage_mode", storageMode)
	addStat("num_cpu_core", num_cpu_core)
	addStat("cpu_utilization", getCpuPercent())

	indexerState := common.IndexerState(is.indexerState.Value())
	if indexerState == common.INDEXER_PREPARE_UNPAUSE {
		indexerState = common.INDEXER_PAUSED
	}
	addStat("indexer_state", fmt.Sprintf("%s", indexerState))

	addStat("timings/stats_response", is.statsResponse.Value())

	for _, s := range is.indexes {
		var scanLat, waitLat, scanReqLat, scanReqInitLat, scanReqAllocLat int64
		reqs := s.numRequests.Value()

		if reqs > 0 {
			scanDur := s.scanDuration.Value()
			waitDur := s.scanWaitDuration.Value()
			scanReqDur := s.scanReqDuration.Value()
			scanReqInitDur := s.scanReqInitDuration.Value()
			scanReqAllocDur := s.scanReqAllocDuration.Value()
			scanLat = scanDur / reqs
			waitLat = waitDur / reqs
			scanReqLat = scanReqDur / reqs
			scanReqInitLat = scanReqInitDur / reqs
			scanReqAllocLat = scanReqAllocDur / reqs
		}

		name := common.FormatIndexInstDisplayName(s.name, s.replicaId)
		prefix = fmt.Sprintf("%s:%s:", s.bucket, name)

		addStat("total_scan_duration", s.scanDuration.Value())
		addStat("total_scan_request_duration", s.scanReqDuration.Value())
		addStat("insert_bytes", s.insertBytes.Value())
		addStat("num_docs_pending", s.numDocsPending.Value())
		addStat("scan_wait_duration", s.scanWaitDuration.Value())
		addStat("num_docs_indexed", s.numDocsIndexed.Value())
		addStat("num_docs_processed", s.numDocsProcessed.Value())
		addStat("num_requests", s.numRequests.Value())
		addStat("num_completed_requests", s.numCompletedRequests.Value())
		addStat("num_rows_returned", s.numRowsReturned.Value())
		addStat("disk_size", s.diskSize.Value())
		addStat("build_progress", s.buildProgress.Value())
		addStat("completion_progress", s.completionProgress.Value())
		addStat("num_docs_queued", s.numDocsQueued.Value())
		addStat("delete_bytes", s.deleteBytes.Value())
		addStat("data_size", s.dataSize.Value())
		addStat("frag_percent", s.fragPercent.Value())
		addStat("scan_bytes_read", s.scanBytesRead.Value())
		addStat("get_bytes", s.getBytes.Value())
		addStat("items_count", s.itemsCount.Value())
		addStat("avg_ts_interval", s.avgTsInterval.Value())
		addStat("avg_ts_items_count", s.avgTsItemsCount.Value())
		addStat("num_commits", s.numCommits.Value())
		addStat("num_snapshots", s.numSnapshots.Value())
		addStat("num_compactions", s.numCompactions.Value())
		addStat("flush_queue_size", postiveNum(s.numDocsFlushQueued.Value()-s.numDocsIndexed.Value()))
		addStat("num_items_flushed", s.numItemsFlushed.Value())
		addStat("avg_scan_latency", scanLat)
		addStat("avg_scan_wait_latency", waitLat)
		addStat("avg_scan_request_latency", scanReqLat)
		addStat("avg_scan_request_init_latency", scanReqInitLat)
		addStat("avg_scan_request_alloc_latency", scanReqAllocLat)
		addStat("num_flush_queued", s.numDocsFlushQueued.Value())
		addStat("since_last_snapshot", s.sinceLastSnapshot.Value())
		addStat("num_snapshot_waiters", s.numSnapshotWaiters.Value())
		addStat("num_last_snapshot_reply", s.numLastSnapshotReply.Value())
		addStat("num_items_restored", s.numItemsRestored.Value())
		addStat("disk_store_duration", s.diskSnapStoreDuration.Value())
		addStat("disk_load_duration", s.diskSnapLoadDuration.Value())
		addStat("not_ready_errcount", s.notReadyError.Value())
		addStat("client_cancel_errcount", s.clientCancelError.Value())
		addStat("avg_scan_rate", s.avgScanRate.Value())
		addStat("avg_mutation_rate", s.avgMutationRate.Value())
		addStat("avg_drain_rate", s.avgDrainRate.Value())
		addStat("last_rollback_time", s.lastRollbackTime.Value())
		addStat("progress_stat_time", s.progressStatTime.Value())
		addStat("resident_percent", s.residentPercent.Value())
		addStat("cache_hit_percent", s.cacheHitPercent.Value())

		addStat("timings/dcp_getseqs", s.Timings.dcpSeqs.Value())
		addStat("timings/storage_clone_handle", s.Timings.stCloneHandle.Value())
		addStat("timings/storage_commit", s.Timings.stCommit.Value())
		addStat("timings/storage_new_iterator", s.Timings.stNewIterator.Value())
		addStat("timings/storage_snapshot_create", s.Timings.stSnapshotCreate.Value())
		addStat("timings/storage_snapshot_close", s.Timings.stSnapshotClose.Value())
		addStat("timings/storage_persist_snapshot_create", s.Timings.stPersistSnapshotCreate.Value())
		addStat("timings/storage_get", s.Timings.stKVGet.Value())
		addStat("timings/storage_set", s.Timings.stKVSet.Value())
		addStat("timings/storage_iterator_next", s.Timings.stIteratorNext.Value())
		addStat("timings/scan_pipeline_iterate", s.Timings.stScanPipelineIterate.Value())
		addStat("timings/storage_del", s.Timings.stKVDelete.Value())
		addStat("timings/storage_info", s.Timings.stKVInfo.Value())
		addStat("timings/storage_meta_get", s.Timings.stKVMetaGet.Value())
		addStat("timings/storage_meta_set", s.Timings.stKVMetaSet.Value())
	}

	for _, s := range is.buckets {
		prefix = fmt.Sprintf("%s:", s.bucket)
		addStat("num_rollbacks", s.numRollbacks.Value())
		addStat("mutation_queue_size", s.mutationQueueSize.Value())
		addStat("num_mutations_queued", s.numMutationsQueued.Value())
		addStat("ts_queue_size", s.tsQueueSize.Value())
		addStat("num_nonalign_ts", s.numNonAlignTS.Value())
		if st := common.BucketSeqsTiming(s.bucket); st != nil {
			addStat("timings/dcp_getseqs", st.Value())
		}
	}

	return statsMap
}

func (is IndexerStats) MarshalJSON() ([]byte, error) {
	return json.Marshal(is.GetStats())
}

func (s IndexerStats) Clone() *IndexerStats {
	var clone IndexerStats
	clone = s
	clone.indexes = make(map[common.IndexInstId]*IndexStats)
	clone.buckets = make(map[string]*BucketStats)
	for k, v := range s.indexes {
		clone.indexes[k] = v
	}
	for k, v := range s.buckets {
		clone.buckets[k] = v
	}

	return &clone
}

func NewIndexerStats() *IndexerStats {
	s := &IndexerStats{}
	s.Init()
	return s
}

type statsManager struct {
	sync.Mutex
	config                common.ConfigHolder
	stats                 IndexerStatsHolder
	supvCmdch             MsgChannel
	supvMsgch             MsgChannel
	lastStatTime          time.Time
	cacheUpdateInProgress bool
	statsLogDumpInterval  uint64
}

func NewStatsManager(supvCmdch MsgChannel,
	supvMsgch MsgChannel, config common.Config) (*statsManager, Message) {
	s := &statsManager{
		supvCmdch:            supvCmdch,
		supvMsgch:            supvMsgch,
		lastStatTime:         time.Unix(0, 0),
		statsLogDumpInterval: config["settings.statsLogDumpInterval"].Uint64(),
	}

	s.config.Store(config)

	go s.run()
	go s.runStatsDumpLogger()
	StartCpuCollector()
	return s, &MsgSuccess{}
}

func (s *statsManager) RegisterRestEndpoints() {
	mux := GetHTTPMux()
	mux.HandleFunc("/stats", s.handleStatsReq)
	mux.HandleFunc("/stats/mem", s.handleMemStatsReq)
	mux.HandleFunc("/stats/storage/mm", s.handleStorageMMStatsReq)
	mux.HandleFunc("/stats/storage", s.handleStorageStatsReq)
	mux.HandleFunc("/stats/reset", s.handleStatsResetReq)
}

func (s *statsManager) tryUpdateStats(sync bool) {
	waitCh := make(chan struct{})
	conf := s.config.Load()
	timeout := time.Millisecond * time.Duration(conf["stats_cache_timeout"].Uint64())

	s.Lock()
	cacheTime := s.lastStatTime
	shouldUpdate := !s.cacheUpdateInProgress

	if s.lastStatTime.Unix() == 0 {
		sync = true
	}

	// Refresh cache if cache ttl has expired
	if shouldUpdate && time.Now().Sub(cacheTime) > timeout || sync {
		s.cacheUpdateInProgress = true
		s.Unlock()

		go func() {

			stats_list := []MsgType{STORAGE_STATS, SCAN_STATS, INDEX_PROGRESS_STATS, INDEXER_STATS}
			for _, t := range stats_list {
				ch := make(chan bool)
				msg := &MsgStatsRequest{
					mType:    t,
					respch:   ch,
					fetchDcp: true,
				}

				s.supvMsgch <- msg
				<-ch
			}

			s.supvMsgch <- &MsgStatsRequest{mType: INDEX_STATS_DONE, respch: nil}

			s.Lock()
			s.lastStatTime = time.Now()
			s.cacheUpdateInProgress = false
			s.Unlock()
			close(waitCh)
		}()

		if sync {
			<-waitCh
		}
	} else {
		s.Unlock()
	}
}

func (s *statsManager) handleStatsReq(w http.ResponseWriter, r *http.Request) {
	_, valid, _ := common.IsAuthValid(r)
	if !valid {
		w.WriteHeader(401)
		w.Write([]byte("401 Unauthorized"))
		return
	}

	sync := false
	if r.Method == "POST" || r.Method == "GET" {
		if r.URL.Query().Get("async") == "false" {
			sync = true
		}
		stats := s.stats.Get()

		t0 := time.Now()
		if common.IndexerState(stats.indexerState.Value()) != common.INDEXER_BOOTSTRAP {
			s.tryUpdateStats(sync)
		}
		bytes, _ := stats.MarshalJSON()
		w.WriteHeader(200)
		w.Write(bytes)
		stats.statsResponse.Put(time.Since(t0))
	} else {
		w.WriteHeader(400)
		w.Write([]byte("Unsupported method"))
	}
}

func (s *statsManager) handleMemStatsReq(w http.ResponseWriter, r *http.Request) {
	_, valid, _ := common.IsAuthValid(r)
	if !valid {
		w.WriteHeader(401)
		w.Write([]byte("401 Unauthorized"))
		return
	}

	stats := new(runtime.MemStats)
	if r.Method == "POST" || r.Method == "GET" {
		runtime.ReadMemStats(stats)
		bytes, _ := json.Marshal(stats)
		w.WriteHeader(200)
		w.Write(bytes)
	} else {
		w.WriteHeader(400)
		w.Write([]byte("Unsupported method"))
	}
}

func (s *statsManager) getStorageStats() string {
	var result string
	replych := make(chan []IndexStorageStats)
	statReq := &MsgIndexStorageStats{respch: replych}
	s.supvMsgch <- statReq
	res := <-replych

	result += "[\n"
	for i, sts := range res {
		if i > 0 {
			result += ","
		}
		result += fmt.Sprintf("{\n\"Index\": \"%s:%s\", \"Id\": %d,\n", sts.Bucket, sts.Name, sts.InstId)
		result += fmt.Sprintf("\"Stats\":\n")
		for _, data := range sts.GetInternalData() {
			result += data
		}
		result += "}\n"
	}

	result += "]"

	return result
}

func (s *statsManager) handleStorageStatsReq(w http.ResponseWriter, r *http.Request) {
	_, valid, _ := common.IsAuthValid(r)
	if !valid {
		w.WriteHeader(401)
		w.Write([]byte("401 Unauthorized"))
		return
	}

	if r.Method == "POST" || r.Method == "GET" {

		w.WriteHeader(200)
		w.Write([]byte(s.getStorageStats()))

	} else {
		w.WriteHeader(400)
		w.Write([]byte("Unsupported method"))
	}
}

func (s *statsManager) handleStorageMMStatsReq(w http.ResponseWriter, r *http.Request) {
	_, valid, _ := common.IsAuthValid(r)
	if !valid {
		w.WriteHeader(401)
		w.Write([]byte("401 Unauthorized"))
		return
	}

	if r.Method == "POST" || r.Method == "GET" {

		w.WriteHeader(200)
		w.Write([]byte(mm.Stats()))

	} else {
		w.WriteHeader(400)
		w.Write([]byte("Unsupported method"))
	}
}

func (s *statsManager) handleStatsResetReq(w http.ResponseWriter, r *http.Request) {
	_, valid, _ := common.IsAuthValid(r)
	if !valid {
		w.WriteHeader(401)
		w.Write([]byte("401 Unauthorized"))
		return
	}

	if r.Method == "POST" || r.Method == "GET" {
		s.supvMsgch <- &MsgResetStats{}
		w.WriteHeader(200)
		w.Write([]byte("OK"))
	} else {
		w.WriteHeader(400)
		w.Write([]byte("Unsupported method"))
	}
}

func (s *statsManager) run() {
loop:
	for {
		select {
		case cmd, ok := <-s.supvCmdch:
			if ok {
				switch cmd.GetMsgType() {
				case STORAGE_MGR_SHUTDOWN:
					logging.Infof("SettingsManager::run Shutting Down")
					s.supvCmdch <- &MsgSuccess{}
					break loop
				case UPDATE_INDEX_INSTANCE_MAP:
					s.handleIndexInstanceUpdate(cmd)
				case CONFIG_SETTINGS_UPDATE:
					s.handleConfigUpdate(cmd)
				}
			} else {
				break loop
			}
		}
	}
}

func (s *statsManager) handleIndexInstanceUpdate(cmd Message) {
	req := cmd.(*MsgUpdateInstMap)
	s.stats.Set(req.GetStatsObject())
	s.supvCmdch <- &MsgSuccess{}
}

func (s *statsManager) handleConfigUpdate(cmd Message) {
	cfg := cmd.(*MsgConfigUpdate)
	s.config.Store(cfg.GetConfig())
	atomic.StoreUint64(&s.statsLogDumpInterval, cfg.GetConfig()["settings.statsLogDumpInterval"].Uint64())
	s.supvCmdch <- &MsgSuccess{}
}

func (s *statsManager) runStatsDumpLogger() {
	for {
		stats := s.stats.Get()
		if stats != nil {
			bytes, _ := stats.MarshalJSON()
			var storageStats string
			if logging.IsEnabled(logging.Debug) {
				storageStats = fmt.Sprintf("\n==== StorageStats ====\n%s", s.getStorageStats())
			} else if common.GetStorageMode() == common.FORESTDB && logging.IsEnabled(logging.Timing) {
				storageStats = fmt.Sprintf("\n==== StorageStats ====\n%s", s.getStorageStats())
			}
			logging.Infof("PeriodicStats = %s%s", string(bytes), storageStats)
		}

		time.Sleep(time.Second * time.Duration(atomic.LoadUint64(&s.statsLogDumpInterval)))
	}
}

func postiveNum(n int64) int64 {
	if n < 0 {
		return 0
	}

	return n
}
