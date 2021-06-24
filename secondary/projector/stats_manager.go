package projector

import (
	"fmt"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/dataport"
	memcached "github.com/couchbase/indexing/secondary/dcp/transport/client"
	"github.com/couchbase/indexing/secondary/logging"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/projector"
)

const (
	UPDATE_STATS_MAP byte = iota + 1
	STATS_LOG_INTERVAL_UPDATE
	VBSEQNOS_LOG_INTERVAL_UPDATE
)

type BucketStats struct {
	bucket string
	topic  string
	opaque uint16

	// Key -> Log prefix of DCP Feed
	// Value -> Pointer to stats object of DCP feed
	dcpStats map[string]interface{}

	// Key -> Log prefix of KVData
	// Value -> Pointer to stats object KVData
	kvstats map[string]interface{}

	// Key -> Log prefix for Workers
	// Value -> Slice containing pointer to stats object of all workers
	wrkrStats map[string][]interface{}

	// Key -> <bucketname>:<indexname>
	// Value -> Pointer to stats object for index evaluator
	evaluatorStats map[string]interface{}
}

func (bs *BucketStats) Init() {
	bs.dcpStats = make(map[string]interface{}, 0)
	bs.kvstats = make(map[string]interface{}, 0)
	bs.wrkrStats = make(map[string][]interface{}, 0)
	bs.evaluatorStats = make(map[string]interface{})
}

func (bs *BucketStats) clone() *BucketStats {
	cbs := &BucketStats{}
	cbs.Init()
	cbs.topic = bs.topic
	cbs.bucket = bs.bucket
	cbs.opaque = bs.opaque
	for key, value := range bs.dcpStats {
		if value != nil {
			cbs.dcpStats[key] = value
		}
	}

	for key, value := range bs.kvstats {
		if value != nil {
			cbs.kvstats[key] = value
		}
	}

	for key, value := range bs.wrkrStats {
		if value != nil {
			wrkrstat := make([]interface{}, 0)
			for _, stat := range value {
				wrkrstat = append(wrkrstat, stat)
			}
			cbs.wrkrStats[key] = wrkrstat
		}
	}

	for key, value := range bs.evaluatorStats {
		if value != nil {
			cbs.evaluatorStats[key] = value
		}
	}
	return cbs
}

type FeedStats struct {
	bucketStats map[string]*BucketStats
	endpStats   map[string]interface{}

	// For other topic level stats
}

func (fs *FeedStats) Init() {
	fs.bucketStats = make(map[string]*BucketStats, 0)
	fs.endpStats = make(map[string]interface{}, 0)
}

func (fs *FeedStats) clone() *FeedStats {
	cfs := &FeedStats{}
	cfs.Init()
	for key, value := range fs.bucketStats {
		if value != nil {
			cfs.bucketStats[key] = value.clone()
		}
	}

	for key, value := range fs.endpStats {
		if value != nil {
			cfs.endpStats[key] = value
		}
	}

	return cfs
}

type ProjectorStats struct {
	feedStats map[string]*FeedStats

	statsLogTime        int64 // Time when component stats were logged
	lastVbseqnosLogTime int64 // Time when last vbseqnos were logged
	evalStatsLogTime    int64 // Time when last eval stats were logged
}

func NewProjectorStats() *ProjectorStats {
	ps := &ProjectorStats{}
	ps.Init()
	ps.lastVbseqnosLogTime = time.Now().UnixNano()
	ps.evalStatsLogTime = time.Now().UnixNano()
	ps.statsLogTime = time.Now().UnixNano()
	return ps
}

func (ps *ProjectorStats) Init() {
	ps.feedStats = make(map[string]*FeedStats, 0)
}

func (ps *ProjectorStats) Clone() *ProjectorStats {
	cps := &ProjectorStats{}
	cps.Init()
	for topic, feedStat := range ps.feedStats {
		clonedFeedStat := feedStat.clone()
		cps.feedStats[topic] = clonedFeedStat
	}

	cps.lastVbseqnosLogTime = ps.lastVbseqnosLogTime
	cps.evalStatsLogTime = ps.evalStatsLogTime
	cps.statsLogTime = ps.statsLogTime
	return cps
}

type ProjectorStatsHolder struct {
	ptr unsafe.Pointer
}

func (p *ProjectorStatsHolder) Get() *ProjectorStats {
	return (*ProjectorStats)(atomic.LoadPointer(&p.ptr))
}

func (p *ProjectorStatsHolder) Set(s *ProjectorStats) {
	atomic.StorePointer(&p.ptr, unsafe.Pointer(s))
}

type statsManager struct {
	stats                ProjectorStatsHolder
	cmdCh                chan []interface{}
	stopCh               chan bool
	stopLogger           int32
	statsLogDumpInterval int64
	vbseqnosLogInterval  int64
	evalStatsLogInterval int64
	lastStatTime         time.Time
	config               common.ConfigHolder
}

func NewStatsManager(cmdCh chan []interface{}, stopCh chan bool, config common.Config) *statsManager {
	sm := &statsManager{
		cmdCh:        cmdCh,
		stopCh:       stopCh,
		lastStatTime: time.Unix(0, 0),
	}

	if val, ok := config["projector.statsLogDumpInterval"]; ok {
		atomic.StoreInt64(&sm.statsLogDumpInterval, int64(val.Int()))
	} else {
		// Use default value of 60 seconds
		atomic.StoreInt64(&sm.statsLogDumpInterval, int64(60))
	}

	si := atomic.LoadInt64(&sm.statsLogDumpInterval)
	if val, ok := config["projector.vbseqnosLogIntervalMultiplier"]; ok {
		atomic.StoreInt64(&sm.vbseqnosLogInterval, int64(val.Int())*si)
	} else {
		// Use default value of 5 * statsLogDumpInterval seconds
		atomic.StoreInt64(&sm.vbseqnosLogInterval, 5*si)
	}

	// Use default value of 300 seconds
	atomic.StoreInt64(&sm.evalStatsLogInterval, int64(300))

	logging.Infof("StatsManager: Stats logging interval set to: %v seconds", atomic.LoadInt64(&sm.statsLogDumpInterval))
	logging.Infof("StatsManager: vbseqnos logging interval set to: %v seconds", atomic.LoadInt64(&sm.vbseqnosLogInterval))
	logging.Infof("StatsManager: eval stats logging interval set to: %v seconds", atomic.LoadInt64(&sm.evalStatsLogInterval))

	sm.config.Store(config)
	go sm.run()
	go sm.logger()
	return sm
}

func (sm *statsManager) run() {
	for {
		select {
		case msg := <-sm.cmdCh:
			switch msg[0].(byte) {
			case UPDATE_STATS_MAP:
				ps := msg[1].(*ProjectorStats)
				sm.stats.Set(ps)
			case STATS_LOG_INTERVAL_UPDATE:
				val := msg[1].(int)
				atomic.StoreInt64(&sm.statsLogDumpInterval, int64(val))
			case VBSEQNOS_LOG_INTERVAL_UPDATE:
				val := msg[1].(int)
				atomic.StoreInt64(&sm.vbseqnosLogInterval, int64(val)*atomic.LoadInt64(&sm.statsLogDumpInterval))
			}
		case <-sm.stopCh:
			atomic.StoreInt32(&sm.stopLogger, 1)
			return
		}
	}
}

func (sm *statsManager) logger() {
	for {
		//Get the projector stats
		ps := sm.stats.Get()
		if ps != nil {
			now := time.Now().UnixNano()
			var logStats, logVbsenos, logEvalStats bool

			// Check if projector component stats should be logged in this iteration
			if (now - ps.statsLogTime) > atomic.LoadInt64(&sm.statsLogDumpInterval)*1e9 {
				logStats = true
				ps.statsLogTime = now
			}
			// Check if vbseqnos should be logged in this iteration
			if (now - ps.lastVbseqnosLogTime) > atomic.LoadInt64(&sm.vbseqnosLogInterval)*1e9 {
				logVbsenos = true
				ps.lastVbseqnosLogTime = now
			}
			// Check if eval stats should be logged in this iteration
			if (now - ps.evalStatsLogTime) > atomic.LoadInt64(&sm.evalStatsLogInterval)*1e9 {
				logEvalStats = true
				ps.evalStatsLogTime = now
			}

			// For each topic
			for _, feedStats := range ps.feedStats {
				// For each bucket
				for _, bucketStats := range feedStats.bucketStats {
					kvdataClosed := false
					if logStats {
						for key, value := range bucketStats.dcpStats {
							switch value.(type) {
							case *memcached.DcpStats:
								val := value.(*memcached.DcpStats)
								if !val.IsClosed() {
									stats, latency := val.String()
									logging.Infof("%v dcp latency stats %v", key, latency)
									logging.Infof("%v stats: %v", key, stats)
								} else {
									logging.Tracef("%v closed", key)
								}
							default:
								logging.Errorf("Unknown Dcp stats type for %v", key)
								continue
							}
						}

						for key, value := range bucketStats.kvstats {
							switch (value).(type) {
							case *KvdataStats:
								val := (value).(*KvdataStats)
								if !val.IsClosed() {
									stats, _ := val.String()
									logging.Infof("%v stats: %v", key, stats)
								} else {
									kvdataClosed = true
									logging.Tracef("%v closed", key)
								}
							default:
								logging.Errorf("Unknown Kvdata stats type for %v", key)
								continue
							}
						}

						for key, value := range bucketStats.wrkrStats {
							// Get the type of any worker
							switch (value[0]).(type) {
							case *WorkerStats:
								val := (value[0]).(*WorkerStats)
								if !val.IsClosed() {
									logging.Infof("%v stats: %v", key, Accmulate(value))
								}
							default:
								logging.Errorf("Unknown worker stats type for %v", key)
								continue
							}
						}
					}

					// Log vbseqno's for every "vbseqnosLogInterval" seconds
					if logVbsenos {
						for key, value := range bucketStats.kvstats {
							switch (value).(type) {
							case *KvdataStats:
								val := (value).(*KvdataStats)
								if !val.IsClosed() {
									_, vbseqnos := val.String()
									logging.Infof("%v vbseqnos: [%v]", key, vbseqnos)
								} else {
									logging.Tracef("%v closed", key)
								}
							default:
								logging.Errorf("Unknown Kvdata stats type for %v", key)
								continue
							}
						}
					}

					// Log eval stats for every evalStatsLogInterval
					if logEvalStats && !kvdataClosed {
						// As of this commit, only IndexEvaluatorStats are supported
						logPrefix := fmt.Sprintf("EVAL[%v #%v] ##%x ", bucketStats.bucket, bucketStats.topic, bucketStats.opaque)
						var evalStats string
						var skippedStr string

						for key, value := range bucketStats.evaluatorStats {
							switch (value).(type) {
							case *protobuf.IndexEvaluatorStats:
								keyStr := fmt.Sprintf("%v", key)
								avg := value.(*protobuf.IndexEvaluatorStats).MovingAvg()
								evalStats += fmt.Sprintf("\"%v\":%v,", keyStr+":avgLatency", avg)
								errSkip := value.(*protobuf.IndexEvaluatorStats).GetAndResetErrorSkip()
								errSkipAll := value.(*protobuf.IndexEvaluatorStats).GetErrorSkipAll()
								evalStats += fmt.Sprintf("\"%v\":%v,", keyStr+":skipCount", errSkipAll)
								if errSkip != 0 {
									if len(skippedStr) == 0 {
										skippedStr = fmt.Sprintf("In last %v, projector skipped "+
											"evaluating some documents due to errors. Please see the "+
											"projector.log for details. Skipped document counts for "+
											"following indexes are:\n",
											time.Duration(atomic.LoadInt64(&sm.evalStatsLogInterval)*1e9))
									}
									skippedStr += fmt.Sprintf("\"%v\":%v,", key, errSkip)
								}
							default:
								logging.Errorf("%v Unknown type for evaluator stats", logPrefix)
								continue
							}
						}
						if len(evalStats) > 0 {
							logging.Infof("%v stats: {%v}", logPrefix, evalStats[0:len(evalStats)-1])
						}
						if len(skippedStr) != 0 {
							// Some mutations were skipped by the index evaluator.
							// Report it in the console logs.
							skippedStr = skippedStr[0 : len(skippedStr)-1]
							cfg := sm.config.Load()
							clusterAddr, ok := cfg["projector.clusterAddr"]
							if ok {
								common.Console(clusterAddr.String(), skippedStr)
							}
						}
					}
				}

				if logStats {
					// Log the endpoint stats for this feed
					for key, value := range feedStats.endpStats {
						switch value.(type) {
						case *dataport.EndpointStats:
							val := value.(*dataport.EndpointStats)
							if !val.IsClosed() {
								logging.Infof("%v stats: %v", key, val.String())
							} else {
								logging.Tracef("%v closed", key)
							}
						default:
							logging.Errorf("Unknown Endpoint stats type for %v", key)
							continue
						}
					}
				}
			}
		}
		if atomic.LoadInt32(&sm.stopLogger) == 1 {
			return
		}
		time.Sleep(time.Second * time.Duration(atomic.LoadInt64(&sm.statsLogDumpInterval)))
	}
}

func Accmulate(wrkr []interface{}) string {
	var dataChLen, outgoingMut uint64
	for _, stats := range wrkr {
		wrkrStat := stats.(*WorkerStats)
		dataChLen += (uint64)(len(wrkrStat.datach))
		outgoingMut += wrkrStat.outgoingMut.Value()
	}
	return fmt.Sprintf(
		"{\"datachLen\":%v,\"outgoingMut\":%v}", dataChLen, outgoingMut)
}
