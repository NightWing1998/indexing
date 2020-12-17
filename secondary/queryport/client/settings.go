// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package client

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/planner"
)

type ClientSettings struct {
	numReplica     int32
	numPartition   int32
	backfillLimit  int32
	scanLagPercent uint64
	scanLagItem    uint64
	prune_replica  int32
	queueSize      uint64
	concurrency    uint32
	usePlanner     uint32
	config         common.Config
	cancelCh       chan struct{}

	storageMode string
	mutex       sync.RWMutex

	needRefresh          bool
	allowCJsonScanFormat uint32
	allowPartialQuorum   uint32
}

func NewClientSettings(needRefresh bool) *ClientSettings {

	s := &ClientSettings{
		config:               nil,
		cancelCh:             make(chan struct{}, 1),
		needRefresh:          needRefresh,
		allowCJsonScanFormat: 1, // Initialize to default config value
	}

	if needRefresh {
		config, err := common.GetSettingsConfig(common.SystemConfig)
		if err != nil {
			logging.Errorf("ClientSettings: Fail to initialize metakv for reading latest indexer setting (%v).  Will use default indexer setting.", err)
		} else {
			s.config = config
		}
	}

	if s.config == nil {
		s.config = common.SystemConfig.Clone()
	}

	if needRefresh {
		go func() {
			fn := func(r int, err error) error {
				if r > 0 {
					logging.Errorf("ClientSettings: metakv notifier failed (%v)..Restarting %v", err, r)
				}
				err = metakv.RunObserveChildren(common.IndexingSettingsMetaDir, s.metaKVCallback, s.cancelCh)
				return err
			}
			rh := common.NewRetryHelper(200, time.Second, 2, fn)
			err := rh.Run()
			if err != nil {
				logging.Errorf("ClientSettings: metakv notifier failed even after max retries.")
			}
		}()
	}

	s.handleSettings(s.config)

	return s
}

func (s *ClientSettings) Close() {

	close(s.cancelCh)
}

func (s *ClientSettings) metaKVCallback(path string, value []byte, rev interface{}) error {

	if path == common.IndexingSettingsMetaPath {
		logging.Infof("New settings received: \n%s", string(value))

		config := s.config.Clone()
		config.Update(value)
		s.config = config

		s.handleSettings(s.config)
	}

	return nil
}

func (s *ClientSettings) handleSettings(config common.Config) {

	numReplica := int32(config["indexer.settings.num_replica"].Int())
	if numReplica >= 0 {
		atomic.StoreInt32(&s.numReplica, numReplica)
	} else {
		logging.Errorf("ClientSettings: invalid setting value for num_replica=%v", numReplica)
	}

	numPartition := int32(config["indexer.numPartitions"].Int())
	if numPartition > 0 {
		atomic.StoreInt32(&s.numPartition, numPartition)
	} else {
		logging.Errorf("ClientSettings: invalid setting value for numPartitions=%v", numPartition)
	}

	backfillLimit := int32(config["queryport.client.settings.backfillLimit"].Int())
	if backfillLimit >= 0 {
		atomic.StoreInt32(&s.backfillLimit, backfillLimit)
	} else {
		logging.Errorf("ClientSettings: invalid setting value for backfillLimit=%v", backfillLimit)
	}

	scanLagPercent := config["queryport.client.scanLagPercent"].Float64()
	if scanLagPercent >= 0 {
		atomic.StoreUint64(&s.scanLagPercent, math.Float64bits(scanLagPercent))
	} else {
		logging.Errorf("ClientSettings: invalid setting value for scanLagPercent=%v", scanLagPercent)
	}

	scanLagItem := config["queryport.client.scanLagItem"].Int()
	if scanLagItem >= 0 {
		atomic.StoreUint64(&s.scanLagItem, uint64(scanLagItem))
	} else {
		logging.Errorf("ClientSettings: invalid setting value for scanLagItem=%v", scanLagItem)
	}

	prune_replica := config["queryport.client.disable_prune_replica"].Bool()
	if prune_replica {
		atomic.StoreInt32(&s.prune_replica, int32(1))
	} else {
		atomic.StoreInt32(&s.prune_replica, int32(0))
	}

	queueSize := config["queryport.client.scan.queue_size"].Int()
	if queueSize >= 0 {
		atomic.StoreUint64(&s.queueSize, uint64(queueSize))
	} else {
		logging.Errorf("ClientSettings: invalid setting value for queueSize=%v", queueSize)
	}

	concurrency := config["queryport.client.scan.max_concurrency"].Int()
	if concurrency >= 0 {
		atomic.StoreUint32(&s.concurrency, uint32(concurrency))
	} else {
		logging.Errorf("ClientSettings: invalid setting value for max_concurrency=%v", concurrency)
	}

	allowCJsonScanFormat, ok := config["queryport.client.allowCJsonScanFormat"]
	if ok {
		if allowCJsonScanFormat.Bool() {
			atomic.StoreUint32(&s.allowCJsonScanFormat, 1)
		} else {
			atomic.StoreUint32(&s.allowCJsonScanFormat, 0)
		}
	} else {
		// Use default config value on error
		logging.Errorf("ClientSettings: missing allowCJsonScanFormat")
		atomic.StoreUint32(&s.allowCJsonScanFormat, 1)
	}

	allowPartialQuorum, ok := config["indexer.allowPartialQuorum"]
	if ok {
		if allowPartialQuorum.Bool() {
			atomic.StoreUint32(&s.allowPartialQuorum, 1)
		} else {
			atomic.StoreUint32(&s.allowPartialQuorum, 0)
		}
	} else {
		// Use default config value on error
		logging.Errorf("ClientSettings: missing allowPartialQuorum")
		atomic.StoreUint32(&s.allowPartialQuorum, 0)
	}

	usePlanner, ok := config["queryport.client.usePlanner"]
	if ok {
		if usePlanner.Bool() {
			atomic.StoreUint32(&s.usePlanner, 1)
		} else {
			atomic.StoreUint32(&s.usePlanner, 0)
		}
	} else {
		// Use default config value on error
		logging.Errorf("ClientSettings: missing usePlanner")
		atomic.StoreUint32(&s.usePlanner, 1)
	}

	storageMode := config["indexer.settings.storage_mode"].String()
	if len(storageMode) != 0 {
		func() {
			s.mutex.Lock()
			defer s.mutex.Unlock()
			s.storageMode = storageMode
		}()
	}

	restRequestTimeout, ok := config["queryport.client.restRequestTimeout"]
	if ok {
		planner.SetRestRequestTimeout(uint32(restRequestTimeout.Int()))
	}

	if s.needRefresh {
		logLevel := config["queryport.client.log_level"].String()
		level := logging.Level(logLevel)
		logging.SetLogLevel(level)
	}
}

func (s *ClientSettings) NumReplica() int32 {
	return atomic.LoadInt32(&s.numReplica)
}

func (s *ClientSettings) NumPartition() int32 {
	return atomic.LoadInt32(&s.numPartition)
}

func (s *ClientSettings) StorageMode() string {

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	return s.storageMode
}

func (s *ClientSettings) BackfillLimit() int32 {
	return atomic.LoadInt32(&s.backfillLimit)
}

func (s *ClientSettings) ScanLagPercent() float64 {
	bits := atomic.LoadUint64(&s.scanLagPercent)
	return math.Float64frombits(bits)
}

func (s *ClientSettings) ScanLagItem() uint64 {
	return atomic.LoadUint64(&s.scanLagItem)
}

func (s *ClientSettings) DisablePruneReplica() bool {
	if atomic.LoadInt32(&s.prune_replica) == 1 {
		return true
	}
	return false
}

func (s *ClientSettings) ScanQueueSize() uint64 {
	return atomic.LoadUint64(&s.queueSize)
}

func (s *ClientSettings) MaxConcurrency() uint32 {
	return atomic.LoadUint32(&s.concurrency)
}

func (s *ClientSettings) AllowCJsonScanFormat() bool {
	return atomic.LoadUint32(&s.allowCJsonScanFormat) == 1
}

func (s *ClientSettings) AllowPartialQuorum() bool {
	return atomic.LoadUint32(&s.allowPartialQuorum) == 1
}

func (s *ClientSettings) UsePlanner() bool {
	return atomic.LoadUint32(&s.usePlanner) == 1
}
