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
	"errors"
	"fmt"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/pipeline"
	"github.com/couchbase/indexing/secondary/stubs/nitro/mm"
	"github.com/couchbase/indexing/secondary/stubs/nitro/plasma"

	"io/ioutil"
	"net/http"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"time"
)

const (
	indexCompactonMetaPath = common.IndexingMetaDir + "triggerCompaction"
	compactionDaysSetting  = "indexer.settings.compaction.days_of_week"
)

// Implements dynamic settings management for indexer
type settingsManager struct {
	supvCmdch       MsgChannel
	supvMsgch       MsgChannel
	config          common.Config
	cancelCh        chan struct{}
	compactionToken []byte
}

func NewSettingsManager(supvCmdch MsgChannel,
	supvMsgch MsgChannel, config common.Config) (settingsManager, common.Config, Message) {
	s := settingsManager{
		supvCmdch: supvCmdch,
		supvMsgch: supvMsgch,
		config:    config,
		cancelCh:  make(chan struct{}),
	}

	config, err := common.GetSettingsConfig(config)
	if err != nil {
		return s, nil, &MsgError{
			err: Error{
				category: INDEXER,
				cause:    err,
				severity: FATAL,
			}}
	}

	initGlobalSettings(nil, config)
	http.HandleFunc("/settings", s.handleSettingsReq)
	http.HandleFunc("/triggerCompaction", s.handleCompactionTrigger)
	http.HandleFunc("/settings/runtime/freeMemory", s.handleFreeMemoryReq)
	http.HandleFunc("/settings/runtime/forceGC", s.handleForceGCReq)
	http.HandleFunc("/plasmaDiag", s.handlePlasmaDiag)

	go func() {
		fn := func(r int, err error) error {
			if r > 0 {
				logging.Errorf("IndexerSettingsManager: metakv notifier failed (%v)..Restarting %v", err, r)
			}
			err = metakv.RunObserveChildren("/", s.metaKVCallback, s.cancelCh)
			return err
		}
		rh := common.NewRetryHelper(MAX_METAKV_RETRIES, time.Second, 2, fn)
		err := rh.Run()
		if err != nil {
			logging.Fatalf("IndexerSettingsManager: metakv notifier failed even after max retries. Restarting indexer.")
			os.Exit(1)
		}
	}()

	indexerConfig := config.SectionConfig("indexer.", true)
	return s, indexerConfig, &MsgSuccess{}
}

func (s *settingsManager) writeOk(w http.ResponseWriter) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK\n"))
}

func (s *settingsManager) writeError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusBadRequest)
	w.Write([]byte(err.Error() + "\n"))
}

func (s *settingsManager) writeJson(w http.ResponseWriter, json []byte) {
	header := w.Header()
	header["Content-Type"] = []string{"application/json"}
	w.WriteHeader(200)
	w.Write(json)
	w.Write([]byte("\n"))
}

func (s *settingsManager) validateAuth(w http.ResponseWriter, r *http.Request) (cbauth.Creds, bool) {
	creds, valid, err := common.IsAuthValid(r)
	if err != nil {
		s.writeError(w, err)
	} else if valid == false {
		w.WriteHeader(401)
		w.Write([]byte("401 Unauthorized\n"))
	}
	return creds, valid
}

func (s *settingsManager) handleSettingsReq(w http.ResponseWriter, r *http.Request) {
	creds, ok := s.validateAuth(w, r)
	if !ok {
		return
	}

	if !common.IsAllowed(creds, []string{"cluster.settings!write"}, w) {
		return
	}

	if r.Method == "POST" {
		bytes, _ := ioutil.ReadAll(r.Body)

		err := validateSettings(bytes)
		if err != nil {
			s.writeError(w, err)
			return
		}

		config := s.config.FilterConfig(".settings.")
		current, rev, err := metakv.Get(common.IndexingSettingsMetaPath)
		if err == nil {
			if len(current) > 0 {
				config.Update(current)
			}
			err = config.Update(bytes)
		}

		if err != nil {
			s.writeError(w, err)
			return
		}

		//settingsConfig := config.FilterConfig(".settings.")
		newSettingsBytes := config.Json()
		if err = metakv.Set(common.IndexingSettingsMetaPath, newSettingsBytes, rev); err != nil {
			s.writeError(w, err)
			return
		}
		s.writeOk(w)

	} else if r.Method == "GET" {
		settingsConfig, err := common.GetSettingsConfig(s.config)
		if err != nil {
			s.writeError(w, err)
			return
		}
		// handle ?internal=ok
		if query := r.URL.Query(); query != nil {
			param, ok := query["internal"]
			if ok && len(param) > 0 && param[0] == "ok" {
				s.writeJson(w, settingsConfig.Json())
				return
			}
		}
		s.writeJson(w, settingsConfig.FilterConfig(".settings.").Json())

	} else {
		s.writeError(w, errors.New("Unsupported method"))
		return
	}
}

func (s *settingsManager) handleCompactionTrigger(w http.ResponseWriter, r *http.Request) {
	creds, ok := s.validateAuth(w, r)
	if !ok {
		return
	}

	if !common.IsAllowed(creds, []string{"cluster.settings!write"}, w) {
		return
	}

	_, rev, err := metakv.Get(indexCompactonMetaPath)
	if err != nil {
		s.writeError(w, err)
		return
	}

	newToken := time.Now().String()
	if err = metakv.Set(indexCompactonMetaPath, []byte(newToken), rev); err != nil {
		s.writeError(w, err)
		return
	}

	s.writeOk(w)
}

func (s *settingsManager) handlePlasmaDiag(w http.ResponseWriter, r *http.Request) {
	creds, ok := s.validateAuth(w, r)
	if !ok {
		return
	}

	if !common.IsAllowed(creds, []string{"cluster.settings!write"}, w) {
		return
	}

	plasma.Diag.HandleHttp(w, r)
}

func (s *settingsManager) run() {
loop:
	for {
		select {
		case cmd, ok := <-s.supvCmdch:
			if ok {
				if cmd.GetMsgType() == STORAGE_MGR_SHUTDOWN {
					logging.Infof("SettingsManager::run Shutting Down")
					close(s.cancelCh)
					s.supvCmdch <- &MsgSuccess{}
					break loop
				}
			} else {
				break loop
			}
		}
	}
}

func (s *settingsManager) metaKVCallback(path string, value []byte, rev interface{}) error {
	if path == common.IndexingSettingsMetaPath {
		logging.Infof("New settings received: \n%s", string(value))

		upgradedConfig, upgraded := tryUpgradeConfig(value)
		if upgraded {
			if err := metakv.Set(common.IndexingSettingsMetaPath, upgradedConfig, rev); err != nil {
				return err
			}
			return nil
		}

		config := s.config.Clone()
		config.Update(value)
		initGlobalSettings(s.config, config)
		s.config = config

		indexerConfig := s.config.SectionConfig("indexer.", true)
		s.supvMsgch <- &MsgConfigUpdate{
			cfg: indexerConfig,
		}
	} else if path == indexCompactonMetaPath {
		currentToken := s.compactionToken
		s.compactionToken = value
		if bytes.Equal(currentToken, value) {
			return nil
		}

		logging.Infof("Manual compaction trigger requested")
		replych := make(chan []IndexStorageStats)
		statReq := &MsgIndexStorageStats{respch: replych}
		s.supvMsgch <- statReq
		stats := <-replych
		// XXX: minFile size check can be applied
		go func() {
			for _, is := range stats {
				errch := make(chan error)
				compactReq := &MsgIndexCompact{
					instId: is.InstId,
					errch:  errch,
				}
				logging.Infof("ManualCompaction: Compacting index instance:%v", is.InstId)
				s.supvMsgch <- compactReq
				err := <-errch
				if err == nil {
					logging.Infof("ManualCompaction: Finished compacting index instance:%v", is.InstId)
				} else {
					logging.Errorf("ManualCompaction: Index instance:%v Compaction failed with reason - %v", is.InstId, err)
				}
			}
		}()
	}

	return nil
}

func (s *settingsManager) handleFreeMemoryReq(w http.ResponseWriter, r *http.Request) {
	creds, ok := s.validateAuth(w, r)
	if !ok {
		return
	}

	if !common.IsAllowed(creds, []string{"cluster.settings!write"}, w) {
		return
	}

	logging.Infof("Received force free memory request. Executing FreeOSMemory...")
	debug.FreeOSMemory()
	mm.FreeOSMemory()
	s.writeOk(w)
}

func (s *settingsManager) handleForceGCReq(w http.ResponseWriter, r *http.Request) {
	creds, ok := s.validateAuth(w, r)
	if !ok {
		return
	}

	if !common.IsAllowed(creds, []string{"cluster.settings!write"}, w) {
		return
	}

	logging.Infof("Received force GC request. Executing GC...")
	runtime.GC()
	s.writeOk(w)
}

func setLogger(config common.Config) {
	logLevel := config["indexer.settings.log_level"].String()
	level := logging.Level(logLevel)
	logging.Infof("Setting log level to %v", level)
	logging.SetLogLevel(level)
}

func setBlockPoolSize(o, n common.Config) {
	var oldSz, newSz int
	if o != nil {
		oldSz = o["indexer.settings.bufferPoolBlockSize"].Int()
	}

	newSz = n["indexer.settings.bufferPoolBlockSize"].Int()

	if oldSz < newSz {
		pipeline.SetupBlockPool(newSz)
		logging.Infof("Setting buffer block size to %d bytes", newSz)
	} else if oldSz > newSz {
		logging.Errorf("Setting buffer block size from %d to %d failed "+
			" - Only sizes higher than current size is allowed during runtime",
			oldSz, newSz)
	}
}

func initGlobalSettings(oldCfg, newCfg common.Config) {
	setBlockPoolSize(oldCfg, newCfg)

	ncpu := common.SetNumCPUs(newCfg["indexer.settings.max_cpu_percent"].Int())
	logging.Infof("Setting maxcpus = %d", ncpu)

	setLogger(newCfg)
	useMutationSyncPool = newCfg["indexer.useMutationSyncPool"].Bool()
}

func initStorageSettings(newCfg common.Config) {

	allowLargeKeys = newCfg["settings.allow_large_keys"].Bool()
	if common.GetStorageMode() == common.FORESTDB {
		allowLargeKeys = false
	}

	if allowLargeKeys {
		maxArrayKeyLength = DEFAULT_MAX_ARRAY_KEY_SIZE
		maxSecKeyLen = DEFAULT_MAX_SEC_KEY_LEN
	} else {
		maxArrayKeyLength = newCfg["settings.max_array_seckey_size"].Int()
		maxSecKeyLen = newCfg["settings.max_seckey_size"].Int()
	}
	maxArrayKeyBufferLength = maxArrayKeyLength * 3
	maxArrayIndexEntrySize = maxArrayKeyBufferLength + MAX_DOCID_LEN + 2
	arrayEncBufPool = common.NewByteBufferPool(maxArrayIndexEntrySize + ENCODE_BUF_SAFE_PAD)

	maxSecKeyBufferLen = maxSecKeyLen * 3
	maxIndexEntrySize = maxSecKeyBufferLen + MAX_DOCID_LEN + 2
	encBufPool = common.NewByteBufferPool(maxIndexEntrySize + ENCODE_BUF_SAFE_PAD)

	ErrSecKeyTooLong = errors.New(fmt.Sprintf("Secondary key is too long (> %d)", maxSecKeyLen))
}

func validateSettings(value []byte) error {
	newConfig, err := common.NewConfig(value)
	if err != nil {
		return err
	}
	if val, ok := newConfig[compactionDaysSetting]; ok {
		for _, day := range val.Strings() {
			if !isValidDay(day) {
				msg := "Index circular compaction days_of_week is case-sensitive " +
					"and must have zero or more comma-separated values of " +
					"Sunday,Monday,Tuesday,Wednesday,Thursday,Friday,Saturday"
				return errors.New(msg)
			}
		}
	}

	if val, ok := newConfig["indexer.settings.max_seckey_size"]; ok {
		if val.Int() <= 0 {
			return errors.New("Setting should be an integer greater than 0")
		}
	}

	if val, ok := newConfig["indexer.settings.max_array_seckey_size"]; ok {
		if val.Int() <= 0 {
			return errors.New("Setting should be an integer greater than 0")
		}
	}

	// ToDo: Validate other settings
	return nil
}

func isValidDay(day string) bool {
	validDays := []string{"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"}
	for _, validDay := range validDays {
		if day == validDay {
			return true
		}
	}
	return false
}

func isValidDaysOfWeek(value []byte) bool {
	conf, _ := common.NewConfig(value)
	if val, ok := conf[compactionDaysSetting]; ok {
		for _, day := range val.Strings() {
			if !isValidDay(day) {
				return false
			}
		}
	}
	return true
}

// Try upgrading the config and fix any issues in config values
// Return true if upgraded, else false
func tryUpgradeConfig(value []byte) ([]byte, bool) {
	conf, _ := common.NewConfig(value)
	if val, ok := conf[compactionDaysSetting]; ok {
		if !isValidDaysOfWeek(value) {
			conf.SetValue(compactionDaysSetting, strings.Title(val.String()))
			if isValidDaysOfWeek(conf.Json()) {
				return conf.Json(), true
			} else {
				logging.Errorf("%v has invalid value %v. Setting it to empty value. "+
					"Update the setting to a valid value.",
					compactionDaysSetting, val.String())
				conf.SetValue(compactionDaysSetting, "")
				return conf.Json(), true
			}
		}
	}

	return value, false
}
