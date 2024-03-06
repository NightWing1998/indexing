package indexer

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/stats"
)

type resourceTracker interface {
	usage() (int64, int64)
	lastCaptureTs() time.Time
	threshold() uint8
	updateThreshold(uint8)
	updateLastCaptureTs(time.Time)
	shouldRecordUsage(*time.Time) bool
	profiler() func(w io.Writer) error
	name() string
}

var gSystemStateLogger SystemStateLogger

type SystemStateLogger struct {
	stopCh   chan struct{}
	lock     *sync.Mutex
	trackers map[string]resourceTracker
	logDir   string
	running  bool
}

// Creates new system state logger with directories for the trackers.
//
// Dir structure:
//   - @logDir/@tracker.name()/<ts>.prof
func NewSystemStateLogger(logDir string, trackers []resourceTracker) (*SystemStateLogger, error) {
	var dirs, err = os.ReadDir(logDir)
	if errors.Is(err, os.ErrNotExist) {
		err = os.MkdirAll(logDir, os.ModePerm)
	}
	if err != nil {
		return nil, err
	}
	for _, tracker := range trackers {
		var found = false
		for _, dir := range dirs {
			if dir.IsDir() && dir.Name() == tracker.name() {
				found = true
			}
		}
		if !found {
			err = os.MkdirAll(filepath.Join(logDir, tracker.name()), os.ModePerm)
			if err != nil {
				return nil, err
			}
		}
	}
	var trackerMap = make(map[string]resourceTracker)
	for _, tracker := range trackers {
		trackerMap[tracker.name()] = tracker
	}
	return &SystemStateLogger{
		running: false,
		logDir:  logDir,

		lock: &sync.Mutex{},

		trackers: trackerMap,
	}, nil
}

func (ssl *SystemStateLogger) Run() bool {
	ssl.lock.Lock()
	defer ssl.lock.Unlock()

	if ssl.running {
		logging.Warnf("ssl::Run already running")
		return false
	}

	ssl.running = true
	ssl.stopCh = make(chan struct{})

	go func() {
		for {
			if ssl.stopCh != nil {
				select {
				case <-ssl.stopCh:
					ssl.lock.Lock()
					defer ssl.lock.Unlock()
					ssl.stopCh = nil
					ssl.running = false
					logging.Infof("ssl::Run exiting...")
					return
				default:
					// because the capture can be time consuming and we could have received a stop
					// while capture was running, we ideally want to stop asap after stop so
					// any sleep/cron should be done such that after capture we check the stopCh
					time.Sleep(10 * time.Second)
					ssl.captureSystemState()
				}
			} else {
				return
			}
		}
	}()

	return true
}

func (ssl *SystemStateLogger) Stop() bool {
	ssl.lock.Lock()
	defer ssl.lock.Unlock()
	if !ssl.running || ssl.stopCh == nil {
		return false
	}
	close(ssl.stopCh)
	return true
}

// func (ssl *SystemStateLogger) shouldRecordMemoryUsage(now *time.Time) bool {
// 	var used, quota = ssl.memUsageCallback()
// 	return used*100/quota >= uint64(ssl.memoryThreshold) &&
// 		now.Sub(ssl.lastMemoryCaptureTimestamp).Minutes() >= 5
// }

// func (ssl *SystemStateLogger) shouldRecordCpuUsage(now *time.Time) bool {
// 	var usage, cores = ssl.cpuUsageCallback()
// 	return uint64(usage)/cores > uint64(ssl.cpuThreshold) &&
// 		now.Sub(ssl.lastCpuCaptureTimestamp).Minutes() >= 5
// }

func (ssl *SystemStateLogger) captureSystemState() {
	ssl.lock.Lock()
	defer ssl.lock.Unlock()

	var now = time.Now()
	var captureWaitGroup sync.WaitGroup

	// if ssl.memUsageCallback != nil && ssl.memoryThreshold > 0 {
	// 	if ssl.shouldRecordMemoryUsage(&now) {
	// 		captureWaitGroup.Add(1)
	// 		ssl.lastMemoryCaptureTimestamp = now
	// 		// save memory profile in a go-routine

	// 	}
	// }

	// if ssl.cpuUsageCallback != nil && ssl.cpuThreshold > 0 {
	// 	if ssl.shouldRecordCpuUsage(&now) {
	// 		captureWaitGroup.Add(1)
	// 		ssl.lastCpuCaptureTimestamp = now
	// 		// save CPU profile in a go-routine
	// 	}
	// }

	for _, tracker := range ssl.trackers {
		if tracker == nil {
			continue
		}
		if tracker.shouldRecordUsage(&now) {
			captureWaitGroup.Add(1)
			// save profile to disk
			var path = filepath.Join(ssl.logDir, tracker.name(), fmt.Sprintf("%v.prof.gz", now.UnixMilli()))
			var file, err = os.OpenFile(path, os.O_CREATE|os.O_WRONLY, os.ModePerm)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					err = os.MkdirAll(filepath.Join(ssl.logDir, tracker.name()), os.ModePerm)
				}
			}
			if err != nil {
				logging.Errorf("ssl:cSS failed to capture system for %v with err %v", tracker.name(), err)
				captureWaitGroup.Done()
				continue
			}
			go func(file *os.File, tracker resourceTracker) {
				defer file.Close()
				defer captureWaitGroup.Done()

				var writer io.Writer
				writer, err = gzip.NewWriterLevel(file, gzip.BestSpeed)
				if err != nil {
					logging.Warnf("ssl:cSS failed to create compressed writer for %v tracker with err %v. falling back to normal mode",
						tracker.name(), err)
					writer = file
				} else {
					defer writer.(*gzip.Writer).Close()
				}
				err = tracker.profiler()(writer)
				if err != nil {
					logging.Warnf("ssl:cSS failed to write profile for %v with err %v", tracker.name(), err)
					return
				}
				tracker.updateLastCaptureTs(now)
				logging.Infof("ssl:cSS captured prof for %v at %v", tracker.name(), path)
			}(file, tracker)
		}
	}

	captureWaitGroup.Wait()
}

func (ssl *SystemStateLogger) updateTracker(name string, tracker resourceTracker) {
	ssl.lock.Lock()
	defer ssl.lock.Unlock()

	if tracker == nil {
		delete(ssl.trackers, name)
	} else {
		ssl.trackers[name] = tracker
	}
}

// Track Goheap usage against indexer quota
type GoheapResourceTracker struct {
	thresh      *uint64
	lastCapture *time.Time
	quota       stats.Int64Val
	inuse       stats.Int64Val
}

func (grt *GoheapResourceTracker) threshold() uint8 {
	return uint8(atomic.LoadUint64(grt.thresh))
}

func (grt *GoheapResourceTracker) updateThreshold(thresh uint8) {
	atomic.StoreUint64(grt.thresh, uint64(thresh))
}

func (grt *GoheapResourceTracker) lastCaptureTs() time.Time {
	return *grt.lastCapture
}

func (grt *GoheapResourceTracker) name() string {
	return "memory"
}

func (grt *GoheapResourceTracker) usage() (int64, int64) {
	// quota*15% is usually what we want to limit indexer to
	return grt.inuse.Value(), ((grt.quota.Value() * 15) / 100)
}

// only to be called from the update loop in captureSystemState which is expected to be running only
// instance at a time for a tracker so no lock required here
func (grt *GoheapResourceTracker) updateLastCaptureTs(now time.Time) {
	grt.lastCapture = &now
}

func (grt *GoheapResourceTracker) profiler() func(w io.Writer) error {
	return func(w io.Writer) error {
		return pprof.Lookup("heap").WriteTo(w, 1)
	}
}

func (grt *GoheapResourceTracker) shouldRecordUsage(now *time.Time) bool {
	if grt.lastCapture == nil {
		return true
	}
	var inuse, quota = grt.usage()
	var thresh = grt.threshold()
	if thresh == 0 {
		return false
	}
	return (now.Sub(*grt.lastCapture) > 5*time.Minute && (inuse*int64(thresh)*100) > quota)
}

func NewGoheapResourceTracker(quotaPtr, inusePtr stats.Int64Val, thresh uint64) *GoheapResourceTracker {
	return &GoheapResourceTracker{
		quota:       quotaPtr,
		inuse:       inusePtr,
		thresh:      &thresh,
		lastCapture: nil,
	}
}
