package projector

import "fmt"
import "sync"
import "io"
import "time"
import "os"
import "net/http"
import "strings"
import "encoding/json"
import "runtime"
import "runtime/pprof"
import "runtime/debug"

import ap "github.com/couchbase/indexing/secondary/adminport"
import c "github.com/couchbase/indexing/secondary/common"
import projC "github.com/couchbase/indexing/secondary/projector/client"
import protobuf "github.com/couchbase/indexing/secondary/protobuf/projector"
import "github.com/golang/protobuf/proto"
import "github.com/couchbase/indexing/secondary/logging"

// Projector data structure, a projector is connected to
// one or more upstream kv-nodes. Works in tandem with
// projector's adminport.
type Projector struct {
	admind ap.Server // admin-port server
	// lock protected fields.
	rw             sync.RWMutex
	topics         map[string]*Feed // active topics
	topicSerialize map[string]*sync.Mutex
	config         c.Config // full configuration information.
	// immutable config params
	name        string // human readable name of the projector
	clusterAddr string // kv cluster's address to connect
	pooln       string // kv pool-name
	adminport   string // projector listens on this adminport
	maxvbs      int
	cpuProfFd   *os.File
	logPrefix   string
}

// NewProjector creates a news projector instance and
// starts a corresponding adminport.
func NewProjector(maxvbs int, config c.Config) *Projector {
	p := &Projector{
		topics:         make(map[string]*Feed),
		topicSerialize: make(map[string]*sync.Mutex),
		maxvbs:         maxvbs,
		pooln:          "default", // TODO: should this be configurable ?
	}

	// Setup dynamic configuration propagation
	config, err := c.GetSettingsConfig(config)
	c.CrashOnError(err)

	pconfig := config.SectionConfig("projector.", true /*trim*/)
	p.name = pconfig["name"].String()
	p.clusterAddr = pconfig["clusterAddr"].String()
	p.adminport = pconfig["adminport.listenAddr"].String()
	ef := config["projector.routerEndpointFactory"]
	config["projector.routerEndpointFactory"] = ef

	p.config = config
	p.ResetConfig(config)

	p.logPrefix = fmt.Sprintf("PROJ[%s]", p.adminport)

	cluster := p.clusterAddr
	if !strings.HasPrefix(p.clusterAddr, "http://") {
		cluster = "http://" + cluster
	}

	apConfig := config.SectionConfig("projector.adminport.", true)
	apConfig.SetValue("name", "PRAM")
	reqch := make(chan ap.Request)
	p.admind = ap.NewHTTPServer(apConfig, reqch)

	// set GOGC percent
	gogc := pconfig["gogc"].Int()
	oldGogc := debug.SetGCPercent(gogc)
	fmsg := "%v changing GOGC percentage from %v to %v\n"
	logging.Infof(fmsg, p.logPrefix, oldGogc, gogc)

	watchInterval := config["projector.watchInterval"].Int()
	staleTimeout := config["projector.staleTimeout"].Int()
	go c.MemstatLogger(int64(config["projector.memstatTick"].Int()))
	go p.mainAdminPort(reqch)
	go p.watcherDameon(watchInterval, staleTimeout)

	callb := func(cfg c.Config) {
		logging.Infof("%v settings notifier from metakv\n", p.logPrefix)
		cfg.LogConfig(p.logPrefix)
		p.ResetConfig(cfg)
	}
	c.SetupSettingsNotifier(callb, make(chan struct{}))

	logging.Infof("%v started ...\n", p.logPrefix)
	return p
}

// GetConfig returns the config object from projector.
func (p *Projector) GetConfig() c.Config {
	p.rw.Lock()
	defer p.rw.Unlock()
	return p.config.Clone()
}

// ResetConfig accepts a full-set or subset of global configuration
// and updates projector related fields.
func (p *Projector) ResetConfig(config c.Config) {
	p.rw.Lock()
	defer p.rw.Unlock()
	defer logging.Infof("%v\n", c.LogRuntime())

	// reset configuration.
	if cv, ok := config["projector.settings.log_level"]; ok {
		logging.SetLogLevel(logging.Level(cv.String()))
	}
	if cv, ok := config["projector.maxCpuPercent"]; ok {
		c.SetNumCPUs(cv.Int())
	}
	if cv, ok := config["projector.gogc"]; ok {
		gogc := cv.Int()
		oldGogc := debug.SetGCPercent(gogc)
		fmsg := "%v changing GOGC percentage from %v to %v\n"
		logging.Infof(fmsg, p.logPrefix, oldGogc, gogc)
	}
	if cv, ok := config["projector.memstatTick"]; ok {
		c.Memstatch <- int64(cv.Int())
	}
	p.config = p.config.Override(config)

	// CPU-profiling
	cpuProfile, ok := config["projector.cpuProfile"]
	if ok && cpuProfile.Bool() && p.cpuProfFd == nil {
		cpuProfFname, ok := config["projector.cpuProfFname"]
		if ok {
			fname := cpuProfFname.String()
			logging.Infof("%v cpu profiling => %q\n", p.logPrefix, fname)
			p.cpuProfFd = p.startCPUProfile(fname)

		} else {
			logging.Errorf("Missing cpu-profile o/p filename\n")
		}

	} else if ok && !cpuProfile.Bool() {
		if p.cpuProfFd != nil {
			pprof.StopCPUProfile()
			logging.Infof("%v cpu profiling stopped\n", p.logPrefix)
		}
		p.cpuProfFd = nil

	} else if ok {
		logging.Warnf("%v cpu profiling already active !!\n", p.logPrefix)
	}

	// MEM-profiling
	memProfile, ok := config["projector.memProfile"]
	if ok && memProfile.Bool() {
		memProfFname, ok := config["projector.memProfFname"]
		if ok {
			fname := memProfFname.String()
			if p.takeMEMProfile(fname) {
				logging.Infof("%v mem profile => %q\n", p.logPrefix, fname)
			}
		} else {
			logging.Errorf("Missing mem-profile o/p filename\n")
		}
	}
}

// GetFeedConfig from current configuration settings.
func (p *Projector) GetFeedConfig() c.Config {
	p.rw.Lock()
	defer p.rw.Unlock()

	config, _ := c.NewConfig(map[string]interface{}{})
	config["clusterAddr"] = p.config["clusterAddr"] // copy by value.
	pconfig := p.config.SectionConfig("projector.", true /*trim*/)
	for _, key := range FeedConfigParams() {
		config.Set(key, pconfig[key])
	}
	return config
}

// GetFeed object for `topic`.
// - return ErrorTopicMissing if topic is not started.
func (p *Projector) GetFeed(topic string) (*Feed, error) {
	getfeed := func() (*Feed, bool) {
		p.rw.RLock()
		defer p.rw.RUnlock()
		feed, ok := p.topics[topic]
		return feed, ok
	}

	if feed, ok := getfeed(); ok {
		if err := feed.Ping(); err != nil {
			return nil, err
		}
		return feed, nil
	}
	return nil, projC.ErrorTopicMissing
}

// GetFeeds return a list of all feeds.
func (p *Projector) GetFeeds() []*Feed {
	p.rw.RLock()
	defer p.rw.RUnlock()

	feeds := make([]*Feed, 0)
	for _, feed := range p.topics {
		feeds = append(feeds, feed)
	}
	return feeds
}

// AddFeed object for `topic`.
// - return ErrorTopicExist if topic is duplicate.
func (p *Projector) AddFeed(topic string, feed *Feed) (err error) {
	p.rw.Lock()
	defer p.rw.Unlock()

	if _, ok := p.topics[topic]; ok {
		return projC.ErrorTopicExist
	}
	p.topics[topic] = feed
	opaque := feed.GetOpaque()
	logging.Infof("%v ##%x feed %q added ...\n", p.logPrefix, opaque, topic)
	return
}

// DelFeed object for `topic`.
// - return ErrorTopicMissing if topic is not started.
func (p *Projector) DelFeed(topic string) (err error) {
	p.rw.Lock()
	defer p.rw.Unlock()

	feed, ok := p.topics[topic]
	if ok == false {
		return projC.ErrorTopicMissing
	}
	delete(p.topics, topic)
	opaque := feed.GetOpaque()
	logging.Infof("%v ##%x ... feed %q deleted\n", p.logPrefix, opaque, topic)

	go func() { // GC
		now := time.Now()
		runtime.GC()
		fmsg := "%v ##%x GC() took %v\n"
		logging.Infof(fmsg, p.logPrefix, opaque, time.Since(now))
	}()
	return
}

//---- handler for admin-port request

// - return couchbase SDK error if any.
func (p *Projector) doVbmapRequest(
	request *protobuf.VbmapRequest, opaque uint16) ap.MessageMarshaller {

	response := &protobuf.VbmapResponse{}

	pooln := request.GetPool()
	bucketn := request.GetBucket()
	kvaddrs := request.GetKvaddrs()

	// log this request.
	prefix := p.logPrefix
	fmsg := "%v ##%x doVbmapRequest() {%q, %q, %v}\n"
	logging.Infof(fmsg, prefix, pooln, bucketn, kvaddrs, opaque)
	defer logging.Infof("%v ##%x doVbmapRequest() returns ...\n", prefix, opaque)

	// get vbmap from bucket connection.
	bucket, err := c.ConnectBucket(p.clusterAddr, pooln, bucketn)
	if err != nil {
		logging.Errorf("%v ##%x ConnectBucket(): %v\n", prefix, opaque, err)
		response.Err = protobuf.NewError(err)
		return response
	}
	defer bucket.Close()

	bucket.Refresh()
	m, err := bucket.GetVBmap(kvaddrs)
	if err != nil {
		logging.Errorf("%v ##%x GetVBmap(): %v\n", prefix, opaque, err)
		response.Err = protobuf.NewError(err)
		return response
	}

	// compose response
	response.Kvaddrs = make([]string, 0, len(kvaddrs))
	response.Kvvbnos = make([]*protobuf.Vbuckets, 0, len(kvaddrs))
	for kvaddr, vbnos := range m {
		response.Kvaddrs = append(response.Kvaddrs, kvaddr)
		response.Kvvbnos = append(
			response.Kvvbnos, &protobuf.Vbuckets{Vbnos: c.Vbno16to32(vbnos)})
	}
	return response
}

// - return couchbase SDK error if any.
func (p *Projector) doFailoverLog(
	request *protobuf.FailoverLogRequest, opaque uint16) ap.MessageMarshaller {

	response := &protobuf.FailoverLogResponse{}

	pooln := request.GetPool()
	bucketn := request.GetBucket()
	vbuckets := request.GetVbnos()

	// log this request.
	prefix := p.logPrefix
	fmsg := "%v ##%x doFailoverLog() {%q, %q, %v}\n"
	logging.Infof(fmsg, prefix, opaque, pooln, bucketn, vbuckets)
	defer logging.Infof("%v ##%x doFailoverLog() returns ...\n", prefix, opaque)

	bucket, err := c.ConnectBucket(p.clusterAddr, pooln, bucketn)
	if err != nil {
		logging.Errorf("%v ##%x ConnectBucket(): %v\n", prefix, opaque, err)
		response.Err = protobuf.NewError(err)
		return response
	}
	defer bucket.Close()

	config := p.GetConfig()
	protoFlogs := make([]*protobuf.FailoverLog, 0, len(vbuckets))
	vbnos := c.Vbno32to16(vbuckets)
	dcpConfig := map[string]interface{}{
		"genChanSize":    config["projector.dcp.genChanSize"].Int(),
		"dataChanSize":   config["projector.dcp.dataChanSize"].Int(),
		"numConnections": config["projector.dcp.numConnections"].Int(),
	}
	flogs, err := bucket.GetFailoverLogs(opaque, vbnos, dcpConfig)
	if err == nil {
		for vbno, flog := range flogs {
			vbuuids := make([]uint64, 0, len(flog))
			seqnos := make([]uint64, 0, len(flog))
			for _, x := range flog {
				vbuuids = append(vbuuids, x[0])
				seqnos = append(seqnos, x[1])
			}
			protoFlog := &protobuf.FailoverLog{
				Vbno:    proto.Uint32(uint32(vbno)),
				Vbuuids: vbuuids,
				Seqnos:  seqnos,
			}
			protoFlogs = append(protoFlogs, protoFlog)
		}
	} else {
		logging.Errorf("%v ##%x GetFailoverLogs(): %v\n", prefix, opaque, err)
		response.Err = protobuf.NewError(err)
		return response
	}
	response.Logs = protoFlogs
	return response
}

// - return ErrorInvalidKVaddrs for malformed vbuuid.
// - return ErrorInconsistentFeed for malformed feed request.
// - return ErrorInvalidVbucketBranch for malformed vbuuid.
// - return dcp-client failures.
// - return ErrorResponseTimeout if request is not completed within timeout.
func (p *Projector) doMutationTopic(
	request *protobuf.MutationTopicRequest,
	opaque uint16) ap.MessageMarshaller {

	topic := request.GetTopic()

	// log this request.
	prefix := p.logPrefix
	logging.Infof("%v ##%x doMutationTopic() %q\n", prefix, opaque, topic)
	defer logging.Infof("%v ##%x doMutationTopic() returns ...\n", prefix, opaque)

	var err error
	feed, _ := p.acquireFeed(topic)
	defer p.releaseFeed(topic)
	if feed == nil {
		config := p.GetFeedConfig()
		feed, err = NewFeed(p.pooln, topic, p, config, opaque)
		if err != nil {
			fmsg := "%v ##%x unable to create feed %v\n"
			logging.Errorf(fmsg, prefix, opaque, topic)
			return (&protobuf.TopicResponse{}).SetErr(err)
		}
	}
	response, err := feed.MutationTopic(request, opaque)
	if err != nil {
		response.SetErr(err)
	}
	p.AddFeed(topic, feed)
	return response
}

// - return ErrorTopicMissing if feed is not started.
// - return ErrorInvalidBucket if bucket is not added.
// - return ErrorInvalidVbucketBranch for malformed vbuuid.
// - return dcp-client failures.
// - return ErrorResponseTimeout if request is not completed within timeout.
func (p *Projector) doRestartVbuckets(
	request *protobuf.RestartVbucketsRequest,
	opaque uint16) ap.MessageMarshaller {

	topic := request.GetTopic()

	// log this request.
	prefix := p.logPrefix
	logging.Infof("%v ##%x doRestartVbuckets() %q\n", prefix, opaque, topic)
	defer logging.Infof("%v ##%x doRestartVbuckets() returns ...\n", prefix, opaque)

	feed, err := p.acquireFeed(topic)
	defer p.releaseFeed(topic)
	if err != nil {
		logging.Errorf("%v ##%x acquireFeed(): %v\n", prefix, opaque, err)
		response := &protobuf.TopicResponse{}
		if err != projC.ErrorTopicMissing {
			response = feed.GetTopicResponse()
		}
		return response.SetErr(err)
	}

	response, err := feed.RestartVbuckets(request, opaque)
	if err == nil {
		return response
	}
	return response.SetErr(err)
}

// - return ErrorTopicMissing if feed is not started.
// - return ErrorInvalidBucket if bucket is not added.
// - return ErrorInvalidVbucketBranch for malformed vbuuid.
// - return dcp-client failures.
// - return ErrorResponseTimeout if request is not completed within timeout.
func (p *Projector) doShutdownVbuckets(
	request *protobuf.ShutdownVbucketsRequest,
	opaque uint16) ap.MessageMarshaller {

	topic := request.GetTopic()

	// log this request.
	prefix := p.logPrefix
	logging.Infof("%v ##%x doShutdownVbuckets() %q\n", prefix, opaque, topic)
	defer logging.Infof("%v ##%x doShutdownVbuckets() returns ...\n", prefix, opaque)

	feed, err := p.acquireFeed(topic)
	defer p.releaseFeed(topic)
	if err != nil {
		logging.Errorf("%v ##%x acquireFeed(): %v\n", prefix, opaque, err)
		return protobuf.NewError(err)
	}

	err = feed.ShutdownVbuckets(request, opaque)
	return protobuf.NewError(err)
}

// - return ErrorTopicMissing if feed is not started.
// - return ErrorInconsistentFeed for malformed feed request
// - return ErrorInvalidVbucketBranch for malformed vbuuid.
// - return dcp-client failures.
// - return ErrorResponseTimeout if request is not completed within timeout.
func (p *Projector) doAddBuckets(
	request *protobuf.AddBucketsRequest, opaque uint16) ap.MessageMarshaller {

	topic := request.GetTopic()

	// log this request.
	prefix := p.logPrefix
	logging.Infof("%v ##%x doAddBuckets() %q\n", prefix, opaque, topic)
	defer logging.Infof("%v ##%x doAddBuckets() returns ...\n", prefix, opaque)

	feed, err := p.acquireFeed(topic)
	defer p.releaseFeed(topic)
	if err != nil {
		logging.Errorf("%v ##%x acquireFeed(): %v\n", prefix, opaque, err)
		response := &protobuf.TopicResponse{}
		if err != projC.ErrorTopicMissing {
			response = feed.GetTopicResponse()
		}
		return response.SetErr(err)
	}

	response, err := feed.AddBuckets(request, opaque)
	if err == nil {
		return response
	}
	return response.SetErr(err)
}

// - return ErrorTopicMissing if feed is not started.
// - return ErrorInvalidBucket if bucket is not added.
// - return ErrorInvalidVbucketBranch for malformed vbuuid.
// - return dcp-client failures.
// - return ErrorResponseTimeout if request is not completed within timeout.
func (p *Projector) doDelBuckets(
	request *protobuf.DelBucketsRequest, opaque uint16) ap.MessageMarshaller {

	topic := request.GetTopic()

	// log this request.
	prefix := p.logPrefix
	logging.Infof("%v ##%x doDelBuckets() %q\n", prefix, opaque, topic)
	defer logging.Infof("%v ##%x doDelBuckets() returns ...\n", prefix, opaque)

	feed, err := p.acquireFeed(topic)
	defer p.releaseFeed(topic)
	if err != nil {
		logging.Errorf("%v ##%x acquireFeed(): %v\n", prefix, opaque, err)
		return protobuf.NewError(err)
	}

	err = feed.DelBuckets(request, opaque)
	return protobuf.NewError(err)
}

// - return ErrorTopicMissing if feed is not started.
// - return ErrorInconsistentFeed for malformed feed request
// - otherwise, error is empty string.
func (p *Projector) doAddInstances(
	request *protobuf.AddInstancesRequest, opaque uint16) ap.MessageMarshaller {

	topic := request.GetTopic()

	// log this request.
	prefix := p.logPrefix
	logging.Infof("%v ##%x doAddInstances() %q\n", prefix, opaque, topic)
	defer logging.Infof("%v ##%x doAddInstances() returns ...\n", prefix, opaque)

	feed, err := p.acquireFeed(topic)
	defer p.releaseFeed(topic)
	if err != nil {
		logging.Errorf("%v ##%x acquireFeed(): %v\n", prefix, opaque, err)
		return protobuf.NewError(err)
	}

	response, err := feed.AddInstances(request, opaque)
	if err != nil {
		response.SetErr(err)
	}
	return response
}

// - return ErrorTopicMissing if feed is not started.
// - otherwise, error is empty string.
func (p *Projector) doDelInstances(
	request *protobuf.DelInstancesRequest, opaque uint16) ap.MessageMarshaller {

	topic := request.GetTopic()

	// log this request.
	prefix := p.logPrefix
	logging.Infof("%v ##%x doDelInstances() %q\n", prefix, opaque, topic)
	defer logging.Infof("%v ##%x doDelInstances() returns ...\n", prefix, opaque)

	feed, err := p.acquireFeed(topic)
	defer p.releaseFeed(topic)
	if err != nil {
		logging.Errorf("%v ##%x acquireFeed(): %v\n", prefix, opaque, err)
		return protobuf.NewError(err)
	}

	err = feed.DelInstances(request, opaque)
	return protobuf.NewError(err)
}

// - return ErrorTopicMissing if feed is not started.
// - otherwise, error is empty string.
func (p *Projector) doRepairEndpoints(
	request *protobuf.RepairEndpointsRequest,
	opaque uint16) ap.MessageMarshaller {

	topic := request.GetTopic()

	// log this request.
	prefix := p.logPrefix
	logging.Infof("%v ##%x doRepairEndpoints() %q\n", prefix, opaque, topic)
	defer logging.Infof("%v ##%x doRepairEndpoints() returns ...\n", prefix, opaque)

	feed, err := p.acquireFeed(topic)
	defer p.releaseFeed(topic)
	if err != nil {
		logging.Errorf("%v ##%x acquireFeed(): %v\n", prefix, opaque, err)
		return protobuf.NewError(err)
	}

	err = feed.RepairEndpoints(request, opaque)
	return protobuf.NewError(err)
}

// - return ErrorTopicMissing if feed is not started.
// - otherwise, error is empty string.
func (p *Projector) doShutdownTopic(
	request *protobuf.ShutdownTopicRequest,
	opaque uint16) ap.MessageMarshaller {

	topic := request.GetTopic()

	// log this request.
	prefix := p.logPrefix
	logging.Infof("%v ##%x doShutdownTopic() %q\n", prefix, opaque, topic)
	defer logging.Infof("%v ##%x doShutdownTopic() returns ...\n", prefix, opaque)

	feed, err := p.acquireFeed(topic)
	defer p.releaseFeed(topic)
	if err != nil {
		logging.Errorf("%v ##%x acquireFeed(): %v\n", p.logPrefix, opaque, err)
		return protobuf.NewError(err)
	}

	p.DelFeed(topic)
	err = feed.Shutdown(opaque)
	return protobuf.NewError(err)
}

func (p *Projector) doStatistics() interface{} {
	logging.Infof("%v doStatistics()\n", p.logPrefix)
	defer logging.Infof("%v doStatistics() returns ...\n", p.logPrefix)

	m := map[string]interface{}{
		"clusterAddr": p.clusterAddr,
		"adminport":   p.adminport,
	}
	stats, _ := c.NewStatistics(m)

	feeds, _ := c.NewStatistics(nil)
	for topic, feed := range p.topics {
		feeds.Set(topic, feed.GetStatistics())
	}
	stats.Set("feeds", feeds)
	return map[string]interface{}(stats)
}

//--------------
// http handlers
//--------------

func validateAuth(w http.ResponseWriter, r *http.Request) bool {
	_, valid, err := c.IsAuthValid(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error() + "\n"))
	} else if valid == false {
		w.WriteHeader(401)
		w.Write([]byte("401 Unauthorized\n"))
	}
	return valid
}

// handle projector statistics
func (p *Projector) handleStats(w http.ResponseWriter, r *http.Request) {
	valid := validateAuth(w, r)
	if !valid {
		return
	}

	logging.Infof("%s Request %q\n", p.logPrefix, r.URL.Path)

	contentType := r.Header.Get("Content-Type")
	isJSON := strings.Contains(contentType, "application/json")

	stats := p.doStatistics().(map[string]interface{})
	if isJSON {
		data, err := json.Marshal(stats)
		if err != nil {
			logging.Errorf("%v encoding statistics: %v\n", p.logPrefix, err)
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		fmt.Fprintf(w, "%s", string(data))
		return
	}
	fmt.Fprintf(w, "%s", c.Statistics(stats).Lines())
}

// handle settings
func (p *Projector) handleSettings(w http.ResponseWriter, r *http.Request) {
	valid := validateAuth(w, r)
	if !valid {
		return
	}

	logging.Infof("%s Request %q %q\n", p.logPrefix, r.Method, r.URL.Path)
	switch r.Method {
	case "GET":
		header := w.Header()
		header["Content-Type"] = []string{"application/json"}
		fmt.Fprintf(w, "%s", string(p.GetConfig().Json()))

	case "POST":
		dataIn := make([]byte, r.ContentLength)
		// read settings
		if err := requestRead(r.Body, dataIn); err != nil {
			logging.Errorf("%v handleSettings() POST: %v\n", p.logPrefix, err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		// parse settings
		newConfig := make(map[string]interface{})
		if err := json.Unmarshal(dataIn, &newConfig); err != nil {
			fmsg := "%v handleSettings() json decoding: %v\n"
			logging.Errorf(fmsg, p.logPrefix, err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		// update projector settings
		logging.Infof("%v updating projector config ...\n", p.logPrefix)
		config, _ := c.NewConfig(newConfig)
		config.LogConfig(p.logPrefix)
		p.ResetConfig(config)
		// update feed settings
		feedConfig := config.SectionConfig("projector.", true /*trim*/)
		for _, feed := range p.GetFeeds() {
			if err := feed.ResetConfig(feedConfig); err != nil {
				fmsg := "%v feed(`%v`).ResetConfig: %v"
				logging.Errorf(fmsg, p.logPrefix, feed.topic, err)
			}
		}

	default:
		http.Error(w, "only GET POST supported", http.StatusMethodNotAllowed)
	}
}

//----------------
// local functions
//----------------

// start cpu profiling.
func (p *Projector) startCPUProfile(filename string) *os.File {
	if filename == "" {
		fmsg := "%v empty cpu profile filename\n"
		logging.Errorf(fmsg, p.logPrefix, filename)
		return nil
	}
	fd, err := os.Create(filename)
	if err != nil {
		logging.Errorf("%v unable to create %q: %v\n", p.logPrefix, filename, err)
	}
	pprof.StartCPUProfile(fd)
	return fd
}

func (p *Projector) takeMEMProfile(filename string) bool {
	if filename == "" {
		fmsg := "%v empty mem profile filename\n"
		logging.Errorf(fmsg, p.logPrefix, filename)
		return false
	}
	fd, err := os.Create(filename)
	if err != nil {
		logging.Errorf("%v unable to create %q: %v\n", p.logPrefix, filename, err)
		return false
	}
	pprof.WriteHeapProfile(fd)
	defer fd.Close()
	return true
}

// return list of active topics
func (p *Projector) listTopics() []string {
	p.rw.Lock()
	defer p.rw.Unlock()
	topics := make([]string, 0, len(p.topics))
	for topic := range p.topics {
		topics = append(topics, topic)
	}
	return topics
}

func (p *Projector) acquireFeed(topic string) (*Feed, error) {
	p.rw.Lock()
	mu, ok := p.topicSerialize[topic]
	if !ok {
		mu = new(sync.Mutex)
	}
	p.topicSerialize[topic] = mu
	p.rw.Unlock()

	mu.Lock() // every acquireFeed is accompanied by releaseFeed. lock always!

	feed, err := p.GetFeed(topic)
	if err != nil {
		p.DelFeed(topic)
		return nil, projC.ErrorTopicMissing
	}
	return feed, nil
}

func (p *Projector) releaseFeed(topic string) {
	p.rw.RLock()
	mu := p.topicSerialize[topic]
	p.rw.RUnlock()
	mu.Unlock()
}

func requestRead(r io.Reader, data []byte) (err error) {
	var c int

	n, start := len(data), 0
	for n > 0 && err == nil {
		// Per http://golang.org/pkg/io/#Reader, it is valid for Read to
		// return EOF with non-zero number of bytes at the end of the
		// input stream
		c, err = r.Read(data[start:])
		n -= c
		start += c
	}
	if n == 0 {
		return nil
	}
	return err
}
