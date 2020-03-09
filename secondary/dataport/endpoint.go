// endpoint concurrency model:
//
//                  NewRouterEndpoint()
//                            |
//                            |
//                         (spawn)
//                            |
//                            |  (flushTick || > bufferSize)
//        Ping() -----*----> run -------------------------------> TCP
//                    |       ^
//        Send() -----*       | endpoint routine buffers messages,
//                    |       | batches them based on timeout and
//       Close() -----*       | message-count and periodically flushes
//                            | them out via dataport-client.
//                            |
//                            V
//                          buffers

package dataport

import "fmt"
import "net"
import "time"
import "strconv"
import "strings"
import "sync/atomic"

import c "github.com/couchbase/indexing/secondary/common"
import "github.com/couchbase/indexing/secondary/transport"
import "github.com/couchbase/indexing/secondary/logging"
import "github.com/couchbase/indexing/secondary/security"
import "github.com/couchbase/indexing/secondary/stats"

// RouterEndpoint structure, per topic, to gather key-versions / mutations
// from one or more vbuckets and push them downstream to a
// specific node.
type RouterEndpoint struct {
	topic     string
	timestamp int64  // immutable
	raddr     string // immutable
	cluster   string
	// config params
	logPrefix string
	keyChSize int // channel size for key-versions
	// live update is possible
	block      bool          // should endpoint block when remote is slow
	bufferSize int           // size of buffer to wait till flush
	bufferTm   time.Duration // timeout to flush endpoint-buffer
	harakiriTm time.Duration // timeout after which endpoint commits harakiri

	// gen-server
	ch    chan []interface{} // carries control commands
	finch chan bool
	done  uint32
	// downstream
	pkt  *transport.TransportPacket
	conn net.Conn
	// statistics
	stats *EndpointStats
}

type EndpointStats struct {
	closed      stats.BoolVal
	mutCount    stats.Uint64Val
	upsertCount stats.Uint64Val
	deleteCount stats.Uint64Val
	upsdelCount stats.Uint64Val
	syncCount   stats.Uint64Val
	beginCount  stats.Uint64Val
	endCount    stats.Uint64Val
	snapCount   stats.Uint64Val
	flushCount  stats.Uint64Val
	prjLatency  stats.Average
	endpCh      chan []interface{}
}

func (stats *EndpointStats) Init() {
	stats.closed.Init()
	stats.mutCount.Init()
	stats.upsertCount.Init()
	stats.deleteCount.Init()
	stats.upsdelCount.Init()
	stats.syncCount.Init()
	stats.beginCount.Init()
	stats.endCount.Init()
	stats.snapCount.Init()
	stats.flushCount.Init()
	stats.prjLatency.Init()
}

func (stats *EndpointStats) IsClosed() bool {
	return stats.closed.Value()
}

func (stats *EndpointStats) String() string {
	var stitems [14]string
	stitems[0] = `"mutCount":` + strconv.FormatUint(stats.mutCount.Value(), 10)
	stitems[1] = `"upsertCount":` + strconv.FormatUint(stats.upsertCount.Value(), 10)
	stitems[2] = `"deleteCount":` + strconv.FormatUint(stats.deleteCount.Value(), 10)
	stitems[3] = `"upsdelCount":` + strconv.FormatUint(stats.upsdelCount.Value(), 10)
	stitems[4] = `"syncCount":` + strconv.FormatUint(stats.syncCount.Value(), 10)
	stitems[5] = `"beginCount":` + strconv.FormatUint(stats.beginCount.Value(), 10)
	stitems[6] = `"endCount":` + strconv.FormatUint(stats.endCount.Value(), 10)
	stitems[7] = `"snapCount":` + strconv.FormatUint(stats.snapCount.Value(), 10)
	stitems[8] = `"flushCount":` + strconv.FormatUint(stats.flushCount.Value(), 10)
	stitems[9] = `"latency.min":` + strconv.FormatInt(stats.prjLatency.Min(), 10)
	stitems[10] = `"latency.max":` + strconv.FormatInt(stats.prjLatency.Max(), 10)
	stitems[11] = `"latency.avg":` + strconv.FormatInt(stats.prjLatency.Mean(), 10)
	stitems[12] = `"latency.movingAvg":` + strconv.FormatInt(stats.prjLatency.MovingAvg(), 10)
	stitems[13] = `"endpChLen":` + strconv.FormatUint((uint64)(len(stats.endpCh)), 10)
	statjson := strings.Join(stitems[:], ",")
	return fmt.Sprintf("{%v}", statjson)
}

// NewRouterEndpoint instantiate a new RouterEndpoint
// routine and return its reference.
func NewRouterEndpoint(
	cluster, topic, raddr string, maxvbs int,
	config c.Config) (*RouterEndpoint, error) {

	conn, err := security.MakeConn(raddr)
	if err != nil {
		return nil, err
	}

	endpoint := &RouterEndpoint{
		topic:      topic,
		raddr:      raddr,
		cluster:    cluster,
		finch:      make(chan bool),
		timestamp:  time.Now().UnixNano(),
		keyChSize:  config["keyChanSize"].Int(),
		block:      config["remoteBlock"].Bool(),
		bufferSize: config["bufferSize"].Int(),
		bufferTm:   time.Duration(config["bufferTimeout"].Int()),
		harakiriTm: time.Duration(config["harakiriTimeout"].Int()),
		stats:      &EndpointStats{},
	}
	endpoint.ch = make(chan []interface{}, endpoint.keyChSize)
	endpoint.conn = conn

	endpoint.stats.Init()
	endpoint.stats.endpCh = endpoint.ch
	// TODO: add configuration params for transport flags.
	flags := transport.TransportFlag(0).SetProtobuf()
	maxPayload := config["maxPayload"].Int()
	endpoint.pkt = transport.NewTransportPacket(maxPayload, flags)
	endpoint.pkt.SetEncoder(transport.EncodingProtobuf, protobufEncode)
	endpoint.pkt.SetDecoder(transport.EncodingProtobuf, protobufDecode)

	endpoint.bufferTm *= time.Millisecond
	endpoint.harakiriTm *= time.Millisecond

	endpoint.logPrefix = fmt.Sprintf(
		"ENDP[<-(%v,%4x)<-%v #%v]",
		endpoint.raddr, uint16(endpoint.timestamp), cluster, topic)

	go endpoint.run(endpoint.ch)
	logging.Infof("%v started ...\n", endpoint.logPrefix)
	return endpoint, nil
}

// commands
const (
	endpCmdPing byte = iota + 1
	endpCmdSend
	endpCmdResetConfig
	endpCmdGetStatistics
	endpCmdClose
)

// Ping whether endpoint is active, synchronous call.
func (endpoint *RouterEndpoint) Ping() bool {

	return atomic.LoadUint32(&endpoint.done) == 0
}

// ResetConfig synchronous call.
func (endpoint *RouterEndpoint) ResetConfig(config c.Config) error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{endpCmdResetConfig, config, respch}
	_, err := c.FailsafeOp(endpoint.ch, respch, cmd, endpoint.finch)
	return err
}

// Send KeyVersions to other end, asynchronous call.
// Asynchronous call. Return ErrorChannelFull that can be used by caller.
func (endpoint *RouterEndpoint) Send(data interface{}) error {
	cmd := []interface{}{endpCmdSend, data}
	if endpoint.block {
		return c.FailsafeOpAsync(endpoint.ch, cmd, endpoint.finch)
	}
	return c.FailsafeOpNoblock(endpoint.ch, cmd, endpoint.finch)
}

// GetStatistics for this endpoint, synchronous call.
func (endpoint *RouterEndpoint) GetStatistics() map[string]interface{} {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{endpCmdGetStatistics, respch}
	resp, _ := c.FailsafeOp(endpoint.ch, respch, cmd, endpoint.finch)
	return resp[0].(map[string]interface{})
}

// Get the map of endpoint name to pointer for the stats object
func (endpoint *RouterEndpoint) GetStats() map[string]interface{} {
	if atomic.LoadUint32(&endpoint.done) == 0 && endpoint.stats != nil {
		endpStat := make(map[string]interface{}, 0)
		key := fmt.Sprintf(
			"ENDP[<-(%v,%4x)<-%v #%v]",
			endpoint.raddr, uint16(endpoint.timestamp), endpoint.cluster, endpoint.topic)
		endpStat[key] = endpoint.stats
		return endpStat
	}
	return nil
}

func (endpoint *RouterEndpoint) logStats() {
	key := fmt.Sprintf(
		"<-(%v,%4x)<-%v #%v",
		endpoint.raddr, uint16(endpoint.timestamp), endpoint.cluster, endpoint.topic)
	stats := endpoint.stats.String()
	logging.Infof("ENDP[%v] stats: %v", key, stats)
}

// Close this endpoint.
func (endpoint *RouterEndpoint) Close() error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{endpCmdClose, respch}
	resp, err := c.FailsafeOp(endpoint.ch, respch, cmd, endpoint.finch)
	return c.OpError(err, resp, 0)
}

// WaitForExit will block until endpoint exits.
func (endpoint *RouterEndpoint) WaitForExit() error {
	return c.FailsafeOpAsync(nil, []interface{}{}, endpoint.finch)
}

// run
func (endpoint *RouterEndpoint) run(ch chan []interface{}) {
	flushTick := time.NewTicker(endpoint.bufferTm)
	harakiri := time.NewTimer(endpoint.harakiriTm)

	defer func() { // panic safe
		if r := recover(); r != nil {
			logging.Errorf("%v run() crashed: %v\n", endpoint.logPrefix, r)
			logging.Errorf("%s", logging.StackTrace())
		}
		if flushTick != nil {
			flushTick.Stop()
		}
		if harakiri != nil {
			harakiri.Stop()
		}
		// close the connection
		endpoint.conn.Close()
		// close this endpoint
		atomic.StoreUint32(&endpoint.done, 1)
		close(endpoint.finch)
		//Update closed in stats object and log the stats before exiting
		endpoint.stats.closed.Set(true)
		endpoint.logStats()
		logging.Infof("%v ... stopped\n", endpoint.logPrefix)
	}()

	raddr := endpoint.raddr
	lastActiveTime := time.Now()
	buffers := newEndpointBuffers(raddr)

	messageCount := 0
	flushBuffers := func() (err error) {
		fmsg := "%v sent %v mutations to %q\n"
		logging.Tracef(fmsg, endpoint.logPrefix, messageCount, raddr)
		if messageCount > 0 {
			err = buffers.flushBuffers(endpoint, endpoint.conn, endpoint.pkt)
			if err != nil {
				logging.Errorf("%v flushBuffers() %v\n", endpoint.logPrefix, err)
			}
			endpoint.stats.flushCount.Add(1)
		}
		messageCount = 0
		return
	}

loop:
	for {
		select {
		case msg := <-ch:
			switch msg[0].(byte) {
			case endpCmdPing:
				respch := msg[1].(chan []interface{})
				respch <- []interface{}{true}

			case endpCmdSend:
				data, ok := msg[1].(*c.DataportKeyVersions)
				if !ok {
					panic(fmt.Errorf("invalid data type %T\n", msg[1]))
				}

				kv := data.Kv
				buffers.addKeyVersions(
					data.Bucket, data.Vbno, data.Vbuuid,
					data.Opaque2, kv, endpoint)
				logging.Tracef("%v added %v keyversions <%v:%v:%v> to %q\n",
					endpoint.logPrefix, kv.Length(), data.Vbno, kv.Seqno,
					kv.Commands, buffers.raddr)

				messageCount++ // count queued up mutations.
				if messageCount > endpoint.bufferSize {
					if err := flushBuffers(); err != nil {
						break loop
					}
				}

				lastActiveTime = time.Now()

			case endpCmdResetConfig:
				prefix := endpoint.logPrefix
				config := msg[1].(c.Config)
				if cv, ok := config["remoteBlock"]; ok {
					endpoint.block = cv.Bool()
				}
				if cv, ok := config["bufferSize"]; ok {
					endpoint.bufferSize = cv.Int()
				}
				if cv, ok := config["bufferTimeout"]; ok {
					endpoint.bufferTm = time.Duration(cv.Int())
					endpoint.bufferTm *= time.Millisecond
					flushTick.Stop()
					flushTick = time.NewTicker(endpoint.bufferTm)
				}
				if cv, ok := config["harakiriTimeout"]; ok {
					endpoint.harakiriTm = time.Duration(cv.Int())
					endpoint.harakiriTm *= time.Millisecond
					if harakiri != nil { // load harakiri only when it is active
						harakiri.Reset(endpoint.harakiriTm)
						fmsg := "%v reloaded harakiriTm: %v\n"
						logging.Infof(fmsg, prefix, endpoint.harakiriTm)
					}
				}
				respch := msg[2].(chan []interface{})
				respch <- []interface{}{nil}

			case endpCmdGetStatistics: // TODO: this is defunct now.
				respch := msg[1].(chan []interface{})
				stats := endpoint.newStats()
				respch <- []interface{}{map[string]interface{}(stats)}

			case endpCmdClose:
				respch := msg[1].(chan []interface{})
				flushBuffers()
				respch <- []interface{}{nil}
				break loop
			}

		case <-flushTick.C:
			if err := flushBuffers(); err != nil {
				break loop
			}
			// FIXME: Ideally we don't have to reload the harakir here,
			// because _this_ execution path happens only when there is
			// little activity in the data-path. On the other hand,
			// downstream can block for reasons independant of datapath,
			// hence the precaution.
			lastActiveTime = time.Now()

		case <-harakiri.C:
			if time.Since(lastActiveTime) > endpoint.harakiriTm {
				logging.Infof("%v committed harakiri\n", endpoint.logPrefix)
				flushBuffers()
				break loop
			}
			harakiri.Reset(endpoint.harakiriTm)
		}
	}
}

func (endpoint *RouterEndpoint) newStats() c.Statistics {
	m := map[string]interface{}{}
	stats, _ := c.NewStatistics(m)
	return stats
}
