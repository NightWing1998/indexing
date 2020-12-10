package common

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/indexing/secondary/security"

	couchbase "github.com/couchbase/indexing/secondary/dcp"
	memcached "github.com/couchbase/indexing/secondary/dcp/transport/client"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/stats"
)

const seqsReqChanSize = 20000
const seqsBufSize = 64 * 1024

//TODO (Collections): Should number of workers be configurable?
const workersPerReader = 10

const BUCKET_ID string = ""

var clusterVersion int64

var errConnClosed = errors.New("dcpSeqnos - conn closed already")
var errFetchSeqnosPanic = errors.New("Recovered from an error in FetchSeqnos")

// cache Bucket{} and DcpFeed{} objects, its underlying connections
// to make Stats-Seqnos fast.
var dcp_buckets_seqnos struct {
	rw        sync.RWMutex
	numVbs    int
	buckets   map[string]*couchbase.Bucket // bucket ->*couchbase.Bucket
	errors    map[string]error             // bucket -> error
	readerMap map[string]*vbSeqnosReader   // bucket->*vbSeqnosReader
}

func init() {
	dcp_buckets_seqnos.buckets = make(map[string]*couchbase.Bucket)
	dcp_buckets_seqnos.errors = make(map[string]error)
	dcp_buckets_seqnos.readerMap = make(map[string]*vbSeqnosReader)

	go pollForDeletedBuckets()
}

type vbSeqnosResponse struct {
	seqnos []uint64
	err    error
}

type kvConn struct {
	mc      *memcached.Client
	seqsbuf []uint64
	tmpbuf  []byte
}

func newKVConn(mc *memcached.Client, numVbs int) *kvConn {
	return &kvConn{mc: mc, seqsbuf: make([]uint64, numVbs), tmpbuf: make([]byte, seqsBufSize)}
}

type vbSeqnosRequest struct {
	cid         string
	bucketLevel bool
	respCh      chan *vbSeqnosResponse
	numQueued   int
}

func (req *vbSeqnosRequest) Reply(response *vbSeqnosResponse) {
	req.respCh <- response
}

func (req *vbSeqnosRequest) Response() ([]uint64, error) {
	response := <-req.respCh
	return response.seqnos, response.err
}

//------------------------------------------
// Worker implementation
//------------------------------------------

type worker struct {
	bucket       string
	workerId     int
	reqCh        chan vbSeqnosRequest // channel on which reader (dispatcher) sends msg on
	internalCh   chan workerResult    // communication channel between two worker routines
	workerQueue  chan vbSeqnosRequest // channel on which processRequest sends request to fetchSeqnos
	dispatcherCh chan workerDoneMsg   // channel on worker communicates back with dispatcher (reader)

	kvfeeds   map[string]*kvConn           // Map of kvaddr -> mc conn
	workerMap map[string][]vbSeqnosRequest // Map collId to list of requests containing response channels
	reader    *vbSeqnosReader              // The dispatcher (reader) which owns the worker
	donech    chan bool                    // Indicate done to worker routines processRequest and fetchSeqnos
	wg        *sync.WaitGroup              // Indicate that worker is done on this WaitGroup
}

// a workerDoneMsg message is sent from worker's processRequest
// goroutine to dispatcher to indicate worker is done with a collectionID
type workerDoneMsg struct {
	cid      string
	workerId int
}

// a workerResult message is sent from worker's fetchSeqnos
// goroutine to processRequest goroutine.
type workerResult struct {
	cid       string
	seqs      []uint64
	numQueued int
	err       error
}

// create a new worker with its own set of kvfeeds
// Start two goroutines: one to process request from dispatcher
// and the other to fetch seqnos from KV
func newWorker(workerid int, bucket string, dispCh chan workerDoneMsg,
	feeds map[string]*kvConn, wg *sync.WaitGroup, vbsr *vbSeqnosReader) *worker {

	w := &worker{
		workerId:     workerid,
		bucket:       bucket,
		reqCh:        make(chan vbSeqnosRequest, seqsReqChanSize),
		internalCh:   make(chan workerResult, seqsReqChanSize),
		workerQueue:  make(chan vbSeqnosRequest, seqsReqChanSize),
		dispatcherCh: dispCh,

		kvfeeds:   feeds,
		workerMap: make(map[string][]vbSeqnosRequest),
		reader:    vbsr,
		donech:    make(chan bool),
		wg:        wg,
	}

	wg.Add(1)
	go w.processRequest()
	go w.fetchSeqnos()

	return w
}

// Listens to messages on three channels:
// 1. reqCh: Receives message from dispatcher and sends cid to the other goroutine
//    if not already being processed
// 2. internalCh: Receives result from other goroutine and replies on all queued
//    response channels for that collection.
// 3. donech: shutdown message from dispatcher (vbSeqnosReader)
func (w *worker) processRequest() {

	processResponse := func(resp workerResult) {
		cid := resp.cid
		queuedReqs := w.workerMap[cid]
		delete(w.workerMap, cid)

		response := &vbSeqnosResponse{
			seqnos: resp.seqs,
			err:    resp.err,
		}

		if resp.err != nil {
			dcp_buckets_seqnos.rw.Lock()
			dcp_buckets_seqnos.errors[w.bucket] = resp.err
			dcp_buckets_seqnos.rw.Unlock()
		}

		for i := 0; i < resp.numQueued; i++ {
			queuedReqs[i].Reply(response)
		}
	}

	defer func() {
		// Close workerQueue so that it will not accept any new requests
		close(w.workerQueue)

		// Respond back to all out-standing requests in internalCh
		for response := range w.internalCh {
			processResponse(response)
		}

		// Respond back to all out-standing requests in worker reqCh
		for req := range w.reqCh {
			resp := &vbSeqnosResponse{
				seqnos: nil,
				err:    errors.New("vbSeqnosWorker is closed. Retry the operation"),
			}
			req.Reply(resp)
		}

		w.wg.Done()
	}()

loop:
	for {
		select {
		case req, ok := <-w.reqCh:
			if ok {
				if queuedReqs, exists := w.workerMap[req.cid]; exists {
					queuedReqs = append(queuedReqs, req)
					w.workerMap[req.cid] = queuedReqs
				} else {
					queuedReqs := make([]vbSeqnosRequest, 0)
					queuedReqs = append(queuedReqs, req)
					w.workerMap[req.cid] = queuedReqs
					req.numQueued = len(queuedReqs)
					w.workerQueue <- req
				}
			}
		case resp, ok := <-w.internalCh:
			if ok {
				queuedReqs := w.workerMap[resp.cid]
				processResponse(resp)

				newQueuedReqs := queuedReqs[resp.numQueued:]
				l := len(newQueuedReqs)
				if l == 0 {
					w.dispatcherCh <- workerDoneMsg{cid: resp.cid, workerId: w.workerId}
				} else {
					lastReq := newQueuedReqs[l-1]
					w.workerMap[lastReq.cid] = newQueuedReqs
					lastReq.numQueued = l
					w.workerQueue <- lastReq
				}
			}
		case <-w.donech:
			break loop
		}
	}
}

// Listens to messages on two channels:
// 1. workerQueue: Receives message from other worker routine and fetches sequence
//    numbers for the collection.
//    if not already being processed
// 2. donech: shutdown message from dispatcher (vbSeqnosReader)
func (w *worker) fetchSeqnos() {

	defer func() {
		// Close internalCh so that no new results can be pushed
		close(w.internalCh)

		// Respond back to all out-standing requests in workerQueu
		for req := range w.workerQueue {
			resp := &vbSeqnosResponse{
				seqnos: nil,
				err:    errors.New("vbSeqnosWorker is closed. Retry the operation"),
			}
			req.Reply(resp)
		}
	}()

loop:
	for {
		select {
		case req, ok := <-w.workerQueue:
			if ok {
				cid := req.cid

				// Get KV Seqnum for bucket or collection
				t0 := time.Now()
				seqnos, err := FetchSeqnos(w.kvfeeds, cid, req.bucketLevel)
				w.reader.seqsTiming.Put(time.Since(t0))
				w.internalCh <- workerResult{
					cid:       cid,
					seqs:      seqnos,
					numQueued: req.numQueued,
					err:       err,
				}
			}
		case <-w.donech:
			break loop
		}
	}

	// Cleanup all feeds
	for _, kvfeed := range w.kvfeeds {
		kvfeed.mc.Close()
	}
}

//------------------------------------------
// Worker implementation end
//------------------------------------------

//------------------------------------------
// vbSeqnosReader implementation
//------------------------------------------

// Bucket level seqnos reader for the cluster
type vbSeqnosReader struct {
	bucket     string
	seqsTiming stats.TimingStat

	requestCh    chan interface{}   // request channel for Seqnos processing
	donech       chan bool          // channel used to shut down the vbSeqnosReader main routine
	workers      []*worker          // list of workers who actually process Seqnos
	workerRespCh chan workerDoneMsg // channel on which workers communicate back with dispatcher
	wg           sync.WaitGroup     // Wait group to track completion of all workers

	// Book keeping information about whether a task
	// is currently queued in any worker for processing
	dispatcherMap map[string]int

	kvfeeds     map[string]*kvConn      // Connections used for MinSeqnos processings
	minSeqReqCh chan vbMinSeqnosRequest // request channel for MinSeqnos processing

}

func newVbSeqnosReader(cluster, pooln, bucket string,
	kvfeeds map[string]*kvConn) (*vbSeqnosReader, error) {

	r := &vbSeqnosReader{
		bucket:        bucket,
		requestCh:     make(chan interface{}, seqsReqChanSize),
		donech:        make(chan bool),
		workers:       make([]*worker, workersPerReader),
		workerRespCh:  make(chan workerDoneMsg, 100),
		dispatcherMap: make(map[string]int),
		kvfeeds:       kvfeeds,
		minSeqReqCh:   make(chan vbMinSeqnosRequest, seqsReqChanSize),
	}

	r.seqsTiming.Init()

	mu := &sync.Mutex{}
	errSlice := make([]error, workersPerReader)
	var wg sync.WaitGroup

	// Init the workers
	for i := 0; i < workersPerReader; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			kvfeeds, err := getKVFeeds(cluster, pooln, bucket)
			if err != nil {
				mu.Lock()
				errSlice[index] = err
				mu.Unlock()
				return
			}
			w := newWorker(index, bucket, r.workerRespCh, kvfeeds, &r.wg, r)
			mu.Lock()
			r.workers[index] = w
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	// Process the error map and close all workers in case
	// an error was observed while opening feed for any worker
	for _, errObs := range errSlice {
		if errObs != nil {
			for _, w := range r.workers {
				if w != nil {
					for _, kvf := range w.kvfeeds {
						kvf.mc.Close()
					}
				}
			}
			return nil, errObs
		}
	}

	go r.Routine()
	go r.processMinSeqNos()

	return r, nil
}

func (r *vbSeqnosReader) Close() {
	for _, w := range r.workers {
		close(w.donech)
	}
	close(r.donech)
}

func (r *vbSeqnosReader) GetBucketSeqnos() (seqs []uint64, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errConnClosed
		}
	}()

	req := vbSeqnosRequest{
		cid:         BUCKET_ID,
		bucketLevel: true,
		respCh:      make(chan *vbSeqnosResponse, 1),
	}

	r.requestCh <- req
	seqs, err = req.Response()
	return
}

func (r *vbSeqnosReader) GetCollectionSeqnos(cid string) (seqs []uint64, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errConnClosed
		}
	}()

	req := vbSeqnosRequest{
		cid:         cid,
		bucketLevel: false,
		respCh:      make(chan *vbSeqnosResponse, 1),
	}

	r.requestCh <- req
	seqs, err = req.Response()
	return
}

func (r *vbSeqnosReader) enqueueRequest(req interface{}) {
	defer func() {
		if rec := recover(); rec != nil {
			//if requestCh is closed, reader has closed already
			logging.Errorf("vbSeqnosReader::enqueueRequest Request channel closed for bucket: %v", r.bucket)
			// Respond back to outstanding callers
			response := &vbSeqnosResponse{
				seqnos: nil,
				err:    errConnClosed,
			}
			switch req.(type) {

			case vbSeqnosRequest:
				//repond to same request type
				inReq := req.(vbSeqnosRequest)
				inReq.Reply(response)

			case vbMinSeqnosRequest:
				//anything else goes back
				inReq := req.(vbMinSeqnosRequest)
				inReq.Reply(response)
			}
		}
	}()

	r.requestCh <- req
	return
}

// This routine is responsible for computing request batches on the fly
// and issue single 'dcp seqno' per batch which is actually done in worker routines
// This main routine listens on three channels:
// 1. requestCh: The requests of type vbSeqnosRequest are "dispatched" to workers
//    to be processed in batched way. vbMinSeqnosRequest are sent to another routine
//    that does not batch. This is because minSeqnos requests are infrequent and they
//    do not need to be batched.
// 2. workerRespCh: a message from worker that it is done with a collection id (and its
//    batch of requests so that dispatcher (i.e. the vbSeqnosReader) can clear its book-keeping
// 3. donech: shutdown down the vbSeqnosReader
func (r *vbSeqnosReader) Routine() {

	defer func() {
		close(r.workerRespCh)
		close(r.requestCh)
		close(r.minSeqReqCh)

		// drain the request channel
		for req := range r.requestCh {
			resp := &vbSeqnosResponse{
				seqnos: nil,
				err:    errors.New("vbSeqnosReader is closed. Retry the operation"),
			}
			switch req.(type) {
			case vbSeqnosRequest:
				sreq := req.(vbSeqnosRequest)
				sreq.Reply(resp)
			case vbMinSeqnosRequest:
				sreq := req.(vbMinSeqnosRequest)
				sreq.Reply(resp)
			}
		}
	}()

loop:
	for {
		select {
		case req, ok := <-r.requestCh:
			if ok {
				switch req.(type) {

				case vbSeqnosRequest:
					sreq := req.(vbSeqnosRequest)

					// If cid is being processed by a worker,
					// dispatch this request to that worker.
					if workerId, exists := r.dispatcherMap[sreq.cid]; exists {
						r.workers[workerId].reqCh <- sreq
					} else {
						// Dispatch to least loaded worker
						workerId := r.getNextWorker()
						r.workers[workerId].reqCh <- sreq
						r.dispatcherMap[sreq.cid] = workerId
					}
				case vbMinSeqnosRequest:
					sreq := req.(vbMinSeqnosRequest)
					r.minSeqReqCh <- sreq
				}
			}

		case wMsg, ok := <-r.workerRespCh:
			if ok {
				if wid, exists := r.dispatcherMap[wMsg.cid]; exists {
					if wMsg.workerId == wid {
						delete(r.dispatcherMap, wMsg.cid)
					}
				}
			}
		case <-r.donech:
			break loop
		}
	}

	for _, w := range r.workers {
		close(w.reqCh)
	}

	r.wg.Wait()
}

func (r *vbSeqnosReader) getNextWorker() int {

	//TODO (Collections): Switch to least loaded worker logic
	// Find least size of all workers queues
	// Find the workers with least size
	// Choose a random among those least loaded workers

	// For now, going with random worker approach for simplicity
	// always pick a random worker from all workers
	return randomNum(0, len(r.workers)-1)
}

func (r *vbSeqnosReader) processMinSeqNos() {
	for req := range r.minSeqReqCh {
		seqnos, err := FetchMinSeqnos(r.kvfeeds, req.cid, req.bucketLevel)
		response := &vbSeqnosResponse{
			seqnos: seqnos,
			err:    err,
		}
		if err != nil {
			dcp_buckets_seqnos.rw.Lock()
			dcp_buckets_seqnos.errors[r.bucket] = err
			dcp_buckets_seqnos.rw.Unlock()
		}
		req.Reply(response)
	}

	// Cleanup all feeds
	for _, kvfeed := range r.kvfeeds {
		kvfeed.mc.Close()
	}
}

func getKVFeeds(cluster, pooln, bucketn string) (map[string]*kvConn, error) {
	var bucket *couchbase.Bucket
	var err error
	var clustVer int

	bucket, clustVer, err = ConnectBucket2(cluster, pooln, bucketn)
	if err != nil {
		logging.Errorf("Unable to connect with bucket %q\n", bucketn)
		return nil, err
	}

	UpdateClusterVersion((int64)(clustVer))
	kvfeeds := make(map[string]*kvConn)

	defer func() {
		if err != nil {
			for _, kvfeed := range kvfeeds {
				kvfeed.mc.Close()
			}
		}
	}()

	// get all kv-nodes
	if err = bucket.Refresh(); err != nil {
		logging.Errorf("bucket.Refresh(): %v\n", err)
		return nil, err
	}

	// get current list of kv-nodes
	var m map[string][]uint16
	m, err = bucket.GetVBmap(nil)
	if err != nil {
		logging.Errorf("GetVBmap() failed: %v\n", err)
		return nil, err
	}

	// Empty kv-nodes list without error should never happen.
	// Return an error and caller can retry on error if needed.
	if len(m) == 0 {
		err = fmt.Errorf("Empty kv-nodes list")
		logging.Errorf("addDBSbucket:: Error %v for bucket %v", err, bucketn)
		return nil, err
	}

	// calculate and cache the number of vbuckets.
	if dcp_buckets_seqnos.numVbs == 0 { // to happen only first time.
		for _, vbnos := range m {
			dcp_buckets_seqnos.numVbs += len(vbnos)
		}
	}

	if dcp_buckets_seqnos.numVbs == 0 {
		err = fmt.Errorf("Found 0 vbuckets - perhaps the bucket is not ready yet")
		return nil, err
	}

	// make sure a feed is available for all kv-nodes
	var conn *memcached.Client
	connMap := make(map[string]string) // key-> local addr, value -> remote addr
	for kvaddr := range m {

		conn, err = bucket.GetMcConn(kvaddr)
		if err != nil {
			logging.Errorf("GetMcConn(): %v\n", err)
			return nil, err
		}

		if conn.GetLocalAddr() != "" {
			connMap[conn.GetLocalAddr()] = conn.GetRemoteAddr()
		}

		if clustVer >= INDEXER_70_VERSION {
			err := tryEnableCollection(conn)
			if err != nil {
				logging.Errorf("feed.DcpGetSeqnos() error while enabling collection for connection: %v -> %v",
					conn.GetLocalAddr(), conn.GetRemoteAddr())
				return nil, err
			}
		}
		kvfeeds[kvaddr] = newKVConn(conn, dcp_buckets_seqnos.numVbs)
	}

	logging.Infof("{bucket,feeds} %q created for dcp_seqno worker cache..., established connections: %v\n", bucketn, connMap)
	return kvfeeds, nil
}

func addDBSbucket(cluster, pooln, bucketn string) (err error) {
	var bucket *couchbase.Bucket
	var clustVer int

	bucket, clustVer, err = ConnectBucket2(cluster, pooln, bucketn)
	if err != nil {
		logging.Errorf("Unable to connect with bucket %q\n", bucketn)
		return err
	}
	UpdateClusterVersion((int64)(clustVer))

	kvfeeds := make(map[string]*kvConn)

	defer func() {
		if err == nil {
			dcp_buckets_seqnos.buckets[bucketn] = bucket
			reader, e := newVbSeqnosReader(cluster, pooln, bucketn, kvfeeds)
			if e == nil {
				dcp_buckets_seqnos.readerMap[bucketn] = reader
			} else {
				err = e
			}
		} else {
			for _, kvfeed := range kvfeeds {
				kvfeed.mc.Close()
			}
		}
	}()

	// get all kv-nodes
	if err = bucket.Refresh(); err != nil {
		logging.Errorf("bucket.Refresh(): %v\n", err)
		return err
	}

	// get current list of kv-nodes
	var m map[string][]uint16
	m, err = bucket.GetVBmap(nil)
	if err != nil {
		logging.Errorf("GetVBmap() failed: %v\n", err)
		return err
	}

	// Empty kv-nodes list without error should never happen.
	// Return an error and caller can retry on error if needed.
	if len(m) == 0 {
		err = fmt.Errorf("Empty kv-nodes list")
		logging.Errorf("addDBSbucket:: Error %v for bucket %v", err, bucketn)
		return err
	}

	// calculate and cache the number of vbuckets.
	if dcp_buckets_seqnos.numVbs == 0 { // to happen only first time.
		for _, vbnos := range m {
			dcp_buckets_seqnos.numVbs += len(vbnos)
		}
	}

	if dcp_buckets_seqnos.numVbs == 0 {
		err = fmt.Errorf("Found 0 vbuckets - perhaps the bucket is not ready yet")
		return
	}

	connMap := make(map[string]string)
	// make sure a feed is available for all kv-nodes
	var conn *memcached.Client
	for kvaddr := range m {
		conn, err = bucket.GetMcConn(kvaddr)
		if err != nil {
			logging.Errorf("GetMcConn(): %v\n", err)
			return err
		}

		if conn.GetLocalAddr() != "" {
			connMap[conn.GetLocalAddr()] = conn.GetRemoteAddr()
		}

		if clustVer >= INDEXER_70_VERSION {
			err = tryEnableCollection(conn)
			if err != nil {
				return err
			}
		}
		kvfeeds[kvaddr] = newKVConn(conn, dcp_buckets_seqnos.numVbs)
	}

	logging.Infof("{bucket,feeds} %q created for dcp_seqno cache..., established connections: %v\n", bucketn, connMap)
	return nil
}

func delDBSbucket(bucketn string, checkErr bool) {
	dcp_buckets_seqnos.rw.Lock()
	defer dcp_buckets_seqnos.rw.Unlock()

	if !checkErr || dcp_buckets_seqnos.errors[bucketn] != nil {
		bucket, ok := dcp_buckets_seqnos.buckets[bucketn]
		if ok && bucket != nil {
			bucket.Close()
		}
		delete(dcp_buckets_seqnos.buckets, bucketn)

		reader, ok := dcp_buckets_seqnos.readerMap[bucketn]
		if ok && reader != nil {
			reader.Close()
		}
		delete(dcp_buckets_seqnos.readerMap, bucketn)

		delete(dcp_buckets_seqnos.errors, bucketn)
	}
}

func BucketSeqsTiming(bucket string) *stats.TimingStat {
	dcp_buckets_seqnos.rw.RLock()
	defer dcp_buckets_seqnos.rw.RUnlock()
	if reader, ok := dcp_buckets_seqnos.readerMap[bucket]; ok {
		return &reader.seqsTiming
	}

	return nil
}

// BucketSeqnos return list of {{vbno,seqno}..} for all vbuckets.
// this call might fail due to,
// - concurrent access that can preserve a deleted/failed bucket object.
// - pollForDeletedBuckets() did not get a chance to cleanup
//   a deleted bucket.
// in both the cases if the call is retried it should get fixed, provided
// a valid bucket exists.
// This method fetches Bucket level seqnos
func BucketSeqnos(cluster, pooln, bucketn string) (l_seqnos []uint64, err error) {
	// any type of error will cleanup the bucket and its kvfeeds.
	defer func() {
		if err != nil {
			delDBSbucket(bucketn, true)
		}
	}()

	var reader *vbSeqnosReader

	reader, err = func() (*vbSeqnosReader, error) {
		dcp_buckets_seqnos.rw.RLock()
		reader, ok := dcp_buckets_seqnos.readerMap[bucketn]
		dcp_buckets_seqnos.rw.RUnlock()
		if !ok { // no {bucket,kvfeeds} found, create!
			dcp_buckets_seqnos.rw.Lock()
			defer dcp_buckets_seqnos.rw.Unlock()

			// Recheck if reader is still not present since we acquired write lock
			// after releasing the read lock.
			if reader, ok = dcp_buckets_seqnos.readerMap[bucketn]; !ok {
				if err = addDBSbucket(cluster, pooln, bucketn); err != nil {
					return nil, err
				}
				// addDBSbucket has populated the reader
				reader = dcp_buckets_seqnos.readerMap[bucketn]
			}
		}
		return reader, nil
	}()
	if err != nil {
		return nil, err
	}

	l_seqnos, err = reader.GetBucketSeqnos()
	return
}

// Sequence numbers for a specific collection
func CollectionSeqnos(cluster, pooln, bucketn string,
	cid string) (l_seqnos []uint64, err error) {

	// any type of error will cleanup the bucket and its kvfeeds.
	defer func() {
		if err != nil {
			delDBSbucket(bucketn, true)
		}
	}()

	var reader *vbSeqnosReader

	reader, err = func() (*vbSeqnosReader, error) {
		dcp_buckets_seqnos.rw.RLock()
		reader, ok := dcp_buckets_seqnos.readerMap[bucketn]
		dcp_buckets_seqnos.rw.RUnlock()
		if !ok { // no {bucket,kvfeeds} found, create!
			dcp_buckets_seqnos.rw.Lock()
			defer dcp_buckets_seqnos.rw.Unlock()

			// Recheck if reader is still not present since we acquired write lock
			// after releasing the read lock.
			if reader, ok = dcp_buckets_seqnos.readerMap[bucketn]; !ok {
				if err = addDBSbucket(cluster, pooln, bucketn); err != nil {
					return nil, err
				}
				// addDBSbucket has populated the reader
				reader = dcp_buckets_seqnos.readerMap[bucketn]
			}
		}
		return reader, nil
	}()
	if err != nil {
		return nil, err
	}

	l_seqnos, err = reader.GetCollectionSeqnos(cid)
	return
}

func GetSeqnos(cluster, pool, bucket, cid string) (l_seqnos []uint64, err error) {

	if cid != DEFAULT_COLLECTION_ID {
		return CollectionSeqnos(cluster, pool, bucket, cid)
	} else {
		globalClustVer := atomic.LoadInt64(&clusterVersion)
		if globalClustVer >= INDEXER_70_VERSION {
			return CollectionSeqnos(cluster, pool, bucket, cid)
		}
	}
	return BucketSeqnos(cluster, pool, bucket)
}

func GetMinSeqnos(cluster, pool, bucket, cid string) (l_seqnos []uint64, err error) {

	if cid != DEFAULT_COLLECTION_ID {
		return CollectionMinSeqnos(cluster, pool, bucket, cid)
	} else {
		globalClustVer := atomic.LoadInt64(&clusterVersion)
		if globalClustVer >= INDEXER_70_VERSION {
			return CollectionMinSeqnos(cluster, pool, bucket, cid)
		}
	}
	return BucketMinSeqnos(cluster, pool, bucket)
}

func ResetBucketSeqnos() error {
	dcp_buckets_seqnos.rw.Lock()

	bucketns := make([]string, 0, len(dcp_buckets_seqnos.buckets))

	for bucketn, _ := range dcp_buckets_seqnos.buckets {
		bucketns = append(bucketns, bucketn)
	}

	dcp_buckets_seqnos.rw.Unlock()

	for _, bucketn := range bucketns {
		delDBSbucket(bucketn, false)
	}

	return nil
}

func FetchSeqnos(kvfeeds map[string]*kvConn, cid string, bucketLevel bool) (l_seqnos []uint64, err error) {
	defer func() {
		if r := recover(); r != nil {
			// Return error as callers take care of retry.
			logging.Errorf("%v: number of kvfeeds is %d", errFetchSeqnosPanic, len(kvfeeds))
			l_seqnos = nil
			err = errFetchSeqnosPanic
		}
	}()

	var wg sync.WaitGroup

	// Buffer for storing kv_seqs from each node
	kv_seqnos_node := make(map[string][]uint64)
	errors := make(map[string]error, len(kvfeeds))
	var mu sync.Mutex

	if len(kvfeeds) == 0 {
		err = fmt.Errorf("Empty kvfeeds")
		logging.Errorf("FetchSeqnos:: %v", err)
		return nil, err
	}

	for kvaddr, feed := range kvfeeds {
		wg.Add(1)
		go func(kvaddress string, feed *kvConn) {
			defer wg.Done()

			if bucketLevel {
				err := couchbase.GetSeqs(feed.mc, feed.seqsbuf, feed.tmpbuf)
				if err != nil {
					mu.Lock()
					errors[kvaddress] = err
					mu.Unlock()
					return
				}
			} else {
				// A call to CollectionSeqnos implies cluster is fully upgraded to 7.0
				err := tryEnableCollection(feed.mc)
				if err != nil {
					mu.Lock()
					errors[kvaddress] = err
					mu.Unlock()
					return
				}
				err = couchbase.GetCollectionSeqs(feed.mc, feed.seqsbuf, feed.tmpbuf, cid)
				if err != nil {
					mu.Lock()
					errors[kvaddress] = err
					mu.Unlock()
					return
				}
			}

			mu.Lock()
			kv_seqnos_node[kvaddress] = feed.seqsbuf
			mu.Unlock()

		}(kvaddr, feed)
	}

	wg.Wait()

	var seqnos []uint64
	i := 0
	for kvaddr, kv_seqnos := range kv_seqnos_node {
		if i == 0 {
			seqnos = kv_seqnos
		}
		err := errors[kvaddr]
		if err != nil {
			conn := kvfeeds[kvaddr].mc
			logging.Errorf("feed.DcpGetSeqnos(): %v from node: %v\n", err, conn.GetRemoteAddr())
			return nil, err
		}

		for vbno, seqno := range kv_seqnos {
			prev := seqnos[vbno]
			if prev < seqno {
				seqnos[vbno] = seqno
			}
		}
		i++
	}
	// The following code is to detect rebalance or recovery !!
	// this is not yet supported in KV, GET_SEQNOS returns all
	// seqnos.
	if len(seqnos) < dcp_buckets_seqnos.numVbs {
		fmsg := "unable to get seqnos ts for all vbuckets (%v out of %v)"
		err = fmt.Errorf(fmsg, len(seqnos), dcp_buckets_seqnos.numVbs)
		logging.Errorf("%v\n", err)
		return nil, err
	}
	// sort them
	vbnos := make([]int, 0, dcp_buckets_seqnos.numVbs)
	for vbno := range seqnos {
		vbnos = append(vbnos, int(vbno))
	}
	sort.Ints(vbnos)
	// gather seqnos.
	l_seqnos = make([]uint64, 0, dcp_buckets_seqnos.numVbs)
	for _, vbno := range vbnos {
		l_seqnos = append(l_seqnos, seqnos[uint16(vbno)])
	}

	return l_seqnos, nil
}

func pollForDeletedBuckets() {
	for {
		time.Sleep(10 * time.Second)
		todels := []string{}
		func() {
			dcp_buckets_seqnos.rw.Lock()
			defer dcp_buckets_seqnos.rw.Unlock()
			for bucketn, bucket := range dcp_buckets_seqnos.buckets {
				if bucket.Refresh() != nil {
					// lazy detect bucket deletes
					todels = append(todels, bucketn)
				}
			}
		}()
		func() {
			var bucketn string
			var bucket *couchbase.Bucket

			dcp_buckets_seqnos.rw.RLock()
			defer func() {
				if r := recover(); r != nil {
					logging.Warnf("failover race in bucket: %v", r)
					todels = append(todels, bucketn)
				}
				dcp_buckets_seqnos.rw.RUnlock()
			}()
			for bucketn, bucket = range dcp_buckets_seqnos.buckets {
				if m, err := bucket.GetVBmap(nil); err != nil {
					// idle detect failures.
					todels = append(todels, bucketn)
				} else if len(m) != len(dcp_buckets_seqnos.readerMap[bucketn].kvfeeds) {
					// lazy detect kv-rebalance
					todels = append(todels, bucketn)
				}
			}
		}()
		for _, bucketn := range todels {
			delDBSbucket(bucketn, false)
		}
	}
}

func SetDcpMemcachedTimeout(val uint32) {
	memcached.SetDcpMemcachedTimeout(val)
}

//MinSeqnos Implementation
type vbMinSeqnosRequest struct {
	cid         string
	bucketLevel bool
	respCh      chan *vbSeqnosResponse
}

func (req *vbMinSeqnosRequest) Reply(response *vbSeqnosResponse) {
	req.respCh <- response
}

func (req *vbMinSeqnosRequest) Response() ([]uint64, error) {
	response := <-req.respCh
	return response.seqnos, response.err
}

// BucketMinSeqnos return list of {{vbno,seqno}..} for all vbuckets.
// this call might fail due to,
// - concurrent access that can preserve a deleted/failed bucket object.
// - pollForDeletedBuckets() did not get a chance to cleanup
//   a deleted bucket.
// in both the cases if the call is retried it should get fixed, provided
// a valid bucket exists.
// This method fetches Bucket level min seqnos
func BucketMinSeqnos(cluster, pooln, bucketn string) (l_seqnos []uint64, err error) {
	// any type of error will cleanup the bucket and its kvfeeds.
	defer func() {
		if err != nil {
			delDBSbucket(bucketn, true)
		}
	}()

	var reader *vbSeqnosReader

	reader, err = func() (*vbSeqnosReader, error) {
		dcp_buckets_seqnos.rw.RLock()
		reader, ok := dcp_buckets_seqnos.readerMap[bucketn]
		dcp_buckets_seqnos.rw.RUnlock()
		if !ok { // no {bucket,kvfeeds} found, create!
			dcp_buckets_seqnos.rw.Lock()
			defer dcp_buckets_seqnos.rw.Unlock()

			// Recheck if reader is still not present since we acquired write lock
			// after releasing the read lock.
			if reader, ok = dcp_buckets_seqnos.readerMap[bucketn]; !ok {
				if err = addDBSbucket(cluster, pooln, bucketn); err != nil {
					return nil, err
				}
				// addDBSbucket has populated the reader
				reader = dcp_buckets_seqnos.readerMap[bucketn]
			}
		}
		return reader, nil
	}()
	if err != nil {
		return nil, err
	}

	l_seqnos, err = reader.GetBucketMinSeqnos()
	return
}

// Sequence numbers for a specific collections
func CollectionMinSeqnos(cluster, pooln, bucketn string, cid string) (l_seqnos []uint64, err error) {
	// any type of error will cleanup the bucket and its kvfeeds.
	defer func() {
		if err != nil {
			delDBSbucket(bucketn, true)
		}
	}()

	var reader *vbSeqnosReader

	reader, err = func() (*vbSeqnosReader, error) {
		dcp_buckets_seqnos.rw.RLock()
		reader, ok := dcp_buckets_seqnos.readerMap[bucketn]
		dcp_buckets_seqnos.rw.RUnlock()
		if !ok { // no {bucket,kvfeeds} found, create!
			dcp_buckets_seqnos.rw.Lock()
			defer dcp_buckets_seqnos.rw.Unlock()

			// Recheck if reader is still not present since we acquired write lock
			// after releasing the read lock.
			if reader, ok = dcp_buckets_seqnos.readerMap[bucketn]; !ok {
				if err = addDBSbucket(cluster, pooln, bucketn); err != nil {
					return nil, err
				}
				// addDBSbucket has populated the reader
				reader = dcp_buckets_seqnos.readerMap[bucketn]
			}
		}
		return reader, nil
	}()
	if err != nil {
		return nil, err
	}

	l_seqnos, err = reader.GetCollectionMinSeqnos(cid)
	return
}

func (r *vbSeqnosReader) GetBucketMinSeqnos() (seqs []uint64, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errConnClosed
		}
	}()

	req := vbMinSeqnosRequest{
		cid:         BUCKET_ID,
		bucketLevel: true,
		respCh:      make(chan *vbSeqnosResponse, 1),
	}

	r.requestCh <- req
	seqs, err = req.Response()
	return
}

func (r *vbSeqnosReader) GetCollectionMinSeqnos(cid string) (seqs []uint64, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errConnClosed
		}
	}()

	req := vbMinSeqnosRequest{
		cid:         cid,
		bucketLevel: false,
		respCh:      make(chan *vbSeqnosResponse, 1),
	}

	r.requestCh <- req
	seqs, err = req.Response()
	return
}

func FetchMinSeqnos(kvfeeds map[string]*kvConn, cid string, bucketLevel bool) (l_seqnos []uint64, err error) {
	defer func() {
		if r := recover(); r != nil {
			// Return error as callers take care of retry.
			logging.Errorf("%v: number of kvfeeds is %d", errFetchSeqnosPanic, len(kvfeeds))
			l_seqnos = nil
			err = errFetchSeqnosPanic
		}
	}()

	var wg sync.WaitGroup

	// Buffer for storing kv_seqs from each node
	kv_seqnos_node := make([][]uint64, len(kvfeeds))
	errors := make([]error, len(kvfeeds))

	if len(kvfeeds) == 0 {
		err = fmt.Errorf("Empty kvfeeds")
		logging.Errorf("FetchMinSeqnos:: %v", err)
		return nil, err
	}

	i := 0
	for _, feed := range kvfeeds {
		wg.Add(1)
		go func(index int, feed *kvConn) {
			defer wg.Done()
			kv_seqnos_node[index] = feed.seqsbuf
			if bucketLevel {
				errors[index] = couchbase.GetSeqsAllVbStates(feed.mc,
					kv_seqnos_node[index], feed.tmpbuf)
			} else {
				// A call to CollectionMinSeqnos implies cluster is fully upgraded to 7.0
				err = tryEnableCollection(feed.mc)
				if err != nil {
					errors[index] = err
					return
				}
				errors[index] = couchbase.GetCollectionSeqsAllVbStates(feed.mc,
					kv_seqnos_node[index], feed.tmpbuf, cid)
			}

		}(i, feed)
		i++
	}

	wg.Wait()

	seqnos := kv_seqnos_node[0]
	for i, kv_seqnos := range kv_seqnos_node {
		err := errors[i]
		if err != nil {
			logging.Errorf("feed.FetchMinSeqnos(): %v\n", err)
			return nil, err
		}

		for vbno, seqno := range kv_seqnos {
			prev := seqnos[vbno]
			//in case of no replica, seqnum is 0
			if prev == 0 {
				seqnos[vbno] = seqno
			} else if prev > seqno &&
				seqno != 0 {
				seqnos[vbno] = seqno
			}
		}
	}
	// The following code is to detect rebalance or recovery !!
	// this is not yet supported in KV, GET_SEQNOS returns all
	// seqnos.
	if len(seqnos) < dcp_buckets_seqnos.numVbs {
		fmsg := "unable to get seqnos ts for all vbuckets (%v out of %v)"
		err = fmt.Errorf(fmsg, len(seqnos), dcp_buckets_seqnos.numVbs)
		logging.Errorf("%v\n", err)
		return nil, err
	}
	// sort them
	vbnos := make([]int, 0, dcp_buckets_seqnos.numVbs)
	for vbno := range seqnos {
		vbnos = append(vbnos, int(vbno))
	}
	sort.Ints(vbnos)
	// gather seqnos.
	l_seqnos = make([]uint64, 0, dcp_buckets_seqnos.numVbs)
	for _, vbno := range vbnos {
		l_seqnos = append(l_seqnos, seqnos[uint16(vbno)])
	}
	return l_seqnos, nil
}

var ErrNoEntry = errors.New("Entry not found in Failover Log")
var ErrIncompleteLog = errors.New("Incomplete Failover Log")

// FailoverLog containing vbuuid and sequence number
type vbFlog [][2]uint64
type FailoverLog map[int]vbFlog

// Latest will return the recent vbuuid and its high-seqno.
func (flog FailoverLog) Latest(vb int) (vbuuid, seqno uint64, err error) {
	if flog != nil {
		if fl, ok := flog[vb]; ok {
			latest := fl[0]
			return latest[0], latest[1], nil
		}
	}
	return vbuuid, seqno, ErrNoEntry
}

// LowestVbuuid would return the lowest vbuuid for a given seqno
// if the entry is found
func (flog FailoverLog) LowestVbuuid(vb int, seqno uint64) (vbuuid uint64, err error) {
	if flog != nil {
		if fl, ok := flog[vb]; ok {
			for _, f := range fl {
				if f[1] == seqno {
					vbuuid = f[0]
				}
			}
		}
	}
	if vbuuid != 0 {
		return
	}
	return vbuuid, ErrNoEntry
}

func BucketFailoverLog(cluster, pooln, bucketn string, numVb int) (fl FailoverLog, ret error) {

	//panic safe
	defer func() {
		if r := recover(); r != nil {
			ret = fmt.Errorf("%v", r)
			logging.Warnf("BucketFailoverLog failed : %v", ret)
		}
	}()

	logging.Infof("BucketFailoverLog %v", bucketn)

	bucket, err := ConnectBucket(cluster, pooln, bucketn)
	if err != nil {
		logging.Warnf("BucketFailoverLog failed : %v", err)
		ret = err
		return
	}
	defer bucket.Close()

	vbnos := listOfVbnos(numVb)
	dcpConfig := map[string]interface{}{
		"genChanSize":    10000,
		"dataChanSize":   10000,
		"numConnections": 1,
	}

	failoverLog := make(FailoverLog)
	flogs, err := bucket.GetFailoverLogs(0 /*opaque*/, vbnos, dcpConfig)

	if err == nil {
		if len(flogs) != numVb {
			ret = ErrIncompleteLog
			return
		}
		for vbno, flog := range flogs {
			vbflog := make(vbFlog, len(flog))
			for i, x := range flog {
				vbflog[i][0] = x[0]
				vbflog[i][1] = x[1]
			}
			failoverLog[int(vbno)] = vbflog
		}
	} else {
		logging.Warnf("BucketFailoverLog failed: %v", err)
		ret = err
		return
	}
	logging.Infof("BucketFailoverLog returns %v ", failoverLog)

	fl = failoverLog
	return
}

func listOfVbnos(maxVbno int) []uint16 {
	// list of vbuckets
	vbnos := make([]uint16, 0, maxVbno)
	for i := 0; i < maxVbno; i++ {
		vbnos = append(vbnos, uint16(i))
	}
	return vbnos
}

func randomNum(min, max int) int {
	rand.Seed(time.Now().UnixNano())
	Min, Max := float64(min), float64(max)
	return int(rand.Float64()*(Max-Min) + Min)
}

func getConnName() (string, error) {
	uuid, _ := NewUUID()
	name := uuid.Str()
	if name == "" {
		err := fmt.Errorf("getConnName: invalid uuid.")

		// probably not a good idea to fail if uuid
		// based name fails. Can return const string
		return "", err
	}
	connName := "secidx:getseqnos" + name
	return connName, nil
}

func tryEnableCollection(conn *memcached.Client) error {
	if !conn.IsCollectionsEnabled() {

		connName, err := getConnName()
		if err != nil {
			return err
		}

		conn.SetMcdConnectionDeadline()
		defer conn.ResetMcdConnectionDeadline()

		err = conn.EnableCollections(connName)
		if err != nil {
			return err
		}
	}
	return nil
}

func UpdateClusterVersion(clustVer int64) {
	for {
		globalClustVer := atomic.LoadInt64(&clusterVersion)
		if clustVer > globalClustVer {
			atomic.CompareAndSwapInt64(&clusterVersion, globalClustVer, clustVer)
		} else {
			return
		}
	}
}

// This method is a light weight version of serviceChangeNotifier
// only used to retrieve and update cluster version.
// Session consistent scans will use this cluster version information
// to choose between retrieving CollectionSeqnos and BucketSeqnos
func WatchClusterVersionChanges(clusterAddr string) {

	selfRestart := func() {
		time.Sleep(10 * time.Millisecond)
		go WatchClusterVersionChanges(clusterAddr)
		return
	}

	path := "/poolsStreaming/" + DEFAULT_POOL
	params := &security.RequestParams{
		UserAgent: "WatchClusterVersionChanges",
	}
	res, err := security.GetWithAuth(clusterAddr+path, params)
	if err != nil {
		logging.Errorf("WatchClusterVersionChanges: Error while getting with auth, err: %v", err)
		selfRestart()
		return
	}

	if res.StatusCode != 200 {
		bod, _ := ioutil.ReadAll(io.LimitReader(res.Body, 512))
		res.Body.Close()

		logging.Errorf("WatchClusterVersionChanges: HTTP error %v getting %q: %s", res.Status, path, bod)
		selfRestart()
		return
	}

	var p couchbase.Pool

	reader := bufio.NewReader(res.Body)
	defer res.Body.Close()
	for {

		if atomic.LoadInt64(&clusterVersion) >= INDEXER_70_VERSION {
			logging.Infof("WatchClusterVersionChanges: Cluster version is >= INDEXER_70_VERSION")
			return
		}

		bs, err := reader.ReadBytes('\n')
		if err != nil {
			logging.Errorf("WatchClusterVersionChanges: Error while reading body, err: %v", err)
			selfRestart()
			return
		}
		if len(bs) == 1 && bs[0] == '\n' {
			continue
		}

		err = json.Unmarshal(bs, &p)
		if err != nil {
			logging.Errorf("WatchClusterVersionChanges: Error while unmarshalling pools, err: %v", err)
			selfRestart()
			return
		}

		clusterCompat := math.MaxInt32
		for _, node := range p.Nodes {
			if node.ClusterCompatibility < clusterCompat {
				clusterCompat = node.ClusterCompatibility
			}
		}

		clustVer := 0
		if clusterCompat != math.MaxInt32 {
			version := clusterCompat / 65536
			minorVersion := clusterCompat - (version * 65536)
			clustVer = int(GetVersion(uint32(version), uint32(minorVersion)))
		}

		// Will update cluster version only if there is a change
		UpdateClusterVersion((int64)(clustVer))
	}
}

func GetClusterVersion() int64 {
	return atomic.LoadInt64(&clusterVersion)
}
