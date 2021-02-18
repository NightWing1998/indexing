package common

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/couchbase/indexing/secondary/stats"

	couchbase "github.com/couchbase/indexing/secondary/dcp"

	memcached "github.com/couchbase/indexing/secondary/dcp/transport/client"
	"github.com/couchbase/indexing/secondary/logging"
)

const seqsReqChanSize = 20000
const seqsBufSize = 64 * 1024

var errConnClosed = errors.New("dcpSeqnos - conn closed already")
var errCollectSeqnosPanic = errors.New("Recovered from an error in CollectSeqnos")

// cache Bucket{} and DcpFeed{} objects, its underlying connections
// to make Stats-Seqnos fast.
var dcp_buckets_seqnos struct {
	rw        sync.RWMutex
	numVbs    int
	buckets   map[string]*couchbase.Bucket // bucket ->*couchbase.Bucket
	errors    map[string]error             // bucket -> error
	readerMap VbSeqnosReaderHolder         // bucket->*vbSeqnosReader
}

func init() {
	dcp_buckets_seqnos.buckets = make(map[string]*couchbase.Bucket)
	dcp_buckets_seqnos.errors = make(map[string]error)
	dcp_buckets_seqnos.readerMap.Init()

	go pollForDeletedBuckets()
}

// Holder for VbSeqnosReaderHolder
type VbSeqnosReaderHolder struct {
	ptr *unsafe.Pointer
}

func (readerHolder *VbSeqnosReaderHolder) Init() {
	readerHolder.ptr = new(unsafe.Pointer)
}

func (readerHolder *VbSeqnosReaderHolder) Set(vbseqnosReaderMap map[string]*vbSeqnosReader) {
	atomic.StorePointer(readerHolder.ptr, unsafe.Pointer(&vbseqnosReaderMap))
}

func (readerHolder *VbSeqnosReaderHolder) Get() map[string]*vbSeqnosReader {
	if ptr := atomic.LoadPointer(readerHolder.ptr); ptr != nil {
		return *(*map[string]*vbSeqnosReader)(ptr)
	} else {
		return make(map[string]*vbSeqnosReader)
	}
}

func (readerHolder *VbSeqnosReaderHolder) Clone() map[string]*vbSeqnosReader {
	clone := make(map[string]*vbSeqnosReader)
	if ptr := atomic.LoadPointer(readerHolder.ptr); ptr != nil {
		currMap := *(*map[string]*vbSeqnosReader)(ptr)
		for bucket, reader := range currMap {
			clone[bucket] = reader
		}
	}
	return clone
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

func newKVConn(mc *memcached.Client) *kvConn {
	return &kvConn{mc: mc, seqsbuf: make([]uint64, 1024), tmpbuf: make([]byte, seqsBufSize)}
}

type vbSeqnosRequest chan *vbSeqnosResponse

func (ch *vbSeqnosRequest) Reply(response *vbSeqnosResponse) {
	*ch <- response
}

func (ch *vbSeqnosRequest) Response() ([]uint64, error) {
	response := <-*ch
	return response.seqnos, response.err
}

// Bucket level seqnos reader for the cluster
type vbSeqnosReader struct {
	bucket     string
	kvfeeds    map[string]*kvConn
	requestCh  chan interface{}
	seqsTiming stats.TimingStat
}

func newVbSeqnosReader(bucket string, kvfeeds map[string]*kvConn) *vbSeqnosReader {
	r := &vbSeqnosReader{
		bucket:    bucket,
		kvfeeds:   kvfeeds,
		requestCh: make(chan interface{}, seqsReqChanSize),
	}

	r.seqsTiming.Init()

	go r.Routine()
	return r
}

func (r *vbSeqnosReader) Close() {
	close(r.requestCh)
}

func (r *vbSeqnosReader) GetSeqnos() (seqs []uint64, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errConnClosed
		}
	}()

	req := make(vbSeqnosRequest, 1)
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
// and issue single 'dcp seqno' per batch.
func (r *vbSeqnosReader) Routine() {
	for req := range r.requestCh {

		switch req.(type) {

		case vbSeqnosRequest:
			sreq := req.(vbSeqnosRequest)
			l := len(r.requestCh)
			t0 := time.Now()
			seqnos, err := CollectSeqnos(r.kvfeeds)
			response := &vbSeqnosResponse{
				seqnos: seqnos,
				err:    err,
			}
			r.seqsTiming.Put(time.Since(t0))
			if err != nil {
				dcp_buckets_seqnos.rw.Lock()
				dcp_buckets_seqnos.errors[r.bucket] = err
				dcp_buckets_seqnos.rw.Unlock()
			}
			sreq.Reply(response)

			// Read outstanding requests that can be served by
			// using the same response
			for i := 0; i < l; i++ {
				sreq := <-r.requestCh
				switch sreq.(type) {

				case vbSeqnosRequest:
					//repond to same request type
					req := sreq.(vbSeqnosRequest)
					req.Reply(response)

				case vbMinSeqnosRequest:
					//anything else goes back
					r.enqueueRequest(sreq)

				}
			}

		case vbMinSeqnosRequest:

			sreq := req.(vbMinSeqnosRequest)
			l := len(r.requestCh)
			seqnos, err := CollectMinSeqnos(r.kvfeeds)
			response := &vbSeqnosResponse{
				seqnos: seqnos,
				err:    err,
			}
			if err != nil {
				dcp_buckets_seqnos.rw.Lock()
				dcp_buckets_seqnos.errors[r.bucket] = err
				dcp_buckets_seqnos.rw.Unlock()
			}
			sreq.Reply(response)

			// Read outstanding requests that can be served by
			// using the same response
			for i := 0; i < l; i++ {
				sreq := <-r.requestCh
				switch sreq.(type) {

				case vbMinSeqnosRequest:
					//repond to same request type
					req := sreq.(vbMinSeqnosRequest)
					req.Reply(response)

				case vbSeqnosRequest:
					//anything else goes back
					r.enqueueRequest(sreq)

				}
			}
		}
	}

	// Cleanup all feeds
	for _, kvfeed := range r.kvfeeds {
		kvfeed.mc.Close()
	}
}

func addDBSbucket(cluster, pooln, bucketn string) (err error) {
	var bucket *couchbase.Bucket

	bucket, err = ConnectBucket(cluster, pooln, bucketn)
	if err != nil {
		logging.Errorf("Unable to connect with bucket %q\n", bucketn)
		return err
	}

	kvfeeds := make(map[string]*kvConn)

	defer func() {
		if err == nil {
			dcp_buckets_seqnos.buckets[bucketn] = bucket
			reader := newVbSeqnosReader(bucketn, kvfeeds)
			cloneReaderMap := dcp_buckets_seqnos.readerMap.Clone()
			cloneReaderMap[bucketn] = reader
			dcp_buckets_seqnos.readerMap.Set(cloneReaderMap)

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

	// make sure a feed is available for all kv-nodes
	var conn *memcached.Client

	for kvaddr := range m {
		uuid, _ := NewUUID()
		name := uuid.Str()
		if name == "" {
			err = fmt.Errorf("invalid uuid")
			logging.Errorf("NewUUID() failed: %v\n", err)
			return err
		}
		fname := couchbase.NewDcpFeedName("getseqnos-" + name)
		conn, err = bucket.GetDcpConn(fname, kvaddr)
		if err != nil {
			logging.Errorf("StartDcpFeedOver(): %v\n", err)
			return err
		}
		kvfeeds[kvaddr] = newKVConn(conn)
	}

	logging.Infof("{bucket,feeds} %q created for dcp_seqno cache...\n", bucketn)
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

		cloneReaderMap := dcp_buckets_seqnos.readerMap.Clone()
		reader, ok := cloneReaderMap[bucketn]
		if ok && reader != nil {
			reader.Close()
		}
		delete(cloneReaderMap, bucketn)
		dcp_buckets_seqnos.readerMap.Set(cloneReaderMap)

		delete(dcp_buckets_seqnos.errors, bucketn)
	}
}

func BucketSeqsTiming(bucket string) *stats.TimingStat {
	readerMap := dcp_buckets_seqnos.readerMap.Get()
	if reader, ok := readerMap[bucket]; ok {
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
func BucketSeqnos(cluster, pooln, bucketn string) (l_seqnos []uint64, err error) {
	// any type of error will cleanup the bucket and its kvfeeds.
	defer func() {
		if err != nil {
			delDBSbucket(bucketn, true)
		}
	}()

	var reader *vbSeqnosReader

	reader, err = func() (*vbSeqnosReader, error) {
		readerMap := dcp_buckets_seqnos.readerMap.Get()
		reader, ok := readerMap[bucketn]
		if !ok { // no {bucket,kvfeeds} found, create!
			dcp_buckets_seqnos.rw.Lock()
			defer dcp_buckets_seqnos.rw.Unlock()

			// Recheck if reader is still not present since we acquired write lock
			// after releasing the read lock.
			readerMap = dcp_buckets_seqnos.readerMap.Get()
			if reader, ok = readerMap[bucketn]; !ok {
				if err = addDBSbucket(cluster, pooln, bucketn); err != nil {
					return nil, err
				}
				readerMap = dcp_buckets_seqnos.readerMap.Get()
				// addDBSbucket has populated the reader
				reader = readerMap[bucketn]
			}
		}
		return reader, nil
	}()
	if err != nil {
		return nil, err
	}

	l_seqnos, err = reader.GetSeqnos()
	return
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

func CollectSeqnos(kvfeeds map[string]*kvConn) (l_seqnos []uint64, err error) {
	defer func() {
		if r := recover(); r != nil {
			// Return error as callers take care of retry.
			logging.Errorf("%v: number of kvfeeds is %d", errCollectSeqnosPanic, len(kvfeeds))
			l_seqnos = nil
			err = errCollectSeqnosPanic
		}
	}()

	var wg sync.WaitGroup

	// Buffer for storing kv_seqs from each node
	kv_seqnos_node := make([][]uint64, len(kvfeeds))
	errors := make([]error, len(kvfeeds))

	if len(kvfeeds) == 0 {
		err = fmt.Errorf("Empty kvfeeds")
		logging.Errorf("CollectSeqnos:: %v", err)
		return nil, err
	}

	i := 0
	for _, feed := range kvfeeds {
		wg.Add(1)
		go func(index int, feed *kvConn) {
			defer wg.Done()
			kv_seqnos_node[index] = feed.seqsbuf
			errors[index] = couchbase.GetSeqs(feed.mc, kv_seqnos_node[index], feed.tmpbuf)
		}(i, feed)
		i++
	}

	wg.Wait()

	seqnos := kv_seqnos_node[0]
	for i, kv_seqnos := range kv_seqnos_node {
		err := errors[i]
		if err != nil {
			logging.Errorf("feed.DcpGetSeqnos(): %v\n", err)
			return nil, err
		}

		for vbno, seqno := range kv_seqnos {
			prev := seqnos[vbno]
			if prev < seqno {
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
			readerMap := dcp_buckets_seqnos.readerMap.Get()
			for bucketn, bucket = range dcp_buckets_seqnos.buckets {
				if m, err := bucket.GetVBmap(nil); err != nil {
					// idle detect failures.
					todels = append(todels, bucketn)
				} else if len(m) != len(readerMap[bucketn].kvfeeds) {
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

type vbMinSeqnosRequest chan *vbSeqnosResponse

func (ch *vbMinSeqnosRequest) Reply(response *vbSeqnosResponse) {
	*ch <- response
}

func (ch *vbMinSeqnosRequest) Response() ([]uint64, error) {
	response := <-*ch
	return response.seqnos, response.err
}

// BucketMinSeqnos return list of {{vbno,seqno}..} for all vbuckets.
// this call might fail due to,
// - concurrent access that can preserve a deleted/failed bucket object.
// - pollForDeletedBuckets() did not get a chance to cleanup
//   a deleted bucket.
// in both the cases if the call is retried it should get fixed, provided
// a valid bucket exists.
func BucketMinSeqnos(cluster, pooln, bucketn string) (l_seqnos []uint64, err error) {
	// any type of error will cleanup the bucket and its kvfeeds.
	defer func() {
		if err != nil {
			delDBSbucket(bucketn, true)
		}
	}()

	var reader *vbSeqnosReader

	reader, err = func() (*vbSeqnosReader, error) {
		readerMap := dcp_buckets_seqnos.readerMap.Get()
		reader, ok := readerMap[bucketn]
		if !ok { // no {bucket,kvfeeds} found, create!
			dcp_buckets_seqnos.rw.Lock()
			defer dcp_buckets_seqnos.rw.Unlock()

			// Recheck if reader is still not present since we acquired write lock
			// after releasing the read lock.
			readerMap = dcp_buckets_seqnos.readerMap.Get()
			if reader, ok = readerMap[bucketn]; !ok {
				if err = addDBSbucket(cluster, pooln, bucketn); err != nil {
					return nil, err
				}
				readerMap = dcp_buckets_seqnos.readerMap.Get()
				// addDBSbucket has populated the reader
				reader = readerMap[bucketn]
			}
		}
		return reader, nil
	}()
	if err != nil {
		return nil, err
	}

	l_seqnos, err = reader.GetMinSeqnos()
	return
}

func (r *vbSeqnosReader) GetMinSeqnos() (seqs []uint64, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errConnClosed
		}
	}()

	req := make(vbMinSeqnosRequest, 1)
	r.requestCh <- req
	seqs, err = req.Response()
	return
}

func CollectMinSeqnos(kvfeeds map[string]*kvConn) (l_seqnos []uint64, err error) {
	defer func() {
		if r := recover(); r != nil {
			// Return error as callers take care of retry.
			logging.Errorf("%v: number of kvfeeds is %d", errCollectSeqnosPanic, len(kvfeeds))
			l_seqnos = nil
			err = errCollectSeqnosPanic
		}
	}()

	var wg sync.WaitGroup

	// Buffer for storing kv_seqs from each node
	kv_seqnos_node := make([][]uint64, len(kvfeeds))
	errors := make([]error, len(kvfeeds))

	if len(kvfeeds) == 0 {
		err = fmt.Errorf("Empty kvfeeds")
		logging.Errorf("CollectMinSeqnos:: %v", err)
		return nil, err
	}

	i := 0
	for _, feed := range kvfeeds {
		wg.Add(1)
		go func(index int, feed *kvConn) {
			defer wg.Done()
			kv_seqnos_node[index] = feed.seqsbuf
			errors[index] = couchbase.GetSeqsAllVbStates(feed.mc, kv_seqnos_node[index], feed.tmpbuf)
		}(i, feed)
		i++
	}

	wg.Wait()

	seqnos := kv_seqnos_node[0]
	for i, kv_seqnos := range kv_seqnos_node {
		err := errors[i]
		if err != nil {
			logging.Errorf("feed.CollectMinSeqnos(): %v\n", err)
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
