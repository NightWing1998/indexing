// Package indexer file scan replay covers info about how to perform scan replay
package indexer

import (
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	l "github.com/couchbase/indexing/secondary/logging"
)

// ScanBook -> captures the incoming scans. main interface for interaction. APIs - RecordScan, ReplayTopKScans()

// ScanLog -> essential info from scan request

// ScanTitle - aggregated details for scan on defn; count, rows, etc. -> this will be the entitiy
// 		by which the book will be sorted

type scanBook interface {
	RecordScan(scanInfo)
	RecordScan2(*ScanRequest)
	ReplayTopKScans() []scanInfo
	ReplayTopKScans2() []*ScanRequest
}

type scanInfo interface {
	DefnID() uint64
	IndexName() string
	Low() []byte
	High() []byte
	Keys() []IndexKey
	Compare(scanInfo) int
}

// scanLogNode -> node of linkedlist to store scan arrays
type scanLogNode struct {
	req  scanInfo
	next *atomic.Pointer[scanLogNode]
}

// TODO: define marshal
func (node *scanLogNode) Marshal() []byte {
	return nil
}

func newScanLogNode(r scanInfo) *scanLogNode {
	return &scanLogNode{
		req:  r,
		next: new(atomic.Pointer[scanLogNode]),
	}
}

type scanLogList struct {
	head    *atomic.Pointer[scanLogNode]
	len     *atomic.Uint64
	mem     *atomic.Uint64
	flush   sync.Once
	refSync sync.WaitGroup
}

func newScanLogList() *scanLogList {
	ll := new(scanLogList)
	ll.head = new(atomic.Pointer[scanLogNode])
	ll.len = new(atomic.Uint64)
	ll.mem = new(atomic.Uint64)
	return ll
}

func (ll *scanLogList) append(r scanInfo) (uint64, uint64) {
	node := newScanLogNode(r)

	var head = ll.head.Load()
	for ; !ll.head.CompareAndSwap(head, node); head = ll.head.Load() {
		time.Sleep(1 * time.Millisecond)
	}

	node.next.Store(head)
	// ll.head.CompareAndSwap(head, node)

	newListLen := ll.len.Add(1)
	// TODO: sizeof should be of r's struct not interface else sizeof will return mem value of the pointer not the struct
	size := unsafe.Sizeof(r) + unsafe.Sizeof(*node)
	newMemSize := ll.mem.Add(uint64(size))

	return newListLen, newMemSize
}

func (ll *scanLogList) getCurrMemSize() uint64 {
	return ll.mem.Load()
}

func (ll *scanLogList) getCurrLen() uint64 {
	return ll.len.Load()
}

// once a flush is called, the ll structure is useless and cannot be flushed again. any appends are useless
func (ll *scanLogList) saveToDisk(path string) (bool, error) {
	var flushed = false
	var err error

	f, e := os.Create(path)
	if e != nil {
		err = e
		ll.flush = sync.Once{}
		return false, err
	}
	defer f.Close()

	for head := ll.head.Load(); head != nil; head = head.next.Load() {
		f.Write(head.Marshal())
		f.Write([]byte{'\n'})
	}

	flushed = true

	return flushed, err
}

func (ll *scanLogList) incrRefCount() {
	ll.refSync.Add(1)
}

func (ll *scanLogList) decrRefCount() {
	ll.refSync.Done()
}

func (ll *scanLogList) waitForNoRefs() {
	ll.refSync.Wait()
}

// MAIN DS and algo:
// ****the above algo has to be performed on a copy or iterator of the actual titles****
// Heap of a (heapified interval tree or range tree) - Read top k from this heap
// how does it functions psuedocode:
// * heap is a max heap on count times a defnID is scanned. nothing else. structure of the node is count, tree-ish ds
// * pop top element from heap. its a tree-ish ds with 2 properties:
// 		a. interval tree (to combine frequently scanned close intervals. we are scanning a skiplist eventually so this is good)
// 		b. max heap (combined intervals have to kept in a max heap by maybe count or rows scanned - needs to experimented with. what is better more rows or higher count)
// * from the tree-ish ds, we pop the max element, subtract count from the root and insert back in heap (plus run heapify)
// * size of heap is always going to be that of num of indexes. currently we support 10k indexes at max so that is the num nodes
// * above steps give 1 item to replay. continue the process to get top `K` items

// goals of the scan book:
// - after a cold recovery, make the index hot again
// - replay top user scans when cold recovery is done so when we replay the scan book, indexes can serve the hot requests very fast
// - no impact on scan workload of the user
// - bootstrap can get longer
// - restrict mem usage to 50% only after replay
// - has to be disk first

/*
how it will look with disk in picture:
- log_0
- log_1
- log_2
...
^^^ scanLog files

later these get transformed into defn_ptn_tree_heap -
- defn0_ptn3_heaped.tree
- defn0_ptn3_heaped.tree
...
^^^ tree-ish data structure. completely on file. brought in mem only when scan replays are done or log file compaction
*/

type scanBookImpl struct {
	sync.Mutex
	log            atomic.Pointer[scanLogList] // head of the linked list - current log file.
	dir            string
	logFileCounter int
	pastLogs       []*scanLogList
	memLimit       *uint64
	lenLimit       *uint64
}

func (book *scanBookImpl) RecordScan(r scanInfo) {
	var log = book.log.Load()

	log.incrRefCount()
	currLen, currSize := log.append(r)
	log.decrRefCount()

	if currLen > atomic.LoadUint64(book.lenLimit) ||
		currSize > atomic.LoadUint64(book.memLimit) {
		log.flush.Do(func() {
			var newLog = newScanLogList()
			book.log.Store(newLog)
			counter := book.logFileCounter
			book.logFileCounter++

			go func(counter int, log *scanLogList, book *scanBookImpl) {
				log.waitForNoRefs()

				path := filepath.Join(book.dir, "log_"+strconv.Itoa(counter))
				saved, err := log.saveToDisk(path)
				if err != nil || !saved {
					l.Warnf("book::RecordScan: failed to save log to disk with error %v", err)
					book.pastLogs = append(book.pastLogs, log)
				}
			}(counter, log, book)
		})
	}
}

func (book *scanBookImpl) RecordScan2(r *ScanRequest) {}

func (book *scanBookImpl) ReplayTopKScans() []scanInfo {
	return nil
}

func (book *scanBookImpl) ReplayTopKScans2() []*ScanRequest {
	return nil
}

func NewScanBook(logDir string, memLimit, lenLimit *uint64) scanBook {
	book := &scanBookImpl{
		dir:      logDir,
		memLimit: memLimit,
		lenLimit: lenLimit,
	}

	book.log.Store(newScanLogList())

	return book
}
