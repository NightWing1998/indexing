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
	"github.com/couchbase/indexing/secondary/common"
	"sync/atomic"
)

//-----------------------------
// Queue with a rotating buffer
//-----------------------------

type allocator struct {
	bufPool *common.BytesBufPool
	unused  [][]byte

	size  int64
	count int64
	head  int64
	free  int64

	//stats
	numMalloc int64
}

type Row struct {
	key  []byte
	len  int
	last bool

	mem *allocator
}

type Queue struct {
	size    int64
	count   int64
	limit   int64
	isClose int32

	buf  []Row
	head int64 // pos of head of buf
	free int64 // pos of head of free list

	notifych chan bool
	enqch    chan bool
	deqch    chan bool
	donech   chan bool

	enqCount int64
	deqCount int64

	mem *allocator
}

//
// Constructor
//
func NewQueue(size int64, limit int64, notifych chan bool, mem *allocator) *Queue {

	rbuf := &Queue{}
	rbuf.size = size
	rbuf.limit = limit
	rbuf.buf = make([]Row, size)
	rbuf.notifych = notifych
	rbuf.enqch = make(chan bool, 1)
	rbuf.deqch = make(chan bool, 1)
	rbuf.donech = make(chan bool)
	rbuf.mem = mem

	for i, _ := range rbuf.buf {
		rbuf.buf[i].init(rbuf.mem)
	}

	return rbuf
}

//
// This funciton notifies a new row added to buffer.
//
func (b *Queue) notifyEnq() {

	select {
	case b.enqch <- true:
	default:
	}

	if b.notifych != nil {
		select {
		case b.notifych <- true:
		default:
		}
	}
}

//
// This funciton notifies a new row removed from buffer
//
func (b *Queue) notifyDeq() {

	select {
	case b.deqch <- true:
	default:
	}
}

//
// Add a new row to the rotating buffer.  If the buffer is full,
// this function will be blocked.
//
func (b *Queue) Enqueue(key *Row) {

	for {
		count := atomic.LoadInt64(&b.count)

		if count < b.size {
			next := b.free + 1
			if next >= b.size {
				next = 0
			}

			b.buf[b.free].copy(key)

			b.free = next
			if atomic.AddInt64(&b.count, 1) == b.limit || key.last {
				b.notifyEnq()
			}

			atomic.AddInt64(&b.enqCount, 1)
			return
		}

		select {
		case <-b.deqch:
		case <-b.donech:
			return
		}
	}
}

//
// Remove the first row from the rotating buffer.  If the buffer is empty,
// this function will be blocked.
//
func (b *Queue) Dequeue(row *Row) bool {

	for {
		count := atomic.LoadInt64(&b.count)

		if count > 0 {
			next := b.head + 1
			if next >= b.size {
				next = 0
			}

			row.copy(&b.buf[b.head])

			b.buf[b.head].freeKeyBuf()
			b.head = next

			if atomic.AddInt64(&b.count, -1) == (b.size - 1) {
				b.notifyDeq()
			}

			atomic.AddInt64(&b.deqCount, 1)
			return true
		}

		select {
		case <-b.enqch:
		case <-b.donech:
			return false
		}
	}

	return false
}

//
// Get a copy of the first row in the rotating buffer.  This
// function is not blocked and will not return a copy.
//
func (b *Queue) Peek(row *Row) bool {

	count := atomic.LoadInt64(&b.count)

	if count > 0 {
		row.copy(&b.buf[b.head])
		return true
	}

	return false
}

func (b *Queue) Len() int64 {

	return atomic.LoadInt64(&b.count)
}

func (b *Queue) Cap() int64 {

	return b.size
}

func (b *Queue) EnqueueCount() int64 {

	return b.enqCount
}

func (b *Queue) DequeueCount() int64 {

	return b.deqCount
}

// Unblock all Enqueue and Dequeue calls
func (b *Queue) Close() {

	if atomic.SwapInt32(&b.isClose, 1) == 0 {
		close(b.donech)
	}
}

func (b *Queue) Free() {
	for i := 0; i < int(b.size); i++ {
		b.buf[i].freeKeyBuf()
	}
}

func (b *Queue) GetBytesBuf() *common.BytesBufPool {
	return b.mem.bufPool
}

func (b *Queue) GetAllocator() *allocator {
	return b.mem
}

//-----------------------------
// allocator
//-----------------------------

func newAllocator(size int64, bufPool *common.BytesBufPool) *allocator {

	r := &allocator{
		size:    size,
		unused:  make([][]byte, size),
		bufPool: bufPool,
	}

	return r
}

func (r *allocator) get() []byte {

	count := atomic.LoadInt64(&r.count)

	if count == 0 || r.size == 0 {
		atomic.AddInt64(&r.numMalloc, 1)
		bufPtr := r.bufPool.Get()
		return (*bufPtr)[:0]
	}

	bufPtr := r.unused[r.head]
	r.unused[r.head] = nil

	next := r.head + 1
	if next >= r.size {
		next = 0
	}
	r.head = next

	atomic.AddInt64(&r.count, -1)

	return bufPtr[:0]
}

func (r *allocator) put(buf []byte) {

	if buf == nil {
		return
	}

	count := atomic.LoadInt64(&r.count)

	if count == r.size || r.size == 0 {
		r.bufPool.Put(&buf)
		return
	}

	r.unused[r.free] = buf

	next := r.free + 1
	if next >= r.size {
		next = 0
	}
	r.free = next

	atomic.AddInt64(&r.count, 1)
}

func (r *allocator) Free() bool {

	for i := 0; i < int(r.size); i++ {
		if r.unused[i] != nil {
			r.bufPool.Put(&r.unused[i])
			r.unused[i] = nil
		}
	}

	r.count = 0
	r.free = 0
	r.head = 0

	return true
}

//-----------------------------
// Row
//-----------------------------

func (r *Row) copy(source *Row) {

	r.len = source.len
	r.last = source.last
	r.copyKey(source.key)
}

func (r *Row) copyKey(key []byte) {
	if len(key) == 0 {
		if r.key != nil {
			r.key = r.key[:0]
		}
		return
	}

	if r.key == nil {
		r.initKeyBuf()
	}

	if len(key) > cap(r.key) {
		r.key = make([]byte, 0, len(key))
	}

	r.key = r.key[:0]
	r.key = append(r.key, key...)

	return
}

func (r *Row) initKeyBuf() {
	bufPtr := r.mem.get()
	r.key = bufPtr[:0]
}

func (r *Row) freeKeyBuf() {
	if r.key != nil {
		r.mem.put(r.key)
		r.key = nil
	}
}

func (r *Row) init(mem *allocator) {
	r.mem = mem
}
