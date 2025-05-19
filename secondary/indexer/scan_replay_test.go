// Package indexer file scan replay covers info about how to perform scan replay
package indexer

import (
	"testing"
)

func createScanBookImplHelper(t *testing.T) scanBook {
	t.Helper()

	logDir := t.TempDir()
	memLimit := uint64(1024) // 1024b = 1kb
	lenLimit := uint64(10)

	scanBook := NewScanBook(logDir, &memLimit, &lenLimit)

	return scanBook
}

type testScanInfo struct {
	defnId uint64
	name   string
	low    []byte
	high   []byte
	keys   []IndexKey
}

func (tsi *testScanInfo) DefnID() uint64 {
	return tsi.defnId
}

func (tsi *testScanInfo) IndexName() string {
	return tsi.name
}

func (tsi *testScanInfo) Low() []byte {
	return tsi.low
}

func (tsi *testScanInfo) High() []byte {
	return tsi.high
}

func (tsi *testScanInfo) Keys() []IndexKey {
	return tsi.keys
}

func (tsi *testScanInfo) Compare(otherScan scanInfo) int {
	if tsi.defnId == otherScan.DefnID() && tsi.name == otherScan.IndexName() {
		return 0
	}
	return -1
}

func TestRecordScan(t *testing.T) {
	book := createScanBookImplHelper(t)

	var scan = &testScanInfo{defnId: 1, name: "wow"}

	book.RecordScan(scan)

	scanBookInternal := book.(*scanBookImpl)

	log := scanBookInternal.log.Load()

	iter := log.head.Load()
	if iter == nil {
		t.Fatalf("no items in log. expected one item")
	}

	if iter.req.Compare(scan) != 0 {
		t.Fatal("recorded scan and actual scan are not the same")
	}

	if iter.next.Load() != nil {
		t.Fatal("book keeping incorrect")
	}

	t.Logf("good scanner - mem %v, len %v", log.getCurrMemSize(), log.getCurrLen())
}
