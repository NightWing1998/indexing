// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.
package client

import "time"
import "unsafe"
import "io"
import "net"
import "sync/atomic"
import "fmt"
import "syscall"
import "strings"
import "sync"
import "io/ioutil"
import "errors"
import commonjson "github.com/couchbase/indexing/secondary/common/json"

import "github.com/couchbase/indexing/secondary/logging"
import "github.com/couchbase/indexing/secondary/common"
import "github.com/couchbase/indexing/secondary/security"
import mclient "github.com/couchbase/indexing/secondary/manager/client"
import "github.com/couchbase/query/value"

// TODO:
// - Timeit() uses the wall-clock time instead of process-time to compute
//   load. This is very crude.

// Identify an instance of resoponse handler
type ResponseHandlerId int

// ResponseHandler shall interpret response packets from server
// and handle them. If handler is not interested in receiving any
// more response it shall return false, else it shall continue
// until *protobufEncode.StreamEndResponse message is received.
type ResponseHandler func(resp ResponseReader) bool

// ResponseReader to obtain the actual data returned from server,
// handlers, should first call Error() and then call GetEntries().
type ResponseReader interface {
	// GetEntries returns a list of secondary-key and corresponding
	// primary-key if returned value is nil, then there are no more
	// entries for this query.
	GetEntries(dataEncFmt common.DataEncodingFormat) (*common.ScanResultEntries, [][]byte, error)

	// Error returns the error value, if nil there is no error.
	Error() error
}

// ResponseSender is responsible for forwarding result to the client
// after streams from multiple servers/ResponseHandler have been merged.
// mskey - marshalled sec key (as Value)
// uskey - unmarshalled sec key (as byte)
type ResponseSender func(pkey []byte, mskey []value.Value, uskey common.ScanResultKey, tmpbuf *[]byte) (bool, *[]byte)

// ResponseHandlerFactory returns an instance of ResponseHandler
type ResponseHandlerFactory func(id ResponseHandlerId, instId uint64, partitions []common.PartitionId) ResponseHandler

// ScanRequestHandler initiates a request to a single server connection
type ScanRequestHandler func(*GsiScanClient, *common.IndexDefn, int64, []common.PartitionId, ResponseHandler) (error, bool)

// CountRequestHandler initiates a request to a single server connection
type CountRequestHandler func(*GsiScanClient, *common.IndexDefn, int64, []common.PartitionId) (int64, error, bool)

// ResponseTimer updates timing of responses
type ResponseTimer func(instID uint64, partitionId common.PartitionId, value float64)

// ResponseWaiter for backfill done
type BackfillWaiter func()

// scanClientMaker fetches a scan client
type scanClientMaker func(scanport string) *GsiScanClient

// Remoteaddr string in the shape of "<host:port>"
type Remoteaddr string

// Inclusion specifier for range queries.
type Inclusion uint32

type Scans []*Scan

type Scan struct {
	Seek   common.SecondaryKey
	Filter []*CompositeElementFilter
}

type CompositeElementFilter struct {
	Low       interface{}
	High      interface{}
	Inclusion Inclusion
}

type IndexProjection struct {
	EntryKeys  []int64
	PrimaryKey bool
}

//Groupby/Aggregate
type GroupKey struct {
	EntryKeyId int32  // Id that can be used in IndexProjection
	KeyPos     int32  // >=0 means use expr at index key position otherwise use Expr
	Expr       string // group expression
}

type Aggregate struct {
	AggrFunc   common.AggrFuncType // Aggregate operation
	EntryKeyId int32               // Id that can be used in IndexProjection
	KeyPos     int32               // >=0 means use expr at index key position otherwise use Expr
	Expr       string              // Aggregate expression
	Distinct   bool                // Aggregate only on Distinct values with in the group
}

type GroupAggr struct {
	Name               string       // name of the index aggregate
	Group              []*GroupKey  // group keys, nil means no group by
	Aggrs              []*Aggregate // aggregates with in the group, nil means no aggregates
	DependsOnIndexKeys []int32      // GROUP and Aggregates Depends on List of index keys positions
	IndexKeyNames      []string     // Index key names used in expressions
	AllowPartialAggr   bool         // Partial aggregates are allowed
	OnePerPrimaryKey   bool         // Leading Key is ALL & equality span consider one per docid
}

type IndexKeyOrder struct {
	KeyPos []int
	Desc   []bool
}

const (
	// Neither does not include low-key and high-key
	Neither Inclusion = iota
	// Low includes low-key but does not include high-key
	Low
	// High includes high-key but does not include low-key
	High
	// Both includes both low-key and high-key
	Both
)

// BridgeAccessor for Create,Drop,List,Refresh operations.
type BridgeAccessor interface {
	// Synchronously update current server metadata to the client
	// A Refresh call followed by a Sync() ensures that client is
	// up to date wrt the server.
	Sync() error

	// Refresh shall refresh to latest set of index managed by GSI
	// cluster, cache it locally and return the list of index.
	Refresh() ([]*mclient.IndexMetadata, uint64, uint64, error)

	// Nodes shall return a map of adminport and queryport for indexer
	// nodes.
	Nodes() ([]*IndexerService, error)

	// CreateIndex and return defnID of created index.
	// name
	//      index name
	// bucket
	//      bucket name in which index is defined.
	// using
	//      token should always be GSI.
	// exprType
	//      token specifies how in interpret partnExpr, whereExpr, secExprs
	// partnExpr
	//      marshalled expression of type `exprType` that emits partition
	//      value from a kv-document.
	// whereExpr
	//      marshalled predicate-expression of type `exprType` that emits
	//      a boolean from a kv-document.
	// secExprs
	//      marshalled list of expression of type `exprType` that emits
	//      an array of secondary-key values from a kv-document.
	// isPrimary
	//      specify whether the index is created on docid.
	// with
	//      JSON marshalled description about index deployment (and more...).
	CreateIndex(
		name, bucket, using, exprType, whereExpr string,
		secExprs []string, desc []bool, isPrimary bool,
		scheme common.PartitionScheme, partitionKeys []string,
		with []byte) (defnID uint64, err error)

	// BuildIndexes to build a deferred set of indexes. This call implies
	// that indexes specified are already created.
	BuildIndexes(defnIDs []uint64) error

	// MoveIndex to move a set of indexes to different node.
	MoveIndex(defnID uint64, with map[string]interface{}) error

	// AlterReplicaCount to change replica count of index
	AlterReplicaCount(action string, defnID uint64, with map[string]interface{}) error

	// DropIndex to drop index specified by `defnID`.
	// - if index is in deferred build state, it shall be removed
	//   from deferred list.
	DropIndex(defnID uint64) error

	// GetScanports shall return list of queryports for all indexer in
	// the cluster.
	GetScanports() (queryports []string)

	// GetScanport shall fetch queryport address for indexer,
	// if `retry` is ZERO, pick the indexer under least
	// load, else do a round-robin, based on the retry count,
	// if more than one indexer is found hosing the index or an
	// equivalent index.
	GetScanport(
		defnID uint64,
		excludes map[common.IndexDefnId]map[common.PartitionId]map[uint64]bool,
		skips map[common.IndexDefnId]bool) (queryport []string, targetDefnID uint64, targetInstID []uint64,
		rollbackTime []int64, partition [][]common.PartitionId, numPartitions uint32, ok bool)

	// GetIndexDefn will return the index-definition structure for defnID.
	GetIndexDefn(defnID uint64) *common.IndexDefn

	// GetIndexInst will return the index-instance structure for instId.
	GetIndexInst(instId uint64) *mclient.InstanceDefn

	// GetIndexReplica will return the index-instance structure for defnId.
	GetIndexReplica(defnId uint64) []*mclient.InstanceDefn

	// IndexState returns the current state of index `defnID` and error.
	IndexState(defnID uint64) (common.IndexState, error)

	// IsPrimary returns whether index is on primary key.
	IsPrimary(defnID uint64) bool

	//Return the number of replica and equivalent indexes
	NumReplica(defnID uint64) int

	// Timeit will add `value` to incrementalAvg for index-load.
	Timeit(instID uint64, partitionId common.PartitionId, value float64)

	// Close this accessor.
	Close()
}

// GsiAccessor for index operation on GSI cluster.
type GsiAccessor interface {
	BridgeAccessor

	// LookupStatistics for a single secondary-key.
	LookupStatistics(
		defnID uint64, requestId string, v common.SecondaryKey) (common.IndexStatistics, error)

	// RangeStatistics for index range.
	RangeStatistics(
		defnID uint64, requestId string, low, high common.SecondaryKey,
		inclusion Inclusion) (common.IndexStatistics, error)

	// Lookup scan index between low and high.
	Lookup(
		defnID uint64, requestId string, values []common.SecondaryKey,
		distinct bool, limit int64,
		cons common.Consistency, vector *TsConsistency,
		callb ResponseHandler) error

	// Lookup scan index between low and high.
	LookupInternal(
		defnID uint64, requestId string, values []common.SecondaryKey,
		distinct bool, limit int64,
		cons common.Consistency, vector *TsConsistency,
		broker *RequestBroker) error

	// Range scan index between low and high.
	Range(
		defnID uint64, requestId string, low, high common.SecondaryKey,
		inclusion Inclusion, distinct bool, limit int64,
		cons common.Consistency, vector *TsConsistency,
		callb ResponseHandler) error

	// Range scan index between low and high.
	RangeInternal(
		defnID uint64, requestId string, low, high common.SecondaryKey,
		inclusion Inclusion, distinct bool, limit int64,
		cons common.Consistency, vector *TsConsistency,
		broker *RequestBroker) error

	// ScanAll for full table scan.
	ScanAll(
		defnID uint64, requestId string, limit int64,
		cons common.Consistency, vector *TsConsistency,
		callb ResponseHandler) error

	// ScanAll for full table scan.
	ScanAllInternal(
		defnID uint64, requestId string, limit int64,
		cons common.Consistency, vector *TsConsistency,
		broker *RequestBroker) error

	// Multiple scans with composite index filters
	MultiScan(
		defnID uint64, requestId string, scans Scans,
		reverse, distinct bool, projection *IndexProjection, offset, limit int64,
		cons common.Consistency, vector *TsConsistency,
		callb ResponseHandler) error

	// Multiple scans with composite index filters
	MultiScanInternal(
		defnID uint64, requestId string, scans Scans,
		reverse, distinct bool, projection *IndexProjection, offset, limit int64,
		cons common.Consistency, vector *TsConsistency,
		broker *RequestBroker) error

	// CountLookup of all entries in index.
	CountLookup(
		defnID uint64, requestId string, values []common.SecondaryKey,
		cons common.Consistency, vector *TsConsistency) (int64, error)

	// CountLookup of all entries in index.
	CountLookupInternal(
		defnID uint64, requestId string, values []common.SecondaryKey,
		cons common.Consistency, vector *TsConsistency,
		broker *RequestBroker) (int64, error)

	// CountRange of all entries in index.
	CountRange(
		defnID uint64, requestId string,
		low, high common.SecondaryKey, inclusion Inclusion,
		cons common.Consistency, vector *TsConsistency) (int64, error)

	// CountRange of all entries in index.
	CountRangeInternal(
		defnID uint64, requestId string,
		low, high common.SecondaryKey, inclusion Inclusion,
		cons common.Consistency, vector *TsConsistency,
		broker *RequestBroker) (int64, error)

	// Count using MultiScan
	MultiScanCount(
		defnID uint64, requestId string,
		scans Scans, distinct bool,
		cons common.Consistency, vector *TsConsistency) (int64, error)

	// Count using MultiScan
	MultiScanCountInternal(
		defnID uint64, requestId string,
		scans Scans, distinct bool,
		cons common.Consistency, vector *TsConsistency,
		broker *RequestBroker) (int64, error)

	// Scan API3 with grouping and aggregates support
	Scan3(
		defnID uint64, requestId string, scans Scans,
		reverse, distinct bool, projection *IndexProjection, offset, limit int64,
		groupAggr *GroupAggr,
		cons common.Consistency, vector *TsConsistency,
		callb ResponseHandler) error

	// Scan API3 with grouping and aggregates support
	Scan3Internal(
		defnID uint64, requestId string, scans Scans,
		reverse, distinct bool, projection *IndexProjection, offset, limit int64,
		groupAggr *GroupAggr,
		cons common.Consistency, vector *TsConsistency,
		broker *RequestBroker) error

	// StorageStatistics API4 for getting per partition storage stats.
	// Return value is a slice of maps, each map is storage stats per partition
	StorageStatistics(defnID uint64, requestId string) ([]map[string]interface{}, error)
}

var useMetadataProvider = true
var pInitOnce sync.Once

// IndexerService returns the status of the indexer node
// as observed by the GsiClient.
type IndexerService struct {
	Adminport string
	Queryport string
	Httpport  string
	Status    string // one of "initial", "online", "recovery"
}

// GsiClient for accessing GSI cluster. The client shall
// use `adminport` for meta-data operation and `queryport`
// for index-scan related operations.
type GsiClient struct {
	bridge       BridgeAccessor // manages adminport
	cluster      string
	maxvb        int
	config       common.Config
	queryClients unsafe.Pointer // map[string(queryport)]*GsiScanClient
	bucketHash   unsafe.Pointer // map[string]uint64 // bucket -> crc64
	metaCh       chan bool      // listen to metadata changes
	settings     *ClientSettings
	killch       chan bool
	numScans     int64
	scanResponse int64
	dataEncFmt   uint32
}

// NewGsiClient returns client to access GSI cluster.
func NewGsiClient(
	cluster string, config common.Config) (c *GsiClient, err error) {

	return NewGsiClientWithSettings(cluster, config, false, true)
}

func NewGsiClientWithSettings(
	cluster string, config common.Config, needRefresh bool, encryptLocalHost bool) (c *GsiClient, err error) {

	if useMetadataProvider {
		c, err = makeWithMetaProvider(cluster, config, needRefresh, encryptLocalHost)
	} else {
		c, err = makeWithCbq(cluster, config, encryptLocalHost)
	}
	if err != nil {
		return nil, err
	}
	c.maxvb = -1

	var clusterVer uint64
	var refreshErr error
	_, _, clusterVer, refreshErr = c.Refresh()
	if refreshErr == nil {
		c.UpdateDataEncodingFormat(clusterVer)
	} else {
		// Use old data format if c.Refresh() returns error
		c.SetDataEncodingFormat(common.DATA_ENC_JSON)
	}

	return c, nil
}

func (c *GsiClient) Bridge() BridgeAccessor {
	return c.bridge
}

// IndexState implements BridgeAccessor{} interface.
func (c *GsiClient) IndexState(defnID uint64) (common.IndexState, error) {
	if c.bridge == nil {
		return common.INDEX_STATE_ERROR, ErrorClientUninitialized
	}
	return c.bridge.IndexState(defnID)
}

// Sync implements BridgeAccessor{} interface.
func (c *GsiClient) Sync() error {
	if c.bridge == nil {
		return ErrorClientUninitialized
	}
	return c.bridge.Sync()
}

func (c *GsiClient) UpdateDataEncodingFormat(clusterVer uint64) {
	if clusterVer >= common.INDEXER_65_VERSION {
		msg := "GsiClient::UpdateUsecjson: using collatejson as data format "
		msg += "between indexer and GsiClient"
		logging.Infof(msg)
		c.SetDataEncodingFormat(common.DATA_ENC_COLLATEJSON)
	} else {
		c.SetDataEncodingFormat(common.DATA_ENC_JSON)
	}
}

// Refresh implements BridgeAccessor{} interface.
func (c *GsiClient) Refresh() ([]*mclient.IndexMetadata, uint64, uint64, error) {
	if c.bridge == nil {
		return nil, 0, 0, ErrorClientUninitialized
	}
	return c.bridge.Refresh()
}

// Nodes implements BridgeAccessor{} interface.
func (c *GsiClient) Nodes() ([]*IndexerService, error) {
	if c.bridge == nil {
		return nil, ErrorClientUninitialized
	}
	return c.bridge.Nodes()
}

// BucketSeqnos will return the current vbucket-timestamp using GET_SEQNOS
// command.
func (c *GsiClient) BucketSeqnos(
	bucketn string, hash64 uint64) (*TsConsistency, error) {

	seqnos, err := common.BucketSeqnos(c.cluster, "default" /*pool*/, bucketn)
	if err != nil {
		return nil, err
	}
	vbnos := make([]uint16, len(seqnos))
	for i := range seqnos {
		vbnos[i] = uint16(i)
	}
	vector := NewTsConsistency(vbnos, seqnos, nil)
	vector.Crc64 = hash64
	return vector, nil
}

// BucketTs will return the current vbucket-timestamp using STATS
// command.
func (c *GsiClient) BucketTs(bucketn string) (*TsConsistency, error) {
	b, err := common.ConnectBucket(c.cluster, "default" /*pooln*/, bucketn)
	if err != nil {
		return nil, err
	}
	defer b.Close()

	if c.maxvb == -1 {
		if c.maxvb, err = common.MaxVbuckets(b); err != nil {
			return nil, err
		}
	}
	seqnos, vbuuids, err := common.BucketTs(b, c.maxvb)
	if err != nil {
		return nil, err
	}
	vbnos := make([]uint16, c.maxvb)
	for i := range vbnos {
		vbnos[i] = uint16(i)
	}
	return NewTsConsistency(vbnos, seqnos, vbuuids), nil
}

// CreateIndex implements BridgeAccessor{} interface.
func (c *GsiClient) CreateIndex(
	name, bucket, using, exprType, partnExpr, whereExpr string,
	secExprs []string, isPrimary bool,
	with []byte) (defnID uint64, err error) {

	return c.CreateIndex3(name, bucket, using, exprType,
		whereExpr, secExprs, nil, isPrimary, common.SINGLE, nil, with)
}

// CreateIndex implements BridgeAccessor{} interface.
func (c *GsiClient) CreateIndex3(
	name, bucket, using, exprType, whereExpr string,
	secExprs []string, desc []bool, isPrimary bool,
	scheme common.PartitionScheme, partitionKeys []string,
	with []byte) (defnID uint64, err error) {

	err = common.IsValidIndexName(name)
	if err != nil {
		return 0, err
	}

	if c.bridge == nil {
		return defnID, ErrorClientUninitialized
	}

	logging.Infof("CreateIndex %v %v ...", bucket, name)
	begin := time.Now()
	defnID, err = c.bridge.CreateIndex(
		name, bucket, using, exprType, whereExpr,
		secExprs, desc, isPrimary, scheme, partitionKeys, with)
	fmsg := "CreateIndex %v %v/%v using:%v exprType:%v " +
		"whereExpr:%v secExprs:%v desc:%v isPrimary:%v scheme:%v " +
		" partitionKeys:%v with:%v - elapsed(%v) err(%v)"
	logging.Infof(
		fmsg, defnID, bucket, name, using, exprType, logging.TagUD(whereExpr),
		logging.TagUD(secExprs), desc, isPrimary, scheme, logging.TagUD(partitionKeys), string(with), time.Since(begin), err)
	return defnID, err
}

// BuildIndexes implements BridgeAccessor{} interface.
func (c *GsiClient) BuildIndexes(defnIDs []uint64) error {
	if c.bridge == nil {
		return ErrorClientUninitialized
	}

	logging.Infof("BuildIndexes %v ...", defnIDs)
	begin := time.Now()
	err := c.bridge.BuildIndexes(defnIDs)
	fmsg := "BuildIndexes %v - elapsed(%v), err(%v)"
	logging.Infof(fmsg, defnIDs, time.Since(begin), err)
	return err
}

// MoveIndex implements BridgeAccessor{} interface.
func (c *GsiClient) MoveIndex(defnID uint64, with map[string]interface{}) error {
	if c.bridge == nil {
		return ErrorClientUninitialized
	}

	logging.Infof("MoveIndex %v ...", defnID)
	begin := time.Now()
	err := c.bridge.MoveIndex(defnID, with)
	fmsg := "MoveIndex %v - elapsed(%v), err(%v)"
	logging.Infof(fmsg, defnID, time.Since(begin), err)
	return err
}

// AlterReplicaCount implements BridgeAccessor{} interface.
func (c *GsiClient) AlterReplicaCount(action string, defnID uint64, with map[string]interface{}) error {
	if c.bridge == nil {
		return ErrorClientUninitialized
	}

	logging.Infof("AlterReplicaCount %v %v ...", defnID, action)
	begin := time.Now()
	err := c.bridge.AlterReplicaCount(action, defnID, with)
	fmsg := "AlterReplicaCount %v - elapsed(%v), err(%v)"
	logging.Infof(fmsg, defnID, time.Since(begin), err)
	return err
}

// DropIndex implements BridgeAccessor{} interface.
func (c *GsiClient) DropIndex(defnID uint64) error {
	if c.bridge == nil {
		return ErrorClientUninitialized
	}

	logging.Infof("DropIndex %v ...", defnID)
	begin := time.Now()
	err := c.bridge.DropIndex(defnID)
	fmsg := "DropIndex %v - elapsed(%v), err(%v)"
	logging.Infof(fmsg, defnID, time.Since(begin), err)
	return err
}

// LookupStatistics for a single secondary-key.
func (c *GsiClient) LookupStatistics(
	defnID uint64, requestId string, value common.SecondaryKey) (common.IndexStatistics, error) {

	return nil, ErrorNotImplemented
}

// RangeStatistics for index range.
func (c *GsiClient) RangeStatistics(
	defnID uint64, requestId string, low, high common.SecondaryKey,
	inclusion Inclusion) (common.IndexStatistics, error) {

	return nil, ErrorNotImplemented
}

// Lookup scan index between low and high.
func (c *GsiClient) Lookup(
	defnID uint64, requestId string, values []common.SecondaryKey,
	distinct bool, limit int64,
	cons common.Consistency, vector *TsConsistency,
	callb ResponseHandler) (err error) {

	dataEncFmt := c.GetDataEncodingFormat()
	broker := makeDefaultRequestBroker(callb, dataEncFmt)
	return c.LookupInternal(defnID, requestId, values, distinct, limit, cons, vector, broker)
}

// Lookup scan index between low and high.
func (c *GsiClient) LookupInternal(
	defnID uint64, requestId string, values []common.SecondaryKey,
	distinct bool, limit int64,
	cons common.Consistency, vector *TsConsistency,
	broker *RequestBroker) (err error) {

	if c.bridge == nil {
		return ErrorClientUninitialized
	}

	// check whether the index is present and available.
	if _, err = c.bridge.IndexState(defnID); err != nil {
		return err
	}

	begin := time.Now()

	handler := func(qc *GsiScanClient, index *common.IndexDefn, rollbackTime int64, partitions []common.PartitionId,
		callb ResponseHandler) (error, bool) {
		var err error

		dataEncFmt := broker.GetDataEncodingFormat()

		vector, err = c.getConsistency(qc, cons, vector, index.Bucket)
		if err != nil {
			return err, false
		}
		return qc.Lookup(
			uint64(index.DefnId), requestId, values, distinct, broker.GetLimit(), cons,
			vector, callb, rollbackTime, partitions, dataEncFmt, broker.DoRetry())
	}

	broker.SetScanRequestHandler(handler)
	broker.SetLimit(limit)

	_, err = c.doScan(defnID, requestId, broker)
	if err != nil { // callback with error
		return err
	}

	fmsg := "Lookup {%v,%v} - elapsed(%v) err(%v)"
	logging.Verbosef(fmsg, defnID, requestId, time.Since(begin), err)
	return
}

// Range scan index between low and high.
func (c *GsiClient) Range(
	defnID uint64, requestId string, low, high common.SecondaryKey,
	inclusion Inclusion, distinct bool, limit int64,
	cons common.Consistency, vector *TsConsistency,
	callb ResponseHandler) (err error) {

	dataEncFmt := c.GetDataEncodingFormat()
	broker := makeDefaultRequestBroker(callb, dataEncFmt)
	return c.RangeInternal(defnID, requestId, low, high, inclusion, distinct, limit, cons, vector, broker)
}

// Range scan index between low and high.
func (c *GsiClient) RangeInternal(
	defnID uint64, requestId string, low, high common.SecondaryKey,
	inclusion Inclusion, distinct bool, limit int64,
	cons common.Consistency, vector *TsConsistency,
	broker *RequestBroker) (err error) {

	if c.bridge == nil {
		return ErrorClientUninitialized
	}

	// check whether the index is present and available.
	if _, err = c.bridge.IndexState(defnID); err != nil {
		return err
	}

	begin := time.Now()

	handler := func(qc *GsiScanClient, index *common.IndexDefn, rollbackTime int64, partitions []common.PartitionId,
		handler ResponseHandler) (error, bool) {
		var err error

		dataEncFmt := broker.GetDataEncodingFormat()

		vector, err = c.getConsistency(qc, cons, vector, index.Bucket)
		if err != nil {
			return err, false
		}
		if c.bridge.IsPrimary(uint64(index.DefnId)) {
			var l, h []byte
			var what string
			// primary keys are plain sequence of binary.
			if low != nil && len(low) > 0 {
				if l, what = curePrimaryKey(low[0]); what == "after" {
					return nil, true
				}
			}
			if high != nil && len(high) > 0 {
				if h, what = curePrimaryKey(high[0]); what == "before" {
					return nil, true
				}
			}
			return qc.RangePrimary(
				uint64(index.DefnId), requestId, l, h, inclusion, distinct,
				broker.GetLimit(), cons, vector, handler, rollbackTime,
				partitions, dataEncFmt, broker.DoRetry())
		}
		// dealing with secondary index.
		return qc.Range(
			uint64(index.DefnId), requestId, low, high, inclusion, distinct,
			broker.GetLimit(), cons, vector, handler, rollbackTime, partitions,
			dataEncFmt, broker.DoRetry())
	}

	broker.SetScanRequestHandler(handler)
	broker.SetLimit(limit)

	_, err = c.doScan(defnID, requestId, broker)
	if err != nil { // callback with error
		return err
	}

	fmsg := "Range {%v,%v} - elapsed(%v) err(%v)"
	logging.Verbosef(fmsg, defnID, requestId, time.Since(begin), err)
	return
}

// ScanAll for full table scan.
func (c *GsiClient) ScanAll(
	defnID uint64, requestId string, limit int64,
	cons common.Consistency, vector *TsConsistency,
	callb ResponseHandler) (err error) {

	dataEncFmt := c.GetDataEncodingFormat()
	broker := makeDefaultRequestBroker(callb, dataEncFmt)
	return c.ScanAllInternal(defnID, requestId, limit, cons, vector, broker)
}

// ScanAll for full table scan.
func (c *GsiClient) ScanAllInternal(
	defnID uint64, requestId string, limit int64,
	cons common.Consistency, vector *TsConsistency,
	broker *RequestBroker) (err error) {

	if c.bridge == nil {
		return ErrorClientUninitialized
	}

	// check whether the index is present and available.
	if _, err = c.bridge.IndexState(defnID); err != nil {
		return err
	}

	begin := time.Now()

	handler := func(qc *GsiScanClient, index *common.IndexDefn, rollbackTime int64, partitions []common.PartitionId,
		handler ResponseHandler) (error, bool) {
		var err error

		dataEncFmt := broker.GetDataEncodingFormat()

		vector, err = c.getConsistency(qc, cons, vector, index.Bucket)
		if err != nil {
			return err, false
		}
		return qc.ScanAll(uint64(index.DefnId), requestId, broker.GetLimit(),
			cons, vector, handler, rollbackTime, partitions, dataEncFmt, broker.DoRetry())
	}

	broker.SetScanRequestHandler(handler)
	broker.SetLimit(limit)

	_, err = c.doScan(defnID, requestId, broker)
	if err != nil { // callback with error
		return err
	}

	fmsg := "ScanAll {%v,%v} - elapsed(%v) err(%v)"
	logging.Verbosef(fmsg, defnID, requestId, time.Since(begin), err)
	return
}

func (c *GsiClient) MultiScan(
	defnID uint64, requestId string, scans Scans, reverse,
	distinct bool, projection *IndexProjection, offset, limit int64,
	cons common.Consistency, vector *TsConsistency,
	callb ResponseHandler) (err error) {

	dataEncFmt := c.GetDataEncodingFormat()
	broker := makeDefaultRequestBroker(callb, dataEncFmt)
	return c.MultiScanInternal(defnID, requestId, scans, reverse, distinct, projection, offset, limit, cons, vector, broker)
}

func (c *GsiClient) MultiScanInternal(
	defnID uint64, requestId string, scans Scans, reverse,
	distinct bool, projection *IndexProjection, offset, limit int64,
	cons common.Consistency, vector *TsConsistency,
	broker *RequestBroker) (err error) {

	if c.bridge == nil {
		return ErrorClientUninitialized
	}

	// check whether the index is present and available.
	if _, err = c.bridge.IndexState(defnID); err != nil {
		return err
	}

	begin := time.Now()

	handler := func(qc *GsiScanClient, index *common.IndexDefn, rollbackTime int64, partitions []common.PartitionId,
		handler ResponseHandler) (error, bool) {
		var err error

		dataEncFmt := broker.GetDataEncodingFormat()

		vector, err = c.getConsistency(qc, cons, vector, index.Bucket)
		if err != nil {
			return err, false
		}

		if c.bridge.IsPrimary(uint64(index.DefnId)) {
			return qc.MultiScanPrimary(
				uint64(index.DefnId), requestId, scans, reverse, distinct,
				projection, broker.GetOffset(), broker.GetLimit(), cons,
				vector, handler, rollbackTime, partitions, dataEncFmt, broker.DoRetry())
		}

		return qc.MultiScan(
			uint64(index.DefnId), requestId, scans, reverse, distinct,
			projection, broker.GetOffset(), broker.GetLimit(), cons, vector,
			handler, rollbackTime, partitions, dataEncFmt, broker.DoRetry())
	}

	broker.SetScanRequestHandler(handler)
	broker.SetLimit(limit)
	broker.SetOffset(offset)
	broker.SetScans(scans)
	broker.SetProjection(projection)
	broker.SetDistinct(distinct)

	_, err = c.doScan(defnID, requestId, broker)
	if err != nil { // callback with error
		return err
	}

	fmsg := "Scans {%v,%v} - elapsed(%v) err(%v)"
	logging.Verbosef(fmsg, defnID, requestId, time.Since(begin), err)
	return
}

func (c *GsiClient) CountLookup(
	defnID uint64, requestId string, values []common.SecondaryKey,
	cons common.Consistency, vector *TsConsistency) (count int64, err error) {

	dataEncFmt := c.GetDataEncodingFormat()
	broker := makeDefaultRequestBroker(nil, dataEncFmt)
	return c.CountLookupInternal(defnID, requestId, values, cons, vector, broker)
}

// CountLookup to count number entries for given set of keys.
func (c *GsiClient) CountLookupInternal(
	defnID uint64, requestId string, values []common.SecondaryKey,
	cons common.Consistency, vector *TsConsistency,
	broker *RequestBroker) (count int64, err error) {

	if c.bridge == nil {
		return count, ErrorClientUninitialized
	}

	// check whether the index is present and available.
	if _, err := c.bridge.IndexState(defnID); err != nil {
		return 0, err
	}

	begin := time.Now()

	handler := func(qc *GsiScanClient, index *common.IndexDefn, rollbackTime int64, partitions []common.PartitionId) (int64, error, bool) {
		var err error
		var count int64

		vector, err = c.getConsistency(qc, cons, vector, index.Bucket)
		if err != nil {
			return 0, err, false
		}

		if c.bridge.IsPrimary(uint64(index.DefnId)) {
			equals := make([][]byte, 0, len(values))
			// primary keys are plain sequence of binary.
			for _, value := range values {
				e, _ := curePrimaryKey(value[0])
				equals = append(equals, e)
			}

			count, err = qc.CountLookupPrimary(
				uint64(index.DefnId), requestId, equals, cons, vector, rollbackTime, partitions, broker.DoRetry())
			return count, err, false
		}

		count, err = qc.CountLookup(uint64(index.DefnId), requestId, values, cons, vector, rollbackTime, partitions, broker.DoRetry())
		return count, err, false
	}

	broker.SetCountRequestHandler(handler)

	count, err = c.doScan(defnID, requestId, broker)

	fmsg := "CountLookup {%v,%v} - elapsed(%v) err(%v)"
	logging.Verbosef(fmsg, defnID, requestId, time.Since(begin), err)
	return count, err
}

func (c *GsiClient) CountRange(
	defnID uint64, requestId string,
	low, high common.SecondaryKey,
	inclusion Inclusion,
	cons common.Consistency, vector *TsConsistency) (count int64, err error) {

	dataEncFmt := c.GetDataEncodingFormat()
	broker := makeDefaultRequestBroker(nil, dataEncFmt)
	return c.CountRangeInternal(defnID, requestId, low, high, inclusion, cons, vector, broker)
}

// CountRange to count number entries in the given range.
func (c *GsiClient) CountRangeInternal(
	defnID uint64, requestId string,
	low, high common.SecondaryKey,
	inclusion Inclusion,
	cons common.Consistency, vector *TsConsistency,
	broker *RequestBroker) (count int64, err error) {

	if c.bridge == nil {
		return count, ErrorClientUninitialized
	}

	// check whether the index is present and available.
	if _, err := c.bridge.IndexState(defnID); err != nil {
		return 0, err
	}

	begin := time.Now()

	handler := func(qc *GsiScanClient, index *common.IndexDefn, rollbackTime int64, partitions []common.PartitionId) (int64, error, bool) {
		var err error
		var count int64

		vector, err = c.getConsistency(qc, cons, vector, index.Bucket)
		if err != nil {
			return 0, err, false
		}
		if c.bridge.IsPrimary(uint64(index.DefnId)) {
			var l, h []byte
			var what string
			// primary keys are plain sequence of binary.
			if low != nil && len(low) > 0 {
				if l, what = curePrimaryKey(low[0]); what == "after" {
					return 0, nil, true
				}
			}
			if high != nil && len(high) > 0 {
				if h, what = curePrimaryKey(high[0]); what == "before" {
					return 0, nil, true
				}
			}
			count, err = qc.CountRangePrimary(
				uint64(index.DefnId), requestId, l, h, inclusion, cons, vector, rollbackTime, partitions, broker.DoRetry())
			return count, err, false
		}

		count, err = qc.CountRange(
			uint64(index.DefnId), requestId, low, high, inclusion, cons, vector, rollbackTime, partitions, broker.DoRetry())
		return count, err, false
	}

	broker.SetCountRequestHandler(handler)

	count, err = c.doScan(defnID, requestId, broker)

	fmsg := "CountRange {%v,%v} - elapsed(%v) err(%v)"
	logging.Verbosef(fmsg, defnID, requestId, time.Since(begin), err)
	return count, err
}

func (c *GsiClient) MultiScanCount(
	defnID uint64, requestId string,
	scans Scans, distinct bool,
	cons common.Consistency, vector *TsConsistency) (count int64, err error) {

	dataEncFmt := c.GetDataEncodingFormat()
	broker := makeDefaultRequestBroker(nil, dataEncFmt)
	return c.MultiScanCountInternal(defnID, requestId, scans, distinct, cons, vector, broker)
}

func (c *GsiClient) MultiScanCountInternal(
	defnID uint64, requestId string,
	scans Scans, distinct bool,
	cons common.Consistency, vector *TsConsistency,
	broker *RequestBroker) (count int64, err error) {

	if c.bridge == nil {
		return count, ErrorClientUninitialized
	}

	// check whether the index is present and available.
	if _, err := c.bridge.IndexState(defnID); err != nil {
		return 0, err
	}

	begin := time.Now()

	handler := func(qc *GsiScanClient, index *common.IndexDefn, rollbackTime int64, partitions []common.PartitionId) (int64, error, bool) {
		var err error

		vector, err = c.getConsistency(qc, cons, vector, index.Bucket)
		if err != nil {
			return 0, err, false
		}
		if c.bridge.IsPrimary(uint64(index.DefnId)) {
			count, err = qc.MultiScanCountPrimary(
				uint64(index.DefnId), requestId, scans, distinct, cons, vector, rollbackTime, partitions, broker.DoRetry())
			return count, err, false
		}

		count, err = qc.MultiScanCount(
			uint64(index.DefnId), requestId, scans, distinct, cons, vector, rollbackTime, partitions, broker.DoRetry())
		return count, err, false
	}

	broker.SetCountRequestHandler(handler)

	count, err = c.doScan(defnID, requestId, broker)

	fmsg := "MultiScanCount {%v,%v} - elapsed(%v) err(%v)"
	logging.Verbosef(fmsg, defnID, requestId, time.Since(begin), err)
	return count, err
}

func (c *GsiClient) Scan3(
	defnID uint64, requestId string, scans Scans, reverse,
	distinct bool, projection *IndexProjection, offset, limit int64,
	groupAggr *GroupAggr, indexOrder *IndexKeyOrder,
	cons common.Consistency, vector *TsConsistency,
	callb ResponseHandler) (err error) {

	dataEncFmt := c.GetDataEncodingFormat()
	broker := makeDefaultRequestBroker(callb, dataEncFmt)
	return c.Scan3Internal(defnID, requestId, scans, reverse, distinct,
		projection, offset, limit, groupAggr, indexOrder, cons, vector, broker)
}

func (c *GsiClient) Scan3Internal(
	defnID uint64, requestId string, scans Scans, reverse,
	distinct bool, projection *IndexProjection, offset, limit int64,
	groupAggr *GroupAggr, indexOrder *IndexKeyOrder,
	cons common.Consistency, vector *TsConsistency,
	broker *RequestBroker) (err error) {

	if c.bridge == nil {
		return ErrorClientUninitialized
	}

	// check whether the index is present and available.
	if _, err = c.bridge.IndexState(defnID); err != nil {
		return err
	}

	begin := time.Now()

	handler := func(qc *GsiScanClient, index *common.IndexDefn, rollbackTime int64, partitions []common.PartitionId,
		handler ResponseHandler) (error, bool) {
		var err error

		dataEncFmt := broker.GetDataEncodingFormat()

		vector, err = c.getConsistency(qc, cons, vector, index.Bucket)
		if err != nil {
			return err, false
		}

		if c.bridge.IsPrimary(uint64(index.DefnId)) {
			return qc.Scan3Primary(
				uint64(index.DefnId), requestId, scans, reverse, distinct,
				projection, broker.GetOffset(), broker.GetLimit(), groupAggr,
				broker.GetSorted(), cons, vector, handler, rollbackTime,
				partitions, dataEncFmt, broker.DoRetry())
		}

		return qc.Scan3(
			uint64(index.DefnId), requestId, scans, reverse, distinct,
			projection, broker.GetOffset(), broker.GetLimit(), groupAggr,
			broker.GetSorted(), cons, vector, handler, rollbackTime,
			partitions, dataEncFmt, broker.DoRetry())
	}

	broker.SetScanRequestHandler(handler)
	broker.SetLimit(limit)
	broker.SetOffset(offset)
	broker.SetScans(scans)
	broker.SetGroupAggr(groupAggr)
	broker.SetProjection(projection)
	broker.SetSorted(indexOrder != nil)
	broker.SetDistinct(distinct)
	broker.SetIndexOrder(indexOrder)

	_, err = c.doScan(defnID, requestId, broker)
	if err != nil { // callback with error
		return err
	}

	fmsg := "Scan3 {%v,%v} - elapsed(%v) err(%v)"
	logging.Verbosef(fmsg, defnID, requestId, time.Since(begin), err)
	return
}

//-------------------------------------
// StorageStatistics implementation
//-------------------------------------
type StorageStats struct {
	Index       string
	Id          uint64
	PartitionId common.PartitionId
	Stats       map[string]interface{}
}

const STAT_PARTITION_ID = "PARTITION_ID"
const STAT_NUM_PAGES = "NUM_PAGES"
const STAT_NUM_ITEMS = "NUM_ITEMS"
const STAT_RESIDENT_RATIO = "RESIDENT_RATIO"
const STAT_NUM_INSERT = "NUM_INSERT"
const STAT_NUM_DELETE = "NUM_DELETE"
const STAT_AVG_ITEM_SIZE = "AVG_ITEM_SIZE"
const STAT_AVG_PAGE_SIZE = "AVG_PAGE_SIZE"

// A set of partitions for given index definition is chosen using metaclient's
// GetScanport. It returns a set of target replica InstanceIds with corresponding
// PartitionIds per InstanceID. It is possible that some partitions are from one
// replica and other are from a different replica, this is the same logic that applies
// when partitions/replica are chosen do a scan (See doScan).
// There is no replica retry based on excludes for storage stats. Consumer of this
// API should retry in case of error.
//
// Steps to retrieve StorageStatistics:
// 1. Get a set of queryports, corresponding targetInstanceIds and partitions per InstanceId
// 2. Get adminports from queryports and construct statsUrls for participating indexer nodes
// 3. For stats of each node, get targetInstanceId of corresponding node and pick partition stats
//    for that targetInstanceId.
// 4. Filter relevant storage stats as needed by CBO
func (c *GsiClient) StorageStatistics(defnID uint64, requestId string) ([]map[string]interface{}, error) {

	var excludes map[common.IndexDefnId]map[common.PartitionId]map[uint64]bool
	skips := make(map[common.IndexDefnId]bool)

	storageMode := c.Settings().StorageMode()

	if storageMode == "forestdb" {
		// StorageStatistics not supported for forestdb
		return nil, nil
	}

	if queryports, _, targetInstIds, _, partitions, _, ok := c.bridge.GetScanport(defnID, excludes, skips); ok {

		// urls is list of Stats REST endpoints for all indexer nodes
		// hosting the requested index
		statUrls := []string{}
		nodes, err := c.Nodes()
		if err != nil {
			return nil, err
		}

		for _, qp := range queryports {
			for _, n := range nodes {
				if qp == n.Queryport {
					url := "http://" + n.Httpport + "/stats/storage"
					statUrls = append(statUrls, url)
				}
			}
		}

		stats, err := getStatsFromIndexerNodes(statUrls, targetInstIds, partitions, storageMode)
		if err != nil {
			return nil, err
		}
		return stats, nil
	}

	return nil, errors.New("Unable to retrieve storage statistics from any replica index.")
}

func getStatsFromIndexerNodes(statUrls []string, targetInstIds []uint64,
	partitions [][]common.PartitionId, storageMode string) ([]map[string]interface{}, error) {

	storageStats := make([]map[string]interface{}, 0)
	for i, statUrl := range statUrls {

		resp, err := getWithAuth(statUrl)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		bytes, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			errStr := fmt.Sprintf("Error reading stats from %v : %v", statUrl, err)
			return nil, errors.New(errStr)
		}

		var nodeStats []StorageStats
		err = commonjson.Unmarshal(bytes, &nodeStats)
		if err != nil {
			errStr := fmt.Sprintf("Error unmarshalling stats from %v : %v", statUrl, err)
			return nil, errors.New(errStr)
		}
		for _, nodeStat := range nodeStats {
			if targetInstIds[i] == nodeStat.Id && contains(partitions[i], nodeStat.PartitionId) {
				partnStats := getStatsForPartition(nodeStat, storageMode)
				storageStats = append(storageStats, partnStats)
			}
		}
	}
	return storageStats, nil
}

func contains(partitionIds []common.PartitionId, partitionId common.PartitionId) bool {
	for _, id := range partitionIds {
		if partitionId == id {
			return true
		}
	}
	return false
}

func getStatsForPartition(instStats StorageStats, storageMode string) map[string]interface{} {

	if storageMode == "plasma" {
		storageStats := make(map[string]interface{})
		storageStats[STAT_PARTITION_ID] = instStats.PartitionId
		stats := instStats.Stats
		mainStoreStats := stats["MainStore"].(map[string]interface{})
		storageStats[STAT_NUM_PAGES] = mainStoreStats["num_pages"]
		storageStats[STAT_NUM_ITEMS] = mainStoreStats["items_count"]
		storageStats[STAT_RESIDENT_RATIO] = mainStoreStats["resident_ratio"]
		storageStats[STAT_NUM_INSERT] = mainStoreStats["inserts"]
		storageStats[STAT_NUM_DELETE] = mainStoreStats["deletes"]
		storageStats[STAT_AVG_ITEM_SIZE] = mainStoreStats["avg_item_size"]
		storageStats[STAT_AVG_PAGE_SIZE] = mainStoreStats["avg_page_size"]
		return storageStats
	}

	if storageMode == "memory_optimized" {
		storageStats := make(map[string]interface{})
		storageStats[STAT_PARTITION_ID] = instStats.PartitionId
		stats := instStats.Stats
		items_count := stats["items_count"].(int64)
		data_size := stats["data_size"].(int64)
		avg_item_size := int64(0)
		if items_count > 0 {
			avg_item_size = data_size / items_count
		}
		storageStats[STAT_NUM_ITEMS] = items_count
		storageStats[STAT_AVG_ITEM_SIZE] = avg_item_size
		return storageStats
	}

	return nil
}

//-------------------------------------
// StorageStatistics implementation end
//-------------------------------------

// DescribeError return error description as human readable string.
func (c *GsiClient) DescribeError(err error) string {
	if desc, ok := errorDescriptions[err.Error()]; ok {
		return desc
	}
	return err.Error()
}

// DescribeError return error description as human readable string.
func (c *GsiClient) Settings() *ClientSettings {
	return c.settings
}

// Close the client and all open connections with server.
func (c *GsiClient) Close() {
	if c == nil {
		return
	}
	if c.settings != nil {
		c.settings.Close()
	}
	if c.bridge == nil {
		return
	}
	c.bridge.Close()
	qcs := *((*map[string]*GsiScanClient)(atomic.LoadPointer(&c.queryClients)))
	for _, qc := range qcs {
		qc.Close()
	}
	close(c.killch)
}

func (c *GsiClient) updateScanClients() {
	newclients, staleclients := map[string]bool{}, map[string]bool{}
	cache := map[string]bool{}
	qcs := *((*map[string]*GsiScanClient)(atomic.LoadPointer(&c.queryClients)))
	// add new indexer-nodes
	for _, queryport := range c.bridge.GetScanports() {
		cache[queryport] = true
		if _, ok := qcs[queryport]; !ok {
			newclients[queryport] = true
		}
	}
	// forget stale indexer-nodes.
	for queryport, qc := range qcs {
		if _, ok := cache[queryport]; !ok {
			qc.Close()
			staleclients[queryport] = true
		}
	}
	if len(newclients) > 0 || len(staleclients) > 0 {
		clients := make(map[string]*GsiScanClient)
		for queryport, qc := range qcs {
			if _, ok := staleclients[queryport]; ok {
				continue
			}
			qc.RefreshServerVersion()
			clients[queryport] = qc
		}
		for queryport := range newclients {
			if qc, err := NewGsiScanClient(queryport, c.config); err == nil {
				clients[queryport] = qc
			} else {
				logging.Errorf("Unable to initialize gsi scanclient (%v)", err)
			}
		}

		atomic.StorePointer(&c.queryClients, unsafe.Pointer(&clients))
	}
}

func (c *GsiClient) getScanClients(queryports []string) ([]*GsiScanClient, bool) {

	qcs := *((*map[string]*GsiScanClient)(atomic.LoadPointer(&c.queryClients)))

	qc := make([]*GsiScanClient, len(queryports))
	var ok bool

	for i, queryport := range queryports {
		if _, ok = qcs[queryport]; ok {
			qc[i] = qcs[queryport]
		} else {
			break
		}
	}

	return qc, ok
}

func (c *GsiClient) updateExcludes(defnID uint64, excludes map[common.IndexDefnId]map[common.PartitionId]map[uint64]bool,
	errMap map[common.PartitionId]map[uint64]error) map[common.IndexDefnId]map[common.PartitionId]map[uint64]bool {

	defnId := common.IndexDefnId(defnID)

	if excludes == nil {
		excludes = make(map[common.IndexDefnId]map[common.PartitionId]map[uint64]bool)
	}

	if _, ok := excludes[defnId]; !ok {
		excludes[defnId] = make(map[common.PartitionId]map[uint64]bool)
	}

	for partnId, instErrMap := range errMap {
		for instId, err := range instErrMap {
			if !isgone(err) {
				if _, ok := excludes[defnId][partnId]; !ok {
					excludes[defnId][partnId] = make(map[uint64]bool)
				}
				excludes[defnId][partnId][instId] = true
			} else {
				// if it is network error, then
				// exclude all partitions on all replicas
				// residing on the failed node.

				// doScan() may scan the partition from insts or rebalInsts.
				// So the failure can be coming from insts or rebalInsts.
				// But the function GetIndexInst() will only look for the
				// inst under insts.
				// 1) Non-partitioned index.  InstId unqiuely identify if
				//    it is coming from insts or rebalInsts.  If it is
				//    coming from rebalInsts, we do not add it to exclude map.
				// 2) Partitioned index.  Same InstId could be used in both
				//    insts or rebalInsts.  If inst contains the partition,
				//    then it will add to the exclude map, even if
				//    the error may indeed coming from rebalInsts.  This is
				//    fine since inst must be skipped in pickRandom
				//    in the first place, otherwise, rebalInsts will not be used.
				//    So adding it to the exclude list will not affect skipRandom.
				//
				if inst := c.bridge.GetIndexInst(instId); inst != nil {
					failIndexerId := inst.IndexerId[partnId]

					for _, replica := range c.bridge.GetIndexReplica(defnID) {
						for p, indexerId := range replica.IndexerId {
							if indexerId == failIndexerId {
								if _, ok := excludes[defnId][p]; !ok {
									excludes[defnId][p] = make(map[uint64]bool)
								}
								excludes[defnId][p][uint64(replica.InstId)] = true
							}
						}
					}
				}
			}
		}
	}

	return excludes
}

func (c *GsiClient) makeScanClient(scanport string) *GsiScanClient {

	if qc, ok := c.getScanClients([]string{scanport}); ok {
		return qc[0]
	}

	return nil
}

func (c *GsiClient) doScan(defnID uint64, requestId string, broker *RequestBroker) (int64, error) {

	atomic.AddInt64(&c.numScans, 1)
	defer atomic.AddInt64(&c.numScans, -1)

	var excludes map[common.IndexDefnId]map[common.PartitionId]map[uint64]bool
	var err error

	broker.SetResponseTimer(c.bridge.Timeit)
	skips := make(map[common.IndexDefnId]bool)

	wait := c.config["retryIntervalScanport"].Int()
	retry := c.config["retryScanPort"].Int()
	for i := 0; true; {
		foundScanport := false

		queryports, targetDefnID, targetInstIds, rollbackTimes, partitions, numPartitions, ok := c.bridge.GetScanport(defnID, excludes, skips)
		var index *common.IndexDefn
		if ok {
			index = c.bridge.GetIndexDefn(targetDefnID)
			if index == nil {
				err = fmt.Errorf("Index definition not found")
			}
		}

		if ok && index != nil {
			start := time.Now()
			count, scan_errs, partial, refresh := broker.scatter(c.makeScanClient, index, queryports, targetInstIds,
				rollbackTimes, partitions, numPartitions, c.settings)

			if !refresh {
				foundScanport = true

				if c.isTimeit(scan_errs) {
					c.updateScanResponse(time.Now().Sub(start).Nanoseconds())
					return count, getScanError(scan_errs)
				}

				excludes = c.updateExcludes(defnID, excludes, scan_errs)
				if len(scan_errs) != 0 && partial {
					// partially succeeded scans, we don't reset-hash and we don't retry
					return 0, getScanError(scan_errs)

				} else { // TODO: make this error message precise
					// reset the hash so that we do a full STATS for next query.
					c.setBucketHash(index.Bucket, 0)
				}
				err = fmt.Errorf("%v from %v", getScanError(scan_errs), queryports)

				if len(queryports) == len(partitions) && len(queryports) == len(targetInstIds) {
					for i, _ := range queryports {
						logging.Warnf("scan failed: requestId %v queryport %v inst %v partition %v", requestId, queryports[i], targetInstIds[i], partitions[i])
					}
				}
			}
		}

		// If there is an error coming from indexer that cannot serve the scan request
		// (including io error), then exclude this defnID and retry with another replica.
		// If we exhaust all the replica, then GetScanport() will return ok=false.
		if foundScanport {
			logging.Warnf(
				"Scan failed with error for index %v.  Trying scan again with replica, reqId:%v : %v ...\n",
				defnID, requestId, err)
			continue
		}

		// If we cannot find a valid scansport, then retry up to retryScanport by refreshing
		// the clients.
		if i = i + 1; i < retry {
			excludes = nil
			skips = make(map[common.IndexDefnId]bool)
			broker.SetRetry(true)
			logging.Warnf(
				"Fail to find indexers to satisfy query request.  Trying scan again for index %v, reqId:%v : %v ...\n",
				defnID, requestId, err)
			c.updateScanClients()
			time.Sleep(time.Duration(wait) * time.Millisecond)
			continue
		}

		logging.Warnf("Fail to find indexers to satisfy query request.  Terminate scan for index %v,  reqId:%v : %v\n",
			defnID, requestId, err)
		break
	}
	if err != nil {
		return 0, err
	}
	return 0, ErrorNoHost
}

func (c *GsiClient) isTimeit(errMap map[common.PartitionId]map[uint64]error) bool {
	if len(errMap) == 0 {
		return true
	}

	for _, instErrMap := range errMap {
		for _, err := range instErrMap {
			if err.Error() != common.ErrClientCancel.Error() {
				return false
			}
		}
	}

	return true
}

func (c *GsiClient) getConsistency(
	qc *GsiScanClient, cons common.Consistency,
	vector *TsConsistency, bucket string) (*TsConsistency, error) {

	if cons == common.QueryConsistency {
		if vector == nil {
			return nil, ErrorExpectedTimestamp
		}
		return vector, nil
	} else if cons == common.SessionConsistency {
		var err error
		// Server version is old (cb 4.0.x)
		if qc.NeedSessionConsVector() {
			if hash64, ok := c.getBucketHash(bucket); ok && hash64 != 0 {
				begin := time.Now()
				fmsg := "Time taken by GET_SEQNOS call, %v CRC: %v\n"
				defer func() { logging.Debugf(fmsg, time.Since(begin), hash64) }()
				if vector, err = c.BucketSeqnos(bucket, hash64); err != nil {
					return nil, err
				}

			} else {
				begin := time.Now()
				fmsg := "Time taken by STATS call, %v\n"
				defer func() { logging.Debugf(fmsg, time.Since(begin)) }()
				if vector, err = c.BucketTs(bucket); err != nil {
					return nil, err
				}
				vector.Crc64 = common.HashVbuuid(vector.Vbuuids)
				vector.Vbuuids = nil
				c.setBucketHash(bucket, vector.Crc64)
				logging.Debugf("STATS CRC: %v\n", vector.Crc64)
			}
		} else {
			vector = nil
		}
	} else if cons == common.AnyConsistency {
		vector = nil
	} else {
		return nil, ErrorInvalidConsistency
	}
	return vector, nil
}

func (c *GsiClient) setBucketHash(bucketn string, crc64 uint64) {
	for {
		ptr := atomic.LoadPointer(&c.bucketHash)
		oldm := (*map[string]uint64)(ptr)
		newm := map[string]uint64{}
		for k, v := range *oldm {
			newm[k] = v
		}
		newm[bucketn] = crc64
		if atomic.CompareAndSwapPointer(&c.bucketHash, ptr, unsafe.Pointer(&newm)) {
			return
		}
	}
}

func (c *GsiClient) getBucketHash(bucketn string) (uint64, bool) {
	bucketHash := (*map[string]uint64)(atomic.LoadPointer(&c.bucketHash))
	crc64, ok := (*bucketHash)[bucketn]
	return crc64, ok
}

// create GSI client using cbqBridge and ScanCoordinator
func makeWithCbq(cluster string, config common.Config, encryptLocalHost bool) (*GsiClient, error) {
	var err error
	c := &GsiClient{
		cluster: cluster,
		config:  config,
	}

	if err := c.initSecurityContext(encryptLocalHost); err != nil {
		return nil, err
	}

	atomic.StorePointer(&c.bucketHash, (unsafe.Pointer)(new(map[string]uint64)))
	if c.bridge, err = newCbqClient(cluster); err != nil {
		return nil, err
	}
	clients := make(map[string]*GsiScanClient)
	for _, queryport := range c.bridge.GetScanports() {
		if qc, err := NewGsiScanClient(queryport, config); err == nil {
			clients[queryport] = qc
		}
	}
	atomic.StorePointer(&c.queryClients, unsafe.Pointer(&clients))
	return c, nil
}

func makeWithMetaProvider(
	cluster string, config common.Config, needRefresh bool, encryptLocalHost bool) (c *GsiClient, err error) {

	c = &GsiClient{
		cluster:      cluster,
		config:       config,
		queryClients: unsafe.Pointer(new(map[string]*GsiScanClient)),
		metaCh:       make(chan bool, 1),
		settings:     NewClientSettings(needRefresh),
		killch:       make(chan bool, 1),
	}

	if err := c.initSecurityContext(encryptLocalHost); err != nil {
		return nil, err
	}

	atomic.StorePointer(&c.bucketHash, (unsafe.Pointer)(new(map[string]uint64)))
	c.bridge, err = newMetaBridgeClient(cluster, config, c.metaCh, c.settings)
	if err != nil {
		return nil, err
	}
	c.updateScanClients()
	go c.listenMetaChange(c.killch)
	go c.logstats(c.killch)
	return c, nil
}

func (c *GsiClient) listenMetaChange(killch chan bool) {
	for {
		select {
		case <-c.metaCh:
			c.updateScanClients()
		case <-killch:
			return
		}
	}
}

func (c *GsiClient) logstats(killch chan bool) {

	logtick := time.Duration(c.config["logtick"].Int()) * time.Millisecond
	tick := time.NewTicker(logtick)

	defer func() {
		tick.Stop()
	}()

	for {
		select {
		case <-tick.C:
			logging.Infof("num concurrent scans {%v}", atomic.LoadInt64(&c.numScans))
			logging.Infof("average scan response {%v ms}", atomic.LoadInt64(&c.scanResponse)/int64(time.Millisecond))
		case <-killch:
			return
		}
	}
}

func (c *GsiClient) updateScanResponse(value int64) {

	current := atomic.LoadInt64(&c.scanResponse)
	atomic.StoreInt64(&c.scanResponse, (current+value)/2)
}

func (c *GsiClient) SetDataEncodingFormat(val common.DataEncodingFormat) {
	atomic.StoreUint32(&c.dataEncFmt, uint32(val))
}

func (c *GsiClient) GetDataEncodingFormat() common.DataEncodingFormat {
	if !c.Settings().AllowCJsonScanFormat() {
		return common.DATA_ENC_JSON
	}

	return common.DataEncodingFormat(atomic.LoadUint32(&c.dataEncFmt))
}

//--------------------------
// Consistency and Stability
//--------------------------

// TsConsistency specifies a subset of vbuckets to be used as
// timestamp vector to specify consistency criteria.
//
// Timestamp-vector will be ignored for AnyConsistency, computed
// locally by scan-coordinator or accepted as scan-arguments for
// SessionConsistency.
type TsConsistency struct {
	Vbnos   []uint16
	Seqnos  []uint64
	Vbuuids []uint64
	Crc64   uint64
}

// NewTsConsistency returns a new consistency vector object.
func NewTsConsistency(
	vbnos []uint16, seqnos []uint64, vbuuids []uint64) *TsConsistency {

	return &TsConsistency{Vbnos: vbnos, Seqnos: seqnos, Vbuuids: vbuuids}
}

// Override vbucket's {seqno, vbuuid} in the timestamp-vector,
// if vbucket is not present in the vector, append them to vector.
func (ts *TsConsistency) Override(
	vbno uint16, seqno, vbuuid uint64) *TsConsistency {

	for i, vb := range ts.Vbnos {
		if vbno == vb {
			ts.Seqnos[i], ts.Vbuuids[i] = seqno, vbuuid
			return ts
		}
	}
	ts.Vbnos = append(ts.Vbnos, vbno)
	ts.Seqnos = append(ts.Seqnos, seqno)
	ts.Vbuuids = append(ts.Vbuuids, vbuuid)
	return ts
}

func curePrimaryKey(key interface{}) ([]byte, string) {
	if key == nil {
		return nil, "before"
	}
	switch v := key.(type) {
	case []byte:
		return v, "ok"
	case string:
		return []byte(v), "ok"
	case []interface{}:
		return nil, "after"
	case map[string]interface{}:
		return nil, "after"
	}
	return nil, "before"
}

func isAnyGone(scan_err map[common.PartitionId]map[uint64]error) bool {

	if len(scan_err) == 0 {
		return false
	}

	for _, instErrs := range scan_err {
		for _, err := range instErrs {
			if isgone(err) {
				return true
			}
		}
	}

	return false
}

func isgone(scan_err error) bool {
	if scan_err == nil {
		return false
	}

	// if indexer crash in the middle of scan, it can return EOF
	// if a scan is sent to a already crashed indexer, it will return connection refused
	if scan_err == io.EOF {
		return true
	} else if err, ok := scan_err.(net.Error); ok && err.Timeout() {
		return true
	} else if strings.Contains(scan_err.Error(), syscall.ECONNRESET.Error()) || // connection reset
		strings.Contains(scan_err.Error(), syscall.EPIPE.Error()) { // broken pipe
		return true
	}
	return false
}

func getScanError(errMap map[common.PartitionId]map[uint64]error) error {

	if len(errMap) == 0 {
		return nil
	}

	errs := make(map[string]bool)

	for _, instErrMap := range errMap {
		for _, scan_err := range instErrMap {
			if !errs[scan_err.Error()] {
				errs[scan_err.Error()] = true
			}
		}
	}

	var allErrs string
	for errStr, _ := range errs {
		allErrs = fmt.Sprintf("%v %v", allErrs, errStr)
	}

	return fmt.Errorf("%v", allErrs)
}

func (c *GsiClient) initSecurityContext(encryptLocalHost bool) (err error) {

	pInitOnce.Do(func() {
		logger := func(err error) { common.Console(c.cluster, err.Error()) }
		if err = security.InitSecurityContextForClient(logger, c.cluster, "", "", encryptLocalHost); err != nil {
			return
		}

		if err = refreshSecurityContextOnTopology(c.cluster); err != nil {
			return
		}
	})

	return
}

func refreshSecurityContextOnTopology(clusterAddr string) error {

	fn := func(r int, e error) error {
		var cinfo *common.ClusterInfoCache
		url, err := common.ClusterAuthUrl(clusterAddr)
		if err != nil {
			return err
		}

		cinfo, err = common.NewClusterInfoCache(url, "default")
		if err != nil {
			return err
		}

		cinfo.Lock()
		defer cinfo.Unlock()

		if err := cinfo.Fetch(); err != nil {
			return err
		}

		security.SetEncryptPortMapping(cinfo.EncryptPortMapping())

		return nil
	}

	helper := common.NewRetryHelper(10, time.Second, 1, fn)
	return helper.Run()
}
