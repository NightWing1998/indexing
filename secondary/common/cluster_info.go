package common

import (
	"errors"
	"fmt"
	"math"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/indexing/secondary/dcp"
	"github.com/couchbase/indexing/secondary/logging"
)

var (
	ErrInvalidNodeId       = errors.New("Invalid NodeId")
	ErrInvalidService      = errors.New("Invalid service")
	ErrNodeNotBucketMember = errors.New("Node is not a member of bucket")
	ErrValidationFailed    = errors.New("ClusterInfo Validation Failed")
)

var ServiceAddrMap map[string]string

const (
	INDEX_ADMIN_SERVICE = "indexAdmin"
	INDEX_SCAN_SERVICE  = "indexScan"
	INDEX_HTTP_SERVICE  = "indexHttp"
)

const CLUSTER_INFO_INIT_RETRIES = 5
const CLUSTER_INFO_VALIDATION_RETRIES = 10

const BUCKET_UUID_NIL = ""

// Helper object for fetching cluster information
// Can be used by services running on a cluster node to connect with
// local management service for obtaining cluster information.
// Info cache can be updated by using Refresh() method.
type ClusterInfoCache struct {
	sync.RWMutex
	url       string
	poolName  string
	logPrefix string
	retries   int

	useStaticPorts bool
	servicePortMap map[string]string

	client      couchbase.Client
	pool        couchbase.Pool
	nodes       []couchbase.Node
	nodesvs     []couchbase.NodeServices
	node2group  map[NodeId]string // node->group
	failedNodes []couchbase.Node
	addNodes    []couchbase.Node
	version     uint64
}

// Helper object that keeps an instance of ClusterInfoCache cached
// and updated periodically or when things change in the cluster
// Readers/Consumers must lock cinfo before using it
type ClusterInfoClient struct {
	cinfo                   *ClusterInfoCache
	clusterURL              string
	pool                    string
	servicesNotifierRetryTm int
	finch                   chan bool
}

type NodeId int

func NewClusterInfoCache(clusterUrl string, pool string) (*ClusterInfoCache, error) {
	c := &ClusterInfoCache{
		url:        clusterUrl,
		poolName:   pool,
		retries:    CLUSTER_INFO_INIT_RETRIES,
		node2group: make(map[NodeId]string),
	}

	return c, nil
}

func FetchNewClusterInfoCache(clusterUrl string, pool string) (*ClusterInfoCache, error) {

	url, err := ClusterAuthUrl(clusterUrl)
	if err != nil {
		return nil, err
	}

	c, err := NewClusterInfoCache(url, pool)
	if err != nil {
		return nil, err
	}

	if ServiceAddrMap != nil {
		c.SetServicePorts(ServiceAddrMap)
	}

	if err := c.Fetch(); err != nil {
		return nil, err
	}

	return c, nil
}

func SetServicePorts(portMap map[string]string) {
	ServiceAddrMap = portMap
}

func (c *ClusterInfoCache) SetLogPrefix(p string) {
	c.logPrefix = p
}

func (c *ClusterInfoCache) SetMaxRetries(r int) {
	c.retries = r
}

func (c *ClusterInfoCache) SetServicePorts(portMap map[string]string) {

	c.useStaticPorts = true
	c.servicePortMap = portMap

}

func (c *ClusterInfoCache) Fetch() error {

	fn := func(r int, err error) error {
		if r > 0 {
			logging.Infof("%vError occured during cluster info update (%v) .. Retrying(%d)",
				c.logPrefix, err, r)
		}

		vretry := 0
	retry:
		c.client, err = couchbase.Connect(c.url)
		if err != nil {
			return err
		}

		c.pool, err = c.client.GetPool(c.poolName)
		if err != nil {
			return err
		}

		var nodes []couchbase.Node
		var failedNodes []couchbase.Node
		var addNodes []couchbase.Node
		version := uint64(math.MaxUint64)
		for _, n := range c.pool.Nodes {
			if n.ClusterMembership == "active" {
				nodes = append(nodes, n)
			} else if n.ClusterMembership == "inactiveFailed" {
				// node being failed over
				failedNodes = append(failedNodes, n)
			} else if n.ClusterMembership == "inactiveAdded" {
				// node being added (but not yet rebalanced in)
				addNodes = append(addNodes, n)
			} else {
				logging.Warnf("ClsuterInfoCache: unrecognized node membership %v", n.ClusterMembership)
			}

			// Find the minimum cluster compatibility
			v := uint64(n.ClusterCompatibility / (1024 * 64))
			if v < version {
				version = v
			}
		}
		c.nodes = nodes
		c.failedNodes = failedNodes
		c.addNodes = addNodes

		c.version = version
		if c.version == math.MaxUint64 {
			c.version = 0
		}

		found := false
		for _, node := range c.nodes {
			if node.ThisNode {
				found = true
			}
		}

		if !found {
			return errors.New("Current node's cluster membership is not active")
		}

		var poolServs couchbase.PoolServices
		poolServs, err = c.client.GetPoolServices(c.poolName)
		if err != nil {
			return err
		}
		c.nodesvs = poolServs.NodesExt

		if err := c.fetchServerGroups(); err != nil {
			return err
		}

		if !c.validateCache() {
			if vretry < CLUSTER_INFO_VALIDATION_RETRIES {
				vretry++
				logging.Infof("%vValidation Failed for cluster info.. Retrying(%d)",
					c.logPrefix, vretry)
				goto retry
			} else {
				logging.Infof("%vValidation Failed for cluster info.. %v",
					c.logPrefix, c)
				return ErrValidationFailed
			}
		}

		return nil
	}

	rh := NewRetryHelper(c.retries, time.Second, 1, fn)
	return rh.Run()
}

func (c *ClusterInfoCache) FetchWithLock() error {
	c.Lock()
	defer c.Unlock()

	return c.Fetch()
}

func (c *ClusterInfoCache) fetchServerGroups() error {

	groups, err := c.pool.GetServerGroups()
	if err != nil {
		return err
	}

	result := make(map[NodeId]string)
	for nid, cached := range c.nodes {
		found := false
		for _, group := range groups.Groups {
			for _, node := range group.Nodes {
				if node.Hostname == cached.Hostname {
					result[NodeId(nid)] = group.Name
					found = true
				}
			}
		}
		if !found {
			logging.Warnf("ClusterInfoCache Initialization: Unable to identify server group for node %v.", cached.Hostname)
		}
	}

	c.node2group = result
	return nil
}

func (c *ClusterInfoCache) GetClusterVersion() uint64 {
	if c.version < 5 {
		return INDEXER_45_VERSION
	}

	return INDEXER_50_VERSION
}

func (c *ClusterInfoCache) GetServerGroup(nid NodeId) string {

	return c.node2group[nid]
}

func (c *ClusterInfoCache) GetNodesByServiceType(srvc string) (nids []NodeId) {
	for i, svs := range c.nodesvs {
		if _, ok := svs.Services[srvc]; ok {
			nids = append(nids, NodeId(i))
		}
	}

	return
}

func (c *ClusterInfoCache) GetActiveIndexerNodes() (nodes []couchbase.Node) {
	for _, n := range c.nodes {
		for _, s := range n.Services {
			if s == "index" {
				nodes = append(nodes, n)
			}
		}
	}

	return
}

func (c *ClusterInfoCache) GetFailedIndexerNodes() (nodes []couchbase.Node) {
	for _, n := range c.failedNodes {
		for _, s := range n.Services {
			if s == "index" {
				nodes = append(nodes, n)
			}
		}
	}

	return
}

func (c *ClusterInfoCache) GetNewIndexerNodes() (nodes []couchbase.Node) {
	for _, n := range c.addNodes {
		for _, s := range n.Services {
			if s == "index" {
				nodes = append(nodes, n)
			}
		}
	}

	return
}

func (c *ClusterInfoCache) GetNodesByBucket(bucket string) (nids []NodeId, err error) {
	b, berr := c.pool.GetBucket(bucket)
	if berr != nil {
		err = berr
		return
	}
	defer b.Close()

	for i, _ := range c.nodes {
		nid := NodeId(i)
		if _, ok := c.findVBServerIndex(b, nid); ok {
			nids = append(nids, nid)
		}
	}

	return
}

//
// Return UUID of a given bucket.
//
func (c *ClusterInfoCache) GetBucketUUID(bucket string) (uuid string) {

	b, err := c.pool.GetBucket(bucket)
	if err != nil {
		return BUCKET_UUID_NIL
	}
	defer b.Close()

	// This node recognize this bucket.   Make sure its vb is resided in at least one node.
	for i, _ := range c.nodes {
		nid := NodeId(i)
		if _, ok := c.findVBServerIndex(b, nid); ok {
			// find the bucket resides in at least one node
			return b.UUID
		}
	}

	// no nodes recognize this bucket
	return BUCKET_UUID_NIL
}

func (c *ClusterInfoCache) IsEphemeral(bucket string) (bool, error) {
	b, err := c.pool.GetBucket(bucket)
	if err != nil {
		return false, err
	}
	defer b.Close()
	return strings.EqualFold(b.Type, "ephemeral"), nil
}

func (c *ClusterInfoCache) GetCurrentNode() NodeId {
	for i, node := range c.nodes {
		if node.ThisNode {
			return NodeId(i)
		}
	}
	// TODO: can we avoid this panic ?
	panic("Current node is not in active membership")
}

func (c *ClusterInfoCache) IsNodeHealthy(nid NodeId) (bool, error) {
	if int(nid) >= len(c.nodes) {
		return false, ErrInvalidNodeId
	}

	return c.nodes[nid].Status == "healthy", nil
}

func (c *ClusterInfoCache) GetNodeStatus(nid NodeId) (string, error) {
	if int(nid) >= len(c.nodes) {
		return "", ErrInvalidNodeId
	}

	return c.nodes[nid].Status, nil
}

func (c *ClusterInfoCache) GetServiceAddress(nid NodeId, srvc string) (addr string, err error) {
	var port int
	var ok bool

	if int(nid) >= len(c.nodesvs) {
		err = ErrInvalidNodeId
		return
	}

	node := c.nodesvs[nid]
	if port, ok = node.Services[srvc]; !ok {
		logging.Errorf("%vInvalid Service %v for node %v. Nodes %v \n NodeServices %v",
			c.logPrefix, srvc, node, c.nodes, c.nodesvs)
		err = ErrInvalidService
		return
	}

	// For current node, hostname might be empty
	// Insert hostname used to connect to the cluster
	cUrl, err := url.Parse(c.url)
	if err != nil {
		return "", errors.New("Unable to parse cluster url - " + err.Error())
	}
	h, _, _ := net.SplitHostPort(cUrl.Host)
	if node.Hostname == "" {
		node.Hostname = h
	}

	addr = net.JoinHostPort(node.Hostname, fmt.Sprint(port))
	return
}

func (c *ClusterInfoCache) GetVBuckets(nid NodeId, bucket string) (vbs []uint32, err error) {
	b, berr := c.pool.GetBucket(bucket)
	if berr != nil {
		err = berr
		return
	}
	defer b.Close()

	idx, ok := c.findVBServerIndex(b, nid)
	if !ok {
		err = ErrNodeNotBucketMember
		return
	}

	vbmap := b.VBServerMap()

	for vb, idxs := range vbmap.VBucketMap {
		if idxs[0] == idx {
			vbs = append(vbs, uint32(vb))
		}
	}

	return
}

func (c *ClusterInfoCache) findVBServerIndex(b *couchbase.Bucket, nid NodeId) (int, bool) {
	bnodes := b.Nodes()

	for idx, n := range bnodes {
		if c.sameNode(n, c.nodes[nid]) {
			return idx, true
		}
	}

	return 0, false
}

func (c *ClusterInfoCache) sameNode(n1 couchbase.Node, n2 couchbase.Node) bool {
	return n1.Hostname == n2.Hostname
}

func (c *ClusterInfoCache) GetLocalServiceAddress(srvc string) (string, error) {

	if c.useStaticPorts {

		h, err := c.GetLocalHostname()
		if err != nil {
			return "", err
		}

		p, e := c.getStaticServicePort(srvc)
		if e != nil {
			return "", e
		}
		return net.JoinHostPort(h, p), nil

	} else {
		node := c.GetCurrentNode()
		return c.GetServiceAddress(node, srvc)
	}
}

func (c *ClusterInfoCache) GetLocalServicePort(srvc string) (string, error) {
	addr, err := c.GetLocalServiceAddress(srvc)
	if err != nil {
		return addr, err
	}

	_, p, e := net.SplitHostPort(addr)
	if e != nil {
		return p, e
	}

	return net.JoinHostPort("", p), nil
}

func (c *ClusterInfoCache) GetLocalServiceHost(srvc string) (string, error) {

	addr, err := c.GetLocalServiceAddress(srvc)
	if err != nil {
		return addr, err
	}

	h, _, err := net.SplitHostPort(addr)
	if err != nil {
		return "", err
	}

	return h, nil
}

func (c *ClusterInfoCache) GetLocalServerGroup() (string, error) {
	node := c.GetCurrentNode()
	return c.GetServerGroup(node), nil
}

func (c *ClusterInfoCache) GetLocalHostAddress() (string, error) {

	cUrl, err := url.Parse(c.url)
	if err != nil {
		return "", errors.New("Unable to parse cluster url - " + err.Error())
	}

	_, p, _ := net.SplitHostPort(cUrl.Host)

	h, err := c.GetLocalHostname()
	if err != nil {
		return "", err
	}

	return net.JoinHostPort(h, p), nil

}

func (c *ClusterInfoCache) GetLocalHostname() (string, error) {

	cUrl, err := url.Parse(c.url)
	if err != nil {
		return "", errors.New("Unable to parse cluster url - " + err.Error())
	}

	h, _, _ := net.SplitHostPort(cUrl.Host)

	nid := c.GetCurrentNode()

	if int(nid) >= len(c.nodesvs) {
		return "", ErrInvalidNodeId
	}

	node := c.nodesvs[nid]
	if node.Hostname == "" {
		node.Hostname = h
	}

	return node.Hostname, nil

}

func (c *ClusterInfoCache) validateCache() bool {

	if len(c.nodes) != len(c.nodesvs) {
		return false
	}

	//validation not required for single node setup(MB-16494)
	if len(c.nodes) == 1 && len(c.nodesvs) == 1 {
		return true
	}

	var hostList1 []string

	for _, n := range c.nodes {
		hostList1 = append(hostList1, n.Hostname)
	}

	for i, svc := range c.nodesvs {
		h := svc.Hostname
		p := svc.Services["mgmt"]

		if h == "" {
			h = "127.0.0.1"
		}

		hp := net.JoinHostPort(h, fmt.Sprint(p))

		if hostList1[i] != hp {
			return false
		}
	}

	return true
}

func (c *ClusterInfoCache) getStaticServicePort(srvc string) (string, error) {

	if p, ok := c.servicePortMap[srvc]; ok {
		return p, nil
	} else {
		return "", ErrInvalidService
	}

}

func NewClusterInfoClient(clusterURL string, pool string, config Config) (c *ClusterInfoClient, err error) {
	cic := &ClusterInfoClient{
		clusterURL: clusterURL,
		pool:       pool,
		finch:      make(chan bool),
	}
	cic.servicesNotifierRetryTm = 1000 // TODO: read from config

	cinfo, err := FetchNewClusterInfoCache(clusterURL, pool)
	if err != nil {
		return nil, err
	}
	cic.cinfo = cinfo

	go cic.watchClusterChanges()
	return cic, err
}

// Consumer must lock returned cinfo before using it
func (c *ClusterInfoClient) GetClusterInfoCache() *ClusterInfoCache {
	return c.cinfo
}

func (c *ClusterInfoClient) watchClusterChanges() {
	selfRestart := func() {
		time.Sleep(time.Duration(c.servicesNotifierRetryTm) * time.Millisecond)
		go c.watchClusterChanges()
	}

	clusterAuthURL, err := ClusterAuthUrl(c.clusterURL)
	if err != nil {
		logging.Errorf("ClusterInfoClient ClusterAuthUrl(): %v\n", err)
		selfRestart()
		return
	}

	scn, err := NewServicesChangeNotifier(clusterAuthURL, c.pool)
	if err != nil {
		logging.Errorf("ClusterInfoClient NewServicesChangeNotifier(): %v\n", err)
		selfRestart()
		return
	}
	defer scn.Close()

	ticker := time.NewTicker(time.Duration(5) * time.Minute)
	defer ticker.Stop()

	// For observing node services config
	ch := scn.GetNotifyCh()
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				selfRestart()
				return
			} else if err := c.cinfo.FetchWithLock(); err != nil {
				logging.Errorf("cic.cinfo.FetchWithLock(): %v\n", err)
				selfRestart()
				return
			}
		case <-ticker.C:
			if err := c.cinfo.FetchWithLock(); err != nil {
				logging.Errorf("cic.cinfo.FetchWithLock(): %v\n", err)
				selfRestart()
				return
			}
		case <-c.finch:
			return
		}
	}
}

func (c *ClusterInfoClient) Close() {
	defer func() { recover() }() // in case async Close is called. Do we need this?

	close(c.finch)
}
