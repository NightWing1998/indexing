package serverlesstests

import (
	"fmt"
	"log"
	"testing"

	c "github.com/couchbase/indexing/secondary/common"
	cluster "github.com/couchbase/indexing/secondary/tests/framework/clusterutility"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
)

var rebalanceTmpDir string

const SHARD_REBALANCE_DIR = "shard_rebalance_storage_dir"

var absRebalStorageDirPath string

func TestShardRebalanceSetup(t *testing.T) {
	log.Printf("In TestShardRebalanceSetup")
	// Create a tmp dir in the current working directory
	makeStorageDir(t)
}

// At this point, there are 2 index nodes in the cluster: Nodes[1], Nodes[2]
// Indexes are created during the TestShardIdMapping test.
//
// This test removes Nodes[1], Nodes[2] from the cluster, adds
// Nodes[3], Nodes[4] into the cluster and initiates a rebalance
// All the indexes are expected to be moved to Nodes[3] & Nodes[4]
func TestTwoNodeSwapRebalance(t *testing.T) {
	log.Printf("In TestTwoNodeSwapRebalance")
	performSwapRebalance([]string{clusterconfig.Nodes[3], clusterconfig.Nodes[4]}, []string{clusterconfig.Nodes[1], clusterconfig.Nodes[2]}, false, false, false, t)

	for _, bucket := range buckets {
		for _, collection := range collections {
			for i, index := range indexes {
				partns := indexPartnIds[i]
				if i%2 == 0 {
					scanIndexReplicas(index, bucket, scope, collection, []int{0, 1}, numScans, numDocs, len(partns), t)
				}
			}
		}
	}
	testDDLAfterRebalance([]string{clusterconfig.Nodes[3], clusterconfig.Nodes[3]}, t)
}

// Prior to this test, indexes are only on Nodes[3], Nodes[4]
// Nodes[3] is in server group "Group 2" & Nodes [4] is in
// server group "Group 1". Indexer will remove Nodes[4], add
// Nodes[2] (which will be in "Group 1") and initiate rebalance.
// After rebalance, all indexes should exist on Nodes[2] & Nodes[3]
func TestSingleNodeSwapRebalance(t *testing.T) {
	log.Printf("In TestSingleNodeSwapRebalance")

	performSwapRebalance([]string{clusterconfig.Nodes[2]}, []string{clusterconfig.Nodes[4]}, false, false, false, t)

	for _, bucket := range buckets {
		for _, collection := range collections {
			for i, index := range indexes {
				partns := indexPartnIds[i]
				if i%2 == 0 { // scan all non-deferred indexes
					scanIndexReplicas(index, bucket, scope, collection, []int{0, 1}, numScans, numDocs, len(partns), t)
				}
			}
		}
	}
	testDDLAfterRebalance([]string{clusterconfig.Nodes[2], clusterconfig.Nodes[3]}, t)
}

// Prior to this, the indexes existed on Nodes[2] & Nodes[3].
// In this test, the indexer on Nodes[3] will be failed over
// initiating a replica repair code path. Nodes[1] will be
// added to the cluster & the indexes should be re-built on
// Nodes[1]. Final index placement would be on Nodes[1] & Nodes[2]
func TestReplicaRepair(t *testing.T) {
	log.Printf("In TestReplicaRepair")

	// Failover Nodes[3]
	if err := cluster.FailoverNode(kvaddress, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[3]); err != nil {
		FailTestIfError(err, fmt.Sprintf("Error while failing over nodes: %v from cluster", clusterconfig.Nodes[3]), t)
	}

	rebalance(t)

	// Now, add Nodes[1] to the cluster
	if err := cluster.AddNodeWithServerGroup(kvaddress, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[1], "index", "Group 2"); err != nil {
		FailTestIfError(err, fmt.Sprintf("Error while adding node %v cluster in server group: Group 2", clusterconfig.Nodes[1]), t)
	}
	rebalance(t)

	// This sleep will ensure that the stats are propagated to client
	// Also, any pending rebalance cleanup is expected to be done during
	// this time - so that validateShardFiles can see cleaned up directories
	waitForStatsUpdate()

	validateIndexPlacement([]string{clusterconfig.Nodes[1], clusterconfig.Nodes[2]}, t)
	validateShardIdMapping(clusterconfig.Nodes[1], t)
	validateShardIdMapping(clusterconfig.Nodes[2], t)

	for _, bucket := range buckets {
		for _, collection := range collections {
			for i, index := range indexes {
				partns := indexPartnIds[i]
				if i%2 == 0 { // Scan all non-deferred indexes
					scanIndexReplicas(index, bucket, scope, collection, []int{0, 1}, numScans, numDocs, len(partns), t)
				}
			}
		}
	}

	verifyStorageDirContents(t)
	testDDLAfterRebalance([]string{clusterconfig.Nodes[1], clusterconfig.Nodes[2]}, t)
}

// Prior to this, the indexes existed on Nodes[1] & Nodes[2].
// In this test, the indexer on Nodes[2] will be failed over
// and Nodes[1] will be swap rebalanced out initiating both
// replica repair & swap rebalance at same time. Final index
// placement would be on Nodes[3] & Nodes[4]
func TestReplicaRepairAndSwapRebalance(t *testing.T) {
	log.Printf("In TestReplicaRepairAndSwapRebalance")

	// Failover Nodes[2]
	if err := cluster.FailoverNode(kvaddress, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[2]); err != nil {
		FailTestIfError(err, fmt.Sprintf("Error while failing over nodes: %v from cluster", clusterconfig.Nodes[2]), t)
	}

	// Now, add Nodes[3] to the cluster
	if err := cluster.AddNodeWithServerGroup(kvaddress, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[3], "index", "Group 2"); err != nil {
		FailTestIfError(err, fmt.Sprintf("Error while adding node %v cluster in server group: Group 2", clusterconfig.Nodes[3]), t)
	}

	// Now, add Nodes[4] to the cluster
	if err := cluster.AddNodeWithServerGroup(kvaddress, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[4], "index", "Group 1"); err != nil {
		FailTestIfError(err, fmt.Sprintf("Error while adding node %v cluster in server group: Group 2", clusterconfig.Nodes[4]), t)
	}

	// Remove nodes also performs rebalance
	if err := cluster.RemoveNodes(kvaddress, clusterconfig.Username, clusterconfig.Password, []string{clusterconfig.Nodes[1]}); err != nil {
		FailTestIfError(err, fmt.Sprintf("Error while removing nodes: %v from cluster", clusterconfig.Nodes[1]), t)
	}

	// This sleep will ensure that the stats are propagated to client
	// Also, any pending rebalance cleanup is expected to be done during
	// this time - so that validateShardFiles can see cleaned up directories
	waitForStatsUpdate()

	validateIndexPlacement([]string{clusterconfig.Nodes[3], clusterconfig.Nodes[4]}, t)
	validateShardIdMapping(clusterconfig.Nodes[3], t)
	validateShardIdMapping(clusterconfig.Nodes[4], t)

	for _, bucket := range buckets {
		for _, collection := range collections {
			for i, index := range indexes {
				partns := indexPartnIds[i]
				if i%2 == 0 { // Scan all non-deferred indexes
					scanIndexReplicas(index, bucket, scope, collection, []int{0, 1}, numScans, numDocs, len(partns), t)
				}
			}
		}
	}

	verifyStorageDirContents(t)
	testDDLAfterRebalance([]string{clusterconfig.Nodes[3], clusterconfig.Nodes[4]}, t)
}

func TestBuildDeferredIndexesAfterRebalance(t *testing.T) {
	log.Printf("In TestBuildDeferredIndexesAfterRebalance")
	index := indexes[1]
	for _, bucket := range buckets {
		for _, collection := range collections {
			n1qlStatement := fmt.Sprintf("build index on `%v`.`%v`.`%v`(%v)", bucket, scope, collection, index)
			execN1qlAndWaitForStatus(n1qlStatement, bucket, scope, collection, index, "Ready", t)
		}
	}

	waitForStatsUpdate()
	validateShardIdMapping(clusterconfig.Nodes[3], t)
	validateShardIdMapping(clusterconfig.Nodes[4], t)

	partns := indexPartnIds[1]
	for _, bucket := range buckets {
		for _, collection := range collections {
			scanIndexReplicas(index, bucket, scope, collection, []int{0, 1}, numScans, numDocs, len(partns), t)
		}
	}

}

// Prior to this test, indexes exist on nodes[3] & nodes[4]
// Indexer tries to drop an index and verifies if the index
// is properly dropped or not
func TestDropIndexAfterRebalance(t *testing.T) {
	log.Printf("In TestDropIndexAfterRebalance")
	for _, bucket := range buckets {
		for _, collection := range collections {
			for i, index := range indexes {
				if i == 0 || i == 1 { // Drop only 0th and 1st index in the list
					err := secondaryindex.DropSecondaryIndex2(index, bucket, scope, collection, indexManagementAddress)
					if err != nil {
						t.Fatalf("Error while dropping index: %v, err: %v", index, err)
					}
				}
			}
		}
	}
	for _, bucket := range buckets {
		for _, collection := range collections {
			for i, index := range indexes {
				if i == 0 || i == 1 { // Drop only 0th and 1st index in the list
					waitForReplicaDrop(index, bucket, scope, collection, 0, t) // wait for replica drop-0
					waitForReplicaDrop(index, bucket, scope, collection, 1, t) // wait for replica drop-1
				}
			}
		}
	}

	waitForStatsUpdate()

	validateShardIdMapping(clusterconfig.Nodes[3], t)
	validateShardIdMapping(clusterconfig.Nodes[4], t)

	for _, bucket := range buckets {
		for _, collection := range collections {
			for i, index := range indexes {
				if i == 0 || i == 1 { // Scan only 0th and 1st index in the list
					scanResults, e := secondaryindex.ScanAll2(index, bucket, scope, collection, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
					if e == nil {
						t.Fatalf("Error excpected when scanning for dropped index but scan didnt fail. index: %v, bucket: %v, scope: %v, collection: %v\n", index, bucket, scope, collection)
						log.Printf("Length of scanResults = %v", len(scanResults))
					} else {
						log.Printf("Scan failed as expected with error: %v, index: %v, bucket: %v, scope: %v, collection: %v\n", e, index, bucket, scope, collection)
					}
				}
			}
		}
	}
}

// Prior to this, indexes existed on nodes[3] & nodes[4].
// The earlier test has dropped the indexes[0], indexes[1]
// This test will do a swap rebalance by removing nodes[3],
// nodes[4], adds nodes[1] & nodes[2]. After rebalance,
// destination should contain only indexes[2:5]
func TestRebalanceAfterDropIndexes(t *testing.T) {
	log.Printf("In TestRebalanceAfterDropIndexes")

	performSwapRebalance([]string{clusterconfig.Nodes[1], clusterconfig.Nodes[2]}, []string{clusterconfig.Nodes[3], clusterconfig.Nodes[4]}, false, false, false, t)

	for _, bucket := range buckets {
		for _, collection := range collections {
			for i, index := range indexes {
				partns := indexPartnIds[i]
				if i%2 == 0 && i > 1 { // indexes[0] & indexes[1] are dropped in earlier tests
					scanIndexReplicas(index, bucket, scope, collection, []int{0, 1}, numScans, numDocs, len(partns), t)
				} else if i == 0 || i == 1 { // Scan only 0th and 1st index in the list
					scanResults, e := secondaryindex.ScanAll2(index, bucket, scope, collection, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
					if e == nil {
						t.Fatalf("Error excpected when scanning for dropped index but scan didnt fail. index: %v, bucket: %v, scope: %v, collection: %v\n", index, bucket, scope, collection)
						log.Printf("Length of scanResults = %v", len(scanResults))
					} else {
						log.Printf("Scan failed as expected with error: %v, index: %v, bucket: %v, scope: %v, collection: %v\n", e, index, bucket, scope, collection)
					}
				}
			}
		}
	}
}

// Prior to this test, the indexes[0], indexes[1] are dropped on
// all collections. This test will re-create them. All indexes
// exist on nodes[1] and nodes[2] due to prior test
func TestCreateIndexsAfterRebalance(t *testing.T) {
	log.Printf("In TestCreateIndexesAfterRebalance")

	for _, bucket := range buckets {
		for _, collection := range collections {
			// Create a normal index
			n1qlStatement := fmt.Sprintf("create index %v on `%v`.`%v`.`%v`(age)", indexes[0], bucket, scope, collection)
			execN1qlAndWaitForStatus(n1qlStatement, bucket, scope, collection, indexes[0], "Ready", t)
			// Create an index with defer_build
			n1qlStatement = fmt.Sprintf("create index %v on `%v`.`%v`.`%v`(age) with {\"defer_build\":true}", indexes[1], bucket, scope, collection)
			execN1qlAndWaitForStatus(n1qlStatement, bucket, scope, collection, indexes[1], "Created", t)
		}
	}

	waitForStatsUpdate()
	validateShardIdMapping(clusterconfig.Nodes[1], t)
	validateShardIdMapping(clusterconfig.Nodes[2], t)

	index := indexes[0]
	partns := indexPartnIds[0]
	for _, bucket := range buckets {
		for _, collection := range collections {
			// Scan only the newly created index
			scanIndexReplicas(index, bucket, scope, collection, []int{0, 1}, numScans, numDocs, len(partns), t)
		}
	}
}

// Prior to this test, all indexes existed on nodes[1] & nodes[2].
// In this test, indexer would drop the C1 & C2 collections of each
// bucket and initiates a rebalance. Rebalance is performed by removing
// nodes[1], nodes[2] from the cluster and adding nodes[3], nodes[4].
//  After rebalance, only the indexes on default collection are expected
// to exist on nodes[3] & nodes[4]

func TestRebalanceAfterDroppedCollections(t *testing.T) {
	log.Printf("In TestRebalanceAfterDroppedCollections")
	for _, bucket := range buckets {
		for _, collection := range collections {
			if collection != "_default" {
				kvutility.DropCollection(bucket, scope, collection, clusterconfig.Username, clusterconfig.Password, kvaddress)
			}
		}
	}

	performSwapRebalance([]string{clusterconfig.Nodes[3], clusterconfig.Nodes[4]}, []string{clusterconfig.Nodes[1], clusterconfig.Nodes[2]}, false, false, false, t)

	for _, bucket := range buckets {
		for _, collection := range collections {
			for i, index := range indexes {
				partns := indexPartnIds[i]
				if collection == "_default" {
					if i%2 == 0 {
						scanIndexReplicas(index, bucket, scope, collection, []int{0, 1}, numScans, numDocs, len(partns), t)
					}
				} else { // Scan on all non-default collections should fail
					if i%2 == 0 { // Scan on all other non-deferred indexes should fail due to keyspace drop
						scanResults, e := secondaryindex.ScanAll2(index, bucket, scope, collection, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
						if e == nil {
							t.Fatalf("Error excpected when scanning for dropped index but scan didnt fail. index: %v, bucket: %v, scope: %v, collection: %v\n", index, bucket, scope, collection)
							log.Printf("Length of scanResults = %v", len(scanResults))
						} else {
							log.Printf("Scan failed as expected with error: %v, index: %v, bucket: %v, scope: %v, collection: %v\n", e, index, bucket, scope, collection)
						}
					}
				}
			}
		}
	}
}
