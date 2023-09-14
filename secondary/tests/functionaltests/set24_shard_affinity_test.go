package functionaltests

import (
	"io"
	"log"
	"net/http"
	"os"
	"testing"
	"time"

	c "github.com/couchbase/indexing/secondary/common"
	json "github.com/couchbase/indexing/secondary/common/json"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
)

func getIndexStatusFromIndexer() (*tc.IndexStatusResponse, error) {
	url, err := makeurl("/getIndexStatus?getAll=true")
	if err != nil {
		return nil, err
	}

	var resp *http.Response
	resp, err = http.Get(url)
	if resp != nil {
		defer resp.Body.Close()
	}

	if err != nil {
		return nil, err
	}

	var respbody []byte
	respbody, err = io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var st tc.IndexStatusResponse
	err = json.Unmarshal(respbody, &st)
	if err != nil {
		return nil, err
	}

	return &st, nil
}

func getShardGroupingFromLiveCluster() (tc.AlternateShardMap, error) {
	statuses, err := getIndexStatusFromIndexer()
	if err != nil {
		return nil, err
	}

	shardGrouping := make(tc.AlternateShardMap)
	for _, status := range statuses.Status {
		var replicaMap map[int]map[c.PartitionId][]string
		var partnMap map[c.PartitionId][]string

		var ok bool

		if defnStruct, ok := shardGrouping[status.DefnId]; !ok {
			replicaMap = make(map[int]map[c.PartitionId][]string)
			shardGrouping[status.DefnId] = struct {
				Name         string
				NumReplica   int
				NumPartition int
				IsPrimary    bool
				ReplicaMap   map[int]map[c.PartitionId][]string
			}{
				Name:         status.Name,
				NumReplica:   status.NumReplica,
				NumPartition: status.NumPartition,
				IsPrimary:    status.IsPrimary,
				ReplicaMap:   replicaMap,
			}
		} else {
			replicaMap = defnStruct.ReplicaMap
			defnStruct.NumPartition += status.NumPartition
		}

		if partnMap, ok = replicaMap[status.ReplicaId]; !ok {
			partnMap = make(map[c.PartitionId][]string)
			replicaMap[status.ReplicaId] = partnMap
		}

		for _, partShardMap := range status.AlternateShardIds {
			for partnId, shards := range partShardMap {
				partnMap[c.PartitionId(partnId)] = shards
			}
		}

	}
	return shardGrouping, nil
}

func performClusterStateValidationByAlternateShards(t *testing.T) {
	shardGrouping, err := getShardGroupingFromLiveCluster()
	tc.HandleError(err, "Err in getting Index Status from live cluster")

	if errMap := tc.ValidateClusterState(shardGrouping); len(errMap[tc.ALTERNATE_SHARD_AFFINITY_INVALID_CLUSTER_STATE]) != 0 {
		t.Fatalf("%v:performClusterStateValidationByAlternateShards Shard grouping in live cluster is not valid: %v for live cluster %v", t.Name(), errMap, shardGrouping)
	}
}

func TestWithShardAffinity(t *testing.T) {

	if os.Getenv("STORAGE") != "plasma" {
		t.Skipf("Shard affinity tests only valid with plasma storage")
		return
	}

	err := secondaryindex.ChangeIndexerSettings("indexer.settings.enableShardAffinity", true, clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Unable to change indexer setting `indexer.settings.enableShardAffinity`")
	err = secondaryindex.ChangeIndexerSettings("indexer.planner.honourNodesInDefn", true, clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Unable to change indexer setting `indexer.planner.honourNodesInDefn`")

	defer func() {
		err := secondaryindex.ChangeIndexerSettings("indexer.settings.enableShardAffinity", false, clusterconfig.Username, clusterconfig.Password, kvaddress)
		tc.HandleError(err, "Unable to change indexer setting `indexer.settings.enableShardAffinity`")

		err = secondaryindex.ChangeIndexerSettings("indexer.planner.honourNodesInDefn", false, clusterconfig.Username, clusterconfig.Password, kvaddress)
		tc.HandleError(err, "Unable to change indexer setting `indexer.planner.honourNodesInDefn`")
	}()

	t.Run("RebalanceSetupCluster", func(subt *testing.T) {
		TestRebalanceSetupCluster(subt)
	})
	defer t.Run("RebalanceResetCluster", func(subt *testing.T) {
		TestRebalanceResetCluster(subt)
	})

	t.Run("TestCreateDocsBeforeRebalance", func(subt *testing.T) {
		TestCreateDocsBeforeRebalance(subt)
	})

	t.Run("TestCreateIndexesBeforeRebalance", func(subt *testing.T) {
		TestCreateIndexesBeforeRebalance(subt)
	})

	t.Run("TestShardAffinityInInitialCluster", func(subt *testing.T) {
		performClusterStateValidationByAlternateShards(subt)
	})

	t.Run("TestIndexNodeRebalanceIn", func(subt *testing.T) {
		TestIndexNodeRebalanceIn(subt)

		performClusterStateValidationByAlternateShards(subt)
	})

	t.Run("TestCreateReplicatedIndexesBeforeRebalance", func(subt *testing.T) {
		TestCreateReplicatedIndexesBeforeRebalance(subt)

		performClusterStateValidationByAlternateShards(subt)
	})

	t.Run("TestIndexNodeRebalanceOut", func(subt *testing.T) {
		TestIndexNodeRebalanceOut(subt)

		performClusterStateValidationByAlternateShards(subt)
	})

	log.Printf("going to sleep")
	time.Sleep(100 * time.Second)

	t.Run("TestFailoverAndRebalance", func(subt *testing.T) {
		TestFailoverAndRebalance(subt)

		performClusterStateValidationByAlternateShards(subt)
	})

	t.Run("TestSwapRebalance", func(subt *testing.T) {
		TestSwapRebalance(t)

		performClusterStateValidationByAlternateShards(subt)
	})

	t.Run("TestRebalanceReplicaRepair", func(subt *testing.T) {
		subt.Skipf("Test is disabled temporarily")
		TestRebalanceReplicaRepair(subt)
	})

	t.Run("TestFailureAndRebalanceDuringInitialIndexBuild", func(subt *testing.T) {
		subt.Skipf("Test is disabled temporarily")
		TestFailureAndRebalanceDuringInitialIndexBuild(subt)
	})

	t.Run("TestRedistributWhenNodeIsAddedForFalse", func(subt *testing.T) {
		subt.SkipNow()
		TestRedistributeWhenNodeIsAddedForFalse(subt)

		performClusterStateValidationByAlternateShards(subt)
	})

	t.Run("TestRedistributeWhenNodeInAddedForTrue", func(subt *testing.T) {
		subt.SkipNow()
		TestRedistributeWhenNodeIsAddedForTrue(subt)

		performClusterStateValidationByAlternateShards(subt)
	})

}
