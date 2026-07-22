# Graph Report - .  (2026-07-21)

## Corpus Check
- Large corpus: 954 files · ~4,424,467 words. Semantic extraction will be expensive (many Claude tokens). Consider running on a subfolder.

## Summary
- 16348 nodes · 48640 edges · 494 communities (433 shown, 61 thin omitted)
- Extraction: 78% EXTRACTED · 22% INFERRED · 0% AMBIGUOUS · INFERRED: 10482 edges (avg confidence: 0.8)
- Token cost: 1,074,531 input · 0 output

## Community Hubs (Navigation)
- Metadata Provider & Index Status
- Indexer Event Loop Core
- Index DDL & Replica Ops
- Functional Test Scan Framework
- Test Utilities & DCP Helpers
- DDL Command Tokens & Scheduling
- Plasma Storage Slice
- DDL Prepare & Storage Mode
- BHive Vector Storage Slice
- Cluster Manager Agent
- Indexer Message Types
- Planner Placement & Costing
- Storage Reader & Metering
- Rebalance Cleanup & Context
- Rebalance Test Setup
- Metadata Repo & Request Handler
- Shard Rebalancer
- Stream Keyspace State
- Test MetaKV & Token Utils
- Common Utilities & Versioning
- Planner Node Usage Model
- Shard Mapping Test Helpers
- MemDB Storage Slice
- KV Test Data Loading
- Common JSON & Encoding
- Slice Deletion & Array Index
- Index Snapshot Maps
- Query Client Metadata Cache
- Metadata Encryption Keys
- ForestDB Slice & Compaction
- Pause-Resume Lifecycle
- Queryport Server Handlers
- CollateJSON Codec
- Planner Slot Placement
- Snapshot Lifecycle Workers
- Scan Aggregation Results
- Index Instance Maps & Compaction
- Backup-Restore Tests
- Planner Sizing Estimator
- DCP Collections Manifest
- Indexer: Rebalancer
- Tests: set03_planner_test.go
- Indexer: scan_request.go
- Queryport: GsiClient
- Common: IndexDefn
- Common: NewDecoder
- Indexer: TsVbuuid
- Indexer: ScanWorker
- Indexer: SliceId
- Indexer: MsgStartShardTransfer
- Indexer: mockSlice
- Common: clusterInfoCacheLiteManager
- Indexer: ShardId
- MemDB: MemDB
- Common: json/encode.go
- Manager: Statistics
- Common: ClusterInfoCache
- Indexer: mutationMgr
- Protobuf: DcpEvent
- Common: dcp_seqno.go
- Common: settingsManager
- Planner: IndexUsage
- Projector: Feed
- Tests: cluster_setup.go
- Indexer: indexer.go
- Projector: Projector
- Tests: set13_groups_aggrs_test.go
- DCP: DcpFeed
- Tools: HandleCommand
- Indexer: requestHandlerCache
- IOWrap: io_wrappers.go
- Queryport: Consistency
- Indexer: ScanRequest
- Indexer: NewIndexer
- Indexer: streamWorker
- Common: AggrFuncType
- Tests: functionaltests/common_test.go
- Common: secondary/common/util.go
- Vector: AcquireGlobal
- Common: NodesInfo
- Indexer: statsManager
- Pipeline: ItemWriter
- Indexer: mockSlice
- Indexer: IndexInst
- Tests: Context
- Indexer: IndexerStats
- DCP: MCResponse
- Protobuf: MutationTopicRequest
- Common: TransferToken
- Common: NewEaRKeyCache
- MemDB: MemDB
- Common: ClusterAuthUrl
- Indexer: KVSender
- MemDB: Skiplist
- Queryport: ScanResultEntries
- Security: tls.go
- Logging: logging.go
- Indexer: Row
- Indexer: Pauser
- Indexer: Resumer
- Indexer: NewAtomicMutationQueue
- Scanreport: stubContext
- Stats: Uint64Val
- Protobuf: Instance
- Dataport: NewKeyVersions
- Queryport: ClientSettings
- Bin: 2ifab.py
- Indexer: MasterServiceManager
- Common: scanner
- Tools: LogMsg
- Planner: AlternateShardId
- Manager: RestoreContext
- Tools: perfContext
- Indexer: taskObj
- MemDB: rawFileWriter
- Indexer: rebalance_service_manager.go
- MemDB: New
- Docs: 2nd Index Tooling Metric Support + Query Metric Suppor
- CLI: main
- Common: InternalVersion
- CollateJSON: NewCodec
- Common: ClusterInfoCacheLiteClient
- Indexer: EncodeAndWrite
- MemDB: memdb_test.go
- Projector: VbucketWorker
- ForestDB: Config
- MemDB: Equal
- Vector: MetricType
- DCP: Bucket
- ForestDB: Doc
- Indexer: setTransferTokenInMetakv
- Planner: proxy.go
- Tests: n1qlclient.go
- Queryport: secondary_index.go
- Security: tls_setting.go
- Common: decodeState
- Protobuf: N1QLTransform
- MemDB: New
- Planner: ShardDealer
- Indexer: CpuThrottle
- Indexer: schedIndexCreator
- Vector: getLastError
- Tests: set04_restful_test.go
- Indexer: requestHandlerContext
- DCP: DcpFeed
- Planner: shard_dealer_test.go
- Vector: convertTo1D
- Indexer: testServer
- Manager: Coordinator
- Indexer: compactionDaemon
- Indexer: isAllowed
- Projector: dcp_seqno_local.go
- Planner: simulator
- Tests: perfstat.go
- DCP: CommandCode
- Docs: GSI RESTful 2i API
- Vector: MockCodebook
- CollateJSON: Int
- DCP: TapFeed
- Tests: GetIndexerNodesHttpAddresses
- Dataport: Server
- Dataport: TransportFlag
- Queryport: Scan6
- Manager: watcher
- Projector: KVData
- Common: common/util_test.go
- Indexer: IndexEntry
- Indexer: MsgStreamUpdate
- Queryport: cbqClient
- Mock: AuthImpl
- Projector: recover
- Protobuf: AddInstancesRequest
- Common: TsVbuuid
- Common: VbmapResponse
- Adminport: httpServer
- Projector: EncryptionMgr
- Common: logstats_file_handler.go
- DCP: mc_test.go
- Indexer: StatsMap
- Indexer: ddlSettings
- Indexer: KeyDataType
- Queryport: Server
- Queryport: Error
- Tools: copy
- Stats: Int64Val
- Indexer: EncryptionMgr
- Indexer: GenericServiceManager
- Common: scanner_test.go
- DCP: server/server_test.go
- Dataport: RouterEndpoint
- Indexer: recoverCodebook
- Indexer: RowHeap
- Adminport: admin_test.go
- Common: KeyPartitionContainer
- DCP: randomScan
- Indexer: IndexStats
- Tests: testContext
- Common: Bucket
- Tests: qcmdContext
- Indexer: indexer/stats_manager.go
- Projector: ResetConfig
- Tools: loadgen.go
- Queryport: connectionPool
- Queryport: client/conn_pool_test.go
- Common: NewServicesChangeNotifier
- Dataport: NewServer
- ForestDB: File
- Queryport: secondaryIndex6
- Indexer: MsgRestartVbuckets
- Queryport: gsiKeyspace
- Projector: statsManager
- Vector: codebookSparse
- Vector: codebook_concurrency_enterprise.go
- Common: Pointer
- Common: NewOperation
- Tools: multibuckets.go
- Transport: TransportPacket
- Indexer: ForestDBIterator
- Indexer: request_handler.go
- Scanreport: aggregate_test.go
- Indexer: MeteringThrottlingMgr
- Common: EaRKeyCache
- Common: KeyVersions
- DCP: mc_storage.go
- Indexer: restServer
- Indexer: FlatFileStatsPersister
- Indexer: Slice
- Indexer: TestVectorPipelineMergeOperator
- Tools: loganalyse/main.go
- Protobuf: TsVbuuid
- Docs: System Diagram: Multiple Buckets
- Vector: codebookIVFPQ
- Common: VbKeyVersions
- Indexer: ShardType
- DCP: newConnectionPool
- Indexer: fdbSnapshot
- Indexer: MeteringThrottlingMgr
- Bin: periodicstats.py
- Indexer: bhive_slice_community.go
- Logging: system_event.go
- Manager: env
- MemDB: mIterator
- Projector: monitorMemUsage
- Common: NewClusterInfoCacheLiteClient
- Manager: common/topology.go
- DCP: connectionPool
- Queryport: secondaryIndex
- ForestDB: Iterator
- Indexer: AutofailoverServiceManager
- Indexer: StorageStatistics
- Indexer: MsgEncryptionDropKey
- Projector: FakeBucket
- Common: NewStatistics
- Docs: Secondary Indexing System Diagram
- Docs: System Diagram: DDL Flow
- MemDB: Node
- Vector: IndexFactory
- Security: GetSecuritySetting
- Dataport: Client
- Indexer: getIndexStatus
- Indexer: SliceEncryptionCallbacks
- Stubs: plasma/plasma_community.go
- Projector: ProjectorStats
- Tests: dolint
- DCP: mc.go
- Common: NewRetryHelper
- ForestDB: SnapInfo
- Indexer: MsgStream
- Common: tagkey_test.go
- Manager: util/util.go
- DCP: pools_test.go
- Indexer: indexer/plasma_community.go
- TestCode: test_action.go
- Tests: dotest
- Cmakelists.txt: GSI CMake Build Configuration
- DCP: SeqOrderState
- Tests: jsondocscanner.go
- Indexer: GetEncryptionKeysBlocking
- MemDB: Item
- MemDB: Iterator
- Tools: qcmdContext
- Docs: System Diagram: Scan Flow
- Tools: generate_json.go
- Common: HandleClientCertAndAuth
- Indexer: GetBucketKDT
- Pipeline: Pipeline
- MemDB: AccessBarrier
- Tools: parseStorageStats.go
- CollateJSON: checkfiles.go
- DCP: AtomicMutationQueue
- Common: VectorQuantizer
- Common: common/timestamp.go
- Indexer: NewScanAdmissionController
- DCP: transport/tap.go
- Indexer: NewPlasmaSlice
- Stubs: plasma/plasma_enterprise.go
- Indexer: SlabManager
- Indexer: ShardStats
- Tests: ByteSliceToString
- ForestDB: size_t
- Indexer: ClusterIndexMetadata
- Manager: CoordinatorState
- NatSort: natsort/sort.go
- Planner: UsageBasedCostMethod
- Common: LogStatsFileHandler
- Skills: GSI Skills (Claude Skills for GSI Engineering
- Stats: Histogram
- Tools: upr/upr.go
- Security: IsToolsConfigUsed
- Tools: Run
- Indexer: MsgUpdateInstMap
- Common: Sample
- Indexer: MsgClustMgrUpdate
- Indexer: handleIndexStatusRequest
- Manager: Txnid
- Manager: NewError
- Queryport: newBackfillCryptReader
- Tools: SiftData
- Indexer: Ctx
- Docs: Secondary Index Design Overview
- Indexer: metering_enterprise.go
- MemDB: Node
- Common: Counter
- Indexer: Error
- ForestDB: Dummy
- Manager: eventManager
- MemDB: Config
- Projector: MemManager
- Protobuf: KeyPartition
- Protobuf: SinglePartition
- Protobuf: TestPartition
- DCP: upr_feed/feed.go
- Docs: Secondary Index Terminology
- Agents.md: Indexer Microservice
- CollateJSON: Codec
- Stubs: bhive_community.go
- Indexer: RebalancePhase
- Common: NewTsVbuuid
- DCP: mcops
- Indexer: pause_objutil.go
- Indexer: index_reader.go
- Indexer: cpu.go
- Indexer: monitorItemsCount
- Indexer: MsgError
- Indexer: testPauseOrResume
- Queryport: statistics
- Projector: bucketDcp
- Stubs: AggregateRecorder
- Docs: Indexer1
- Docs: InitialBuild_Load Sequence Diagram
- Docs: Insert Workflow Sequence Diagram
- Docs: Index Coordinator
- Stubs: mm_enterprise.go
- Common: NewSessionPermissionsCache
- Protobuf: projector/common.go
- Indexer: Console
- Indexer: SnapshotWaitersMapHolder
- Queryport: secondaryIndex3
- Common: memstat.go
- Docs: Projector
- Docs: Stability Timestamp
- Docs: Router
- Docs: Mutation Execution Flow
- CollateJSON: extractEncodedField
- ForestDB: FileInfo
- ForestDB: KVStoreInfo
- Indexer: MsgDropIndex
- MemDB: NodeList
- Protobuf: FailoverLogResponse
- DCP: hello.go
- DCP: main
- Docs: Bootstrap Sequence Diagram
- Docs: InitialBuild Prepare Sequence Diagram
- Docs: Indexer1 (Scan Coordinator
- Indexer: pause_copier_enterprise.go
- MemDB: StatsReport
- Tests: setup.sh
- Tools: dumpconfig.go
- Common: pause_resume_defs.go
- ForestDB: advLock
- Indexer: GetHTTPMux
- Indexer: computeShardProgress
- Build.sh: build.sh
- DCP: mc_res.go
- DCP: dcp/util.go
- Docs: Deferred Index Build (defer_build
- Tests: runtest_clusterrun.sh
- Tools: n1qlperf.sh
- Tools: bufferedscan.sh
- Agents.md: AI Agent Contribution Guidelines
- Queryport: defs.go
- Common: CollectionScope
- Common: ConfigHolder
- Common: ear_rest.go
- Common: NewUUID
- Indexer: bucketStateEnum
- Indexer: IndexStorageStats
- Indexer: MsgIndexerDropCollection
- Manager: LogProposal
- Protobuf: VbmapRequest
- Docs: DCP Protocol
- Tools: DefaultKVStoreConfig
- Indexer: pause_copier_community.go
- Tests: filemgr
- Common: ByteSlices
- Testdata: Test Data Provenance Note
- Indexer: validateAuth
- Indexer: MsgDDLInProgressResponse
- Indexer: MsgDestroyLocalShardData
- Indexer: MsgIndexStorageStats
- Common: parseTag
- Protobuf: FailoverLogRequest
- Common: fold.go
- DCP: main
- Docs: Deployment Diagram (Secondary Index Design
- Goextended: OnceOnSuccess
- MemDB: system_windows.go
- Platform: thp_linux.go
- Tests: verify_thp
- Tools: fdb_throughput.c
- Agents.md: Indexer Dataport
- Audit: audit.go
- Common: ComputeMinTs
- Indexer: compactionHistory
- Indexer: MsgRestoreAndUnlockShards
- Indexer: MsgRestoreShardDone
- Queryport: GetMarshalledInternalVersion
- DCP: main
- Docs: Projector Memory Estimation Formula
- Tests: dobuild
- Tests: standalone-runner.sh
- ForestDB: Error
- License.txt: Per-file Licensing Notice
- Bin: cbq_drop.sh
- Bin: cbq_sanity.sh
- DCP: basic.go
- DCP: Receive2
- Tests: build
- Tests: builder
- Tests: domain
- Tests: Plasma SMAT Fuzz Testing Instructions
- Protobuf: KeyVersions
- Protobuf: Payload
- DCP: VBHash
- Indexer: CheckResult
- Projector: watcherDameon
- Protobuf: TsVb
- Scanreport: aggregateServerMetrics
- ForestDB: error.sh script
- Tests: del-failed
- Tests: dowatch
- Tests: kick
- Tests: redo
- Tests: container-runner.sh
- Tests: setupvm
- Go.mod: github.com/couchbase/indexing
- Readme.md: Couchbase Secondary Indexes (repo README

## God Nodes (most connected - your core abstractions)
1. `T` - 1153 edges
2. `IndexInstId` - 389 edges
3. `Indexer` - 381 edges
4. `PartitionId` - 331 edges
5. `FailTestIfError()` - 327 edges
6. `IndexDefnId` - 326 edges
7. `Message` - 320 edges
8. `StreamId` - 286 edges
9. `Unmarshal()` - 285 edges
10. `IndexUsage` - 225 edges

## Surprising Connections (you probably didn't know these)
- `Claude Contribution Guidelines (duplicate of agents.md)` --semantically_similar_to--> `AI Agent Contribution Guidelines`  [INFERRED] [semantically similar]
  claude.md → agents.md
- `indexer Build Target` --conceptually_related_to--> `Indexer Microservice`  [INFERRED]
  CMakeLists.txt → agents.md
- `CollateJSON Library` --conceptually_related_to--> `Indexer Microservice`  [INFERRED]
  secondary/collatejson/README.rst → agents.md
- `Secondary Index Repository README` --conceptually_related_to--> `Indexer Microservice`  [INFERRED]
  secondary/README.md → agents.md
- `GetShardCompatVersion_Plasma()` --calls--> `IsServerlessDeployment()`  [INFERRED]
  secondary/indexer/plasma_enterprise.go → secondary/common/deployment_model.go

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Mutation Ingestion Pipeline (projector to storage flush)** — agents_projector, agents_dataport, agents_mutation_queue, agents_timekeeper, agents_flusher [EXTRACTED 1.00]
- **GSI GoModBuild Binary Targets** — cmakelists_indexer_target, cmakelists_projector_target, cmakelists_cbindex_target, cmakelists_cbindexperf_target, cmakelists_cbindexplan_target [EXTRACTED 1.00]
- **GSI Functional Test Data Corpus Set** — devcontainer_testdata_twitterfeed1_corpus, devcontainer_testdata_users10k_corpus, devcontainer_testdata_users_mut_corpus [EXTRACTED 1.00]
- **Mutation Data Path from KV to Index Storage** — secondary_docs_design_markdown_projector_projector, secondary_docs_design_markdown_router_router, secondary_docs_design_markdown_system_transporter, secondary_docs_design_markdown_projector_design_keyversions, secondary_docs_design_overview_indexer [EXTRACTED 1.00]
- **StateContext Replication and Master Election** — secondary_docs_design_markdown_index_manager_index_coordinator, secondary_docs_design_markdown_index_manager_index_coordinator_replica, secondary_docs_design_markdown_index_manager_statecontext, secondary_docs_design_markdown_bootstrap_ns_server [EXTRACTED 1.00]
- **Timestamp Promotion Lifecycle (HW to Stability to Persistent to Scan)** — secondary_docs_design_markdown_terminology_high_watermark_timestamp, secondary_docs_design_markdown_terminology_stability_timestamp, secondary_docs_design_markdown_terminology_persistent_timestamp, secondary_docs_design_markdown_terminology_scan_timestamp [EXTRACTED 1.00]
- **Docker-based GSI CI Pipeline** — secondary_tests_ci_scripts_ci_setup_docker_ci_setup, secondary_tests_ci_scripts_setup_run_ci_dc_ci_runner, secondary_tests_ci_scripts_setup_run_standalone_dc_standalone_runner, secondary_tests_ci_scripts_ci_setup_ci_machine_dockerfile, secondary_tests_ci_scripts_ci_setup_apache_server_dockerfile [EXTRACTED 1.00]
- **Secondary Index Scan Query Flow (N1QL to Indexer)** — secondary_docs_n1ql_integration_query_path, secondary_docs_n1ql_scan_indexentry, secondary_docs_querysdk_query_access_protocol, secondary_docs_restful_2i_scan_apis [INFERRED 0.85]
- **GSI Engineering Skills Suite (shared paths.env convention)** — secondary_skills_readme_gsi_skills, secondary_skills_cbcollect_investigation_skill, secondary_skills_ci_investigation_skill, secondary_skills_run_couchbase_server_skill, secondary_skills_readme_paths_env_config [EXTRACTED 1.00]
- **Secondary Index Architectural Flow (Key Modules)** — secondary_docs_design_other_presentation_projector, secondary_docs_design_other_presentation_router, secondary_docs_design_other_presentation_indexer, secondary_docs_design_other_presentation_index_coordinator [EXTRACTED 1.00]
- **Stability and Consistency Model** — secondary_docs_design_other_presentation_timestamp, secondary_docs_design_other_presentation_stability_timestamp, secondary_docs_design_other_presentation_scan_stability, secondary_docs_design_other_presentation_data_consistency [EXTRACTED 1.00]
- **Lexicographic Number Encoding Scheme** — secondary_collatejson_docs_data2bin_recursive_length_prefix_representation, secondary_collatejson_docs_data2bin_signed_integer_encoding, secondary_collatejson_docs_data2bin_small_decimal_encoding, secondary_collatejson_docs_data2bin_large_decimal_encoding, secondary_collatejson_docs_data2bin_floating_point_encoding [EXTRACTED 1.00]
- **Index Coordinator Bootstrap and StateContext Election** — secondary_docs_design_images_bootstrap_ns_server, secondary_docs_design_images_bootstrap_index_coordinator_master, secondary_docs_design_images_bootstrap_index_coordinator_replica [EXTRACTED 1.00]
- **Indexer Bootstrap Handshake and Stream Setup** — secondary_docs_design_images_bootstrap_indexer, secondary_docs_design_images_bootstrap_index_coordinator_master, secondary_docs_design_images_bootstrap_projector [EXTRACTED 1.00]
- **StateContext Replication and Distribution** — secondary_docs_design_images_bootstrap_index_coordinator_master, secondary_docs_design_images_bootstrap_index_coordinator_replica, secondary_docs_design_images_bootstrap_indexer, secondary_docs_design_images_bootstrap_statecontext [INFERRED 0.85]
- **DELETE Mutation Workflow (steps 1-11)** — secondary_docs_design_images_deleteworkflow_projector, secondary_docs_design_images_deleteworkflow_router, secondary_docs_design_images_deleteworkflow_indexer1, secondary_docs_design_images_deleteworkflow_indexer2, secondary_docs_design_images_deleteworkflow_index_coordinator [EXTRACTED 1.00]
- **Stability Snapshot Generation and Ack (steps 7-11)** — secondary_docs_design_images_deleteworkflow_index_coordinator, secondary_docs_design_images_deleteworkflow_indexer1, secondary_docs_design_images_deleteworkflow_indexer2, secondary_docs_design_images_deleteworkflow_stability_timestamp [EXTRACTED 1.00]
- **Query Serving Flow (Client -> Query Cluster -> Index/KV Clusters)** — secondary_docs_design_images_deployment_client, secondary_docs_design_images_deployment_query_cluster, secondary_docs_design_images_deployment_index_cluster, secondary_docs_design_images_deployment_kv_cluster [INFERRED 0.85]
- **Initial Index Build and Load Sequence** — secondary_docs_design_images_initialbuild_load_index_coordinator, secondary_docs_design_images_initialbuild_load_projector1, secondary_docs_design_images_initialbuild_load_router1, secondary_docs_design_images_initialbuild_load_indexer1, secondary_docs_design_images_initialbuild_load_indexer2 [EXTRACTED 1.00]
- **StateContext replication with wait-for-ack across coordinator master and replicas** — secondary_docs_design_images_initialbuild_prepare_index_coordinator_master, secondary_docs_design_images_initialbuild_prepare_index_coordinator_replica1, secondary_docs_design_images_initialbuild_prepare_index_coordinator_replica2, secondary_docs_design_images_initialbuild_prepare_statecontext [EXTRACTED 1.00]
- **CREATE INDEX prepare flow: master requests indexers to allocate storage and update metadata** — secondary_docs_design_images_initialbuild_prepare_index_coordinator_master, secondary_docs_design_images_initialbuild_prepare_indexer1, secondary_docs_design_images_initialbuild_prepare_indexer2, secondary_docs_design_images_initialbuild_prepare_create_index_request [EXTRACTED 1.00]
- **INSERT Mutation Routing Flow (Projector to Router to Indexers)** — secondary_docs_design_images_insertworkflow_projector, secondary_docs_design_images_insertworkflow_router, secondary_docs_design_images_insertworkflow_indexer1, secondary_docs_design_images_insertworkflow_indexer2 [EXTRACTED 1.00]
- **Stability Timestamp Coordination (Coordinator broadcast and Indexer acks)** — secondary_docs_design_images_insertworkflow_index_coordinator, secondary_docs_design_images_insertworkflow_indexer1, secondary_docs_design_images_insertworkflow_indexer2 [EXTRACTED 1.00]
- **Scan request scatter-gather across Index Client, Scan Coordinator, and participating Indexers** — secondary_docs_design_images_scanworkflow_index_client, secondary_docs_design_images_scanworkflow_indexer1_scan_coordinator, secondary_docs_design_images_scanworkflow_indexer2 [EXTRACTED 1.00]
- **Mutation Data Flow: ep-engine to Projector to Router to Mutation Queue to Indexer (steps 1-5)** — secondary_docs_design_images_systemdiagram_ep_engine, secondary_docs_design_images_systemdiagram_projector, secondary_docs_design_images_systemdiagram_router, secondary_docs_design_images_systemdiagram_mutation_queue, secondary_docs_design_images_systemdiagram_indexer [EXTRACTED 1.00]
- **Index Coordinator replicates state to Replica and persists Metadata (steps 8-10)** — secondary_docs_design_images_systemdiagram_index_coordinator, secondary_docs_design_images_systemdiagram_index_coordinator_replica, secondary_docs_design_images_systemdiagram_metadata [EXTRACTED 1.00]
- **Indexer writes to Back Index and Main Index storage (step 11)** — secondary_docs_design_images_systemdiagram_indexer, secondary_docs_design_images_systemdiagram_back_index, secondary_docs_design_images_systemdiagram_main_index [EXTRACTED 1.00]
- **DDL Request Flow (steps 1-4): Server Component -> Index Client -> Index Coordinator -> Indexer -> Index Storage** — secondary_docs_design_images_systemdiagramddl_server_component, secondary_docs_design_images_systemdiagramddl_index_client, secondary_docs_design_images_systemdiagramddl_index_coordinator, secondary_docs_design_images_systemdiagramddl_indexer, secondary_docs_design_images_systemdiagramddl_index_storage [EXTRACTED 1.00]
- **DDL Metadata Persistence and Replication (step 5)** — secondary_docs_design_images_systemdiagramddl_index_coordinator, secondary_docs_design_images_systemdiagramddl_index_coordinator_replica, secondary_docs_design_images_systemdiagramddl_metadata_store [EXTRACTED 1.00]
- **Mutation Stream Setup on KV Node (step 7): Coordinator requests Projector; ep-engine feeds Projector which routes vb streams via Router** — secondary_docs_design_images_systemdiagramddl_index_coordinator, secondary_docs_design_images_systemdiagramddl_projector, secondary_docs_design_images_systemdiagramddl_ep_engine, secondary_docs_design_images_systemdiagramddl_router [EXTRACTED 1.00]
- **KV to Indexer Mutation Stream Pipeline (steps 1-5)** — secondary_docs_design_images_systemdiagrammultiplebuckets_ep_engine, secondary_docs_design_images_systemdiagrammultiplebuckets_projector, secondary_docs_design_images_systemdiagrammultiplebuckets_router, secondary_docs_design_images_systemdiagrammultiplebuckets_mutation_queues, secondary_docs_design_images_systemdiagrammultiplebuckets_indexer [EXTRACTED 1.00]
- **Coordinator Metadata Replication Across Indexer Nodes (steps 8-10)** — secondary_docs_design_images_systemdiagrammultiplebuckets_index_coordinator, secondary_docs_design_images_systemdiagrammultiplebuckets_index_coordinator_replica, secondary_docs_design_images_systemdiagrammultiplebuckets_metadata_store [EXTRACTED 1.00]
- **Per-Bucket VBucket Streams Multiplexed by Projector** — secondary_docs_design_images_systemdiagrammultiplebuckets_bucket_b1, secondary_docs_design_images_systemdiagrammultiplebuckets_bucket_b2, secondary_docs_design_images_systemdiagrammultiplebuckets_vbucket_streams, secondary_docs_design_images_systemdiagrammultiplebuckets_projector [INFERRED 0.85]
- **Scan Request Flow (Steps 1-7: Server Component -> Index Client -> Indexers -> Index Storage -> back to Client)** — secondary_docs_design_images_systemdiagramscan_server_component, secondary_docs_design_images_systemdiagramscan_index_client, secondary_docs_design_images_systemdiagramscan_indexer_node1_indexer, secondary_docs_design_images_systemdiagramscan_indexer_node2_indexer, secondary_docs_design_images_systemdiagramscan_index_storage [EXTRACTED 1.00]
- **Inter-Indexer Scan Coordination (Steps 3-4 between Indexer Node1 and Node2)** — secondary_docs_design_images_systemdiagramscan_indexer_node1_indexer, secondary_docs_design_images_systemdiagramscan_indexer_node2_indexer, secondary_docs_design_images_systemdiagramscan_bucket_timestamps [INFERRED 0.75]

## Communities (494 total, 61 thin omitted)

### Community 0 - "Metadata Provider & Index Status"
Cohesion: 0.02
Nodes (49): event, IndexerStatus, IndexStatsHolder, MetadataProvider, metadataRepo, ServiceMap, Settings, watcher (+41 more)

### Community 1 - "Indexer Event Loop Core"
Cohesion: 0.03
Nodes (21): buildDoneSpec, Indexer, kvRequest, resetList, CopyIndexInstList(), CrashOnError(), GetBucketFromKeyspaceId(), EncryptionMgr (+13 more)

### Community 2 - "Index DDL & Replica Ops"
Cohesion: 0.02
Nodes (78): IndexDefnId, IndexInst, IndexInstId, PartitionId, PartnAlternateShardIdMap, PartnShardIdMap, RebalanceState, TrainingPhase (+70 more)

### Community 3 - "Functional Test Scan Framework"
Cohesion: 0.05
Nodes (193): ArrayIndexScanResponseActual, ScanResponse, ArrayType, ClearMap(), KillIndexer(), PrintArrayScanResultsActual(), PrintScanResults(), PrintScanResultsActual() (+185 more)

### Community 4 - "Test Utilities & DCP Helpers"
Cohesion: 0.03
Nodes (178): Optional[T], User, T, isASCIILetter(), TestFold(), TestFoldAgainstUnicode(), TestWriteOptionsString(), BenchmarkEncodingRequest() (+170 more)

### Community 5 - "DDL Command Tokens & Scheduling"
Cohesion: 0.03
Nodes (121): BuildCommandToken, CommandListener, CreateCommandToken, CreateCommandTokenList, DeleteCommandToken, DeleteCommandTokenList, DropInstanceCommandToken, DropInstanceCommandTokenList (+113 more)

### Community 6 - "Plasma Storage Slice"
Cohesion: 0.02
Nodes (40): meteringStats, plasmaSlice, plasmaSnapshotInfo, token, tokens, windowFunc, Plasma, getMemFree() (+32 more)

### Community 7 - "DDL Prepare & Storage Mode"
Cohesion: 0.03
Nodes (57): PrepareCreateRequest, PrepareCreateRequestOp, DDLRequestSource, IndexKey, IndexState, IndexStatistics, MetadataRequestContext, SparseVector (+49 more)

### Community 8 - "BHive Vector Storage Slice"
Cohesion: 0.02
Nodes (36): Bhive, bhiveSlice, bhiveSnapshot, bhiveSnapshotInfo, InstanceGroup, RecoveryPointCallback, ComputeSHA256ForByteArray(), ComputeSHA256ForFloat32Array() (+28 more)

### Community 9 - "Cluster Manager Agent"
Cohesion: 0.04
Nodes (23): ClustMgrAgent, InitialBuildInfo, Message, Timekeeper, Config, NewClustMgrAgent(), NewMetaNotifier(), SplitKeyspaceId() (+15 more)

### Community 10 - "Indexer Message Types"
Cohesion: 0.02
Nodes (44): EaRKeyInfoForRebal, EaRKeyPathInfo, MsgBuildIndexResponse, MsgBulkUpdateIndexError, MsgCheckDDLInProgress, MsgClustMgrLocal, MsgClustMgrTopology, MsgCodebookTransferResp (+36 more)

### Community 11 - "Planner Placement & Costing"
Cohesion: 0.05
Nodes (148): ConstraintMethod, CostMethod, IndexSpec, PlacementMethod, Plan, Planner, RunConfig, RunStats (+140 more)

### Community 12 - "Storage Reader & Metering"
Cohesion: 0.04
Nodes (37): AggregateRecorderWithCtx, CmpEntry, EntryCallback, FinishCallback, IndexReaderContext, InlineFilterCallback, memdbSnapshot, plasmaReaderCtx (+29 more)

### Community 13 - "Rebalance Cleanup & Context"
Cohesion: 0.04
Nodes (38): Cleanup, failedShardsContainer, rebalanceContext, RebalanceProvider, RebalanceServiceManager, RebalanceToken, RebalSource, RebalTokens (+30 more)

### Community 14 - "Rebalance Test Setup"
Cohesion: 0.06
Nodes (134): sparseRebalanceConfig, PostOptionsRequestToMetaKV2(), RemoveNode(), SetDataAndIndexQuota(), GetIndexSlicePath(), GetIndexStatusResponse(), IndexStatusResponse, HandleError() (+126 more)

### Community 15 - "Metadata Repo & Request Handler"
Cohesion: 0.03
Nodes (48): CustomRequestHandler, EmbeddedServer, IRepoIterator, LocalRepoRef, MetadataKind, MetadataRepo, RemoteRepoRef, Reply (+40 more)

### Community 16 - "Shard Rebalancer"
Cohesion: 0.04
Nodes (43): Group, batchBuildReq, ShardRebalancer, IsServerlessDeployment(), asyncCollectProgress(), extractIndexInfoFromRenamePath(), generateCodebookRenamePaths(), generateCodebookRenamePaths2() (+35 more)

### Community 17 - "Stream Keyspace State"
Cohesion: 0.04
Nodes (59): StreamId, KeyspaceIdAbortInProgressMap, KeyspaceIdAllowMarkFirstSnap, KeyspaceIdBlockMergeForRecovery, KeyspaceIdCollectionId, KeyspaceIdDrainEnabledMap, KeyspaceIdFlushDone, KeyspaceIdFlushEnabledMap (+51 more)

### Community 18 - "Test MetaKV & Token Utils"
Cohesion: 0.05
Nodes (117): MetakvRecurciveDel(), DeleteAllCommandTokens(), isMasterTag(), MarshalTestOptions(), PostOptionsRequestToMetaKV(), ResetMetaKV(), UnmarshalTestOptions(), TestActionAtTag() (+109 more)

### Community 19 - "Common Utilities & Versioning"
Cohesion: 0.03
Nodes (93): ServerPriority, BugA, BugB, BugC, BugD, BugX, BugY, BugZ (+85 more)

### Community 20 - "Planner Node Usage Model"
Cohesion: 0.04
Nodes (36): IndexerNode, RandomPlacement, createNewEjectedNode(), genDefragUtilStats(), getNumIndexRepaired(), getNumTenantsForNode(), Solution, pruneIndexers() (+28 more)

### Community 21 - "Shard Mapping Test Helpers"
Cohesion: 0.04
Nodes (101): AlternateShardMap, ArrayIndexScanResponse, InvalidClusterState, compactionStats, Result, ScanResult, main(), mf() (+93 more)

### Community 22 - "MemDB Storage Slice"
Cohesion: 0.03
Nodes (32): indexMutation, memdbSlice, memdbSnapshotInfo, addArrayKeySizeStat(), docIdFromEntryBytes(), getArrayKeySizesStats(), getKeySizesStats(), Config (+24 more)

### Community 23 - "KV Test Data Loading"
Cohesion: 0.05
Nodes (106): KeyValues, ScanResponseActual, ConnectBucket(), GetFromPools(), PutInPools(), Duration, LogPerfStat(), ExpectedMultiScanResponse_Primary() (+98 more)

### Community 24 - "Common JSON & Encoding"
Cohesion: 0.03
Nodes (76): All, Ambig, byteWithMarshalJSON, byteWithMarshalText, byteWithPtrMarshalJSON, byteWithPtrMarshalText, embed, Embed0 (+68 more)

### Community 25 - "Slice Deletion & Array Index"
Cohesion: 0.05
Nodes (47): keySizeConfig, MutationMeta, ArrayIndexItems(), CompareArrayEntriesWithCount(), FlattenArray(), Codec, splitSecondaryArrayKey(), getKeySizeConfig() (+39 more)

### Community 26 - "Index Snapshot Maps"
Cohesion: 0.04
Nodes (36): IndexSnapMap, IndexSnapMapHolder, IndexSnapshotContainer, KeyspaceIdInstList, KeyspaceIdInstsPerWorker, PartnSnapMap, snapshotWaiter, SnapshotWaitersContainer (+28 more)

### Community 27 - "Query Client Metadata Cache"
Cohesion: 0.04
Nodes (32): comboIndexCacheEntry, IndexMetadata, indexTopology, InstanceDefn, loadHeuristics, loadStats, metadataClient, RollbackTimeChange (+24 more)

### Community 28 - "Metadata Encryption Keys"
Cohesion: 0.03
Nodes (45): MetaEncryptCallbacks, IndexManager, MetadataNotifier, EaRKey, EncrKeysInfo, KeyID, NewMetaEncryptionCallbacks(), MarshallIndexInst() (+37 more)

### Community 29 - "ForestDB Slice & Compaction"
Cohesion: 0.03
Nodes (26): fdbSlice, indexItem, SnapshotInfo, SnapshotInfoContainer, Config, Duration, EaRKey, File (+18 more)

### Community 30 - "Pause-Resume Lifecycle"
Cohesion: 0.05
Nodes (30): PauseResumeRunningMap, pauseResumeRunningMeta, PauseServiceManager, PauseToken, PauseTokenType, ptFilterFn, putFilterFn, rdtFilterFn (+22 more)

### Community 31 - "Queryport Server Handlers"
Cohesion: 0.04
Nodes (25): BackfillWaiter, CountRequestHandler, doneStatus, RequestBroker, ResponseHandlerFactory, ResponseHandlerId, ResponseSender, ResponseTimer (+17 more)

### Community 32 - "CollateJSON Codec"
Cohesion: 0.05
Nodes (69): Integer, Length, Missing, DecodeFloat(), DecodeInt(), DecodeLD(), DecodeSD(), doDecodeInt() (+61 more)

### Community 33 - "Planner Slot Placement"
Cohesion: 0.03
Nodes (4): estimateCodebookMemUsage(), Solution, newSolution(), isEligibleIndex()

### Community 34 - "Snapshot Lifecycle Workers"
Cohesion: 0.06
Nodes (33): CancelCb, IndexSnapshot, PartitionSnapshot, ScanAdmissionController, ScanAdmissionRequest, ScanCoordinator, ScanResponseWriter, CloneIndexSnapshot() (+25 more)

### Community 35 - "Scan Aggregation Results"
Cohesion: 0.05
Nodes (42): Aggregate, aggrResult, aggrRow, aggrVal, entryCache, IndexScanDecoder, IndexScanWriter, ScanPipeline (+34 more)

### Community 36 - "Index Instance Maps & Compaction"
Cohesion: 0.04
Nodes (32): IndexInstMap, CompactionManager, Flusher, IndexInstMapHolder, IndexPartnMap, IndexPartnMapHolder, MsgChannel, MsgUpdateWorker (+24 more)

### Community 37 - "Backup-Restore Tests"
Cohesion: 0.09
Nodes (85): Backup(), GetBucketUUID(), GetNodeUUID(), Restore(), ensureManifest(), WaitForCollectionCreation(), CreateBucket(), DeleteBucket() (+77 more)

### Community 38 - "Planner Sizing Estimator"
Cohesion: 0.04
Nodes (25): CommandType, GeneralSizingMethod, GreedyPlanner, IndexerNodePool, MOISizingMethod, PlasmaSizingMethod, ReplicaMap, ServerGroupMap (+17 more)

### Community 39 - "DCP Collections Manifest"
Cohesion: 0.05
Nodes (35): CollectionManifest, AuthHandler, basicAuth, BucketInfo, Client, GenericMcdAuthHandler, Node, Pool (+27 more)

### Community 40 - "Indexer: Rebalancer"
Cohesion: 0.06
Nodes (28): RebalancerType, Callbacks, DoneCallback, NodeLoad, NodeLoadSlice, ProgressCallback, Rebalancer, Mutex (+20 more)

### Community 41 - "Tests: set03_planner_test.go"
Cohesion: 0.07
Nodes (86): bypassReplicaRepairConstraintCheckTestCase, equivIndexAcrossSGTestCase, excludeInTestCase, greedyPlannerFuncTestCase, greedyPlannerIdxDistTestCase, heterogenousRebalTestCase, incrPlacementTestCase, initialPlacementTestCase (+78 more)

### Community 42 - "Indexer: scan_request.go"
Cohesion: 0.05
Nodes (49): Aggregate, centroidScans, CompositeElementFilter, Filter, Filters, GroupAggr, IndexKeyOrder, IndexPoint (+41 more)

### Community 43 - "Queryport: GsiClient"
Cohesion: 0.06
Nodes (40): Aggregate, BridgeAccessor, CompositeElementFilter, GroupAggr, GroupKey, GsiAccessor, GsiClient, Inclusion (+32 more)

### Community 44 - "Common: IndexDefn"
Cohesion: 0.04
Nodes (44): ClusterInfoProvider, ExprType, HashScheme, IndexDefn, SecExprAttr, SecExprAttrsArray, MsgClustMgrCleanupPartition, MetaIterator (+36 more)

### Community 45 - "Common: NewDecoder"
Cohesion: 0.04
Nodes (65): dropKey, Entry, TestEntry, codeNode, codeResponse, Decoder, decodeThis, Delim (+57 more)

### Community 46 - "Indexer: TsVbuuid"
Cohesion: 0.03
Nodes (12): MsgKeyspaceHWT, MsgKVStreamRepair, MsgMutMgrFlushDone, MsgMutMgrFlushMutationQueue, MsgRecovery, MsgRestartVbucketsResponse, MsgRollback, MsgRollbackDone (+4 more)

### Community 47 - "Indexer: ScanWorker"
Cohesion: 0.05
Nodes (33): ConciseSparseVector, AtomicRowBuffer, IndexScanSource2, MergeOperator, rowDedupKey, ScanJob, ScanWorker, WorkerPool (+25 more)

### Community 48 - "Indexer: SliceId"
Cohesion: 0.05
Nodes (45): StorageEngine, HashedSliceContainer, PartitionInst, SliceContainer, SliceId, GetStorageDirs(), GetStorageEngineForIndexDefn(), IndexInst (+37 more)

### Community 49 - "Indexer: MsgStartShardTransfer"
Cohesion: 0.03
Nodes (9): MsgConfigUpdate, MsgEarKeyCopy, MsgShardTransferCleanup, MsgShardTransferStagingCleanup, MsgStartShardRestore, MsgStartShardTransfer, ShardTransferStatistics, Config (+1 more)

### Community 50 - "Indexer: mockSlice"
Cohesion: 0.03
Nodes (12): mockReaderContext, mockSliceSnapshot, SliceStatus, mockSnapshot, Config, mockSlice, mockSnapshotInfo, snapshotFeeder (+4 more)

### Community 51 - "Common: clusterInfoCacheLiteManager"
Cohesion: 0.05
Nodes (33): bucketInfo, clusterInfoCacheLite, clusterInfoCacheLiteManager, collectionInfo, eventManager, EventType, notifier, PoolInfo (+25 more)

### Community 52 - "Indexer: ShardId"
Cohesion: 0.07
Nodes (31): ShardId, HTTPSetReqAuthCb, Copier, MsgClearShardType, MsgShardTransferResp, plasmaCopyConfigMeta, ShardRefCount, ShardTypeMapper (+23 more)

### Community 53 - "MemDB: MemDB"
Cohesion: 0.06
Nodes (32): CheckPointCallback, deltaWrContext, FileWriter, flushNode, ItemCallback, ItemEntry, KeyCompare, restoreStats (+24 more)

### Community 54 - "Common: json/encode.go"
Cohesion: 0.07
Nodes (55): arrayEncoder, byIndex, byName, byString, condAddrEncoder, encoderFunc, encodeState, encOpts (+47 more)

### Community 55 - "Manager: Statistics"
Cohesion: 0.04
Nodes (51): AuthRequest, AuthResponse, CheckToken, CommitCreateRequest, CommitCreateRequestOp, CommitCreateResponse, DedupedIndexStats, IndexStats (+43 more)

### Community 56 - "Common: ClusterInfoCache"
Cohesion: 0.05
Nodes (7): ClusterInfoCache, ClusterInfoClient, Bucket, Client, Duration, Node, RWMutex

### Community 57 - "Indexer: mutationMgr"
Cohesion: 0.06
Nodes (30): BucketQueueMap, DoneChannel, EncodeCompatMode, IndexerMutationQueue, IndexQueueMap, KeyspaceIdFlusherMap, KeyspaceIdStopChMap, MutationChannel (+22 more)

### Community 58 - "Protobuf: DcpEvent"
Cohesion: 0.05
Nodes (19): DcpEvent, Engine, IndexEvaluator, IndexEvaluatorStats, IndexInst, Partition, FetchRandomKVSample(), processResponse() (+11 more)

### Community 59 - "Common: dcp_seqno.go"
Cohesion: 0.06
Nodes (50): FailoverLog, kvConn, vbFlog, vbItemCountRequest, vbItemCountResponse, vbMinSeqnosRequest, vbSeqnosReader, VbSeqnosReaderHolder (+42 more)

### Community 60 - "Common: settingsManager"
Cohesion: 0.06
Nodes (25): Config, ConfigValue, settingsManager, MapSettings(), NewConfig(), IsDefaultDeployment(), IsProvisionedDeployment(), GetSettingsConfig() (+17 more)

### Community 61 - "Planner: IndexUsage"
Cohesion: 0.05
Nodes (15): IndexerConstraint, IndexUsage, ViolationCode, FormatIndexInstDisplayName(), FormatIndexPartnDisplayName(), GetStatsPrefix(), addToInstRenamePath(), addToInstRenamePath2() (+7 more)

### Community 62 - "Projector: Feed"
Cohesion: 0.07
Nodes (25): Projector, BucketAccess, BucketFeeder, controlFinKVData, controlStreamEnd, controlStreamRequest, Feed, Subscriber (+17 more)

### Community 63 - "Tests: cluster_setup.go"
Cohesion: 0.07
Nodes (66): strSlice, AddNode(), AddNodeAndRebalance(), addNodeFromRest(), AddNodeWithServerGroup(), addNodeWithServerGroupFromRest(), addServerGroup(), failoverFromRest() (+58 more)

### Community 64 - "Indexer: indexer.go"
Cohesion: 0.04
Nodes (37): HandlerFunc, Header, currRequest, IndexDefnCodebookMap, KeyspaceIdCurrRequest, KeyspaceIdFlushInProgressMap, KeyspaceIdIndexCountMap, KeyspaceIdMinMergeTs (+29 more)

### Community 65 - "Projector: Projector"
Cohesion: 0.06
Nodes (31): Client, MessageMarshaller, Request, Server, IndexerMonitor, RefreshSecurityContextOnTopology(), FeedConfigParams(), CloseAndUnregisterEndpoint() (+23 more)

### Community 66 - "Tests: set13_groups_aggrs_test.go"
Cohesion: 0.07
Nodes (67): ClusterConfiguration, GetTaskListResponse, GroupAggrScanResponse, GroupAggrScanResponseActual, IndexStatus, IndexStatusResponse, Aggdoc, Aggdoc1 (+59 more)

### Community 67 - "DCP: DcpFeed"
Cohesion: 0.07
Nodes (23): DcpFeed, DcpFeedname2, DcpStream, FailoverLog, StreamRequestValue, appOpaque(), composeOpaque(), computeLatency() (+15 more)

### Community 68 - "Tools: HandleCommand"
Cohesion: 0.06
Nodes (51): Command, IndexCreate, IndexOperation, doRequest(), get(), Config, FlagSet, main() (+43 more)

### Community 69 - "Indexer: requestHandlerCache"
Cohesion: 0.07
Nodes (28): CacheEncryptionCallbacks, CryptFileWriter, EncryptionCtx, requestHandlerCache, workChEntry_encryptDrop, workChEntry_encryptUpdate, workChEntry_getInUseKeys, workChEntry_keepKeys (+20 more)

### Community 70 - "IOWrap: io_wrappers.go"
Cohesion: 0.08
Nodes (55): main(), CanRenameFile(), CopyDir(), CopyFile(), doMoveDir(), FileSize(), FileSync(), FileMode (+47 more)

### Community 71 - "Queryport: Consistency"
Cohesion: 0.13
Nodes (22): GsiScanClient, ResponseHandler, TsConsistency, Consistency, DataEncodingFormat, NewTsConsistency(), NewTsConsistency(), getEmptySpanForPrimary() (+14 more)

### Community 72 - "Indexer: ScanRequest"
Cohesion: 0.07
Nodes (38): Filter, ScanRequest, SliceSnapshot, computeNprobesAndCtxs(), GroupAggr, IndexInst, IndexKeyOrder, IndexStats (+30 more)

### Community 73 - "Indexer: NewIndexer"
Cohesion: 0.07
Nodes (24): BuildMode, IndexType, StorageMode, GetBuildMode(), SetBuildMode(), NewClusterInfoProvider(), GetClusterStorageMode(), GetStorageMode() (+16 more)

### Community 74 - "Indexer: streamWorker"
Cohesion: 0.07
Nodes (24): ProjectorVersion, firstSnapFlag, KeyspaceIdEnableOSO, KeyspaceIdQueueMap, KeyspaceIdSessionId, MsgUpdateKeyspaceIdQueue, MutationStreamReader, streamWorker (+16 more)

### Community 75 - "Common: AggrFuncType"
Cohesion: 0.06
Nodes (13): AggrFunc, AggrFuncCount, AggrFuncCountN, AggrFuncMax, AggrFuncMin, AggrFuncSum, AggrFuncType, Value (+5 more)

### Community 76 - "Tests: functionaltests/common_test.go"
Cohesion: 0.21
Nodes (56): IndexProjection, Scans, ExpectedMultiScanResponse(), DeleteDocs2(), get3FieldsMultipleSeeks(), get3FieldsMultipleSeeks_Identical(), get3FieldsSingleSeek(), getBoundaryFilters() (+48 more)

### Community 77 - "Common: secondary/common/util.go"
Cohesion: 0.07
Nodes (50): Optional, Handler, MsgClustMgrGetInuseKeys, argParse(), getlogFile(), File, main(), NewEndpointFactory() (+42 more)

### Community 78 - "Vector: AcquireGlobal"
Cohesion: 0.08
Nodes (13): Token, AcquireGlobal(), ReleaseGlobal(), convertToFaissMetric(), clip(), IndexImpl, IndexImpl, WriteIndexIntoBuffer() (+5 more)

### Community 79 - "Common: NodesInfo"
Cohesion: 0.07
Nodes (11): NodeId, NodesInfo, NodeServices, buildEncryptPortMapping(), computeServerVersion(), Node, newNodesInfo(), newNodesInfoWithError() (+3 more)

### Community 80 - "Indexer: statsManager"
Cohesion: 0.11
Nodes (19): IndexerState, statsManager, Audit(), Request, IsAuthValid(), Request, getStatsToBePersistedBinary(), getStatsToBePersistedMap() (+11 more)

### Community 81 - "Pipeline: ItemWriter"
Cohesion: 0.06
Nodes (23): IndexScanSource, BlockBufferReader, BlockBufferWriter, ItemReader, ItemReadWriter, ItemWriter, src, Conn (+15 more)

### Community 82 - "Indexer: mockSlice"
Cohesion: 0.05
Nodes (14): scannerTestHarness, SortOrder, Key, Inclusion, mockSlice, mockSnapshot, mockSnapshotInfo, snapshotFeeder (+6 more)

### Community 83 - "Indexer: IndexInst"
Cohesion: 0.04
Nodes (10): MsgAddIndexInst, MsgClustMgrResetIndexOnRollback, MsgClustMgrResetIndexOnUpgrade, MsgCreateIndex, MsgRecoverIndex, MsgRecoverIndexResp, MsgTKMergeStream, MsgUpdateSnapMap (+2 more)

### Community 84 - "Tests: Context"
Cohesion: 0.10
Nodes (53): ActionID, Context, smatContext, NewMutationMeta(), TestCrasher(), close1(), closeSlices(), closeSnaps() (+45 more)

### Community 85 - "Indexer: IndexerStats"
Cohesion: 0.07
Nodes (13): BucketStats, IndexerStats, IndexerStatsHolder, KeyspaceStats, KeyspaceStatsMap, KeyspaceStatsMapHolder, MapHolder, MsgUpdateKeyspaceStatsMap (+5 more)

### Community 86 - "DCP: MCResponse"
Cohesion: 0.08
Nodes (7): CASState, GetDcpMemcachedTimeout(), Client, MCRequest, Time, Writer, MCResponse

### Community 87 - "Protobuf: MutationTopicRequest"
Cohesion: 0.04
Nodes (12): AddBucketsRequest, MutationTopicRequest, RestartVbucketsRequest, ShutdownVbucketsRequest, TimestampResponse, Error, NewError(), getKeyspaceIdMap() (+4 more)

### Community 88 - "Common: TransferToken"
Cohesion: 0.06
Nodes (26): DDLDuringRebalanceVersion, RebalKeyInfo, ShardRebalanceSchedulingVersion, ShardTokenState, TokenBuildSource, TokenState, TokenTransferMode, TransferToken (+18 more)

### Community 89 - "Common: NewEaRKeyCache"
Cohesion: 0.12
Nodes (52): fakeProvider, NewEaRKeyCache(), EaRKey, EncrKeysInfo, KeyDataType, Mutex, makeInfo(), makeKey() (+44 more)

### Community 90 - "MemDB: MemDB"
Cohesion: 0.08
Nodes (20): dirOpCtx, dirOpGuard, encryptedFileVisitor, EncryptionStats, keyIdVisitor, keyRotationJanitor, keyRotationVisitor, RotationType (+12 more)

### Community 91 - "Common: ClusterAuthUrl"
Cohesion: 0.07
Nodes (27): IndexRequest, FetchNewClusterInfoCache(), FetchNewClusterInfoCache2(), Config, NewClusterInfoCache(), NewClusterInfoClient(), ClusterAuthUrl(), GetBucketUUID() (+19 more)

### Community 92 - "Indexer: KVSender"
Cohesion: 0.11
Nodes (24): KVSender, checkVbListInTS(), compareIfActiveTsEqual(), debugPrintTs(), execWithStopCh(), formatInstances(), getTopicForStreamId(), Client (+16 more)

### Community 93 - "MemDB: Skiplist"
Cohesion: 0.09
Nodes (19): compare(), Node, Pointer, Skiplist, Skiplist, DefaultConfig(), defaultItemSize(), Node (+11 more)

### Community 94 - "Queryport: ScanResultEntries"
Cohesion: 0.06
Nodes (19): bypassResponseReader, Queue, Row, ScanResultEntries, ScanResultKey, ResponseStream, StreamEndResponse, ClientTimings (+11 more)

### Community 95 - "Security: tls.go"
Cohesion: 0.09
Nodes (48): convertHttpError(), ConvertHttpResponse(), Get(), GetLocalHost(), getTLSTransport(), GetURL(), GetWithAuth(), GetWithAuthAndETag() (+40 more)

### Community 96 - "Logging: logging.go"
Cohesion: 0.08
Nodes (27): destination, Ender, Logger, LogLevel, TimedNStackTraces, TimedStackTrace, Debugf(), Errorf() (+19 more)

### Community 97 - "Indexer: Row"
Cohesion: 0.07
Nodes (10): BytesBufPool, allocator, ConCacheObj, ConnectionContext, Queue, Row, NewByteBufferPool(), newAllocator() (+2 more)

### Community 98 - "Indexer: Pauser"
Cohesion: 0.09
Nodes (23): PauseUploadToken, Pauser, PauseResumeCallbacks, PauseResumeDoneCallback, PauseResumeProgressCallback, ChecksumAndCompress(), collectComputeAndReportProgress(), collectPauserResumerProgress() (+15 more)

### Community 99 - "Indexer: Resumer"
Cohesion: 0.09
Nodes (15): ResumeDownloadToken, Resumer, MetakvDel(), IndexInst, ChecksumAndUncompress(), decodeResumeDownloadToken(), IndexIdList, KVEntry (+7 more)

### Community 100 - "Indexer: NewAtomicMutationQueue"
Cohesion: 0.10
Nodes (29): atomicMutationQueue, Mutation, MutationKeys, node, NewMutation(), NewMutationKeys(), getAllocPollInterval(), Bool (+21 more)

### Community 101 - "Scanreport: stubContext"
Cohesion: 0.07
Nodes (30): datastoreContext, stubContext, NewIndexDefnId(), NewRequestBroker(), Credentials, Duration, Error, Time (+22 more)

### Community 102 - "Stats: Uint64Val"
Cohesion: 0.04
Nodes (7): Pointer, BoolVal, Float64Val, MapVal, StringVal, TimeVal, Uint64Val

### Community 103 - "Protobuf: Instance"
Cohesion: 0.07
Nodes (14): DelInstancesRequest, Instance, RepairEndpointsRequest, ShutdownTopicRequest, TopicResponse, Client, TsVbuuid, NewAddBucketsRequest() (+6 more)

### Community 104 - "Dataport: NewKeyVersions"
Cohesion: 0.13
Nodes (46): NewKeyVersions(), NewStreamPayload(), NewVbKeyVersions(), BenchmarkKVEqual(), B, TestKVEqual(), TestPayloadKeyVersions(), TestPayloadVbmap() (+38 more)

### Community 105 - "Queryport: ClientSettings"
Cohesion: 0.05
Nodes (10): ClientSettings, SetRestRequestTimeout(), Bool, Config, Duration, KVEntry, Pointer, RWMutex (+2 more)

### Community 106 - "Bin: 2ifab.py"
Cohesion: 0.07
Nodes (47): bucket_flush(), cb_install(), cb_service(), cb_uninstall(), cleanall(), cluster_init(), create_buckets(), failover() (+39 more)

### Community 107 - "Indexer: MasterServiceManager"
Cohesion: 0.08
Nodes (18): MasterServiceManager, ServerlessManager, Cancel, DefragmentedUtilizationInfo, HealthInfo, NodeID, NodeInfo, PauseParams (+10 more)

### Community 108 - "Common: scanner"
Cohesion: 0.12
Nodes (39): scanner, SyntaxError, TestUnmarshal(), checkValid(), isSpace(), nextValue(), quoteChar(), state0() (+31 more)

### Community 109 - "Tools: LogMsg"
Cohesion: 0.09
Nodes (17): FeedLines, LogMsg, LogMsgs, Request, Requests, analyseLog(), analyseSession(), argParse() (+9 more)

### Community 110 - "Planner: AlternateShardId"
Cohesion: 0.07
Nodes (29): AlternateShard_GroupId, AlternateShard_ReplicaId, AlternateShard_SlotId, AlternateShardId, PartnDistMap, ReplicaDistMap, ReplicaLoad, ShardLoad (+21 more)

### Community 111 - "Manager: RestoreContext"
Cohesion: 0.09
Nodes (25): DeploymentModel, RestoreContext, GetDeploymentModel(), MakeDeploymentModel(), SetDeploymentModel(), populateAggregatedStorageMetrics(), populateStorageTenantMetrics(), addIndexes() (+17 more)

### Community 112 - "Tools: perfContext"
Cohesion: 0.07
Nodes (28): Config, Job, JobResult, perfContext, Result, ScanConfig, ScanResult, parseConfig() (+20 more)

### Community 113 - "Indexer: taskObj"
Cohesion: 0.08
Nodes (14): TaskType, PauseMetadata, taskObj, TestActionStatus, generateNodeDir(), Buffer, CancelFunc, NodeID (+6 more)

### Community 114 - "MemDB: rawFileWriter"
Cohesion: 0.07
Nodes (18): CryptFileReader, FileReader, forestdbFileReader, forestdbFileWriter, rawFileReader, rawFileReaderEncrypted, rawFileWriter, rawFileWriterEncrypted (+10 more)

### Community 115 - "Indexer: rebalance_service_manager.go"
Cohesion: 0.13
Nodes (22): waiters, GetHTTPReqInfo(), filterRunParamsByBucket(), findTopologyByCollection(), GetGlobalTopology(), getWithAuth(), Creds, runParams (+14 more)

### Community 116 - "MemDB: New"
Cohesion: 0.13
Nodes (32): EqualKeyFn, HashFn, NodeTable, ntResult, object, CompareNodeTable(), decodePointer(), encodePointer() (+24 more)

### Community 117 - "Docs: 2nd Index Tooling Metric Support + Query Metric Suppor"
Cohesion: 0.05
Nodes (45): Canonical (Shortest) Representation Uniqueness, Efficient Lexicographic Encoding of Numbers (Peter Seymour, 2008), Floating Point Encoding (Sign, Exponent, Mantissa), Large Decimal Encoding, Lexicographic Order Preservation of Numbers, Recursive Length-Prefix Representation of Natural Numbers, Signed Integer Encoding via Digit Inversion, Small Decimal Encoding (+37 more)

### Community 118 - "CLI: main"
Cohesion: 0.08
Nodes (34): Config, DefaultScanRange, Job, JobResult, RandomScanRange, Result, ScanConfig, ScanRange (+26 more)

### Community 119 - "Common: InternalVersion"
Cohesion: 0.11
Nodes (24): InternalVersion, internalVersionCache, internalVersionChecker, InternalVersionJson, internalVersionMonitor, nodeList, NodesInfoProvider, GetInternalClusterVersion() (+16 more)

### Community 120 - "CollateJSON: NewCodec"
Cohesion: 0.08
Nodes (36): NewCodec(), BenchmarkCompare(), BenchmarkDecode(), BenchmarkEncode(), B, readLines(), TestArrayExplodeJoin(), TestArrayExplodeJoin2() (+28 more)

### Community 122 - "Indexer: EncodeAndWrite"
Cohesion: 0.09
Nodes (31): protoResponseWriter, ScanReqType, Conn, logScanReport(), NewProtoWriter(), packageReport(), ProtobufDecode(), ProtobufEncode() (+23 more)

### Community 123 - "MemDB: memdb_test.go"
Cohesion: 0.13
Nodes (40): Debug(), DefaultConfig(), Config, NewWithConfig(), CountItems(), doGet(), doInsert(), doReplace() (+32 more)

### Community 124 - "Projector: VbucketWorker"
Cohesion: 0.09
Nodes (19): CollectionsEngineMapHolder, EndpointMapHolder, VbucketMapHolder, VbucketWorker, WorkerStats, CloneEndpoints(), CloneEngines(), CloneVbucketMap() (+11 more)

### Community 125 - "ForestDB: Config"
Cohesion: 0.06
Nodes (5): CompactOpt, Config, DurabilityOpt, OpenFlags, SeqTreeOpt

### Community 126 - "MemDB: Equal"
Cohesion: 0.21
Nodes (38): DiskUsage(), Config, testDirOpGuardWithCancel(), testDirOpGuardWithMultiplePremption(), testEncryptedVsUnencryptedDiskSize(), testEncryptedVsUnencryptedDiskSizeWithDeltaFiles(), testEncryptionCleanupDropKeyFiles(), testEncryptionCleanupStaleSnapshotFromSnapKeys() (+30 more)

### Community 127 - "Vector: MetricType"
Cohesion: 0.07
Nodes (29): Codebook, MetricType, PrecomputedDistanceEncoder, SparseCodebook, ScalarQuantizerRange, ConvertSimilarityToMetric(), NewCodebook(), recoverCodebookIVFPQ() (+21 more)

### Community 128 - "DCP: Bucket"
Cohesion: 0.10
Nodes (13): gatheredStats, multiError, UpdateFunc, WriteOptions, WriteUpdateFunc, errorCollector(), getStatsParallel(), Client (+5 more)

### Community 129 - "ForestDB: Doc"
Cohesion: 0.07
Nodes (10): Doc, fdb_kvs_handle, SeqNum, KVStore, allocDoc(), freeDoc(), allocKVStore(), freeKVStore() (+2 more)

### Community 130 - "Indexer: setTransferTokenInMetakv"
Cohesion: 0.14
Nodes (24): IsBuildErrAfterTraining(), IsRebalSafeVectorTrainingError(), IsRetryableTrainListSizeError(), IsVectorTrainingError(), IsVectorTrainingErrorQualifyingDocs(), checkAllIndexersWarmedup(), checkBhiveInstGraphReady(), checkBhiveInstGraphReadyTT() (+16 more)

### Community 131 - "Planner: proxy.go"
Cohesion: 0.12
Nodes (38): LocalIndexMetadata, RestResponse, GetIndexStatKey(), cleanseIndexLayout(), ConvertToIndexUsages(), createIndexerNode(), findIndexerByNodeId(), findTopologyByCollection() (+30 more)

### Community 132 - "Tests: n1qlclient.go"
Cohesion: 0.15
Nodes (39): Ranges2, ChangeQuerySettings(), convertN1QLStats(), convertN1QLStats2(), filtertoranges2(), filtertoranges3(), getConsistency(), GetOrCreateN1QLClient() (+31 more)

### Community 133 - "Queryport: secondary_index.go"
Cohesion: 0.09
Nodes (27): ConnectionSecurityConfig, indexConfig, InternalVersionHandler, Range2, cleanupAllTmpFiles(), cleanupTmpFiles(), encryptBackfill(), getDefaultTmpDir() (+19 more)

### Community 134 - "Security: tls_setting.go"
Cohesion: 0.10
Nodes (32): Int64, UpdateOrRefreshSecurityContextOnTopologyChange(), buildLocalAddr(), EncryptionRequired(), encryptLocalHost(), EncryptPort(), GetEncryptPortMapping(), GetEncryptPorts() (+24 more)

### Community 135 - "Common: decodeState"
Cohesion: 0.13
Nodes (18): decodeState, InvalidUnmarshalError, UnmarshalFieldError, UnmarshalTypeError, unquotedValue, getu4(), Unmarshaler, Type (+10 more)

### Community 136 - "Protobuf: N1QLTransform"
Cohesion: 0.11
Nodes (35): sortByIndices, CollateJSONEncode(), CollateJSONEncode2(), CompileN1QLExpression(), explodeArrayEntries(), filterArrayForMissingEntries(), getValidVectorsFromArray(), AnnotatedValue (+27 more)

### Community 137 - "MemDB: New"
Cohesion: 0.08
Nodes (28): Config, Node, Pointer, Rand, Skiplist, NewBuilder(), NewBuilderWithConfig(), CompareBytes() (+20 more)

### Community 138 - "Planner: ShardDealer"
Cohesion: 0.13
Nodes (19): asGroupID, asReplicaID, asSlotID, nodeHost, ShardCategory, ShardDealer, GetMinPartnsPerShardFromMap(), createShardDealerForIndexers() (+11 more)

### Community 139 - "Indexer: CpuThrottle"
Cohesion: 0.11
Nodes (13): cgroupCpuStats, CpuThrottle, float64Holder, Mutex, Pointer, NewCpuThrottle(), initSystemStatsHandler(), NewSystemStats() (+5 more)

### Community 140 - "Indexer: schedIndexCreator"
Cohesion: 0.11
Nodes (13): schedIndexCreator, schedIndexQueue, schedTokenMonitor, scheduledIndex, scheduledIndexState, Config, Mutex, RWMutex (+5 more)

### Community 141 - "Vector: getLastError"
Cohesion: 0.09
Nodes (9): faissIndex, ParameterSpace, FaissParameterSpace, NewParameterSpace(), getLastError(), decodeListNo(), extractLabels(), IndexImpl (+1 more)

### Community 142 - "Tests: set04_restful_test.go"
Cohesion: 0.12
Nodes (37): doHttpRequest(), doHttpRequestReturnBody(), getscans(), getscanscount(), Request, Response, makeurl(), makeUrlForIndexNode() (+29 more)

### Community 143 - "Indexer: requestHandlerContext"
Cohesion: 0.12
Nodes (13): ScheduleCreateRequest, requestHandlerContext, Crc64Checksum(), getWithAuthAndETag(), Duration, LocalIndexMetadata, Once, Reader (+5 more)

### Community 144 - "DCP: DcpFeed"
Cohesion: 0.12
Nodes (21): DcpFeed, DcpFeedName, FailoverLog, FeedInfo, NewDcpFeedName2(), addtofeed(), copyconfig(), failsafeOp() (+13 more)

### Community 145 - "Planner: shard_dealer_test.go"
Cohesion: 0.24
Nodes (35): createIdxParam, clusterStr(), createDummyIndexerNode(), createDummyIndexerNodes(), createDummyIndexUsage(), createDummyPartitionedIndexUsages(), createDummyReplicaIndexUsages(), createDummyReplicaPartitionedIndexUsage() (+27 more)

### Community 146 - "Vector: convertTo1D"
Cohesion: 0.15
Nodes (31): RecoverCodebook(), computeCoarseCodeSize(), NewCodebookIVFPQ(), TestCodebookIVFPQ(), TestComputeDistanceEncodedPQ(), TestIVFPQConcurrentTiming(), TestIVFPQTiming(), validate_code_size() (+23 more)

### Community 147 - "Indexer: testServer"
Cohesion: 0.22
Nodes (15): testServer, equal2Key(), getProjection(), getScans(), Config, Creds, Inclusion, Request (+7 more)

### Community 148 - "Manager: Coordinator"
Cohesion: 0.09
Nodes (9): ElectionSite, Coordinator, PeerListener, Cond, IRepository, MsgFactory, QuorumVerifier, TxnState (+1 more)

### Community 149 - "Indexer: compactionDaemon"
Cohesion: 0.11
Nodes (10): compactionDaemon, indexCompaction, MsgIndexCompact, computeGarbage(), IndexStats, Mutex, Timer, indexCompactionName() (+2 more)

### Community 150 - "Indexer: isAllowed"
Cohesion: 0.23
Nodes (10): Request, ResponseWriter, doAuth(), Request, ResponseWriter, isAllowed(), rhSend(), rhSendHttpError() (+2 more)

### Community 151 - "Projector: dcp_seqno_local.go"
Cohesion: 0.11
Nodes (20): kvConn, vbSeqnosReader, vbSeqnosRequest, vbSeqnosResponse, addDBSbucket(), BucketSeqsTiming(), CollectSeqnos(), delDBSbucket() (+12 more)

### Community 152 - "Planner: simulator"
Cohesion: 0.13
Nodes (7): BucketSpec, CollectionSpec, simulator, WorkloadSpec, Rand, SAPlanner, Solution

### Community 153 - "Tests: perfstat.go"
Cohesion: 0.13
Nodes (22): aggmtx, aggstats, ByFn, ByStat, bytesBuffer, ByTest, filename, filestat (+14 more)

### Community 154 - "DCP: CommandCode"
Cohesion: 0.09
Nodes (21): argParse(), DcpFeed, FailoverLog, listOfVbnos(), main(), mf(), printFlogs(), receive() (+13 more)

### Community 155 - "Docs: GSI RESTful 2i API"
Cohesion: 0.07
Nodes (34): Secondary Index Clustering via NS-server and gometa, N1QL Integration Points, Query Path (N1QL to Indexer), Read Your Own Write (Timestamp Vector Scan), WHERE Clause in CREATE INDEX DDL, IndexEntry Structure (EntryKey/PrimaryKey), Missing Secondary-Key Behaviour, Shape of Scan Results for N1QL Consumption (+26 more)

### Community 156 - "Vector: MockCodebook"
Cohesion: 0.08
Nodes (10): MockCodebook, VectorMetadata, VectorSimilarity, getVectors(), areCentroidsEqual(), assignPointsToCentroids(), calculateCentroid(), calculateDistance() (+2 more)

### Community 157 - "CollateJSON: Int"
Cohesion: 0.10
Nodes (24): argParse(), compile(), evaluate(), generateFloats(), generateInteger(), generateJSON(), generateLD(), generateSD() (+16 more)

### Community 158 - "DCP: TapFeed"
Cohesion: 0.10
Nodes (14): TapArguments, TapEvent, TapFeed, TapOpcode, TapFeed, Bucket, Bucket, main() (+6 more)

### Community 159 - "Tests: GetIndexerNodesHttpAddresses"
Cohesion: 0.19
Nodes (29): MemdbSnapshotInfo, snapshotInfoContainer, GetMemDBSnapshots(), List, NewSnapshotInfoContainer(), GetIndexerSetting(), GetIndexInstIds(), GetIndexSlicePath2() (+21 more)

### Community 160 - "Dataport: Server"
Cohesion: 0.14
Nodes (15): activeVb, ConnectionError, keeper, netConn, Server, serverMessage, closeConnection(), doReceive() (+7 more)

### Community 161 - "Dataport: TransportFlag"
Cohesion: 0.13
Nodes (15): netAddr, testConnection, BenchmarkReceiveKeyVersions(), BenchmarkReceiveVbmap(), BenchmarkSendVbKeyVersions(), BenchmarkSendVbmap(), constructVbKeyVersions(), Addr (+7 more)

### Community 162 - "Queryport: Scan6"
Cohesion: 0.21
Nodes (16): IndexKeyOrders, secondaryIndex2, IndexConnection, IndexKeyOrder, IndexPartitionSets, ScanConsistency, n1qlError(), n1qlindexordertogsi() (+8 more)

### Community 163 - "Manager: watcher"
Cohesion: 0.10
Nodes (16): notificationHandle, observeHandle, watcher, Cond, EventType, IRepository, MsgFactory, Mutex (+8 more)

### Community 164 - "Projector: KVData"
Cohesion: 0.12
Nodes (9): KVData, KvdataStats, getKVTs(), Config, RouterEndpoint, RWMutex, TsVbuuid, NewKVData() (+1 more)

### Community 165 - "Common: common/util_test.go"
Cohesion: 0.10
Nodes (32): CommonStrings(), ExcludeStrings(), ExcludeUint32(), ExcludeUint64(), HasString(), HasUint32(), HasUint64(), RemoveString() (+24 more)

### Community 166 - "Indexer: IndexEntry"
Cohesion: 0.12
Nodes (8): bhiveCentroidId, IndexEntry, IndexKey, NilIndexKey, primaryKey, secondaryKey, NewBhiveCentroidId(), NewSecondaryKey()

### Community 167 - "Indexer: MsgStreamUpdate"
Cohesion: 0.07
Nodes (3): MsgIndexSnapRequest, MsgStreamUpdate, Time

### Community 168 - "Queryport: cbqClient"
Cohesion: 0.10
Nodes (10): cbqClient, indexError, IndexerService, indexInfo, indexMetaResponse, Client, indexRequest, Response (+2 more)

### Community 169 - "Mock: AuthImpl"
Cohesion: 0.08
Nodes (15): ClientAuthType, DropKeysCallback, GetInUseKeysCallback, GuardrailStatuses, AuthImpl, RefreshKeysCallback, ConfigRefreshCallback(), ClusterEncryptionConfig (+7 more)

### Community 170 - "Projector: recover"
Cohesion: 0.11
Nodes (14): EngineMap, Vbucket, Codec, StringValuePoolForIndex, Values, Config, StreamStatus, NewVbucket() (+6 more)

### Community 171 - "Protobuf: AddInstancesRequest"
Cohesion: 0.09
Nodes (9): Evaluator, Router, AddInstancesRequest, DelBucketsRequest, NewEngine(), getEvaluators(), getRouters(), FeedVersion (+1 more)

### Community 173 - "Common: VbmapResponse"
Cohesion: 0.09
Nodes (15): Vbuckets, VbmapResponse, Intersection(), BenchmarkVbno16to32(), BenchmarkVbno32to16(), BenchmarkVbucketsIntersection(), BenchmarkVbucketsSort(), BenchmarkVbucketsUnion() (+7 more)

### Community 174 - "Adminport: httpServer"
Cohesion: 0.11
Nodes (18): ConnState, Config, Conn, Duration, Listener, Mutex, Reader, Request (+10 more)

### Community 175 - "Projector: EncryptionMgr"
Cohesion: 0.11
Nodes (12): EncryptionMgr, StatsHooks, workItem, workItemType, Bool, Config, EaRKey, KeyDataType (+4 more)

### Community 176 - "Common: logstats_file_handler.go"
Cohesion: 0.14
Nodes (20): CBCWriter, encryptedStatsWriter, KeyProvider, decompressAndEncryptTo(), decryptAndCompressTo(), decryptFileIfNeeded(), DecryptStatsLogFiles(), encryptCompressedStatsFile() (+12 more)

### Community 177 - "DCP: mc_test.go"
Cohesion: 0.10
Nodes (22): StatValue, tracked, BenchmarkDecodeResponse(), BenchmarkTransmitReq(), BenchmarkTransmitReqLarge(), BenchmarkTransmitReqNull(), B, panics() (+14 more)

### Community 178 - "Indexer: StatsMap"
Cohesion: 0.18
Nodes (7): StatsIndexSpec, StatAggrFunc, StatsMap, statsSpec, Creds, NewStatsMap(), StatVal

### Community 179 - "Indexer: ddlSettings"
Cohesion: 0.08
Nodes (7): ddlSettings, ComputeMinPartitionMapFromConfig(), Bool, Config, Pointer, RWMutex, setShardDealerConfig()

### Community 180 - "Indexer: KeyDataType"
Cohesion: 0.17
Nodes (8): EncryptionCallbacks, getActiveEarKeyFromEncrKeysInfo(), KeyDataType, logKDT(), logKeyIDs(), mergeMap(), RegisterCallbacks(), setInUseKeysTest()

### Community 181 - "Queryport: Server"
Cohesion: 0.14
Nodes (16): ConnectionHandler, request, RequestHandler, Server, ServerStats, Application(), Config, Conn (+8 more)

### Community 182 - "Queryport: Error"
Cohesion: 0.19
Nodes (11): Index, IndexKeys, IndexPartition, PartitionType, PrimaryIndex, Error, Expression, Value (+3 more)

### Community 183 - "Tools: copy"
Cohesion: 0.11
Nodes (14): SparseData, SparseVector, encodeQuantizedCodes(), replaceDummyCentroidId(), allocNode(), debugMarkFree(), Node, Pointer (+6 more)

### Community 184 - "Stats: Int64Val"
Cohesion: 0.11
Nodes (3): DcpStats, Average, Int64Val

### Community 185 - "Indexer: EncryptionMgr"
Cohesion: 0.14
Nodes (8): EncryptionMgr, Bool, Config, Int32, KeyID, Mutex, RWMutex, NewEncryptionMgr()

### Community 186 - "Indexer: GenericServiceManager"
Cohesion: 0.12
Nodes (17): GenericServiceManager, genericWaiter, genericWaiters, GetTaskListResponse, TaskResponse, Cancel, Config, Mutex (+9 more)

### Community 187 - "Common: scanner_test.go"
Cohesion: 0.13
Nodes (26): example, indentErrorTest, TestLargeByteSlice(), TestMarshal(), TestUnmarshalMarshal(), ExampleIndent(), Compact(), Buffer (+18 more)

### Community 188 - "DCP: server/server_test.go"
Cohesion: 0.13
Nodes (22): FuncHandler(), MCRequest, Reader, ReadWriteCloser, Writer, HandleIO(), HandleMessage(), must() (+14 more)

### Community 189 - "Dataport: RouterEndpoint"
Cohesion: 0.13
Nodes (8): EndpointStats, RouterEndpoint, FailsafeOpAsync2(), getEndpLogPrefix(), Config, Conn, Duration, NewRouterEndpoint()

### Community 190 - "Indexer: recoverCodebook"
Cohesion: 0.12
Nodes (15): EaRKey, KeyDataType, DecryptFileByChunk(), FileMode, Reader, NewAESGCM256ContextWithOpenSSL(), NewCryptFileReaderWithLabel(), NewEncryptionCtx() (+7 more)

### Community 191 - "Indexer: RowHeap"
Cohesion: 0.14
Nodes (6): RowHeap, RowsCompareLessFn, TopKRowHeap, Row, NewTopKRowHeap(), TestRowHeap()

### Community 192 - "Adminport: admin_test.go"
Cohesion: 0.10
Nodes (17): testMessage, httpClient, BenchmarkClientRequest(), doServer(), B, Server, init(), makeLargeString() (+9 more)

### Community 193 - "Common: KeyPartitionContainer"
Cohesion: 0.14
Nodes (7): Endpoint, KeyPartitionContainer, KeyPartitionDefn, PartitionContainer, PartitionDefn, PartitionKey, NewKeyPartitionContainer()

### Community 194 - "DCP: randomScan"
Cohesion: 0.13
Nodes (10): randomScan, RandomScanner, UUID, getConnName(), Bucket, Client, Bucket, WaitGroup (+2 more)

### Community 195 - "Indexer: IndexStats"
Cohesion: 0.12
Nodes (4): IndexStats, IndexTimingStats, computeAvgArrayLength(), computeAvgItemSize()

### Community 196 - "Tests: testContext"
Cohesion: 0.10
Nodes (8): Credentials, Duration, EaRKey, Error, KeyDataType, Time, Unit, testContext

### Community 197 - "Common: Bucket"
Cohesion: 0.14
Nodes (12): LEB128Dec(), LEB128DecToStr(), LEB128Enc(), LEB128EncFrmStr(), PrependLEB128EncKey(), PrependLEB128EncStrKey(), compareByteSlices(), TestEmptyCollectionID() (+4 more)

### Community 198 - "Tests: qcmdContext"
Cohesion: 0.10
Nodes (8): qcmdContext, Credentials, Duration, EaRKey, Error, KeyDataType, Time, Unit

### Community 199 - "Indexer: indexer/stats_manager.go"
Cohesion: 0.11
Nodes (18): FloatStatAggrFunc, LostReplicaInfo, LostReplicasResponse, replicaKey, statLogger, StatsPersister, TimingStatAggrFunc, buildLostReplicaInfo() (+10 more)

### Community 200 - "Projector: ResetConfig"
Cohesion: 0.14
Nodes (20): MemThrottler, Init(), SetDefaultGCPercent(), SetForceGCOnThreshold(), SetRelaxGCThreshold(), SetRSSThreshold(), SetStatsCollectionInterval(), SetUsedMemThreshold() (+12 more)

### Community 201 - "Tools: loadgen.go"
Cohesion: 0.17
Nodes (22): argParse(), compile(), debugf(), doDelete(), doRead(), doUpdate(), evaluate(), gendocs() (+14 more)

### Community 202 - "Queryport: connectionPool"
Cohesion: 0.17
Nodes (9): ActiveConnOperation, authInfo, connection, connectionPool, Operation, EWMA, Conn, Duration (+1 more)

### Community 203 - "Queryport: client/conn_pool_test.go"
Cohesion: 0.22
Nodes (22): testServer, newConnectionPool(), addToActiveConnections(), closePoolAndServer(), createPoolAndStartServer(), Conn, connectionPool, Listener (+14 more)

### Community 204 - "Common: NewServicesChangeNotifier"
Cohesion: 0.15
Nodes (10): Notification, NotificationType, serviceNotifierInstance, ServicesChangeNotifier, BucketName, Client, Mutex, URL (+2 more)

### Community 205 - "Dataport: NewServer"
Cohesion: 0.15
Nodes (18): DataportKeyVersions, StreamStatus, VbConnectionMap, StreamID(), Client, makeVbmaps(), TestClient(), TestStreamBegin() (+10 more)

### Community 206 - "ForestDB: File"
Cohesion: 0.10
Nodes (9): CommitOpt, fdb_file_handle, fdb_kvs_config, KVStoreConfig, Pointer, FdbFileVersionToString(), FdbStringToFileVersion(), File (+1 more)

### Community 207 - "Queryport: secondaryIndex6"
Cohesion: 0.11
Nodes (8): IndexDistanceType, IndexStorageMode, secondaryIndex4, secondaryIndex6, IndexStatType, gsistatnameton1ql(), gsistatston1ql(), gsistatston1ql2()

### Community 208 - "Indexer: MsgRestartVbuckets"
Cohesion: 0.09
Nodes (3): MsgRestartVbuckets, MsgStreamInfo, Vbucket

### Community 209 - "Queryport: gsiKeyspace"
Cohesion: 0.13
Nodes (11): gsiKeyspace, monitor, getKeyForIndexer(), Duration, RWMutex, initGSIMonitor(), MonitorIndexer(), UnmonitorIndexer() (+3 more)

### Community 210 - "Projector: statsManager"
Cohesion: 0.12
Nodes (12): statsManager, CleanupStaleStatsLogTempFiles(), ForceRotateStatsLog(), Config, EaRKey, EncryptionMgr, LogStats, LogStatsFileHandler (+4 more)

### Community 211 - "Vector: codebookSparse"
Cohesion: 0.09
Nodes (5): getSparseNormalizationEnabled(), IndexImpl, ProjectSparseConcise(), splitmix64(), codebookSparse

### Community 212 - "Vector: codebook_concurrency_enterprise.go"
Cohesion: 0.12
Nodes (10): atomicSem, atomicToken, Semaphore, shardedSem, shardedToken, init(), initShardedSemaphore(), NewAtomicSemaphore() (+2 more)

### Community 213 - "Common: Pointer"
Cohesion: 0.12
Nodes (6): bucketInfoHolder, BucketNameNumVBucketsMapHolder, collectionInfoHolder, nodesInfoHolder, poolInfoHolder, Pointer

### Community 214 - "Common: NewOperation"
Cohesion: 0.21
Nodes (15): Operation, OperationsMonitor, OperationTimeoutCallback, Duration, RWMutex, Time, NewOperation(), NewOperationsMonitor() (+7 more)

### Community 215 - "Tools: multibuckets.go"
Cohesion: 0.13
Nodes (15): RouterEndpoint, RouterEndpointFactory, ClusterUrl(), argParse(), bucketTimestamp(), getProjectorAdminport(), TsVbuuid, main() (+7 more)

### Community 216 - "Transport: TransportPacket"
Cohesion: 0.16
Nodes (13): Encoder, fullRead(), Decoder, computeChecksum(), connWrite(), Receive(), safeBufSlice(), Send() (+5 more)

### Community 217 - "Indexer: ForestDBIterator"
Cohesion: 0.17
Nodes (13): ForestDBIterator, allocFDBSnapIterator(), freeFDBSnapIterator(), Iterator, KVStore, Snapshot, init(), newFDBSnapshotIterator() (+5 more)

### Community 218 - "Indexer: request_handler.go"
Cohesion: 0.10
Nodes (18): IndexRequest, IndexResponse, IndexStatusResponse, indexStatusSorter, NodeUUIDsResponse, RequestType, RestoreResponse, eTagValid() (+10 more)

### Community 219 - "Scanreport: aggregate_test.go"
Cohesion: 0.15
Nodes (21): ServerCounts, aggregateAvgTimings(), aggregatePartitionCounts(), AggregateScanReportsFn(), aggregateTotalCounts(), copyServerCounts(), copyServerTimings(), TestAggregateAvgTimings() (+13 more)

### Community 220 - "Indexer: MeteringThrottlingMgr"
Cohesion: 0.11
Nodes (8): Config, Duration, AggregateRecorder, MeteringThrottlingMgr, Units, UnitType, ResponseWriter, NewMeteringManager()

### Community 221 - "Common: EaRKeyCache"
Cohesion: 0.19
Nodes (10): CbauthEaRKeyProvider, EaRKeyCache, EaRKeyProvider, cloneEarKeys(), EaRKey, EncrKeysInfo, KeyDataType, RWMutex (+2 more)

### Community 223 - "DCP: mc_storage.go"
Cohesion: 0.17
Nodes (19): chanReq, reqHandler, storage, connectionHandler(), Conn, Listener, MCRequest, RequestHandler (+11 more)

### Community 224 - "Indexer: restServer"
Cohesion: 0.18
Nodes (13): handler, reqHandler, request, restServer, target, bucketHandler(), genErrStr(), Creds (+5 more)

### Community 225 - "Indexer: FlatFileStatsPersister"
Cohesion: 0.16
Nodes (7): FlatFileStatsPersister, StatsEncryptionCallbacks, Config, EaRKey, KeyDataType, NewFlatFilePersister(), NewStatsManager()

### Community 226 - "Indexer: Slice"
Cohesion: 0.16
Nodes (14): ientry, MockSliceContainer, Slice, flushWorker(), Duration, M, Rand, WaitGroup (+6 more)

### Community 227 - "Indexer: TestVectorPipelineMergeOperator"
Cohesion: 0.19
Nodes (19): encodeVector(), getProtoScans(), getScanRequest1(), getSliceSnapshot1(), getVectorDataFeeder(), Duration, Scan, projToProtoProj() (+11 more)

### Community 228 - "Tools: loganalyse/main.go"
Cohesion: 0.20
Nodes (16): layout, addInstance(), getIndex(), getIndexer(), getIndexerInfo1(), getIndexerInfo2(), getIndexInfo2(), getIndexInfo3() (+8 more)

### Community 229 - "Protobuf: TsVbuuid"
Cohesion: 0.14
Nodes (4): TsVbuuid, FailoverLog, TsVbuuid, NewTsVbuuid()

### Community 230 - "Docs: System Diagram: Multiple Buckets"
Cohesion: 0.26
Nodes (22): System Diagram: Multiple Buckets, Backfill Queues, Bucket B1, Bucket B2, Catchup Queues, ep-engine, Index Coordinator, Index Coordinator Replica (+14 more)

### Community 231 - "Vector: codebookIVFPQ"
Cohesion: 0.16
Nodes (5): AcquireTraining(), ReleaseTraining(), IndexImpl, RenormL2(), codebookIVFPQ

### Community 232 - "Common: VbKeyVersions"
Cohesion: 0.13
Nodes (11): Payload, VbKeyVersions, KeyVersions, Application(), Config, processMutations(), sortedKeyspaceIds(), sprintCommandCount() (+3 more)

### Community 233 - "Indexer: ShardType"
Cohesion: 0.11
Nodes (7): ShardType, MsgFetchShardKeys, MsgPopulateShardType, ShardKeysResp, ShardLockUnlockReq, ShardKeyBundle, ShardKeyBundle

### Community 234 - "DCP: newConnectionPool"
Cohesion: 0.18
Nodes (17): testT, newConnectionPool(), BenchmarkBestCaseCPGet(), B, Client, TestConnPool(), TestConnPoolClosed(), TestConnPoolClosedFull() (+9 more)

### Community 235 - "Indexer: fdbSnapshot"
Cohesion: 0.12
Nodes (4): fdbSnapshotInfo, fdbSnapshot, KVStore, TsVbuuid

### Community 236 - "Indexer: MeteringThrottlingMgr"
Cohesion: 0.17
Nodes (5): Config, MeteringThrottlingMgr, ResponseWriter, NewMeteringManager(), StatsHttpHandler

### Community 237 - "Bin: periodicstats.py"
Cohesion: 0.17
Nodes (20): exec_matchers(), graph_allocation(), graph_count(), graph_dcplatency(), graph_endp(), graph_gsi(), graph_idxstats(), graph_kvdata() (+12 more)

### Community 238 - "Indexer: bhive_slice_community.go"
Cohesion: 0.10
Nodes (5): DestroyShard_Bhive(), GetEmptyShardInfo_Bhive(), bhiveReaderCtx, IndexInst, RemapSlice_Bhive()

### Community 239 - "Logging: system_event.go"
Cohesion: 0.18
Nodes (19): EventSeverity, ErrorEvent(), FatalEvent(), getClient(), getErrLogger(), Client, Duration, InfoEvent() (+11 more)

### Community 240 - "Manager: env"
Cohesion: 0.15
Nodes (6): config, env, node, Addr, newEnv(), resolveAddr()

### Community 241 - "MemDB: mIterator"
Cohesion: 0.20
Nodes (7): Iterator, Node, Pointer, NewMergeIterator(), heapItem, mIterator, nodeHeap

### Community 242 - "Projector: monitorMemUsage"
Cohesion: 0.17
Nodes (14): GetCpuPercent(), GetMemFree(), GetMemTotal(), GetRSS(), computeThrottleLevel(), GetForceGCOnThreshold(), GetGCPercent(), GetRelaxGCThreshold() (+6 more)

### Community 243 - "Common: NewClusterInfoCacheLiteClient"
Cohesion: 0.15
Nodes (11): BucketInfoProvider, CollectionInfoProvider, RWLockable, StubRWMutex, Config, newClusterInfoCacheLite(), NewClusterInfoCacheLiteClient(), Config (+3 more)

### Community 244 - "Manager: common/topology.go"
Cohesion: 0.12
Nodes (14): GlobalTopology, IndexDefnDistribution, IndexInstDistribution, IndexKeyPartDistribution, IndexPartDistribution, IndexSinglePartDistribution, IndexSliceLocator, IndexTopology (+6 more)

### Community 245 - "DCP: connectionPool"
Cohesion: 0.22
Nodes (11): connectionPool, defaultMkConn(), GetCollectionSeqs(), GetCollectionSeqsAllVbStates(), GetSeqs(), GetSeqsAllVbStates(), GetSeqsWithExtras(), Client (+3 more)

### Community 247 - "ForestDB: Iterator"
Cohesion: 0.17
Nodes (7): Iterator, IteratorOpt, SeekOpt, allocIterator(), freeIterator(), KVStore, KVStore

### Community 248 - "Indexer: AutofailoverServiceManager"
Cohesion: 0.18
Nodes (12): AutofailoverServiceManager, cpuThrottleExpirer, HealthInfo, IndexStatus, Mutex, NodeID, Ticker, Time (+4 more)

### Community 249 - "Indexer: StorageStatistics"
Cohesion: 0.16
Nodes (5): IndexWriter, StorageStatistics, ComputePercent(), ComputePercentFloat(), getCompressionRatio()

### Community 250 - "Indexer: MsgEncryptionDropKey"
Cohesion: 0.15
Nodes (5): MsgEncryptionDropKey, MsgEncryptionGetInuseKeys, MsgEncryptionUpdateKey, EaRKey, KeyDataType

### Community 251 - "Projector: FakeBucket"
Cohesion: 0.13
Nodes (5): FakeBucket, FakeStream, FailoverLog, TsVbuuid, NewFakeBuckets()

### Community 252 - "Common: NewStatistics"
Cohesion: 0.18
Nodes (16): NewStatistics(), BenchmarkStatDecode(), BenchmarkStatDecrScalar(), BenchmarkStatDecrVector(), BenchmarkStatEncode(), BenchmarkStatGet(), BenchmarkStatIncrScalar(), BenchmarkStatIncrVector() (+8 more)

### Community 253 - "Docs: Secondary Indexing System Diagram"
Cohesion: 0.28
Nodes (19): Back Index, Backfill Queue, Catchup Queue, Secondary Indexing System Diagram, ep-engine, HW Timestamp, Index Coordinator, Index Coordinator Replica (+11 more)

### Community 254 - "Docs: System Diagram: DDL Flow"
Cohesion: 0.29
Nodes (19): Catchup Queue, System Diagram: DDL Flow, ep-engine, Index Client, Index Coordinator, Index Coordinator Replica, Index Storage (Forward/Back Index files, e.g. I1(FI)/I1(BI)), Indexer (+11 more)

### Community 255 - "MemDB: Node"
Cohesion: 0.23
Nodes (7): allocNode(), debugMarkFree(), Pointer, Node, NodeRef, NodeMM, NodeRefMM

### Community 256 - "Vector: IndexFactory"
Cohesion: 0.22
Nodes (16): IndexImpl, IndexFactory(), IndexImpl, NewIndexFlat(), NewIndexIVF_HNSW(), NewIndexIVFPQ(), NewIndexIVFPQ_HNSW(), NewIndexIVFRaBitQ_HNSW() (+8 more)

### Community 257 - "Security: GetSecuritySetting"
Cohesion: 0.24
Nodes (18): CertPool, ClientHelloInfo, CRLScope, crlVerifyPeerCertificate(), getCertPoolForClient(), getCertPoolForServer(), getCurrentTLSConfigFromSettingForServer(), getLatestServerTLSConfig() (+10 more)

### Community 258 - "Dataport: Client"
Cohesion: 0.18
Nodes (6): Client, FailsafeOpAsync(), Config, Conn, Duration, NewClient()

### Community 259 - "Indexer: getIndexStatus"
Cohesion: 0.16
Nodes (10): constraints, addHost(), BucketRequestHandler(), buildTopologyMapPerCollection(), Host2key(), Counter, Creds, IndexTopology (+2 more)

### Community 260 - "Indexer: SliceEncryptionCallbacks"
Cohesion: 0.12
Nodes (6): cursorCtx, readersReserve, SliceEncryptionCallbacks, KeyDataType, nopSliceEncryptionCallbacks(), Weighted

### Community 261 - "Stubs: plasma/plasma_community.go"
Cohesion: 0.12
Nodes (9): StubType, MemTunerConfig, MemTunerDistStats, Request, ResponseWriter, Time, MakeMemTunerConfig(), MakeMemTunerDistStats() (+1 more)

### Community 262 - "Projector: ProjectorStats"
Cohesion: 0.19
Nodes (8): FeedStats, KeyspaceIdStats, ProjectorStats, ProjectorStatsHolder, Accmulate(), AccumulateJson(), Pointer, NewProjectorStats()

### Community 263 - "Tests: dolint"
Cohesion: 0.12
Nodes (8): dolint script, C_INCLUDE_PATH, CGO_CPPFLAGS, CGO_LDFLAGS, ensure_golangci_lint(), GO111MODULE, GOPATH, PATH

### Community 264 - "DCP: mc.go"
Cohesion: 0.15
Nodes (10): CasFunc, CasOp, ObservedStatus, ObserveResult, Connect(), Duration, ReadWriteCloser, TestConnect() (+2 more)

### Community 265 - "Common: NewRetryHelper"
Cohesion: 0.18
Nodes (9): CbAuthHandler, retryFunc, RetryHelper, Duration, NewRetryHelper(), Client, GetCollectionItemCount(), Projector (+1 more)

### Community 266 - "ForestDB: SnapInfo"
Cohesion: 0.15
Nodes (7): CommitMarker, SnapInfo, SnapInfos, SnapMarker, fdb_snapshot_info_t, fdb_snapshot_marker_t, File

### Community 267 - "Indexer: MsgStream"
Cohesion: 0.13
Nodes (3): MsgStream, MutationSnapshot, StreamStatus

### Community 268 - "Common: tagkey_test.go"
Cohesion: 0.12
Nodes (16): badCodeTag, badFormatTag, basicLatin2xTag, basicLatin3xTag, basicLatin4xTag, basicLatin5xTag, basicLatin6xTag, basicLatin7xTag (+8 more)

### Community 269 - "Manager: util/util.go"
Cohesion: 0.13
Nodes (8): ProjectorStreamClient, Client, TsVbuuid, NewFakeProjector(), fakeAddressProvider, fakeProjector, TestDefaultClientEnv, TestDefaultClientFactory

### Community 270 - "DCP: pools_test.go"
Cohesion: 0.21
Nodes (15): assert(), Node, Pointer, mkNL(), TestBucketConnPool(), TestBucketConnPoolConcurrent(), TestCommonAddressSuffixCommon(), TestCommonAddressSuffixEmpty() (+7 more)

### Community 271 - "Indexer: indexer/plasma_community.go"
Cohesion: 0.12
Nodes (9): deleteFreeWriters(), DestroyShard_Plasma(), GetEmptyShardInfo_Plasma(), Config, IndexInst, IndexStats, MeteringThrottlingMgr, NewPlasmaSlice() (+1 more)

### Community 272 - "TestCode: test_action.go"
Cohesion: 0.15
Nodes (11): CorruptIndex(), Config, IndexInst, IgnoreAlternateShardIds(), UseOldIndexPath(), CorruptIndex(), Config, IndexInst (+3 more)

### Community 273 - "Tests: dotest"
Cohesion: 0.21
Nodes (16): dotest script, C_INCLUDE_PATH, CGO_LDFLAGS, collect_logs(), error_email(), error_exit(), GO111MODULE, GOPATH (+8 more)

### Community 274 - "Cmakelists.txt: GSI CMake Build Configuration"
Cohesion: 0.18
Nodes (16): Projector Microservice, GSI CMake Build Configuration, cbindex Build Target, cbindexperf Build Target, cbindexplan Build Target, FAISS / vectors Build Option, ForestDB Dependency, indexer Build Target (+8 more)

### Community 275 - "DCP: SeqOrderState"
Cohesion: 0.18
Nodes (7): endpointBuffers, Conn, KeyVersions, RouterEndpoint, newEndpointBuffers(), NewSeqOrderState(), SeqOrderState

### Community 276 - "Tests: jsondocscanner.go"
Cohesion: 0.17
Nodes (12): Result, ResultList, applyFilters(), compareResult(), compareSecondaryKeys(), ExpectedMultiScanCount(), ExpectedScanLimitResponse_string(), CompositeElementFilter (+4 more)

### Community 277 - "Indexer: GetEncryptionKeysBlocking"
Cohesion: 0.25
Nodes (3): getActiveKeyIdCipherFromEncrKeysInfo(), EaRKey, EncrKeysInfo

### Community 278 - "MemDB: Item"
Cohesion: 0.27
Nodes (6): Item, MemDB, Pointer, Reader, Writer, ItemSize()

### Community 279 - "MemDB: Iterator"
Cohesion: 0.23
Nodes (4): Iterator, MemDB, Node, Snapshot

### Community 280 - "Tools: qcmdContext"
Cohesion: 0.16
Nodes (5): qcmdContext, Credentials, Error, Time, Unit

### Community 281 - "Docs: System Diagram: Scan Flow"
Cohesion: 0.35
Nodes (16): Bucket Timestamps (HWT/ST for B1, B2), Catchup Queue, System Diagram: Scan Flow, Index Client, Index Coordinator, Index Coordinator Replica, Index Instances (I1-I4), Index Storage Snapshots (Forward/Back Index: I1-I4 FI/BI) (+8 more)

### Community 282 - "Tools: generate_json.go"
Cohesion: 0.32
Nodes (15): generateJsonFromInp(), generateSparseVector(), getAltEmail(), getCity(), getCoins(), getCountry(), getCounty(), getEmail() (+7 more)

### Community 283 - "Common: HandleClientCertAndAuth"
Cohesion: 0.19
Nodes (9): CbAuthHttpReqWrapper, ConnectionState, createBasicAuthValue(), Conn, Creds, HttpRequest, Mutex, HandleClientCertAndAuth() (+1 more)

### Community 284 - "Indexer: GetBucketKDT"
Cohesion: 0.14
Nodes (5): MsgEncryptionImportKeys, GetBucketKDT(), KeyDataType, EncrKeysInfo, KeyDataType

### Community 285 - "Pipeline: Pipeline"
Cohesion: 0.23
Nodes (8): Pipeline, pipelineObject, Runnable, Source, Writer, Filter, Sink, WaitGroup

### Community 286 - "MemDB: AccessBarrier"
Cohesion: 0.29
Nodes (9): CompareBS(), Mutex, Pointer, Skiplist, newAccessBarrier(), newBarrierSession(), AccessBarrier, BarrierSession (+1 more)

### Community 287 - "Tools: parseStorageStats.go"
Cohesion: 0.27
Nodes (14): findIndexInfo(), formatStatFloat64(), formatStatInt64(), formatStorageStats(), getStorageStatsAsBytes(), Scanner, isPeriodicStats(), isStorageStats() (+6 more)

### Community 288 - "CollateJSON: checkfiles.go"
Cohesion: 0.23
Nodes (10): codeList, codeObj, argParse(), doSort(), encodeLines(), Codec, lines(), main() (+2 more)

### Community 289 - "DCP: AtomicMutationQueue"
Cohesion: 0.23
Nodes (5): AtomicMutationQueue, node, MCRequest, Pointer, NewAtomicMutationQueue()

### Community 290 - "Common: VectorQuantizer"
Cohesion: 0.20
Nodes (7): QuantizationType, VectorQuantizer, computeNlistFromItemsCount(), ParseSparseVectorDescription(), ParseVectorDesciption(), TestSparseVectorQuantizerParser(), TestVectorQuantizerParser()

### Community 291 - "Common: common/timestamp.go"
Cohesion: 0.24
Nodes (7): TsVb, TsVbuuidPoolMap, TsVbuuidPoolMapHolder, Pointer, init(), NewTsVbuuid2(), NewTsVbuuidCached()

### Community 292 - "Indexer: NewScanAdmissionController"
Cohesion: 0.32
Nodes (13): SetClusterStorageMode(), NewScanAdmissionController(), SkipTest_scanCoordinator_fillCodebookMap(), Test_scanCoordinator_findIndexInstance(), TestScanAdmissionController(), TestScanAdmissionControllerConfigDisable(), TestScanAdmissionControllerConfigEnable(), TestScanAdmissionControllerIndependentMemoryConditions() (+5 more)

### Community 293 - "DCP: transport/tap.go"
Cohesion: 0.23
Nodes (10): Reader, MCRequest, TapParseBool(), TapParseUint16(), TapParseUint64(), TapParseVBList(), TestTapConnectFlagNameString(), TapConnect (+2 more)

### Community 294 - "Indexer: NewPlasmaSlice"
Cohesion: 0.14
Nodes (11): BackupCorruptedSlice_Plasma(), DestroyShard_Plasma(), GetEmptyShardInfo_Plasma(), GetShardCompatVersion_Plasma(), Config, IndexStats, MeteringThrottlingMgr, ListPlasmaSlices() (+3 more)

### Community 295 - "Stubs: plasma/plasma_enterprise.go"
Cohesion: 0.16
Nodes (6): MemTunerConfig, MemTunerDistStats, Time, MakeMemTunerConfig(), MakeMemTunerDistStats(), RunMemQuotaTuner()

### Community 296 - "Indexer: SlabManager"
Cohesion: 0.22
Nodes (4): Arena, SlabManager, Mutex, NewSlabManager()

### Community 297 - "Indexer: ShardStats"
Cohesion: 0.21
Nodes (3): ShardStats, MsgShardStatsRequest, NewShardStats()

### Community 298 - "Tests: ByteSliceToString"
Cohesion: 0.23
Nodes (11): SliceHeader, StringHeader, ByteSliceToString(), Pointer, StringToByteSlice(), TestByteSliceToString(), TestBytesToString_WithUnusedBytes(), TestSliceHeadersCompatible() (+3 more)

### Community 299 - "ForestDB: size_t"
Cohesion: 0.18
Nodes (4): CompareBytesReversed(), Pointer, KVStore, size_t

### Community 300 - "Indexer: ClusterIndexMetadata"
Cohesion: 0.21
Nodes (7): BackupResponse, ClusterIndexMetadata, LocalIndexMetadata, getFilters(), getRestoreRemapParam(), IndexTopology, CreateRestoreContext()

### Community 301 - "Manager: CoordinatorState"
Cohesion: 0.22
Nodes (6): CoordinatorState, Mutex, PeerStatus, RequestHandle, NewCoordinator(), newCoordinatorState()

### Community 302 - "NatSort: natsort/sort.go"
Cohesion: 0.26
Nodes (9): stringSlice, atoi(), commonPrefix(), isDigit(), leadDigits(), Less(), LessRunes(), Strings() (+1 more)

### Community 304 - "Common: LogStatsFileHandler"
Cohesion: 0.18
Nodes (3): LogStatsFileHandler, SyncWriteCloser, NewLogStatsFileHandler()

### Community 305 - "Skills: GSI Skills (Claude Skills for GSI Engineering"
Cohesion: 0.24
Nodes (13): cbcollect-investigation Skill, ci-investigation Skill, GSI Skills (Claude Skills for GSI Engineering), In-Repo Deep-Dive Docs (secondary/docs/llmdocs), Shared GSI Skills Config (paths.env), run-couchbase-server Skill, GSI Smoke Test Driver (smoke.sh), apache_server.dockerfile (+5 more)

### Community 307 - "Tools: upr/upr.go"
Cohesion: 0.29
Nodes (12): argParse(), DcpFeed, FailoverLog, listOfVbnos(), main(), mf(), printFlogs(), receive() (+4 more)

### Community 308 - "Security: IsToolsConfigUsed"
Cohesion: 0.24
Nodes (8): PeerPipe, ShouldUseClientCertAuth(), GetToolsCreds(), GetToolsTLSConf(), Config, IsToolsConfigUsed(), setupClientTLSConfigTools(), ToolsConfig

### Community 309 - "Tools: Run"
Cohesion: 0.29
Nodes (10): Cluster, Config, generateJson(), generateVectors(), getAddrWithMemcachedPort(), openGocbCollection(), randFromAlphabet(), randString() (+2 more)

### Community 313 - "Indexer: handleIndexStatusRequest"
Cohesion: 0.44
Nodes (4): getETagFromHttpHeader(), sendNotModified(), sendWithETag(), validateRequest()

### Community 315 - "Manager: NewError"
Cohesion: 0.50
Nodes (10): errCategory, errCode, Error, errSeverity, category(), NewError(), NewError2(), NewError3() (+2 more)

### Community 316 - "Queryport: newBackfillCryptReader"
Cohesion: 0.21
Nodes (8): backfillCryptReader, backfillCryptWriter, File, newBackfillCryptReader(), newBackfillCryptWriter(), File, newBackfillCryptReader(), newBackfillCryptWriter()

### Community 317 - "Tools: SiftData"
Cohesion: 0.24
Nodes (8): SiftData, getSiftData(), Config, Decoder, File, Mutex, OpenSiftData(), OpenSiftQueryAndGroundTruth()

### Community 318 - "Indexer: Ctx"
Cohesion: 0.23
Nodes (5): Ctx, AggregateRecorderWithCtx, getUserCtx(), Duration, AggregateRecorderWithCtx

### Community 319 - "Docs: Secondary Index Design Overview"
Cohesion: 0.18
Nodes (12): 2i Configuration Parameters, Deployment Options, Indexer Design (stub), Consistency In Indexes (John Liang), Distributed Indexing Design Proposal (Steve Yen), Distributed Range Partitioned Indexes (Yen, Sallings), Durable Transactions (John Liang), Indexer (Local Indexer) (+4 more)

### Community 320 - "Indexer: metering_enterprise.go"
Cohesion: 0.29
Nodes (4): getNoUserCtx(), AggregateRecorder, Units, UnitType

### Community 321 - "MemDB: Node"
Cohesion: 0.24
Nodes (3): Pointer, Node, NodeRef

### Community 322 - "Common: Counter"
Cohesion: 0.24
Nodes (3): Counter, MarshallCounter(), UnmarshallCounter()

### Community 323 - "Indexer: Error"
Cohesion: 0.25
Nodes (6): IndexerErrCode, IndexerError, errCategory, errCode, Error, errSeverity

### Community 324 - "ForestDB: Dummy"
Cohesion: 0.18
Nodes (5): Dummy, Logger, DefaultConfig(), Destroy(), Config

### Community 325 - "Manager: eventManager"
Cohesion: 0.27
Nodes (6): eventManager, EventType, notifier, Mutex, notifier, newEventManager()

### Community 326 - "MemDB: Config"
Cohesion: 0.24
Nodes (3): Config, FileType, GetKeyByIdCb

### Community 328 - "Protobuf: KeyPartition"
Cohesion: 0.31
Nodes (3): KeyPartition, IndexInst, NewKeyPartition()

### Community 329 - "Protobuf: SinglePartition"
Cohesion: 0.25
Nodes (3): SinglePartition, IndexInst, NewSinglePartition()

### Community 330 - "Protobuf: TestPartition"
Cohesion: 0.31
Nodes (3): TestPartition, IndexInst, NewTestParitition()

### Community 331 - "DCP: upr_feed/feed.go"
Cohesion: 0.29
Nodes (9): addKVset(), argParse(), events(), getBucket(), Bucket, DcpFeed, handleEvent(), main() (+1 more)

### Community 332 - "Docs: Secondary Index Terminology"
Cohesion: 0.27
Nodes (11): CollateJSON Binary Comparison, Rollback Context, KV-Index System Diagram, Back Index, Backfill Queue, Catchup Queue, Failover Timestamp, Forward Index (+3 more)

### Community 333 - "Agents.md: Indexer Microservice"
Cohesion: 0.22
Nodes (10): Indexer Microservice, Metadata Provider (adminport), Planner, Scan Client (scanport), codec.Decode, codec.Encode, CollateJSON Library, CollateJSON Known Issues / TODO (+2 more)

### Community 334 - "CollateJSON: Codec"
Cohesion: 0.20
Nodes (4): AlternateHandling, Codec, Level(), Tag

### Community 335 - "Stubs: bhive_community.go"
Cohesion: 0.20
Nodes (3): StubType, Request, ResponseWriter

### Community 336 - "Indexer: RebalancePhase"
Cohesion: 0.24
Nodes (3): RebalancePhase, RebalancePhaseRequest, MsgUpdateRebalancePhase

### Community 337 - "Common: NewTsVbuuid"
Cohesion: 0.29
Nodes (8): NewTsVbuuid(), BenchmarkCompareVbuuuids(), B, TsVbuuid, TestAsRecent(), TestCompareVbuuids(), TestSortTimestamp(), verifyTimestamp()

### Community 338 - "DCP: mcops"
Cohesion: 0.31
Nodes (5): mcops, addToMap(), MCRequest, init(), newMCops()

### Community 339 - "Indexer: pause_objutil.go"
Cohesion: 0.24
Nodes (8): ArchiveEnum, PauseObjutil, ReadAtSeeker, ReaderAt, ArchiveInfoFromRemotePath(), Reader, NewPauseObjutil(), Seeker

### Community 340 - "Indexer: index_reader.go"
Cohesion: 0.31
Nodes (8): Counter, Exister, Inclusion, IndexReader, Looker, RangeCounter, Ranger, Snapshot

### Community 341 - "Indexer: cpu.go"
Cohesion: 0.31
Nodes (8): cpuCollector, getCpuPercent(), getRSS(), StartCpuCollector(), updateCpuPercent(), updateMemFree(), updateMemTotal(), updateRSS()

### Community 342 - "Indexer: monitorItemsCount"
Cohesion: 0.29
Nodes (3): IndexInfo, TimestampedCounts, IndexInfo

### Community 343 - "Indexer: MsgError"
Cohesion: 0.24
Nodes (3): MsgError, MsgStreamError, Error

### Community 345 - "Queryport: statistics"
Cohesion: 0.29
Nodes (5): statistics, IndexStatistics, Values, newStatistics(), skey2Values()

### Community 346 - "Projector: bucketDcp"
Cohesion: 0.22
Nodes (4): bucketDcp, Bucket, DcpFeed, TsVbuuid

### Community 347 - "Stubs: AggregateRecorder"
Cohesion: 0.27
Nodes (3): AggregateRecorder, Units, UnitType

### Community 348 - "Docs: Indexer1"
Cohesion: 0.42
Nodes (10): Delete Workflow Sequence Diagram, High Watermark Timestamp, Index Coordinator, Indexer1, Indexer2, Mutation Queue, Projector, Router (+2 more)

### Community 349 - "Docs: InitialBuild_Load Sequence Diagram"
Cohesion: 0.38
Nodes (10): InitialBuild_Load Sequence Diagram, Index Coordinator (master), Indexer1, Indexer2, Initial Build Queue, Initial Index Build and Load Flow, KV Store, Projector1 (+2 more)

### Community 350 - "Docs: Insert Workflow Sequence Diagram"
Cohesion: 0.47
Nodes (10): Insert Workflow Sequence Diagram, High Watermark Timestamp, Index Coordinator, Indexer1, Indexer2, Mutation Queue, Projector, Router (+2 more)

### Community 351 - "Docs: Index Coordinator"
Cohesion: 0.24
Nodes (10): IndexManager Design, IndexDefinition, Index Coordinator, Index Coordinator Replica, StateContext, Initial Build Queue, Initial Index Build Flow (CREATE INDEX), John's Initial Index Build Document (+2 more)

### Community 354 - "Common: NewSessionPermissionsCache"
Cohesion: 0.47
Nodes (3): sessionPermissionsCache, Creds, NewSessionPermissionsCache()

### Community 355 - "Protobuf: projector/common.go"
Cohesion: 0.22
Nodes (6): TsVbFull, Snapshot, NewSnapshot(), NewTsVb(), NewTsVbFull(), TsVb

### Community 356 - "Indexer: Console"
Cohesion: 0.25
Nodes (5): Console(), Duration, Config, IndexStorageStats, Time

### Community 357 - "Indexer: SnapshotWaitersMapHolder"
Cohesion: 0.39
Nodes (4): SnapshotWaitersMap, SnapshotWaitersMapHolder, copySnapshotWaiterMap(), Pointer

### Community 358 - "Queryport: secondaryIndex3"
Cohesion: 0.25
Nodes (6): secondaryIndex3, AggregateType, GroupAggr, IndexGroupAggregates, n1qlaggrtypetogsi(), n1qlgroupaggrtogsi()

### Community 359 - "Common: memstat.go"
Cohesion: 0.44
Nodes (8): LogStats, MemStats, MemstatLogger(), MemstatLogger2(), newPauseNs(), PrintMemstats(), PrintMemstats2(), reprList()

### Community 360 - "Docs: Projector"
Cohesion: 0.31
Nodes (9): System Bootstrap Sequence, ns_server (Cluster Manager), IndexManager, Backfill Stream, Catchup Stream, KeyVersions Message, Maintenance Stream, Projector Design (+1 more)

### Community 361 - "Docs: Stability Timestamp"
Cohesion: 0.28
Nodes (9): HWHeartbeat Message, Index Rebalance, Scan Consistency Options, Query Execution Flow, Scan Coordinator, Index-Query System Diagram, Persistent Timestamp, Scan Timestamp (+1 more)

### Community 362 - "Docs: Router"
Cohesion: 0.28
Nodes (9): IndexTopology, TopologyProvider Interface, Partition/Replica/Shard/Slice Model, System Invariants, DropData Drop Indicator, Router, Smart Throttling, Transporter (+1 more)

### Community 363 - "Docs: Mutation Execution Flow"
Cohesion: 0.22
Nodes (9): John's Execution Flow Document, Mutation Execution Flow, SYNC Message, Scan Stability Options, High-Watermark Timestamp, Mutation Queue, Stable Scan, Tearing Read (+1 more)

### Community 364 - "CollateJSON: extractEncodedField"
Cohesion: 0.43
Nodes (4): flipBits(), getEncodedDatum(), getEncodedString(), Codec

### Community 368 - "MemDB: NodeList"
Cohesion: 0.43
Nodes (3): NodeList, Node, NewNodeList()

### Community 370 - "DCP: hello.go"
Cohesion: 0.54
Nodes (7): doMoreOps(), doOps(), exploreBucket(), explorePool(), Bucket, main(), maybeFatal()

### Community 371 - "DCP: main"
Cohesion: 0.43
Nodes (7): addKVset(), argParse(), getTestConnection(), Bucket, DcpFeed, main(), receiveMutations()

### Community 372 - "Docs: Bootstrap Sequence Diagram"
Cohesion: 0.71
Nodes (8): Bootstrap Sequence Diagram, Index Coordinator (master), Index Coordinator (replica), Indexer, ns_server, Projector, Router, StateContext

### Community 373 - "Docs: InitialBuild Prepare Sequence Diagram"
Cohesion: 0.57
Nodes (8): InitialBuild Prepare Sequence Diagram, CREATE INDEX Request (prepare phase, INIT to READY), Index Coordinator (master), Index Coordinator (replica1), Index Coordinator (replica2), Indexer1, Indexer2, StateContext (CAS-versioned index state)

### Community 374 - "Docs: Indexer1 (Scan Coordinator"
Cohesion: 0.43
Nodes (8): Scan Workflow Sequence Diagram, Index Client, Index Coordinator, Indexer1 (Scan Coordinator), Indexer2, Local Index Scan over Slices, Scan Timestamp from Consistency/Stability Options, Scan Scatter-Gather Workflow

### Community 375 - "Indexer: pause_copier_enterprise.go"
Cohesion: 0.54
Nodes (7): generatePlasmaCopierConfig(), generatePlasmaCopierConfigForPauseResume(), Config, MakeFileCopier(), MakeFileCopierForPauseResume(), setCopierTestConfigIfEnabled(), unsetCopierTestConfigIfEnabled()

### Community 378 - "Tests: setup.sh"
Cohesion: 0.25
Nodes (7): CBAUTH_REVRPC_URL, CGO_CFLAGS, CGO_LDFLAGS, GO111MODULE, GOPATH, setup.sh script, WORKSPACE

### Community 379 - "Tools: dumpconfig.go"
Cohesion: 0.46
Nodes (7): argParse(), main(), paramList(), populateMap(), sortMap(), sortParams(), usage()

### Community 380 - "Common: pause_resume_defs.go"
Cohesion: 0.33
Nodes (4): PauseToken, PauseTokenType, PauseUploadState, ResumeDownloadState

### Community 383 - "Indexer: computeShardProgress"
Cohesion: 0.38
Nodes (3): getDestNode(), getProgressForToken(), IndexStatusResponse

### Community 384 - "Build.sh: build.sh"
Cohesion: 0.52
Nodes (6): build_indexer(), build_projector(), build_protobuf(), clean_indexer(), clean_projector(), build.sh script

### Community 385 - "DCP: mc_res.go"
Cohesion: 0.43
Nodes (6): errStatus(), IsFatal(), IsNotFound(), IsUnknownScopeOrCollection(), TestIsFatal(), TestIsNotFound()

### Community 386 - "DCP: dcp/util.go"
Cohesion: 0.33
Nodes (5): CleanupHost(), FindCommonSuffix(), TestCleanupHost(), TestFindCommonSuffix(), TestParseURL()

### Community 387 - "Docs: Deferred Index Build (defer_build"
Cohesion: 0.29
Nodes (7): Deferred Index Build (defer_build), Secondary Index Deployment Options, Deployment Plan Design Doc (Google Doc), Deployment Plan nodes Clause, Initial Build Load Phase, Initial Build Timestamp, 2i Jan Code Drop Features

### Community 388 - "Tests: runtest_clusterrun.sh"
Cohesion: 0.29
Nodes (6): CBAUTH_REVRPC_URL, NS_SERVER_CBAUTH_PWD, NS_SERVER_CBAUTH_RPC_URL, NS_SERVER_CBAUTH_URL, NS_SERVER_CBAUTH_USER, runtest_clusterrun.sh script

### Community 389 - "Tools: n1qlperf.sh"
Cohesion: 0.29
Nodes (6): CBAUTH_REVRPC_URL, NS_SERVER_CBAUTH_PWD, NS_SERVER_CBAUTH_RPC_URL, NS_SERVER_CBAUTH_URL, NS_SERVER_CBAUTH_USER, n1qlperf.sh script

### Community 390 - "Tools: bufferedscan.sh"
Cohesion: 0.29
Nodes (6): CBAUTH_REVRPC_URL, NS_SERVER_CBAUTH_PWD, NS_SERVER_CBAUTH_RPC_URL, NS_SERVER_CBAUTH_URL, NS_SERVER_CBAUTH_USER, bufferedscan.sh script

### Community 391 - "Agents.md: AI Agent Contribution Guidelines"
Cohesion: 0.33
Nodes (6): AI Agent Contribution Guidelines, ns_server / cbauth Cluster Manager, Rebalance Service Manager, Claude Contribution Guidelines (duplicate of agents.md), golangci-lint Configuration, SystemConfig Feature Flags

### Community 392 - "Queryport: defs.go"
Cohesion: 0.40
Nodes (5): IndexResponse, RequestType, IndexIdList, IndexRequest, IndexIdList

### Community 393 - "Common: CollectionScope"
Cohesion: 0.53
Nodes (5): Collection, CollectionScope, index, limit, insertVectorAsXATTR()

### Community 394 - "Common: ConfigHolder"
Cohesion: 0.47
Nodes (4): ConfigHolder, Config, Pointer, createStagingDir()

### Community 395 - "Common: ear_rest.go"
Cohesion: 0.47
Nodes (5): earBucketInfo, earSettings, earTypeSettings, earFetch(), IsEncryptionAtRestEnabled()

### Community 396 - "Common: NewUUID"
Cohesion: 0.53
Nodes (3): UUID, NewUUID(), SeedProcess()

### Community 402 - "Docs: DCP Protocol"
Cohesion: 0.33
Nodes (6): Timestamp struct (common/timestamp.go), DCP Client Requirements, DCP Protocol, ForestDB Storage Backend, Couchbase Secondary Indexes Project Overview, projector/upr.go UPR Interface

### Community 403 - "Tools: DefaultKVStoreConfig"
Cohesion: 0.67
Nodes (5): DefaultKVStoreConfig(), do_test1(), do_test2(), do_test3(), main()

### Community 404 - "Indexer: pause_copier_community.go"
Cohesion: 0.53
Nodes (5): Config, MakeFileCopier(), MakeFileCopierForPauseResume(), setCopierTestConfigIfEnabled(), unsetCopierTestConfigIfEnabled()

### Community 408 - "Testdata: Test Data Provenance Note"
Cohesion: 0.70
Nodes (5): Test Data Provenance Note, TwitterFeed1 Test Data Corpus, Users10k Test Data Corpus, Users_mut Test Data Corpus, Test Framework Constants (testdata S3/GDrive URLs)

### Community 409 - "Indexer: validateAuth"
Cohesion: 0.60
Nodes (3): Request, ResponseWriter, validateAuth()

### Community 413 - "Common: parseTag"
Cohesion: 0.50
Nodes (3): tagOptions, parseTag(), TestTagParsing()

### Community 416 - "DCP: main"
Cohesion: 0.60
Nodes (4): argParse(), getTestConnection(), Bucket, main()

### Community 417 - "Docs: Deployment Diagram (Secondary Index Design"
Cohesion: 0.80
Nodes (5): Deployment Diagram (Secondary Index Design), Client (laptop icon), Index Cluster, KV Cluster, Query Cluster

### Community 418 - "Goextended: OnceOnSuccess"
Cohesion: 0.40
Nodes (3): Mutex, OnceOnSuccess, Uint32

### Community 419 - "MemDB: system_windows.go"
Cohesion: 0.50
Nodes (4): Dir_Sync(), GetDefaultNumFD(), GetIOConcurrency(), FileMode

### Community 420 - "Platform: thp_linux.go"
Cohesion: 0.70
Nodes (4): EnsureTHPDisabled(), getKernelVersion(), int8ToStr(), parseKernelVersion()

### Community 422 - "Tools: fdb_throughput.c"
Cohesion: 0.70
Nodes (4): do_test1(), do_test2(), do_test3(), main()

### Community 423 - "Agents.md: Indexer Dataport"
Cohesion: 0.50
Nodes (4): Indexer Dataport, Flusher Worker Pool, Atomic Mutation Queue, Timekeeper State Machine

### Community 424 - "Audit: audit.go"
Cohesion: 0.50
Nodes (3): AuditEvent, CommonAuditFields, InitAuditService()

### Community 429 - "Queryport: GetMarshalledInternalVersion"
Cohesion: 0.50
Nodes (3): GetMarshalledInternalVersion(), Request, ResponseWriter

### Community 430 - "DCP: main"
Cohesion: 0.83
Nodes (3): main(), maybeFatal(), pathToID()

### Community 431 - "Docs: Projector Memory Estimation Formula"
Cohesion: 0.67
Nodes (4): Projector Memory Estimation Formula, Projector Sizing Guide, Projector Performance Observations, Projector Stress Test Configurations

### Community 432 - "Tests: dobuild"
Cohesion: 0.83
Nodes (3): dobuild script, error_exit(), note_version()

### Community 433 - "Tests: standalone-runner.sh"
Cohesion: 0.50
Nodes (3): PATH, standalone-runner.sh script, TS

### Community 435 - "License.txt: Per-file Licensing Notice"
Cohesion: 1.00
Nodes (3): Per-file Licensing Notice, Apache License 2.0, Couchbase Business Source License 1.1

### Community 445 - "Tests: Plasma SMAT Fuzz Testing Instructions"
Cohesion: 0.67
Nodes (3): go-fuzz, smat (State Machine Assisted Fuzz Testing), Plasma SMAT Fuzz Testing Instructions

## Knowledge Gaps
- **217 isolated node(s):** `github.com/couchbase/indexing`, `Request`, `Server`, `Client`, `Codec` (+212 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **61 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `T` connect `Test Utilities & DCP Helpers` to `Functional Test Scan Framework`, `Storage Reader & Metering`, `Rebalance Test Setup`, `Metadata Repo & Request Handler`, `Test MetaKV & Token Utils`, `Common Utilities & Versioning`, `Shard Mapping Test Helpers`, `KV Test Data Loading`, `Common JSON & Encoding`, `Slice Deletion & Array Index`, `Metadata Encryption Keys`, `CollateJSON Codec`, `Backup-Restore Tests`, `Tests: set03_planner_test.go`, `Common: NewDecoder`, `Common: json/encode.go`, `Protobuf: DcpEvent`, `Tests: cluster_setup.go`, `Indexer: indexer.go`, `Tests: set13_groups_aggrs_test.go`, `Tests: functionaltests/common_test.go`, `Common: secondary/common/util.go`, `Pipeline: ItemWriter`, `Tests: Context`, `Common: TransferToken`, `Common: NewEaRKeyCache`, `Indexer: NewAtomicMutationQueue`, `Scanreport: stubContext`, `Dataport: NewKeyVersions`, `Common: scanner`, `Planner: AlternateShardId`, `MemDB: New`, `CollateJSON: NewCodec`, `Indexer: EncodeAndWrite`, `MemDB: memdb_test.go`, `MemDB: Equal`, `Vector: MetricType`, `Queryport: secondary_index.go`, `Common: decodeState`, `Protobuf: N1QLTransform`, `MemDB: New`, `Tests: set04_restful_test.go`, `Planner: shard_dealer_test.go`, `Vector: convertTo1D`, `DCP: CommandCode`, `DCP: TapFeed`, `Tests: GetIndexerNodesHttpAddresses`, `Dataport: TransportFlag`, `Common: common/util_test.go`, `Common: VbmapResponse`, `DCP: mc_test.go`, `Common: scanner_test.go`, `DCP: server/server_test.go`, `Indexer: RowHeap`, `Adminport: admin_test.go`, `Common: Bucket`, `Indexer: indexer/stats_manager.go`, `Queryport: client/conn_pool_test.go`, `Dataport: NewServer`, `Common: NewOperation`, `Indexer: ForestDBIterator`, `Scanreport: aggregate_test.go`, `Indexer: Slice`, `Indexer: TestVectorPipelineMergeOperator`, `DCP: newConnectionPool`, `Common: NewStatistics`, `Vector: IndexFactory`, `DCP: mc.go`, `Common: tagkey_test.go`, `DCP: pools_test.go`, `Common: VectorQuantizer`, `Indexer: NewScanAdmissionController`, `DCP: transport/tap.go`, `Tests: ByteSliceToString`, `Common: NewTsVbuuid`, `DCP: mc_res.go`, `DCP: dcp/util.go`, `Common: parseTag`?**
  _High betweenness centrality (0.150) - this node is a cross-community bridge._
- **Why does `IndexInstId` connect `Index DDL & Replica Ops` to `Metadata Provider & Index Status`, `Indexer Event Loop Core`, `Indexer: setTransferTokenInMetakv`, `Indexer: getIndexStatus`, `DDL Command Tokens & Scheduling`, `Plasma Storage Slice`, `DDL Prepare & Storage Mode`, `BHive Vector Storage Slice`, `Cluster Manager Agent`, `Indexer Message Types`, `Planner Placement & Costing`, `Storage Reader & Metering`, `Rebalance Cleanup & Context`, `Indexer: IndexStorageStats`, `Metadata Repo & Request Handler`, `Shard Rebalancer`, `Indexer: indexer/plasma_community.go`, `Planner: ShardDealer`, `Tests: set04_restful_test.go`, `Indexer: compactionDaemon`, `MemDB Storage Slice`, `Slice Deletion & Array Index`, `Index Snapshot Maps`, `Query Client Metadata Cache`, `ForestDB Slice & Compaction`, `Tests: GetIndexerNodesHttpAddresses`, `Planner Slot Placement`, `Snapshot Lifecycle Workers`, `Index Instance Maps & Compaction`, `Indexer: NewPlasmaSlice`, `Indexer: MsgStreamUpdate`, `Indexer: Rebalancer`, `Queryport: GsiClient`, `Common: IndexDefn`, `Indexer: SliceId`, `Indexer: StatsMap`, `Indexer: mockSlice`, `Indexer: MsgUpdateInstMap`, `Manager: Statistics`, `Indexer: mutationMgr`, `Protobuf: DcpEvent`, `Planner: IndexUsage`, `Indexer: indexer.go`, `Tests: set13_groups_aggrs_test.go`, `Indexer: IndexStats`, `Indexer: ScanRequest`, `Queryport: secondaryIndex6`, `Indexer: RebalancePhase`, `Indexer: statsManager`, `Indexer: mockSlice`, `Indexer: IndexInst`, `Indexer: IndexerStats`, `Common: TransferToken`, `Indexer: Resumer`, `Indexer: NewAtomicMutationQueue`, `Indexer: fdbSnapshot`, `Indexer: MsgDropIndex`, `Manager: RestoreContext`, `Logging: system_event.go`, `Indexer: rebalance_service_manager.go`, `Indexer: StorageStatistics`, `Indexer: computeShardProgress`?**
  _High betweenness centrality (0.077) - this node is a cross-community bridge._
- **Why does `Unmarshal()` connect `Common JSON & Encoding` to `Metadata Provider & Index Status`, `Indexer Event Loop Core`, `Index DDL & Replica Ops`, `Functional Test Scan Framework`, `Test Utilities & DCP Helpers`, `DDL Command Tokens & Scheduling`, `Plasma Storage Slice`, `DDL Prepare & Storage Mode`, `BHive Vector Storage Slice`, `Rebalance Cleanup & Context`, `Rebalance Test Setup`, `Metadata Repo & Request Handler`, `Shard Rebalancer`, `Test MetaKV & Token Utils`, `Common Utilities & Versioning`, `Shard Mapping Test Helpers`, `KV Test Data Loading`, `Query Client Metadata Cache`, `Metadata Encryption Keys`, `ForestDB Slice & Compaction`, `Pause-Resume Lifecycle`, `Queryport Server Handlers`, `CollateJSON Codec`, `Scan Aggregation Results`, `Backup-Restore Tests`, `DCP Collections Manifest`, `Indexer: Rebalancer`, `Tests: set03_planner_test.go`, `Queryport: GsiClient`, `Common: IndexDefn`, `Common: NewDecoder`, `MemDB: MemDB`, `Manager: Statistics`, `Common: dcp_seqno.go`, `Common: settingsManager`, `Tests: cluster_setup.go`, `Projector: Projector`, `Tools: HandleCommand`, `Indexer: requestHandlerCache`, `IOWrap: io_wrappers.go`, `Indexer: statsManager`, `Protobuf: MutationTopicRequest`, `Common: TransferToken`, `Queryport: ScanResultEntries`, `Security: tls.go`, `Indexer: Pauser`, `Indexer: Resumer`, `Protobuf: Instance`, `Dataport: NewKeyVersions`, `Common: scanner`, `Tools: perfContext`, `Indexer: taskObj`, `Indexer: rebalance_service_manager.go`, `CLI: main`, `Common: InternalVersion`, `CollateJSON: NewCodec`, `Indexer: EncodeAndWrite`, `Vector: MetricType`, `DCP: Bucket`, `Indexer: setTransferTokenInMetakv`, `Tests: n1qlclient.go`, `Common: decodeState`, `Protobuf: N1QLTransform`, `Tests: set04_restful_test.go`, `Indexer: requestHandlerContext`, `Vector: convertTo1D`, `Indexer: testServer`, `Indexer: isAllowed`, `Planner: simulator`, `CollateJSON: Int`, `Tests: GetIndexerNodesHttpAddresses`, `Queryport: cbqClient`, `Protobuf: AddInstancesRequest`, `Common: VbmapResponse`, `Queryport: Error`, `Common: scanner_test.go`, `Adminport: admin_test.go`, `Common: Bucket`, `Indexer: FlatFileStatsPersister`, `Indexer: AutofailoverServiceManager`, `Indexer: StorageStatistics`, `Common: NewStatistics`, `Common: tagkey_test.go`, `DCP: pools_test.go`, `Tools: parseStorageStats.go`, `Indexer: ClusterIndexMetadata`, `Common: Counter`, `Indexer: monitorItemsCount`, `Indexer: testPauseOrResume`, `Protobuf: FailoverLogResponse`, `Indexer: computeShardProgress`, `Protobuf: VbmapRequest`, `Protobuf: FailoverLogRequest`?**
  _High betweenness centrality (0.076) - this node is a cross-community bridge._
- **Are the 316 inferred relationships involving `FailTestIfError()` (e.g. with `TestCreate2Drop1Scan2()` and `TestCreateDropCreate()`) actually correct?**
  _`FailTestIfError()` has 316 INFERRED edges - model-reasoned connections that need verification._
- **What connects `github.com/couchbase/indexing`, `Request`, `Server` to the rest of the system?**
  _217 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Metadata Provider & Index Status` be split into smaller, more focused modules?**
  _Cohesion score 0.02274698795180723 - nodes in this community are weakly interconnected._
- **Should `Indexer Event Loop Core` be split into smaller, more focused modules?**
  _Cohesion score 0.03276959447172213 - nodes in this community are weakly interconnected._