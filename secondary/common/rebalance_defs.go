// @author Couchbase <info@couchbase.com>
// @copyright 2016-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package common

import (
	"fmt"
	"strings"
)

type TokenState byte

const (
	//
	// Transfer token states labeled with their flow sequence #s (or "x" for unused)
	// and categorized as Master (non-move bookkeeping), move Source, or move Dest.
	// Owners are defined by function getTransferTokenOwner. Processing and state changes
	// are mainly in functions processTokenAsMaster, processTokenAsSource,
	// processTokenAsDest, and tokenMergeOrReady.
	//
	TransferTokenCreated TokenState = iota // 1. Dest: Clone source index metadata into
	// local metadata; change state to TTAccepted. (Original design is first check
	// if enough resources and if not, change state to TTRefused and rerun planner,
	// but this is not implemented.)
	TransferTokenAccepted // 2. Master: Change state to TTInitiate.
	TransferTokenRefused  // x. Master (but really unused): No-op.
	TransferTokenInitiate // 3. Dest: Initiate index build if non-deferred and change state.
	// to TTInProgress; if build is deferred it may change state to TTTokenMerge instead.
	TransferTokenInProgress // 4. Dest: No-op in processTokenAsDest; processed in
	// tokenMergeOrReady. Build in progress or staged for start. May pass through state
	// TTMerge. Change state to TTReady when done.
	TransferTokenReady // 5. Source: Ready to delete source idx (dest idx now taking all
	// traffic). Queue source index for later async drop. Drop processing will change
	// state to TTCommit after the drop is complete.
	TransferTokenCommit // 6. Master: Source index is deleted. Change master's in-mem token
	// state to TTCommit amd metakv token state to TTDeleted.
	TransferTokenDeleted // 7. Master: All TT processing done. Delete the TT from metakv.
	TransferTokenError   // x. Unused; kept so down-level iotas match.
	TransferTokenMerge   // 4.5 Dest (partitioned indexes only): No-op in processTokenAsDest;
	// processed in tokenMergeOrReady. Tells indexer to merge dest partn's temp "proxy"
	// IndexDefn w/ "real" IndexDefn for that index. Change state to TTReady when done.
)

func (ts TokenState) String() string {

	switch ts {
	case TransferTokenCreated:
		return "TransferTokenCreated"
	case TransferTokenAccepted:
		return "TransferTokenAccepted"
	case TransferTokenRefused:
		return "TransferTokenRefused"
	case TransferTokenInitiate:
		return "TransferTokenInitiate"
	case TransferTokenInProgress:
		return "TransferTokenInProgress"
	case TransferTokenReady:
		return "TransferTokenReady"
	case TransferTokenCommit:
		return "TransferTokenCommit"
	case TransferTokenDeleted:
		return "TransferTokenDeleted"
	case TransferTokenError:
		return "TransferTokenError"
	case TransferTokenMerge:
		return "TransferTokenMerge"
	}

	return "unknown"

}

// TokenBuildSource is the type of the TransferToken.BuildSource field, which is currently unused
// but will be used in future when rebalance is done using file copy rather than DCP.
type TokenBuildSource byte

const (
	TokenBuildSourceDcp TokenBuildSource = iota
	TokenBuildSourcePeer
)

func (bs TokenBuildSource) String() string {

	switch bs {
	case TokenBuildSourceDcp:
		return "Dcp"
	case TokenBuildSourcePeer:
		return "Peer"
	}
	return "unknown"
}

type TokenTransferMode byte

const (
	TokenTransferModeMove TokenTransferMode = iota // moving idx from source to dest
	TokenTransferModeCopy                          // no source node; idx created on dest during rebalance (replica repair)
)

func (tm TokenTransferMode) String() string {

	switch tm {
	case TokenTransferModeMove:
		return "Move"
	case TokenTransferModeCopy:
		return "Copy"
	}
	return "unknown"
}

// TransferToken represents a sindgle index partition movement for rebalance or move index.
// These get stored in metakv, which makes callbacks on creation and each change.
type TransferToken struct {
	MasterId     string // rebal master nodeUUID (32-digit random hex)
	SourceId     string // index source nodeUUID
	DestId       string // index dest   nodeUUID
	RebalId      string
	State        TokenState  // current TT state; usually tells the NEXT thing to be done
	InstId       IndexInstId // true instance ID for non-partitioned; may be proxy ID for partitioned
	RealInstId   IndexInstId // 0 for non-partitioned or non-proxy partitioned, else true instId to merge partn to
	IndexInst    IndexInst
	Error        string            // English error text; empty if no error
	BuildSource  TokenBuildSource  // unused
	TransferMode TokenTransferMode // move (rebalance) vs copy (replica repair)

	IsEmptyNodeBatch bool //indicates the token is part of batch for empty node

	//used for logging
	SourceHost string
	DestHost   string
}

// TransferToken.Clone returns a copy of the transfer token it is called on. Since the type is
// only one layer deep, this can be done by returning the value receiver as Go already copied it.
func (tt TransferToken) Clone() TransferToken {
	return tt
}

// TransferToken.IsUserDeferred returns whether the transfer token represents an index
// that was created by the user as deferred (with {"defer_build":true}) and not yet built.
// Rebalance will move the index metadata from source to dest but will NOT build these.
// All deferred indexes have flag setting
//   tt.IndexInst.Defn.Deferred == true
// User-deferred indexes additionally have state value
//   tt.IndexInst.State == INDEX_STATE_READY
// whereas system-deferred indexes have a different State (usually INDEX_STATE_ACTIVE).
func (tt *TransferToken) IsUserDeferred() bool {
	return tt.IndexInst.Defn.Deferred && tt.IndexInst.State == INDEX_STATE_READY
}

// TransferToken.String returns a human-friendly string representation of a TT.
func (tt *TransferToken) String() string {
	var sb strings.Builder
	sbp := &sb

	fmt.Fprintf(sbp, " MasterId: %v ", tt.MasterId)
	fmt.Fprintf(sbp, "SourceId: %v ", tt.SourceId)
	if len(tt.SourceHost) != 0 {
		fmt.Fprintf(sbp, "(%v) ", tt.SourceHost)
	}
	fmt.Fprintf(sbp, "DestId: %v ", tt.DestId)
	if len(tt.DestHost) != 0 {
		fmt.Fprintf(sbp, "(%v) ", tt.DestHost)
	}
	fmt.Fprintf(sbp, "RebalId: %v ", tt.RebalId)
	fmt.Fprintf(sbp, "State: %v ", tt.State)
	fmt.Fprintf(sbp, "BuildSource: %v ", tt.BuildSource)
	fmt.Fprintf(sbp, "TransferMode: %v ", tt.TransferMode)
	if tt.Error != "" {
		fmt.Fprintf(sbp, "Error: %v ", tt.Error)
	}
	fmt.Fprintf(sbp, "InstId: %v ", tt.InstId)
	fmt.Fprintf(sbp, "RealInstId: %v ", tt.RealInstId)
	fmt.Fprintf(sbp, "Partitions: %v ", tt.IndexInst.Defn.Partitions)
	fmt.Fprintf(sbp, "Versions: %v ", tt.IndexInst.Defn.Versions)
	fmt.Fprintf(sbp, "Inst: %v\n", tt.IndexInst)

	return sb.String()
}
