// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package vector

import (
	faiss "github.com/couchbase/indexing/secondary/vector/faiss"
)

type MetricType int

const (
	METRIC_L2 MetricType = iota
	METRIC_INNER_PRODUCT
)

func (m MetricType) String() string {

	switch m {

	case METRIC_L2:
		return "L2"
	case METRIC_INNER_PRODUCT:
		return "INNER_PRODUCT"
	}

	return ""
}

type Codebook interface {

	//Train the codebook using input vectors.
	Train(vecs [][]float32) error

	//IsTrained returns true if codebook has been trained.
	IsTrained() bool

	//Compute the quantized code for a given input vector.
	//Must be run on a trained codebook.
	EncodeVector(vec []float32) ([]uint8, error)

	//Compute the quantized codes for a given list of input vectors.
	//Must be run on a trained codebook.
	EncodeVectors(vecs [][]float32) ([][]uint8, error)

	//Find the nearest k centroidIDs for a given vector.
	//Must be run on a trained codebook.
	FindNearestCentroids(vec []float32, k int64) ([]int64, error)

	//Computes the distance table for given vector.
	//Distance table contains the precomputed distance of the given
	//vector from each subvector m(determined by the number of subquantizers).
	//Distance table is a matrix of dimension M * ksub where
	//M = number of subquantizers
	//ksub = number of centroids for each subquantizer (2**nbits)
	ComputeDistanceTable(vec []float32) ([][]float32, error)

	//Compute the distance between a vector(using distance table) and
	//quantized code of another vector.
	ComputeDistance(code []uint8, dtable [][]float32) float32
}

func SerializeCodebook(Codebook) ([]byte, error) {
	return nil, nil
}

func DeserializeCodebook([]byte) (Codebook, error) {
	return nil, nil
}

func convertToFaissMetric(metric MetricType) int {

	switch metric {
	case METRIC_L2:
		return faiss.MetricL2
	case METRIC_INNER_PRODUCT:
		return faiss.MetricInnerProduct

	}
	//default to L2
	return faiss.MetricL2
}
