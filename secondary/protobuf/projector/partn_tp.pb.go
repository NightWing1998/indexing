// Code generated by protoc-gen-go.
// source: partn_tp.proto
// DO NOT EDIT!

package protobuf

import proto "github.com/golang/protobuf/proto"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = math.Inf

// Example TestPartition, can be used during development.
type TestPartition struct {
	Endpoints        []string `protobuf:"bytes,1,rep,name=endpoints" json:"endpoints,omitempty"`
	CoordEndpoint    *string  `protobuf:"bytes,2,opt,name=coordEndpoint" json:"coordEndpoint,omitempty"`
	XXX_unrecognized []byte   `json:"-"`
}

func (m *TestPartition) Reset()         { *m = TestPartition{} }
func (m *TestPartition) String() string { return proto.CompactTextString(m) }
func (*TestPartition) ProtoMessage()    {}

func (m *TestPartition) GetEndpoints() []string {
	if m != nil {
		return m.Endpoints
	}
	return nil
}

func (m *TestPartition) GetCoordEndpoint() string {
	if m != nil && m.CoordEndpoint != nil {
		return *m.CoordEndpoint
	}
	return ""
}

func init() {
}