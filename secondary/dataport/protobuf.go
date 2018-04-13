// Protobuf encoding scheme for payload

package dataport

import "errors"

import c "github.com/couchbase/indexing/secondary/common"
import protobuf "github.com/couchbase/indexing/secondary/protobuf/data"
import "github.com/golang/protobuf/proto"

// ErrorTransportVersion
var ErrorTransportVersion = errors.New("dataport.transportVersion")

// ErrorMissingPayload
var ErrorMissingPayload = errors.New("dataport.missingPlayload")

// protobufEncode encode payload message into protobuf array of bytes. Return
// `data` can be transported to the other end and decoded back to Payload
// message.
func protobufEncode(payload interface{}) (data []byte, err error) {
	pl := protobuf.Payload{
		Version: proto.Uint32(uint32(ProtobufVersion())),
	}

	switch val := payload.(type) {
	case []*c.VbKeyVersions:
		pl.Vbkeys = make([]*protobuf.VbKeyVersions, 0, len(val))
		for _, vb := range val { // for each VbKeyVersions
			pvb := &protobuf.VbKeyVersions{
				Bucketname: proto.String(vb.Bucket),
				Vbucket:    proto.Uint32(uint32(vb.Vbucket)),
				Vbuuid:     proto.Uint64(vb.Vbuuid),
				ProjVer:    protobuf.ProjectorVersion(int32(vb.ProjVer)).Enum(),
			}
			pvb.Kvs = make([]*protobuf.KeyVersions, 0, len(vb.Kvs))
			for _, kv := range vb.Kvs { // for each mutation
				pkv := &protobuf.KeyVersions{
					Seqno: proto.Uint64(kv.Seqno),
				}
				if kv.Docid != nil && len(kv.Docid) > 0 {
					pkv.Docid = kv.Docid
				}
				if len(kv.Uuids) == 0 {
					continue
				}
				l := len(kv.Uuids)
				pkv.Uuids = make([]uint64, 0, l)
				pkv.Commands = make([]uint32, 0, l)
				pkv.Keys = make([][]byte, 0, l)
				pkv.Oldkeys = make([][]byte, 0, l)
				for i, uuid := range kv.Uuids { // for each key-version
					pkv.Uuids = append(pkv.Uuids, uuid)
					pkv.Commands = append(pkv.Commands, uint32(kv.Commands[i]))
					pkv.Keys = append(pkv.Keys, kv.Keys[i])
					pkv.Oldkeys = append(pkv.Oldkeys, kv.Oldkeys[i])
				}
				pvb.Kvs = append(pvb.Kvs, pkv)
			}
			pl.Vbkeys = append(pl.Vbkeys, pvb)
		}

	case *c.VbConnectionMap:
		pl.Vbmap = &protobuf.VbConnectionMap{
			Bucket:   proto.String(val.Bucket),
			Vbuuids:  val.Vbuuids,
			Vbuckets: c.Vbno16to32(val.Vbuckets),
		}
	}

	if err == nil {
		data, err = proto.Marshal(&pl)
	}
	return
}

// protobufDecode complements protobufEncode() API. `data` returned by encode
// is converted back to *protobuf.VbConnectionMap, or []*protobuf.VbKeyVersions
// and returns back the value inside the payload
func protobufDecode(data []byte) (value interface{}, err error) {
	pl := &protobuf.Payload{}
	if err = proto.Unmarshal(data, pl); err != nil {
		return nil, err
	}
	currVer := ProtobufVersion()
	if ver := byte(pl.GetVersion()); ver == currVer {
		// do nothing
	} else if ver > currVer {
		return nil, ErrorTransportVersion
	} else {
		pl = protoMsgConvertor[ver](pl)
	}

	if value = pl.Value(); value == nil {
		return nil, ErrorMissingPayload
	}
	return value, nil
}

func protobuf2Vbmap(vbmap *protobuf.VbConnectionMap) *c.VbConnectionMap {
	return &c.VbConnectionMap{
		Bucket:   vbmap.GetBucket(),
		Vbuckets: c.Vbno32to16(vbmap.GetVbuckets()),
		Vbuuids:  vbmap.GetVbuuids(),
	}
}

func protobuf2KeyVersions(keys []*protobuf.KeyVersions) []*c.KeyVersions {
	kvs := make([]*c.KeyVersions, 0, len(keys))
	size := 4 // To avoid reallocs
	for _, key := range keys {
		kv := &c.KeyVersions{
			Seqno:    key.GetSeqno(),
			Docid:    key.GetDocid(),
			Uuids:    make([]uint64, 0, size),
			Commands: make([]byte, 0, size),
			Keys:     make([][]byte, 0, size),
			Oldkeys:  make([][]byte, 0, size),
		}
		commands := key.GetCommands()
		newkeys := key.GetKeys()
		oldkeys := key.GetOldkeys()
		for i, uuid := range key.GetUuids() {
			kv.Uuids = append(kv.Uuids, uuid)
			kv.Commands = append(kv.Commands, byte(commands[i]))
			kv.Keys = append(kv.Keys, newkeys[i])
			kv.Oldkeys = append(kv.Oldkeys, oldkeys[i])
		}
		kvs = append(kvs, kv)
	}
	return kvs
}

func protobuf2VbKeyVersions(protovbs []*protobuf.VbKeyVersions) []*c.VbKeyVersions {
	vbs := make([]*c.VbKeyVersions, 0, len(protovbs))
	for _, protovb := range protovbs {
		vb := &c.VbKeyVersions{
			Bucket:  protovb.GetBucketname(),
			Vbucket: uint16(protovb.GetVbucket()),
			Vbuuid:  protovb.GetVbuuid(),
			Kvs:     protobuf2KeyVersions(protovb.GetKvs()),
		}
		vbs = append(vbs, vb)
	}
	return vbs
}

// ProtobufVersion return version of protobuf schema used in packet transport.
func ProtobufVersion() byte {
	return (c.ProtobufDataPathMajorNum << 4) | c.ProtobufDataPathMinorNum
}

var protoMsgConvertor = map[byte]func(*protobuf.Payload) *protobuf.Payload{}
