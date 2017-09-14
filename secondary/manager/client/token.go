// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package client

import (
	"encoding/json"
	"errors"
	"fmt"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
)

/////////////////////////////////////////////////////////////////////////
// Const
////////////////////////////////////////////////////////////////////////

const DeleteDDLCommandTokenTag = "commandToken/delete/"
const DDLMetakvDir = c.IndexingMetaDir + "ddl/"
const DeleteDDLCommandTokenPath = DDLMetakvDir + DeleteDDLCommandTokenTag

const BuildDDLCommandTokenTag = "commandToken/build/"
const BuildDDLCommandTokenPath = DDLMetakvDir + BuildDDLCommandTokenTag

const IndexerVersionTokenTag = "versionToken"
const InfoMetakvDir = c.IndexingMetaDir + "info/"
const IndexerVersionTokenPath = InfoMetakvDir + IndexerVersionTokenTag

const IndexerStorageModeTokenTag = "storageModeToken/"
const IndexerStorageModeTokenPath = InfoMetakvDir + IndexerStorageModeTokenTag

//////////////////////////////////////////////////////////////
// Concrete Type
//////////////////////////////////////////////////////////////

type DeleteCommandToken struct {
	Name   string
	Bucket string
	DefnId c.IndexDefnId
}

type BuildCommandToken struct {
	Name   string
	Bucket string
	DefnId c.IndexDefnId
}

type IndexerVersionToken struct {
	Version uint64
}

type IndexerStorageModeToken struct {
	NodeUUID         string
	Override         string
	LocalStorageMode string
}

//////////////////////////////////////////////////////////////
// Delete Token Management
//////////////////////////////////////////////////////////////

//
// Generate a token to metakv for recovery purpose
//
func PostDeleteCommandToken(defnId c.IndexDefnId) error {

	commandToken := &DeleteCommandToken{
		DefnId: defnId,
	}

	id := fmt.Sprintf("%v", defnId)
	if err := c.MetakvSet(DeleteDDLCommandTokenPath+id, commandToken); err != nil {
		return errors.New(fmt.Sprintf("Fail to delete index.  Internal Error = %v", err))
	}

	return nil
}

//
// Does token exist? Return true only if token exist and there is no error.
//
func DeleteCommandTokenExist(defnId c.IndexDefnId) (bool, error) {

	commandToken := &DeleteCommandToken{}
	id := fmt.Sprintf("%v", defnId)
	return c.MetakvGet(DeleteDDLCommandTokenPath+id, commandToken)
}

//
// Unmarshall
//
func UnmarshallDeleteCommandToken(data []byte) (*DeleteCommandToken, error) {

	r := new(DeleteCommandToken)
	if err := json.Unmarshal(data, r); err != nil {
		return nil, err
	}

	return r, nil
}

func MarshallDeleteCommandToken(r *DeleteCommandToken) ([]byte, error) {

	buf, err := json.Marshal(&r)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

/////////////////////////////////////////////////////////////
// Build Token Management
//////////////////////////////////////////////////////////////

//
// Generate a token to metakv for recovery purpose
//
func PostBuildCommandToken(defnId c.IndexDefnId) error {

	commandToken := &BuildCommandToken{
		DefnId: defnId,
	}

	id := fmt.Sprintf("%v", defnId)
	if err := c.MetakvSet(BuildDDLCommandTokenPath+id, commandToken); err != nil {
		return errors.New(fmt.Sprintf("Fail to buildindex.  Internal Error = %v", err))
	}

	return nil
}

//
// Does token exist? Return true only if token exist and there is no error.
//
func BuildCommandTokenExist(defnId c.IndexDefnId) (bool, error) {

	commandToken := &BuildCommandToken{}
	id := fmt.Sprintf("%v", defnId)
	return c.MetakvGet(BuildDDLCommandTokenPath+id, commandToken)
}

//
// Unmarshall
//
func UnmarshallBuildCommandToken(data []byte) (*BuildCommandToken, error) {

	r := new(BuildCommandToken)
	if err := json.Unmarshal(data, r); err != nil {
		return nil, err
	}

	return r, nil
}

//
// Marshall
//
func MarshallBuildCommandToken(r *BuildCommandToken) ([]byte, error) {

	buf, err := json.Marshal(&r)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

//////////////////////////////////////////////////////////////
// Version Management
//////////////////////////////////////////////////////////////

//
// Generate a token to metakv for indexer version
//
func PostIndexerVersionToken(version uint64) error {

	token := &IndexerVersionToken{
		Version: version,
	}

	if err := c.MetakvSet(IndexerVersionTokenPath, token); err != nil {
		logging.Errorf("Fail to post indexer version to metakv.  Internal Error = %v", err)
		return err
	}

	return nil
}

//
// Does token exist? Return true only if token exist and there is no error.
//
func GetIndexerVersionToken() (uint64, error) {

	token := &IndexerVersionToken{}
	found, err := c.MetakvGet(IndexerVersionTokenPath, token)
	if err != nil {
		logging.Errorf("Fail to get indexer version from metakv.  Internal Error = %v", err)
		return 0, err
	}

	if !found {
		return 0, nil
	}

	return token.Version, nil
}

//
// Unmarshall
//
func UnmarshallIndexerVersionToken(data []byte) (*IndexerVersionToken, error) {

	r := new(IndexerVersionToken)
	if err := json.Unmarshal(data, r); err != nil {
		return nil, err
	}

	return r, nil
}

func MarshallIndexerVersionToken(r *IndexerVersionToken) ([]byte, error) {

	buf, err := json.Marshal(&r)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

//////////////////////////////////////////////////////////////
// Storage Mode Management
//////////////////////////////////////////////////////////////

//
// Generate a token to metakv for indexer storage mode
//
func PostIndexerStorageModeOverride(nodeUUID string, override string) error {

	if len(nodeUUID) == 0 {
		return errors.New("NodeUUId is not specified. Fail to set storage mode override.")
	}

	token, err := GetIndexerStorageModeToken(nodeUUID)
	if err != nil {
		logging.Errorf("Fail to read indexer storage mode to metakv for node %v.  Internal Error = %v", nodeUUID, err)
		return err
	}

	if token == nil {
		token = &IndexerStorageModeToken{
			NodeUUID: nodeUUID,
		}
	}
	token.Override = override

	if err := c.MetakvSet(IndexerStorageModeTokenPath+nodeUUID, token); err != nil {
		logging.Errorf("Fail to post indexer storage mode to metakv for node %v.  Internal Error = %v", nodeUUID, err)
		return err
	}

	return nil
}

//
// Generate a token to metakv for indexer storage mode
//
func PostIndexerLocalStorageMode(nodeUUID string, storageMode c.StorageMode) error {

	if len(nodeUUID) == 0 {
		return errors.New("NodeUUId is not specified. Fail to set local storage mode in metakv.")
	}

	token, err := GetIndexerStorageModeToken(nodeUUID)
	if err != nil {
		logging.Errorf("Fail to read indexer storage mode to metakv for node %v.  Internal Error = %v", nodeUUID, err)
		return err
	}

	if token == nil {
		token = &IndexerStorageModeToken{
			NodeUUID: nodeUUID,
		}
	}

	token.LocalStorageMode = string(c.StorageModeToIndexType(storageMode))

	if err := c.MetakvSet(IndexerStorageModeTokenPath+nodeUUID, token); err != nil {
		logging.Errorf("Fail to post indexer storage mode to metakv for node %v.  Internal Error = %v", nodeUUID, err)
		return err
	}

	return nil
}

//
// Does token exist? Return true only if token exist and there is no error.
//
func GetIndexerStorageModeToken(nodeUUID string) (*IndexerStorageModeToken, error) {

	if len(nodeUUID) == 0 {
		return nil, errors.New("NodeUUId is not specified. Fail to get storage mode token.")
	}

	token := &IndexerStorageModeToken{}
	found, err := c.MetakvGet(IndexerStorageModeTokenPath+nodeUUID, token)
	if err != nil {
		logging.Errorf("Fail to get indexer storage token from metakv for node %v.  Internal Error = %v", nodeUUID, err)
		return nil, err
	}

	if !found {
		return nil, nil
	}

	return token, nil
}

//
// Does token exist? Return true only if token exist and there is no error.
//
func GetIndexerStorageModeOverride(nodeUUID string) (string, error) {

	if len(nodeUUID) == 0 {
		return "", errors.New("NodeUUId is not specified. Fail to get storage mode override.")
	}

	token, err := GetIndexerStorageModeToken(nodeUUID)
	if err != nil {
		logging.Errorf("Fail to read indexer storage mode to metakv for node %v.  Internal Error = %v", nodeUUID, err)
		return "", err
	}

	if token != nil {
		return token.Override, nil
	}

	return "", nil
}

//
// Does token exist? Return true only if token exist and there is no error.
//
func GetIndexerLocalStorageMode(nodeUUID string) (c.StorageMode, error) {

	if len(nodeUUID) == 0 {
		return c.NOT_SET, errors.New("NodeUUId is not specified. Fail to get storage mode override.")
	}

	token, err := GetIndexerStorageModeToken(nodeUUID)
	if err != nil {
		logging.Errorf("Fail to read indexer storage mode to metakv for node %v.  Internal Error = %v", nodeUUID, err)
		return c.NOT_SET, err
	}

	if token != nil {
		return c.IndexTypeToStorageMode(c.IndexType(token.LocalStorageMode)), nil
	}

	return c.NOT_SET, nil
}

//
//
// Unmarshall
//
func UnmarshallIndexerStorageModeToken(data []byte) (*IndexerStorageModeToken, error) {

	r := new(IndexerStorageModeToken)
	if err := json.Unmarshal(data, r); err != nil {
		return nil, err
	}

	return r, nil
}

func MarshallIndexerStorageModeToken(r *IndexerStorageModeToken) ([]byte, error) {

	buf, err := json.Marshal(&r)
	if err != nil {
		return nil, err
	}

	return buf, nil
}
