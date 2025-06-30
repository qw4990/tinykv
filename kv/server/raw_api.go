package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, nil
	}
	defer reader.Close()
	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	}
	resp := &kvrpcpb.RawGetResponse{
		Value: val,
	}
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	if err := server.storage.Write(req.Context, []storage.Modify{{
		Data: storage.Put{
			Cf:    req.Cf,
			Key:   req.Key,
			Value: req.Value,
		},
	}}); err != nil {
		return nil, err
	}
	resp := &kvrpcpb.RawPutResponse{}
	return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	if err := server.storage.Write(req.Context, []storage.Modify{{
		Data: storage.Delete{
			Cf:  req.Cf,
			Key: req.Key,
		},
	}}); err != nil {
		return nil, err
	}
	resp := &kvrpcpb.RawDeleteResponse{}
	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, nil
	}
	defer reader.Close()

	iter := reader.IterCF(req.Cf)
	defer iter.Close()
	resp := &kvrpcpb.RawScanResponse{
		Kvs: make([]*kvrpcpb.KvPair, 0, req.Limit),
	}
	for iter.Seek(req.StartKey); iter.Valid(); iter.Next() {
		if len(resp.Kvs) >= int(req.Limit) {
			break
		}
		item := iter.Item()
		key := item.Key()
		value, err := item.Value()
		if err != nil {
			return nil, err
		}
		resp.Kvs = append(resp.Kvs, &kvrpcpb.KvPair{
			Key:   key,
			Value: value,
		})
	}
	return resp, nil
}
