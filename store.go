package linker

import (
	"github.com/boltdb/bolt"
	"github.com/hootuu/utils/errors"
	"github.com/hootuu/utils/sys"
	"go.mongodb.org/mongo-driver/bson"
	"go.uber.org/zap"
	"path/filepath"
)

var gDB *bolt.DB

func initStoreIfNeeded(storePath string) *errors.Error {
	var nErr error
	gDB, nErr = bolt.Open(filepath.Join(storePath, "linker.db"), 0600,
		&bolt.Options{ReadOnly: false})
	if nErr != nil {
		sys.Info("Linker Store Init Failed:", nErr.Error())
		return errors.Sys("init linker store failed")
	}
	return nil
}

func DB() *bolt.DB {
	if gDB == nil {
		sys.Error("should call linker.Init first.")
	}
	return gDB
}

func StorePut(bucket *bolt.Bucket, key []byte, obj interface{}) error {
	bsonData, err := bson.Marshal(obj)
	if err != nil {
		gLogger.Error("bson.Marshal error", zap.Error(err))
		return err
	}
	err = bucket.Put(key, bsonData)
	if err != nil {
		gLogger.Error("bucket.Put error", zap.Error(err))
		return err
	}
	return nil
}

func StoreGet[T any](bucket *bolt.Bucket, key []byte) (*T, error) {
	var m T
	bsonData := bucket.Get(key)
	if bsonData == nil {
		return nil, nil
	}
	err := bson.Unmarshal(bsonData, &m)
	if err != nil {
		gLogger.Error("bson.Unmarshal error", zap.Error(err))
		return nil, err
	}
	return &m, nil
}
