package linker

import (
	"github.com/boltdb/bolt"
	"go.mongodb.org/mongo-driver/bson"
	"log/slog"
)

var gDB *bolt.DB

func Init(storePath string) error {
	var err error
	gDB, err = bolt.Open(storePath+"/linker.db", 0600, nil)
	if err != nil {
		return err
	}
	return nil
}

func DB() *bolt.DB {
	if gDB == nil {
		slog.Error("should call linker.Init first.")
	}
	return gDB
}

func StorePut(bucket *bolt.Bucket, key []byte, obj interface{}) error {
	bsonData, err := bson.Marshal(obj)
	if err != nil {
		slog.Error("bson.Marshal error", err)
		return err
	}
	err = bucket.Put(key, bsonData)
	if err != nil {
		slog.Error("bucket.Put error", err)
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
		slog.Error("bson.Unmarshal error", err)
		return nil, err
	}
	return &m, nil
}
