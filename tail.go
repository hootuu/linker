package tail

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"github.com/boltdb/bolt"
	"os"
	"sync"
)

var gDB *bolt.DB
var gMinSync = 3
var gChanMap map[string]chan int
var gLock sync.Mutex

func Init(dbFilePath string) error {
	fi, err := os.Stat(dbFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.Mkdir(dbFilePath, 0755)
			if err != nil {
				return err
			}
			fi, _ = os.Stat(dbFilePath)
		} else {
			return err
		}
	}
	if !fi.IsDir() {
		return errors.New(dbFilePath + " is not dir.")
	}
	db, err := bolt.Open(dbFilePath+"/tail.db", 0600, nil)
	if err != nil {
		return err
	}
	gDB = db
	gChanMap = make(map[string]chan int)
	return nil
}

func Ack(versionKey string) error {
	chainChan, ok := gChanMap[versionKey]
	if !ok {
		return nil
	}
	chainChan <- 1
	return nil
}

func Tail(vn string, chain string, tail string) (string, error) {
	if gDB == nil {
		return "", errors.New("must call Init first")
	}
	if len(vn) == 0 {
		return "", errors.New("invalid vn")
	}
	if len(chain) == 0 {
		return "", errors.New("invalid chain")
	}
	if len(tail) == 0 {
		return "", errors.New("invalid tail")
	}
	versionKey := buildVersionKey(vn, chain, tail)
	gLock.Lock()
	defer gLock.Unlock()
	chainChan, ok := gChanMap[versionKey]
	if !ok {
		chainChan = make(chan int, 100)
		gChanMap[versionKey] = chainChan
	}
	err := gDB.Update(func(tx *bolt.Tx) error {
		bucket, innerErr := tx.CreateBucketIfNotExists([]byte(vn))
		if innerErr != nil {
			return innerErr
		}
		innerErr = bucket.Put([]byte(chain), []byte(tail))
		if innerErr != nil {
			return innerErr
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	var wg sync.WaitGroup
	wg.Add(gMinSync)
	go func() {
		count := 0
		for count < gMinSync {
			<-chainChan
			count++
			wg.Done()
		}

	}()
	wg.Wait()
	defer func() {
		delete(gChanMap, versionKey)
	}()
	return versionKey, nil
}

func buildVersionKey(vn string, chain string, tail string) string {
	hash := md5.Sum([]byte(vn + "_" + chain + "_" + tail))
	hashString := hex.EncodeToString(hash[:])
	return hashString
}
