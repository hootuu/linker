package linker

import (
	"github.com/boltdb/bolt"
	"github.com/hootuu/domain/chain"
	"github.com/hootuu/domain/scope"
	"github.com/hootuu/utils/errors"
	"go.uber.org/zap"
	"sync"
)

const HeadKP = "H"
const TailKP = "T"

type Line struct {
	vn       chain.Cid
	chainKey chain.Key
	lock     sync.Mutex
	head     *NodePack
	tail     *NodePack
}

func (line *Line) Head() (*NodePack, *errors.Error) {
	var head *NodePack
	nErr := DB().View(func(tx *bolt.Tx) error {
		iBucket, iErr := tx.CreateBucketIfNotExists([]byte(line.vn))
		if iErr != nil {
			gLogger.Error("CreateBucketIfNotExists error", zap.Error(iErr))
			return iErr
		}
		headKey := line.buildKey(HeadKP)
		head, iErr = StoreGet[NodePack](iBucket, headKey)
		if iErr != nil {
			return iErr
		}
		return nil
	})
	if nErr != nil {
		return nil, errors.Sys("Get Head Failed:"+nErr.Error(), nErr)
	}
	return head, nil
}

func (line *Line) Tail() (*NodePack, *errors.Error) {
	var tail *NodePack
	nErr := DB().View(func(tx *bolt.Tx) error {
		iBucket, iErr := tx.CreateBucketIfNotExists([]byte(line.vn))
		if iErr != nil {
			gLogger.Error("CreateBucketIfNotExists error", zap.Error(iErr))
			return iErr
		}
		tailKey := line.buildKey(TailKP)
		tail, iErr = StoreGet[NodePack](iBucket, tailKey)
		if iErr != nil {
			return iErr
		}
		return nil
	})
	if nErr != nil {
		return nil, errors.Sys("Get Tail Failed:"+nErr.Error(), nErr)
	}
	return tail, nil
}

func (line *Line) Rectify() *errors.Error {
	localTail, err := line.Tail()
	if err != nil {
		return err
	}
	sf := GetSeekerFactory()
	if sf == nil {
		return nil
	}
	var wg sync.WaitGroup
	var lock sync.Mutex
	hasErr := false
	for {
		seeker, ok := sf.Next()
		if !ok {
			break
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			iErr := line.doRectify(seeker, localTail, &lock)
			if iErr != nil {
				hasErr = true
			}
		}()
	}
	wg.Wait()
	if hasErr {
		return errors.Sys("one didn't make it")
	}
	return nil
}

func (line *Line) doRectify(seeker Seeker, localTail *NodePack, lock *sync.Mutex) error {
	rmTail, err := seeker.GetTail(&scope.Lead{
		VN:    "",
		Scope: "",
	})
	if err != nil {
		gLogger.Error("seeker.GetTail failed", zap.Error(err))
		return err
	}
	if rmTail == nil {
		return nil
	}
	if localTail == nil {
		err := line.updateTo(rmTail)
		if err != nil {
			return err
		}
		return nil
	}

	if localTail.Cid == rmTail.Cid && localTail.Node.Block == rmTail.Node.Block {
		return nil
	}

	if localTail.Node.Block >= rmTail.Node.Block {
		return nil
	}

	err = line.updateTo(rmTail)
	if err != nil {
		return err
	}
	return nil
}

func (line *Line) updateTo(tail *NodePack) error {
	//slog.Error("implements it....")
	return nil
}

func (line *Line) Genesis(link chain.CreationLink) *errors.Error {
	line.lock.Lock()
	defer line.lock.Unlock()
	nErr := DB().Update(func(tx *bolt.Tx) error {
		iBucket, iErr := tx.CreateBucketIfNotExists([]byte(line.vn))
		if iErr != nil {
			gLogger.Error("CreateBucketIfNotExists error", zap.Error(iErr))
			return iErr
		}

		tailKey := line.buildKey(TailKP)
		tailNodePack, iErr := StoreGet[NodePack](iBucket, tailKey)
		if iErr != nil {
			return iErr
		}
		if tailNodePack != nil {
			return nil
		}
		headKey := line.buildKey(HeadKP)
		headNode := HeadNode(link.Lead, chain.CreationLinkData)
		headNodePack, iErr := NewNodePack(headNode)
		if iErr != nil {
			return iErr
		}
		iErr = StorePut(iBucket, headKey, headNodePack)
		if iErr != nil {
			return iErr
		}
		line.head = headNodePack
		iErr = StorePut(iBucket, tailKey, headNodePack)
		if iErr != nil {
			return iErr
		}
		line.tail = tailNodePack
		return nil
	})
	if nErr != nil {
		gLogger.Error("db.update error:", zap.Error(nErr))
		return errors.Sys("Genesis Failed")
	}
	return nil
}

func (line *Line) Append(link chain.Link) (*chain.Lead, *errors.Error) {
	line.lock.Lock()
	defer line.lock.Unlock()
	nErr := DB().Update(func(tx *bolt.Tx) error {
		iBucket, iErr := tx.CreateBucketIfNotExists([]byte(line.vn))
		if iErr != nil {
			gLogger.Error("CreateBucketIfNotExists error", zap.Error(iErr))
			return iErr
		}

		tailKey := line.buildKey(TailKP)
		tailNodePack, iErr := StoreGet[NodePack](iBucket, tailKey)
		if iErr != nil {
			return iErr
		}
		if tailNodePack == nil {
			headKey := line.buildKey(HeadKP)
			headNode := HeadNode(link.Lead, link.Data)
			headNodePack, iErr := NewNodePack(headNode)
			if iErr != nil {
				return iErr
			}
			iErr = StorePut(iBucket, headKey, headNodePack)
			if iErr != nil {
				return iErr
			}
			line.head = headNodePack
			tailNodePack := headNodePack
			iErr = StorePut(iBucket, tailKey, tailNodePack)
			if iErr != nil {
				return iErr
			}
			line.tail = tailNodePack
			return nil
		}
		tailNodePack, iErr = tailNodePack.Next(link.Data)
		if iErr != nil {
			return iErr
		}
		iErr = StorePut(iBucket, tailKey, tailNodePack)
		if iErr != nil {
			return iErr
		}
		line.tail = tailNodePack
		return nil
	})
	if nErr != nil {
		gLogger.Error("db.update error:", zap.Error(nErr))
		return nil, errors.Sys("Linker Append Failed:" + nErr.Error())
	}
	return &chain.Lead{
		Head: line.head.Cid,
		Tail: line.tail.Cid,
	}, nil
}

func (line *Line) buildKey(kp string) []byte {
	return []byte(line.chainKey + "_" + kp)
}

var gLineFactory = make(map[string]map[string]*Line)
var gLineLock sync.Mutex

func MustGetLine(vn chain.Cid, chainKey chain.Key) *Line {
	vnDict, ok := gLineFactory[vn]
	if !ok {
		gLineLock.Lock()
		vnDict = make(map[string]*Line)
		gLineFactory[vn] = vnDict
		gLineLock.Unlock()
	}
	lineM, ok := vnDict[chainKey]
	if !ok {
		gLineLock.Lock()
		lineM = &Line{
			vn:       vn,
			chainKey: chainKey,
		}
		vnDict[chainKey] = lineM
		gLineLock.Unlock()
	}
	return lineM
}
