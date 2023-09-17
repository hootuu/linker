package linker

import (
	"errors"
	"github.com/boltdb/bolt"
	"github.com/hootuu/domain/chain"
	"github.com/hootuu/domain/scope"
	"log/slog"
	"sync"
)

const HeadKP = "H"
const TailKP = "T"

type Link struct {
	vn       chain.Cid
	chainKey chain.Key
	lock     sync.Mutex
	head     *NodePack
	tail     *NodePack
}

func (link *Link) Head() (*NodePack, error) {
	var head *NodePack
	err := DB().View(func(tx *bolt.Tx) error {
		iBucket, iErr := tx.CreateBucketIfNotExists([]byte(link.vn))
		if iErr != nil {
			slog.Error("CreateBucketIfNotExists error", iErr)
			return iErr
		}
		headKey := link.buildKey(HeadKP)
		head, iErr = StoreGet[NodePack](iBucket, headKey)
		if iErr != nil {
			return iErr
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return head, nil
}

func (link *Link) Tail() (*NodePack, error) {
	var tail *NodePack
	err := DB().View(func(tx *bolt.Tx) error {
		iBucket, iErr := tx.CreateBucketIfNotExists([]byte(link.vn))
		if iErr != nil {
			slog.Error("CreateBucketIfNotExists error", iErr)
			return iErr
		}
		tailKey := link.buildKey(TailKP)
		tail, iErr = StoreGet[NodePack](iBucket, tailKey)
		if iErr != nil {
			return iErr
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return tail, nil
}

func (link *Link) Rectify() error {
	localTail, err := link.Tail()
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
			iErr := link.doRectify(seeker, localTail, &lock)
			if iErr != nil {
				hasErr = true
			}
		}()
	}
	wg.Wait()
	if hasErr {
		return errors.New("one didn't make it")
	}
	return nil
}

func (link *Link) doRectify(seeker Seeker, localTail *NodePack, lock *sync.Mutex) error {
	rmTail, err := seeker.GetTail(&scope.Lead{
		VN:    "",
		Scope: "",
	})
	if err != nil {
		slog.Error("seeker.GetTail failed", err)
		return err
	}
	if rmTail == nil {
		return nil
	}
	if localTail == nil {
		err := link.updateTo(rmTail)
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

	err = link.updateTo(rmTail)
	if err != nil {
		return err
	}
	return nil
}

func (link *Link) updateTo(tail *NodePack) error {
	slog.Error("implements it....")
	return nil
}

func (link *Link) Append(lead scope.Lead, data chain.Cid) (*chain.Lead, error) {
	link.lock.Lock()
	defer link.lock.Unlock()
	err := DB().Update(func(tx *bolt.Tx) error {
		iBucket, iErr := tx.CreateBucketIfNotExists([]byte(link.vn))
		if iErr != nil {
			slog.Error("CreateBucketIfNotExists error", iErr)
			return iErr
		}

		tailKey := link.buildKey(TailKP)
		tailNodePack, iErr := StoreGet[NodePack](iBucket, tailKey)
		if iErr != nil {
			return iErr
		}
		if tailNodePack == nil {
			headKey := link.buildKey(HeadKP)
			headNode := HeadNode(lead, data)
			headNodePack, iErr := NewNodePack(headNode)
			if iErr != nil {
				return iErr
			}
			iErr = StorePut(iBucket, headKey, headNodePack)
			if iErr != nil {
				return iErr
			}
			link.head = headNodePack
			tailNodePack := headNodePack
			iErr = StorePut(iBucket, tailKey, tailNodePack)
			if iErr != nil {
				return iErr
			}
			link.tail = tailNodePack
			return nil
		}
		tailNodePack, iErr = tailNodePack.Next(data)
		if iErr != nil {
			return iErr
		}
		iErr = StorePut(iBucket, tailKey, tailNodePack)
		if iErr != nil {
			return iErr
		}
		link.tail = tailNodePack
		return nil
	})
	if err != nil {
		slog.Error("db.update error:", err)
		return nil, err
	}
	return &chain.Lead{
		Head: link.head.Cid,
		Tail: link.tail.Cid,
	}, nil
}

func (link *Link) buildKey(kp string) []byte {
	return []byte(link.chainKey + "_" + kp)
}

var gLinkFactory = make(map[string]map[string]*Link)
var gLinkLock sync.Mutex

func GetLink(vn chain.Cid, chainKey chain.Key) *Link {
	vnDict, ok := gLinkFactory[vn]
	if !ok {
		gLinkLock.Lock()
		vnDict = make(map[string]*Link)
		gLinkFactory[vn] = vnDict
		gLinkLock.Unlock()
	}
	linkM, ok := vnDict[chainKey]
	if !ok {
		gLinkLock.Lock()
		linkM = &Link{
			vn:       vn,
			chainKey: chainKey,
		}
		vnDict[chainKey] = linkM
		gLinkLock.Unlock()
	}
	return linkM
}
