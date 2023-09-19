package linker

import (
	"github.com/hootuu/domain/chain"
	"github.com/hootuu/utils/errors"
	"github.com/hootuu/utils/logger"
	"github.com/hootuu/utils/sys"
)

var gLogger = logger.GetLogger("linker")

func InitIfNeeded(path string) *errors.Error {
	return initStoreIfNeeded(path)
}

func Care(link chain.CreationLink) (bool, *errors.Error) {
	line := MustGetLine(link.Lead.VN, link.GetChainKey())
	head, err := line.Head()
	if err != nil {
		return false, errors.Sys("Do Care Check failed")
	}
	if head == nil {
		return false, nil
	}
	return true, nil
}

func Genesis(link chain.CreationLink) *errors.Error {
	line := MustGetLine(link.Lead.VN, link.GetChainKey())
	return line.Genesis(link)
}

func Append(link chain.Link) (*chain.Lead, *errors.Error) {
	line := MustGetLine(link.Lead.VN, link.GetChainKey())
	cLead, err := line.Append(link)
	if err != nil {
		return nil, err
	}
	sys.Info("Tail to: [", cLead.Head, ", ", cLead.Tail, "]")
	return cLead, nil
}
