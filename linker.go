package linker

import (
	"github.com/hootuu/domain/chain"
	"github.com/hootuu/domain/scope"
)

func Append(vnCid chain.Cid, chainKey chain.Key, lead scope.Lead, dataCid chain.Cid) (*chain.Lead, error) {
	link := GetLink(vnCid, chainKey)
	return link.Append(lead, dataCid)
}
