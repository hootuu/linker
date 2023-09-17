package linker

import (
	"github.com/hootuu/domain/scope"
	"log/slog"
)

type Seeker interface {
	GetTail(lead *scope.Lead) (*NodePack, error)
}

type SeekerFactory interface {
	Next() (Seeker, bool)
}

var gSeekerFactory SeekerFactory

func InjectSeekerFactory(sf SeekerFactory) {
	gSeekerFactory = sf
}

func GetSeekerFactory() SeekerFactory {
	if gSeekerFactory == nil {
		slog.Error("must call InjectSeekerFactory first")
	}
	return gSeekerFactory
}
