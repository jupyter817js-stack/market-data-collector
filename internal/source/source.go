package source

import (
	"context"

	"github.com/your-org/pol-odds-collector/internal/model"
)

type Collector interface {
	Name() string
	FetchOdds(ctx context.Context, sportKey string) (model.FetchBatch, error)
}
