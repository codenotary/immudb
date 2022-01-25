package embed

import (
	"context"
	"github.com/codenotary/immudb/pkg/uuid"
)

type localUUIDProvider struct {
	dataDir, defaultDbDir string
}

func NewLocalUUIDProvider(dataDir, defaultDbDir string) *localUUIDProvider {
	return &localUUIDProvider{dataDir, defaultDbDir}
}

func (r *localUUIDProvider) CurrentUUID(ctx context.Context) (string, error) {
	UUID, err := uuid.GetOrSetUUID(r.dataDir, r.defaultDbDir)
	if err != nil {
		return "", err
	}
	return UUID.String(), nil
}
