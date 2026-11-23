package resolvers_test

import (
	"context"
	"testing"

	"github.com/iqhive/avro/v2/soe"
	"github.com/iqhive/avro/v2/soe/resolvers"
	"github.com/stretchr/testify/require"
)

func TestMemorySchemaStore_GetSchemaRejectsInvalidFingerprintLength(t *testing.T) {
	store := resolvers.NewMemorySchemaStore()

	for _, fingerprint := range [][]byte{nil, {}, make([]byte, 7), make([]byte, 9)} {
		schema, err := store.GetSchema(context.Background(), fingerprint)
		require.ErrorIs(t, err, soe.ErrUnknownSchema)
		require.Nil(t, schema)
	}
}
