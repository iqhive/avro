package resolvers

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/iqhive/avro/v2"
	"github.com/iqhive/avro/v2/soe"
	"github.com/iqhive/avro/v2/soe/internal/canonical"
)

// MemorySchemaStore is a basic in-memory schema store and resolver
// implementation.
type MemorySchemaStore struct {
	schemas sync.Map // map[uint64]avro.Schema, by soe.ComputeFingerprint
	// canonical holds the same schemas by the fingerprint Java's
	// SchemaNormalization computes (see soe.Codec.Decode).
	canonical sync.Map // map[uint64]avro.Schema
}

// NewMemorySchemaStore creates a new MemorySchemaStore.
func NewMemorySchemaStore() *MemorySchemaStore {
	return &MemorySchemaStore{}
}

// AddSchema puts a schema into the registry.
func (s *MemorySchemaStore) AddSchema(schema avro.Schema) error {
	fp, err := soe.ComputeFingerprint(schema)
	if err != nil {
		return err
	}

	key := keyFromFingerprint(fp)
	s.schemas.Store(key, schema)
	s.canonical.Store(keyFromFingerprint(canonical.Fingerprint(schema)), schema)
	return nil
}

// GetSchema implements SchemaResolver. A fingerprint is looked up as
// soe.ComputeFingerprint gives it, and then as Java's SchemaNormalization
// gives it, over the schema's Parsing Canonical Form; two schemas that differ
// only in logical types share the second, and the last one added has it.
func (s *MemorySchemaStore) GetSchema(_ context.Context, fingerprint []byte) (avro.Schema, error) {
	if len(fingerprint) != 8 {
		return nil, fmt.Errorf("%w: invalid fingerprint length %d", soe.ErrUnknownSchema, len(fingerprint))
	}
	key := keyFromFingerprint(fingerprint)
	schema, ok := s.schemas.Load(key)
	if !ok {
		schema, ok = s.canonical.Load(key)
	}
	if !ok {
		return nil, fmt.Errorf("%w: %x", soe.ErrUnknownSchema, fingerprint)
	}
	return schema.(avro.Schema), nil
}

// keyFromFingerprint creates an arbitrary uint64 encoding of a schema
// fingerprint, suitable as a map key.
func keyFromFingerprint(fingerprint []byte) uint64 {
	return binary.LittleEndian.Uint64(fingerprint)
}
