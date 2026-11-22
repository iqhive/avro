package avro

import (
	"encoding/json"
	"fmt"
)

// externalRefsIdentity returns fingerprints of named schemas the schema references but does not define.
func externalRefsIdentity(schema Schema) []byte {
	return appendExternalRefs(nil, schema, map[NamedSchema]struct{}{})
}

func appendExternalRefs(identity []byte, schema Schema, seen map[NamedSchema]struct{}) []byte {
	ref, isRef := schema.(*RefSchema)
	if isRef {
		schema = ref.Schema()
	}
	if named, ok := schema.(NamedSchema); ok {
		if _, ok = seen[named]; ok {
			return identity
		}
		seen[named] = struct{}{}
	}
	if isRef {
		fp := schema.Fingerprint()
		identity = append(identity, fp[:]...)
	}
	for _, child := range schemaChildren(schema) {
		identity = appendExternalRefs(identity, child, seen)
	}
	return identity
}

// defaultsIdentity returns the field defaults of every record schema reaches,
// as JSON, or nil if none has one. Encoders write a default for a field the Go
// value does not have, and the canonical form behind Fingerprint leaves
// defaults out, so the cache identity must add them.
func defaultsIdentity(schema Schema) []byte {
	return appendDefaults(nil, schema, map[NamedSchema]struct{}{})
}

func appendDefaults(identity []byte, schema Schema, seen map[NamedSchema]struct{}) []byte {
	if ref, ok := schema.(*RefSchema); ok {
		schema = ref.Schema()
	}
	if named, ok := schema.(NamedSchema); ok {
		if _, ok = seen[named]; ok {
			return identity
		}
		seen[named] = struct{}{}
	}
	if rec, ok := schema.(*RecordSchema); ok {
		for _, f := range rec.Fields() {
			if !f.HasDefault() {
				continue
			}
			def, err := json.Marshal(f.Default())
			if err != nil {
				def = []byte(fmt.Sprintf("%#v", f.Default()))
			}
			identity = fmt.Appendf(identity, "%s.%s=%s;", rec.FullName(), f.Name(), def)
		}
	}
	for _, child := range schemaChildren(schema) {
		identity = appendDefaults(identity, child, seen)
	}
	return identity
}
