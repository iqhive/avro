// Package canonical writes a schema's Parsing Canonical Form as Java's
// SchemaNormalization does, for the single-object fingerprint Java writes.
package canonical

import (
	"strconv"
	"strings"

	"github.com/iqhive/avro/v2"
	"github.com/iqhive/avro/v2/pkg/crc64"
)

// Fingerprint returns the little-endian CRC-64-AVRO fingerprint of schema's
// Parsing Canonical Form, which is what Java's SchemaNormalization computes
// and its single-object encoder writes.
func Fingerprint(schema avro.Schema) []byte {
	h := crc64.SumWithByteOrder([]byte(Form(schema)), crc64.LittleEndian)
	return h[:]
}

// Form returns schema's Parsing Canonical Form. schema.String() is the form
// with logical types kept; this keeps only name, type, fields, symbols, items,
// values and size, in that order, writes a primitive as its name alone, as
// Java does whatever its logical type (Python's avro and goavro write
// {"type":"long"} for a primitive with one), and writes a named schema as its
// full name after its first appearance.
func Form(schema avro.Schema) string {
	var b strings.Builder
	write(&b, schema, map[string]bool{})
	return b.String()
}

func write(b *strings.Builder, s avro.Schema, seen map[string]bool) {
	if ref, ok := s.(*avro.RefSchema); ok {
		s = ref.Schema()
	}
	if n, ok := s.(avro.NamedSchema); ok {
		if seen[n.FullName()] {
			b.WriteString(`"` + n.FullName() + `"`)
			return
		}
		seen[n.FullName()] = true
	}
	switch s := s.(type) {
	case *avro.RecordSchema:
		typ := "record"
		if s.IsError() {
			typ = "error"
		}
		b.WriteString(`{"name":"` + s.FullName() + `","type":"` + typ + `","fields":[`)
		for i, f := range s.Fields() {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(`{"name":"` + f.Name() + `","type":`)
			write(b, f.Type(), seen)
			b.WriteByte('}')
		}
		b.WriteString(`]}`)
	case *avro.EnumSchema:
		b.WriteString(`{"name":"` + s.FullName() + `","type":"enum","symbols":[`)
		for i, sym := range s.Symbols() {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(`"` + sym + `"`)
		}
		b.WriteString(`]}`)
	case *avro.FixedSchema:
		b.WriteString(`{"name":"` + s.FullName() + `","type":"fixed","size":` + strconv.Itoa(s.Size()) + `}`)
	case *avro.ArraySchema:
		b.WriteString(`{"type":"array","items":`)
		write(b, s.Items(), seen)
		b.WriteByte('}')
	case *avro.MapSchema:
		b.WriteString(`{"type":"map","values":`)
		write(b, s.Values(), seen)
		b.WriteByte('}')
	case *avro.UnionSchema:
		b.WriteByte('[')
		for i, t := range s.Types() {
			if i > 0 {
				b.WriteByte(',')
			}
			write(b, t, seen)
		}
		b.WriteByte(']')
	default:
		// A primitive or null, whose logical type is not part of the form.
		b.WriteString(`"` + string(s.Type()) + `"`)
	}
}
