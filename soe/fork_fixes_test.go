package soe_test

// Regression tests for defects found in the soe package after the fork from
// hamba/avro and iskorotkov/avro. Each test says what went wrong.

import (
	"bytes"
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iqhive/avro/v2"
	"github.com/iqhive/avro/v2/pkg/crc64"
	"github.com/iqhive/avro/v2/soe"
	"github.com/iqhive/avro/v2/soe/resolvers"
)

type (
	fixLine struct {
		Qty int32 `avro:"qty"`
	}
	fixOrder struct {
		ID    string                    `avro:"id"`
		At    time.Time                 `avro:"at"`
		Price *big.Rat                  `avro:"price"`
		FX    *big.Rat                  `avro:"fx"`
		Dur   avro.LogicalDuration      `avro:"dur"`
		State string                    `avro:"state"`
		Lines []fixLine                 `avro:"lines"`
		Extra map[string]*time.Duration `avro:"extra"`
		Next  *fixOrder                 `avro:"next"`
		Same  *big.Rat                  `avro:"same"`
	}
)

// canonicalCases are schemas with logical types and the Parsing Canonical
// Form Java's SchemaNormalization gives each: only name, type, fields,
// symbols, items, values and size are kept, a primitive is its name alone,
// and a named schema after its first appearance is its full name. A value of
// each is given too.
var canonicalCases = []struct {
	name, schema, canonical string
	value                   func() any
}{
	{
		name:      "timestamp-millis",
		schema:    `{"type":"long","logicalType":"timestamp-millis"}`,
		canonical: `"long"`,
		value:     func() any { return time.UnixMilli(1700000000123).UTC() },
	},
	{
		name:      "uuid",
		schema:    `{"type":"string","logicalType":"uuid"}`,
		canonical: `"string"`,
		value:     func() any { return "6ba7b810-9dad-11d1-80b4-00c04fd430c8" },
	},
	{
		name:      "date",
		schema:    `{"type":"int","logicalType":"date"}`,
		canonical: `"int"`,
		value:     func() any { return time.Date(2026, 11, 23, 0, 0, 0, 0, time.UTC) },
	},
	{
		name: "record",
		schema: `{"type":"record","name":"Order","namespace":"fixsoe","doc":"an order","aliases":["Old"],"fields":[
  {"name":"id","type":{"type":"string","logicalType":"uuid"},"doc":"the id"},
  {"name":"at","type":{"type":"long","logicalType":"timestamp-micros"},"aliases":["when"]},
  {"name":"price","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}},
  {"name":"fx","type":{"type":"fixed","name":"Money","size":8,"logicalType":"decimal","precision":18,"scale":4}},
  {"name":"dur","type":{"type":"fixed","name":"Dur","namespace":"fixsoe.time","size":12,"logicalType":"duration"}},
  {"name":"state","type":{"type":"enum","name":"State","symbols":["NEW","DONE"],"default":"NEW"}},
  {"name":"lines","type":{"type":"array","items":{"type":"record","name":"Line","fields":[
    {"name":"qty","type":"int","order":"descending","default":1}]}}},
  {"name":"extra","type":{"type":"map","values":["null",{"type":"int","logicalType":"time-millis"}]}},
  {"name":"next","type":["null","Order"],"default":null},
  {"name":"same","type":"fixsoe.Money"}
]}`,
		canonical: `{"name":"fixsoe.Order","type":"record","fields":[` +
			`{"name":"id","type":"string"},` +
			`{"name":"at","type":"long"},` +
			`{"name":"price","type":"bytes"},` +
			`{"name":"fx","type":{"name":"fixsoe.Money","type":"fixed","size":8}},` +
			`{"name":"dur","type":{"name":"fixsoe.time.Dur","type":"fixed","size":12}},` +
			`{"name":"state","type":{"name":"fixsoe.State","type":"enum","symbols":["NEW","DONE"]}},` +
			`{"name":"lines","type":{"type":"array","items":{"name":"fixsoe.Line","type":"record","fields":[{"name":"qty","type":"int"}]}}},` +
			`{"name":"extra","type":{"type":"map","values":["null","int"]}},` +
			`{"name":"next","type":["null","fixsoe.Order"]},` +
			`{"name":"same","type":"fixsoe.Money"}]}`,
		value: func() any {
			d := 1500 * time.Millisecond
			return &fixOrder{
				ID:    "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
				At:    time.UnixMicro(1700000000123456).UTC(),
				Price: big.NewRat(12345, 100),
				FX:    big.NewRat(-5, 4),
				Dur:   avro.LogicalDuration{Months: 1, Days: 2, Milliseconds: 3},
				State: "DONE",
				Lines: []fixLine{{Qty: 2}, {Qty: 3}},
				Extra: map[string]*time.Duration{"wait": &d},
				Next:  &fixOrder{Price: big.NewRat(0, 1), FX: big.NewRat(0, 1), Same: big.NewRat(0, 1), State: "NEW", Lines: []fixLine{}, Extra: map[string]*time.Duration{}},
				Same:  big.NewRat(7, 1),
			}
		},
	},
}

// specFingerprint is the little-endian CRC-64-AVRO fingerprint of a schema's
// Parsing Canonical Form, which Java's SchemaNormalization computes.
func specFingerprint(canonical string) []byte {
	h := crc64.SumWithByteOrder([]byte(canonical), crc64.LittleEndian)
	return h[:]
}

// withFingerprint returns data, a single-object body, with fp in its header.
func withFingerprint(data, fp []byte) []byte {
	out := bytes.Clone(data)
	copy(out[2:10], fp)
	return out
}

// newTarget returns a pointer to a new value of v's type, to decode into.
func newTarget(v any) any {
	switch v.(type) {
	case time.Time:
		return new(time.Time)
	case string:
		return new(string)
	default:
		return new(fixOrder)
	}
}

// assertSameValue fails t unless got, from newTarget, encodes to data, the
// body it was decoded from. Comparing encodings tells values apart as the
// schema does, which reflect.DeepEqual on a big.Rat or a time.Time does not.
func assertSameValue(t *testing.T, codec *soe.Codec, data []byte, got any) {
	t.Helper()
	switch p := got.(type) {
	case *time.Time:
		got = *p
	case *string:
		got = *p
	default:
		// A *fixOrder encodes as it is.
	}
	again, err := codec.Encode(got)
	require.NoError(t, err)
	assert.Equal(t, data, again)
}

// The single-object decoders compared the header's fingerprint only with the
// one this package writes, the CRC-64-AVRO of String(), which keeps logical
// types. Java's fingerprint is that of the Parsing Canonical Form as its
// SchemaNormalization computes it, which drops them, so for any schema with a
// logical type Java's bodies were refused as written by another schema. Both
// are accepted now. The fingerprint written is unchanged, so peers that only
// know it still read this package's bodies.
func TestFix_DecodeAcceptsJavasFingerprint(t *testing.T) {
	for _, tc := range canonicalCases {
		t.Run(tc.name, func(t *testing.T) {
			schema, err := avro.ParseWithCache(tc.schema, "", &avro.SchemaCache{})
			require.NoError(t, err)
			codec, err := soe.NewCodec(schema)
			require.NoError(t, err)
			value := tc.value()
			data, err := codec.Encode(value)
			require.NoError(t, err)
			written, err := soe.ComputeFingerprint(schema)
			require.NoError(t, err)
			require.Equal(t, written, data[2:10], "the fingerprint written changed")
			spec := specFingerprint(tc.canonical)
			require.NotEqual(t, written, spec, "the case needs a schema whose two fingerprints differ")

			body := withFingerprint(data, spec)
			got := newTarget(value)
			require.NoError(t, codec.Decode(body, got), "a body with Java's fingerprint")
			assertSameValue(t, codec, data, got)

			// Another schema's fingerprint is still refused.
			other := specFingerprint(`"bytes"`)
			assert.ErrorContains(t, codec.Decode(withFingerprint(data, other), newTarget(value)), "bad fingerprint")

			store := resolvers.NewMemorySchemaStore()
			require.NoError(t, store.AddSchema(schema))
			got = newTarget(value)
			require.NoError(t, soe.NewDynamicDecoder(store).Decode(context.Background(), body, got),
				"a body with Java's fingerprint, through the memory schema store")
			assertSameValue(t, codec, data, got)
		})
	}
}
