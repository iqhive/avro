package avro_test

import (
	"bytes"
	"encoding/json"
	"math"
	"math/big"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/iqhive/avro/v2"
)

// fuzzRichSchema reaches most decoder paths: every primitive, unions into
// interfaces and pointers, arrays and maps of records and unions, enums,
// fixed, decimals, the time types, and a recursive record.
const fuzzRichSchema = `{"type":"record","name":"Rich","namespace":"fz","fields":[
	{"name":"i","type":"int"},
	{"name":"l","type":"long"},
	{"name":"f","type":"float"},
	{"name":"d","type":"double"},
	{"name":"b","type":"boolean"},
	{"name":"s","type":"string"},
	{"name":"by","type":"bytes"},
	{"name":"u","type":["null","int","string",{"type":"record","name":"In","fields":[{"name":"x","type":"long"}]}]},
	{"name":"nu","type":["null","In"]},
	{"name":"a","type":{"type":"array","items":"In"}},
	{"name":"ai","type":{"type":"array","items":"int"}},
	{"name":"m","type":{"type":"map","values":["null","double"]}},
	{"name":"e","type":{"type":"enum","name":"E","symbols":["A","B","C"]}},
	{"name":"x","type":{"type":"fixed","name":"X","size":4}},
	{"name":"dec","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":3}},
	{"name":"fdec","type":{"type":"fixed","name":"FD","size":6,"logicalType":"decimal","precision":12,"scale":2}},
	{"name":"ts","type":{"type":"long","logicalType":"timestamp-micros"}},
	{"name":"date","type":{"type":"int","logicalType":"date"}},
	{"name":"tm","type":{"type":"int","logicalType":"time-millis"}},
	{"name":"dur","type":{"type":"fixed","name":"Dur","size":12,"logicalType":"duration"}},
	{"name":"rec","type":["null",{"type":"record","name":"Node","fields":[{"name":"v","type":"int"},{"name":"next","type":["null","Node"]}]}]}
]}`

type fuzzIn struct {
	X int64 `avro:"x"`
}

type fuzzNode struct {
	V    int32     `avro:"v"`
	Next *fuzzNode `avro:"next"`
}

type fuzzRich struct {
	I    int32                `avro:"i"`
	L    int64                `avro:"l"`
	F    float32              `avro:"f"`
	D    float64              `avro:"d"`
	B    bool                 `avro:"b"`
	S    string               `avro:"s"`
	By   []byte               `avro:"by"`
	U    any                  `avro:"u"`
	NU   *fuzzIn              `avro:"nu"`
	A    []fuzzIn             `avro:"a"`
	AI   []int16              `avro:"ai"`
	M    map[string]*float64  `avro:"m"`
	E    string               `avro:"e"`
	X    [4]byte              `avro:"x"`
	Dec  *big.Rat             `avro:"dec"`
	FDec *big.Rat             `avro:"fdec"`
	TS   time.Time            `avro:"ts"`
	Date time.Time            `avro:"date"`
	TM   time.Duration        `avro:"tm"`
	Dur  avro.LogicalDuration `avro:"dur"`
	Rec  *fuzzNode            `avro:"rec"`
}

func fuzzRichSeeds(f *testing.F, schema avro.Schema) {
	f.Helper()
	one := 1.5
	seeds := []fuzzRich{
		{E: "A", Dec: big.NewRat(0, 1), FDec: big.NewRat(0, 1)},
		{
			I: -7, L: 1 << 40, F: 1.25, D: -2.5, B: true, S: "héllo", By: []byte{0, 1, 2},
			U: "str", NU: &fuzzIn{X: 3}, A: []fuzzIn{{X: 1}, {X: -2}}, AI: []int16{1, -1, 300},
			M: map[string]*float64{"k": &one, "n": nil}, E: "C", X: [4]byte{1, 2, 3, 4},
			Dec: big.NewRat(-12345, 1000), FDec: big.NewRat(99, 100),
			TS: time.Date(1969, 7, 20, 20, 17, 0, 0, time.UTC), Date: time.Date(2026, 11, 22, 0, 0, 0, 0, time.UTC),
			TM: 3 * time.Second, Dur: avro.LogicalDuration{Months: 1, Days: 2, Milliseconds: 3},
			Rec: &fuzzNode{V: 1, Next: &fuzzNode{V: 2}},
		},
	}
	for _, s := range seeds {
		data, err := avro.Marshal(schema, s)
		if err != nil {
			f.Fatalf("marshal seed: %v", err)
		}
		f.Add(data)
	}
	f.Add([]byte{})
}

// FuzzReusedTarget decodes each input into a fresh value and into a value
// that already holds a decoded message, and wants the same result: a decoder
// must not leave the previous message behind, nor write through it.
func FuzzReusedTarget(f *testing.F) {
	schema := avro.MustParse(fuzzRichSchema)
	fuzzRichSeeds(f, schema)
	prev, err := avro.Marshal(schema, fuzzRich{
		By: []byte("prev"), U: 7, NU: &fuzzIn{X: 9}, A: []fuzzIn{{X: 1}, {X: 2}, {X: 3}}, AI: []int16{4, 5},
		M: map[string]*float64{"old": nil}, E: "B", Dec: big.NewRat(5, 1), FDec: big.NewRat(6, 1),
		Rec: &fuzzNode{V: 8},
	})
	if err != nil {
		f.Fatal(err)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		var fresh fuzzRich
		freshErr := avro.Unmarshal(schema, data, &fresh)

		var reused fuzzRich
		if err := avro.Unmarshal(schema, prev, &reused); err != nil {
			t.Fatal(err)
		}
		reusedErr := avro.Unmarshal(schema, data, &reused)

		if (freshErr == nil) != (reusedErr == nil) {
			t.Fatalf("fresh error %v, reused error %v", freshErr, reusedErr)
		}
		if freshErr != nil {
			return
		}
		// Maps merge into a reused map, by design: compare without them. NaN
		// is not equal to itself, so compare floats by their bits.
		fresh.M, reused.M = nil, nil
		if math.Float32bits(fresh.F) == math.Float32bits(reused.F) {
			fresh.F, reused.F = 0, 0
		}
		if math.Float64bits(fresh.D) == math.Float64bits(reused.D) {
			fresh.D, reused.D = 0, 0
		}
		if !reflect.DeepEqual(fresh, reused) {
			t.Fatalf("decoding into a reused value differs:\nfresh  %+v\nreused %+v", fresh, reused)
		}
	})
}

// FuzzGenericRoundTrip decodes each input into `any`, encodes the result
// again and decodes that, and wants the same value back; and wants ReadNext
// to agree on whether the input is valid.
func FuzzGenericRoundTrip(f *testing.F) {
	schema := avro.MustParse(fuzzRichSchema)
	fuzzRichSeeds(f, schema)

	f.Fuzz(func(t *testing.T, data []byte) {
		var got any
		err := avro.Unmarshal(schema, data, &got)

		r := avro.NewReader(nil, 0).Reset(data)
		_ = r.ReadNext(schema)
		if (err == nil) != (r.Error == nil) {
			t.Fatalf("Unmarshal error %v, ReadNext error %v", err, r.Error)
		}
		if err != nil {
			return
		}

		again, err := avro.Marshal(schema, got)
		if err != nil {
			// A decoder does not hold a decimal to its precision, an encoder
			// does: that difference is by design.
			if strings.Contains(err.Error(), "precision") {
				return
			}
			t.Fatalf("encoding what was decoded: %v\nvalue %#v", err, got)
		}
		var back any
		if err := avro.Unmarshal(schema, again, &back); err != nil {
			t.Fatalf("decoding what was encoded: %v", err)
		}
		if !reflect.DeepEqual(withoutNaN(got), withoutNaN(back)) {
			t.Fatalf("round trip differs:\nfirst  %#v\nsecond %#v", got, back)
		}
	})
}

// FuzzSchemaReparse parses each input as a schema and wants its canonical and
// full JSON forms to parse back to the same schema, and the schema to be
// compatible with and resolvable against itself.
func FuzzSchemaReparse(f *testing.F) {
	for _, s := range []string{
		fuzzRichSchema,
		`"int"`,
		`["null","string"]`,
		`{"type":"enum","name":"E","symbols":["A","B"],"default":"A"}`,
		`{"type":"record","name":"R","fields":[{"name":"a","type":"int","default":1},{"name":"b","type":["null","R"],"default":null}]}`,
		`{"type":"fixed","name":"F","size":16,"logicalType":"decimal","precision":20,"scale":4}`,
		`{"type":"map","values":{"type":"array","items":{"type":"bytes","logicalType":"decimal","precision":5,"scale":2}}}`,
	} {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, in string) {
		cache := &avro.SchemaCache{}
		s, err := avro.ParseWithCache(in, "", cache)
		if err != nil {
			return
		}
		// A named type in the null namespace inside a namespaced one prints as
		// a bare name, which parses back into the enclosing namespace: an
		// ambiguity of the specification's canonical form, not of this
		// library.
		if nullNamespaceInside(s, "", map[string]bool{}) {
			return
		}

		canonical, err := avro.ParseWithCache(s.String(), "", &avro.SchemaCache{})
		if err != nil {
			t.Fatalf("the canonical form %s does not parse: %v", s.String(), err)
		}
		if canonical.Fingerprint() != s.Fingerprint() {
			t.Fatalf("the canonical form %s parses to %s", s.String(), canonical.String())
		}

		full, err := json.Marshal(s)
		if err != nil {
			t.Fatalf("MarshalJSON: %v", err)
		}
		reparsed, err := avro.ParseWithCache(string(full), "", &avro.SchemaCache{})
		if err != nil {
			t.Fatalf("the full form %s does not parse: %v", full, err)
		}
		if reparsed.Fingerprint() != s.Fingerprint() {
			t.Fatalf("the full form %s parses to %s, want %s", full, reparsed.String(), s.String())
		}
		again, err := json.Marshal(reparsed)
		if err != nil || !bytes.Equal(again, full) {
			t.Fatalf("the full form is not stable:\n%s\n%s (%v)", full, again, err)
		}

		compat := avro.NewSchemaCompatibility()
		if err := compat.Compatible(s, s); err != nil {
			t.Fatalf("a schema is not compatible with itself: %v", err)
		}
		if _, err := compat.Resolve(s, s); err != nil {
			t.Fatalf("a schema does not resolve against itself: %v", err)
		}
	})
}

// withoutNaN returns v with every NaN float in its maps and slices replaced
// by a marker, since NaN is not equal to itself.
func withoutNaN(v any) any {
	switch x := v.(type) {
	case float32:
		if x != x {
			return "NaN"
		}
	case float64:
		if x != x {
			return "NaN"
		}
	case map[string]any:
		m := make(map[string]any, len(x))
		for k, e := range x {
			m[k] = withoutNaN(e)
		}
		return m
	case []any:
		s := make([]any, len(x))
		for i, e := range x {
			s[i] = withoutNaN(e)
		}
		return s
	}
	return v
}

// nullNamespaceInside reports whether s holds a named type in the null
// namespace inside a named type that has one.
func nullNamespaceInside(s avro.Schema, enclosing string, seen map[string]bool) bool {
	if n, ok := s.(avro.NamedSchema); ok {
		if seen[n.FullName()] {
			return false
		}
		seen[n.FullName()] = true
		if n.Namespace() == "" && enclosing != "" {
			return true
		}
		enclosing = n.Namespace()
	}
	switch x := s.(type) {
	case *avro.RecordSchema:
		for _, f := range x.Fields() {
			if nullNamespaceInside(f.Type(), enclosing, seen) {
				return true
			}
		}
	case *avro.ArraySchema:
		return nullNamespaceInside(x.Items(), enclosing, seen)
	case *avro.MapSchema:
		return nullNamespaceInside(x.Values(), enclosing, seen)
	case *avro.UnionSchema:
		for _, t := range x.Types() {
			if nullNamespaceInside(t, enclosing, seen) {
				return true
			}
		}
	}
	return false
}
