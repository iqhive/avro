package avro_test

// Regression tests for decode bounds, cache identities and reader errors
// found in iqhive/avro after v2.36.0. Each test says what went wrong.

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iqhive/avro/v2"
)

// readNext decodes b with ReadNext and returns the reader's error.
func readNext(schema avro.Schema, b []byte) error {
	r := avro.NewReader(nil, 0)
	r.Reset(b)
	r.ReadNext(schema)
	return r.Error
}

// nullArrays is an array of n arrays of 1024 nulls: three bytes each.
func nullArrays(n int64) []byte {
	b := longs(n)
	for range n {
		b = append(b, longs(1024, 0)...)
	}
	return append(b, 0)
}

// An array whose items take no bytes (a null, an empty record, a fixed of size
// 0, a record whose fields all come from defaults) was grown as its elements
// "arrived", which costs no input: five bytes claiming 2^22 elements made the
// decoder allocate hundreds of megabytes, and a count of 2^40 asked for
// terabytes. Arrays of such arrays did the same in three bytes per 1024
// elements.
func TestFix_ZeroWidthArrayElementsAreBounded(t *testing.T) {
	const limit = 32 << 20
	data := longs(1<<22, 0)
	nested := nullArrays(1 << 14)
	nullArray := avro.MustParse(`{"type":"array","items":"null"}`)
	nullArrayArray := avro.MustParse(`{"type":"array","items":{"type":"array","items":"null"}}`)
	emptyRecord := avro.MustParse(`{"type":"array","items":{"type":"record","name":"bounds.Empty","fields":[]}}`)
	zeroFixed := avro.MustParse(`{"type":"array","items":{"type":"fixed","name":"bounds.Zero","size":0}}`)
	writer := avro.MustParse(`{"type":"array","items":{"type":"record","name":"bounds.Grown","fields":[]}}`)
	reader, err := avro.ParseWithCache(`{"type":"array","items":{"type":"record","name":"bounds.Grown","fields":[
		{"name":"x","type":"long","default":1}]}}`, "", &avro.SchemaCache{})
	require.NoError(t, err)
	resolved, err := avro.NewSchemaCompatibility().Resolve(reader, writer)
	require.NoError(t, err)

	for _, tc := range []struct {
		name   string
		decode func() error
	}{
		{"null into []any", func() error { return avro.Unmarshal(nullArray, data, new([]any)) }},
		{"null into any", func() error { return avro.Unmarshal(nullArray, data, new(any)) }},
		{"empty record into []struct", func() error {
			return avro.Unmarshal(emptyRecord, data, new([]struct{ A, B int64 }))
		}},
		{"zero fixed into any", func() error { return avro.Unmarshal(zeroFixed, data, new(any)) }},
		{"zero fixed into [][0]byte", func() error { return avro.Unmarshal(zeroFixed, data, new([][0]byte)) }},
		{"record of defaults under resolution", func() error {
			return avro.Unmarshal(resolved, data, new([]struct {
				X int64 `avro:"x"`
			}))
		}},
		{"ReadNext empty record", func() error { return readNext(emptyRecord, data) }},
		{"ReadNext null", func() error { return readNext(nullArray, data) }},
		{"ReadArrayCB reading nothing", func() error {
			r := avro.NewReader(nil, 0)
			r.Reset(data)
			var got []any
			r.ReadArrayCB(func(*avro.Reader) bool {
				got = append(got, nil)
				return true
			})
			return r.Error
		}},
		{"arrays of null arrays into any", func() error { return avro.Unmarshal(nullArrayArray, nested, new(any)) }},
		{"ReadNext arrays of null arrays", func() error { return readNext(nullArrayArray, nested) }},
		{"a stream of small values", func() error {
			// Each value is three bytes holding 1024 nulls: the slack is
			// the stream's, not each value's.
			stream := bytes.Repeat(longs(1024, 0), 1<<12)
			dec := avro.NewDecoderForSchema(nullArray, bytes.NewReader(stream))
			for {
				var v []any
				if err := dec.Decode(&v); err != nil {
					if errors.Is(err, io.EOF) {
						return nil
					}
					return err
				}
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var err error
			n := allocatedBy(func() { err = tc.decode() })
			assert.Error(t, err, "the elements claimed take no bytes of the input")
			assert.Less(t, n, uint64(limit), "decoding allocated %d MiB", n>>20)
		})
	}

	// Raising Config.MaxSliceAllocSize, to its own default (1<<48 on 64-bit
	// platforms) or past it, does not raise the bound, and lowering it to
	// 1<<16 does not either.
	for _, size := range []int{math.MaxInt, int(min(1<<48, int64(math.MaxInt))), 1 << 30, 1 << 16} {
		cfg := avro.Config{MaxSliceAllocSize: size}.Freeze()
		var err error
		n := allocatedBy(func() { err = cfg.Unmarshal(nullArray, data, new([]any)) })
		assert.Error(t, err, "MaxSliceAllocSize %d", size)
		assert.Less(t, n, uint64(limit), "MaxSliceAllocSize %d: allocated %d MiB", size, n>>20)
		assert.Error(t, cfg.Unmarshal(nullArray, longs(1<<16, 0), new([]any)), "MaxSliceAllocSize %d", size)
	}

	// A single block, as Java writes one, holds at most 8 for each byte of
	// its value (here its two-byte count) plus 1024.
	var got []any
	require.NoError(t, avro.Unmarshal(nullArray, longs(1040, 0), &got))
	assert.Len(t, got, 1040)
	assert.Error(t, avro.Unmarshal(nullArray, longs(1041, 0), &got))

	// A value earns its credit with its own bytes only: after 100 KB of other
	// values in one stream, which would pay for 800000, a 5-byte value
	// claiming 800000 nulls is refused.
	padded := avro.MustParse(`{"type":"record","name":"bounds.Padded2","fields":[{"name":"s","type":"string"},
		{"name":"n","type":{"type":"array","items":"null"}}]}`)
	type paddedValue struct {
		S string `avro:"s"`
		N []any  `avro:"n"`
	}
	var history bytes.Buffer
	enc := avro.NewEncoderForSchema(padded, &history)
	for range 1000 {
		require.NoError(t, enc.Encode(paddedValue{S: strings.Repeat("x", 100)}))
	}
	stream := append(history.Bytes(), append([]byte{0x00}, longs(800_000, 0)...)...)
	var streamErr error
	n := allocatedBy(func() {
		dec := avro.NewDecoderForSchema(padded, bytes.NewReader(stream))
		for {
			if streamErr = dec.Decode(&paddedValue{}); streamErr != nil {
				return
			}
		}
	})
	assert.NotErrorIs(t, streamErr, io.EOF, "the last value claims more than it pays for")
	assert.Less(t, n, uint64(limit), "decoding the stream allocated %d MiB", n>>20)

	// This library's own output reads back at any length and block length:
	// it writes such blocks at most 63 long, one byte of count each.
	for _, blockLength := range []int{0, 100, 10_000} {
		cfg := avro.Config{BlockLength: blockLength}.Freeze()
		many, err := cfg.Marshal(nullArray, make([]any, 100_000))
		require.NoError(t, err)
		require.NoError(t, cfg.Unmarshal(nullArray, many, &got), "BlockLength %d", blockLength)
		assert.Len(t, got, 100_000)
		r := avro.NewReader(nil, 0, avro.WithReaderConfig(cfg))
		r.Reset(many)
		assert.Len(t, r.ReadNext(nullArray), 100_000)
		assert.NoError(t, r.Error)
	}

	// And in long streams: five values of 1000 nulls, and 100000 events of
	// eight empty marks each, written by avro.Encoder.
	type event struct {
		ID    int64      `avro:"id"`
		Marks []struct{} `avro:"marks"`
	}
	events := avro.MustParse(`{"type":"record","name":"bounds.Event","fields":[{"name":"id","type":"long"},
		{"name":"marks","type":{"type":"array","items":{"type":"record","name":"bounds.Mark","fields":[]}}}]}`)
	for _, tc := range []struct {
		name   string
		schema avro.Schema
		n      int
		value  func(i int) any
		into   func() any
	}{
		{"1000-null values", nullArray, 5, func(int) any { return make([]any, 1000) }, func() any { return new([]any) }},
		{
			"events of eight marks", events, 100_000, func(i int) any { return event{ID: int64(i), Marks: make([]struct{}, 8)} },
			func() any { return new(event) },
		},
	} {
		var buf bytes.Buffer
		enc := avro.NewEncoderForSchema(tc.schema, &buf)
		for i := range tc.n {
			require.NoError(t, enc.Encode(tc.value(i)))
		}
		dec := avro.NewDecoderForSchema(tc.schema, &buf)
		for i := range tc.n {
			require.NoError(t, dec.Decode(tc.into()), "%s: value %d", tc.name, i)
		}
	}
}

// Skipping an array whose items take no bytes took one step per claimed
// element and read no input, so eleven bytes claiming 2^63 nulls kept a core
// busy for centuries: for a field the Go struct lacks, and for a writer field
// the reader schema lacks. Decoding such an array into []struct{}, whose
// elements take no memory either, spun the same way.
func TestFix_ZeroWidthArrayDecodeTimeIsBounded(t *testing.T) {
	inChild(t, func() {
		writer := avro.MustParse(`{"type":"record","name":"bounds.Padded","fields":[
			{"name":"pad","type":{"type":"array","items":"null"}},{"name":"a","type":"int"}]}`)
		reader, err := avro.ParseWithCache(`{"type":"record","name":"bounds.Padded","fields":[{"name":"a","type":"int"}]}`,
			"", &avro.SchemaCache{})
		if err != nil {
			childFail(err.Error())
		}
		resolved, err := avro.NewSchemaCompatibility().Resolve(reader, writer)
		if err != nil {
			childFail(err.Error())
		}
		// The largest count the platform reads, 2^63-1 on 64-bit ones.
		count := int64(math.MaxInt)
		data := append(longs(count, 0), longs(42)...)
		type withoutPad struct {
			A int32 `avro:"a"`
		}
		nullArray := avro.MustParse(`{"type":"array","items":"null"}`)

		for _, tc := range []struct {
			name   string
			decode func() error
		}{
			{"skip a field the struct lacks", func() error {
				var v withoutPad
				if err := avro.Unmarshal(writer, data, &v); err != nil || v.A != 42 {
					return fmt.Errorf("a=%d, err=%v", v.A, err)
				}
				return nil
			}},
			{"skip a writer field the reader lacks", func() error {
				var v withoutPad
				if err := avro.Unmarshal(resolved, data, &v); err != nil || v.A != 42 {
					return fmt.Errorf("a=%d, err=%v", v.A, err)
				}
				return nil
			}},
			{"skip a writer field the reader lacks, into a map", func() error {
				var v map[string]any
				if err := avro.Unmarshal(resolved, data, &v); err != nil || v["a"] != 42 {
					return fmt.Errorf("a=%v, err=%v", v["a"], err)
				}
				return nil
			}},
			{"decode into []struct{}", func() error {
				if err := avro.Unmarshal(nullArray, longs(count, 0), new([]struct{})); err == nil {
					return errors.New("no error")
				}
				return nil
			}},
		} {
			done := make(chan error, 1)
			go func() { done <- tc.decode() }()
			select {
			case err := <-done:
				if err != nil {
					childFail(tc.name + ": " + err.Error())
				}
			case <-time.After(5 * time.Second):
				childFail(fmt.Sprintf("%s: still decoding %d bytes after 5s", tc.name, len(data)))
			}
		}
	})
}

// wrappedSchema is a record whose field holds the record again inside depth
// arrays (or maps), behind a null union.
func wrappedSchema(name, kind string, depth int) string {
	inner := `"` + name + `"`
	for range depth {
		if kind == "array" {
			inner = `{"type":"array","items":` + inner + `}`
		} else {
			inner = `{"type":"map","values":` + inner + `}`
		}
	}
	return `{"type":"record","name":"` + name + `","fields":[{"name":"next","type":["null",` + inner + `]}]}`
}

// wrappedData is levels records of wrappedSchema, each holding the next in
// one-element blocks (a map's key is empty), and the last holding null.
func wrappedData(kind string, levels, depth int) []byte {
	open := []byte{0x02}
	if kind == "map" {
		open = []byte{0x02, 0x00}
	}
	var b bytes.Buffer
	for range levels {
		b.WriteByte(0x02) // union branch 1
		for range depth {
			b.Write(open)
		}
	}
	b.WriteByte(0x00) // the last record's null
	b.Write(bytes.Repeat([]byte{0x00}, levels*depth))
	return b.Bytes()
}

type deepArrayNode struct {
	Next *[][][][][][][][]deepArrayNode `avro:"next"`
}

type deepMapNode struct {
	Next *map[string]map[string]map[string]map[string]map[string]map[string]map[string]map[string]deepMapNode `avro:"next"`
}

// deepArrayValue is levels records of deepArrayNode, each in the one element
// of eight nested arrays.
func deepArrayValue(levels int) *deepArrayNode {
	n := &deepArrayNode{}
	for range levels {
		next := [][][][][][][][]deepArrayNode{{{{{{{{*n}}}}}}}}
		n = &deepArrayNode{Next: &next}
	}
	return n
}

// deepMapValue is levels records of deepMapNode, each in the one entry of eight
// nested maps.
func deepMapValue(levels int) *deepMapNode {
	n := &deepMapNode{}
	for range levels {
		next := map[string]map[string]map[string]map[string]map[string]map[string]map[string]map[string]deepMapNode{
			"": {"": {"": {"": {"": {"": {"": {"": *n}}}}}}},
		}
		n = &deepMapNode{Next: &next}
	}
	return n
}

// Config.MaxDepth counted records only, but each array or map between two
// records is a level of Go recursion too: 9999 records under the default
// limit, each inside 100 arrays, recursed a million levels and overflowed the
// goroutine stack, a fatal error, on decode, skip, ReadNext and encode.
func TestFix_MaxDepthCountsArraysAndMaps(t *testing.T) {
	inChild(t, func() {
		debug.SetMaxStack(64 << 20)
		const levels, depth = 9999, 100
		for _, kind := range []string{"array", "map"} {
			schema := avro.MustParse(wrappedSchema("bounds.Wrapped"+kind, kind, depth))
			data := wrappedData(kind, levels, depth)
			checks := []struct {
				name string
				err  error
			}{
				{"into any", avro.Unmarshal(schema, data, new(any))},
				{"skipped", avro.Unmarshal(schema, data, &struct{}{})},
				{"ReadNext", readNext(schema, data)},
			}
			for _, c := range checks {
				if c.err == nil || !strings.Contains(c.err.Error(), "MaxDepth") {
					childFail(fmt.Sprintf("%s %s: %v", kind, c.name, c.err))
				}
			}
		}

		typedArray := avro.MustParse(wrappedSchema("bounds.TypedArray", "array", 8))
		typedMap := avro.MustParse(wrappedSchema("bounds.TypedMap", "map", 8))
		for _, c := range []struct {
			name string
			err  error
		}{
			{"typed arrays", avro.Unmarshal(typedArray, wrappedData("array", levels, 8), &deepArrayNode{})},
			{"typed maps", avro.Unmarshal(typedMap, wrappedData("map", levels, 8), &deepMapNode{})},
			{"encode arrays", func() error { _, err := avro.Marshal(typedArray, deepArrayValue(levels)); return err }()},
			{"encode maps", func() error { _, err := avro.Marshal(typedMap, deepMapValue(levels)); return err }()},
		} {
			if c.err == nil || !strings.Contains(c.err.Error(), "MaxDepth") {
				childFail(fmt.Sprintf("%s: %v", c.name, c.err))
			}
		}

		// Arrays and maps a few levels deep still decode and encode.
		for _, c := range []struct {
			schema  avro.Schema
			v, back any
		}{
			{typedArray, deepArrayValue(3), &deepArrayNode{}},
			{typedMap, deepMapValue(3), &deepMapNode{}},
		} {
			b, err := avro.Marshal(c.schema, c.v)
			if err != nil {
				childFail("encode a shallow value: " + err.Error())
			}
			if err := avro.Unmarshal(c.schema, b, c.back); err != nil || !reflect.DeepEqual(c.back, c.v) {
				childFail(fmt.Sprintf("decode a shallow value: %v", err))
			}
		}
	})
}

// Each record, array and map decoder (and encoder) adds its field name or Go
// type to an error on the way out, and each level built a new message holding
// every level below it: an error at the bottom of data nested 5000 deep made
// a 125 KB message and took hundreds of megabytes and most of a second.
func TestFix_DeepErrorContextIsBounded(t *testing.T) {
	const levels, limit = 5000, 16 << 20
	tree := avro.MustParse(`{"type":"record","name":"bounds.Tree","fields":[
		{"name":"l","type":["bounds.Tree","null"]},{"name":"r","type":["bounds.Tree","null"]}]}`)
	// levels records down the left branch, then a union index that is out
	// of range.
	treeData := append(make([]byte, levels), 0x7f)
	listOfArrays := avro.MustParse(wrappedSchema("bounds.ErrWrapped", "array", 1))
	// levels records, each in a one-element array, then a bad union index.
	var arrayData []byte
	for range levels / 2 {
		arrayData = append(arrayData, 0x02, 0x02)
	}
	arrayData = append(arrayData, 0x7f)
	listOfMaps := avro.MustParse(wrappedSchema("bounds.ErrMapped", "map", 1))
	// levels records, each in a one-entry map, then a bad union index.
	var mapData []byte
	for range levels / 2 {
		mapData = append(mapData, 0x02, 0x02, 0x00)
	}
	mapData = append(mapData, 0x7f)
	type tnode struct {
		L *tnode `avro:"l"`
		R *tnode `avro:"r"`
	}
	// A record whose last level lacks its required field.
	var value any = map[string]any{}
	for range levels {
		value = map[string]any{"l": map[string]any{"bounds.Tree": value}, "r": nil}
	}

	for _, tc := range []struct {
		name string
		run  func() error
	}{
		{"into a map", func() error { return avro.Unmarshal(tree, treeData, new(map[string]any)) }},
		{"into any", func() error { return avro.Unmarshal(tree, treeData, new(any)) }},
		{"maps into any", func() error { return avro.Unmarshal(listOfMaps, mapData, new(any)) }},
		{"into a struct", func() error { return avro.Unmarshal(tree, treeData, &tnode{}) }},
		{"arrays into any", func() error { return avro.Unmarshal(listOfArrays, arrayData, new(any)) }},
		{"encode a map", func() error { _, err := avro.Marshal(tree, value); return err }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var err error
			n := allocatedBy(func() { err = tc.run() })
			require.Error(t, err)
			assert.Less(t, len(err.Error()), 8<<10, "a %d-byte error", len(err.Error()))
			assert.Less(t, n, uint64(limit), "allocated %d MiB", n>>20)
		})
	}

	// A path of 32 levels keeps every one: the cause's own wraps ("reading
	// long: ") do not count. Past 32 the outer levels are cut, and a "..."
	// says so.
	chain := avro.MustParse(`{"type":"record","name":"bounds.Chain","fields":[
		{"name":"next","type":["null","bounds.Chain"]},{"name":"v","type":"long"}]}`)
	for _, tc := range []struct {
		records, nexts int
		cut            bool
	}{{31, 30, false}, {32, 31, false}, {33, 31, true}, {200, 31, true}} {
		// The last record's long is cut short.
		data := append(bytes.Repeat([]byte{0x02}, tc.records-1), 0x00, 0x80)
		err := avro.Unmarshal(chain, data, &chainNode{})
		require.Error(t, err)
		assert.Equal(t, tc.nexts, strings.Count(err.Error(), "Next: "), "%d records: %s", tc.records, err)
		assert.Contains(t, err.Error(), "V: reading long: ")
		assert.Equal(t, tc.cut, strings.HasPrefix(err.Error(), "...: "), "%d records: %s", tc.records, err)
	}

	// A shallow error keeps its whole path.
	shallow := avro.MustParse(`{"type":"record","name":"bounds.Outer","fields":[
		{"name":"inner","type":{"type":"record","name":"bounds.Inner","fields":[{"name":"n","type":"long"}]}}]}`)
	type inner struct {
		N int64 `avro:"n"`
	}
	type outer struct {
		Inner inner `avro:"inner"`
	}
	err := avro.Unmarshal(shallow, []byte{0x80}, &outer{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Inner: N: ")
}

type chainNode struct {
	Next *chainNode `avro:"next"`
	V    int64      `avro:"v"`
}

// newParsed parses s into a schema cache of its own, so that two schemas with
// one name can be told apart.
func newParsed(t *testing.T, s string) avro.Schema {
	t.Helper()
	schema, err := avro.ParseWithCache(s, "", &avro.SchemaCache{})
	require.NoError(t, err)
	return schema
}

// Decoders are cached under the schema's cache identity and the Go type. For a
// resolved record holding a resolved enum, the identity covered the record's
// field defaults and the writer's schema but not the enum's default, so two
// readers that differ only in that default shared one decoder: the second read
// a writer-only symbol as the first reader's default.
func TestFix_DecoderCacheKeepsEnumDefaultsApart(t *testing.T) {
	cfg := avro.Config{}.Freeze()
	writer := newParsed(t, `{"type":"record","name":"R","fields":[
		{"name":"e","type":{"type":"enum","name":"E","symbols":["A","B","C"]}},
		{"name":"es","type":{"type":"array","items":"E"}}]}`)
	reader := func(def string) avro.Schema {
		schema := newParsed(t, `{"type":"record","name":"R","fields":[
			{"name":"e","type":{"type":"enum","name":"E","symbols":["A","B"],"default":"`+def+`"}},
			{"name":"es","type":{"type":"array","items":"E"}}]}`)
		resolved, err := avro.NewSchemaCompatibility().Resolve(schema, writer)
		require.NoError(t, err)
		return resolved
	}
	type rec struct {
		E  string   `avro:"e"`
		Es []string `avro:"es"`
	}
	data, err := cfg.Marshal(writer, rec{E: "C", Es: []string{"C"}})
	require.NoError(t, err)

	resA, resB := reader("A"), reader("B")
	assert.NotEqual(t, resA.CacheFingerprint(), resB.CacheFingerprint())
	var a, b rec
	require.NoError(t, cfg.Unmarshal(resA, data, &a))
	require.NoError(t, cfg.Unmarshal(resB, data, &b))
	assert.Equal(t, rec{E: "A", Es: []string{"A"}}, a)
	assert.Equal(t, rec{E: "B", Es: []string{"B"}}, b, "the second reader's default")
}

// The struct decoder matches a schema field to a Go field by the field's
// aliases too, but aliases were in neither the fingerprint nor the cache
// identity: a schema whose field has no alias, decoded first, left its decoder
// (which skips the field) in the cache for the schema whose field has one.
func TestFix_DecoderCacheKeepsFieldAliasesApart(t *testing.T) {
	cfg := avro.Config{}.Freeze()
	plain := newParsed(t, `{"type":"record","name":"R","fields":[{"name":"renamed","type":"long"}]}`)
	aliased := newParsed(t, `{"type":"record","name":"R","fields":[{"name":"renamed","type":"long","aliases":["old"]}]}`)
	type target struct {
		Old int64 `avro:"old"`
	}
	data := longs(42)

	assert.NotEqual(t, plain.CacheFingerprint(), aliased.CacheFingerprint())
	var first, second target
	require.NoError(t, cfg.Unmarshal(plain, data, &first))
	require.NoError(t, cfg.Unmarshal(aliased, data, &second))
	assert.Zero(t, first.Old, "the plain schema has no field old")
	assert.Equal(t, int64(42), second.Old, "the aliased schema fills old")
}

// failingReader returns its data, then err.
type failingReader struct {
	data []byte
	err  error
}

func (f *failingReader) Read(p []byte) (int, error) {
	if len(f.data) == 0 {
		return 0, f.err
	}
	n := copy(p, f.data)
	f.data = f.data[n:]
	return n, nil
}

// Decoder.Decode returned io.EOF, the clean end of the stream, whenever its
// reader failed between two values, whatever the reader's error was: a reset
// connection read as a complete stream.
func TestFix_DecoderReportsReadErrorBetweenValues(t *testing.T) {
	boom := errors.New("connection reset")
	dec := avro.NewDecoderForSchema(avro.MustParse(`"long"`), &failingReader{data: longs(1), err: boom})
	var v int64
	require.NoError(t, dec.Decode(&v))
	err := dec.Decode(&v)
	assert.ErrorIs(t, err, boom)
	assert.NotErrorIs(t, err, io.EOF)

	// The end of the input between values is still io.EOF.
	dec = avro.NewDecoderForSchema(avro.MustParse(`"long"`), bytes.NewReader(longs(1)))
	require.NoError(t, dec.Decode(&v))
	assert.Equal(t, io.EOF, dec.Decode(&v))
}

// A reader error inside a string, bytes, fixed, float, double or boolean was
// replaced by io.ErrUnexpectedEOF, so a transport error read as truncated
// input; ints and longs kept it. An input that ends inside a value still
// reports io.ErrUnexpectedEOF.
func TestFix_DecoderKeepsReadErrorInsideValue(t *testing.T) {
	boom := errors.New("connection reset")
	for _, tc := range []struct {
		typ  string
		data []byte
	}{
		{`"boolean"`, nil},
		{`"float"`, []byte{1, 2}},
		{`"double"`, []byte{1, 2}},
		{`"string"`, []byte{0x14, 'a'}},
		{`"bytes"`, []byte{0x14, 'a'}},
		{`{"type":"fixed","name":"F8","size":8}`, []byte{1, 2}},
		{`"long"`, []byte{0x80}},
	} {
		t.Run(tc.typ, func(t *testing.T) {
			// A long first, so that the value under test is never at the start.
			schema := newParsed(t, `{"type":"record","name":"Mid","fields":[{"name":"a","type":"long"},{"name":"b","type":`+tc.typ+`}]}`)
			data := append(longs(1), tc.data...)

			dec := avro.NewDecoderForSchema(schema, &failingReader{data: data, err: boom})
			assert.ErrorIs(t, dec.Decode(new(any)), boom, "Decoder.Decode")

			r := avro.NewReader(&failingReader{data: data, err: boom}, 16)
			r.ReadNext(schema)
			assert.ErrorIs(t, r.Error, boom, "ReadNext")

			dec = avro.NewDecoderForSchema(schema, bytes.NewReader(data))
			assert.ErrorIs(t, dec.Decode(new(any)), io.ErrUnexpectedEOF, "Decoder.Decode of truncated input")
			assert.ErrorIs(t, avro.Unmarshal(schema, data, new(any)), io.ErrUnexpectedEOF, "Unmarshal of truncated input")
		})
	}
}

// A field the Go struct lacks is skipped, and for a resolved schema the skip
// read it in the reader's shape, not the writer's: a reader-only field with a
// default, inside a skipped record, was skipped as if the writer had written
// it, and an int or long read as a float or double was skipped as four or
// eight bytes instead of a varint. The fields after it were misread or failed.
// Arrays and maps are written both with and without block sizes: a skip uses
// the sizes when they are there, as Java's writer leaves them out.
func TestFix_SkipResolvedFieldAsWritten(t *testing.T) {
	noSizes := avro.Config{DisableBlockSizeHeader: true}.Freeze()
	rec := func(name, fields string) string {
		return `{"type":"record","name":"` + name + `","fields":[` + fields + `]}`
	}
	// The writer's and the reader's record R hold the field f and then z.
	withF := func(f string) string {
		return rec("R", `{"name":"f","type":`+f+`},{"name":"z","type":"long"}`)
	}
	inner := func(fields string) string { return rec("I", fields) }
	tree := func(extra string) string {
		return rec("N", `{"name":"v","type":"int"},{"name":"next","type":["null","N"]}`+extra)
	}
	for _, tc := range []struct {
		name           string
		writer, reader string
		f              any
	}{
		{"int as long", `"int"`, `"long"`, 7},
		{"int as float", `"int"`, `"float"`, 7},
		{"int as double", `"int"`, `"double"`, 7},
		{"long as float", `"long"`, `"float"`, int64(7)},
		{"long as double", `"long"`, `"double"`, int64(7)},
		{"float as double", `"float"`, `"double"`, float32(7)},
		{"string as bytes", `"string"`, `"bytes"`, "seven"},
		{"bytes as string", `"bytes"`, `"string"`, []byte("seven")},
		{
			"record with a reader-only field",
			inner(`{"name":"a","type":"int"}`), inner(`{"name":"a","type":"int"},{"name":"b","type":"long","default":5}`),
			map[string]any{"a": 7},
		},
		{
			"record with a writer-only field",
			inner(`{"name":"a","type":"int"},{"name":"c","type":"string"}`), inner(`{"name":"a","type":"int"}`),
			map[string]any{"a": 7, "c": "x"},
		},
		{
			"record with a promoted field",
			inner(`{"name":"a","type":"int"}`), inner(`{"name":"a","type":"double"}`),
			map[string]any{"a": 7},
		},
		{
			"enum with a default",
			`{"type":"enum","name":"E","symbols":["A","B","C"]}`, `{"type":"enum","name":"E","symbols":["A","B"],"default":"A"}`,
			"C",
		},
		{"int as a union branch", `"int"`, `["null","double"]`, 7},
		{"union as one type", `["int","long"]`, `"double"`, map[string]any{"long": int64(7)}},
		{"union of promotions", `["null","int"]`, `["null","float"]`, map[string]any{"int": 7}},
		{"array of promotions", `{"type":"array","items":"int"}`, `{"type":"array","items":"double"}`, []any{1, 2, 3}},
		{"map of promotions", `{"type":"map","values":"long"}`, `{"type":"map","values":"float"}`, map[string]any{"k": int64(1)}},
		{
			"array of records with reader-only fields",
			`{"type":"array","items":` + inner(`{"name":"a","type":"int"}`) + `}`,
			`{"type":"array","items":` + inner(`{"name":"a","type":"int"},{"name":"b","type":"string","default":"x"}`) + `}`,
			[]any{map[string]any{"a": 1}, map[string]any{"a": 2}},
		},
		{
			"map of records with reader-only fields",
			`{"type":"map","values":` + inner(`{"name":"a","type":"int"}`) + `}`,
			`{"type":"map","values":` + inner(`{"name":"a","type":"int"},{"name":"b","type":"long","default":0}`) + `}`,
			map[string]any{"k": map[string]any{"a": 1}},
		},
		{
			"recursive record with a reader-only field",
			tree(``), tree(`,{"name":"w","type":"double","default":0}`),
			map[string]any{"v": 1, "next": map[string]any{"N": map[string]any{"v": 2, "next": nil}}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			writer := newParsed(t, withF(tc.writer))
			reader := newParsed(t, withF(tc.reader))
			resolved, err := avro.NewSchemaCompatibility().Resolve(reader, writer)
			require.NoError(t, err)
			for _, cfg := range []avro.API{avro.DefaultConfig, noSizes} {
				data, err := cfg.Marshal(writer, map[string]any{"f": tc.f, "z": int64(42)})
				require.NoError(t, err)

				var full map[string]any
				require.NoError(t, avro.Unmarshal(resolved, data, &full), "decoding every field")
				require.Equal(t, int64(42), full["z"])

				var onlyZ struct {
					Z int64 `avro:"z"`
				}
				require.NoError(t, avro.Unmarshal(resolved, data, &onlyZ), "skipping f")
				assert.Equal(t, int64(42), onlyZ.Z)
			}
		})
	}
}
