package avro_test

// Regression tests for defects found in iqhive/avro after the fork from
// hamba/avro and iskorotkov/avro. Each test says what went wrong.

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"os"
	"os/exec"
	"reflect"
	"regexp"
	"runtime"
	"runtime/debug"
	"strconv"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iqhive/avro/v2"
)

// inChild runs fn in a fresh copy of the test binary and fails t unless that
// copy exits cleanly. A fault or a stack overflow ends the whole process and
// recover cannot catch it, so a test that would otherwise kill the test binary
// runs its body here instead. fn reports a wrong result by printing it and
// exiting non-zero.
func inChild(t *testing.T, fn func()) {
	t.Helper()
	const env = "AVRO_TEST_IN_CHILD"
	if os.Getenv(env) == t.Name() {
		fn()
		os.Exit(0)
	}
	//nolint:gosec // It runs this test binary again, with a pattern made from the test's own name.
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^"+regexp.QuoteMeta(t.Name())+"$", "-test.count=1", "-test.timeout=5m")
	cmd.Env = append(os.Environ(), env+"="+t.Name())
	out, err := cmd.CombinedOutput()
	if err != nil {
		if len(out) > 4000 {
			out = out[:4000]
		}
		t.Fatalf("%s failed in a child process (%v):\n%s", t.Name(), err, out)
	}
}

// childFail prints a wrong result in a child process and ends it.
func childFail(msg string) {
	fmt.Println(msg)
	os.Exit(1)
}

// longs encodes vs as Avro longs, one after another.
func longs(vs ...int64) []byte {
	w := avro.NewWriter(nil, 0)
	for _, v := range vs {
		w.WriteLong(v)
	}
	return bytes.Clone(w.Buffer())
}

// noPanic runs fn and fails t if it panics.
func noPanic(t *testing.T, what string, fn func()) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("%s panicked: %v", what, r)
		}
	}()
	fn()
}

// A union decoded into a struct held by value, not through a pointer, was
// decoded as if the struct's first word were a pointer to the struct: the
// record went to a fresh allocation whose address was stored over the first
// field, and a non-zero first field was written through as an address. A null
// left whatever the struct held.
func TestFix_NullableUnionIntoStructValue(t *testing.T) {
	type inner struct {
		A int64  `avro:"a"`
		B string `avro:"b"`
	}
	type outer struct {
		In inner `avro:"in"`
	}
	schema := avro.MustParse(`{"type":"record","name":"Outer","fields":[{"name":"in","type":["null",
		{"type":"record","name":"Inner","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"}]}]}]}`)

	var got outer
	require.NoError(t, avro.Unmarshal(schema, []byte{0x02, 0x54, 0x0a, 'h', 'e', 'l', 'l', 'o'}, &got))
	assert.Equal(t, inner{A: 42, B: "hello"}, got.In)

	got = outer{In: inner{B: "stale"}}
	require.NoError(t, avro.Unmarshal(schema, []byte{0x00}, &got))
	assert.Equal(t, inner{}, got.In, "a null into a struct value leaves its zero value")
}

// time.Time is a struct, so a timestamp under a nullable union decoded into a
// time.Time value took the same wrong path and came back as garbage.
func TestFix_NullableUnionIntoTimeValue(t *testing.T) {
	type rec struct {
		T time.Time `avro:"t"`
	}
	schema := avro.MustParse(`{"type":"record","name":"TimeRec","fields":[{"name":"t","type":["null",
		{"type":"long","logicalType":"timestamp-millis"}]}]}`)
	want := time.Date(2026, 11, 22, 1, 2, 3, 4e6, time.UTC)
	data := append([]byte{0x02}, longs(want.UnixMilli())...)

	var got rec
	require.NoError(t, avro.Unmarshal(schema, data, &got))
	assert.True(t, want.Equal(got.T), "got %v, want %v", got.T, want)
}

type valueUnion struct {
	Int *int
	Str *string
}

func (u *valueUnion) FromAny(payload any) error {
	switch v := payload.(type) {
	case int:
		u.Int = &v
	case string:
		u.Str = &v
	default:
		return fmt.Errorf("unexpected %T", payload)
	}
	return nil
}

func (u *valueUnion) ToAny() (any, error) {
	switch {
	case u.Int != nil:
		return u.Int, nil
	case u.Str != nil:
		return u.Str, nil
	}
	return nil, nil
}

// A UnionConverter held by value (its methods on the pointer) took the same
// path: the pointer to a fresh converter was stored over the value's first
// field, so Int pointed at a valueUnion.
func TestFix_UnionConverterHeldByValue(t *testing.T) {
	type rec struct {
		U valueUnion `avro:"u"`
	}
	schema := avro.MustParse(`{"type":"record","name":"ConvRec","fields":[{"name":"u","type":["null","int","string"]}]}`)

	var got rec
	require.NoError(t, avro.Unmarshal(schema, []byte{0x02, 0x54}, &got))
	require.NotNil(t, got.U.Int)
	assert.Equal(t, 42, *got.U.Int)
	assert.Nil(t, got.U.Str)
}

// A decimal decoded into a big.Rat value stored a fresh *big.Rat over the
// value's first word, so the value read as 0 although the encoder had written
// it from the same type.
func TestFix_BytesDecimalIntoBigRatValue(t *testing.T) {
	type rec struct {
		D big.Rat `avro:"d"`
	}
	schema := avro.MustParse(`{"type":"record","name":"RatRec","fields":[{"name":"d",
		"type":{"type":"bytes","logicalType":"decimal","precision":9,"scale":2}}]}`)
	want := big.NewRat(12345, 100)
	data, err := avro.Marshal(schema, rec{D: *want})
	require.NoError(t, err)

	var got rec
	require.NoError(t, avro.Unmarshal(schema, data, &got))
	assert.Zero(t, got.D.Cmp(want), "got %s, want %s", got.D.FloatString(2), want.FloatString(2))
}

// An `any` reused across messages holds the *big.Rat the first decode put
// there, and the second decode went into that big.Rat's value: the same defect
// returned the first message's value for the second.
func TestFix_BytesDecimalIntoReusedAny(t *testing.T) {
	schema := avro.MustParse(`{"type":"bytes","logicalType":"decimal","precision":9,"scale":2}`)
	first, err := avro.Marshal(schema, big.NewRat(12345, 100))
	require.NoError(t, err)
	second, err := avro.Marshal(schema, big.NewRat(99999, 100))
	require.NoError(t, err)

	var got any
	require.NoError(t, avro.Unmarshal(schema, first, &got))
	require.NoError(t, avro.Unmarshal(schema, second, &got))
	r, ok := got.(*big.Rat)
	require.True(t, ok, "got %T", got)
	assert.Equal(t, "999.99", r.FloatString(2))
}

// A null decoded into a reused slice under a nullable union cleared the
// slice's data pointer and kept its length and capacity, so indexing the slice
// faulted, and with a long enough slice read and wrote program memory.
func TestFix_NullIntoReusedSlice(t *testing.T) {
	longsSchema := avro.MustParse(`["null",{"type":"array","items":"long"}]`)
	list := []int64{1, 2, 3}
	require.NoError(t, avro.Unmarshal(longsSchema, []byte{0x00}, &list))
	// Only the header is checked: with the defect, touching an element faults.
	if len(list) != 0 || cap(list) != 0 {
		t.Errorf("after a null the []int64 has len %d and cap %d, want an empty slice", len(list), cap(list))
	}

	bytesSchema := avro.MustParse(`["null","bytes"]`)
	b := []byte("abc")
	require.NoError(t, avro.Unmarshal(bytesSchema, []byte{0x00}, &b))
	if len(b) != 0 || cap(b) != 0 {
		t.Errorf("after a null the []byte has len %d and cap %d, want an empty slice", len(b), cap(b))
	}
}

// An empty array decoded into a reused slice left the slice as it was, so the
// message read as the previous message's list.
func TestFix_EmptyArrayIntoReusedSlice(t *testing.T) {
	type item struct {
		A int `avro:"a"`
	}
	scalar := avro.MustParse(`{"type":"array","items":"long"}`)
	list := []int64{1, 2, 3}
	require.NoError(t, avro.Unmarshal(scalar, []byte{0x00}, &list))
	assert.Empty(t, list)

	records := avro.MustParse(`{"type":"array","items":{"type":"record","name":"Item","fields":[{"name":"a","type":"int"}]}}`)
	items := []item{{A: 1}, {A: 2}}
	require.NoError(t, avro.Unmarshal(records, []byte{0x00}, &items))
	assert.Empty(t, items)
}

type selfPointer *selfPointer

// A Go type defined as a pointer to itself sent codec construction into
// endless recursion: a fatal stack overflow, on every call, that recover
// cannot catch.
func TestFix_PointerTypeThatPointsToItself(t *testing.T) {
	inChild(t, func() {
		debug.SetMaxStack(64 << 20)
		schema := avro.MustParse(`{"type":"record","name":"Self","fields":[]}`)
		var p selfPointer
		if err := avro.Unmarshal(schema, []byte{}, &p); err == nil {
			childFail("Unmarshal into `type P *P` returned no error")
		}
		if _, err := avro.Marshal(schema, p); err == nil {
			childFail("Marshal of `type P *P` returned no error")
		}
	})
}

type zeroSize struct{}

var zeroSizeSink *zeroSize

// A union decoded into `any` through a registered pointer type decoded into
// the pointed-to type's own storage as if it were a pointer slot. That stored
// a pointer where the garbage collector does not look for one, and for a
// zero-size type it wrote over the word every zero-size allocation shares.
func TestFix_ResolvedUnionOfRegisteredPointerType(t *testing.T) {
	cfg := avro.Config{}.Freeze()
	cfg.Register("fixes.Empty", &zeroSize{})
	schema := avro.MustParse(`["null",{"type":"record","name":"fixes.Empty","fields":[]}]`)

	var got any
	require.NoError(t, cfg.Unmarshal(schema, []byte{0x02}, &got))
	assert.IsType(t, &zeroSize{}, got)

	// Every zero-size allocation is the address of one runtime word.
	zeroSizeSink = new(zeroSize)
	word := (*uintptr)(unsafe.Pointer(zeroSizeSink))
	if *word != 0 {
		*word = 0 // Put it back for the rest of the tests.
		t.Error("decoding wrote over the word that every zero-size allocation shares")
	}
}

type wideNotRat struct {
	A [8]int64
}

// The encoder chose the decimal codec for any struct under a bytes decimal
// schema (an operator-precedence slip), so a struct that is not a big.Rat was
// read as one, past its end for smaller types; and a big.Rat under a schema
// that is not a decimal panicked.
func TestFix_EncodeStructUnderBytesDecimal(t *testing.T) {
	decimal := avro.MustParse(`{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`)
	noPanic(t, "Marshal of a struct that is not a big.Rat", func() {
		_, err := avro.Marshal(decimal, wideNotRat{})
		assert.Error(t, err, "a struct that is not a big.Rat cannot be encoded as a decimal")
	})
	noPanic(t, "Marshal of a big.Rat as a long", func() {
		_, err := avro.Marshal(avro.MustParse(`"long"`), *big.NewRat(1, 2))
		assert.Error(t, err)
	})
}

// Encoding a decimal with more fractional digits than its scale truncated a
// positive value toward zero but rounded a negative one away from it (big.Int
// Div is Euclidean): 1.239 became 1.23 and -1.239 became -1.24.
func TestFix_DecimalTruncatesNegativeValuesTowardZero(t *testing.T) {
	for _, s := range []string{
		`{"type":"bytes","logicalType":"decimal","precision":9,"scale":2}`,
		`{"type":"fixed","name":"fixes.dec8","size":8,"logicalType":"decimal","precision":9,"scale":2}`,
	} {
		schema := avro.MustParse(s)
		for _, tc := range []struct{ in, want string }{{"1.239", "1.23"}, {"-1.239", "-1.23"}} {
			in, _ := new(big.Rat).SetString(tc.in)
			data, err := avro.Marshal(schema, in)
			require.NoError(t, err)
			var got *big.Rat
			require.NoError(t, avro.Unmarshal(schema, data, &got))
			assert.Equal(t, tc.want, got.FloatString(2), "%s under %s", tc.in, s)
		}
	}
}

// A date before 1970 with a time of day was encoded as the day after:
// integer division truncates toward zero, and days before the epoch need the
// floor.
func TestFix_DateBefore1970WithTimeOfDay(t *testing.T) {
	schema := avro.MustParse(`{"type":"int","logicalType":"date"}`)
	data, err := avro.Marshal(schema, time.Date(1969, 7, 20, 20, 17, 0, 0, time.UTC))
	require.NoError(t, err)
	var got time.Time
	require.NoError(t, avro.Unmarshal(schema, data, &got))
	assert.Equal(t, time.Date(1969, 7, 20, 0, 0, 0, 0, time.UTC), got)
}

// Values that do not fit the Go type they were decoded into were silently
// wrapped: an Avro int 300 decoded into an int8 read as 44, and -1 into a
// uint16 as 65535.
func TestFix_DecodeIntegerOutOfRangeForGoType(t *testing.T) {
	intSchema := avro.MustParse(`"int"`)
	longSchema := avro.MustParse(`"long"`)
	ints := avro.MustParse(`{"type":"array","items":"int"}`)
	longArray := avro.MustParse(`{"type":"array","items":"long"}`)
	resolved, err := avro.NewSchemaCompatibility().Resolve(longSchema, intSchema)
	require.NoError(t, err)

	for _, tc := range []struct {
		name   string
		schema avro.Schema
		data   []byte
		v      any
	}{
		{"int 300 into int8", intSchema, longs(300), new(int8)},
		{"int -1 into uint8", intSchema, longs(-1), new(uint8)},
		{"int 70000 into int16", intSchema, longs(70000), new(int16)},
		{"int -1 into uint16", intSchema, longs(-1), new(uint16)},
		{"long 1<<33 into uint32", longSchema, longs(1 << 33), new(uint32)},
		{"long -1 into uint32", longSchema, longs(-1), new(uint32)},
		{"int -1 promoted to long into uint32", resolved, longs(-1), new(uint32)},
		{"array of int 300 into []int8", ints, longs(1, 300, 0), new([]int8)},
		{"array of int -1 into []uint", ints, longs(1, -1, 0), new([]uint)},
		{"array of int -1 into []uint16", ints, longs(1, -1, 0), new([]uint16)},
		{"array of long 1<<40 into []int32", longArray, longs(1, 1<<40, 0), new([]int32)},
		{"array of long -1 into []uint32", longArray, longs(1, -1, 0), new([]uint32)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := avro.Unmarshal(tc.schema, tc.data, tc.v)
			assert.Error(t, err, "decoded as %v", reflect.ValueOf(tc.v).Elem().Interface())
		})
	}
}

// A Go int or uint beyond the int32 range encoded as an Avro int was
// silently truncated.
func TestFix_EncodeIntegerOutOfRangeForAvroInt(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("int is 32 bits here, so every int fits")
	}
	intSchema := avro.MustParse(`"int"`)
	ints := avro.MustParse(`{"type":"array","items":"int"}`)
	// Variables, not constants: the constants would not compile where int is
	// 32 bits.
	large, wide := int64(3_000_000_000), uint64(1<<40)
	for _, tc := range []struct {
		name   string
		schema avro.Schema
		v      any
	}{
		{"int 3e9", intSchema, int(large)},
		{"int -3e9", intSchema, int(-large)},
		{"[]int{3e9}", ints, []int{int(large)}},
		{"[]uint{1<<40}", ints, []uint{uint(wide)}},
		{"[]uint{MaxUint}", ints, []uint{math.MaxUint}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, err := avro.Marshal(tc.schema, tc.v)
			assert.Error(t, err, "encoded as % x", b)
		})
	}
}

// The reader handed out byte slices and strings carved from one shared slab,
// each with the rest of the slab as spare capacity, so appending to a decoded
// []byte overwrote the values decoded after it, strings included.
func TestFix_DecodedBytesDoNotShareCapacity(t *testing.T) {
	type rec struct {
		A []byte `avro:"a"`
		B string `avro:"b"`
		C []byte `avro:"c"`
	}
	schema := avro.MustParse(`{"type":"record","name":"Slab","fields":[{"name":"a","type":"bytes"},
		{"name":"b","type":"string"},{"name":"c","type":"bytes"}]}`)
	data, err := avro.Marshal(schema, rec{A: []byte("x"), B: "yz", C: []byte("w")})
	require.NoError(t, err)

	var got rec
	require.NoError(t, avro.Unmarshal(schema, data, &got))
	got.A = append(got.A, 'Q', 'Q', 'Q')
	assert.Equal(t, "yz", got.B)
	assert.Equal(t, []byte("w"), got.C)

	var generic any
	require.NoError(t, avro.Unmarshal(schema, data, &generic))
	m := generic.(map[string]any)
	a := m["a"].([]byte)
	_ = append(a, 'Q', 'Q', 'Q')
	assert.Equal(t, "yz", m["b"])
	assert.Equal(t, []byte("w"), m["c"])
}

// allocatedBy returns roughly how many bytes fn allocated.
func allocatedBy(fn func()) uint64 {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	fn()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// An array or map block count was trusted for the allocation before a single
// element was read, so a few bytes claiming millions of elements made the
// decoder allocate hundreds of megabytes (or exhaust memory) for data that was
// not there.
func TestFix_BlockCountDoesNotDriveAllocation(t *testing.T) {
	const limit = 32 << 20
	for _, tc := range []struct {
		name   string
		schema string
		data   []byte
		v      func() any
	}{
		{"[]int64", `{"type":"array","items":"long"}`, longs(1<<26, 1), func() any { return new([]int64) }},
		{
			"[]record", `{"type":"array","items":{"type":"record","name":"AllocItem","fields":[{"name":"a","type":"long"}]}}`,
			longs(1<<24, 1), func() any {
				type item struct {
					A int64 `avro:"a"`
				}
				return new([]item)
			},
		},
		{
			"map[string]int64", `{"type":"map","values":"long"}`, append(longs(1<<22), 0x02, 'k', 0x02),
			func() any { return new(map[string]int64) },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema := avro.MustParse(tc.schema)
			var err error
			n := allocatedBy(func() { err = avro.Unmarshal(schema, tc.data, tc.v()) })
			assert.Error(t, err, "the data ends after one element")
			assert.Less(t, n, uint64(limit), "decoding %d bytes allocated %d MiB", len(tc.data), n>>20)
		})
	}
}

var countingKeyCalls int

type countingKey struct{ s string }

func (k *countingKey) UnmarshalText(b []byte) error {
	countingKeyCalls++
	k.s = string(b)
	return nil
}

// The decoder of a map whose keys are TextUnmarshalers did not stop at an
// error, so a block count of a million followed by nothing ran the key
// decoder a million times over the exhausted input.
func TestFix_MapWithTextUnmarshalerKeysStopsAtError(t *testing.T) {
	schema := avro.MustParse(`{"type":"map","values":"long"}`)
	countingKeyCalls = 0
	var got map[*countingKey]int64
	err := avro.Unmarshal(schema, longs(1<<20), &got)
	assert.Error(t, err)
	assert.LessOrEqual(t, countingKeyCalls, 1)
}

// Encoding a struct that lacks a field whose type is a union of three or more
// types with a null default panicked (a nil type reached encoder selection).
func TestFix_EncodeMissingFieldWithNullDefaultInWideUnion(t *testing.T) {
	type rec struct {
		A int `avro:"a"`
	}
	schema := avro.MustParse(`{"type":"record","name":"WideDef","fields":[{"name":"a","type":"int"},
		{"name":"u","type":["null","int","string"],"default":null}]}`)
	noPanic(t, "Marshal", func() {
		data, err := avro.Marshal(schema, rec{A: 1})
		require.NoError(t, err)
		assert.Equal(t, []byte{0x02, 0x00}, data)
	})
}

// Encoding a map record that lacks a field whose union default is a named
// type looked the union branch up by its type ("record") instead of its name.
func TestFix_EncodeMapRecordWithNamedUnionDefault(t *testing.T) {
	schema := avro.MustParse(`{"type":"record","name":"MapDef","fields":[{"name":"a","type":"int"},
		{"name":"u","type":[{"type":"record","name":"MapDefIn","fields":[{"name":"x","type":"int"}]},"null"],"default":{"x":5}}]}`)
	data, err := avro.Marshal(schema, map[string]any{"a": 1})
	require.NoError(t, err)
	assert.Equal(t, []byte{0x02, 0x00, 0x0a}, data)
}

// Encoders were cached under a schema identity that leaves out field
// defaults, so after encoding with one schema, encoding with a schema that
// differs only in a default wrote the first schema's default.
func TestFix_EncoderCacheKeepsSchemasWithDifferentDefaultsApart(t *testing.T) {
	type rec struct {
		A int `avro:"a"`
	}
	one := avro.MustParse(`{"type":"record","name":"fixes.CacheDef","fields":[{"name":"a","type":"int"},{"name":"x","type":"int","default":1}]}`)
	two := avro.MustParse(`{"type":"record","name":"fixes.CacheDef","fields":[{"name":"a","type":"int"},{"name":"x","type":"int","default":2}]}`)

	data, err := avro.Marshal(one, rec{A: 1})
	require.NoError(t, err)
	assert.Equal(t, []byte{0x02, 0x02}, data)
	data, err = avro.Marshal(two, rec{A: 1})
	require.NoError(t, err)
	assert.Equal(t, []byte{0x02, 0x04}, data)

	// The same through a union, whose own identity is built from its types.
	unionOne := avro.MustParse(`["null",{"type":"record","name":"fixes.CacheDefU","fields":[{"name":"a","type":"int"},{"name":"x","type":"int","default":1}]}]`)
	unionTwo := avro.MustParse(`["null",{"type":"record","name":"fixes.CacheDefU","fields":[{"name":"a","type":"int"},{"name":"x","type":"int","default":2}]}]`)
	data, err = avro.Marshal(unionOne, &rec{A: 1})
	require.NoError(t, err)
	assert.Equal(t, []byte{0x02, 0x02, 0x02}, data)
	data, err = avro.Marshal(unionTwo, &rec{A: 1})
	require.NoError(t, err)
	assert.Equal(t, []byte{0x02, 0x02, 0x04}, data)
}

// Unmarshal into a nil interface panicked.
func TestFix_UnmarshalIntoNil(t *testing.T) {
	noPanic(t, "Unmarshal into nil", func() {
		assert.Error(t, avro.Unmarshal(avro.MustParse(`"int"`), []byte{0x02}, nil))
	})
}

// A null decoded into a reused UnionConverter pointer left the converter
// holding the previous message's value.
func TestFix_NullIntoReusedUnionConverter(t *testing.T) {
	schema := avro.MustParse(`["null","int","string"]`)
	var got *valueUnion
	require.NoError(t, avro.Unmarshal(schema, []byte{0x02, 0x54}, &got))
	require.NoError(t, avro.Unmarshal(schema, []byte{0x00}, &got))
	assert.Nil(t, got)
}

// A union decoded into a reused map[string]any added its branch to whatever
// the map held, so the map named two branches (which the encoder then
// refuses), and a null left the previous branch in place.
func TestFix_UnionIntoReusedMap(t *testing.T) {
	schema := avro.MustParse(`["null","int","string"]`)
	got := map[string]any{}
	require.NoError(t, avro.Unmarshal(schema, []byte{0x04, 0x02, 'x'}, &got))
	require.NoError(t, avro.Unmarshal(schema, []byte{0x02, 0x54}, &got))
	assert.Equal(t, map[string]any{"int": 42}, got)
	require.NoError(t, avro.Unmarshal(schema, []byte{0x00}, &got))
	assert.Empty(t, got)
}

// Skipping a field of type null (one the Go type does not have) failed with
// "schema type null is unsupported".
func TestFix_SkipNullField(t *testing.T) {
	type rec struct {
		A int `avro:"a"`
		B int `avro:"b"`
	}
	schema := avro.MustParse(`{"type":"record","name":"SkipNull","fields":[{"name":"a","type":"int"},
		{"name":"n","type":"null"},{"name":"b","type":"int"}]}`)
	var got rec
	require.NoError(t, avro.Unmarshal(schema, []byte{0x02, 0x04}, &got))
	assert.Equal(t, rec{A: 1, B: 2}, got)
}

// Skipping an int or long accepted a varint longer than the type allows,
// which reading it rejects, and carried on from the wrong place.
func TestFix_SkipRejectsOverlongVarint(t *testing.T) {
	type rec struct {
		B int `avro:"b"`
	}
	for typ, size := range map[string]int{"int": 5, "long": 10} {
		schema := avro.MustParse(`{"type":"record","name":"Overlong` + typ + `","fields":[{"name":"a","type":"` + typ + `"},{"name":"b","type":"int"}]}`)
		// size continuation bytes, one more than the type allows, then b = 1.
		data := append(bytes.Repeat([]byte{0xff}, size), 0x02)
		var got rec
		assert.Error(t, avro.Unmarshal(schema, data, &got), "skipping an overlong %s", typ)
	}
}

// Decoding deeply nested data for a recursive schema recursed once per level
// with no limit: about a megabyte of input overflowed the goroutine stack, a
// fatal error that recover cannot catch.
func TestFix_DeeplyNestedRecursiveData(t *testing.T) {
	inChild(t, func() {
		debug.SetMaxStack(64 << 20)
		schema := avro.MustParse(`{"type":"record","name":"fixes.Node","fields":[{"name":"next","type":["null","fixes.Node"]}]}`)
		data := append(bytes.Repeat([]byte{0x02}, 1_000_000), 0x00)

		type node struct {
			Next *node `avro:"next"`
		}
		var n node
		if err := avro.Unmarshal(schema, data, &n); err == nil {
			childFail("decoding into a struct returned no error")
		}
		var generic any
		if err := avro.Unmarshal(schema, data, &generic); err == nil {
			childFail("decoding into any returned no error")
		}
		type nothing struct{}
		if err := avro.Unmarshal(schema, data, &nothing{}); err == nil {
			childFail("skipping returned no error")
		}
		r := avro.NewReader(bytes.NewReader(data), 1024)
		if r.ReadNext(schema); r.Error == nil {
			childFail("ReadNext returned no error")
		}
	})
}

type cyclicNode struct {
	Next *cyclicNode `avro:"next"`
}

// Encoding a Go value that refers to itself recursed for ever, the same
// fatal stack overflow.
func TestFix_EncodeCyclicValue(t *testing.T) {
	inChild(t, func() {
		debug.SetMaxStack(64 << 20)
		schema := avro.MustParse(`{"type":"record","name":"fixes.Cyclic","fields":[{"name":"next","type":["null","fixes.Cyclic"]}]}`)
		n := &cyclicNode{}
		n.Next = n
		if _, err := avro.Marshal(schema, n); err == nil {
			childFail("encoding a cyclic value returned no error")
		}
	})
}

// The largest decimal precision a fixed type can hold was worked out for an
// unsigned value, one digit too many for some sizes (a 3-byte fixed accepted
// precision 7, whose negative values then encoded as positive ones), and for
// 128 bytes or more it overflowed to infinity, which converted to an int
// differently on each platform and dropped the logical type on amd64.
func TestFix_FixedDecimalPrecisionFitsSignedSize(t *testing.T) {
	logical := func(s string) avro.LogicalSchema {
		return avro.MustParse(s).(*avro.FixedSchema).Logical()
	}
	assert.Nil(t, logical(`{"type":"fixed","name":"fixes.f3p7","size":3,"logicalType":"decimal","precision":7}`),
		"7 digits do not fit 3 signed bytes")
	assert.NotNil(t, logical(`{"type":"fixed","name":"fixes.f3p6","size":3,"logicalType":"decimal","precision":6}`))
	assert.NotNil(t, logical(`{"type":"fixed","name":"fixes.f200","size":200,"logicalType":"decimal","precision":10}`))
}

// The fixed-to-uint64 codec used its own struct, shared by every goroutine
// using the cached codec, as its scratch buffer, so concurrent decodes raced
// and returned each other's values.
func TestFix_FixedUint64Concurrently(t *testing.T) {
	schema := avro.MustParse(`{"type":"fixed","name":"fixes.u64","size":8}`)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var wrong int
	for g := range 8 {
		wg.Go(func() {
			for i := range 2000 {
				want := uint64(g)<<32 | uint64(i)
				data := binary.BigEndian.AppendUint64(nil, want)
				var got uint64
				if err := avro.Unmarshal(schema, data, &got); err != nil || got != want {
					mu.Lock()
					wrong++
					mu.Unlock()
				}
				data, err := avro.Marshal(schema, want)
				if err != nil || binary.BigEndian.Uint64(data) != want {
					mu.Lock()
					wrong++
					mu.Unlock()
				}
			}
		})
	}
	wg.Wait()
	assert.Zero(t, wrong, "concurrent fixed uint64 decodes and encodes returned wrong values")
}

// A nil *big.Rat under a decimal schema panicked on encode.
func TestFix_EncodeNilBigRat(t *testing.T) {
	type rec struct {
		D *big.Rat `avro:"d"`
	}
	for _, typ := range []string{
		`{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`,
		`{"type":"fixed","name":"fixes.nilrat","size":4,"logicalType":"decimal","precision":4,"scale":2}`,
	} {
		schema := avro.MustParse(`{"type":"record","name":"NilRat","fields":[{"name":"d","type":` + typ + `}]}`)
		noPanic(t, "Marshal of a nil *big.Rat", func() {
			_, err := avro.Marshal(schema, rec{})
			assert.Error(t, err)
		})
	}
}

// When a reader field took its default (the writer did not have it), an error
// decoding that default was dropped, so a field of the wrong Go type was left
// at its zero value with no error, where the same field present in the data
// is an error.
func TestFix_DefaultDecodeErrorIsReported(t *testing.T) {
	writer := avro.MustParse(`{"type":"record","name":"fixes.Def","fields":[{"name":"a","type":"int"}]}`)
	reader := avro.MustParse(`{"type":"record","name":"fixes.Def","fields":[{"name":"a","type":"int"},
		{"name":"s","type":"string","default":"x"}]}`)
	resolved, err := avro.NewSchemaCompatibility().Resolve(reader, writer)
	require.NoError(t, err)

	type rec struct {
		A int `avro:"a"`
		S int `avro:"s"`
	}
	var got rec
	assert.Error(t, avro.Unmarshal(resolved, []byte{0x02}, &got), "an int cannot hold the string default")
}

// A type converter on a nullable union returning a value of another type had
// that value's memory stored as the target type, unchecked.
func TestFix_UnionTypeConverterResultIsChecked(t *testing.T) {
	cfg := avro.Config{}.Freeze()
	cfg.RegisterTypeConverters(avro.TypeConversionFuncs{
		AvroType: avro.Union,
		DecoderTypeConversion: func(any, avro.Schema) (any, error) {
			return "not the target type", nil
		},
	})

	var p *int64
	assert.Error(t, cfg.Unmarshal(avro.MustParse(`["null","long"]`), []byte{0x02, 0x54}, &p))
	var b []byte
	assert.Error(t, cfg.Unmarshal(avro.MustParse(`["null","bytes"]`), []byte{0x02, 0x02, 'x'}, &b))
}

// Data that ends before the value does was accepted: Unmarshal treated
// running out of input as success, and skipping a field past the end did
// too, so a truncated message decoded with zero values and no error.
func TestFix_TruncatedInputIsAnError(t *testing.T) {
	record := avro.MustParse(`{"type":"record","name":"fixes.Trunc","fields":[{"name":"a","type":"int"},{"name":"b","type":"long"}]}`)
	type rec struct {
		A int32 `avro:"a"`
		B int64 `avro:"b"`
	}
	type onlyA struct {
		A int32 `avro:"a"`
	}
	withString := avro.MustParse(`{"type":"record","name":"fixes.TruncS","fields":[{"name":"a","type":"int"},{"name":"s","type":"string"}]}`)
	withFixed := avro.MustParse(`{"type":"record","name":"fixes.TruncF","fields":[{"name":"a","type":"int"},
		{"name":"f","type":{"type":"fixed","name":"fixes.f8","size":8}}]}`)

	for _, tc := range []struct {
		name   string
		schema avro.Schema
		data   []byte
		v      func() any
	}{
		{"record missing its last long", record, []byte{0x02}, func() any { return &rec{} }},
		{"record with half its last long", record, []byte{0x02, 0x80}, func() any { return &rec{} }},
		{"record missing its last long, into any", record, []byte{0x02}, func() any { return new(any) }},
		{"array of three holding two", avro.MustParse(`{"type":"array","items":"long"}`), longs(3, 1, 2), func() any { return new([]int64) }},
		{"map missing its value", avro.MustParse(`{"type":"map","values":"long"}`), []byte{0x02, 0x02, 'k'}, func() any { return new(map[string]int64) }},
		{"skipped string past the end", withString, []byte{0x02, 0x0a, 'a', 'b'}, func() any { return &onlyA{} }},
		{"skipped fixed past the end", withFixed, []byte{0x02, 1, 2}, func() any { return &onlyA{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Error(t, avro.Unmarshal(tc.schema, tc.data, tc.v()), "Unmarshal")
			dec := avro.NewDecoderForSchema(tc.schema, bytes.NewReader(tc.data))
			err := dec.Decode(tc.v())
			assert.Error(t, err, "Decoder.Decode")
			assert.NotErrorIs(t, err, io.EOF, "a value cut short is not the end of the stream")
		})
	}
}

// Resolving a reader schema against a writer schema recursed for ever when
// they had a record that refers to itself: a fatal stack overflow.
func TestFix_ResolveRecursiveSchema(t *testing.T) {
	inChild(t, func() {
		debug.SetMaxStack(64 << 20)
		writer := avro.MustParse(`{"type":"record","name":"fixes.RNode","fields":[{"name":"v","type":"int"},
			{"name":"next","type":["null","fixes.RNode"]}]}`)
		reader := avro.MustParse(`{"type":"record","name":"fixes.RNode","fields":[{"name":"v","type":"long"},
			{"name":"w","type":"string","default":"d"},{"name":"next","type":["null","fixes.RNode"]}]}`)
		resolved, err := avro.NewSchemaCompatibility().Resolve(reader, writer)
		if err != nil {
			childFail("Resolve: " + err.Error())
		}

		type wnode struct {
			V    int32  `avro:"v"`
			Next *wnode `avro:"next"`
		}
		data, err := avro.Marshal(writer, wnode{V: 1, Next: &wnode{V: 2}})
		if err != nil {
			childFail("Marshal: " + err.Error())
		}
		type rnode struct {
			V    int64  `avro:"v"`
			W    string `avro:"w"`
			Next *rnode `avro:"next"`
		}
		var got rnode
		if err := avro.Unmarshal(resolved, data, &got); err != nil {
			childFail("Unmarshal: " + err.Error())
		}
		if got.V != 1 || got.W != "d" || got.Next == nil || got.Next.V != 2 || got.Next.W != "d" || got.Next.Next != nil {
			childFail(fmt.Sprintf("decoded %+v, next %+v", got, got.Next))
		}
	})
}

// An int varint's fifth byte carries the top 4 bits and a long's tenth byte
// the top bit, but the readers kept whatever bits fitted and dropped the rest,
// so a malformed value read as a different, valid one. ReadNext, which read a
// union's branch as a long, refused such a branch number while the decoders,
// which read it as an int, took the wrapped one.
func TestFix_VarintBeyondItsTypeIsAnError(t *testing.T) {
	union := avro.MustParse(`["null","int","string"]`)
	for _, tc := range []struct {
		name   string
		schema avro.Schema
		data   []byte
		v      any
	}{
		{"int with bits past 32", avro.MustParse(`"int"`), []byte{0xff, 0xff, 0xff, 0xff, 0x7f}, new(int32)},
		{"long with bits past 64", avro.MustParse(`"long"`), append(bytes.Repeat([]byte{0xff}, 9), 0x7f), new(int64)},
		{
			"array of int with bits past 32", avro.MustParse(`{"type":"array","items":"int"}`),
			[]byte{0x02, 0xff, 0xff, 0xff, 0xff, 0x7f, 0x00},
			new([]int32),
		},
		// Branch 1 plus 2^32: 1 once the high bits are dropped.
		{"union branch past 32 bits", union, []byte{0x82, 0x80, 0x80, 0x80, 0x20, 0x54}, new(any)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Error(t, avro.Unmarshal(tc.schema, tc.data, tc.v), "Unmarshal")
			r := avro.NewReader(nil, 0).Reset(tc.data)
			r.ReadNext(tc.schema)
			assert.Error(t, r.Error, "ReadNext")
		})
	}
}

// A fixed schema could declare any size, and the library allocated that many
// bytes: decoding a fixed into `any` made the whole array before a single
// byte of data was read, and parsing a fixed field with a default made it at
// schema-parse time. A sixty-byte schema from an untrusted OCF header or
// schema registry could demand a terabyte. Fixed sizes are now capped.
func TestFix_FixedSizeIsCapped(t *testing.T) {
	const max = 1 << 24

	_, err := avro.NewFixedSchema("F", "", max+1, nil)
	require.Error(t, err)
	assert.ErrorContains(t, err, "greater than the maximum")

	fixed, err := avro.NewFixedSchema("F", "", max, nil)
	require.NoError(t, err)
	assert.Equal(t, max, fixed.Size())

	for _, tc := range []struct {
		schema string
		want   string
	}{
		{`{"type":"fixed","name":"F","size":68719476736}`, "greater than the maximum"},
		{`{"type":"fixed","name":"F","size":4294967296}`, "greater than the maximum"},
		{`{"type":"fixed","name":"F","size":4294967312}`, "greater than the maximum"},
		{`{"type":"fixed","name":"F","size":1.5}`, "must be an integer"},
		{`{"type":"record","name":"R","fields":[{"name":"f",
			"type":{"type":"fixed","name":"F","size":1099511627776},"default":""}]}`, "greater than the maximum"},
	} {
		_, err := avro.Parse(tc.schema)
		require.Error(t, err, "%s", tc.schema)
		assert.ErrorContains(t, err, tc.want, "%s", tc.schema)
	}

	// A fixed still decodes into `any`.
	small := avro.MustParse(`{"type":"fixed","name":"F","size":4}`)
	var got any
	require.NoError(t, avro.Unmarshal(small, []byte{0xde, 0xad, 0xbe, 0xef}, &got))
	assert.Equal(t, [4]byte{0xde, 0xad, 0xbe, 0xef}, got)
}

// A bytes.decimal could declare any precision, and with it any scale, and
// decoding a value computed 10^scale from scratch: a schema from an
// untrusted OCF header or schema registry with scale 999999999 spent
// unbounded time and memory on every value decoded. Such a decimal is now
// refused the way an invalid one always was, and the schema falls back to
// plain bytes.
func TestFix_BytesDecimalPrecisionIsCapped(t *testing.T) {
	schema := avro.MustParse(`{"type":"bytes","logicalType":"decimal","precision":999999999,"scale":999999998}`)

	// The logical type is dropped, so the value decodes as plain bytes.
	var got any
	require.NoError(t, avro.Unmarshal(schema, []byte{0x02, 0xff}, &got))
	assert.Equal(t, []byte{0xff}, got)

	// The same holds for a fixed.decimal whose precision the cap, not the
	// physical size, rejects: a 41600-byte fixed can hold 100000 digits, but
	// the cap stops at the same place it does for bytes.
	fixed := avro.MustParse(`{"type":"fixed","name":"F","size":41600,
		"logicalType":"decimal","precision":100001,"scale":100001}`)
	assert.Nil(t, fixed.(*avro.FixedSchema).Logical())

	// A decimal at the cap is still a decimal.
	atCap := avro.MustParse(`{"type":"bytes","logicalType":"decimal","precision":100000,"scale":100000}`)
	assert.Equal(t, avro.Decimal, atCap.(*avro.PrimitiveSchema).Logical().Type())
	var rat any
	require.NoError(t, avro.Unmarshal(atCap, []byte{0x00}, &rat))
	assert.Equal(t, big.NewRat(0, 1), rat)
}

// The decimal cap also holds where a decimal is used rather than parsed, so
// a schema built in Go cannot bypass it: decoding or encoding with a scale
// past the cap is an error, not unbounded work.
func TestFix_DecimalScaleCappedWhereUsed(t *testing.T) {
	bytesDec := avro.NewPrimitiveSchema(avro.Bytes, avro.NewDecimalLogicalSchema(200000, 200000))
	fixedDec, err := avro.NewFixedSchema("F", "", 4, avro.NewDecimalLogicalSchema(200000, 200000))
	require.NoError(t, err)

	var got any
	err = avro.Unmarshal(bytesDec, []byte{0x00}, &got)
	assert.Error(t, err, "decode bytes.decimal into any")
	assert.ErrorContains(t, err, "greater than the maximum")

	var rat big.Rat
	err = avro.Unmarshal(bytesDec, []byte{0x00}, &rat)
	assert.ErrorContains(t, err, "greater than the maximum", "decode bytes.decimal into big.Rat")

	err = avro.Unmarshal(fixedDec, []byte{0x80, 0x00, 0x00, 0x00}, &got)
	assert.ErrorContains(t, err, "greater than the maximum", "decode fixed.decimal")

	r := avro.NewReader(nil, 0).Reset([]byte{0x00})
	assert.Nil(t, r.ReadNext(bytesDec), "ReadNext")
	assert.Error(t, r.Error, "ReadNext")

	_, err = avro.Marshal(bytesDec, big.NewRat(1, 4))
	assert.Error(t, err, "encode bytes.decimal from *big.Rat")

	_, err = avro.Marshal(bytesDec, *big.NewRat(1, 4))
	assert.Error(t, err, "encode bytes.decimal from big.Rat")

	_, err = avro.Marshal(fixedDec, big.NewRat(1, 4))
	assert.Error(t, err, "encode fixed.decimal")
}

// FuzzSchemaReparse found a schema that parsed but whose marshalled form did
// not parse back: MarshalJSON wrote a fixed default, which parse stores as a
// [size]byte, as a JSON array, and a bytes default as a base64 string, but
// the specification defines both as JSON strings whose Unicode code points
// 0-255 map to byte values 0-255. A bytes default also re-parsed to different
// bytes, silently. The fuzzer's input is kept as a corpus seed at
// testdata/fuzz/FuzzSchemaReparse/cc94f53cf86f8ee4.
func TestFix_MarshalledDefaultsParseBack(t *testing.T) {
	s := avro.MustParse(`{
		"type": "record",
		"name": "R",
		"fields": [
			{"name": "zero", "type": {"type": "fixed", "name": "Zero", "size": 0}, "default": ""},
			{"name": "fix",  "type": {"type": "fixed", "name": "Fix",  "size": 3}, "default": "ab\u0000"},
			{"name": "by",   "type": "bytes", "default": "\u00ffx"},
			{"name": "arr",  "type": {"type": "array", "items": "bytes"}, "default": ["ab"]},
			{"name": "m",    "type": {"type": "map", "values": {"type": "fixed", "name": "MF", "size": 2}}, "default": {"k": "ab"}}
		]
	}`)

	full, err := json.Marshal(s)
	require.NoError(t, err)

	// Bytes and fixed defaults are strings, one code point per byte, not
	// arrays or base64, and bytes over 0x7f escape so they reparse as the
	// same byte.
	assert.Contains(t, string(full), `"default":""`)
	assert.Contains(t, string(full), `"default":"ab\u0000"`)
	assert.Contains(t, string(full), `"default":"\u00ffx"`)
	assert.Contains(t, string(full), `"default":["ab"]`)
	assert.Contains(t, string(full), `"default":{"k":"ab"}`)
	assert.NotContains(t, string(full), "YWI=")

	reparsed, err := avro.Parse(string(full))
	require.NoError(t, err)

	again, err := json.Marshal(reparsed)
	require.NoError(t, err)
	assert.JSONEq(t, string(full), string(again))

	want := s.(*avro.RecordSchema)
	got := reparsed.(*avro.RecordSchema)
	require.Len(t, got.Fields(), len(want.Fields()))
	for i, field := range want.Fields() {
		assert.Equal(t, field.Default(), got.Fields()[i].Default(), "field %s", field.Name())
	}
}

func TestFix_RecursiveRecordDefaultRejectedWithoutPanic(t *testing.T) {
	_, err := avro.Parse(`{"type":"record","name":"R","fields":[
		{"name":"next","type":"R","default":{}}]}`)
	require.Error(t, err)
}

func TestFix_RecordDefaultDoesNotInventMissingDefaults(t *testing.T) {
	_, err := avro.Parse(`{
		"type":"record","name":"Outer","fields":[{
			"name":"inner",
			"type":{"type":"record","name":"Inner","fields":[
				{"name":"required","type":"null"},
				{"name":"optional","type":["null","int"]}
			]},
			"default":{}
		}]
	}`)
	require.Error(t, err)
}

func TestFix_FixedDefaultMustHaveDeclaredSize(t *testing.T) {
	for _, def := range []string{`""`, `"a"`, `"abcd"`} {
		in := `{"type":"record","name":"R","fields":[{
			"name":"f","type":{"type":"fixed","name":"F","size":3},
			"default":` + def + `}]}`
		_, err := avro.Parse(in)
		require.Error(t, err, "default %s", def)
	}

	s, err := avro.Parse(`{"type":"record","name":"R","fields":[{
		"name":"f","type":{"type":"fixed","name":"F","size":3},"default":"abc"}]}`)
	require.NoError(t, err)
	assert.Equal(t, [3]byte{'a', 'b', 'c'}, s.(*avro.RecordSchema).Fields()[0].Default())
}

func TestFix_NumericDefaultsAreExactAndRangeChecked(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  int64
	}{
		{"9223372036854775807", math.MaxInt64},
		{"9007199254740993", 9007199254740993},
		{"1.5", 1},
		{"-2147483648", -2147483648},
	} {
		in := `{"type":"record","name":"R","fields":[{"name":"n","type":"long","default":` + tc.value + `}]}`
		s, err := avro.Parse(in)
		require.NoError(t, err, "long default %s", tc.value)
		assert.Equal(t, tc.want, s.(*avro.RecordSchema).Fields()[0].Default())
	}

	for _, tc := range []struct {
		value string
		want  int
	}{{"1.0", 1}, {"1e0", 1}, {"0e-100000", 0}} {
		in := `{"type":"record","name":"R","fields":[{"name":"n","type":"int","default":` + tc.value + `}]}`
		s, err := avro.Parse(in)
		require.NoError(t, err, "int default %s", tc.value)
		assert.Equal(t, tc.want, s.(*avro.RecordSchema).Fields()[0].Default(), "int default %s", tc.value)
	}

	for _, tc := range []struct{ typ, def string }{
		{"int", "1.5"},
		{"int", "1.0000000000000001"},
		{"int", "1e-100000"},
		{"int", "2147483648"},
		{"long", "1e100"},
		{"float", "3.4028236e38"},
	} {
		in := fmt.Sprintf(`{"type":"record","name":"R","fields":[
			{"name":"n","type":%q,"default":%s}]}`, tc.typ, tc.def)
		_, err := avro.Parse(in)
		require.Error(t, err, "%s default %s", tc.typ, tc.def)
	}
}

func TestFix_AliasCannotShadowDefinedName(t *testing.T) {
	for _, namedType := range []string{"record", "enum", "fixed"} {
		t.Run(namedType, func(t *testing.T) {
			var definition string
			switch namedType {
			case "record":
				definition = `{"type":"record","name":"S","aliases":["R"],"fields":[]}`
			case "enum":
				definition = `{"type":"enum","name":"S","aliases":["R"],"symbols":["A"]}`
			case "fixed":
				definition = `{"type":"fixed","name":"S","aliases":["R"],"size":1}`
			}
			_, err := avro.Parse(`{"type":"record","name":"Container","fields":[
				{"name":"first","type":{"type":"record","name":"R","fields":[]}},
				{"name":"second","type":` + definition + `},
				{"name":"ref","type":"R"}
			]}`)
			require.Error(t, err)
		})
	}
}

func TestFix_AmbiguousReservedKeyCaseIsRejected(t *testing.T) {
	_, err := avro.ParseWithCache(
		`{"type":"record","Name":"R","NAME":"S","fields":[]}`,
		"", &avro.SchemaCache{},
	)
	require.Error(t, err, "reserved keys must be matched case-sensitively")
}

func TestFix_EnumStringAllocationsAreBounded(t *testing.T) {
	symbols := make([]string, 2000)
	for i := range symbols {
		symbols[i] = fmt.Sprintf("A%06d", i)
	}
	s, err := avro.NewEnumSchema("E", "", symbols)
	require.NoError(t, err)

	allocs := testing.AllocsPerRun(2, func() {
		_ = s.String()
	})
	require.Less(t, allocs, float64(100), "canonicalization must not allocate once per symbol")
}

func TestFix_TruncatedFixedRecordDoesNotAmplifyAllocations(t *testing.T) {
	var declaration bytes.Buffer
	declaration.WriteString(`{"type":"record","name":"R","fields":[`)
	for i := range 8 {
		if i > 0 {
			declaration.WriteByte(',')
		}
		fmt.Fprintf(&declaration, `{"name":"f%d","type":{"type":"fixed","name":"F%d","size":1048576}}`, i, i)
	}
	declaration.WriteString(`]}`)
	schema := avro.MustParse(declaration.String())
	cfg := avro.Config{DisableCaching: true}.Freeze()

	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	var got any
	err := cfg.Unmarshal(schema, nil, &got)
	runtime.ReadMemStats(&after)

	require.Error(t, err)
	allocated := after.TotalAlloc - before.TotalAlloc
	assert.Less(t, allocated, uint64(4<<20), "allocated %d bytes for empty input", allocated)
}

func TestFix_ResetClearsReaderAndWriterErrors(t *testing.T) {
	r := avro.NewReader(nil, 0).Reset([]byte{0x80})
	_ = r.ReadInt()
	require.Error(t, r.Error)

	r.Reset([]byte{0x02})
	assert.Equal(t, int32(1), r.ReadInt())
	require.NoError(t, r.Error)

	w := avro.NewWriter(nil, 0)
	w.Error = io.ErrUnexpectedEOF
	w.Reset(nil)
	require.NoError(t, w.Error)
	w.WriteInt(1)
	require.NoError(t, w.Error)
}
