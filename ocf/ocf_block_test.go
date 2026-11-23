package ocf_test

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iqhive/avro/v2"
	"github.com/iqhive/avro/v2/ocf"
)

// A record that takes no bytes (an empty record) is valid, and the encoder
// writes a block of three of them with no data, but Decode returned io.EOF for
// each: the value decoder read an exhausted block as the end of a stream.
func TestDecoder_ZeroWidthRecords(t *testing.T) {
	type empty struct{}
	var buf bytes.Buffer
	enc, err := ocf.NewEncoder(`{"type":"record","name":"ocfblock.Empty","fields":[]}`, &buf)
	require.NoError(t, err)
	for range 3 {
		require.NoError(t, enc.Encode(empty{}))
	}
	require.NoError(t, enc.Close())

	dec, err := ocf.NewDecoder(&buf)
	require.NoError(t, err)
	n := 0
	for dec.HasNext() {
		require.NoError(t, dec.Decode(&empty{}), "record %d", n)
		n++
	}
	require.NoError(t, dec.Error())
	assert.Equal(t, 3, n)

	// Long files of them read back too, whatever the block length, as do
	// files of records holding arrays of nulls.
	for _, blockLength := range []int{0, 100, 5000} {
		buf.Reset()
		enc, err = ocf.NewEncoder(`{"type":"record","name":"ocfblock.Empty","fields":[]}`, &buf, ocf.WithBlockLength(blockLength))
		require.NoError(t, err)
		for range 5000 {
			require.NoError(t, enc.Encode(empty{}))
		}
		require.NoError(t, enc.Close())
		assert.Equal(t, 5000, countRecords(t, buf.Bytes(), func() any { return &empty{} }), "block length %d", blockLength)
	}
	buf.Reset()
	enc, err = ocf.NewEncoder(`{"type":"array","items":"null"}`, &buf)
	require.NoError(t, err)
	for range 300 {
		require.NoError(t, enc.Encode(make([]any, 13)))
	}
	require.NoError(t, enc.Close())
	assert.Equal(t, 300, countRecords(t, buf.Bytes(), func() any { return new([]any) }))
}

// countRecords reads every record of an OCF file and fails t on an error.
func countRecords(t *testing.T, file []byte, into func() any) int {
	t.Helper()
	dec, err := ocf.NewDecoder(bytes.NewReader(file))
	require.NoError(t, err)
	n := 0
	for dec.HasNext() {
		require.NoError(t, dec.Decode(into()), "record %d", n)
		n++
	}
	require.NoError(t, dec.Error())
	return n
}

// zigzag appends v as an Avro long.
func zigzag(b []byte, v int64) []byte {
	return binary.AppendUvarint(b, uint64(v<<1)^uint64(v>>63))
}

// fileOfBlocks is an OCF file of the schema with the given blocks of raw data.
func fileOfBlocks(t *testing.T, schema string, blocks ...[]byte) []byte {
	t.Helper()
	sync := [16]byte{5, 5, 5}
	var buf bytes.Buffer
	enc, err := ocf.NewEncoderWithSchema(avro.MustParse(schema), &buf, ocf.WithSyncBlock(sync))
	require.NoError(t, err)
	require.NoError(t, enc.Flush())
	for _, b := range blocks {
		buf.Write(b)
		buf.Write(sync[:])
	}
	return buf.Bytes()
}

// Records that take no bytes cost a block nothing, so a block of size 0 could
// claim any count: a 120-byte file claiming 2^62 empty records yielded records
// for thousands of years. A block may hold as many such records as an array may
// hold elements that take no bytes: 8 for each byte of its count, plus 1024,
// and no more than Config.MaxSliceAllocSize.
func TestDecoder_ZeroWidthRecordsAreBounded(t *testing.T) {
	const schema = `{"type":"record","name":"ocfblock.Nothing","fields":[{"name":"n","type":"null"}]}`
	type nothing struct{}
	lowered := avro.Config{MaxSliceAllocSize: 100}.Freeze()
	for _, tc := range []struct {
		count int64
		cfg   avro.API
		ok    bool
	}{
		{1040, avro.DefaultConfig, true},
		{1041, avro.DefaultConfig, false},
		{1 << 62, avro.DefaultConfig, false},
		{100, lowered, true},
		{101, lowered, false},
	} {
		file := fileOfBlocks(t, schema, zigzag(zigzag(nil, tc.count), 0))
		dec, err := ocf.NewDecoder(bytes.NewReader(file), ocf.WithDecoderConfig(tc.cfg))
		require.NoError(t, err)
		if !tc.ok {
			assert.False(t, dec.HasNext(), "count %d", tc.count)
			assert.ErrorContains(t, dec.Error(), "take no bytes", "count %d", tc.count)
			continue
		}
		n := 0
		for dec.HasNext() {
			require.NoError(t, dec.Decode(&nothing{}))
			n++
		}
		require.NoError(t, dec.Error())
		assert.Equal(t, int(tc.count), n)
	}
}

// Data left in a block after its last record was dropped silently, or read as
// the next block's first record, so a count that was too low lost records
// without an error. Java's reader refuses such a block, and so does this one.
func TestDecoder_DataLeftInBlockIsAnError(t *testing.T) {
	file := fileOfBlocks(t, `"long"`,
		[]byte{0x02, 0x04, 0x02, 0x04}, // count 1, size 2, the longs 1 and 2
		[]byte{0x02, 0x02, 0x06},       // count 1, size 1, the long 3
	)
	dec, err := ocf.NewDecoder(bytes.NewReader(file))
	require.NoError(t, err)
	var got []int64
	for dec.HasNext() {
		var v int64
		require.NoError(t, dec.Decode(&v))
		got = append(got, v)
	}
	assert.Equal(t, []int64{1}, got)
	assert.ErrorContains(t, dec.Error(), "more data than its records")
}

// A block whose count claims more records than its data holds made Decode
// return io.EOF, the clean end of a stream, and Error() stayed nil, so a caller
// that stops at io.EOF read a corrupt file as complete.
func TestDecoder_ShortBlockIsAnError(t *testing.T) {
	sync := [16]byte{7, 7, 7}
	var buf bytes.Buffer
	enc, err := ocf.NewEncoderWithSchema(avro.MustParse(`"long"`), &buf, ocf.WithSyncBlock(sync))
	require.NoError(t, err)
	require.NoError(t, enc.Flush())
	buf.Write([]byte{0x06, 0x04, 0x02, 0x04}) // count 3, size 2, the longs 1 and 2 only
	buf.Write(sync[:])
	buf.Write([]byte{0x02, 0x02, 0x06}) // count 1, size 1, the long 3
	buf.Write(sync[:])

	dec, err := ocf.NewDecoder(&buf)
	require.NoError(t, err)
	var got []int64
	var last error
	for dec.HasNext() {
		var v int64
		if last = dec.Decode(&v); last != nil {
			break
		}
		got = append(got, v)
	}
	assert.Equal(t, []int64{1, 2}, got)
	assert.ErrorIs(t, last, io.ErrUnexpectedEOF)
	assert.ErrorIs(t, dec.Error(), io.ErrUnexpectedEOF)
	assert.False(t, dec.HasNext(), "nothing is read past a corrupt block")
}
