package avro

import (
	"errors"
	"fmt"
	"io"
	"reflect"
	"unsafe"

	"github.com/modern-go/reflect2"
)

func createDecoderOfArray(d *decoderContext, schema *ArraySchema, typ reflect2.Type) ValDecoder {
	if typ.Kind() != reflect.Slice {
		return &errorDecoder{err: fmt.Errorf("avro: %s is unsupported for Avro %s", typ.String(), schema.Type())}
	}

	sliceType := typ.(*reflect2.UnsafeSliceType)
	if dec, ok := newScalarArrayDecoder(schema, sliceType); ok {
		return dec
	}

	return decoderOfArray(d, schema, typ)
}

func createEncoderOfArray(e *encoderContext, schema *ArraySchema, typ reflect2.Type) ValEncoder {
	if typ.Kind() != reflect.Slice {
		return &errorEncoder{err: fmt.Errorf("avro: %s is unsupported for Avro %s", typ.String(), schema.Type())}
	}

	sliceType := typ.(*reflect2.UnsafeSliceType)
	if enc, ok := newScalarArrayEncoder(e, schema, sliceType); ok {
		return enc
	}

	return encoderOfArray(e, schema, typ)
}

func decoderOfArray(d *decoderContext, arr *ArraySchema, typ reflect2.Type) ValDecoder {
	sliceType := typ.(*reflect2.UnsafeSliceType)
	decoder := decoderOfType(d, arr.Items(), sliceType.Elem())

	return &arrayDecoder{typ: sliceType, decoder: decoder, noBytes: d.noBytes.takesNoBytes(arr.Items())}
}

type arrayDecoder struct {
	typ     *reflect2.UnsafeSliceType
	decoder ValDecoder
	// noBytes is whether the elements take no bytes of the input.
	noBytes bool
}

func (d *arrayDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	if !r.enter() {
		return
	}

	var size int
	sliceType := d.typ

	if sliceType.UnsafeIsNil(ptr) {
		sliceType.UnsafeSet(ptr, sliceType.UnsafeMakeSlice(0, 0))
	}
	// A reused slice holds the previous value: an empty array must leave it
	// empty.
	sliceType.UnsafeGrow(ptr, 0)

	for {
		l, _ := r.ReadBlockHeader()
		if l == 0 {
			break
		}

		if l > r.cfg.getMaxSliceAllocSize()-size {
			r.ReportError("decode array", "size is greater than `Config.MaxSliceAllocSize`")
			r.leave()
			return
		}
		if d.noBytes && !r.takeNoBytes(l) {
			r.leave()
			return
		}

		for end := size + l; size < end; {
			n := arrayGrowth(size, end)
			sliceType.UnsafeGrow(ptr, n)
			for ; size < n; size++ {
				elemPtr := sliceType.UnsafeGetIndex(ptr, size)
				d.decoder.Decode(elemPtr, r)
				if r.Error != nil {
					r.Error = withContext(r.Error, "reading "+d.typ.String())
					r.leave()
					return
				}
			}
		}
	}

	if r.Error != nil && !errors.Is(r.Error, io.EOF) {
		r.Error = withContext(r.Error, d.typ.String())
	}
	r.leave()
}

// minArrayGrowth is the fewest elements an array decoder adds to a slice at a
// time.
const minArrayGrowth = 1024

// arrayGrowth returns the length to grow a slice of size decoded elements to,
// on the way to end, the length its block headers claim. It grows by at most
// what has been decoded so far (and at least minArrayGrowth), so a block count
// read from the data cannot make the decoder allocate memory that the data
// does not fill: the allocation stays within about twice what was decoded.
// Elements that take no bytes are decoded without the data, so the decoder
// counts them against the input with Reader.takeNoBytes first.
func arrayGrowth(size, end int) int {
	return size + min(end-size, max(size, minArrayGrowth))
}

// maxNoBytesBlock is the longest block the encoder writes of array elements
// that take no bytes: its count then takes one byte, so the decoders, which
// take noBytesPerByte of them for each byte, read this library's output back
// at any Config.BlockLength.
const maxNoBytesBlock = noBytesPerByte

// noBytesMemo holds takesNoBytes for the records it has looked at. It makes
// its map only when it meets a record, as most decoder and encoder builds
// never do.
type noBytesMemo struct {
	records map[*RecordSchema]bool
}

// takesNoBytes reports whether every value of schema is encoded in no bytes: a
// null, a fixed of size 0, or a record whose fields all take no bytes.
func (m *noBytesMemo) takesNoBytes(schema Schema) bool {
	switch schema.Type() {
	case Null:
		return true
	case Fixed:
		return schema.(*FixedSchema).Size() == 0
	case Ref:
		return m.takesNoBytes(schema.(*RefSchema).Schema())
	case Record:
		rec := schema.(*RecordSchema)
		if noBytes, ok := m.records[rec]; ok {
			return noBytes
		}
		if m.records == nil {
			m.records = make(map[*RecordSchema]bool)
		}
		// A record met again while its fields are looked at is recursive,
		// and with no union (which takes a byte) in between it never ends:
		// it counts as taking bytes, so it is decoded, and fails, element by
		// element.
		m.records[rec] = false
		noBytes := true
		for _, f := range rec.Fields() {
			// Resolution fills a field the writer lacks from its default,
			// which reads nothing.
			if f.action != FieldSetDefault && !m.takesNoBytes(f.Type()) {
				noBytes = false
				break
			}
		}
		m.records[rec] = noBytes
		return noBytes
	default:
		return false
	}
}

func encoderOfArray(e *encoderContext, arr *ArraySchema, typ reflect2.Type) ValEncoder {
	sliceType := typ.(*reflect2.UnsafeSliceType)
	encoder := encoderOfType(e, arr.Items(), sliceType.Elem())
	blockLength := e.cfg.getBlockLength()
	if e.noBytes.takesNoBytes(arr.Items()) {
		blockLength = min(blockLength, maxNoBytesBlock)
	}

	return &arrayEncoder{
		blockLength: blockLength,
		typ:         sliceType,
		encoder:     encoder,
	}
}

type arrayEncoder struct {
	blockLength int
	typ         *reflect2.UnsafeSliceType
	encoder     ValEncoder
}

func (e *arrayEncoder) Encode(ptr unsafe.Pointer, w *Writer) {
	if !w.enter() {
		return
	}

	blockLength := e.blockLength
	length := e.typ.UnsafeLengthOf(ptr)

	for i := 0; i < length; i += blockLength {
		w.WriteBlockCB(func(w *Writer) int64 {
			count := int64(0)
			for j := i; j < i+blockLength && j < length; j++ {
				elemPtr := e.typ.UnsafeGetIndex(ptr, j)
				e.encoder.Encode(elemPtr, w)
				if w.Error != nil && !errors.Is(w.Error, io.EOF) {
					w.Error = withContext(w.Error, e.typ.String())
					return count
				}
				count++
			}

			return count
		})
		// Stop at an error: encoding the rest could only repeat it, and for a
		// value that refers to itself, would repeat it at every level.
		if w.Error != nil {
			w.leave()
			return
		}
	}

	w.WriteBlockHeader(0, 0)

	if w.Error != nil && !errors.Is(w.Error, io.EOF) {
		w.Error = withContext(w.Error, e.typ.String())
	}
	w.leave()
}
