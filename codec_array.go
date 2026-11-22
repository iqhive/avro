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

	return &arrayDecoder{typ: sliceType, decoder: decoder}
}

type arrayDecoder struct {
	typ     *reflect2.UnsafeSliceType
	decoder ValDecoder
}

func (d *arrayDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
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
			return
		}

		for end := size + l; size < end; {
			n := arrayGrowth(size, end)
			sliceType.UnsafeGrow(ptr, n)
			for ; size < n; size++ {
				elemPtr := sliceType.UnsafeGetIndex(ptr, size)
				d.decoder.Decode(elemPtr, r)
				if r.Error != nil {
					if !isMaxDepth(r.Error) {
						r.Error = fmt.Errorf("reading %s: %w", d.typ.String(), r.Error)
					}
					return
				}
			}
		}
	}

	if r.Error != nil && !errors.Is(r.Error, io.EOF) && !isMaxDepth(r.Error) {
		r.Error = fmt.Errorf("%v: %w", d.typ, r.Error)
	}
}

// minArrayGrowth is the fewest elements an array decoder adds to a slice at a
// time.
const minArrayGrowth = 1024

// arrayGrowth returns the length to grow a slice of size decoded elements to,
// on the way to end, the length its block headers claim. It grows by at most
// what has been decoded so far (and at least minArrayGrowth), so a block count
// read from the data cannot make the decoder allocate memory that the data
// does not fill: the allocation stays within about twice what was decoded.
func arrayGrowth(size, end int) int {
	return size + min(end-size, max(size, minArrayGrowth))
}

func encoderOfArray(e *encoderContext, arr *ArraySchema, typ reflect2.Type) ValEncoder {
	sliceType := typ.(*reflect2.UnsafeSliceType)
	encoder := encoderOfType(e, arr.Items(), sliceType.Elem())

	return &arrayEncoder{
		blockLength: e.cfg.getBlockLength(),
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
	blockLength := e.blockLength
	length := e.typ.UnsafeLengthOf(ptr)

	for i := 0; i < length; i += blockLength {
		w.WriteBlockCB(func(w *Writer) int64 {
			count := int64(0)
			for j := i; j < i+blockLength && j < length; j++ {
				elemPtr := e.typ.UnsafeGetIndex(ptr, j)
				e.encoder.Encode(elemPtr, w)
				if w.Error != nil && !errors.Is(w.Error, io.EOF) {
					if !isMaxDepth(w.Error) {
						w.Error = fmt.Errorf("%s: %w", e.typ.String(), w.Error)
					}
					return count
				}
				count++
			}

			return count
		})
		// Stop at an error: encoding the rest could only repeat it, and for a
		// value that refers to itself, would repeat it at every level.
		if w.Error != nil {
			return
		}
	}

	w.WriteBlockHeader(0, 0)

	if w.Error != nil && !errors.Is(w.Error, io.EOF) && !isMaxDepth(w.Error) {
		w.Error = fmt.Errorf("%v: %w", e.typ, w.Error)
	}
}
