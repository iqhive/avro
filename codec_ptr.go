package avro

import (
	"errors"
	"fmt"
	"reflect"
	"unsafe"

	"github.com/modern-go/reflect2"
)

func decoderOfPtr(d *decoderContext, schema Schema, typ reflect2.Type) ValDecoder {
	if pointsToItself(typ) {
		return &errorDecoder{err: fmt.Errorf("avro: %s is a pointer to itself, which no value can end", typ)}
	}
	ptrType := typ.(*reflect2.UnsafePtrType)
	elemType := ptrType.Elem()

	decoder := decoderOfType(d, schema, elemType)

	return &dereferenceDecoder{typ: elemType, decoder: decoder}
}

// pointsToItself reports whether following typ's pointer elements leads back
// to a type already passed, as with `type P *P`. Building a codec for such a
// type would recurse for ever.
func pointsToItself(typ reflect2.Type) bool {
	seen := map[uintptr]bool{}
	for typ.Kind() == reflect.Pointer {
		if seen[typ.RType()] {
			return true
		}
		seen[typ.RType()] = true
		typ = typ.(*reflect2.UnsafePtrType).Elem()
	}
	return false
}

type dereferenceDecoder struct {
	typ     reflect2.Type
	decoder ValDecoder
}

func (d *dereferenceDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	if *(*unsafe.Pointer)(ptr) == nil {
		// Create new instance
		newPtr := d.typ.UnsafeNew()
		d.decoder.Decode(newPtr, r)
		*(*unsafe.Pointer)(ptr) = newPtr
		return
	}

	// Reuse existing instance
	d.decoder.Decode(*(*unsafe.Pointer)(ptr), r)
}

func encoderOfPtr(e *encoderContext, schema Schema, typ reflect2.Type) ValEncoder {
	if pointsToItself(typ) {
		return &errorEncoder{err: fmt.Errorf("avro: %s is a pointer to itself, which no value can end", typ)}
	}
	ptrType := typ.(*reflect2.UnsafePtrType)
	elemType := ptrType.Elem()

	enc := encoderOfType(e, schema, elemType)

	return &dereferenceEncoder{typ: elemType, encoder: enc}
}

type dereferenceEncoder struct {
	typ     reflect2.Type
	encoder ValEncoder
}

func (d *dereferenceEncoder) Encode(ptr unsafe.Pointer, w *Writer) {
	if *(*unsafe.Pointer)(ptr) == nil {
		w.Error = errors.New("avro: cannot encode nil pointer")
		return
	}

	d.encoder.Encode(*(*unsafe.Pointer)(ptr), w)
}

type referenceDecoder struct {
	decoder ValDecoder
}

func (decoder *referenceDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	decoder.decoder.Decode(unsafe.Pointer(&ptr), r)
}
