package avro

import (
	"fmt"
	"unsafe"
)

func createSkipDecoder(schema Schema) ValDecoder {
	return createSkipDecoderWithCache(schema, make(map[[32]byte]ValDecoder))
}

// createSkipDecoderWithCache skips schema as the writer wrote it: a schema
// resolved against a writer's holds the reader's shape, and a skip must read
// the writer's bytes.
func createSkipDecoderWithCache(schema Schema, cache map[[32]byte]ValDecoder) ValDecoder {
	typ := schema.Type()
	if prim, ok := schema.(*PrimitiveSchema); ok && prim.encodedType != "" {
		// A promoted primitive (an int read as a double) was written as the
		// writer's type.
		typ = prim.encodedType
	}
	switch typ {
	case Null:
		// A null takes no bytes.
		return &nullCodec{}

	case Boolean:
		return &boolSkipDecoder{}

	case Int:
		return &intSkipDecoder{}

	case Long:
		return &longSkipDecoder{}

	case Float:
		return &floatSkipDecoder{}

	case Double:
		return &doubleSkipDecoder{}

	case String:
		return &stringSkipDecoder{}

	case Bytes:
		return &bytesSkipDecoder{}

	case Record:
		fingerprint := schema.CacheFingerprint()
		if decoder, ok := cache[fingerprint]; ok {
			return decoder
		}

		deferred := &deferDecoder{}
		cache[fingerprint] = deferred
		deferred.decoder = skipDecoderOfRecord(schema, cache)
		return deferred.decoder

	case Ref:
		ref := schema.(*RefSchema).Schema()
		if decoder, ok := cache[ref.CacheFingerprint()]; ok {
			return decoder
		}
		return createSkipDecoderWithCache(ref, cache)

	case Enum:
		return &enumSkipDecoder{symbols: schema.(*EnumSchema).Symbols()}

	case Array:
		return skipDecoderOfArray(schema, cache)

	case Map:
		return skipDecoderOfMap(schema, cache)

	case Union:
		return skipDecoderOfUnion(schema, cache)

	case Fixed:
		return &fixedSkipDecoder{size: schema.(*FixedSchema).Size()}

	default:
		return &errorDecoder{err: fmt.Errorf("avro: schema type %s is unsupported", schema.Type())}
	}
}

type boolSkipDecoder struct{}

func (*boolSkipDecoder) Decode(_ unsafe.Pointer, r *Reader) {
	r.SkipBool()
}

type intSkipDecoder struct{}

func (*intSkipDecoder) Decode(_ unsafe.Pointer, r *Reader) {
	r.SkipInt()
}

type longSkipDecoder struct{}

func (*longSkipDecoder) Decode(_ unsafe.Pointer, r *Reader) {
	r.SkipLong()
}

type floatSkipDecoder struct{}

func (*floatSkipDecoder) Decode(_ unsafe.Pointer, r *Reader) {
	r.SkipFloat()
}

type doubleSkipDecoder struct{}

func (*doubleSkipDecoder) Decode(_ unsafe.Pointer, r *Reader) {
	r.SkipDouble()
}

type stringSkipDecoder struct{}

func (*stringSkipDecoder) Decode(_ unsafe.Pointer, r *Reader) {
	r.SkipString()
}

type bytesSkipDecoder struct{}

func (c *bytesSkipDecoder) Decode(_ unsafe.Pointer, r *Reader) {
	r.SkipBytes()
}

func skipDecoderOfRecord(schema Schema, cache map[[32]byte]ValDecoder) ValDecoder {
	rec := schema.(*RecordSchema)

	decoders := make([]ValDecoder, 0, len(rec.Fields()))
	noBytes := true
	for _, field := range rec.Fields() {
		// Resolution fills a field the writer lacks from its default: the
		// data does not hold it.
		if field.action == FieldSetDefault {
			continue
		}
		decoder := createSkipDecoderWithCache(field.Type(), cache)
		decoders = append(decoders, decoder)
		noBytes = noBytes && skipsNoBytes(decoder)
	}

	return &recordSkipDecoder{
		decoders: decoders,
		noBytes:  noBytes,
	}
}

type recordSkipDecoder struct {
	decoders []ValDecoder
	// noBytes is whether every field skips no bytes.
	noBytes bool
}

// skipsNoBytes reports whether skip decoder d reads no bytes, whatever the
// input holds. A record whose decoder is still being built (d is then its
// deferDecoder with no decoder yet) is recursive, and with no union (which
// takes a byte) in between it never ends: it counts as reading bytes.
func skipsNoBytes(d ValDecoder) bool {
	switch d := d.(type) {
	case *nullCodec:
		return true
	case *fixedSkipDecoder:
		return d.size == 0
	case *recordSkipDecoder:
		return d.noBytes
	case *deferDecoder:
		return d.decoder != nil && skipsNoBytes(d.decoder)
	default:
		return false
	}
}

func (d *recordSkipDecoder) Decode(_ unsafe.Pointer, r *Reader) {
	if !r.enter() {
		return
	}
	for _, decoder := range d.decoders {
		decoder.Decode(nil, r)
		if r.Error != nil {
			break
		}
	}
	r.leave()
}

type enumSkipDecoder struct {
	symbols []string
}

func (c *enumSkipDecoder) Decode(_ unsafe.Pointer, r *Reader) {
	r.SkipInt()
}

func skipDecoderOfArray(schema Schema, cache map[[32]byte]ValDecoder) ValDecoder {
	arr := schema.(*ArraySchema)
	decoder := createSkipDecoderWithCache(arr.Items(), cache)

	return &sliceSkipDecoder{
		decoder: decoder,
		noBytes: skipsNoBytes(decoder),
	}
}

type sliceSkipDecoder struct {
	decoder ValDecoder
	// noBytes is whether the elements skip no bytes.
	noBytes bool
}

func (d *sliceSkipDecoder) Decode(_ unsafe.Pointer, r *Reader) {
	if !r.enter() {
		return
	}
	for {
		l, size := r.ReadBlockHeader()
		if l == 0 {
			break
		}

		// Elements that take no bytes leave nothing to skip, so a block of
		// them is skipped in one step, whatever its count claims.
		if size > 0 || d.noBytes {
			r.SkipNBytes(size)
			continue
		}

		for range l {
			d.decoder.Decode(nil, r)
			if r.Error != nil {
				break
			}
		}
	}
	r.leave()
}

func skipDecoderOfMap(schema Schema, cache map[[32]byte]ValDecoder) ValDecoder {
	m := schema.(*MapSchema)
	decoder := createSkipDecoderWithCache(m.Values(), cache)

	return &mapSkipDecoder{
		decoder: decoder,
	}
}

type mapSkipDecoder struct {
	decoder ValDecoder
}

func (d *mapSkipDecoder) Decode(_ unsafe.Pointer, r *Reader) {
	if !r.enter() {
		return
	}
	for {
		l, size := r.ReadBlockHeader()
		if l == 0 {
			break
		}

		if size > 0 {
			r.SkipNBytes(size)
			continue
		}

		for range l {
			r.SkipString()
			d.decoder.Decode(nil, r)
			if r.Error != nil {
				break
			}
		}
	}
	r.leave()
}

func skipDecoderOfUnion(schema Schema, cache map[[32]byte]ValDecoder) ValDecoder {
	union := schema.(*UnionSchema)

	decoders := make([]ValDecoder, len(union.Types()))
	for i, typ := range union.Types() {
		if typ.Type() == Null {
			continue
		}
		decoders[i] = createSkipDecoderWithCache(typ, cache)
	}

	return &unionSkipDecoder{
		schema:   union,
		decoders: decoders,
	}
}

type unionSkipDecoder struct {
	schema   *UnionSchema
	decoders []ValDecoder
}

func (d *unionSkipDecoder) Decode(_ unsafe.Pointer, r *Reader) {
	idx, resSchema := getUnionSchema(d.schema, r)
	if resSchema == nil {
		return
	}

	// Null branches have no decoder and no payload.
	if d.decoders[idx] == nil {
		return
	}

	d.decoders[idx].Decode(nil, r)
}

type fixedSkipDecoder struct {
	size int
}

func (d *fixedSkipDecoder) Decode(_ unsafe.Pointer, r *Reader) {
	r.SkipNBytes(d.size)
}
