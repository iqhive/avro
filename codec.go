package avro

import (
	"errors"
	"fmt"
	"io"
	"math/big"
	"reflect"
	"time"
	"unsafe"

	"github.com/modern-go/reflect2"
)

var (
	timeType         = reflect.TypeFor[time.Time]()
	timeDurationType = reflect.TypeFor[time.Duration]()
	ratType          = reflect.TypeFor[big.Rat]()
	durType          = reflect.TypeFor[LogicalDuration]()
)

// errMaxDepth is the error for records, arrays and maps nested deeper than
// Config.MaxDepth. Decoders and encoders return it as it is, without the
// context they add to other errors on the way out.
var errMaxDepth = errors.New("avro: records, arrays and maps are nested deeper than Config.MaxDepth")

// maxErrorContext is how many levels of context (a field name, a Go type) an
// error collects on its way out of nested values. Each level builds a message
// that holds every level below it, so for an error deep in data nested
// thousands of levels, unbounded context took time and memory in proportion
// to the square of the depth. The levels nearest the error are the ones kept,
// and a "..." level marks where the rest were cut.
const maxErrorContext = 32

// contextError is an error with the levels of context it collected on its way
// out of nested values.
type contextError struct {
	msg    string
	levels int
	err    error
}

func (e *contextError) Error() string {
	return e.msg
}

func (e *contextError) Unwrap() error {
	return e.err
}

// contextLevels returns how many levels of context err holds. Only levels
// count, not the cause's own wraps ("reading long: ").
func contextLevels(err error) int {
	if ce, ok := err.(*contextError); ok {
		return ce.levels
	}
	return 0
}

// withContext returns err with one more level of context. Past
// maxErrorContext levels it adds "..." once, so a reader knows the outer
// levels were cut, and then nothing more. errMaxDepth gets no context.
func withContext(err error, context string) error {
	if errors.Is(err, errMaxDepth) {
		return err
	}
	levels := contextLevels(err)
	switch {
	case levels < maxErrorContext:
	case levels == maxErrorContext:
		context = "..."
	default:
		return err
	}
	return &contextError{msg: context + ": " + err.Error(), levels: levels + 1, err: err}
}

// takesNoContext reports whether err takes no more context on its way out:
// errMaxDepth takes none, and an error that holds maxErrorContext levels
// takes only the "..." that levelDecoder and levelEncoder add.
func takesNoContext(err error) bool {
	return errors.Is(err, errMaxDepth) || contextLevels(err) >= maxErrorContext
}

// asLevel makes the context a map codec added to err (which it does with
// fmt.Errorf) one level, and adds the "..." when err holds maxErrorContext
// levels already.
func asLevel(err error) error {
	if err == nil || errors.Is(err, errMaxDepth) || errors.Is(err, io.EOF) {
		return err
	}
	if levels := contextLevels(err); levels >= maxErrorContext {
		return withContext(err, "")
	}
	if _, ok := err.(*contextError); ok {
		return err
	}
	inner := errors.Unwrap(err)
	return &contextError{msg: err.Error(), levels: contextLevels(inner) + 1, err: err}
}

type null struct{}

// ValDecoder represents an internal value decoder.
//
// You should never use ValDecoder directly.
type ValDecoder interface {
	Decode(ptr unsafe.Pointer, r *Reader)
}

// ValEncoder represents an internal value encoder.
//
// You should never use ValEncoder directly.
type ValEncoder interface {
	Encode(ptr unsafe.Pointer, w *Writer)
}

// ReadVal parses Avro value and stores the result in the value pointed to by obj.
func (r *Reader) ReadVal(schema Schema, obj any) {
	if obj == nil {
		r.ReportError("ReadVal", "can not read into nil")
		return
	}
	decoder := r.cfg.getDecoderFromCache(schema.CacheFingerprint(), reflect2.RTypeOf(obj))
	if decoder == nil {
		typ := reflect2.TypeOf(obj)
		if typ.Kind() != reflect.Pointer {
			r.ReportError("ReadVal", "can only unmarshal into pointer")
			return
		}
		decoder = r.cfg.DecoderOf(schema, typ)
	}

	ptr := reflect2.PtrOf(obj)
	if ptr == nil {
		r.ReportError("ReadVal", "can not read into nil pointer")
		return
	}

	decoder.Decode(ptr, r)
}

// WriteVal writes the Avro encoding of obj.
func (w *Writer) WriteVal(schema Schema, val any) {
	encoder := w.cfg.getEncoderFromCache(schema.CacheFingerprint(), reflect2.RTypeOf(val))
	if encoder == nil {
		typ := reflect2.TypeOf(val)
		encoder = w.cfg.EncoderOf(schema, typ)
	}
	encoder.Encode(reflect2.PtrOf(val), w)
}

func (c *frozenConfig) DecoderOf(schema Schema, typ reflect2.Type) ValDecoder {
	rtype := typ.RType()
	decoder := c.getDecoderFromCache(schema.CacheFingerprint(), rtype)
	if decoder != nil {
		return decoder
	}

	ptrType := typ.(*reflect2.UnsafePtrType)
	decoder = decoderOfType(newDecoderContext(c), schema, ptrType.Elem())
	c.addDecoderToCache(schema.CacheFingerprint(), rtype, decoder)
	return decoder
}

type deferDecoder struct {
	decoder ValDecoder
}

func (d *deferDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	d.decoder.Decode(ptr, r)
}

type deferEncoder struct {
	encoder ValEncoder
}

func (d *deferEncoder) Encode(ptr unsafe.Pointer, w *Writer) {
	d.encoder.Encode(ptr, w)
}

type decoderContext struct {
	cfg      *frozenConfig
	decoders map[cacheKey]ValDecoder
	noBytes  noBytesMemo
}

func newDecoderContext(cfg *frozenConfig) *decoderContext {
	return &decoderContext{
		cfg:      cfg,
		decoders: make(map[cacheKey]ValDecoder),
	}
}

type encoderContext struct {
	cfg      *frozenConfig
	encoders map[cacheKey]ValEncoder
	noBytes  noBytesMemo
}

func newEncoderContext(cfg *frozenConfig) *encoderContext {
	return &encoderContext{
		cfg:      cfg,
		encoders: make(map[cacheKey]ValEncoder),
	}
}

//nolint:dupl
func decoderOfType(d *decoderContext, schema Schema, typ reflect2.Type) ValDecoder {
	if dec := createDecoderOfMarshaler(schema, typ); dec != nil {
		return dec
	}

	// Handle eface (empty interface) case when it isn't a union
	if typ.Kind() == reflect.Interface && schema.Type() != Union {
		if _, ok := typ.(*reflect2.UnsafeIFaceType); !ok {
			return newEfaceDecoder(d, schema)
		}
	}

	switch schema.Type() {
	case Null:
		return &nullCodec{}
	case String, Bytes, Int, Long, Float, Double, Boolean:
		return createDecoderOfNative(schema.(*PrimitiveSchema), typ)
	case Record:
		key := cacheKey{fingerprint: schema.CacheFingerprint(), rtype: typ.RType()}
		defDec := &deferDecoder{}
		d.decoders[key] = defDec
		defDec.decoder = createDecoderOfRecord(d, schema.(*RecordSchema), typ)
		return defDec.decoder
	case Ref:
		key := cacheKey{fingerprint: schema.(*RefSchema).Schema().CacheFingerprint(), rtype: typ.RType()}
		if dec, f := d.decoders[key]; f {
			return dec
		}
		return decoderOfType(d, schema.(*RefSchema).Schema(), typ)
	case Enum:
		return createDecoderOfEnum(schema.(*EnumSchema), typ)
	case Array:
		return createDecoderOfArray(d, schema.(*ArraySchema), typ)
	case Map:
		return &levelDecoder{decoder: createDecoderOfMap(d, schema.(*MapSchema), typ)}
	case Union:
		return createDecoderOfUnion(d, schema.(*UnionSchema), typ)
	case Fixed:
		return createDecoderOfFixed(schema.(*FixedSchema), typ)
	default:
		// It is impossible to get here with a valid schema
		return &errorDecoder{err: fmt.Errorf("avro: schema type %s is unsupported", schema.Type())}
	}
}

func (c *frozenConfig) EncoderOf(schema Schema, typ reflect2.Type) ValEncoder {
	if typ == nil {
		typ = reflect2.TypeOf((*null)(nil))
	}

	rtype := typ.RType()
	encoder := c.getEncoderFromCache(schema.CacheFingerprint(), rtype)
	if encoder != nil {
		return encoder
	}

	encoder = encoderOfType(newEncoderContext(c), schema, typ)
	if typ.LikePtr() {
		encoder = &onePtrEncoder{encoder}
	}
	c.addEncoderToCache(schema.CacheFingerprint(), rtype, encoder)
	return encoder
}

type onePtrEncoder struct {
	enc ValEncoder
}

func (e *onePtrEncoder) Encode(ptr unsafe.Pointer, w *Writer) {
	e.enc.Encode(noescape(unsafe.Pointer(&ptr)), w)
}

//nolint:dupl
func encoderOfType(e *encoderContext, schema Schema, typ reflect2.Type) ValEncoder {
	if enc := createEncoderOfMarshaler(schema, typ); enc != nil {
		return enc
	}

	if typ.Kind() == reflect.Interface {
		return &interfaceEncoder{schema: schema, typ: typ}
	}

	switch schema.Type() {
	case Null:
		return &nullCodec{}
	case String, Bytes, Int, Long, Float, Double, Boolean:
		return createEncoderOfNative(schema.(*PrimitiveSchema), typ)
	case Record:
		key := cacheKey{fingerprint: schema.CacheFingerprint(), rtype: typ.RType()}
		defEnc := &deferEncoder{}
		e.encoders[key] = defEnc
		defEnc.encoder = createEncoderOfRecord(e, schema.(*RecordSchema), typ)
		return defEnc.encoder
	case Ref:
		key := cacheKey{fingerprint: schema.(*RefSchema).Schema().CacheFingerprint(), rtype: typ.RType()}
		if enc, f := e.encoders[key]; f {
			return enc
		}
		return encoderOfType(e, schema.(*RefSchema).Schema(), typ)
	case Enum:
		return createEncoderOfEnum(schema.(*EnumSchema), typ)
	case Array:
		return createEncoderOfArray(e, schema.(*ArraySchema), typ)
	case Map:
		return &levelEncoder{encoder: createEncoderOfMap(e, schema.(*MapSchema), typ)}
	case Union:
		return createEncoderOfUnion(e, schema.(*UnionSchema), typ)
	case Fixed:
		return createEncoderOfFixed(schema.(*FixedSchema), typ)
	default:
		// It is impossible to get here with a valid schema
		return &errorEncoder{err: fmt.Errorf("avro: schema type %s is unsupported", schema.Type())}
	}
}

// levelDecoder counts the value its decoder reads as one level of nesting
// against Config.MaxDepth: a map's values may be maps or arrays, each another
// step of Go recursion. Records and arrays count themselves. It also counts
// the context the map decoder adds to an error as one level (asLevel).
type levelDecoder struct {
	decoder ValDecoder
}

func (d *levelDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	if !r.enter() {
		return
	}
	d.decoder.Decode(ptr, r)
	if r.Error != nil {
		r.Error = asLevel(r.Error)
	}
	r.leave()
}

// levelEncoder counts the value its encoder writes as one level of nesting,
// as levelDecoder does.
type levelEncoder struct {
	encoder ValEncoder
}

func (e *levelEncoder) Encode(ptr unsafe.Pointer, w *Writer) {
	if !w.enter() {
		return
	}
	e.encoder.Encode(ptr, w)
	if w.Error != nil {
		w.Error = asLevel(w.Error)
	}
	w.leave()
}

type errorDecoder struct {
	err error
}

func (d *errorDecoder) Decode(_ unsafe.Pointer, r *Reader) {
	if r.Error == nil {
		r.Error = d.err
	}
}

type errorEncoder struct {
	err error
}

func (e *errorEncoder) Encode(_ unsafe.Pointer, w *Writer) {
	if w.Error == nil {
		w.Error = e.err
	}
}
