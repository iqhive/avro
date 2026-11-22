<picture>
  <source media="(prefers-color-scheme: dark)" srcset="http://svg.wiersma.co.za/hamba/project?title=avro&tag=A%20fast%20Go%20avro%20codec&mode=dark">
  <source media="(prefers-color-scheme: light)" srcset="http://svg.wiersma.co.za/hamba/project?title=avro&tag=A%20fast%20Go%20avro%20codec">
  <img alt="Logo" src="http://svg.wiersma.co.za/hamba/project?title=avro&tag=A%20fast%20Go%20avro%20codec">
</picture>

[![Go Report Card](https://goreportcard.com/badge/github.com/iqhive/avro/v2)](https://goreportcard.com/report/github.com/iqhive/avro/v2)
[![Build Status](https://github.com/iqhive/avro/actions/workflows/test.yml/badge.svg)](https://github.com/iqhive/avro/actions)
[![Go Reference](https://pkg.go.dev/badge/github.com/iqhive/avro/v2.svg)](https://pkg.go.dev/github.com/iqhive/avro/v2)
[![GitHub release](https://img.shields.io/github/release/iqhive/avro.svg)](https://github.com/iqhive/avro/releases)
[![GitHub license](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/iqhive/avro/blob/main/LICENCE)

A fast Go avro codec

---

## iqhive/avro

This is iqhive's fork of [hamba/avro](https://github.com/hamba/avro), which was archived in January 2026. It carries every change made in [iskorotkov/avro](https://github.com/iskorotkov/avro), the most active maintained fork, plus fixes of its own.

- The module path is `github.com/iqhive/avro/v2`. Moving from hamba/avro or iskorotkov/avro means changing the import paths.
- iskorotkov/avro's main branch is merged into this repository's `iskorotkov` branch every day. Once a week, what that branch brings in is tested and reviewed, and merged into `main` when the review finds no new bugs or security issues.

Fixed in iqhive/avro (each has a regression test in `fork_fixes_test.go`):

- **Memory safety:** a union decoded into a struct held by value, or into a value-held `UnionConverter`, wrote through
  the struct's first field as a pointer; a `null` into a reused slice left its length over a nil array; a union decoded
  into `any` through a registered pointer type stored a pointer where the garbage collector does not look for one; a
  type converter's result was stored unchecked; and the encoder read a struct under a bytes decimal schema as a
  `big.Rat`, past its end.
- **Crashes:** deeply nested data for a recursive schema, a Go value that refers to itself, `type P *P`, and resolving a
  recursive schema overflowed the goroutine stack (a fatal error). Records may now nest `Config.MaxDepth` deep
  (default 10000). Encoding a nil `*big.Rat`, a missing field whose union default is `null`, and `Unmarshal` into
  `nil` panicked.
- **Denial of service:** array and map block counts no longer drive allocation before the data is read, and a map
  with `TextUnmarshaler` keys stops at the first error.
- **Silent data loss:** truncated input was decoded without error; integers that do not fit the Go or Avro type were
  wrapped; a `big.Rat` value decoded as 0; reused slices, maps, unions and converters kept the previous message's
  values; appending to a decoded `[]byte` overwrote the values decoded after it; pre-1970 dates with a time of day
  were a day late; negative decimals rounded away from zero; fixed decimals accepted a precision their size cannot
  hold; defaults that failed to decode were dropped; and encoders were cached without regard to field defaults.
- **Schema registry client:** a subject was put into the request path unescaped, so one holding `/`, `?`, `#` or `..`
  reached another endpoint (`DeleteSubject("../config")` sent a DELETE for `/config`, the global configuration), and a message whose
  payload encodes to no bytes was refused as too short.
- **Other:** an int or long varint with bits past the type's width read as a different, wrapped value (and `ReadNext`
  disagreed with the decoders about such a union branch); skipping a `null` field failed; skipping accepted over-long
  varints; a map record's union default was looked up by type instead of name; and the fixed-to-`uint64` codec raced
  under concurrent use.

## Why this fork?

`hamba/avro` was archived in January 2026 and will receive no further updates or bug fixes. There is no other actively maintained Go Avro library that matches its performance. This fork exists to keep the library maintained, fix bugs, and improve it further.

Unresolved issues from hamba/avro are tracked in [ISSUES.md](./ISSUES.md).

## Changes since fork

- **Feature:** added Avro 1.12 nanosecond-precision timestamp logical types (`timestamp-nanos`, `local-timestamp-nanos`)
- **Bug fix:** timestamp encoders return an error when the value does not fit the `long` range of the schema precision instead of silently wrapping
- **Bug fix:** corrected `local-timestamp-*` decoding to reinterpret wall-clock components in the local zone instead of subtracting the zone offset, fixing values around DST transitions
- **Bug fix:** `time.Duration` values are now rejected for `local-timestamp-*` schemas instead of being silently encoded as raw nanoseconds
- **Bug fix:** registered missing logical types for local timestamps (`local-timestamp-millis`, `local-timestamp-micros`), which caused decoding errors
- **Bug fix:** fixed enum type duplication in Go code generation (`avrogen`)
- **Go 1.26+ required:** minimum Go version raised to 1.26; codebase updated to use latest Go idioms
- **CI updated:** tested on Go 1.26 and 1.27; tools (golangci-lint, gotestsum) pinned via `go.mod` `tool` directive
- **19.5% decode speedup on internally used benchmark:** optimized inlining and bounds checks in number parsing; iskorotkov/avro now decodes at 118.6 ns/op vs hamba/avro's 147.3 ns/op (Apple M4 Pro, zero allocations)

## How to migrate from hamba/avro?

You have 2 options:

1. Use `replace` directive in `go.mod` and replace hamba/avro with this library. Only 1 file needs to be changed.
2. Replace hamba/avro with iskorotkov/avro in `go.mod`, then rewrite imports in all Go files from hamba/avro to iskorotkov/avro. A lot of files must be updated, but it is a cleaner approach and can be automated with bash script or LLM.

## Overview

Install with:

```shell
go get github.com/iqhive/avro/v2
```

**Note:** This project has renamed the default branch from `master` to `main`. You will need to update your local environment.

## Usage

```go
type SimpleRecord struct {
	A int64  `avro:"a"`
	B string `avro:"b"`
}

schema, err := avro.Parse(`{
    "type": "record",
    "name": "simple",
    "namespace": "org.iskorotkov.avro",
    "fields" : [
        {"name": "a", "type": "long"},
        {"name": "b", "type": "string"}
    ]
}`)
if err != nil {
	log.Fatal(err)
}

in := SimpleRecord{A: 27, B: "foo"}

data, err := avro.Marshal(schema, in)
if err != nil {
	log.Fatal(err)
}

fmt.Println(data)
// Outputs: [54 6 102 111 111]

out := SimpleRecord{}
err = avro.Unmarshal(schema, data, &out)
if err != nil {
	log.Fatal(err)
}

fmt.Println(out)
// Outputs: {27 foo}
```

More examples in the [godoc](https://pkg.go.dev/github.com/iqhive/avro/v2).

### Types Conversions

| Avro                          | Go Struct                                                  | Go Interface             |
| ----------------------------- | ---------------------------------------------------------- | ------------------------ |
| `null`                        | `nil`                                                      | `nil`                    |
| `boolean`                     | `bool`                                                     | `bool`                   |
| `bytes`                       | `[]byte`                                                   | `[]byte`                 |
| `float`                       | `float32`                                                  | `float32`                |
| `double`                      | `float64`                                                  | `float64`                |
| `long`                        | `int`\*, `int64`, `uint32`\**                              | `int`, `int64`, `uint32` |
| `int`                         | `int`\*, `int32`, `int16`, `int8`, `uint8`\**, `uint16`\** | `int`, `uint8`, `uint16` |
| `fixed`                       | `uint64`                                                   | `uint64`                 |
| `string`                      | `string`                                                   | `string`                 |
| `array`                       | `[]T`                                                      | `[]any`                  |
| `enum`                        | `string`                                                   | `string`                 |
| `fixed`                       | `[n]byte`                                                  | `[n]byte`                |
| `map`                         | `map[string]T{}`                                           | `map[string]any`         |
| `record`                      | `struct`                                                   | `map[string]any`         |
| `union`                       | *see below*                                                | *see below*              |
| `int.date`                    | `time.Time`                                                | `time.Time`              |
| `int.time-millis`             | `time.Duration`                                            | `time.Duration`          |
| `long.time-micros`            | `time.Duration`                                            | `time.Duration`          |
| `long.timestamp-millis`       | `time.Time`                                                | `time.Time`              |
| `long.timestamp-micros`       | `time.Time`                                                | `time.Time`              |
| `long.timestamp-nanos`        | `time.Time`                                                | `time.Time`              |
| `long.local-timestamp-millis` | `time.Time`                                                | `time.Time`              |
| `long.local-timestamp-micros` | `time.Time`                                                | `time.Time`              |
| `long.local-timestamp-nanos`  | `time.Time`                                                | `time.Time`              |
| `bytes.decimal`               | `*big.Rat`                                                 | `*big.Rat`               |
| `fixed.decimal`               | `*big.Rat`                                                 | `*big.Rat`               |
| `string.uuid`                 | `string`                                                   | `string`                 |

\* Please note that the size of the Go type `int` is platform dependent. Decoding an Avro `long` into a Go `int` is
only allowed on 64-bit platforms and will result in an error on 32-bit platforms. Encoding a Go `int` as an Avro `int`
is an error when the value does not fit 32 bits.

\** A value that does not fit the Go type is an error, not a wrapped value: a *negative* Avro `int = -100` decoded
into a `uint16`, or `int = 256` decoded into a `uint8`, fails. (iqhive/avro; hamba/avro wrapped such values silently.)

Decoding into a value that already holds data overwrites what the data names. A map keeps the entries the new data
does not name, as `encoding/json` does; a slice, including one decoded from an empty array, holds exactly the new
elements; and struct fields the schema does not name are left as they were.

#### Unions

The following union types are accepted: `map[string]any`, `*T` and `any`.

* **map[string]any:** If the union value is `nil`, a `nil` map will be en/decoded.
When a non-`nil` union value is encountered, a single key is en/decoded. The key is the avro
type name, or schema full name in the case of a named schema (enum, fixed or record).
* ***T:** This is allowed in a "nullable" union. A nullable union is defined as a two schema union,
with one of the types being `null` (ie. `["null", "string"]` or `["string", "null"]`), in this case
a `*T` is allowed, with `T` matching the conversion table above. In the case of a slice, the slice can be used
directly. A struct held by value (`T` rather than `*T`, for a record or a `time.Time`) is decoded in place, and a
`null` leaves its zero value.
* ***struct{}:** implementing the `UnionConverter` interface:

```go
// UnionConverter to handle Avro Union's in a type-safe way
type UnionConverter interface {
    // FromAny payload decode into any of the mentioned types in the Union.
    FromAny(payload any) error
    // ToAny from the Union struct
    ToAny() (any, error)
}

// for example:
const Schema = `{"name": "Payload", "type": "record", "fields": [{"name": "union", "type": ["int", {"type": "record", "name": "test", "fields" : [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]}]}]}`

type Payload struct {
    Union *UnionRecord `avro:"union"`
}

type UnionRecord struct {
    Int  *int
    Test *TestRecord
}

func (u *UnionRecord) ToAny() (any, error) {
    if u.Int != nil {
        return u.Int, nil
    } else if u.Test != nil {
        return u.Test, nil
    }

    return nil, errors.New("no value to encode")
}

func (u *UnionRecord) FromAny(payload any) error {
    switch t := payload.(type) {
    case int:
        u.Int = &t
    case TestRecord:
        u.Test = &t
    default:
        return errors.New("unknown type during decode of union")
    }

    return nil
}

type TestRecord struct {
    A int64  `avro:"a"`
    B string `avro:"b"`
}
```

Note due to way Go checks if some type implements these interface, the type used *must* be a pointer as the interface methods *must*
be implemented with pointer receivers.

* **any:** An `interface` can be provided and the type or name resolved. Primitive types
are pre-registered, but named types, maps and slices will need to be registered with the `Register` function.
In the case of arrays and maps the enclosed schema type or name is postfix to the type with a `:` separator,
e.g `"map:string"`. Behavior when a type cannot be resolved will depend on your chosen configuation options:
 	* !Config.UnionResolutionError && !Config.PartialUnionTypeResolution: the map type above is used
 	* Config.UnionResolutionError && !Config.PartialUnionTypeResolution: an error is returned
 	* !Config.UnionResolutionError && Config.PartialUnionTypeResolution: any registered type will get resolved while any unregistered type will fallback to the map type above.
 	* Config.UnionResolutionError && !Config.PartialUnionTypeResolution: any registered type will get resolved while any unregistered type will return an error.

#### TextMarshaler and TextUnmarshaler

The interfaces `TextMarshaler` and `TextUnmarshaler` are supported for a `string` schema type. The object will
be tested first for implementation of these interfaces, in the case of a `string` schema, before trying regular
encoding and decoding.

Enums may also implement `TextMarshaler` and `TextUnmarshaler`, and must resolve to valid symbols in the given enum schema.

#### Identical Underlying Types

One type can be [ConvertibleTo](https://go.dev/ref/spec#Conversions) another type if they have identical underlying types.
A non-native type is allowed to be used if it can be convertible to *time.Time*, *big.Rat* or *avro.LogicalDuration* for the particular of *LogicalTypes*.

Ex.: `type Timestamp time.Time`

#### Custom Type Conversion

In case of incompatible types, custom type conversion functions can be registered with the `RegisterTypeConverters` function.
This requires the use of `map[string]any` or `[]any`.
The type conversion for encoding will receive the original value that is to be encoded, and must return a data type that is compatible with the schema, as specified in the table above.
The type conversion for decoding will receive the decoded value with a data type that is compatible with the schema, and its return value will be used as the final decoded value.

#### Untrusted Input

When processing untrusted Avro data, it is important to configure protection against Denial of Service (DoS) attacks. The library provides three configuration options to limit resource allocation:

- `Config.MaxByteSliceSize` - Restricts the maximum size of `bytes` and `string` types (default: `1MiB`)
- `Config.MaxSliceAllocSize` - Limits the maximum size of slice allocations (default: unlimited)
- `Config.MaxMapAllocSize` - Limits the maximum size of map allocations (default: unlimited)

For comprehensive security considerations and configuration requirements, please refer to [SECURITY.md](SECURITY.md).

## Benchmark

Benchmark source code can be found at: [https://github.com/iskorotkov/avro-benchmarks](https://github.com/iskorotkov/avro-benchmarks)

```go
goos: darwin
goarch: arm64
pkg: github.com/iskorotkov/avro-benchmarks
cpu: Apple M4 Pro
BenchmarkGoAvroDecode-12                 1248079               960.1 ns/op           418 B/op         27 allocs/op
BenchmarkGoAvroEncode-12                  941415              1310 ns/op             877 B/op         63 allocs/op
BenchmarkGoGenAvroDecode-12              2594593               466.0 ns/op           320 B/op         11 allocs/op
BenchmarkGoGenAvroEncode-12              5187818               229.2 ns/op           240 B/op          3 allocs/op
BenchmarkHambaDecode-12                  8118740               147.3 ns/op            47 B/op          0 allocs/op
BenchmarkHambaEncode-12                  8540484               139.7 ns/op           112 B/op          1 allocs/op
BenchmarkHeetchDecode-12                   96644             12441 ns/op           37773 B/op        385 allocs/op
BenchmarkHeetchEncode-12                 5392798               221.5 ns/op           384 B/op          5 allocs/op
BenchmarkIskorotkovDecode-12            10105106               118.6 ns/op            47 B/op          0 allocs/op
BenchmarkIskorotkovEncode-12             8573349               139.3 ns/op           112 B/op          1 allocs/op
BenchmarkLinkedinDecode-12               1882018               637.8 ns/op          1688 B/op         35 allocs/op
BenchmarkLinkedinEncode-12               4127374               288.0 ns/op           248 B/op          5 allocs/op
```

Always benchmark with your own workload. The result depends heavily on the data input.

## Go structs generation

Go structs can be generated for you from the schema. The types generated follow the same logic in [types conversions](#types-conversions)
You can use the avrogen command line tool to generate the structs, or use it as a lib in internal commands, it's the `gen` package.

Install the struct generator with:

```shell
go install github.com/iqhive/avro/v2/cmd/avrogen@<version>
```

Example usage assuming there's a valid schema in `in.avsc`:

```shell
avrogen -pkg avro -o bla.go -tags json:snake,yaml:upper-camel in.avsc
```

**Tip:** Omit `-o FILE` to dump the generated Go structs to stdout instead of a file.

Check the options and usage with `-h`:

```shell
avrogen -h
```

### Custom logical type mapping with avrogen

You can register custom logical type mappings to be used during code generation.

The format of a custom logical type mapper is `avroLogicalType,goType[,importPath]`. For example,
to map the logical type `uuid` to the Go type `github.com/google/uuid.UUID`, you would use:

```shell
avrogen -pkg avro -o bla.go -logical-type uuid,uuid.UUID,github.com/google/uuid in.avsc
```

If the type you are mapping to is a built-in Go type (e.g., `string`, `int`, etc.), you can omit the import path element in the mapping definition:

```shell
avrogen -pkg avro -o bla.go -logical-type date,int32 in.avsc
```

If you intend to use multiple custom logical type mappings, you can specify the `-logicaltype` flag multiple times.

### Field type overrides

You can override the generated Go type of a specific field, identified by its record's full name and field
name, without editing the schema. The format is `record.field,goType[,importPath]`, and you can specify
`-fieldtype` multiple times, omitting the import path for built-in Go types:

```shell
avrogen -pkg avro -o bla.go -fieldtype a.b.test.someBytes,json.RawMessage,encoding/json in.avsc
```

A union field receives the override type verbatim, so a nullable field needs an explicit `*` in the Go type
(e.g. `*json.RawMessage`).

## Avro schema validation

### avrosv

A small Avro schema validation command-line utility is also available. This simple tool leverages the
schema parsing functionality of the library, showing validation errors or optionally dumping parsed
schemas to the console. It can be used in CI/CD pipelines to validate schema changes in a repository.

Install the Avro schema validator with:

```shell
go install github.com/iqhive/avro/v2/cmd/avrosv@<version>
```

Example usage assuming there's a valid schema in `in.avsc` (exit status code is `0`):

```shell
avrosv in.avsc
```

An invalid schema will result in a diagnostic output and a non-zero exit status code:

```shell
avrosv bad-default-schema.avsc; echo $?
Error: avro: invalid default for field someString. <nil> not a string
2
```

Schemas referencing other schemas can also be validated by providing all of them (schemas are parsed in order):

```shell
avrosv base-schema.avsc schema-withref.avsc
```

Check the options and usage with `-h`:

```shell
avrosv -h
```

### Name Validation

Avro names are validated according to the
[Avro specification](https://avro.apache.org/docs/1.11.1/specification/#names).

However, the official Java library does not validate said names accordingly, resulting to some files out in the wild
to have invalid names. Thus, this library has a configuration option to allow for these invalid names to be parsed.

```go
avro.SkipNameValidation = true
```

Note that this variable is global, so ideally you'd need to unset it after you're done with the invalid schema.

## Go Version Support

This library supports the last two versions of Go. While the minimum Go version is
not guaranteed to increase along side Go, it may jump from time to time to support
additional features. This will be not be considered a breaking change.

## Who uses this library?

* [Apache Arrow for Go](https://github.com/apache/arrow-go)
* [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go)
* [pulsar-client-go](https://github.com/apache/pulsar-client-go)

Create a GitHub Issue if you use this library and want to be included in this list!
