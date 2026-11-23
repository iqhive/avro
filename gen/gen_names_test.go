package gen_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iqhive/avro/v2"
	"github.com/iqhive/avro/v2/gen"
)

// gen named a record's Go type after its short name and kept only the first
// type of each name, so two records named Item in different namespaces became
// one struct: the second record's fields were typed with the first record's
// struct, and its own fields had no Go field at all (decoding skipped them).
func TestStruct_SameShortNameInTwoNamespaces(t *testing.T) {
	schema := `{"type":"record","name":"Top","namespace":"t","fields":[
		{"name":"a","type":{"type":"record","name":"Item","namespace":"a","fields":[{"name":"x","type":"long"}]}},
		{"name":"b","type":{"type":"record","name":"Item","namespace":"b","fields":[{"name":"y","type":"string"}]}},
		{"name":"c","type":"a.Item"}
	]}`

	var buf bytes.Buffer
	err := gen.Struct(schema, &buf, gen.Config{PackageName: "p"})
	require.Error(t, err)
	assert.ErrorContains(t, err, "a.Item")
	assert.ErrorContains(t, err, "b.Item")

	// With full names the two records are two structs.
	buf.Reset()
	require.NoError(t, gen.Struct(schema, &buf, gen.Config{PackageName: "p", FullName: true}))
	assert.Contains(t, buf.String(), "type AItem struct")
	assert.Contains(t, buf.String(), "type BItem struct")
	assert.Contains(t, buf.String(), `avro:"y"`)

	// Full names can clash too (a.b.c and a_b.c are both ABC); the error then
	// does not suggest the FullName already set.
	clash := `{"type":"record","name":"Top","namespace":"t","fields":[
		{"name":"a","type":{"type":"record","name":"c","namespace":"a.b","fields":[{"name":"x","type":"long"}]}},
		{"name":"b","type":{"type":"record","name":"c","namespace":"a_b","fields":[{"name":"y","type":"string"}]}}
	]}`
	err = gen.Struct(clash, &bytes.Buffer{}, gen.Config{PackageName: "p", FullName: true})
	require.Error(t, err)
	assert.ErrorContains(t, err, "a.b.c")
	assert.ErrorContains(t, err, "a_b.c")
	assert.NotContains(t, err.Error(), "FullName")
}

// With enums generated, two enums named Color in different namespaces became
// one Go type with the first enum's symbols.
func TestGenerator_SameShortNameEnumsInTwoNamespaces(t *testing.T) {
	schema, err := avro.ParseWithCache(`{"type":"record","name":"Top","namespace":"t","fields":[
		{"name":"a","type":{"type":"enum","name":"Color","namespace":"a","symbols":["RED"]}},
		{"name":"b","type":{"type":"enum","name":"Color","namespace":"b","symbols":["BLUE"]}}
	]}`, "", &avro.SchemaCache{})
	require.NoError(t, err)

	g := gen.NewGenerator("p", nil, gen.WithEnums(true))
	g.Parse(schema)
	err = g.Write(&bytes.Buffer{})
	require.Error(t, err)
	assert.ErrorContains(t, err, "a.Color")
	assert.ErrorContains(t, err, "b.Color")

	// One record parsed twice, as avrogen does for files that share a type,
	// is one type, not a clash.
	g = gen.NewGenerator("p", nil)
	g.Parse(schema)
	g.Parse(schema)
	var buf bytes.Buffer
	require.NoError(t, g.Write(&buf))
	assert.Equal(t, 1, strings.Count(buf.String(), "type Top struct"))
}
